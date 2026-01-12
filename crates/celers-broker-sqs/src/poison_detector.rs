//! Poison message detection and isolation
//!
//! This module detects messages that repeatedly fail processing and provides
//! utilities to isolate them before they consume system resources or trigger
//! cascading failures.
//!
//! # Features
//!
//! - Automatic detection of repeatedly failing messages
//! - Configurable failure thresholds
//! - Message fingerprinting for tracking
//! - Automatic isolation to poison queue
//! - Failure pattern analysis
//! - Integration with DLQ analytics
//!
//! # Example
//!
//! ```
//! use celers_broker_sqs::poison_detector::{PoisonDetector, PoisonConfig};
//! use std::time::Duration;
//!
//! # fn main() {
//! // Configure poison message detection
//! let config = PoisonConfig::new()
//!     .with_max_failures(5)
//!     .with_failure_window(Duration::from_secs(3600)) // 1 hour
//!     .with_auto_isolate(true);
//!
//! let mut detector = PoisonDetector::new(config);
//!
//! // Track message failures
//! let message_id = "msg-123";
//! let error_msg = "Database connection timeout";
//!
//! if detector.track_failure(message_id, error_msg) {
//!     // Message identified as poison - isolate it
//!     println!("Poison message detected: {}", message_id);
//!     detector.isolate_message(message_id);
//! }
//!
//! // Get poison message statistics
//! let stats = detector.statistics();
//! println!("Detected {} poison messages", stats.poison_message_count);
//! # }
//! ```

use std::collections::HashMap;
use std::time::{Duration, SystemTime};

/// Configuration for poison message detection
#[derive(Debug, Clone)]
pub struct PoisonConfig {
    /// Maximum number of failures before marking as poison
    pub max_failures: usize,

    /// Time window for counting failures
    pub failure_window: Duration,

    /// Automatically isolate poison messages
    pub auto_isolate: bool,

    /// Track error patterns for analysis
    pub track_error_patterns: bool,

    /// Minimum failures for pattern analysis
    pub min_failures_for_pattern: usize,
}

impl PoisonConfig {
    /// Create new configuration with sensible defaults
    pub fn new() -> Self {
        Self {
            max_failures: 5,
            failure_window: Duration::from_secs(3600), // 1 hour
            auto_isolate: true,
            track_error_patterns: true,
            min_failures_for_pattern: 3,
        }
    }

    /// Set maximum failures threshold
    pub fn with_max_failures(mut self, max: usize) -> Self {
        self.max_failures = max.max(1);
        self
    }

    /// Set failure tracking window
    pub fn with_failure_window(mut self, window: Duration) -> Self {
        self.failure_window = window;
        self
    }

    /// Enable or disable auto-isolation
    pub fn with_auto_isolate(mut self, enabled: bool) -> Self {
        self.auto_isolate = enabled;
        self
    }

    /// Enable or disable error pattern tracking
    pub fn with_track_error_patterns(mut self, enabled: bool) -> Self {
        self.track_error_patterns = enabled;
        self
    }

    /// Set minimum failures for pattern analysis
    pub fn with_min_failures_for_pattern(mut self, min: usize) -> Self {
        self.min_failures_for_pattern = min.max(1);
        self
    }
}

impl Default for PoisonConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Failure record for a message
#[derive(Debug, Clone)]
struct FailureRecord {
    message_id: String,
    timestamps: Vec<SystemTime>,
    error_messages: Vec<String>,
    is_isolated: bool,
}

/// Error pattern information
#[derive(Debug, Clone)]
pub struct ErrorPattern {
    /// Error message pattern (e.g., "Database connection timeout")
    pub pattern: String,

    /// Number of occurrences
    pub count: usize,

    /// Affected message IDs
    pub message_ids: Vec<String>,
}

/// Poison message statistics
#[derive(Debug, Clone)]
pub struct PoisonStatistics {
    /// Total number of poison messages detected
    pub poison_message_count: usize,

    /// Total number of isolated messages
    pub isolated_message_count: usize,

    /// Total failures tracked
    pub total_failures: u64,

    /// Common error patterns
    pub error_patterns: Vec<ErrorPattern>,

    /// Messages currently being tracked
    pub tracked_message_count: usize,
}

/// Poison message detector
pub struct PoisonDetector {
    config: PoisonConfig,
    failures: HashMap<String, FailureRecord>,
    isolated_messages: Vec<String>,
    total_failures: u64,
}

impl PoisonDetector {
    /// Create a new poison detector
    pub fn new(config: PoisonConfig) -> Self {
        Self {
            config,
            failures: HashMap::new(),
            isolated_messages: Vec::new(),
            total_failures: 0,
        }
    }

    /// Track a message failure. Returns true if message is identified as poison.
    pub fn track_failure(&mut self, message_id: &str, error_message: &str) -> bool {
        self.total_failures += 1;
        let now = SystemTime::now();

        let (is_poison, should_add_to_isolated) = {
            // Get or create failure record
            let record = self
                .failures
                .entry(message_id.to_string())
                .or_insert_with(|| FailureRecord {
                    message_id: message_id.to_string(),
                    timestamps: Vec::new(),
                    error_messages: Vec::new(),
                    is_isolated: false,
                });

            // Add new failure
            record.timestamps.push(now);
            if self.config.track_error_patterns {
                record.error_messages.push(error_message.to_string());
            }

            // Clean up old failures outside the window (inline to avoid borrow issues)
            if let Some(cutoff_time) = SystemTime::now().checked_sub(self.config.failure_window) {
                record.timestamps.retain(|ts| *ts >= cutoff_time);

                // Keep only recent error messages (matching timestamps)
                if self.config.track_error_patterns
                    && record.error_messages.len() > record.timestamps.len()
                {
                    let excess = record.error_messages.len() - record.timestamps.len();
                    record.error_messages.drain(0..excess);
                }
            }

            // Check if message is now poison
            let is_poison = record.timestamps.len() >= self.config.max_failures;

            // Auto-isolate if configured and message is poison
            let should_add = if is_poison && self.config.auto_isolate && !record.is_isolated {
                record.is_isolated = true;
                true
            } else {
                false
            };

            (is_poison, should_add)
        }; // record borrow ends here

        // Now we can safely modify isolated_messages
        if should_add_to_isolated && !self.isolated_messages.contains(&message_id.to_string()) {
            self.isolated_messages.push(message_id.to_string());
        }

        is_poison
    }

    /// Cleanup failures outside the time window
    #[allow(dead_code)]
    fn cleanup_old_failures(&self, record: &mut FailureRecord) {
        if let Some(cutoff_time) = SystemTime::now().checked_sub(self.config.failure_window) {
            let mut indices_to_remove = Vec::new();

            for (i, timestamp) in record.timestamps.iter().enumerate() {
                if *timestamp < cutoff_time {
                    indices_to_remove.push(i);
                }
            }

            // Remove in reverse order to maintain indices
            for i in indices_to_remove.iter().rev() {
                record.timestamps.remove(*i);
                if self.config.track_error_patterns && *i < record.error_messages.len() {
                    record.error_messages.remove(*i);
                }
            }
        }
    }

    /// Manually isolate a message
    pub fn isolate_message(&mut self, message_id: &str) {
        if let Some(record) = self.failures.get_mut(message_id) {
            if !record.is_isolated {
                record.is_isolated = true;
                self.isolated_messages.push(message_id.to_string());
            }
        }
    }

    /// Check if a message is poison
    pub fn is_poison(&self, message_id: &str) -> bool {
        self.failures
            .get(message_id)
            .map(|r| r.timestamps.len() >= self.config.max_failures)
            .unwrap_or(false)
    }

    /// Check if a message is isolated
    pub fn is_isolated(&self, message_id: &str) -> bool {
        self.failures
            .get(message_id)
            .map(|r| r.is_isolated)
            .unwrap_or(false)
    }

    /// Get failure count for a message
    pub fn failure_count(&self, message_id: &str) -> usize {
        self.failures
            .get(message_id)
            .map(|r| r.timestamps.len())
            .unwrap_or(0)
    }

    /// Get all poison message IDs
    pub fn get_poison_messages(&self) -> Vec<String> {
        self.failures
            .values()
            .filter(|r| r.timestamps.len() >= self.config.max_failures)
            .map(|r| r.message_id.clone())
            .collect()
    }

    /// Get all isolated message IDs
    pub fn get_isolated_messages(&self) -> Vec<String> {
        self.isolated_messages.clone()
    }

    /// Analyze error patterns
    pub fn analyze_error_patterns(&self) -> Vec<ErrorPattern> {
        if !self.config.track_error_patterns {
            return Vec::new();
        }

        let mut pattern_map: HashMap<String, Vec<String>> = HashMap::new();

        // Collect error patterns
        for record in self.failures.values() {
            if record.error_messages.len() >= self.config.min_failures_for_pattern {
                for error_msg in &record.error_messages {
                    // Simplify error message (take first 50 chars as pattern)
                    let pattern = error_msg.chars().take(50).collect::<String>();
                    pattern_map
                        .entry(pattern)
                        .or_default()
                        .push(record.message_id.clone());
                }
            }
        }

        // Convert to ErrorPattern structs
        let mut patterns: Vec<ErrorPattern> = pattern_map
            .into_iter()
            .map(|(pattern, message_ids)| {
                let count = message_ids.len();
                ErrorPattern {
                    pattern,
                    count,
                    message_ids,
                }
            })
            .collect();

        // Sort by count (most common first)
        patterns.sort_by(|a, b| b.count.cmp(&a.count));

        patterns
    }

    /// Get statistics
    pub fn statistics(&self) -> PoisonStatistics {
        let poison_message_count = self.get_poison_messages().len();
        let error_patterns = self.analyze_error_patterns();

        PoisonStatistics {
            poison_message_count,
            isolated_message_count: self.isolated_messages.len(),
            total_failures: self.total_failures,
            error_patterns,
            tracked_message_count: self.failures.len(),
        }
    }

    /// Clear a message's failure history (e.g., after successful processing)
    pub fn clear_message(&mut self, message_id: &str) {
        self.failures.remove(message_id);
        self.isolated_messages.retain(|id| id != message_id);
    }

    /// Clear all tracking data
    pub fn clear_all(&mut self) {
        self.failures.clear();
        self.isolated_messages.clear();
        self.total_failures = 0;
    }

    /// Cleanup old records that are outside the failure window
    pub fn cleanup(&mut self) {
        let cutoff_time = SystemTime::now()
            .checked_sub(self.config.failure_window)
            .unwrap_or_else(SystemTime::now);

        self.failures.retain(|_, record| {
            // Remove old timestamps
            record.timestamps.retain(|ts| *ts >= cutoff_time);

            // Keep record if it has recent failures or is isolated
            !record.timestamps.is_empty() || record.is_isolated
        });
    }

    /// Get detailed failure information for a message
    pub fn get_failure_info(&self, message_id: &str) -> Option<FailureInfo> {
        self.failures.get(message_id).map(|record| {
            let first_failure = record.timestamps.first().copied();
            let last_failure = record.timestamps.last().copied();

            FailureInfo {
                message_id: message_id.to_string(),
                failure_count: record.timestamps.len(),
                first_failure,
                last_failure,
                error_messages: record.error_messages.clone(),
                is_isolated: record.is_isolated,
            }
        })
    }
}

/// Detailed failure information for a message
#[derive(Debug, Clone)]
pub struct FailureInfo {
    /// Message ID
    pub message_id: String,

    /// Number of failures
    pub failure_count: usize,

    /// First failure timestamp
    pub first_failure: Option<SystemTime>,

    /// Last failure timestamp
    pub last_failure: Option<SystemTime>,

    /// Error messages
    pub error_messages: Vec<String>,

    /// Whether message is isolated
    pub is_isolated: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread;

    #[test]
    fn test_poison_config_defaults() {
        let config = PoisonConfig::new();
        assert_eq!(config.max_failures, 5);
        assert_eq!(config.failure_window, Duration::from_secs(3600));
        assert!(config.auto_isolate);
        assert!(config.track_error_patterns);
    }

    #[test]
    fn test_poison_config_builder() {
        let config = PoisonConfig::new()
            .with_max_failures(3)
            .with_failure_window(Duration::from_secs(60))
            .with_auto_isolate(false);

        assert_eq!(config.max_failures, 3);
        assert_eq!(config.failure_window, Duration::from_secs(60));
        assert!(!config.auto_isolate);
    }

    #[test]
    fn test_track_failure_below_threshold() {
        let config = PoisonConfig::new().with_max_failures(3);
        let mut detector = PoisonDetector::new(config);

        let is_poison = detector.track_failure("msg-1", "Error 1");
        assert!(!is_poison);

        let is_poison = detector.track_failure("msg-1", "Error 2");
        assert!(!is_poison);

        assert_eq!(detector.failure_count("msg-1"), 2);
    }

    #[test]
    fn test_track_failure_at_threshold() {
        let config = PoisonConfig::new()
            .with_max_failures(3)
            .with_auto_isolate(false);

        let mut detector = PoisonDetector::new(config);

        detector.track_failure("msg-1", "Error 1");
        detector.track_failure("msg-1", "Error 2");
        let is_poison = detector.track_failure("msg-1", "Error 3");

        assert!(is_poison);
        assert!(detector.is_poison("msg-1"));
    }

    #[test]
    fn test_auto_isolate() {
        let config = PoisonConfig::new()
            .with_max_failures(2)
            .with_auto_isolate(true);

        let mut detector = PoisonDetector::new(config);

        detector.track_failure("msg-1", "Error 1");
        detector.track_failure("msg-1", "Error 2");

        assert!(detector.is_isolated("msg-1"));
        assert_eq!(detector.get_isolated_messages().len(), 1);
    }

    #[test]
    fn test_manual_isolate() {
        let config = PoisonConfig::new().with_auto_isolate(false);
        let mut detector = PoisonDetector::new(config);

        detector.track_failure("msg-1", "Error");
        detector.isolate_message("msg-1");

        assert!(detector.is_isolated("msg-1"));
    }

    #[test]
    fn test_get_poison_messages() {
        let config = PoisonConfig::new()
            .with_max_failures(2)
            .with_auto_isolate(false);

        let mut detector = PoisonDetector::new(config);

        detector.track_failure("msg-1", "Error");
        detector.track_failure("msg-1", "Error");
        detector.track_failure("msg-2", "Error");
        detector.track_failure("msg-2", "Error");

        let poison_messages = detector.get_poison_messages();
        assert_eq!(poison_messages.len(), 2);
        assert!(poison_messages.contains(&"msg-1".to_string()));
        assert!(poison_messages.contains(&"msg-2".to_string()));
    }

    #[test]
    fn test_error_pattern_analysis() {
        let config = PoisonConfig::new()
            .with_max_failures(10)
            .with_min_failures_for_pattern(2);

        let mut detector = PoisonDetector::new(config);

        // Create pattern with multiple occurrences
        detector.track_failure("msg-1", "Database timeout");
        detector.track_failure("msg-1", "Database timeout");
        detector.track_failure("msg-2", "Database timeout");
        detector.track_failure("msg-2", "Database timeout");
        detector.track_failure("msg-3", "Network error");

        let patterns = detector.analyze_error_patterns();
        assert!(!patterns.is_empty());

        // Find the database timeout pattern
        let db_pattern = patterns
            .iter()
            .find(|p| p.pattern.contains("Database timeout"))
            .expect("Database timeout pattern should exist");

        assert!(db_pattern.count >= 2);
    }

    #[test]
    fn test_statistics() {
        let config = PoisonConfig::new().with_max_failures(2);
        let mut detector = PoisonDetector::new(config);

        detector.track_failure("msg-1", "Error");
        detector.track_failure("msg-1", "Error");
        detector.track_failure("msg-2", "Error");

        let stats = detector.statistics();
        assert_eq!(stats.poison_message_count, 1);
        assert_eq!(stats.total_failures, 3);
        assert_eq!(stats.tracked_message_count, 2);
    }

    #[test]
    fn test_clear_message() {
        let config = PoisonConfig::new();
        let mut detector = PoisonDetector::new(config);

        detector.track_failure("msg-1", "Error");
        detector.isolate_message("msg-1");

        assert!(detector.is_isolated("msg-1"));

        detector.clear_message("msg-1");

        assert!(!detector.is_isolated("msg-1"));
        assert_eq!(detector.failure_count("msg-1"), 0);
    }

    #[test]
    fn test_clear_all() {
        let config = PoisonConfig::new();
        let mut detector = PoisonDetector::new(config);

        detector.track_failure("msg-1", "Error");
        detector.track_failure("msg-2", "Error");

        detector.clear_all();

        assert_eq!(detector.statistics().tracked_message_count, 0);
        assert_eq!(detector.statistics().total_failures, 0);
    }

    #[test]
    fn test_get_failure_info() {
        let config = PoisonConfig::new();
        let mut detector = PoisonDetector::new(config);

        detector.track_failure("msg-1", "Error 1");
        thread::sleep(Duration::from_millis(10));
        detector.track_failure("msg-1", "Error 2");

        let info = detector.get_failure_info("msg-1").unwrap();
        assert_eq!(info.message_id, "msg-1");
        assert_eq!(info.failure_count, 2);
        assert_eq!(info.error_messages.len(), 2);
        assert!(info.first_failure.is_some());
        assert!(info.last_failure.is_some());
    }

    #[test]
    fn test_cleanup_old_failures() {
        let config = PoisonConfig::new().with_failure_window(Duration::from_millis(100));

        let mut detector = PoisonDetector::new(config);

        detector.track_failure("msg-1", "Error 1");
        assert_eq!(detector.failure_count("msg-1"), 1);

        thread::sleep(Duration::from_millis(150));

        detector.track_failure("msg-1", "Error 2");
        detector.cleanup();

        // Old failure should be cleaned up, only recent one remains
        assert_eq!(detector.failure_count("msg-1"), 1);
    }

    #[test]
    fn test_failure_window() {
        let config = PoisonConfig::new()
            .with_max_failures(3)
            .with_failure_window(Duration::from_millis(100));

        let mut detector = PoisonDetector::new(config);

        // Add 2 failures
        detector.track_failure("msg-1", "Error 1");
        detector.track_failure("msg-1", "Error 2");

        // Wait for window to expire
        thread::sleep(Duration::from_millis(150));

        // Add another failure (should not be poison since old ones expired)
        let is_poison = detector.track_failure("msg-1", "Error 3");
        detector.cleanup();

        assert!(!is_poison);
        assert_eq!(detector.failure_count("msg-1"), 1);
    }
}
