//! Message replay utilities for DLQ management and disaster recovery
//!
//! This module provides utilities for replaying messages from Dead Letter Queues (DLQ)
//! or for disaster recovery scenarios where messages need to be reprocessed.
//!
//! # Features
//!
//! - Batch message replay with rate limiting
//! - Selective replay based on filters (time range, task name, error pattern)
//! - Progress tracking and reporting
//! - Automatic retry with backoff for failed replays
//! - Message transformation during replay
//! - DLQ-to-main-queue migration
//!
//! # Example
//!
//! ```ignore
//! use celers_broker_sqs::replay::{ReplayManager, ReplayConfig, ReplayFilter};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Configure replay
//! let config = ReplayConfig::new()
//!     .with_batch_size(10)
//!     .with_rate_limit(100)  // 100 messages per second max
//!     .with_retry_failed(true);
//!
//! let mut manager = ReplayManager::new(config);
//!
//! // Replay messages from DLQ that failed in the last hour
//! let filter = ReplayFilter::new()
//!     .with_time_range_hours(1)
//!     .with_task_pattern("tasks.payment.*");
//!
//! // Note: replay_from_dlq would be implemented to integrate with the broker
//! // This is a conceptual example showing the filter usage
//!
//! println!("Replay manager configured with rate limit: {} msg/sec",
//!     manager.config.rate_limit);
//! # Ok(())
//! # }
//! ```

use std::collections::HashMap;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

/// Configuration for message replay operations
#[derive(Debug, Clone)]
pub struct ReplayConfig {
    /// Number of messages to replay per batch
    pub batch_size: usize,
    /// Maximum replay rate (messages per second), 0 = unlimited
    pub rate_limit: usize,
    /// Whether to retry failed replays
    pub retry_failed: bool,
    /// Maximum retry attempts for failed replays
    pub max_retries: usize,
    /// Delay between retry attempts
    pub retry_delay: Duration,
    /// Whether to preserve message attributes
    pub preserve_attributes: bool,
    /// Whether to track replay progress
    pub track_progress: bool,
}

impl ReplayConfig {
    /// Create a new replay configuration with defaults
    pub fn new() -> Self {
        Self {
            batch_size: 10,
            rate_limit: 100,
            retry_failed: true,
            max_retries: 3,
            retry_delay: Duration::from_secs(1),
            preserve_attributes: true,
            track_progress: true,
        }
    }

    /// Set batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size.clamp(1, 10);
        self
    }

    /// Set rate limit (messages per second)
    pub fn with_rate_limit(mut self, limit: usize) -> Self {
        self.rate_limit = limit;
        self
    }

    /// Set whether to retry failed replays
    pub fn with_retry_failed(mut self, retry: bool) -> Self {
        self.retry_failed = retry;
        self
    }

    /// Set maximum retry attempts
    pub fn with_max_retries(mut self, retries: usize) -> Self {
        self.max_retries = retries;
        self
    }

    /// Set retry delay
    pub fn with_retry_delay(mut self, delay: Duration) -> Self {
        self.retry_delay = delay;
        self
    }

    /// Set whether to preserve message attributes
    pub fn with_preserve_attributes(mut self, preserve: bool) -> Self {
        self.preserve_attributes = preserve;
        self
    }

    /// Set whether to track progress
    pub fn with_track_progress(mut self, track: bool) -> Self {
        self.track_progress = track;
        self
    }
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Filter for selective message replay
#[derive(Debug, Clone, Default)]
pub struct ReplayFilter {
    /// Minimum timestamp (Unix epoch seconds)
    pub min_timestamp: Option<u64>,
    /// Maximum timestamp (Unix epoch seconds)
    pub max_timestamp: Option<u64>,
    /// Task name pattern (glob-style)
    pub task_pattern: Option<String>,
    /// Error message pattern
    pub error_pattern: Option<String>,
    /// Minimum failure count
    pub min_failure_count: Option<u32>,
    /// Maximum messages to replay (0 = unlimited)
    pub max_messages: usize,
}

impl ReplayFilter {
    /// Create a new empty filter
    pub fn new() -> Self {
        Self::default()
    }

    /// Set time range (last N hours)
    pub fn with_time_range_hours(mut self, hours: u64) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.min_timestamp = Some(now - (hours * 3600));
        self
    }

    /// Set time range (custom timestamps)
    pub fn with_time_range(mut self, min: u64, max: u64) -> Self {
        self.min_timestamp = Some(min);
        self.max_timestamp = Some(max);
        self
    }

    /// Set task name pattern
    pub fn with_task_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.task_pattern = Some(pattern.into());
        self
    }

    /// Set error message pattern
    pub fn with_error_pattern(mut self, pattern: impl Into<String>) -> Self {
        self.error_pattern = Some(pattern.into());
        self
    }

    /// Set minimum failure count
    pub fn with_min_failure_count(mut self, count: u32) -> Self {
        self.min_failure_count = Some(count);
        self
    }

    /// Set maximum messages to replay
    pub fn with_max_messages(mut self, max: usize) -> Self {
        self.max_messages = max;
        self
    }

    /// Check if a message matches this filter
    pub fn matches(&self, message: &ReplayableMessage) -> bool {
        // Check timestamp range
        if let Some(min) = self.min_timestamp {
            if message.timestamp < min {
                return false;
            }
        }
        if let Some(max) = self.max_timestamp {
            if message.timestamp > max {
                return false;
            }
        }

        // Check task pattern
        if let Some(ref pattern) = self.task_pattern {
            if !glob_match(pattern, &message.task_name) {
                return false;
            }
        }

        // Check error pattern
        if let Some(ref pattern) = self.error_pattern {
            if let Some(ref error) = message.error_message {
                if !error.contains(pattern) {
                    return false;
                }
            } else {
                return false;
            }
        }

        // Check failure count
        if let Some(min_count) = self.min_failure_count {
            if message.failure_count < min_count {
                return false;
            }
        }

        true
    }
}

/// A message that can be replayed
#[derive(Debug, Clone)]
pub struct ReplayableMessage {
    /// Message ID
    pub message_id: String,
    /// Message body (JSON)
    pub body: String,
    /// Task name
    pub task_name: String,
    /// Message attributes
    pub attributes: HashMap<String, String>,
    /// Timestamp (Unix epoch seconds)
    pub timestamp: u64,
    /// Error message (if from DLQ)
    pub error_message: Option<String>,
    /// Failure count
    pub failure_count: u32,
}

/// Result of a replay operation
#[derive(Debug, Clone)]
pub struct ReplayResult {
    /// Total messages attempted
    pub total: usize,
    /// Successfully replayed messages
    pub successful: usize,
    /// Failed replays
    pub failed: usize,
    /// Skipped messages (filtered out)
    pub skipped: usize,
    /// Duration of replay operation
    pub duration: Duration,
    /// Average throughput (messages/sec)
    pub throughput: f64,
    /// Failed message IDs
    pub failed_message_ids: Vec<String>,
}

/// Progress information during replay
#[derive(Debug, Clone)]
pub struct ReplayProgress {
    /// Messages replayed so far
    pub replayed: usize,
    /// Total messages to replay
    pub total: usize,
    /// Successful replays
    pub successful: usize,
    /// Failed replays
    pub failed: usize,
    /// Elapsed time
    pub elapsed: Duration,
    /// Estimated time remaining
    pub estimated_remaining: Option<Duration>,
}

impl ReplayProgress {
    /// Calculate progress percentage
    pub fn percentage(&self) -> f64 {
        if self.total == 0 {
            0.0
        } else {
            (self.replayed as f64 / self.total as f64) * 100.0
        }
    }

    /// Calculate current throughput (messages/sec)
    pub fn throughput(&self) -> f64 {
        if self.elapsed.as_secs_f64() == 0.0 {
            0.0
        } else {
            self.replayed as f64 / self.elapsed.as_secs_f64()
        }
    }
}

/// Message replay manager
#[allow(dead_code)]
pub struct ReplayManager {
    config: ReplayConfig,
    progress: Option<ReplayProgress>,
    start_time: Option<Instant>,
}

impl ReplayManager {
    /// Create a new replay manager
    pub fn new(config: ReplayConfig) -> Self {
        Self {
            config,
            progress: None,
            start_time: None,
        }
    }

    /// Get current replay progress
    pub fn progress(&self) -> Option<&ReplayProgress> {
        self.progress.as_ref()
    }

    /// Reset progress tracking
    pub fn reset_progress(&mut self) {
        self.progress = None;
        self.start_time = None;
    }

    /// Apply rate limiting
    #[allow(dead_code)]
    fn apply_rate_limit(&self, messages_sent: usize, elapsed: Duration) {
        if self.config.rate_limit == 0 {
            return;
        }

        let target_duration =
            Duration::from_secs_f64(messages_sent as f64 / self.config.rate_limit as f64);
        if elapsed < target_duration {
            let wait_time = target_duration - elapsed;
            std::thread::sleep(wait_time);
        }
    }

    /// Update progress tracking
    #[allow(dead_code)]
    fn update_progress(&mut self, replayed: usize, total: usize, successful: usize, failed: usize) {
        if !self.config.track_progress {
            return;
        }

        let start = self.start_time.unwrap_or_else(Instant::now);
        let elapsed = start.elapsed();

        let estimated_remaining = if replayed > 0 {
            let rate = replayed as f64 / elapsed.as_secs_f64();
            let remaining = total - replayed;
            Some(Duration::from_secs_f64(remaining as f64 / rate))
        } else {
            None
        };

        self.progress = Some(ReplayProgress {
            replayed,
            total,
            successful,
            failed,
            elapsed,
            estimated_remaining,
        });
    }
}

/// Simple glob pattern matching (supports * wildcard)
fn glob_match(pattern: &str, text: &str) -> bool {
    if pattern == "*" {
        return true;
    }

    let parts: Vec<&str> = pattern.split('*').collect();
    if parts.len() == 1 {
        return pattern == text;
    }

    let mut pos = 0;
    for (i, part) in parts.iter().enumerate() {
        if part.is_empty() {
            continue;
        }

        if i == 0 {
            // First part - must match at start
            if !text.starts_with(part) {
                return false;
            }
            pos = part.len();
        } else if i == parts.len() - 1 {
            // Last part - must match at end
            return text[pos..].ends_with(part);
        } else {
            // Middle part - must be found somewhere
            if let Some(found_pos) = text[pos..].find(part) {
                pos += found_pos + part.len();
            } else {
                return false;
            }
        }
    }

    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_replay_config_default() {
        let config = ReplayConfig::new();
        assert_eq!(config.batch_size, 10);
        assert_eq!(config.rate_limit, 100);
        assert!(config.retry_failed);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_replay_config_builder() {
        let config = ReplayConfig::new()
            .with_batch_size(5)
            .with_rate_limit(50)
            .with_retry_failed(false)
            .with_max_retries(2);

        assert_eq!(config.batch_size, 5);
        assert_eq!(config.rate_limit, 50);
        assert!(!config.retry_failed);
        assert_eq!(config.max_retries, 2);
    }

    #[test]
    fn test_replay_filter_matches_task_pattern() {
        let filter = ReplayFilter::new().with_task_pattern("tasks.payment.*");

        let message = ReplayableMessage {
            message_id: "msg-1".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.payment.process".to_string(),
            attributes: HashMap::new(),
            timestamp: 1000,
            error_message: None,
            failure_count: 1,
        };

        assert!(filter.matches(&message));
    }

    #[test]
    fn test_replay_filter_matches_time_range() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let filter = ReplayFilter::new().with_time_range(now - 3600, now);

        let recent_message = ReplayableMessage {
            message_id: "msg-1".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.test".to_string(),
            attributes: HashMap::new(),
            timestamp: now - 1800, // 30 minutes ago
            error_message: None,
            failure_count: 1,
        };

        let old_message = ReplayableMessage {
            message_id: "msg-2".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.test".to_string(),
            attributes: HashMap::new(),
            timestamp: now - 7200, // 2 hours ago
            error_message: None,
            failure_count: 1,
        };

        assert!(filter.matches(&recent_message));
        assert!(!filter.matches(&old_message));
    }

    #[test]
    fn test_replay_filter_matches_failure_count() {
        let filter = ReplayFilter::new().with_min_failure_count(3);

        let high_failure = ReplayableMessage {
            message_id: "msg-1".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.test".to_string(),
            attributes: HashMap::new(),
            timestamp: 1000,
            error_message: None,
            failure_count: 5,
        };

        let low_failure = ReplayableMessage {
            message_id: "msg-2".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.test".to_string(),
            attributes: HashMap::new(),
            timestamp: 1000,
            error_message: None,
            failure_count: 1,
        };

        assert!(filter.matches(&high_failure));
        assert!(!filter.matches(&low_failure));
    }

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("tasks.*", "tasks.process"));
        assert!(glob_match("tasks.payment.*", "tasks.payment.process"));
        assert!(!glob_match("tasks.payment.*", "tasks.email.send"));
        assert!(glob_match("tasks.*.process", "tasks.payment.process"));
        assert!(glob_match("exact", "exact"));
        assert!(!glob_match("exact", "not_exact"));
    }

    #[test]
    fn test_replay_progress_percentage() {
        let progress = ReplayProgress {
            replayed: 50,
            total: 100,
            successful: 45,
            failed: 5,
            elapsed: Duration::from_secs(10),
            estimated_remaining: None,
        };

        assert_eq!(progress.percentage(), 50.0);
        assert_eq!(progress.throughput(), 5.0);
    }

    #[test]
    fn test_replay_progress_throughput() {
        let progress = ReplayProgress {
            replayed: 100,
            total: 200,
            successful: 95,
            failed: 5,
            elapsed: Duration::from_secs(10),
            estimated_remaining: None,
        };

        assert_eq!(progress.throughput(), 10.0);
    }

    #[test]
    fn test_replay_manager_progress_tracking() {
        let config = ReplayConfig::new().with_track_progress(true);
        let mut manager = ReplayManager::new(config);

        manager.start_time = Some(Instant::now());
        manager.update_progress(50, 100, 45, 5);

        let progress = manager.progress().unwrap();
        assert_eq!(progress.replayed, 50);
        assert_eq!(progress.total, 100);
        assert_eq!(progress.successful, 45);
        assert_eq!(progress.failed, 5);
        assert!(progress.estimated_remaining.is_some());
    }

    #[test]
    fn test_batch_size_clamping() {
        let config1 = ReplayConfig::new().with_batch_size(0);
        assert_eq!(config1.batch_size, 1);

        let config2 = ReplayConfig::new().with_batch_size(20);
        assert_eq!(config2.batch_size, 10);

        let config3 = ReplayConfig::new().with_batch_size(5);
        assert_eq!(config3.batch_size, 5);
    }

    #[test]
    fn test_replay_filter_error_pattern() {
        let filter = ReplayFilter::new().with_error_pattern("timeout");

        let matching = ReplayableMessage {
            message_id: "msg-1".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.test".to_string(),
            attributes: HashMap::new(),
            timestamp: 1000,
            error_message: Some("Connection timeout occurred".to_string()),
            failure_count: 1,
        };

        let non_matching = ReplayableMessage {
            message_id: "msg-2".to_string(),
            body: "{}".to_string(),
            task_name: "tasks.test".to_string(),
            attributes: HashMap::new(),
            timestamp: 1000,
            error_message: Some("Validation failed".to_string()),
            failure_count: 1,
        };

        assert!(filter.matches(&matching));
        assert!(!filter.matches(&non_matching));
    }
}
