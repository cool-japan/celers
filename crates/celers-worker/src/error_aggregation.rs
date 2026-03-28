//! Error aggregation and reporting for worker tasks
//!
//! This module provides a centralized error tracking system that aggregates
//! errors from worker tasks and provides reporting capabilities for monitoring
//! and debugging.
//!
//! # Features
//!
//! - Error aggregation by task type and error type
//! - Configurable time windows for error tracking
//! - Error rate calculation
//! - Top errors reporting
//! - Error pattern detection
//! - Automatic error pruning based on age
//!
//! # Example
//!
//! ```
//! use celers_worker::{ErrorAggregator, ErrorEntry, ErrorAggregatorConfig};
//! use std::time::Duration;
//!
//! # async fn example() {
//! let config = ErrorAggregatorConfig::new()
//!     .with_window_size(Duration::from_secs(3600))
//!     .with_max_entries(10000);
//!
//! let mut aggregator = ErrorAggregator::new(config);
//!
//! // Record an error
//! aggregator.record_error(
//!     "process_order".to_string(),
//!     "DatabaseError".to_string(),
//!     "Connection timeout".to_string(),
//! );
//!
//! // Get error statistics
//! let total_errors = aggregator.total_errors();
//! let error_rate = aggregator.error_rate("process_order");
//! # }
//! ```

use std::cmp::Reverse;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Configuration for error aggregator
#[derive(Clone)]
pub struct ErrorAggregatorConfig {
    /// Time window for error tracking
    pub window_size: Duration,
    /// Maximum number of error entries to keep
    pub max_entries: usize,
    /// Enable automatic pruning of old errors
    pub auto_prune: bool,
    /// Prune interval
    pub prune_interval: Duration,
    /// Enable error pattern detection
    pub enable_pattern_detection: bool,
}

impl ErrorAggregatorConfig {
    /// Create a new error aggregator configuration
    pub fn new() -> Self {
        Self {
            window_size: Duration::from_secs(3600), // 1 hour
            max_entries: 10000,
            auto_prune: true,
            prune_interval: Duration::from_secs(300), // 5 minutes
            enable_pattern_detection: true,
        }
    }

    /// Set the window size for error tracking
    pub fn with_window_size(mut self, window_size: Duration) -> Self {
        self.window_size = window_size;
        self
    }

    /// Set the maximum number of error entries
    pub fn with_max_entries(mut self, max_entries: usize) -> Self {
        self.max_entries = max_entries;
        self
    }

    /// Enable or disable automatic pruning
    pub fn with_auto_prune(mut self, auto_prune: bool) -> Self {
        self.auto_prune = auto_prune;
        self
    }

    /// Set the prune interval
    pub fn with_prune_interval(mut self, prune_interval: Duration) -> Self {
        self.prune_interval = prune_interval;
        self
    }

    /// Enable or disable pattern detection
    pub fn with_pattern_detection(mut self, enable: bool) -> Self {
        self.enable_pattern_detection = enable;
        self
    }
}

impl Default for ErrorAggregatorConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// A single error entry
#[derive(Clone, Debug)]
pub struct ErrorEntry {
    /// Task name/type
    pub task_name: String,
    /// Error type/category
    pub error_type: String,
    /// Error message
    pub message: String,
    /// When the error occurred
    pub timestamp: SystemTime,
    /// Task ID if available
    pub task_id: Option<String>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl ErrorEntry {
    /// Create a new error entry
    pub fn new(task_name: String, error_type: String, message: String) -> Self {
        Self {
            task_name,
            error_type,
            message,
            timestamp: SystemTime::now(),
            task_id: None,
            metadata: HashMap::new(),
        }
    }

    /// Set the task ID
    pub fn with_task_id(mut self, task_id: String) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Check if the error is within a time window
    pub fn is_within_window(&self, window: Duration) -> bool {
        if let Ok(age) = SystemTime::now().duration_since(self.timestamp) {
            age <= window
        } else {
            false
        }
    }
}

/// Error statistics for a specific task or error type
#[derive(Clone, Debug)]
pub struct ErrorStats {
    /// Total count
    pub count: usize,
    /// First occurrence
    pub first_seen: SystemTime,
    /// Last occurrence
    pub last_seen: SystemTime,
    /// Error rate (errors per second)
    pub rate: f64,
}

impl ErrorStats {
    /// Create new error statistics
    pub fn new() -> Self {
        Self {
            count: 0,
            first_seen: SystemTime::now(),
            last_seen: SystemTime::now(),
            rate: 0.0,
        }
    }

    /// Update statistics with a new error
    pub fn update(&mut self) {
        self.count += 1;
        self.last_seen = SystemTime::now();
    }

    /// Calculate error rate over a time window
    pub fn calculate_rate(&mut self, _window: Duration) {
        if let Ok(duration) = self.last_seen.duration_since(self.first_seen) {
            let seconds = duration.as_secs_f64().max(1.0);
            self.rate = self.count as f64 / seconds;
        }
    }
}

impl Default for ErrorStats {
    fn default() -> Self {
        Self::new()
    }
}

/// Error pattern detected in the system
#[derive(Clone, Debug)]
pub struct ErrorPattern {
    /// Pattern description
    pub description: String,
    /// Affected task types
    pub affected_tasks: Vec<String>,
    /// Error types involved
    pub error_types: Vec<String>,
    /// Frequency of the pattern
    pub frequency: usize,
    /// When the pattern was first detected
    pub detected_at: SystemTime,
}

/// Error aggregator that tracks and reports errors
pub struct ErrorAggregator {
    /// Configuration
    config: ErrorAggregatorConfig,
    /// Error entries
    entries: Arc<RwLock<Vec<ErrorEntry>>>,
    /// Error statistics by task name
    task_stats: Arc<RwLock<HashMap<String, ErrorStats>>>,
    /// Error statistics by error type
    error_type_stats: Arc<RwLock<HashMap<String, ErrorStats>>>,
    /// Detected patterns
    patterns: Arc<RwLock<Vec<ErrorPattern>>>,
    /// Last prune time
    last_prune: Arc<RwLock<Instant>>,
}

impl ErrorAggregator {
    /// Create a new error aggregator
    pub fn new(config: ErrorAggregatorConfig) -> Self {
        Self {
            config,
            entries: Arc::new(RwLock::new(Vec::new())),
            task_stats: Arc::new(RwLock::new(HashMap::new())),
            error_type_stats: Arc::new(RwLock::new(HashMap::new())),
            patterns: Arc::new(RwLock::new(Vec::new())),
            last_prune: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Record a new error
    pub async fn record_error(&self, task_name: String, error_type: String, message: String) {
        let entry = ErrorEntry::new(task_name.clone(), error_type.clone(), message);

        // Add entry
        let mut entries = self.entries.write().await;
        entries.push(entry.clone());
        drop(entries);

        // Update task statistics
        let mut task_stats = self.task_stats.write().await;
        task_stats
            .entry(task_name.clone())
            .or_insert_with(ErrorStats::new)
            .update();
        drop(task_stats);

        // Update error type statistics
        let mut error_type_stats = self.error_type_stats.write().await;
        error_type_stats
            .entry(error_type.clone())
            .or_insert_with(ErrorStats::new)
            .update();
        drop(error_type_stats);

        debug!("Recorded error: {} - {}", task_name, error_type);

        // Detect patterns if enabled
        if self.config.enable_pattern_detection {
            self.detect_patterns().await;
        }

        // Auto-prune if enabled
        if self.config.auto_prune {
            self.maybe_prune().await;
        }

        // Check if we've exceeded max entries
        let entries = self.entries.read().await;
        if entries.len() > self.config.max_entries {
            drop(entries);
            warn!("Error entries exceeded max size, forcing prune");
            self.prune_old_errors().await;
        }
    }

    /// Record an error with full details
    pub async fn record_error_full(&self, entry: ErrorEntry) {
        let task_name = entry.task_name.clone();
        let error_type = entry.error_type.clone();

        // Add entry
        let mut entries = self.entries.write().await;
        entries.push(entry);
        drop(entries);

        // Update statistics
        let mut task_stats = self.task_stats.write().await;
        task_stats
            .entry(task_name)
            .or_insert_with(ErrorStats::new)
            .update();
        drop(task_stats);

        let mut error_type_stats = self.error_type_stats.write().await;
        error_type_stats
            .entry(error_type)
            .or_insert_with(ErrorStats::new)
            .update();
    }

    /// Get total number of errors
    pub async fn total_errors(&self) -> usize {
        self.entries.read().await.len()
    }

    /// Get total errors within the time window
    pub async fn total_errors_in_window(&self) -> usize {
        let entries = self.entries.read().await;
        entries
            .iter()
            .filter(|e| e.is_within_window(self.config.window_size))
            .count()
    }

    /// Get error rate for a specific task (errors per second)
    pub async fn error_rate(&self, task_name: &str) -> f64 {
        let task_stats = self.task_stats.read().await;
        if let Some(stats) = task_stats.get(task_name) {
            stats.rate
        } else {
            0.0
        }
    }

    /// Get errors for a specific task
    pub async fn errors_by_task(&self, task_name: &str) -> Vec<ErrorEntry> {
        let entries = self.entries.read().await;
        entries
            .iter()
            .filter(|e| e.task_name == task_name && e.is_within_window(self.config.window_size))
            .cloned()
            .collect()
    }

    /// Get errors of a specific type
    pub async fn errors_by_type(&self, error_type: &str) -> Vec<ErrorEntry> {
        let entries = self.entries.read().await;
        entries
            .iter()
            .filter(|e| e.error_type == error_type && e.is_within_window(self.config.window_size))
            .cloned()
            .collect()
    }

    /// Get top N most frequent errors
    pub async fn top_errors(&self, n: usize) -> Vec<(String, usize)> {
        let error_type_stats = self.error_type_stats.read().await;
        let mut error_counts: Vec<(String, usize)> = error_type_stats
            .iter()
            .map(|(error_type, stats)| (error_type.clone(), stats.count))
            .collect();

        error_counts.sort_by_key(|item| Reverse(item.1));
        error_counts.into_iter().take(n).collect()
    }

    /// Get top N tasks with most errors
    pub async fn top_error_tasks(&self, n: usize) -> Vec<(String, usize)> {
        let task_stats = self.task_stats.read().await;
        let mut task_counts: Vec<(String, usize)> = task_stats
            .iter()
            .map(|(task, stats)| (task.clone(), stats.count))
            .collect();

        task_counts.sort_by_key(|item| Reverse(item.1));
        task_counts.into_iter().take(n).collect()
    }

    /// Get all detected error patterns
    pub async fn get_patterns(&self) -> Vec<ErrorPattern> {
        self.patterns.read().await.clone()
    }

    /// Clear all error data
    pub async fn clear(&self) {
        self.entries.write().await.clear();
        self.task_stats.write().await.clear();
        self.error_type_stats.write().await.clear();
        self.patterns.write().await.clear();
        info!("Cleared all error data");
    }

    /// Prune errors older than the configured window
    pub async fn prune_old_errors(&self) {
        let mut entries = self.entries.write().await;
        let original_len = entries.len();
        entries.retain(|e| e.is_within_window(self.config.window_size));
        let pruned = original_len - entries.len();

        if pruned > 0 {
            info!("Pruned {} old error entries", pruned);
        }

        *self.last_prune.write().await = Instant::now();
    }

    /// Maybe prune errors if the interval has elapsed
    async fn maybe_prune(&self) {
        let last_prune = *self.last_prune.read().await;
        if last_prune.elapsed() >= self.config.prune_interval {
            self.prune_old_errors().await;
        }
    }

    /// Detect error patterns
    async fn detect_patterns(&self) {
        // Simple pattern detection: look for task types that have multiple error types
        let entries = self.entries.read().await;
        let mut task_error_map: HashMap<String, Vec<String>> = HashMap::new();

        for entry in entries.iter() {
            if entry.is_within_window(self.config.window_size) {
                task_error_map
                    .entry(entry.task_name.clone())
                    .or_default()
                    .push(entry.error_type.clone());
            }
        }

        let mut patterns = self.patterns.write().await;

        for (task_name, error_types) in task_error_map.iter() {
            // Detect if a task has multiple different error types
            let unique_errors: Vec<String> = error_types
                .iter()
                .cloned()
                .collect::<std::collections::HashSet<_>>()
                .into_iter()
                .collect();

            if unique_errors.len() >= 3 {
                // Check if we already have this pattern
                if !patterns
                    .iter()
                    .any(|p| p.affected_tasks.contains(task_name))
                {
                    patterns.push(ErrorPattern {
                        description: format!(
                            "Task '{}' experiencing multiple error types",
                            task_name
                        ),
                        affected_tasks: vec![task_name.clone()],
                        error_types: unique_errors.clone(),
                        frequency: error_types.len(),
                        detected_at: SystemTime::now(),
                    });
                    info!("Detected error pattern for task: {}", task_name);
                }
            }
        }
    }

    /// Generate a summary report
    pub async fn generate_report(&self) -> String {
        let total = self.total_errors().await;
        let in_window = self.total_errors_in_window().await;
        let top_errors = self.top_errors(5).await;
        let top_tasks = self.top_error_tasks(5).await;
        let patterns = self.get_patterns().await;

        let mut report = String::new();
        report.push_str("=== Error Aggregation Report ===\n");
        report.push_str(&format!("Total Errors: {}\n", total));
        report.push_str(&format!("Errors in Window: {}\n", in_window));
        report.push_str("\nTop 5 Error Types:\n");

        for (i, (error_type, count)) in top_errors.iter().enumerate() {
            report.push_str(&format!(
                "  {}. {} ({} occurrences)\n",
                i + 1,
                error_type,
                count
            ));
        }

        report.push_str("\nTop 5 Tasks with Errors:\n");
        for (i, (task, count)) in top_tasks.iter().enumerate() {
            report.push_str(&format!("  {}. {} ({} errors)\n", i + 1, task, count));
        }

        if !patterns.is_empty() {
            report.push_str(&format!("\nDetected Patterns ({}):\n", patterns.len()));
            for (i, pattern) in patterns.iter().enumerate() {
                report.push_str(&format!("  {}. {}\n", i + 1, pattern.description));
            }
        }

        report
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_error_aggregator_record() {
        let config = ErrorAggregatorConfig::default();
        let aggregator = ErrorAggregator::new(config);

        aggregator
            .record_error(
                "test_task".to_string(),
                "TestError".to_string(),
                "Test error message".to_string(),
            )
            .await;

        assert_eq!(aggregator.total_errors().await, 1);
    }

    #[tokio::test]
    async fn test_error_aggregator_stats() {
        let config = ErrorAggregatorConfig::default();
        let aggregator = ErrorAggregator::new(config);

        aggregator
            .record_error(
                "task1".to_string(),
                "Error1".to_string(),
                "Message 1".to_string(),
            )
            .await;

        aggregator
            .record_error(
                "task1".to_string(),
                "Error2".to_string(),
                "Message 2".to_string(),
            )
            .await;

        let errors = aggregator.errors_by_task("task1").await;
        assert_eq!(errors.len(), 2);
    }

    #[tokio::test]
    async fn test_error_aggregator_top_errors() {
        let config = ErrorAggregatorConfig::default();
        let aggregator = ErrorAggregator::new(config);

        for i in 0..5 {
            aggregator
                .record_error(
                    "task".to_string(),
                    format!("Error{}", i),
                    "Message".to_string(),
                )
                .await;
        }

        // Add more of Error0
        for _ in 0..3 {
            aggregator
                .record_error(
                    "task".to_string(),
                    "Error0".to_string(),
                    "Message".to_string(),
                )
                .await;
        }

        let top = aggregator.top_errors(3).await;
        assert!(top.len() <= 3);
        assert_eq!(top[0].0, "Error0"); // Most frequent
        assert_eq!(top[0].1, 4); // 4 occurrences
    }

    #[tokio::test]
    async fn test_error_aggregator_clear() {
        let config = ErrorAggregatorConfig::default();
        let aggregator = ErrorAggregator::new(config);

        aggregator
            .record_error(
                "task".to_string(),
                "Error".to_string(),
                "Message".to_string(),
            )
            .await;

        assert_eq!(aggregator.total_errors().await, 1);

        aggregator.clear().await;
        assert_eq!(aggregator.total_errors().await, 0);
    }

    #[tokio::test]
    async fn test_error_entry_with_metadata() {
        let entry = ErrorEntry::new(
            "task".to_string(),
            "Error".to_string(),
            "Message".to_string(),
        )
        .with_task_id("task-123".to_string())
        .with_metadata("retry_count", "3");

        assert_eq!(entry.task_id, Some("task-123".to_string()));
        assert_eq!(entry.metadata.get("retry_count"), Some(&"3".to_string()));
    }

    #[tokio::test]
    async fn test_error_aggregator_report() {
        let config = ErrorAggregatorConfig::default();
        let aggregator = ErrorAggregator::new(config);

        aggregator
            .record_error(
                "task1".to_string(),
                "Error1".to_string(),
                "Message 1".to_string(),
            )
            .await;

        let report = aggregator.generate_report().await;
        assert!(report.contains("Total Errors: 1"));
        assert!(report.contains("Error1"));
    }

    #[tokio::test]
    async fn test_error_aggregator_prune() {
        let config = ErrorAggregatorConfig::new().with_window_size(Duration::from_millis(100));
        let aggregator = ErrorAggregator::new(config);

        aggregator
            .record_error(
                "task".to_string(),
                "Error".to_string(),
                "Message".to_string(),
            )
            .await;

        assert_eq!(aggregator.total_errors().await, 1);

        // Wait for errors to age out
        tokio::time::sleep(Duration::from_millis(150)).await;

        aggregator.prune_old_errors().await;
        assert_eq!(aggregator.total_errors().await, 0);
    }

    #[tokio::test]
    async fn test_error_stats() {
        let mut stats = ErrorStats::new();
        assert_eq!(stats.count, 0);

        stats.update();
        assert_eq!(stats.count, 1);

        stats.update();
        assert_eq!(stats.count, 2);
    }

    #[tokio::test]
    async fn test_error_aggregator_by_type() {
        let config = ErrorAggregatorConfig::default();
        let aggregator = ErrorAggregator::new(config);

        aggregator
            .record_error(
                "task1".to_string(),
                "DatabaseError".to_string(),
                "Message 1".to_string(),
            )
            .await;

        aggregator
            .record_error(
                "task2".to_string(),
                "DatabaseError".to_string(),
                "Message 2".to_string(),
            )
            .await;

        let errors = aggregator.errors_by_type("DatabaseError").await;
        assert_eq!(errors.len(), 2);
    }

    #[tokio::test]
    async fn test_config_builder() {
        let config = ErrorAggregatorConfig::new()
            .with_window_size(Duration::from_secs(7200))
            .with_max_entries(5000)
            .with_auto_prune(false)
            .with_pattern_detection(false);

        assert_eq!(config.window_size, Duration::from_secs(7200));
        assert_eq!(config.max_entries, 5000);
        assert!(!config.auto_prune);
        assert!(!config.enable_pattern_detection);
    }
}
