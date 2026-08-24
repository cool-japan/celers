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
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Bucket key used once a stats map ([`ErrorAggregator`]'s `task_stats`
/// or `error_type_stats`) has reached its configured cardinality cap and
/// a *new* key would otherwise be added.
const OVERFLOW_STATS_KEY: &str = "__other__";

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
    /// Maximum number of *distinct* keys tracked in the per-task and
    /// per-error-type statistics maps. Once reached, a previously-unseen
    /// task name or error type is folded into a shared overflow bucket
    /// instead of adding a new entry, so cardinality derived from
    /// unbounded input (e.g. an error message used as the "type") cannot
    /// grow the maps without bound even though `entries` itself is
    /// capped by `max_entries`.
    pub max_distinct_stat_keys: usize,
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
            max_distinct_stat_keys: 1000,
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

    /// Set the maximum number of distinct keys tracked per statistics map
    pub fn with_max_distinct_stat_keys(mut self, max: usize) -> Self {
        self.max_distinct_stat_keys = max;
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
    /// Error entries. A `VecDeque` so the `max_entries` cap can be
    /// enforced structurally as a true ring buffer (`pop_front` on
    /// overflow) independent of the time window -- see [`Self::insert_entry`].
    entries: Arc<RwLock<VecDeque<ErrorEntry>>>,
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
            entries: Arc::new(RwLock::new(VecDeque::new())),
            task_stats: Arc::new(RwLock::new(HashMap::new())),
            error_type_stats: Arc::new(RwLock::new(HashMap::new())),
            patterns: Arc::new(RwLock::new(Vec::new())),
            last_prune: Arc::new(RwLock::new(Instant::now())),
        }
    }

    /// Record a new error
    pub async fn record_error(&self, task_name: String, error_type: String, message: String) {
        let entry = ErrorEntry::new(task_name, error_type, message);
        self.insert_entry(entry).await;
    }

    /// Record an error with full details
    pub async fn record_error_full(&self, entry: ErrorEntry) {
        self.insert_entry(entry).await;
    }

    /// Insert `entry` and perform every associated bookkeeping step
    /// (statistics, pattern detection, capacity enforcement).
    /// [`record_error`](Self::record_error) and
    /// [`record_error_full`](Self::record_error_full) both route through
    /// this single path so neither can bypass the `max_entries` cap --
    /// previously `record_error_full` pushed directly onto `entries` and
    /// never pruned at all.
    async fn insert_entry(&self, entry: ErrorEntry) {
        let task_name = entry.task_name.clone();
        let error_type = entry.error_type.clone();

        {
            let mut entries = self.entries.write().await;
            entries.push_back(entry);
            // Hard cap enforced structurally as a ring buffer, independent
            // of the time window: a sustained error rate that produces
            // more than `max_entries` errors within a single `window_size`
            // used to leave the "forcing prune" branch below with nothing
            // to remove (it pruned only by age), so this cannot be
            // skipped by tuning the window.
            if entries.len() > self.config.max_entries {
                warn!("Error entries exceeded max size, forcing prune");
                while entries.len() > self.config.max_entries {
                    entries.pop_front();
                }
            }
        }

        // Update statistics (bounded: see `bump_stats_map`).
        {
            let mut task_stats = self.task_stats.write().await;
            Self::bump_stats_map(
                &mut task_stats,
                &task_name,
                self.config.max_distinct_stat_keys,
            );
        }
        {
            let mut error_type_stats = self.error_type_stats.write().await;
            Self::bump_stats_map(
                &mut error_type_stats,
                &error_type,
                self.config.max_distinct_stat_keys,
            );
        }

        debug!("Recorded error: {} - {}", task_name, error_type);

        // Detect patterns if enabled
        if self.config.enable_pattern_detection {
            self.detect_patterns().await;
        }

        // Auto-prune if enabled (age-based; the hard cap above already
        // guarantees an upper bound regardless of this setting).
        if self.config.auto_prune {
            self.maybe_prune().await;
        }
    }

    /// Insert-or-update `key`'s entry in a stats map, bucketing into a
    /// shared overflow key once the map already holds
    /// `max_distinct_keys` *different* keys and `key` is not one of
    /// them. Without this, per-task/per-error-type cardinality derived
    /// from unbounded input (e.g. an error message folded into the
    /// "type") would let the stats maps grow forever even with `entries`
    /// itself capped.
    fn bump_stats_map(map: &mut HashMap<String, ErrorStats>, key: &str, max_distinct_keys: usize) {
        let target_key = if map.contains_key(key) || map.len() < max_distinct_keys {
            key
        } else {
            OVERFLOW_STATS_KEY
        };
        map.entry(target_key.to_string()).or_default().update();
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

    // --- Regression tests (idx 183) ---------------------------------------

    /// Regression test: a sustained burst of errors that all land within
    /// one `window_size` used to make `prune_old_errors` (age-based) a
    /// no-op, so `max_entries` was not a real cap. The cap must now hold
    /// via `entries` behaving as a structural ring buffer.
    #[tokio::test]
    async fn test_max_entries_is_a_real_cap_under_sustained_burst() {
        let config = ErrorAggregatorConfig::new()
            .with_max_entries(5)
            // A long window means "prune by age" would remove nothing --
            // every entry recorded in this test is well within it.
            .with_window_size(Duration::from_secs(3600))
            .with_pattern_detection(false);
        let aggregator = ErrorAggregator::new(config);

        for i in 0..50 {
            aggregator
                .record_error("task".to_string(), format!("Error{i}"), "msg".to_string())
                .await;
        }

        assert_eq!(
            aggregator.total_errors().await,
            5,
            "entries must never exceed max_entries, regardless of how many arrive within the window"
        );
    }

    /// The cap must hold immediately (synchronously, on every insert),
    /// not just "eventually" once some background sweep runs.
    #[tokio::test]
    async fn test_max_entries_cap_holds_after_every_single_insert() {
        let config = ErrorAggregatorConfig::new()
            .with_max_entries(3)
            .with_window_size(Duration::from_secs(3600))
            .with_pattern_detection(false);
        let aggregator = ErrorAggregator::new(config);

        for i in 0..10 {
            aggregator
                .record_error("task".to_string(), format!("Error{i}"), "msg".to_string())
                .await;
            assert!(aggregator.total_errors().await <= 3);
        }
    }

    /// Regression test: `record_error_full` used to push directly onto
    /// `entries` and never prune at all -- unconditionally unbounded.
    #[tokio::test]
    async fn test_record_error_full_is_capped_too() {
        let config = ErrorAggregatorConfig::new()
            .with_max_entries(4)
            .with_window_size(Duration::from_secs(3600))
            .with_pattern_detection(false);
        let aggregator = ErrorAggregator::new(config);

        for i in 0..25 {
            let entry = ErrorEntry::new("task".to_string(), format!("Error{i}"), "msg".to_string());
            aggregator.record_error_full(entry).await;
        }

        assert_eq!(aggregator.total_errors().await, 4);
    }

    /// The ring buffer must evict the *oldest* entries first, keeping the
    /// most recent ones -- a FIFO cap, not an arbitrary truncation.
    #[tokio::test]
    async fn test_max_entries_cap_keeps_most_recent_entries() {
        let config = ErrorAggregatorConfig::new()
            .with_max_entries(3)
            .with_window_size(Duration::from_secs(3600))
            .with_pattern_detection(false);
        let aggregator = ErrorAggregator::new(config);

        for i in 0..10 {
            aggregator
                .record_error("task".to_string(), "Error".to_string(), format!("msg{i}"))
                .await;
        }

        let remaining = aggregator.errors_by_task("task").await;
        let messages: Vec<&str> = remaining.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(messages, vec!["msg7", "msg8", "msg9"]);
    }

    /// Regression test: `task_stats`/`error_type_stats` are keyed by
    /// caller-supplied strings and were never bounded, so a high-
    /// cardinality source (e.g. distinct task names, or an error message
    /// folded into "error_type") could grow them without bound even
    /// though `entries` is capped. New keys beyond the configured
    /// cardinality must fold into a shared overflow bucket instead.
    #[tokio::test]
    async fn test_stats_maps_are_bounded_with_overflow_bucket() {
        let config = ErrorAggregatorConfig::new()
            .with_max_entries(1000)
            .with_max_distinct_stat_keys(2)
            .with_pattern_detection(false);
        let aggregator = ErrorAggregator::new(config);

        for task in ["A", "B", "C", "D", "E"] {
            aggregator
                .record_error(task.to_string(), "Error".to_string(), "msg".to_string())
                .await;
        }

        // All 5 entries are still recorded individually...
        assert_eq!(aggregator.total_errors().await, 5);

        // ...but the per-task stats map holds at most
        // `max_distinct_stat_keys + 1` keys (the real ones, plus the
        // shared overflow bucket), not one key per distinct task name.
        let top_tasks = aggregator.top_error_tasks(100).await;
        assert_eq!(
            top_tasks.len(),
            3,
            "expected 2 real keys + 1 overflow bucket, got {top_tasks:?}"
        );

        // "A" and "B" arrived first and get real slots (count 1 each);
        // "C", "D", "E" all fold into the shared overflow bucket (count 3),
        // which sorts first since `top_error_tasks` orders by count desc.
        assert_eq!(
            top_tasks[0].1, 3,
            "overflow bucket should have absorbed 3 tasks"
        );
    }
}
