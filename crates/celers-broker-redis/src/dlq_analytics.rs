//! DLQ Analytics and Insights
//!
//! Provides advanced analytics for Dead Letter Queue (DLQ) tasks:
//! - Failure pattern detection
//! - Common error clustering
//! - Root cause analysis helpers
//! - Temporal failure analysis
//! - Error categorization
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::dlq_analytics::{DLQAnalyzer, FailurePattern};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let analyzer = DLQAnalyzer::new("redis://localhost:6379", "my_queue").await?;
//!
//! // Analyze failure patterns
//! let patterns = analyzer.detect_failure_patterns(100).await?;
//! for pattern in patterns {
//!     println!("Pattern: {} ({}% of failures)", pattern.error_type, pattern.percentage);
//!     println!("  Recommendation: {}", pattern.recommendation);
//! }
//!
//! // Cluster similar errors
//! let clusters = analyzer.cluster_errors(50).await?;
//! println!("Found {} error clusters", clusters.len());
//!
//! // Get root cause suggestions
//! let root_causes = analyzer.suggest_root_causes(20).await?;
//! for cause in root_causes {
//!     println!("Potential cause: {}", cause.description);
//! }
//! # Ok(())
//! # }
//! ```

use celers_core::{CelersError, Result, SerializedTask, TaskState};
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::debug;

/// Extract the failure reason recorded on a task, if present.
///
/// Tasks that reach the DLQ carry their failure information in their state.
/// `TaskState::Failed(message)` holds the actual error message produced by the
/// worker (the "task result"), which is what analytics should classify on.
fn failure_reason(task: &SerializedTask) -> Option<String> {
    match &task.metadata.state {
        TaskState::Failed(message) => Some(message.clone()),
        TaskState::Retrying(attempts) => {
            Some(format!("retry attempts exhausted after {attempts} tries"))
        }
        TaskState::Rejected => Some("task rejected".to_string()),
        TaskState::Revoked => Some("task revoked".to_string()),
        TaskState::Custom {
            name,
            metadata: Some(meta),
        } => Some(format!("{name}: {}", String::from_utf8_lossy(meta))),
        TaskState::Custom {
            name,
            metadata: None,
        } => Some(name.clone()),
        _ => None,
    }
}

/// Normalize an error message into a stable signature for clustering.
///
/// Variable substrings (numbers, hex IDs, quoted literals, file paths) are
/// replaced with placeholders so that, e.g.,
/// `Connection refused (os error 111)` and `Connection refused (os error 104)`
/// collapse to the same signature. The result is lowercased and whitespace is
/// collapsed.
fn normalize_error_signature(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    let mut chars = message.chars().peekable();
    let mut last_was_space = false;

    while let Some(c) = chars.next() {
        if c.is_ascii_digit() {
            // Collapse any run of digits into a single placeholder.
            while matches!(chars.peek(), Some(d) if d.is_ascii_digit()) {
                chars.next();
            }
            out.push('#');
            last_was_space = false;
        } else if c == '\'' || c == '"' {
            // Drop quoted literals (typically variable identifiers/values).
            while let Some(&n) = chars.peek() {
                chars.next();
                if n == c {
                    break;
                }
            }
            out.push('?');
            last_was_space = false;
        } else if c.is_whitespace() {
            if !last_was_space {
                out.push(' ');
                last_was_space = true;
            }
        } else {
            out.extend(c.to_lowercase());
            last_was_space = false;
        }
    }

    out.trim().to_string()
}

/// DLQ failure pattern detection and analysis
#[derive(Debug, Clone)]
pub struct DLQAnalyzer {
    client: Client,
    #[allow(dead_code)]
    queue_name: String,
    dlq_key: String,
}

/// A detected failure pattern in the DLQ
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailurePattern {
    /// Error type or category
    pub error_type: String,
    /// Number of occurrences
    pub count: usize,
    /// Percentage of total failures
    pub percentage: f64,
    /// Example task IDs exhibiting this pattern
    pub example_task_ids: Vec<String>,
    /// Recommended action
    pub recommendation: String,
    /// Severity level (1-5, where 5 is most severe)
    pub severity: u8,
}

/// Error cluster representing similar failures
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorCluster {
    /// Cluster ID
    pub id: String,
    /// Representative error message
    pub representative_error: String,
    /// Number of tasks in this cluster
    pub task_count: usize,
    /// Task names in this cluster
    pub task_names: Vec<String>,
    /// Common attributes
    pub common_attributes: HashMap<String, String>,
}

/// Root cause suggestion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RootCause {
    /// Description of the potential root cause
    pub description: String,
    /// Confidence level (0.0-1.0)
    pub confidence: f64,
    /// Affected task count
    pub affected_tasks: usize,
    /// Suggested fix
    pub suggested_fix: String,
    /// Evidence supporting this root cause
    pub evidence: Vec<String>,
}

/// Temporal failure analysis
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalAnalysis {
    /// Time period analyzed
    pub period_seconds: u64,
    /// Failure rate over time (timestamp, count)
    pub failure_timeline: Vec<(i64, usize)>,
    /// Peak failure time
    pub peak_time: Option<i64>,
    /// Average failures per hour
    pub avg_failures_per_hour: f64,
    /// Trend (increasing, decreasing, stable)
    pub trend: FailureTrend,
}

/// Failure trend classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum FailureTrend {
    /// Failure rate increasing
    Increasing,
    /// Failure rate decreasing
    Decreasing,
    /// Failure rate stable
    Stable,
    /// Insufficient data
    Unknown,
}

/// Error category for classification
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ErrorCategory {
    /// Network-related errors
    Network,
    /// Timeout errors
    Timeout,
    /// Validation errors
    Validation,
    /// Resource exhaustion
    ResourceExhaustion,
    /// External service errors
    ExternalService,
    /// Data corruption
    DataCorruption,
    /// Unknown/unclassified
    Unknown,
}

impl ErrorCategory {
    /// Get recommended action for this error category
    pub fn recommendation(&self) -> &'static str {
        match self {
            ErrorCategory::Network => "Check network connectivity and DNS resolution",
            ErrorCategory::Timeout => "Increase timeout values or optimize task execution",
            ErrorCategory::Validation => "Review input validation and data schema",
            ErrorCategory::ResourceExhaustion => "Scale resources or implement rate limiting",
            ErrorCategory::ExternalService => "Check external service health and retry policies",
            ErrorCategory::DataCorruption => "Investigate data integrity and checksums",
            ErrorCategory::Unknown => "Review task logs for more details",
        }
    }

    /// Get severity level for this category
    pub fn severity(&self) -> u8 {
        match self {
            ErrorCategory::DataCorruption => 5,
            ErrorCategory::ResourceExhaustion => 4,
            ErrorCategory::ExternalService => 3,
            ErrorCategory::Timeout => 3,
            ErrorCategory::Network => 2,
            ErrorCategory::Validation => 2,
            ErrorCategory::Unknown => 1,
        }
    }
}

impl DLQAnalyzer {
    /// Create a new DLQ analyzer
    pub async fn new(redis_url: &str, queue_name: &str) -> Result<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to Redis: {}", e)))?;

        Ok(Self {
            client,
            queue_name: queue_name.to_string(),
            dlq_key: format!("{}:dlq", queue_name),
        })
    }

    /// Detect failure patterns in the DLQ
    pub async fn detect_failure_patterns(&self, limit: usize) -> Result<Vec<FailurePattern>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Connection error: {}", e)))?;

        // Get tasks from DLQ
        let tasks: Vec<String> = conn
            .lrange(&self.dlq_key, 0, limit as isize - 1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to read DLQ: {}", e)))?;

        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        // Parse tasks and categorize errors
        let mut error_counts: HashMap<String, (usize, Vec<String>)> = HashMap::new();
        let total_tasks = tasks.len();

        for task_data in tasks {
            if let Ok(task) = serde_json::from_str::<SerializedTask>(&task_data) {
                // Extract error information from task metadata
                let error_type = self.classify_error(&task);
                let entry = error_counts
                    .entry(error_type.clone())
                    .or_insert((0, Vec::new()));
                entry.0 += 1;
                entry.1.push(task.metadata.id.to_string());
            }
        }

        // Convert to failure patterns
        let mut patterns: Vec<FailurePattern> = error_counts
            .into_iter()
            .map(|(error_type, (count, task_ids))| {
                let percentage = (count as f64 / total_tasks as f64) * 100.0;
                let category = self.categorize_error(&error_type);
                FailurePattern {
                    error_type: error_type.clone(),
                    count,
                    percentage,
                    example_task_ids: task_ids.into_iter().take(5).collect(),
                    recommendation: category.recommendation().to_string(),
                    severity: category.severity(),
                }
            })
            .collect();

        // Sort by count descending
        patterns.sort_by_key(|b| std::cmp::Reverse(b.count));

        debug!("Detected {} failure patterns in DLQ", patterns.len());

        Ok(patterns)
    }

    /// Cluster similar errors together
    pub async fn cluster_errors(&self, limit: usize) -> Result<Vec<ErrorCluster>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Connection error: {}", e)))?;

        let tasks: Vec<String> = conn
            .lrange(&self.dlq_key, 0, limit as isize - 1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to read DLQ: {}", e)))?;

        let mut clusters: HashMap<String, ErrorCluster> = HashMap::new();

        for task_data in tasks {
            if let Ok(task) = serde_json::from_str::<SerializedTask>(&task_data) {
                let error_sig = self.extract_error_signature(&task);

                clusters
                    .entry(error_sig.clone())
                    .and_modify(|cluster| {
                        cluster.task_count += 1;
                        if !cluster.task_names.contains(&task.metadata.name) {
                            cluster.task_names.push(task.metadata.name.clone());
                        }
                    })
                    .or_insert_with(|| ErrorCluster {
                        id: error_sig.clone(),
                        representative_error: error_sig,
                        task_count: 1,
                        task_names: vec![task.metadata.name.clone()],
                        common_attributes: HashMap::new(),
                    });
            }
        }

        let mut result: Vec<ErrorCluster> = clusters.into_values().collect();
        result.sort_by_key(|b| std::cmp::Reverse(b.task_count));

        debug!("Clustered errors into {} groups", result.len());

        Ok(result)
    }

    /// Suggest potential root causes
    pub async fn suggest_root_causes(&self, limit: usize) -> Result<Vec<RootCause>> {
        let patterns = self.detect_failure_patterns(limit).await?;
        let mut root_causes = Vec::new();

        for pattern in patterns {
            if pattern.count >= 5 {
                // Only suggest root causes for significant patterns
                let confidence = (pattern.percentage / 100.0).min(0.95);

                root_causes.push(RootCause {
                    description: format!(
                        "{} failures detected ({} occurrences)",
                        pattern.error_type, pattern.count
                    ),
                    confidence,
                    affected_tasks: pattern.count,
                    suggested_fix: pattern.recommendation.clone(),
                    evidence: vec![
                        format!("Affects {}% of failed tasks", pattern.percentage),
                        format!("Severity level: {}/5", pattern.severity),
                    ],
                });
            }
        }

        // Add time-based analysis
        if let Ok(temporal) = self.analyze_temporal_patterns(limit).await {
            if temporal.trend == FailureTrend::Increasing {
                root_causes.push(RootCause {
                    description: "Increasing failure rate detected".to_string(),
                    confidence: 0.8,
                    affected_tasks: limit,
                    suggested_fix: "Investigate recent changes or increased load".to_string(),
                    evidence: vec![
                        format!(
                            "Average {} failures per hour",
                            temporal.avg_failures_per_hour
                        ),
                        "Trend: Increasing".to_string(),
                    ],
                });
            }
        }

        root_causes.sort_by(|a, b| {
            b.confidence
                .partial_cmp(&a.confidence)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        Ok(root_causes)
    }

    /// Analyze temporal failure patterns
    pub async fn analyze_temporal_patterns(&self, limit: usize) -> Result<TemporalAnalysis> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Connection error: {}", e)))?;

        let tasks: Vec<String> = conn
            .lrange(&self.dlq_key, 0, limit as isize - 1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to read DLQ: {}", e)))?;

        // Group failures by hour
        let mut hourly_failures: HashMap<i64, usize> = HashMap::new();
        let mut min_time = i64::MAX;
        let mut max_time = i64::MIN;

        for task_data in &tasks {
            if let Ok(_task) = serde_json::from_str::<SerializedTask>(task_data) {
                // Use current time as proxy (in production, extract from task metadata)
                let timestamp = chrono::Utc::now().timestamp();
                let hour = timestamp / 3600;
                *hourly_failures.entry(hour).or_insert(0) += 1;
                min_time = min_time.min(hour);
                max_time = max_time.max(hour);
            }
        }

        let mut timeline: Vec<(i64, usize)> = hourly_failures.into_iter().collect();
        timeline.sort_by_key(|(time, _)| *time);

        let peak_time = timeline
            .iter()
            .max_by_key(|(_, count)| count)
            .map(|(time, _)| *time);

        let period_seconds = if max_time > min_time {
            ((max_time - min_time) * 3600) as u64
        } else {
            3600
        };

        let avg_failures_per_hour = if !timeline.is_empty() {
            tasks.len() as f64 / timeline.len() as f64
        } else {
            0.0
        };

        // Determine trend
        let trend = if timeline.len() >= 3 {
            let first_half: usize = timeline
                .iter()
                .take(timeline.len() / 2)
                .map(|(_, c)| c)
                .sum();
            let second_half: usize = timeline
                .iter()
                .skip(timeline.len() / 2)
                .map(|(_, c)| c)
                .sum();

            if second_half > first_half * 12 / 10 {
                FailureTrend::Increasing
            } else if second_half < first_half * 8 / 10 {
                FailureTrend::Decreasing
            } else {
                FailureTrend::Stable
            }
        } else {
            FailureTrend::Unknown
        };

        Ok(TemporalAnalysis {
            period_seconds,
            failure_timeline: timeline,
            peak_time,
            avg_failures_per_hour,
            trend,
        })
    }

    /// Classify a task's error type from its recorded failure result.
    ///
    /// The primary source of truth is the error message captured on the task
    /// state (`TaskState::Failed`). When the message names an explicit exception
    /// type (e.g. `ConnectionError: ...`, `TimeoutError`) that type is used
    /// verbatim; otherwise the message keywords are mapped to a canonical error
    /// name. The task name is only used as a last-resort hint when no failure
    /// reason is recorded.
    fn classify_error(&self, task: &SerializedTask) -> String {
        let reason = failure_reason(task);

        // Prefer an explicit exception type embedded in the message, e.g.
        // "ValueError: bad input" or "redis.ConnectionError: ...".
        if let Some(message) = &reason {
            if let Some(explicit) = Self::extract_exception_name(message) {
                return explicit;
            }
        }

        // Fall back to keyword classification over the error message (or the
        // task name if no failure reason is present).
        let haystack = reason
            .unwrap_or_else(|| task.metadata.name.clone())
            .to_lowercase();

        if haystack.contains("timeout") || haystack.contains("timed out") {
            "TimeoutError".to_string()
        } else if haystack.contains("network")
            || haystack.contains("connection")
            || haystack.contains("unreachable")
            || haystack.contains("refused")
        {
            "NetworkError".to_string()
        } else if haystack.contains("validation")
            || haystack.contains("invalid")
            || haystack.contains("parse")
        {
            "ValidationError".to_string()
        } else if haystack.contains("memory") || haystack.contains("resource") {
            "ResourceError".to_string()
        } else if haystack.contains("corrupt") || haystack.contains("checksum") {
            "DataCorruptionError".to_string()
        } else {
            "UnknownError".to_string()
        }
    }

    /// Pull an explicit exception type name from the start of an error message.
    ///
    /// Recognizes the common `SomeError: details` / `module.SomeError: details`
    /// convention. Returns the (unqualified) exception name when it looks like a
    /// real error type (ends in `Error`/`Exception` or is CamelCase), otherwise
    /// `None`.
    fn extract_exception_name(message: &str) -> Option<String> {
        let head = message.split(':').next()?.trim();
        if head.is_empty() || head.contains(char::is_whitespace) {
            return None;
        }

        // Use the last path segment so "redis.exceptions.ConnectionError" -> "ConnectionError".
        let name = head.rsplit('.').next().unwrap_or(head);

        let looks_like_error = name.ends_with("Error")
            || name.ends_with("Exception")
            || (name.chars().next().is_some_and(|c| c.is_ascii_uppercase())
                && name.chars().any(|c| c.is_ascii_lowercase()));

        if looks_like_error {
            Some(name.to_string())
        } else {
            None
        }
    }

    /// Categorize error type
    fn categorize_error(&self, error_type: &str) -> ErrorCategory {
        let error_lower = error_type.to_lowercase();

        if error_lower.contains("network") || error_lower.contains("connection") {
            ErrorCategory::Network
        } else if error_lower.contains("timeout") {
            ErrorCategory::Timeout
        } else if error_lower.contains("validation") || error_lower.contains("invalid") {
            ErrorCategory::Validation
        } else if error_lower.contains("memory") || error_lower.contains("resource") {
            ErrorCategory::ResourceExhaustion
        } else if error_lower.contains("external") || error_lower.contains("api") {
            ErrorCategory::ExternalService
        } else if error_lower.contains("corrupt") || error_lower.contains("checksum") {
            ErrorCategory::DataCorruption
        } else {
            ErrorCategory::Unknown
        }
    }

    /// Extract a normalized error signature for clustering similar failures.
    ///
    /// Uses the recorded failure message and strips variable substrings
    /// (numbers, quoted identifiers, …) so that semantically identical failures
    /// with differing details collapse to one cluster. The task name is
    /// prepended as a namespace so the same error from different task types is
    /// not conflated. Falls back to the task name when no failure reason exists.
    fn extract_error_signature(&self, task: &SerializedTask) -> String {
        match failure_reason(task) {
            Some(message) => {
                let normalized = normalize_error_signature(&message);
                if normalized.is_empty() {
                    task.metadata.name.clone()
                } else {
                    format!("{}::{}", task.metadata.name, normalized)
                }
            }
            None => task.metadata.name.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_category_recommendation() {
        assert_eq!(
            ErrorCategory::Network.recommendation(),
            "Check network connectivity and DNS resolution"
        );
        assert_eq!(
            ErrorCategory::Timeout.recommendation(),
            "Increase timeout values or optimize task execution"
        );
    }

    #[test]
    fn test_error_category_severity() {
        assert_eq!(ErrorCategory::DataCorruption.severity(), 5);
        assert_eq!(ErrorCategory::ResourceExhaustion.severity(), 4);
        assert_eq!(ErrorCategory::ExternalService.severity(), 3);
        assert_eq!(ErrorCategory::Network.severity(), 2);
        assert_eq!(ErrorCategory::Unknown.severity(), 1);
    }

    #[test]
    fn test_failure_trend_values() {
        assert_eq!(FailureTrend::Increasing, FailureTrend::Increasing);
        assert_ne!(FailureTrend::Increasing, FailureTrend::Decreasing);
    }

    /// Build a failed task whose state carries a specific error message.
    fn failed_task(name: &str, error: &str) -> SerializedTask {
        let mut task = SerializedTask::new(name.to_string(), vec![]);
        task.metadata.state = TaskState::Failed(error.to_string());
        task
    }

    /// `Client::open` only parses the URL; it does not open a connection, so
    /// this is safe to build in unit tests without a live Redis.
    fn test_analyzer() -> DLQAnalyzer {
        let client =
            Client::open("redis://localhost:6379").expect("valid redis url should parse in tests");
        DLQAnalyzer {
            client,
            queue_name: "test_queue".to_string(),
            dlq_key: "test_queue:dlq".to_string(),
        }
    }

    #[test]
    fn test_failure_reason_from_state() {
        let task = failed_task("send", "TimeoutError: deadline exceeded");
        assert_eq!(
            failure_reason(&task).as_deref(),
            Some("TimeoutError: deadline exceeded")
        );

        let pending = SerializedTask::new("noop".to_string(), vec![]);
        assert!(failure_reason(&pending).is_none());
    }

    #[test]
    fn test_classify_error_uses_recorded_message() {
        let analyzer = test_analyzer();

        // The task name is generic, but the error message identifies the type.
        let task = failed_task("run_job", "Connection refused (os error 111)");
        assert_eq!(analyzer.classify_error(&task), "NetworkError");

        // Explicit exception type, even with a module path, is used verbatim.
        let task = failed_task("run_job", "redis.exceptions.ConnectionError: down");
        assert_eq!(analyzer.classify_error(&task), "ConnectionError");

        let task = failed_task("run_job", "ValueError: bad payload");
        assert_eq!(analyzer.classify_error(&task), "ValueError");

        // Keyword fallback when no explicit exception name is present.
        let task = failed_task("run_job", "request timed out after waiting");
        assert_eq!(analyzer.classify_error(&task), "TimeoutError");

        // No recorded failure -> falls back to the task name hint.
        let task = SerializedTask::new("network_sync".to_string(), vec![]);
        assert_eq!(analyzer.classify_error(&task), "NetworkError");
    }

    #[test]
    fn test_extract_exception_name() {
        assert_eq!(
            DLQAnalyzer::extract_exception_name("ValueError: x"),
            Some("ValueError".to_string())
        );
        assert_eq!(
            DLQAnalyzer::extract_exception_name("pkg.mod.CustomException: boom"),
            Some("CustomException".to_string())
        );
        // A plain sentence is not an exception name.
        assert_eq!(
            DLQAnalyzer::extract_exception_name("something went wrong"),
            None
        );
        // Single lowercase token is not an error type.
        assert_eq!(DLQAnalyzer::extract_exception_name("oops: details"), None);
    }

    #[test]
    fn test_normalize_error_signature_collapses_variables() {
        // Differing numeric codes collapse to the same signature.
        let a = normalize_error_signature("Connection refused (os error 111)");
        let b = normalize_error_signature("Connection refused (os error 104)");
        assert_eq!(a, b);
        assert_eq!(a, "connection refused (os error #)");

        // Quoted identifiers are replaced.
        let s = normalize_error_signature("KeyError: 'user_id' not found");
        assert_eq!(s, "keyerror: ? not found");
    }

    #[test]
    fn test_extract_error_signature_clusters_similar_failures() {
        let analyzer = test_analyzer();

        let t1 = failed_task("fetch", "Connection refused (os error 111)");
        let t2 = failed_task("fetch", "Connection refused (os error 104)");
        // Same task + same normalized error => identical signature (clusters).
        assert_eq!(
            analyzer.extract_error_signature(&t1),
            analyzer.extract_error_signature(&t2)
        );

        // Same error but a different task type must not be conflated.
        let t3 = failed_task("store", "Connection refused (os error 111)");
        assert_ne!(
            analyzer.extract_error_signature(&t1),
            analyzer.extract_error_signature(&t3)
        );

        // Signature is namespaced by task name.
        assert!(analyzer.extract_error_signature(&t1).starts_with("fetch::"));
    }
}
