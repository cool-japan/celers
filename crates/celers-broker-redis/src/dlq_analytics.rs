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

use celers_core::{CelersError, Result, SerializedTask};
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::debug;

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
        patterns.sort_by(|a, b| b.count.cmp(&a.count));

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
        result.sort_by(|a, b| b.task_count.cmp(&a.task_count));

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

    /// Classify error from task metadata
    fn classify_error(&self, task: &SerializedTask) -> String {
        // Extract error type from task name or metadata
        // This is a simplified implementation - in production, you'd extract from task result
        if task.metadata.name.contains("timeout") {
            "TimeoutError".to_string()
        } else if task.metadata.name.contains("network") {
            "NetworkError".to_string()
        } else if task.metadata.name.contains("validation") {
            "ValidationError".to_string()
        } else {
            "UnknownError".to_string()
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

    /// Extract error signature for clustering
    fn extract_error_signature(&self, task: &SerializedTask) -> String {
        // Simplified: use task name as signature
        // In production, extract from error message and normalize
        task.metadata.name.clone()
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
}
