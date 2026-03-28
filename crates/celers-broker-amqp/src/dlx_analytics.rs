//! Dead Letter Exchange (DLX) Analytics
//!
//! Provides comprehensive analytics and insights for Dead Letter Exchange patterns in AMQP.
//!
//! # Features
//!
//! - **DLX Flow Analysis**: Track message flow through Dead Letter Exchanges
//! - **Failure Pattern Detection**: Identify common failure patterns and root causes
//! - **Retry Analysis**: Analyze retry attempts and success rates
//! - **Queue Health Monitoring**: Monitor dead letter queue depth and growth
//! - **Alert Generation**: Generate alerts for anomalous DLX behavior
//!
//! # Example
//!
//! ```rust
//! use celers_broker_amqp::dlx_analytics::{
//!     DlxAnalyzer, DlxEvent, FailureReason,
//! };
//! use std::time::SystemTime;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create analyzer
//! let analyzer = DlxAnalyzer::new("orders_dlx");
//!
//! // Record dead letter events
//! analyzer.record_event(DlxEvent {
//!     message_id: "msg-123".to_string(),
//!     original_queue: "orders".to_string(),
//!     dlx_queue: "orders_dlx".to_string(),
//!     reason: FailureReason::Rejected,
//!     timestamp: SystemTime::now(),
//!     retry_count: 0,
//!     payload_size: 1024,
//! }).await;
//!
//! // Analyze patterns
//! let insights = analyzer.analyze().await;
//! println!("Total dead lettered: {}", insights.total_dead_lettered);
//! println!("Most common failure: {:?}", insights.most_common_failure_reason);
//! println!("Avg retry attempts: {:.1}", insights.average_retry_attempts);
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;

/// Reason why a message was sent to DLX
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FailureReason {
    /// Message was rejected (basic.reject or basic.nack with requeue=false)
    Rejected,
    /// Message TTL expired
    Expired,
    /// Queue length limit exceeded
    MaxLengthExceeded,
    /// Message was rejected due to routing failure
    RoutingFailed,
    /// Consumer cancelled
    ConsumerCancelled,
    /// Unknown or unspecified reason
    Unknown,
}

impl FailureReason {
    /// Get a human-readable description
    pub fn description(&self) -> &'static str {
        match self {
            FailureReason::Rejected => "Message was explicitly rejected",
            FailureReason::Expired => "Message TTL expired",
            FailureReason::MaxLengthExceeded => "Queue length limit exceeded",
            FailureReason::RoutingFailed => "Message routing failed",
            FailureReason::ConsumerCancelled => "Consumer was cancelled",
            FailureReason::Unknown => "Unknown failure reason",
        }
    }

    /// Check if the failure is recoverable with retry
    pub fn is_recoverable(&self) -> bool {
        matches!(
            self,
            FailureReason::ConsumerCancelled | FailureReason::RoutingFailed
        )
    }
}

/// Event representing a message entering the Dead Letter Exchange
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlxEvent {
    /// Message identifier
    pub message_id: String,
    /// Original queue the message was published to
    pub original_queue: String,
    /// Dead letter queue/exchange name
    pub dlx_queue: String,
    /// Reason for dead lettering
    pub reason: FailureReason,
    /// Timestamp when the event occurred
    pub timestamp: SystemTime,
    /// Number of retry attempts before dead lettering
    pub retry_count: u32,
    /// Size of the message payload in bytes
    pub payload_size: usize,
}

/// Statistics for a specific queue's DLX behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueDlxStats {
    /// Queue name
    pub queue_name: String,
    /// Total messages dead lettered from this queue
    pub total_dead_lettered: u64,
    /// Dead letter rate (messages per second)
    pub dead_letter_rate: f64,
    /// Average retry count before dead lettering
    pub avg_retry_count: f64,
    /// Failure reason breakdown
    pub failure_reasons: HashMap<FailureReason, u64>,
    /// Average payload size of dead lettered messages
    pub avg_payload_size: f64,
}

/// Comprehensive DLX analysis insights
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlxInsights {
    /// Total number of messages dead lettered
    pub total_dead_lettered: u64,
    /// Dead letter rate (messages per second)
    pub dead_letter_rate: f64,
    /// Most common failure reason
    pub most_common_failure_reason: Option<FailureReason>,
    /// Percentage of messages that could be recovered with retry
    pub recoverable_percentage: f64,
    /// Average number of retry attempts before giving up
    pub average_retry_attempts: f64,
    /// Top queues by dead letter count
    pub top_problematic_queues: Vec<(String, u64)>,
    /// Average time from original publish to dead letter
    pub avg_time_to_dlx: Option<Duration>,
    /// Total payload bytes in DLX
    pub total_dlx_payload_bytes: u64,
    /// Health status
    pub health_status: DlxHealthStatus,
    /// Recommendations for improving system reliability
    pub recommendations: Vec<String>,
}

/// Health status of the DLX system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DlxHealthStatus {
    /// Healthy - low dead letter rate
    Healthy,
    /// Warning - elevated dead letter rate
    Warning,
    /// Critical - high dead letter rate indicating system issues
    Critical,
}

impl DlxHealthStatus {
    /// Determine health status from dead letter rate
    pub fn from_rate(rate: f64) -> Self {
        if rate < 1.0 {
            DlxHealthStatus::Healthy
        } else if rate < 10.0 {
            DlxHealthStatus::Warning
        } else {
            DlxHealthStatus::Critical
        }
    }
}

/// Analyzer for Dead Letter Exchange patterns
pub struct DlxAnalyzer {
    /// DLX name
    #[allow(dead_code)]
    dlx_name: String,
    /// Recorded events
    events: Arc<RwLock<Vec<DlxEvent>>>,
    /// Maximum number of events to keep in memory
    max_events: usize,
}

impl DlxAnalyzer {
    /// Create a new DLX analyzer
    ///
    /// # Arguments
    ///
    /// * `dlx_name` - Name of the Dead Letter Exchange to analyze
    pub fn new(dlx_name: &str) -> Self {
        Self {
            dlx_name: dlx_name.to_string(),
            events: Arc::new(RwLock::new(Vec::new())),
            max_events: 10_000,
        }
    }

    /// Create a new DLX analyzer with custom capacity
    pub fn with_capacity(dlx_name: &str, max_events: usize) -> Self {
        Self {
            dlx_name: dlx_name.to_string(),
            events: Arc::new(RwLock::new(Vec::new())),
            max_events,
        }
    }

    /// Record a dead letter event
    pub async fn record_event(&self, event: DlxEvent) {
        let mut events = self.events.write().await;
        events.push(event);

        // Evict oldest events if capacity exceeded
        if events.len() > self.max_events {
            events.remove(0);
        }
    }

    /// Analyze DLX patterns and generate insights
    pub async fn analyze(&self) -> DlxInsights {
        let events = self.events.read().await;

        if events.is_empty() {
            return DlxInsights {
                total_dead_lettered: 0,
                dead_letter_rate: 0.0,
                most_common_failure_reason: None,
                recoverable_percentage: 0.0,
                average_retry_attempts: 0.0,
                top_problematic_queues: Vec::new(),
                avg_time_to_dlx: None,
                total_dlx_payload_bytes: 0,
                health_status: DlxHealthStatus::Healthy,
                recommendations: Vec::new(),
            };
        }

        let total_dead_lettered = events.len() as u64;

        // Calculate rate
        let dead_letter_rate = if events.len() >= 2 {
            let first_time = events[0].timestamp;
            let last_time = events[events.len() - 1].timestamp;
            if let Ok(duration) = last_time.duration_since(first_time) {
                let seconds = duration.as_secs_f64();
                if seconds > 0.0 {
                    events.len() as f64 / seconds
                } else {
                    0.0
                }
            } else {
                0.0
            }
        } else {
            0.0
        };

        // Analyze failure reasons
        let mut failure_counts: HashMap<FailureReason, u64> = HashMap::new();
        for event in events.iter() {
            *failure_counts.entry(event.reason).or_insert(0) += 1;
        }

        let most_common_failure_reason = failure_counts
            .iter()
            .max_by_key(|(_, count)| *count)
            .map(|(reason, _)| *reason);

        // Calculate recoverable percentage
        let recoverable_count = events.iter().filter(|e| e.reason.is_recoverable()).count();
        let recoverable_percentage = (recoverable_count as f64 / events.len() as f64) * 100.0;

        // Calculate average retry attempts
        let total_retries: u32 = events.iter().map(|e| e.retry_count).sum();
        let average_retry_attempts = total_retries as f64 / events.len() as f64;

        // Find top problematic queues
        let mut queue_counts: HashMap<String, u64> = HashMap::new();
        for event in events.iter() {
            *queue_counts
                .entry(event.original_queue.clone())
                .or_insert(0) += 1;
        }

        let mut top_problematic_queues: Vec<(String, u64)> = queue_counts.into_iter().collect();
        top_problematic_queues.sort_by_key(|b| std::cmp::Reverse(b.1));
        top_problematic_queues.truncate(10);

        // Calculate total payload bytes
        let total_dlx_payload_bytes: usize = events.iter().map(|e| e.payload_size).sum();

        // Determine health status
        let health_status = DlxHealthStatus::from_rate(dead_letter_rate);

        // Generate recommendations
        let recommendations = self.generate_recommendations(
            &events,
            most_common_failure_reason,
            recoverable_percentage,
            average_retry_attempts,
            dead_letter_rate,
        );

        DlxInsights {
            total_dead_lettered,
            dead_letter_rate,
            most_common_failure_reason,
            recoverable_percentage,
            average_retry_attempts,
            top_problematic_queues,
            avg_time_to_dlx: None, // Could calculate if we track original publish time
            total_dlx_payload_bytes: total_dlx_payload_bytes as u64,
            health_status,
            recommendations,
        }
    }

    /// Get statistics for a specific queue
    pub async fn get_queue_stats(&self, queue_name: &str) -> Option<QueueDlxStats> {
        let events = self.events.read().await;

        let queue_events: Vec<_> = events
            .iter()
            .filter(|e| e.original_queue == queue_name)
            .collect();

        if queue_events.is_empty() {
            return None;
        }

        let total_dead_lettered = queue_events.len() as u64;

        // Calculate rate
        let dead_letter_rate = if queue_events.len() >= 2 {
            let first_time = queue_events[0].timestamp;
            let last_time = queue_events[queue_events.len() - 1].timestamp;
            if let Ok(duration) = last_time.duration_since(first_time) {
                let seconds = duration.as_secs_f64();
                if seconds > 0.0 {
                    queue_events.len() as f64 / seconds
                } else {
                    0.0
                }
            } else {
                0.0
            }
        } else {
            0.0
        };

        let avg_retry_count = queue_events.iter().map(|e| e.retry_count).sum::<u32>() as f64
            / queue_events.len() as f64;

        let mut failure_reasons: HashMap<FailureReason, u64> = HashMap::new();
        for event in queue_events.iter() {
            *failure_reasons.entry(event.reason).or_insert(0) += 1;
        }

        let avg_payload_size = queue_events.iter().map(|e| e.payload_size).sum::<usize>() as f64
            / queue_events.len() as f64;

        Some(QueueDlxStats {
            queue_name: queue_name.to_string(),
            total_dead_lettered,
            dead_letter_rate,
            avg_retry_count,
            failure_reasons,
            avg_payload_size,
        })
    }

    /// Clear all recorded events
    pub async fn clear(&self) {
        let mut events = self.events.write().await;
        events.clear();
    }

    /// Get the number of recorded events
    pub async fn event_count(&self) -> usize {
        let events = self.events.read().await;
        events.len()
    }

    fn generate_recommendations(
        &self,
        events: &[DlxEvent],
        most_common_failure: Option<FailureReason>,
        recoverable_percentage: f64,
        average_retry_attempts: f64,
        dead_letter_rate: f64,
    ) -> Vec<String> {
        let mut recommendations = Vec::new();

        // High dead letter rate
        if dead_letter_rate > 10.0 {
            recommendations.push(
                "Critical: Dead letter rate is very high (>10/sec). Investigate system health."
                    .to_string(),
            );
        } else if dead_letter_rate > 1.0 {
            recommendations.push(
                "Warning: Elevated dead letter rate (>1/sec). Monitor for trends.".to_string(),
            );
        }

        // Specific failure reason recommendations
        if let Some(reason) = most_common_failure {
            match reason {
                FailureReason::Rejected => {
                    recommendations.push(
                        "Most failures are rejections. Review consumer error handling logic."
                            .to_string(),
                    );
                }
                FailureReason::Expired => {
                    recommendations.push(
                        "Many messages are expiring. Consider increasing TTL or adding consumers."
                            .to_string(),
                    );
                }
                FailureReason::MaxLengthExceeded => {
                    recommendations.push(
                        "Queue length limits being hit. Increase max-length or add consumers."
                            .to_string(),
                    );
                }
                FailureReason::ConsumerCancelled => {
                    recommendations.push(
                        "Consumers are being cancelled. Check consumer stability and connection health.".to_string(),
                    );
                }
                _ => {}
            }
        }

        // High recoverable percentage
        if recoverable_percentage > 50.0 {
            recommendations.push(format!(
                "{:.0}% of failures are recoverable. Implement retry mechanism.",
                recoverable_percentage
            ));
        }

        // Low retry attempts
        if average_retry_attempts < 1.0 && !events.is_empty() {
            recommendations.push(
                "Messages are not being retried before dead lettering. Consider adding retry logic.".to_string(),
            );
        }

        // High retry attempts
        if average_retry_attempts > 5.0 {
            recommendations.push(
                format!(
                    "Average retry count is high ({:.1}). Messages may be permanently failing. Review failure patterns.",
                    average_retry_attempts
                ),
            );
        }

        if recommendations.is_empty() {
            recommendations.push("DLX behavior appears healthy. Continue monitoring.".to_string());
        }

        recommendations
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_dlx_analyzer_basic() {
        let analyzer = DlxAnalyzer::new("test_dlx");

        analyzer
            .record_event(DlxEvent {
                message_id: "msg1".to_string(),
                original_queue: "orders".to_string(),
                dlx_queue: "orders_dlx".to_string(),
                reason: FailureReason::Rejected,
                timestamp: SystemTime::now(),
                retry_count: 2,
                payload_size: 1024,
            })
            .await;

        let insights = analyzer.analyze().await;
        assert_eq!(insights.total_dead_lettered, 1);
        assert_eq!(insights.average_retry_attempts, 2.0);
    }

    #[tokio::test]
    async fn test_failure_reason_descriptions() {
        assert!(!FailureReason::Rejected.description().is_empty());
        assert!(FailureReason::ConsumerCancelled.is_recoverable());
        assert!(!FailureReason::Rejected.is_recoverable());
    }

    #[tokio::test]
    async fn test_queue_stats() {
        let analyzer = DlxAnalyzer::new("test_dlx");

        analyzer
            .record_event(DlxEvent {
                message_id: "msg1".to_string(),
                original_queue: "orders".to_string(),
                dlx_queue: "orders_dlx".to_string(),
                reason: FailureReason::Rejected,
                timestamp: SystemTime::now(),
                retry_count: 1,
                payload_size: 512,
            })
            .await;

        analyzer
            .record_event(DlxEvent {
                message_id: "msg2".to_string(),
                original_queue: "orders".to_string(),
                dlx_queue: "orders_dlx".to_string(),
                reason: FailureReason::Expired,
                timestamp: SystemTime::now(),
                retry_count: 0,
                payload_size: 256,
            })
            .await;

        let stats = analyzer.get_queue_stats("orders").await.unwrap();
        assert_eq!(stats.total_dead_lettered, 2);
        assert_eq!(stats.avg_retry_count, 0.5);
        assert_eq!(stats.avg_payload_size, 384.0);
    }

    #[tokio::test]
    async fn test_health_status() {
        assert_eq!(DlxHealthStatus::from_rate(0.5), DlxHealthStatus::Healthy);
        assert_eq!(DlxHealthStatus::from_rate(5.0), DlxHealthStatus::Warning);
        assert_eq!(DlxHealthStatus::from_rate(15.0), DlxHealthStatus::Critical);
    }

    #[tokio::test]
    async fn test_clear_events() {
        let analyzer = DlxAnalyzer::new("test_dlx");

        analyzer
            .record_event(DlxEvent {
                message_id: "msg1".to_string(),
                original_queue: "test".to_string(),
                dlx_queue: "test_dlx".to_string(),
                reason: FailureReason::Rejected,
                timestamp: SystemTime::now(),
                retry_count: 0,
                payload_size: 100,
            })
            .await;

        assert_eq!(analyzer.event_count().await, 1);
        analyzer.clear().await;
        assert_eq!(analyzer.event_count().await, 0);
    }
}
