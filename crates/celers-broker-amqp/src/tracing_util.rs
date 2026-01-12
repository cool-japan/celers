//! Message tracing and observability utilities for AMQP
//!
//! This module provides utilities for tracing message flow through the AMQP system,
//! debugging message processing, and analyzing message patterns.
//!
//! # Examples
//!
//! ```
//! use celers_broker_amqp::tracing_util::{MessageTrace, TraceEvent, TraceRecorder};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut recorder = TraceRecorder::new();
//!
//! // Record message events
//! let trace_id = "msg-123".to_string();
//! recorder.record_event(trace_id.clone(), TraceEvent::Published {
//!     queue: "task_queue".to_string(),
//!     timestamp: std::time::Instant::now(),
//! });
//!
//! recorder.record_event(trace_id.clone(), TraceEvent::Consumed {
//!     queue: "task_queue".to_string(),
//!     timestamp: std::time::Instant::now(),
//! });
//!
//! // Get trace for message
//! let trace = recorder.get_trace(&trace_id);
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

/// Message trace event
#[derive(Debug, Clone, Serialize)]
pub enum TraceEvent {
    /// Message published to queue
    Published {
        queue: String,
        #[serde(skip)]
        timestamp: Instant,
    },
    /// Message consumed from queue
    Consumed {
        queue: String,
        #[serde(skip)]
        timestamp: Instant,
    },
    /// Message acknowledged
    Acknowledged {
        #[serde(skip)]
        timestamp: Instant,
    },
    /// Message rejected
    Rejected {
        requeue: bool,
        #[serde(skip)]
        timestamp: Instant,
    },
    /// Message sent to DLX
    DeadLettered {
        dlx: String,
        reason: String,
        #[serde(skip)]
        timestamp: Instant,
    },
    /// Message expired
    Expired {
        #[serde(skip)]
        timestamp: Instant,
    },
    /// Custom event
    Custom {
        event_type: String,
        details: String,
        #[serde(skip)]
        timestamp: Instant,
    },
}

impl TraceEvent {
    /// Get event timestamp
    pub fn timestamp(&self) -> Instant {
        match self {
            TraceEvent::Published { timestamp, .. } => *timestamp,
            TraceEvent::Consumed { timestamp, .. } => *timestamp,
            TraceEvent::Acknowledged { timestamp } => *timestamp,
            TraceEvent::Rejected { timestamp, .. } => *timestamp,
            TraceEvent::DeadLettered { timestamp, .. } => *timestamp,
            TraceEvent::Expired { timestamp } => *timestamp,
            TraceEvent::Custom { timestamp, .. } => *timestamp,
        }
    }

    /// Get event name
    pub fn event_name(&self) -> &str {
        match self {
            TraceEvent::Published { .. } => "published",
            TraceEvent::Consumed { .. } => "consumed",
            TraceEvent::Acknowledged { .. } => "acknowledged",
            TraceEvent::Rejected { .. } => "rejected",
            TraceEvent::DeadLettered { .. } => "dead_lettered",
            TraceEvent::Expired { .. } => "expired",
            TraceEvent::Custom { event_type, .. } => event_type,
        }
    }
}

/// Message trace containing all events for a message
#[derive(Debug, Clone, Serialize)]
pub struct MessageTrace {
    /// Trace ID (typically message ID or correlation ID)
    pub trace_id: String,
    /// All events for this message
    pub events: Vec<TraceEvent>,
}

impl MessageTrace {
    /// Create a new message trace
    pub fn new(trace_id: String) -> Self {
        Self {
            trace_id,
            events: Vec::new(),
        }
    }

    /// Add event to trace
    pub fn add_event(&mut self, event: TraceEvent) {
        self.events.push(event);
    }

    /// Get total processing time (from first to last event)
    pub fn total_processing_time(&self) -> Option<Duration> {
        if self.events.len() < 2 {
            return None;
        }

        let first = self.events.first()?.timestamp();
        let last = self.events.last()?.timestamp();

        Some(last.duration_since(first))
    }

    /// Get time between specific events
    pub fn time_between(&self, from_event: &str, to_event: &str) -> Option<Duration> {
        let from_idx = self
            .events
            .iter()
            .position(|e| e.event_name() == from_event)?;
        let to_idx = self
            .events
            .iter()
            .position(|e| e.event_name() == to_event)?;

        if from_idx >= to_idx {
            return None;
        }

        let from_time = self.events[from_idx].timestamp();
        let to_time = self.events[to_idx].timestamp();

        Some(to_time.duration_since(from_time))
    }

    /// Check if message was successfully processed
    pub fn is_successful(&self) -> bool {
        self.events
            .iter()
            .any(|e| matches!(e, TraceEvent::Acknowledged { .. }))
    }

    /// Check if message was rejected
    pub fn is_rejected(&self) -> bool {
        self.events
            .iter()
            .any(|e| matches!(e, TraceEvent::Rejected { .. }))
    }

    /// Check if message was dead-lettered
    pub fn is_dead_lettered(&self) -> bool {
        self.events
            .iter()
            .any(|e| matches!(e, TraceEvent::DeadLettered { .. }))
    }

    /// Get event count
    pub fn event_count(&self) -> usize {
        self.events.len()
    }
}

/// Trace recorder for tracking message events
#[derive(Debug, Clone)]
pub struct TraceRecorder {
    traces: HashMap<String, MessageTrace>,
    max_traces: usize,
}

impl TraceRecorder {
    /// Create a new trace recorder
    pub fn new() -> Self {
        Self {
            traces: HashMap::new(),
            max_traces: 10000,
        }
    }

    /// Create a new trace recorder with custom max traces
    pub fn with_capacity(max_traces: usize) -> Self {
        Self {
            traces: HashMap::new(),
            max_traces,
        }
    }

    /// Record a trace event
    pub fn record_event(&mut self, trace_id: String, event: TraceEvent) {
        // Check if we need to evict old traces
        if self.traces.len() >= self.max_traces && !self.traces.contains_key(&trace_id) {
            // Simple FIFO eviction - remove first trace
            if let Some(first_key) = self.traces.keys().next().cloned() {
                self.traces.remove(&first_key);
            }
        }

        self.traces
            .entry(trace_id.clone())
            .or_insert_with(|| MessageTrace::new(trace_id))
            .add_event(event);
    }

    /// Get trace for a message
    pub fn get_trace(&self, trace_id: &str) -> Option<&MessageTrace> {
        self.traces.get(trace_id)
    }

    /// Get all traces
    pub fn get_all_traces(&self) -> &HashMap<String, MessageTrace> {
        &self.traces
    }

    /// Clear all traces
    pub fn clear(&mut self) {
        self.traces.clear();
    }

    /// Get trace count
    pub fn trace_count(&self) -> usize {
        self.traces.len()
    }

    /// Get successful message count
    pub fn successful_count(&self) -> usize {
        self.traces.values().filter(|t| t.is_successful()).count()
    }

    /// Get rejected message count
    pub fn rejected_count(&self) -> usize {
        self.traces.values().filter(|t| t.is_rejected()).count()
    }

    /// Get dead-lettered message count
    pub fn dead_lettered_count(&self) -> usize {
        self.traces
            .values()
            .filter(|t| t.is_dead_lettered())
            .count()
    }

    /// Get average processing time
    pub fn average_processing_time(&self) -> Option<Duration> {
        let times: Vec<Duration> = self
            .traces
            .values()
            .filter_map(|t| t.total_processing_time())
            .collect();

        if times.is_empty() {
            return None;
        }

        let total: Duration = times.iter().sum();
        Some(total / times.len() as u32)
    }
}

impl Default for TraceRecorder {
    fn default() -> Self {
        Self::new()
    }
}

/// Message flow analyzer
#[derive(Debug, Clone)]
pub struct MessageFlowAnalyzer {
    recorder: TraceRecorder,
}

impl MessageFlowAnalyzer {
    /// Create a new message flow analyzer
    pub fn new() -> Self {
        Self {
            recorder: TraceRecorder::new(),
        }
    }

    /// Record event
    pub fn record_event(&mut self, trace_id: String, event: TraceEvent) {
        self.recorder.record_event(trace_id, event);
    }

    /// Analyze message flow and get insights
    pub fn analyze(&self) -> MessageFlowInsights {
        let total_messages = self.recorder.trace_count();
        let successful_messages = self.recorder.successful_count();
        let rejected_messages = self.recorder.rejected_count();
        let dead_lettered_messages = self.recorder.dead_lettered_count();

        let success_rate = if total_messages > 0 {
            (successful_messages as f64 / total_messages as f64) * 100.0
        } else {
            0.0
        };

        let rejection_rate = if total_messages > 0 {
            (rejected_messages as f64 / total_messages as f64) * 100.0
        } else {
            0.0
        };

        let dead_letter_rate = if total_messages > 0 {
            (dead_lettered_messages as f64 / total_messages as f64) * 100.0
        } else {
            0.0
        };

        MessageFlowInsights {
            total_messages,
            successful_messages,
            rejected_messages,
            dead_lettered_messages,
            success_rate,
            rejection_rate,
            dead_letter_rate,
            average_processing_time: self.recorder.average_processing_time(),
        }
    }

    /// Get the underlying recorder
    pub fn recorder(&self) -> &TraceRecorder {
        &self.recorder
    }
}

impl Default for MessageFlowAnalyzer {
    fn default() -> Self {
        Self::new()
    }
}

/// Message flow insights
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageFlowInsights {
    /// Total number of messages
    pub total_messages: usize,
    /// Number of successful messages
    pub successful_messages: usize,
    /// Number of rejected messages
    pub rejected_messages: usize,
    /// Number of dead-lettered messages
    pub dead_lettered_messages: usize,
    /// Success rate (percentage)
    pub success_rate: f64,
    /// Rejection rate (percentage)
    pub rejection_rate: f64,
    /// Dead letter rate (percentage)
    pub dead_letter_rate: f64,
    /// Average processing time
    #[serde(skip)]
    pub average_processing_time: Option<Duration>,
}

impl MessageFlowInsights {
    /// Check if flow is healthy (>95% success rate)
    pub fn is_healthy(&self) -> bool {
        self.success_rate >= 95.0
    }

    /// Get health status
    pub fn health_status(&self) -> &str {
        if self.success_rate >= 95.0 {
            "healthy"
        } else if self.success_rate >= 80.0 {
            "degraded"
        } else {
            "unhealthy"
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_trace_creation() {
        let trace = MessageTrace::new("msg-123".to_string());
        assert_eq!(trace.trace_id, "msg-123");
        assert_eq!(trace.events.len(), 0);
    }

    #[test]
    fn test_message_trace_add_event() {
        let mut trace = MessageTrace::new("msg-123".to_string());

        trace.add_event(TraceEvent::Published {
            queue: "test_queue".to_string(),
            timestamp: Instant::now(),
        });

        assert_eq!(trace.events.len(), 1);
    }

    #[test]
    fn test_trace_recorder() {
        let mut recorder = TraceRecorder::new();

        recorder.record_event(
            "msg-1".to_string(),
            TraceEvent::Published {
                queue: "queue1".to_string(),
                timestamp: Instant::now(),
            },
        );

        assert_eq!(recorder.trace_count(), 1);
        assert!(recorder.get_trace("msg-1").is_some());
    }

    #[test]
    fn test_trace_recorder_eviction() {
        let mut recorder = TraceRecorder::with_capacity(2);

        recorder.record_event(
            "msg-1".to_string(),
            TraceEvent::Published {
                queue: "queue1".to_string(),
                timestamp: Instant::now(),
            },
        );

        recorder.record_event(
            "msg-2".to_string(),
            TraceEvent::Published {
                queue: "queue2".to_string(),
                timestamp: Instant::now(),
            },
        );

        recorder.record_event(
            "msg-3".to_string(),
            TraceEvent::Published {
                queue: "queue3".to_string(),
                timestamp: Instant::now(),
            },
        );

        // Should still be 2 traces (evicted oldest)
        assert_eq!(recorder.trace_count(), 2);
    }

    #[test]
    fn test_message_trace_is_successful() {
        let mut trace = MessageTrace::new("msg-123".to_string());

        trace.add_event(TraceEvent::Published {
            queue: "queue".to_string(),
            timestamp: Instant::now(),
        });

        trace.add_event(TraceEvent::Acknowledged {
            timestamp: Instant::now(),
        });

        assert!(trace.is_successful());
        assert!(!trace.is_rejected());
    }

    #[test]
    fn test_message_flow_analyzer() {
        let mut analyzer = MessageFlowAnalyzer::new();

        analyzer.record_event(
            "msg-1".to_string(),
            TraceEvent::Published {
                queue: "queue".to_string(),
                timestamp: Instant::now(),
            },
        );

        analyzer.record_event(
            "msg-1".to_string(),
            TraceEvent::Acknowledged {
                timestamp: Instant::now(),
            },
        );

        let insights = analyzer.analyze();
        assert_eq!(insights.total_messages, 1);
        assert_eq!(insights.successful_messages, 1);
        assert_eq!(insights.success_rate, 100.0);
    }

    #[test]
    fn test_message_flow_insights_health() {
        let insights = MessageFlowInsights {
            total_messages: 100,
            successful_messages: 96,
            rejected_messages: 4,
            dead_lettered_messages: 0,
            success_rate: 96.0,
            rejection_rate: 4.0,
            dead_letter_rate: 0.0,
            average_processing_time: None,
        };

        assert!(insights.is_healthy());
        assert_eq!(insights.health_status(), "healthy");
    }

    #[test]
    fn test_trace_event_name() {
        let event = TraceEvent::Published {
            queue: "test".to_string(),
            timestamp: Instant::now(),
        };
        assert_eq!(event.event_name(), "published");

        let event = TraceEvent::Acknowledged {
            timestamp: Instant::now(),
        };
        assert_eq!(event.event_name(), "acknowledged");
    }
}
