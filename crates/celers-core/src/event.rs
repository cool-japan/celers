//! Real-time event types for task and worker lifecycle
//!
//! This module provides Celery-compatible event types for monitoring task execution
//! and worker status. Events can be published to various transports (Redis pub/sub,
//! AMQP fanout, etc.) for real-time monitoring.
//!
//! # Event Types
//!
//! ## Task Events
//! - `TaskSent` - Task was sent to the queue
//! - `TaskReceived` - Task was received by a worker
//! - `TaskStarted` - Task execution started
//! - `TaskSucceeded` - Task completed successfully
//! - `TaskFailed` - Task execution failed
//! - `TaskRetried` - Task is being retried
//! - `TaskRevoked` - Task was revoked/cancelled
//! - `TaskRejected` - Task was rejected by worker
//!
//! ## Worker Events
//! - `WorkerOnline` - Worker came online
//! - `WorkerOffline` - Worker going offline
//! - `WorkerHeartbeat` - Periodic worker heartbeat
//!
//! # Example
//!
//! ```rust
//! use celers_core::event::{Event, TaskEvent, WorkerEvent};
//! use uuid::Uuid;
//! use chrono::Utc;
//!
//! // Create a task started event
//! let event = Event::Task(TaskEvent::Started {
//!     task_id: Uuid::new_v4(),
//!     task_name: "my_task".to_string(),
//!     hostname: "worker-1".to_string(),
//!     timestamp: Utc::now(),
//!     pid: std::process::id(),
//! });
//!
//! // Serialize for transport
//! let json = serde_json::to_string(&event).unwrap();
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Task lifecycle events (Celery-compatible)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum TaskEvent {
    /// Task was sent to the queue
    #[serde(rename = "task-sent")]
    Sent {
        /// Unique task ID
        task_id: Uuid,
        /// Task name (function name)
        task_name: String,
        /// Queue name where task was sent
        queue: String,
        /// Event timestamp
        timestamp: DateTime<Utc>,
        /// Task arguments (serialized)
        #[serde(skip_serializing_if = "Option::is_none")]
        args: Option<String>,
        /// Task keyword arguments (serialized)
        #[serde(skip_serializing_if = "Option::is_none")]
        kwargs: Option<String>,
        /// Task ETA if scheduled
        #[serde(skip_serializing_if = "Option::is_none")]
        eta: Option<DateTime<Utc>>,
        /// Task expiration time
        #[serde(skip_serializing_if = "Option::is_none")]
        expires: Option<DateTime<Utc>>,
        /// Number of retries configured
        #[serde(skip_serializing_if = "Option::is_none")]
        retries: Option<u32>,
    },

    /// Task was received by a worker
    #[serde(rename = "task-received")]
    Received {
        /// Unique task ID
        task_id: Uuid,
        /// Task name
        task_name: String,
        /// Worker hostname
        hostname: String,
        /// Event timestamp
        timestamp: DateTime<Utc>,
        /// Worker process ID
        pid: u32,
    },

    /// Task execution started
    #[serde(rename = "task-started")]
    Started {
        /// Unique task ID
        task_id: Uuid,
        /// Task name
        task_name: String,
        /// Worker hostname
        hostname: String,
        /// Event timestamp
        timestamp: DateTime<Utc>,
        /// Worker process ID
        pid: u32,
    },

    /// Task completed successfully
    #[serde(rename = "task-succeeded")]
    Succeeded {
        /// Unique task ID
        task_id: Uuid,
        /// Task name
        task_name: String,
        /// Worker hostname
        hostname: String,
        /// Event timestamp
        timestamp: DateTime<Utc>,
        /// Execution runtime in seconds
        runtime: f64,
        /// Result value (serialized)
        #[serde(skip_serializing_if = "Option::is_none")]
        result: Option<String>,
    },

    /// Task execution failed
    #[serde(rename = "task-failed")]
    Failed {
        /// Unique task ID
        task_id: Uuid,
        /// Task name
        task_name: String,
        /// Worker hostname
        hostname: String,
        /// Event timestamp
        timestamp: DateTime<Utc>,
        /// Exception type name
        exception: String,
        /// Exception message
        #[serde(skip_serializing_if = "Option::is_none")]
        traceback: Option<String>,
    },

    /// Task is being retried
    #[serde(rename = "task-retried")]
    Retried {
        /// Unique task ID
        task_id: Uuid,
        /// Task name
        task_name: String,
        /// Worker hostname
        hostname: String,
        /// Event timestamp
        timestamp: DateTime<Utc>,
        /// Exception that caused retry
        exception: String,
        /// Current retry attempt number
        retries: u32,
    },

    /// Task was revoked/cancelled
    #[serde(rename = "task-revoked")]
    Revoked {
        /// Unique task ID
        task_id: Uuid,
        /// Task name
        #[serde(skip_serializing_if = "Option::is_none")]
        task_name: Option<String>,
        /// Event timestamp
        timestamp: DateTime<Utc>,
        /// Whether to terminate running task
        terminated: bool,
        /// Signal used for termination
        #[serde(skip_serializing_if = "Option::is_none")]
        signum: Option<i32>,
        /// Whether task should be expired
        expired: bool,
    },

    /// Task was rejected by worker
    #[serde(rename = "task-rejected")]
    Rejected {
        /// Unique task ID
        task_id: Uuid,
        /// Task name
        #[serde(skip_serializing_if = "Option::is_none")]
        task_name: Option<String>,
        /// Worker hostname
        hostname: String,
        /// Event timestamp
        timestamp: DateTime<Utc>,
        /// Rejection reason
        reason: String,
    },
}

/// Worker lifecycle events
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "kebab-case")]
pub enum WorkerEvent {
    /// Worker came online
    #[serde(rename = "worker-online")]
    Online {
        /// Worker hostname
        hostname: String,
        /// Event timestamp
        timestamp: DateTime<Utc>,
        /// Software information
        sw_ident: String,
        /// Software version
        sw_ver: String,
        /// Software system (OS)
        sw_sys: String,
    },

    /// Worker going offline
    #[serde(rename = "worker-offline")]
    Offline {
        /// Worker hostname
        hostname: String,
        /// Event timestamp
        timestamp: DateTime<Utc>,
    },

    /// Periodic worker heartbeat
    #[serde(rename = "worker-heartbeat")]
    Heartbeat {
        /// Worker hostname
        hostname: String,
        /// Event timestamp
        timestamp: DateTime<Utc>,
        /// Current task count in progress
        active: u32,
        /// Tasks processed since last heartbeat
        processed: u64,
        /// Current system load average
        #[serde(skip_serializing_if = "Option::is_none")]
        loadavg: Option<[f64; 3]>,
        /// Heartbeat frequency in seconds
        freq: f64,
    },
}

/// Combined event type for all CeleRS events
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(untagged)]
pub enum Event {
    /// Task lifecycle event
    Task(TaskEvent),
    /// Worker lifecycle event
    Worker(WorkerEvent),
}

impl Event {
    /// Get the event type as a string
    pub fn event_type(&self) -> &'static str {
        match self {
            Event::Task(TaskEvent::Sent { .. }) => "task-sent",
            Event::Task(TaskEvent::Received { .. }) => "task-received",
            Event::Task(TaskEvent::Started { .. }) => "task-started",
            Event::Task(TaskEvent::Succeeded { .. }) => "task-succeeded",
            Event::Task(TaskEvent::Failed { .. }) => "task-failed",
            Event::Task(TaskEvent::Retried { .. }) => "task-retried",
            Event::Task(TaskEvent::Revoked { .. }) => "task-revoked",
            Event::Task(TaskEvent::Rejected { .. }) => "task-rejected",
            Event::Worker(WorkerEvent::Online { .. }) => "worker-online",
            Event::Worker(WorkerEvent::Offline { .. }) => "worker-offline",
            Event::Worker(WorkerEvent::Heartbeat { .. }) => "worker-heartbeat",
        }
    }

    /// Get the timestamp of the event
    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Event::Task(e) => match e {
                TaskEvent::Sent { timestamp, .. } => *timestamp,
                TaskEvent::Received { timestamp, .. } => *timestamp,
                TaskEvent::Started { timestamp, .. } => *timestamp,
                TaskEvent::Succeeded { timestamp, .. } => *timestamp,
                TaskEvent::Failed { timestamp, .. } => *timestamp,
                TaskEvent::Retried { timestamp, .. } => *timestamp,
                TaskEvent::Revoked { timestamp, .. } => *timestamp,
                TaskEvent::Rejected { timestamp, .. } => *timestamp,
            },
            Event::Worker(e) => match e {
                WorkerEvent::Online { timestamp, .. } => *timestamp,
                WorkerEvent::Offline { timestamp, .. } => *timestamp,
                WorkerEvent::Heartbeat { timestamp, .. } => *timestamp,
            },
        }
    }

    /// Get the task ID if this is a task event
    pub fn task_id(&self) -> Option<Uuid> {
        match self {
            Event::Task(e) => Some(match e {
                TaskEvent::Sent { task_id, .. } => *task_id,
                TaskEvent::Received { task_id, .. } => *task_id,
                TaskEvent::Started { task_id, .. } => *task_id,
                TaskEvent::Succeeded { task_id, .. } => *task_id,
                TaskEvent::Failed { task_id, .. } => *task_id,
                TaskEvent::Retried { task_id, .. } => *task_id,
                TaskEvent::Revoked { task_id, .. } => *task_id,
                TaskEvent::Rejected { task_id, .. } => *task_id,
            }),
            Event::Worker(_) => None,
        }
    }

    /// Get the hostname if available
    pub fn hostname(&self) -> Option<&str> {
        match self {
            Event::Task(e) => match e {
                TaskEvent::Sent { .. } => None,
                TaskEvent::Received { hostname, .. } => Some(hostname),
                TaskEvent::Started { hostname, .. } => Some(hostname),
                TaskEvent::Succeeded { hostname, .. } => Some(hostname),
                TaskEvent::Failed { hostname, .. } => Some(hostname),
                TaskEvent::Retried { hostname, .. } => Some(hostname),
                TaskEvent::Revoked { .. } => None,
                TaskEvent::Rejected { hostname, .. } => Some(hostname),
            },
            Event::Worker(e) => match e {
                WorkerEvent::Online { hostname, .. } => Some(hostname),
                WorkerEvent::Offline { hostname, .. } => Some(hostname),
                WorkerEvent::Heartbeat { hostname, .. } => Some(hostname),
            },
        }
    }

    /// Check if this is a task event
    pub fn is_task_event(&self) -> bool {
        matches!(self, Event::Task(_))
    }

    /// Check if this is a worker event
    pub fn is_worker_event(&self) -> bool {
        matches!(self, Event::Worker(_))
    }
}

/// Builder for creating task events with common fields
#[derive(Debug, Clone)]
pub struct TaskEventBuilder {
    task_id: Uuid,
    task_name: String,
    hostname: Option<String>,
    pid: Option<u32>,
}

impl TaskEventBuilder {
    /// Create a new task event builder
    pub fn new(task_id: Uuid, task_name: impl Into<String>) -> Self {
        Self {
            task_id,
            task_name: task_name.into(),
            hostname: None,
            pid: None,
        }
    }

    /// Set the worker hostname
    pub fn hostname(mut self, hostname: impl Into<String>) -> Self {
        self.hostname = Some(hostname.into());
        self
    }

    /// Set the worker process ID
    pub fn pid(mut self, pid: u32) -> Self {
        self.pid = Some(pid);
        self
    }

    /// Build a task-sent event
    pub fn sent(self, queue: impl Into<String>) -> Event {
        Event::Task(TaskEvent::Sent {
            task_id: self.task_id,
            task_name: self.task_name,
            queue: queue.into(),
            timestamp: Utc::now(),
            args: None,
            kwargs: None,
            eta: None,
            expires: None,
            retries: None,
        })
    }

    /// Build a task-received event
    pub fn received(self) -> Event {
        Event::Task(TaskEvent::Received {
            task_id: self.task_id,
            task_name: self.task_name,
            hostname: self.hostname.unwrap_or_else(|| "unknown".to_string()),
            timestamp: Utc::now(),
            pid: self.pid.unwrap_or(0),
        })
    }

    /// Build a task-started event
    pub fn started(self) -> Event {
        Event::Task(TaskEvent::Started {
            task_id: self.task_id,
            task_name: self.task_name,
            hostname: self.hostname.unwrap_or_else(|| "unknown".to_string()),
            timestamp: Utc::now(),
            pid: self.pid.unwrap_or(0),
        })
    }

    /// Build a task-succeeded event
    pub fn succeeded(self, runtime: f64) -> Event {
        Event::Task(TaskEvent::Succeeded {
            task_id: self.task_id,
            task_name: self.task_name,
            hostname: self.hostname.unwrap_or_else(|| "unknown".to_string()),
            timestamp: Utc::now(),
            runtime,
            result: None,
        })
    }

    /// Build a task-failed event
    pub fn failed(self, exception: impl Into<String>) -> Event {
        Event::Task(TaskEvent::Failed {
            task_id: self.task_id,
            task_name: self.task_name,
            hostname: self.hostname.unwrap_or_else(|| "unknown".to_string()),
            timestamp: Utc::now(),
            exception: exception.into(),
            traceback: None,
        })
    }

    /// Build a task-retried event
    pub fn retried(self, exception: impl Into<String>, retries: u32) -> Event {
        Event::Task(TaskEvent::Retried {
            task_id: self.task_id,
            task_name: self.task_name,
            hostname: self.hostname.unwrap_or_else(|| "unknown".to_string()),
            timestamp: Utc::now(),
            exception: exception.into(),
            retries,
        })
    }
}

/// Builder for creating worker events
#[derive(Debug, Clone)]
pub struct WorkerEventBuilder {
    hostname: String,
}

impl WorkerEventBuilder {
    /// Create a new worker event builder
    pub fn new(hostname: impl Into<String>) -> Self {
        Self {
            hostname: hostname.into(),
        }
    }

    /// Build a worker-online event
    pub fn online(self) -> Event {
        Event::Worker(WorkerEvent::Online {
            hostname: self.hostname,
            timestamp: Utc::now(),
            sw_ident: "celers".to_string(),
            sw_ver: env!("CARGO_PKG_VERSION").to_string(),
            sw_sys: std::env::consts::OS.to_string(),
        })
    }

    /// Build a worker-offline event
    pub fn offline(self) -> Event {
        Event::Worker(WorkerEvent::Offline {
            hostname: self.hostname,
            timestamp: Utc::now(),
        })
    }

    /// Build a worker-heartbeat event
    pub fn heartbeat(self, active: u32, processed: u64, loadavg: [f64; 3], freq: f64) -> Event {
        // Only include loadavg if non-zero (indicating it was actually measured)
        let loadavg_opt = if loadavg == [0.0, 0.0, 0.0] {
            None
        } else {
            Some(loadavg)
        };

        Event::Worker(WorkerEvent::Heartbeat {
            hostname: self.hostname,
            timestamp: Utc::now(),
            active,
            processed,
            loadavg: loadavg_opt,
            freq,
        })
    }
}

// ============================================================================
// Event Emitter Trait and Implementations
// ============================================================================

use async_trait::async_trait;
use std::sync::Arc;
use tokio::sync::broadcast;

/// Trait for emitting events to various transports
///
/// Implementations can send events to Redis pub/sub, AMQP fanout exchanges,
/// in-memory channels, or other event sinks.
///
/// # Example
///
/// ```rust
/// use celers_core::event::{Event, EventEmitter, InMemoryEventEmitter};
/// use celers_core::event::WorkerEventBuilder;
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let emitter = InMemoryEventEmitter::new(100);
/// let mut receiver = emitter.subscribe();
///
/// // Emit an event
/// let event = WorkerEventBuilder::new("worker-1").online();
/// emitter.emit(event.clone()).await?;
///
/// // Receive it
/// let received = receiver.recv().await?;
/// assert_eq!(received.event_type(), "worker-online");
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait EventEmitter: Send + Sync {
    /// Emit an event to the transport
    async fn emit(&self, event: Event) -> crate::Result<()>;

    /// Emit multiple events
    async fn emit_batch(&self, events: Vec<Event>) -> crate::Result<()> {
        for event in events {
            self.emit(event).await?;
        }
        Ok(())
    }

    /// Check if the emitter is enabled/active
    fn is_enabled(&self) -> bool {
        true
    }
}

/// No-op event emitter that discards all events
///
/// Useful for testing or when event emission is not needed.
#[derive(Debug, Clone, Default)]
pub struct NoOpEventEmitter;

impl NoOpEventEmitter {
    /// Create a new no-op event emitter
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl EventEmitter for NoOpEventEmitter {
    async fn emit(&self, _event: Event) -> crate::Result<()> {
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        false
    }
}

/// In-memory event emitter using broadcast channels
///
/// Useful for testing and local event distribution.
#[derive(Debug, Clone)]
pub struct InMemoryEventEmitter {
    sender: broadcast::Sender<Event>,
}

impl InMemoryEventEmitter {
    /// Create a new in-memory event emitter with the specified buffer capacity
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Subscribe to events
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }

    /// Get the number of active subscribers
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

#[async_trait]
impl EventEmitter for InMemoryEventEmitter {
    async fn emit(&self, event: Event) -> crate::Result<()> {
        // Ignore send errors (no subscribers)
        let _ = self.sender.send(event);
        Ok(())
    }
}

/// Logging event emitter that logs events using tracing
///
/// Useful for debugging and development.
#[derive(Debug, Clone, Default)]
pub struct LoggingEventEmitter {
    /// Log level for events
    level: LogLevel,
}

/// Log level for event logging
#[derive(Debug, Clone, Copy, Default)]
pub enum LogLevel {
    /// Trace level
    Trace,
    /// Debug level
    #[default]
    Debug,
    /// Info level
    Info,
}

impl LoggingEventEmitter {
    /// Create a new logging event emitter with default (debug) level
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a logging event emitter with specified level
    pub fn with_level(level: LogLevel) -> Self {
        Self { level }
    }
}

#[async_trait]
impl EventEmitter for LoggingEventEmitter {
    async fn emit(&self, event: Event) -> crate::Result<()> {
        let event_type = event.event_type();
        let task_id = event.task_id().map(|id| id.to_string());
        let hostname = event.hostname().map(String::from);

        match self.level {
            LogLevel::Trace => {
                tracing::trace!(
                    event_type = event_type,
                    task_id = ?task_id,
                    hostname = ?hostname,
                    "Event emitted"
                );
            }
            LogLevel::Debug => {
                tracing::debug!(
                    event_type = event_type,
                    task_id = ?task_id,
                    hostname = ?hostname,
                    "Event emitted"
                );
            }
            LogLevel::Info => {
                tracing::info!(
                    event_type = event_type,
                    task_id = ?task_id,
                    hostname = ?hostname,
                    "Event emitted"
                );
            }
        }
        Ok(())
    }
}

/// Composite event emitter that sends to multiple emitters
///
/// Useful for sending events to multiple destinations simultaneously.
#[derive(Clone)]
pub struct CompositeEventEmitter {
    emitters: Vec<Arc<dyn EventEmitter>>,
}

impl CompositeEventEmitter {
    /// Create a new composite emitter
    pub fn new() -> Self {
        Self {
            emitters: Vec::new(),
        }
    }

    /// Add an emitter to the composite
    pub fn with_emitter<E: EventEmitter + 'static>(mut self, emitter: E) -> Self {
        self.emitters.push(Arc::new(emitter));
        self
    }

    /// Add an Arc-wrapped emitter
    pub fn add_arc(mut self, emitter: Arc<dyn EventEmitter>) -> Self {
        self.emitters.push(emitter);
        self
    }
}

impl Default for CompositeEventEmitter {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl EventEmitter for CompositeEventEmitter {
    async fn emit(&self, event: Event) -> crate::Result<()> {
        for emitter in &self.emitters {
            if emitter.is_enabled() {
                emitter.emit(event.clone()).await?;
            }
        }
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.emitters.iter().any(|e| e.is_enabled())
    }
}

impl std::fmt::Debug for CompositeEventEmitter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompositeEventEmitter")
            .field("emitter_count", &self.emitters.len())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_event_serialization() {
        let event = Event::Task(TaskEvent::Started {
            task_id: Uuid::nil(),
            task_name: "test_task".to_string(),
            hostname: "worker-1".to_string(),
            timestamp: DateTime::parse_from_rfc3339("2025-12-01T12:00:00Z")
                .unwrap()
                .with_timezone(&Utc),
            pid: 1234,
        });

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("task-started"));
        assert!(json.contains("test_task"));
        assert!(json.contains("worker-1"));

        // Deserialize back
        let parsed: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(event, parsed);
    }

    #[test]
    fn test_worker_event_serialization() {
        let event = Event::Worker(WorkerEvent::Heartbeat {
            hostname: "worker-1".to_string(),
            timestamp: Utc::now(),
            active: 5,
            processed: 100,
            loadavg: Some([1.0, 0.8, 0.5]),
            freq: 2.0,
        });

        let json = serde_json::to_string(&event).unwrap();
        assert!(json.contains("worker-heartbeat"));
        assert!(json.contains("worker-1"));

        let parsed: Event = serde_json::from_str(&json).unwrap();
        assert_eq!(event.event_type(), parsed.event_type());
    }

    #[test]
    fn test_event_type() {
        let task_event = Event::Task(TaskEvent::Sent {
            task_id: Uuid::new_v4(),
            task_name: "test".to_string(),
            queue: "celery".to_string(),
            timestamp: Utc::now(),
            args: None,
            kwargs: None,
            eta: None,
            expires: None,
            retries: None,
        });

        assert_eq!(task_event.event_type(), "task-sent");
        assert!(task_event.is_task_event());
        assert!(!task_event.is_worker_event());
    }

    #[test]
    fn test_task_event_builder() {
        let task_id = Uuid::new_v4();
        let event = TaskEventBuilder::new(task_id, "my_task")
            .hostname("worker-1")
            .pid(1234)
            .started();

        assert_eq!(event.event_type(), "task-started");
        assert_eq!(event.task_id(), Some(task_id));
        assert_eq!(event.hostname(), Some("worker-1"));
    }

    #[test]
    fn test_worker_event_builder() {
        let event = WorkerEventBuilder::new("worker-1").online();

        assert_eq!(event.event_type(), "worker-online");
        assert!(event.is_worker_event());
        assert_eq!(event.hostname(), Some("worker-1"));
    }

    #[test]
    fn test_task_id_extraction() {
        let task_id = Uuid::new_v4();
        let event = TaskEventBuilder::new(task_id, "test").sent("celery");
        assert_eq!(event.task_id(), Some(task_id));

        let worker_event = WorkerEventBuilder::new("worker-1").online();
        assert_eq!(worker_event.task_id(), None);
    }

    #[tokio::test]
    async fn test_noop_event_emitter() {
        let emitter = NoOpEventEmitter::new();
        let event = WorkerEventBuilder::new("worker-1").online();

        // Should not fail
        emitter.emit(event).await.unwrap();

        // Should report as disabled
        assert!(!emitter.is_enabled());
    }

    #[tokio::test]
    async fn test_in_memory_event_emitter() {
        let emitter = InMemoryEventEmitter::new(10);
        let mut receiver = emitter.subscribe();

        let task_id = Uuid::new_v4();
        let event = TaskEventBuilder::new(task_id, "test_task")
            .hostname("worker-1")
            .started();

        // Emit event
        emitter.emit(event.clone()).await.unwrap();

        // Receive event
        let received = receiver.recv().await.unwrap();
        assert_eq!(received.event_type(), "task-started");
        assert_eq!(received.task_id(), Some(task_id));
    }

    #[tokio::test]
    async fn test_in_memory_emitter_multiple_subscribers() {
        let emitter = InMemoryEventEmitter::new(10);
        let mut receiver1 = emitter.subscribe();
        let mut receiver2 = emitter.subscribe();

        assert_eq!(emitter.subscriber_count(), 2);

        let event = WorkerEventBuilder::new("worker-1").heartbeat(5, 100, [1.0, 0.8, 0.5], 2.0);
        emitter.emit(event).await.unwrap();

        // Both receivers should get the event
        let r1 = receiver1.recv().await.unwrap();
        let r2 = receiver2.recv().await.unwrap();

        assert_eq!(r1.event_type(), "worker-heartbeat");
        assert_eq!(r2.event_type(), "worker-heartbeat");
    }

    #[tokio::test]
    async fn test_logging_event_emitter() {
        let emitter = LoggingEventEmitter::new();
        let event = TaskEventBuilder::new(Uuid::new_v4(), "test")
            .hostname("worker-1")
            .succeeded(1.5);

        // Should not fail (just logs)
        emitter.emit(event).await.unwrap();

        // Test with different log levels
        let emitter_info = LoggingEventEmitter::with_level(LogLevel::Info);
        let event = WorkerEventBuilder::new("worker-1").offline();
        emitter_info.emit(event).await.unwrap();
    }

    #[tokio::test]
    async fn test_composite_event_emitter() {
        let in_memory = InMemoryEventEmitter::new(10);
        let mut receiver = in_memory.subscribe();

        let composite = CompositeEventEmitter::new()
            .with_emitter(in_memory.clone())
            .with_emitter(NoOpEventEmitter::new());

        let event = TaskEventBuilder::new(Uuid::new_v4(), "test")
            .hostname("worker-1")
            .started();

        // Emit through composite
        composite.emit(event.clone()).await.unwrap();

        // Should receive through in_memory
        let received = receiver.recv().await.unwrap();
        assert_eq!(received.event_type(), "task-started");
    }

    #[tokio::test]
    async fn test_emit_batch() {
        let emitter = InMemoryEventEmitter::new(10);
        let mut receiver = emitter.subscribe();

        let events = vec![
            WorkerEventBuilder::new("worker-1").online(),
            TaskEventBuilder::new(Uuid::new_v4(), "task1")
                .hostname("worker-1")
                .started(),
            TaskEventBuilder::new(Uuid::new_v4(), "task2")
                .hostname("worker-1")
                .started(),
        ];

        emitter.emit_batch(events).await.unwrap();

        // Receive all events
        let e1 = receiver.recv().await.unwrap();
        let e2 = receiver.recv().await.unwrap();
        let e3 = receiver.recv().await.unwrap();

        assert_eq!(e1.event_type(), "worker-online");
        assert_eq!(e2.event_type(), "task-started");
        assert_eq!(e3.event_type(), "task-started");
    }
}
