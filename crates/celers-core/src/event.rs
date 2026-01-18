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

/// Combined event type for all `CeleRS` events
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
    #[inline]
    #[must_use]
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
    #[inline]
    #[must_use]
    pub fn timestamp(&self) -> DateTime<Utc> {
        match self {
            Event::Task(e) => match e {
                TaskEvent::Sent { timestamp, .. }
                | TaskEvent::Received { timestamp, .. }
                | TaskEvent::Started { timestamp, .. }
                | TaskEvent::Succeeded { timestamp, .. }
                | TaskEvent::Failed { timestamp, .. }
                | TaskEvent::Retried { timestamp, .. }
                | TaskEvent::Revoked { timestamp, .. }
                | TaskEvent::Rejected { timestamp, .. } => *timestamp,
            },
            Event::Worker(e) => match e {
                WorkerEvent::Online { timestamp, .. }
                | WorkerEvent::Offline { timestamp, .. }
                | WorkerEvent::Heartbeat { timestamp, .. } => *timestamp,
            },
        }
    }

    /// Get the task ID if this is a task event
    #[inline]
    #[must_use]
    pub fn task_id(&self) -> Option<Uuid> {
        match self {
            Event::Task(e) => Some(match e {
                TaskEvent::Sent { task_id, .. }
                | TaskEvent::Received { task_id, .. }
                | TaskEvent::Started { task_id, .. }
                | TaskEvent::Succeeded { task_id, .. }
                | TaskEvent::Failed { task_id, .. }
                | TaskEvent::Retried { task_id, .. }
                | TaskEvent::Revoked { task_id, .. }
                | TaskEvent::Rejected { task_id, .. } => *task_id,
            }),
            Event::Worker(_) => None,
        }
    }

    /// Get the hostname if available
    #[inline]
    #[must_use]
    pub fn hostname(&self) -> Option<&str> {
        match self {
            Event::Task(e) => match e {
                TaskEvent::Received { hostname, .. }
                | TaskEvent::Started { hostname, .. }
                | TaskEvent::Succeeded { hostname, .. }
                | TaskEvent::Failed { hostname, .. }
                | TaskEvent::Retried { hostname, .. }
                | TaskEvent::Rejected { hostname, .. } => Some(hostname),
                TaskEvent::Sent { .. } | TaskEvent::Revoked { .. } => None,
            },
            Event::Worker(e) => match e {
                WorkerEvent::Online { hostname, .. }
                | WorkerEvent::Offline { hostname, .. }
                | WorkerEvent::Heartbeat { hostname, .. } => Some(hostname),
            },
        }
    }

    /// Check if this is a task event
    #[inline]
    #[must_use]
    pub const fn is_task_event(&self) -> bool {
        matches!(self, Event::Task(_))
    }

    /// Check if this is a worker event
    #[inline]
    #[must_use]
    pub const fn is_worker_event(&self) -> bool {
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
    #[must_use]
    pub fn hostname(mut self, hostname: impl Into<String>) -> Self {
        self.hostname = Some(hostname.into());
        self
    }

    /// Set the worker process ID
    #[must_use]
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
    #[must_use]
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
    #[must_use]
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
    #[must_use]
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
    #[must_use]
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
    #[must_use]
    pub fn offline(self) -> Event {
        Event::Worker(WorkerEvent::Offline {
            hostname: self.hostname,
            timestamp: Utc::now(),
        })
    }

    /// Build a worker-heartbeat event
    #[must_use]
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
    #[must_use]
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
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (sender, _) = broadcast::channel(capacity);
        Self { sender }
    }

    /// Subscribe to events
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<Event> {
        self.sender.subscribe()
    }

    /// Get the number of active subscribers
    #[inline]
    #[must_use]
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
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a logging event emitter with specified level
    #[must_use]
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
    #[must_use]
    pub fn new() -> Self {
        Self {
            emitters: Vec::new(),
        }
    }

    /// Add an emitter to the composite
    #[must_use]
    pub fn with_emitter<E: EventEmitter + 'static>(mut self, emitter: E) -> Self {
        self.emitters.push(Arc::new(emitter));
        self
    }

    /// Add an Arc-wrapped emitter
    #[must_use]
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

// ============================================================================
// Event Consumer Infrastructure
// ============================================================================

use std::collections::HashMap;
use std::path::PathBuf;
use tokio::sync::RwLock;

/// Event filter for selecting which events to process
#[derive(Clone)]
pub enum EventFilter {
    /// Accept all events
    All,
    /// Accept only task events
    TaskOnly,
    /// Accept only worker events
    WorkerOnly,
    /// Accept specific event types (e.g., "task-started", "worker-online")
    EventTypes(Vec<String>),
    /// Accept events matching task name pattern
    TaskName(String),
    /// Accept events from specific hostname
    Hostname(String),
    /// Accept events matching custom predicate
    Custom(Arc<dyn Fn(&Event) -> bool + Send + Sync>),
    /// Combine multiple filters with AND logic
    And(Vec<EventFilter>),
    /// Combine multiple filters with OR logic
    Or(Vec<EventFilter>),
}

impl EventFilter {
    /// Check if an event matches this filter
    #[must_use]
    pub fn matches(&self, event: &Event) -> bool {
        match self {
            EventFilter::All => true,
            EventFilter::TaskOnly => matches!(event, Event::Task(_)),
            EventFilter::WorkerOnly => matches!(event, Event::Worker(_)),
            EventFilter::EventTypes(types) => types.contains(&event.event_type().to_string()),
            EventFilter::TaskName(name) => {
                if let Event::Task(task_event) = event {
                    match task_event {
                        TaskEvent::Sent { task_name, .. }
                        | TaskEvent::Received { task_name, .. }
                        | TaskEvent::Started { task_name, .. }
                        | TaskEvent::Succeeded { task_name, .. }
                        | TaskEvent::Failed { task_name, .. }
                        | TaskEvent::Retried { task_name, .. } => task_name == name,
                        TaskEvent::Revoked { task_name, .. }
                        | TaskEvent::Rejected { task_name, .. } => {
                            matches!(task_name.as_ref(), Some(tn) if tn == name)
                        }
                    }
                } else {
                    false
                }
            }
            EventFilter::Hostname(hostname) => {
                let event_hostname = match event {
                    Event::Task(task_event) => match task_event {
                        TaskEvent::Received { hostname, .. }
                        | TaskEvent::Started { hostname, .. }
                        | TaskEvent::Succeeded { hostname, .. }
                        | TaskEvent::Failed { hostname, .. }
                        | TaskEvent::Retried { hostname, .. }
                        | TaskEvent::Rejected { hostname, .. } => Some(hostname),
                        _ => None,
                    },
                    Event::Worker(worker_event) => match worker_event {
                        WorkerEvent::Online { hostname, .. }
                        | WorkerEvent::Offline { hostname, .. }
                        | WorkerEvent::Heartbeat { hostname, .. } => Some(hostname),
                    },
                };
                matches!(event_hostname, Some(h) if h == hostname)
            }
            EventFilter::Custom(predicate) => predicate(event),
            EventFilter::And(filters) => filters.iter().all(|f| f.matches(event)),
            EventFilter::Or(filters) => filters.iter().any(|f| f.matches(event)),
        }
    }

    /// Create a filter that accepts events from a list of task names
    pub fn task_names(names: Vec<String>) -> Self {
        EventFilter::Or(names.into_iter().map(EventFilter::TaskName).collect())
    }

    /// Create a filter that accepts events from a list of hostnames
    pub fn hostnames(names: Vec<String>) -> Self {
        EventFilter::Or(names.into_iter().map(EventFilter::Hostname).collect())
    }
}

impl std::fmt::Debug for EventFilter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EventFilter::All => write!(f, "EventFilter::All"),
            EventFilter::TaskOnly => write!(f, "EventFilter::TaskOnly"),
            EventFilter::WorkerOnly => write!(f, "EventFilter::WorkerOnly"),
            EventFilter::EventTypes(types) => f
                .debug_tuple("EventFilter::EventTypes")
                .field(types)
                .finish(),
            EventFilter::TaskName(name) => {
                f.debug_tuple("EventFilter::TaskName").field(name).finish()
            }
            EventFilter::Hostname(hostname) => f
                .debug_tuple("EventFilter::Hostname")
                .field(hostname)
                .finish(),
            EventFilter::Custom(_) => write!(f, "EventFilter::Custom(<closure>)"),
            EventFilter::And(filters) => f.debug_tuple("EventFilter::And").field(filters).finish(),
            EventFilter::Or(filters) => f.debug_tuple("EventFilter::Or").field(filters).finish(),
        }
    }
}

/// Event handler function type
pub type EventHandler = Arc<
    dyn Fn(Event) -> std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<()>> + Send>>
        + Send
        + Sync,
>;

/// Event receiver trait for consuming events
#[async_trait]
pub trait EventReceiver: Send + Sync {
    /// Receive the next event
    async fn receive(&mut self) -> crate::Result<Option<Event>>;

    /// Receive events with a timeout
    async fn receive_timeout(
        &mut self,
        timeout: std::time::Duration,
    ) -> crate::Result<Option<Event>> {
        tokio::time::timeout(timeout, self.receive())
            .await
            .map_err(|_| crate::CelersError::Broker("Receive timeout".to_string()))?
    }

    /// Check if the receiver is still active
    fn is_active(&self) -> bool {
        true
    }
}

/// Event dispatcher for routing events to handlers based on filters
#[derive(Clone)]
pub struct EventDispatcher {
    handlers: Arc<RwLock<Vec<(EventFilter, EventHandler)>>>,
}

impl EventDispatcher {
    /// Create a new event dispatcher
    #[must_use]
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Register a handler with a filter
    pub async fn register<F, Fut>(&self, filter: EventFilter, handler: F)
    where
        F: Fn(Event) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = crate::Result<()>> + Send + 'static,
    {
        let handler_arc = Arc::new(move |event: Event| {
            Box::pin(handler(event))
                as std::pin::Pin<Box<dyn std::future::Future<Output = crate::Result<()>> + Send>>
        });

        let mut handlers = self.handlers.write().await;
        handlers.push((filter, handler_arc));
    }

    /// Dispatch an event to all matching handlers
    ///
    /// # Errors
    ///
    /// Returns an error if any handler fails to process the event.
    pub async fn dispatch(&self, event: Event) -> crate::Result<()> {
        let handlers = self.handlers.read().await;

        for (filter, handler) in handlers.iter() {
            if filter.matches(&event) {
                handler(event.clone()).await?;
            }
        }

        Ok(())
    }

    /// Dispatch events in batch
    ///
    /// # Errors
    ///
    /// Returns an error if any handler fails to process any event.
    pub async fn dispatch_batch(&self, events: Vec<Event>) -> crate::Result<()> {
        for event in events {
            self.dispatch(event).await?;
        }
        Ok(())
    }

    /// Get the number of registered handlers
    pub async fn handler_count(&self) -> usize {
        self.handlers.read().await.len()
    }

    /// Clear all handlers
    pub async fn clear(&self) {
        self.handlers.write().await.clear();
    }
}

impl Default for EventDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for EventDispatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EventDispatcher")
            .field("handlers", &"Arc<RwLock<Vec<...>>>")
            .finish()
    }
}

/// Event persistence storage backend
#[async_trait]
pub trait EventStorage: Send + Sync {
    /// Store an event
    async fn store(&self, event: &Event) -> crate::Result<()>;

    /// Store multiple events
    async fn store_batch(&self, events: &[Event]) -> crate::Result<()> {
        for event in events {
            self.store(event).await?;
        }
        Ok(())
    }

    /// Query events by filter
    async fn query(&self, filter: &EventFilter, limit: Option<usize>) -> crate::Result<Vec<Event>>;

    /// Query events in a time range
    async fn query_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<usize>,
    ) -> crate::Result<Vec<Event>>;

    /// Delete old events
    async fn cleanup(&self, before: DateTime<Utc>) -> crate::Result<usize>;
}

/// File-based event storage (append-only JSON lines)
pub struct FileEventStorage {
    path: PathBuf,
    file_handle: Arc<RwLock<Option<tokio::fs::File>>>,
}

impl FileEventStorage {
    /// Create a new file-based event storage
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            file_handle: Arc::new(RwLock::new(None)),
        }
    }

    /// Initialize the storage file
    ///
    /// # Errors
    ///
    /// Returns an error if the file cannot be opened or created.
    pub async fn init(&self) -> crate::Result<()> {
        let mut handle = self.file_handle.write().await;
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .await
            .map_err(|e| crate::CelersError::Broker(format!("Failed to open event file: {e}")))?;

        *handle = Some(file);
        Ok(())
    }

    /// Read all events from the file
    async fn read_all(&self) -> crate::Result<Vec<Event>> {
        use tokio::io::{AsyncBufReadExt, BufReader};

        let file = tokio::fs::File::open(&self.path)
            .await
            .map_err(|e| crate::CelersError::Broker(format!("Failed to open event file: {e}")))?;

        let reader = BufReader::new(file);
        let mut lines = reader.lines();
        let mut events = Vec::new();

        while let Some(line) = lines
            .next_line()
            .await
            .map_err(|e| crate::CelersError::Broker(format!("Failed to read line: {e}")))?
        {
            if let Ok(event) = serde_json::from_str::<Event>(&line) {
                events.push(event);
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl EventStorage for FileEventStorage {
    async fn store(&self, event: &Event) -> crate::Result<()> {
        use tokio::io::AsyncWriteExt;

        let mut handle = self.file_handle.write().await;
        if handle.is_none() {
            drop(handle);
            self.init().await?;
            handle = self.file_handle.write().await;
        }

        if let Some(file) = handle.as_mut() {
            let json = serde_json::to_string(event)
                .map_err(|e| crate::CelersError::Serialization(e.to_string()))?;

            file.write_all(json.as_bytes())
                .await
                .map_err(|e| crate::CelersError::Broker(format!("Failed to write event: {e}")))?;
            file.write_all(b"\n")
                .await
                .map_err(|e| crate::CelersError::Broker(format!("Failed to write newline: {e}")))?;
            file.flush()
                .await
                .map_err(|e| crate::CelersError::Broker(format!("Failed to flush: {e}")))?;
        }

        Ok(())
    }

    async fn query(&self, filter: &EventFilter, limit: Option<usize>) -> crate::Result<Vec<Event>> {
        let all_events = self.read_all().await?;
        let mut filtered: Vec<Event> = all_events
            .into_iter()
            .filter(|e| filter.matches(e))
            .collect();

        if let Some(limit) = limit {
            filtered.truncate(limit);
        }

        Ok(filtered)
    }

    async fn query_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<usize>,
    ) -> crate::Result<Vec<Event>> {
        let all_events = self.read_all().await?;
        let mut filtered: Vec<Event> = all_events
            .into_iter()
            .filter(|e| {
                let timestamp = e.timestamp();
                timestamp >= start && timestamp <= end
            })
            .collect();

        if let Some(limit) = limit {
            filtered.truncate(limit);
        }

        Ok(filtered)
    }

    async fn cleanup(&self, before: DateTime<Utc>) -> crate::Result<usize> {
        use tokio::io::AsyncWriteExt;

        let all_events = self.read_all().await?;
        let (keep, remove): (Vec<_>, Vec<_>) = all_events
            .into_iter()
            .partition(|e| e.timestamp() >= before);

        let removed_count = remove.len();

        // Rewrite file with only kept events
        let temp_path = self.path.with_extension("tmp");
        let mut temp_file = tokio::fs::File::create(&temp_path)
            .await
            .map_err(|e| crate::CelersError::Broker(format!("Failed to create temp file: {e}")))?;

        for event in keep {
            let json = serde_json::to_string(&event)
                .map_err(|e| crate::CelersError::Serialization(e.to_string()))?;
            temp_file
                .write_all(json.as_bytes())
                .await
                .map_err(|e| crate::CelersError::Broker(format!("Failed to write: {e}")))?;
            temp_file
                .write_all(b"\n")
                .await
                .map_err(|e| crate::CelersError::Broker(format!("Failed to write newline: {e}")))?;
        }

        temp_file
            .flush()
            .await
            .map_err(|e| crate::CelersError::Broker(format!("Failed to flush: {e}")))?;
        drop(temp_file);

        // Replace original file with temp file
        tokio::fs::rename(&temp_path, &self.path)
            .await
            .map_err(|e| crate::CelersError::Broker(format!("Failed to rename file: {e}")))?;

        // Reinitialize file handle
        let mut handle = self.file_handle.write().await;
        *handle = None;
        drop(handle);
        self.init().await?;

        Ok(removed_count)
    }
}

/// In-memory event storage (for testing and development)
#[derive(Clone)]
pub struct InMemoryEventStorage {
    events: Arc<RwLock<Vec<Event>>>,
    max_size: usize,
}

impl InMemoryEventStorage {
    /// Create a new in-memory event storage
    #[must_use]
    pub fn new(max_size: usize) -> Self {
        Self {
            events: Arc::new(RwLock::new(Vec::new())),
            max_size,
        }
    }

    /// Get the number of stored events
    pub async fn len(&self) -> usize {
        self.events.read().await.len()
    }

    /// Check if storage is empty
    pub async fn is_empty(&self) -> bool {
        self.events.read().await.is_empty()
    }

    /// Clear all events
    pub async fn clear(&self) {
        self.events.write().await.clear();
    }
}

#[async_trait]
impl EventStorage for InMemoryEventStorage {
    async fn store(&self, event: &Event) -> crate::Result<()> {
        let mut events = self.events.write().await;
        events.push(event.clone());

        // Trim to max size (FIFO)
        if events.len() > self.max_size {
            let excess = events.len() - self.max_size;
            events.drain(0..excess);
        }

        Ok(())
    }

    async fn query(&self, filter: &EventFilter, limit: Option<usize>) -> crate::Result<Vec<Event>> {
        let events = self.events.read().await;
        let mut filtered: Vec<Event> = events
            .iter()
            .filter(|e| filter.matches(e))
            .cloned()
            .collect();

        if let Some(limit) = limit {
            filtered.truncate(limit);
        }

        Ok(filtered)
    }

    async fn query_range(
        &self,
        start: DateTime<Utc>,
        end: DateTime<Utc>,
        limit: Option<usize>,
    ) -> crate::Result<Vec<Event>> {
        let events = self.events.read().await;
        let mut filtered: Vec<Event> = events
            .iter()
            .filter(|e| {
                let timestamp = e.timestamp();
                timestamp >= start && timestamp <= end
            })
            .cloned()
            .collect();

        if let Some(limit) = limit {
            filtered.truncate(limit);
        }

        Ok(filtered)
    }

    async fn cleanup(&self, before: DateTime<Utc>) -> crate::Result<usize> {
        let mut events = self.events.write().await;
        let original_len = events.len();
        events.retain(|e| e.timestamp() >= before);
        let removed = original_len - events.len();
        Ok(removed)
    }
}

/// Event stream for real-time event delivery
pub struct EventStream {
    receiver: broadcast::Receiver<Event>,
    filter: EventFilter,
}

impl EventStream {
    /// Create a new event stream with a filter
    #[must_use]
    pub fn new(receiver: broadcast::Receiver<Event>, filter: EventFilter) -> Self {
        Self { receiver, filter }
    }

    /// Receive the next matching event
    ///
    /// # Errors
    ///
    /// Returns an error if the receiver is closed or lagged behind.
    pub async fn recv(&mut self) -> Result<Event, broadcast::error::RecvError> {
        loop {
            let event = self.receiver.recv().await?;
            if self.filter.matches(&event) {
                return Ok(event);
            }
        }
    }

    /// Try to receive an event without blocking
    ///
    /// # Errors
    ///
    /// Returns an error if no event is available, the receiver is closed, or lagged behind.
    pub fn try_recv(&mut self) -> Result<Event, broadcast::error::TryRecvError> {
        loop {
            let event = self.receiver.try_recv()?;
            if self.filter.matches(&event) {
                return Ok(event);
            }
        }
    }
}

// Note: Database-backed event storage is available in celers-backend-db crate
// to avoid adding database dependencies to celers-core

/// Event alert condition for triggering notifications
#[derive(Clone)]
pub enum AlertCondition {
    /// Alert on specific event type
    EventType(String),
    /// Alert when task fails
    TaskFailed,
    /// Alert when task exceeds retry count
    TaskRetryExceeded(u32),
    /// Alert when worker goes offline
    WorkerOffline,
    /// Alert when event rate exceeds threshold (events per second)
    RateExceeds { threshold: f64, window_secs: u64 },
    /// Alert on custom condition
    Custom(Arc<dyn Fn(&Event) -> bool + Send + Sync>),
}

impl std::fmt::Debug for AlertCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AlertCondition::EventType(event_type) => {
                f.debug_tuple("EventType").field(event_type).finish()
            }
            AlertCondition::TaskFailed => write!(f, "TaskFailed"),
            AlertCondition::TaskRetryExceeded(max) => {
                f.debug_tuple("TaskRetryExceeded").field(max).finish()
            }
            AlertCondition::WorkerOffline => write!(f, "WorkerOffline"),
            AlertCondition::RateExceeds {
                threshold,
                window_secs,
            } => f
                .debug_struct("RateExceeds")
                .field("threshold", threshold)
                .field("window_secs", window_secs)
                .finish(),
            AlertCondition::Custom(_) => write!(f, "Custom(<closure>)"),
        }
    }
}

impl AlertCondition {
    /// Check if an event triggers this alert condition
    #[must_use]
    pub fn check(&self, event: &Event, context: &AlertContext) -> bool {
        match self {
            AlertCondition::EventType(event_type) => event.event_type() == event_type,
            AlertCondition::TaskFailed => matches!(event, Event::Task(TaskEvent::Failed { .. })),
            AlertCondition::TaskRetryExceeded(max) => {
                if let Event::Task(TaskEvent::Retried { retries, .. }) = event {
                    retries >= max
                } else {
                    false
                }
            }
            AlertCondition::WorkerOffline => {
                matches!(event, Event::Worker(WorkerEvent::Offline { .. }))
            }
            AlertCondition::RateExceeds {
                threshold,
                window_secs,
            } => {
                let rate = context.get_event_rate(*window_secs);
                rate > *threshold
            }
            AlertCondition::Custom(predicate) => predicate(event),
        }
    }
}

/// Context information for alert conditions
#[derive(Debug, Clone, Default)]
pub struct AlertContext {
    /// Recent event timestamps for rate calculation
    recent_events: Arc<RwLock<Vec<DateTime<Utc>>>>,
}

impl AlertContext {
    /// Create a new alert context
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an event timestamp
    pub async fn record_event(&self, timestamp: DateTime<Utc>) {
        let mut events = self.recent_events.write().await;
        events.push(timestamp);

        // Keep only last 1000 events to prevent unbounded growth
        if events.len() > 1000 {
            let excess = events.len() - 1000;
            events.drain(0..excess);
        }
    }

    /// Get event rate (events per second) over a time window
    #[allow(clippy::unused_self)]
    fn get_event_rate(&self, _window_secs: u64) -> f64 {
        // This is a synchronous approximation for rate calculation
        // In practice, you'd use the async version with proper locking
        0.0 // Placeholder - actual implementation would need async context
    }

    #[allow(clippy::cast_possible_wrap, clippy::cast_precision_loss)]
    /// Get event rate (async version)
    pub async fn get_event_rate_async(&self, window_secs: u64) -> f64 {
        let events = self.recent_events.read().await;
        let now = Utc::now();
        let cutoff = now - chrono::Duration::seconds(window_secs as i64);

        let count = events.iter().filter(|&&ts| ts >= cutoff).count();
        count as f64 / window_secs as f64
    }
}

/// Alert severity level
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum AlertSeverity {
    /// Informational alert
    Info,
    /// Warning alert
    Warning,
    /// Error alert
    Error,
    /// Critical alert requiring immediate attention
    Critical,
}

/// Alert triggered by an event
#[derive(Debug, Clone)]
pub struct Alert {
    /// Alert severity
    pub severity: AlertSeverity,
    /// Alert title/summary
    pub title: String,
    /// Alert description
    pub message: String,
    /// Event that triggered the alert
    pub event: Event,
    /// Timestamp when alert was triggered
    pub timestamp: DateTime<Utc>,
    /// Additional metadata
    pub metadata: HashMap<String, String>,
}

impl Alert {
    /// Create a new alert
    pub fn new(
        severity: AlertSeverity,
        title: impl Into<String>,
        message: impl Into<String>,
        event: Event,
    ) -> Self {
        Self {
            severity,
            title: title.into(),
            message: message.into(),
            event,
            timestamp: Utc::now(),
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the alert
    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }
}

/// Alert handler trait
#[async_trait]
pub trait AlertHandler: Send + Sync {
    /// Handle an alert
    async fn handle(&self, alert: &Alert) -> crate::Result<()>;
}

/// Logging alert handler that logs alerts using tracing
#[derive(Debug, Clone, Default)]
pub struct LoggingAlertHandler;

impl LoggingAlertHandler {
    /// Create a new logging alert handler
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

#[async_trait]
impl AlertHandler for LoggingAlertHandler {
    async fn handle(&self, alert: &Alert) -> crate::Result<()> {
        let task_id = alert.event.task_id();
        let hostname = alert.event.hostname();

        match alert.severity {
            AlertSeverity::Info => {
                tracing::info!(
                    severity = "info",
                    title = %alert.title,
                    message = %alert.message,
                    event_type = alert.event.event_type(),
                    task_id = ?task_id,
                    hostname = ?hostname,
                    "Alert triggered"
                );
            }
            AlertSeverity::Warning => {
                tracing::warn!(
                    severity = "warning",
                    title = %alert.title,
                    message = %alert.message,
                    event_type = alert.event.event_type(),
                    task_id = ?task_id,
                    hostname = ?hostname,
                    "Alert triggered"
                );
            }
            AlertSeverity::Error => {
                tracing::error!(
                    severity = "error",
                    title = %alert.title,
                    message = %alert.message,
                    event_type = alert.event.event_type(),
                    task_id = ?task_id,
                    hostname = ?hostname,
                    "Alert triggered"
                );
            }
            AlertSeverity::Critical => {
                tracing::error!(
                    severity = "critical",
                    title = %alert.title,
                    message = %alert.message,
                    event_type = alert.event.event_type(),
                    task_id = ?task_id,
                    hostname = ?hostname,
                    "CRITICAL ALERT"
                );
            }
        }
        Ok(())
    }
}

/// Type alias for alert handler registry entries
type AlertHandlerEntry = (AlertCondition, AlertSeverity, String, Arc<dyn AlertHandler>);

/// Alert manager for monitoring events and triggering alerts
pub struct AlertManager {
    handlers: Arc<RwLock<Vec<AlertHandlerEntry>>>,
    context: AlertContext,
}

impl AlertManager {
    /// Create a new alert manager
    #[must_use]
    pub fn new() -> Self {
        Self {
            handlers: Arc::new(RwLock::new(Vec::new())),
            context: AlertContext::new(),
        }
    }

    /// Register an alert handler
    pub async fn register<H: AlertHandler + 'static>(
        &self,
        condition: AlertCondition,
        severity: AlertSeverity,
        title: impl Into<String>,
        handler: H,
    ) {
        let mut handlers = self.handlers.write().await;
        handlers.push((condition, severity, title.into(), Arc::new(handler)));
    }

    /// Process an event and trigger alerts if conditions match
    ///
    /// # Errors
    ///
    /// Returns an error if any alert handler fails to process the alert.
    pub async fn process_event(&self, event: Event) -> crate::Result<()> {
        // Record event for rate tracking
        self.context.record_event(event.timestamp()).await;

        let handlers = self.handlers.read().await;

        for (condition, severity, title, handler) in handlers.iter() {
            if condition.check(&event, &self.context) {
                let message = format!("Event {} triggered alert condition", event.event_type());
                let alert = Alert::new(*severity, title.clone(), message, event.clone());
                handler.handle(&alert).await?;
            }
        }

        Ok(())
    }

    /// Get the number of registered alert handlers
    pub async fn handler_count(&self) -> usize {
        self.handlers.read().await.len()
    }

    /// Clear all alert handlers
    pub async fn clear(&self) {
        self.handlers.write().await.clear();
    }
}

impl Default for AlertManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Event monitor for collecting statistics
#[derive(Debug, Clone, Default)]
pub struct EventMonitor {
    stats: Arc<RwLock<EventStats>>,
}

#[derive(Debug, Clone, Default)]
pub struct EventStats {
    pub total_events: u64,
    pub task_events: u64,
    pub worker_events: u64,
    pub events_by_type: HashMap<String, u64>,
    pub events_by_hostname: HashMap<String, u64>,
    pub last_event_time: Option<DateTime<Utc>>,
}

impl EventMonitor {
    /// Create a new event monitor
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Record an event
    pub async fn record(&self, event: &Event) {
        let mut stats = self.stats.write().await;

        stats.total_events += 1;
        stats.last_event_time = Some(event.timestamp());

        match event {
            Event::Task(_) => stats.task_events += 1,
            Event::Worker(_) => stats.worker_events += 1,
        }

        let event_type = event.event_type().to_string();
        *stats.events_by_type.entry(event_type).or_insert(0) += 1;

        if let Some(hostname) = match event {
            Event::Task(task_event) => match task_event {
                TaskEvent::Received { hostname, .. }
                | TaskEvent::Started { hostname, .. }
                | TaskEvent::Succeeded { hostname, .. }
                | TaskEvent::Failed { hostname, .. }
                | TaskEvent::Retried { hostname, .. }
                | TaskEvent::Rejected { hostname, .. } => Some(hostname.clone()),
                _ => None,
            },
            Event::Worker(worker_event) => match worker_event {
                WorkerEvent::Online { hostname, .. }
                | WorkerEvent::Offline { hostname, .. }
                | WorkerEvent::Heartbeat { hostname, .. } => Some(hostname.clone()),
            },
        } {
            *stats.events_by_hostname.entry(hostname).or_insert(0) += 1;
        }
    }

    /// Get current statistics
    pub async fn get_stats(&self) -> EventStats {
        self.stats.read().await.clone()
    }

    /// Reset statistics
    pub async fn reset(&self) {
        let mut stats = self.stats.write().await;
        *stats = EventStats::default();
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
            timestamp: DateTime::parse_from_rfc3339("2026-01-01T12:00:00Z")
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
