//! Real-time event types for task and worker lifecycle
//!
//! [`Event`] is CeleRS' **typed internal model** of the task and worker
//! lifecycle: strongly typed variants, `DateTime<Utc>` timestamps and Rust
//! field names (`task_id`, `task_name`). It is what workers pass around
//! in-process, what [`EventFilter`] and [`EventDispatcher`] match on, and what
//! the event persisters store.
//!
//! It is **not** the on-the-wire shape. A Celery monitor (`celery events`,
//! Flower, a custom consumer) expects float Unix timestamps and Celery's own
//! field names (`uuid`, `name`), plus the `clock`/`utcoffset`/`pid` envelope.
//! That shape is produced by [`Event::to_wire_json`] and parsed back by
//! [`Event::from_wire_str`]; see the [`wire`] module for the encoding and
//! [`message`] for the typed field mapping. It is exactly the shape
//! `celers_protocol::event::EventMessage` describes — the two are connected by
//! real `From`/`TryFrom` impls, not by a string-keyed rename — and
//! it is what every network transport in the workspace
//! (`celers_backend_redis::event_transport`,
//! `celers_broker_amqp::event_transport`) puts on the wire.
//!
//! In short: **typed model in memory and in storage, Celery wire shape on the
//! network.** Do not hand `serde_json::to_string(&event)` to a monitor — that
//! is the internal shape.
//!
//! [`Event::from_wire_str`] reads both directions of a mixed cluster: events
//! CeleRS published and events a real Python Celery worker published on the
//! same `celeryev` channel. See [`wire`] for the two fields Celery omits and
//! the defaults they come back with.
//!
//! # Event Types
//!
//! ## Task Events
//! - `task-sent` - Task was sent to the queue
//! - `task-received` - Task was received by a worker
//! - `task-started` - Task execution started
//! - `task-succeeded` - Task completed successfully
//! - `task-failed` - Task execution failed
//! - `task-retried` - Task is being retried
//! - `task-revoked` - Task was revoked/cancelled
//! - `task-rejected` - Task was rejected by worker
//! - `task-soft-time-limit-exceeded` - Task passed its soft time limit
//!   (a CeleRS extension: Celery has no such event, so a Celery monitor sees
//!   it as an unknown/custom event type rather than failing to parse)
//!
//! ## Worker Events
//! - `worker-online` - Worker came online
//! - `worker-offline` - Worker going offline
//! - `worker-heartbeat` - Periodic worker heartbeat
//!
//! # Example
//!
//! ```rust
//! use celers_core::event::{Event, TaskEvent};
//! use celers_core::event::wire::event_timestamp_now;
//! use uuid::Uuid;
//!
//! // Create a task started event
//! let task_id = Uuid::new_v4();
//! let event = Event::Task(TaskEvent::Started {
//!     task_id,
//!     task_name: "my_task".to_string(),
//!     hostname: "worker-1".to_string(),
//!     timestamp: event_timestamp_now(),
//!     pid: std::process::id(),
//! });
//!
//! // Render the Celery-compatible wire shape a monitor can parse.
//! let json = event.to_wire_json().unwrap();
//! assert!(json.contains(r#""type":"task-started""#));
//! assert!(json.contains(&format!(r#""uuid":"{task_id}""#)));
//!
//! // ...and read it back into the typed model.
//! assert_eq!(Event::from_wire_str(&json).unwrap(), event);
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub mod message;
pub mod wire;

#[cfg(test)]
pub(crate) mod test_events;

pub use wire::{
    adjust_event_clock, current_event_clock, event_timestamp_now, forward_event_clock,
    from_wire_timestamp, to_wire_timestamp, truncate_to_wire_precision, EventEnvelope,
};

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
        #[serde(default)]
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
        ///
        /// Python Celery omits `name` on every event after `task-received`
        /// (a monitor correlates by `uuid`), so an inbound Celery event
        /// deserializes with this empty. CeleRS always emits it.
        #[serde(default)]
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
        ///
        /// Python Celery omits `name` on every event after `task-received`
        /// (a monitor correlates by `uuid`), so an inbound Celery event
        /// deserializes with this empty. CeleRS always emits it.
        #[serde(default)]
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
        ///
        /// Python Celery omits `name` on every event after `task-received`
        /// (a monitor correlates by `uuid`), so an inbound Celery event
        /// deserializes with this empty. CeleRS always emits it.
        #[serde(default)]
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
        ///
        /// Python Celery omits `name` on every event after `task-received`
        /// (a monitor correlates by `uuid`), so an inbound Celery event
        /// deserializes with this empty. CeleRS always emits it.
        #[serde(default)]
        task_name: String,
        /// Worker hostname
        hostname: String,
        /// Event timestamp
        timestamp: DateTime<Utc>,
        /// Exception that caused retry
        exception: String,
        /// Current retry attempt number
        ///
        /// Python Celery's `task-retried` carries no retry count, so an
        /// inbound Celery event deserializes with this at `0`.
        #[serde(default)]
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
        /// Worker hostname
        ///
        /// Python Celery's `task-revoked` is emitted by the worker through the
        /// event dispatcher, so it carries `hostname` like every other worker
        /// event — a monitor uses it to tell *which* worker dropped the task.
        /// CeleRS always emits it.
        ///
        /// It is `#[serde(default)]` because a payload written by CeleRS 0.3.0
        /// (before this field existed) carries no `hostname` of its own, and
        /// during a rolling upgrade rejecting those outright would lose the
        /// revocation entirely rather than lose one field of it.
        #[serde(default)]
        hostname: String,
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
        ///
        /// Python Celery's `task-rejected` reports only `requeue`, so an
        /// inbound Celery event deserializes with this empty.
        #[serde(default)]
        reason: String,
    },

    /// Task passed its soft time limit and was asked to wrap up
    ///
    /// A `CeleRS` extension: Celery raises `SoftTimeLimitExceeded` inside the
    /// task but publishes no event for it, so a monitor sees this as a custom
    /// event type. The task is still running — the hard time limit, not this
    /// event, is what ends it.
    #[serde(rename = "task-soft-time-limit-exceeded")]
    SoftTimeLimitExceeded {
        /// Unique task ID
        task_id: Uuid,
        /// Task name
        #[serde(default)]
        task_name: String,
        /// Worker hostname
        hostname: String,
        /// Event timestamp
        timestamp: DateTime<Utc>,
        /// Seconds the task had been running when the limit tripped
        elapsed_secs: f64,
        /// The configured soft limit, in seconds
        limit_secs: f64,
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
            Event::Task(TaskEvent::SoftTimeLimitExceeded { .. }) => "task-soft-time-limit-exceeded",
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
                | TaskEvent::Rejected { timestamp, .. }
                | TaskEvent::SoftTimeLimitExceeded { timestamp, .. } => *timestamp,
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
                | TaskEvent::Rejected { task_id, .. }
                | TaskEvent::SoftTimeLimitExceeded { task_id, .. } => *task_id,
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
                | TaskEvent::Revoked { hostname, .. }
                | TaskEvent::Rejected { hostname, .. }
                | TaskEvent::SoftTimeLimitExceeded { hostname, .. } => Some(hostname),
                TaskEvent::Sent { .. } => None,
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
            timestamp: wire::event_timestamp_now(),
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
            timestamp: wire::event_timestamp_now(),
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
            timestamp: wire::event_timestamp_now(),
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
            timestamp: wire::event_timestamp_now(),
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
            timestamp: wire::event_timestamp_now(),
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
            timestamp: wire::event_timestamp_now(),
            exception: exception.into(),
            retries,
        })
    }

    /// Build a task-soft-time-limit-exceeded event
    ///
    /// `elapsed` is how long the task had been running when the limit tripped
    /// and `limit` is the configured soft limit; both are reported in seconds.
    #[must_use]
    pub fn soft_time_limit_exceeded(
        self,
        elapsed: std::time::Duration,
        limit: std::time::Duration,
    ) -> Event {
        Event::Task(TaskEvent::SoftTimeLimitExceeded {
            task_id: self.task_id,
            task_name: self.task_name,
            hostname: self.hostname.unwrap_or_else(|| "unknown".to_string()),
            timestamp: wire::event_timestamp_now(),
            elapsed_secs: elapsed.as_secs_f64(),
            limit_secs: limit.as_secs_f64(),
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
            timestamp: wire::event_timestamp_now(),
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
            timestamp: wire::event_timestamp_now(),
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
            timestamp: wire::event_timestamp_now(),
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

/// Combine the failures collected during a best-effort fan-out into a single
/// error, or return `Ok(())` when every sink succeeded.
fn aggregate_failures(what: &str, failures: Vec<String>) -> crate::Result<()> {
    if failures.is_empty() {
        return Ok(());
    }
    Err(crate::CelersError::Other(format!(
        "{} of {what} failed: {}",
        failures.len(),
        failures.join("; ")
    )))
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
    /// Emit to every enabled sink.
    ///
    /// Fan-out is best-effort: one failing sink (a full disk, a closed socket)
    /// must never starve the others of the event, so every emitter is attempted
    /// and failures are aggregated into a single error afterwards.
    async fn emit(&self, event: Event) -> crate::Result<()> {
        let mut failures: Vec<String> = Vec::new();
        for (index, emitter) in self.emitters.iter().enumerate() {
            if emitter.is_enabled() {
                if let Err(e) = emitter.emit(event.clone()).await {
                    tracing::warn!(
                        emitter_index = index,
                        event_type = event.event_type(),
                        error = %e,
                        "Event emitter failed; continuing with the remaining emitters"
                    );
                    failures.push(format!("emitter[{index}]: {e}"));
                }
            }
        }
        aggregate_failures("event emitters", failures)
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
                        | TaskEvent::Retried { task_name, .. }
                        | TaskEvent::SoftTimeLimitExceeded { task_name, .. } => task_name == name,
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
                        | TaskEvent::Revoked { hostname, .. }
                        | TaskEvent::Rejected { hostname, .. }
                        | TaskEvent::SoftTimeLimitExceeded { hostname, .. } => Some(hostname),
                        TaskEvent::Sent { .. } => None,
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
    /// Dispatch is best-effort: every matching handler runs even if an earlier
    /// one fails, so a single misbehaving consumer cannot suppress the event for
    /// metrics, audit persistence or alerting.
    ///
    /// # Errors
    ///
    /// Returns an aggregate error *after* all handlers have been attempted if
    /// one or more of them failed.
    pub async fn dispatch(&self, event: Event) -> crate::Result<()> {
        let handlers = self.handlers.read().await;

        let mut failures: Vec<String> = Vec::new();
        for (index, (filter, handler)) in handlers.iter().enumerate() {
            if filter.matches(&event) {
                if let Err(e) = handler(event.clone()).await {
                    tracing::warn!(
                        handler_index = index,
                        event_type = event.event_type(),
                        error = %e,
                        "Event handler failed; continuing with the remaining handlers"
                    );
                    failures.push(format!("handler[{index}]: {e}"));
                }
            }
        }

        aggregate_failures("event handlers", failures)
    }

    /// Dispatch events in batch
    ///
    /// Every event is dispatched even if an earlier one had a failing handler.
    ///
    /// # Errors
    ///
    /// Returns an aggregate error after all events have been dispatched if one
    /// or more handlers failed.
    pub async fn dispatch_batch(&self, events: Vec<Event>) -> crate::Result<()> {
        let mut failures: Vec<String> = Vec::new();
        for event in events {
            if let Err(e) = self.dispatch(event).await {
                failures.push(e.to_string());
            }
        }
        aggregate_failures("dispatched events", failures)
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

mod file_storage;

pub use file_storage::FileEventStorage;

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

mod monitoring;

pub use monitoring::{
    Alert, AlertCondition, AlertContext, AlertHandler, AlertManager, AlertSeverity, EventMonitor,
    EventStats, LoggingAlertHandler, MAX_TRACKED_HOSTNAMES, OTHER_HOSTNAMES_BUCKET,
    RATE_WINDOW_BUCKETS,
};

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
            timestamp: wire::event_timestamp_now(),
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
            timestamp: wire::event_timestamp_now(),
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

    /// Emitter that always fails, used to prove fan-out is best-effort.
    #[derive(Debug, Default)]
    struct FailingEmitter;

    #[async_trait]
    impl EventEmitter for FailingEmitter {
        async fn emit(&self, _event: Event) -> crate::Result<()> {
            Err(crate::CelersError::Other("sink is down".to_string()))
        }
    }

    /// Emitter that records everything it receives.
    #[derive(Debug, Default, Clone)]
    struct CollectingEmitter {
        seen: Arc<RwLock<Vec<Event>>>,
    }

    #[async_trait]
    impl EventEmitter for CollectingEmitter {
        async fn emit(&self, event: Event) -> crate::Result<()> {
            self.seen.write().await.push(event);
            Ok(())
        }
    }

    fn sample_event() -> Event {
        Event::Task(TaskEvent::Started {
            task_id: Uuid::new_v4(),
            task_name: "t".to_string(),
            hostname: "w1".to_string(),
            timestamp: wire::event_timestamp_now(),
            pid: 1,
        })
    }

    #[tokio::test]
    async fn test_composite_emitter_is_best_effort() {
        // Regression: the first failing emitter aborted the fan-out, starving
        // every later sink (metrics, audit trail, alerting) of the event.
        let collector = CollectingEmitter::default();
        let composite = CompositeEventEmitter::new()
            .with_emitter(FailingEmitter)
            .add_arc(Arc::new(collector.clone()));

        let err = composite.emit(sample_event()).await.unwrap_err();
        assert!(err.to_string().contains("sink is down"));
        assert_eq!(collector.seen.read().await.len(), 1);
    }

    #[tokio::test]
    async fn test_dispatcher_is_best_effort() {
        let dispatcher = EventDispatcher::new();
        dispatcher
            .register(EventFilter::All, |_event| async {
                Err(crate::CelersError::Other("handler exploded".to_string()))
            })
            .await;

        let seen = Arc::new(RwLock::new(0usize));
        let seen_clone = Arc::clone(&seen);
        dispatcher
            .register(EventFilter::All, move |_event| {
                let seen = Arc::clone(&seen_clone);
                async move {
                    *seen.write().await += 1;
                    Ok(())
                }
            })
            .await;

        let err = dispatcher.dispatch(sample_event()).await.unwrap_err();
        assert!(err.to_string().contains("handler exploded"));
        assert_eq!(*seen.read().await, 1);
    }
}
