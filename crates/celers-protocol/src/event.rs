//! Celery event message format
//!
//! This module provides Celery-compatible event messages for task lifecycle
//! and worker state events.
//!
//! # Event Types
//!
//! ## Task Events
//! - `task-sent` - Task was sent to a queue
//! - `task-received` - Task was received by a worker
//! - `task-started` - Task execution started
//! - `task-succeeded` - Task completed successfully
//! - `task-failed` - Task execution failed
//! - `task-rejected` - Task was rejected by a worker
//! - `task-revoked` - Task was revoked
//! - `task-retried` - Task is being retried
//!
//! ## Worker Events
//! - `worker-online` - Worker came online
//! - `worker-offline` - Worker went offline
//! - `worker-heartbeat` - Worker heartbeat

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Event type enumeration
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum EventType {
    // Task events
    TaskSent,
    TaskReceived,
    TaskStarted,
    TaskSucceeded,
    TaskFailed,
    TaskRejected,
    TaskRevoked,
    TaskRetried,

    // Worker events
    WorkerOnline,
    WorkerOffline,
    WorkerHeartbeat,

    // Custom event type
    #[serde(untagged)]
    Custom(String),
}

impl EventType {
    /// Get the event type string
    #[inline]
    pub fn as_str(&self) -> &str {
        match self {
            EventType::TaskSent => "task-sent",
            EventType::TaskReceived => "task-received",
            EventType::TaskStarted => "task-started",
            EventType::TaskSucceeded => "task-succeeded",
            EventType::TaskFailed => "task-failed",
            EventType::TaskRejected => "task-rejected",
            EventType::TaskRevoked => "task-revoked",
            EventType::TaskRetried => "task-retried",
            EventType::WorkerOnline => "worker-online",
            EventType::WorkerOffline => "worker-offline",
            EventType::WorkerHeartbeat => "worker-heartbeat",
            EventType::Custom(s) => s,
        }
    }

    /// Check if this is a task event
    #[inline]
    pub fn is_task_event(&self) -> bool {
        matches!(
            self,
            EventType::TaskSent
                | EventType::TaskReceived
                | EventType::TaskStarted
                | EventType::TaskSucceeded
                | EventType::TaskFailed
                | EventType::TaskRejected
                | EventType::TaskRevoked
                | EventType::TaskRetried
        )
    }

    /// Check if this is a worker event
    #[inline]
    pub fn is_worker_event(&self) -> bool {
        matches!(
            self,
            EventType::WorkerOnline | EventType::WorkerOffline | EventType::WorkerHeartbeat
        )
    }
}

impl std::fmt::Display for EventType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for EventType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "task-sent" => Ok(EventType::TaskSent),
            "task-received" => Ok(EventType::TaskReceived),
            "task-started" => Ok(EventType::TaskStarted),
            "task-succeeded" => Ok(EventType::TaskSucceeded),
            "task-failed" => Ok(EventType::TaskFailed),
            "task-rejected" => Ok(EventType::TaskRejected),
            "task-revoked" => Ok(EventType::TaskRevoked),
            "task-retried" => Ok(EventType::TaskRetried),
            "worker-online" => Ok(EventType::WorkerOnline),
            "worker-offline" => Ok(EventType::WorkerOffline),
            "worker-heartbeat" => Ok(EventType::WorkerHeartbeat),
            other => Ok(EventType::Custom(other.to_string())),
        }
    }
}

/// Base event message structure
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventMessage {
    /// Event type
    #[serde(rename = "type")]
    pub event_type: String,

    /// Timestamp when event occurred
    pub timestamp: f64,

    /// Hostname of the sender
    #[serde(skip_serializing_if = "Option::is_none")]
    pub hostname: Option<String>,

    /// UTC offset
    #[serde(skip_serializing_if = "Option::is_none")]
    pub utcoffset: Option<i32>,

    /// Process ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pid: Option<u32>,

    /// Clock value (for ordering)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clock: Option<u64>,

    /// Additional event-specific fields
    #[serde(flatten)]
    pub fields: HashMap<String, serde_json::Value>,
}

impl EventMessage {
    /// Create a new event message
    pub fn new(event_type: EventType) -> Self {
        Self {
            event_type: event_type.as_str().to_string(),
            timestamp: Utc::now().timestamp() as f64
                + (Utc::now().timestamp_subsec_nanos() as f64 / 1_000_000_000.0),
            hostname: None,
            utcoffset: Some(0),
            pid: None,
            clock: None,
            fields: HashMap::new(),
        }
    }

    /// Create an event with a specific timestamp
    pub fn with_timestamp(mut self, timestamp: DateTime<Utc>) -> Self {
        self.timestamp = timestamp.timestamp() as f64
            + (timestamp.timestamp_subsec_nanos() as f64 / 1_000_000_000.0);
        self
    }

    /// Set the hostname
    pub fn with_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.hostname = Some(hostname.into());
        self
    }

    /// Set the process ID
    pub fn with_pid(mut self, pid: u32) -> Self {
        self.pid = Some(pid);
        self
    }

    /// Set the clock value
    pub fn with_clock(mut self, clock: u64) -> Self {
        self.clock = Some(clock);
        self
    }

    /// Add a custom field
    pub fn with_field(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.fields.insert(key.into(), value);
        self
    }

    /// Get the event type
    pub fn get_type(&self) -> &str {
        &self.event_type
    }

    /// Get the timestamp as DateTime
    pub fn get_datetime(&self) -> DateTime<Utc> {
        DateTime::from_timestamp(
            self.timestamp as i64,
            ((self.timestamp.fract()) * 1_000_000_000.0) as u32,
        )
        .unwrap_or_else(Utc::now)
    }

    /// Serialize to JSON bytes
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes
    pub fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

impl From<EventType> for EventMessage {
    fn from(event_type: EventType) -> Self {
        Self::new(event_type)
    }
}

/// Task-specific event data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TaskEvent {
    /// Base event
    #[serde(flatten)]
    pub base: EventMessage,

    /// Task ID (UUID)
    pub uuid: Uuid,

    /// Task name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    /// Task arguments (as JSON string)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<String>,

    /// Task keyword arguments (as JSON string)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kwargs: Option<String>,

    /// Retry count
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,

    /// ETA for delayed tasks
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eta: Option<String>,

    /// Task expiration
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires: Option<String>,

    /// Parent task ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<Uuid>,

    /// Root task ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_id: Option<Uuid>,

    /// Result (for task-succeeded)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,

    /// Exception type (for task-failed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exception: Option<String>,

    /// Traceback (for task-failed)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub traceback: Option<String>,

    /// Runtime in seconds (for task-succeeded)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub runtime: Option<f64>,

    /// Queue name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,

    /// Exchange name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange: Option<String>,

    /// Routing key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routing_key: Option<String>,
}

impl TaskEvent {
    /// Create a new task event
    pub fn new(event_type: EventType, task_id: Uuid) -> Self {
        Self {
            base: EventMessage::new(event_type),
            uuid: task_id,
            name: None,
            args: None,
            kwargs: None,
            retries: None,
            eta: None,
            expires: None,
            parent_id: None,
            root_id: None,
            result: None,
            exception: None,
            traceback: None,
            runtime: None,
            queue: None,
            exchange: None,
            routing_key: None,
        }
    }

    /// Create a task-sent event
    pub fn sent(task_id: Uuid, task_name: &str) -> Self {
        Self {
            name: Some(task_name.to_string()),
            ..Self::new(EventType::TaskSent, task_id)
        }
    }

    /// Create a task-received event
    pub fn received(task_id: Uuid, task_name: &str) -> Self {
        Self {
            name: Some(task_name.to_string()),
            ..Self::new(EventType::TaskReceived, task_id)
        }
    }

    /// Create a task-started event
    pub fn started(task_id: Uuid, task_name: &str) -> Self {
        Self {
            name: Some(task_name.to_string()),
            ..Self::new(EventType::TaskStarted, task_id)
        }
    }

    /// Create a task-succeeded event
    pub fn succeeded(task_id: Uuid, result: serde_json::Value, runtime: f64) -> Self {
        Self {
            result: Some(result),
            runtime: Some(runtime),
            ..Self::new(EventType::TaskSucceeded, task_id)
        }
    }

    /// Create a task-failed event
    pub fn failed(task_id: Uuid, exception: &str, traceback: Option<&str>) -> Self {
        Self {
            exception: Some(exception.to_string()),
            traceback: traceback.map(|s| s.to_string()),
            ..Self::new(EventType::TaskFailed, task_id)
        }
    }

    /// Create a task-retried event
    pub fn retried(task_id: Uuid, exception: &str, retries: u32) -> Self {
        Self {
            exception: Some(exception.to_string()),
            retries: Some(retries),
            ..Self::new(EventType::TaskRetried, task_id)
        }
    }

    /// Create a task-revoked event
    pub fn revoked(task_id: Uuid, terminated: bool, signum: Option<i32>) -> Self {
        let mut event = Self::new(EventType::TaskRevoked, task_id);
        event
            .base
            .fields
            .insert("terminated".to_string(), serde_json::json!(terminated));
        if let Some(sig) = signum {
            event
                .base
                .fields
                .insert("signum".to_string(), serde_json::json!(sig));
        }
        event
    }

    /// Create a task-rejected event
    pub fn rejected(task_id: Uuid, requeue: bool) -> Self {
        let mut event = Self::new(EventType::TaskRejected, task_id);
        event
            .base
            .fields
            .insert("requeue".to_string(), serde_json::json!(requeue));
        event
    }

    /// Set task name
    pub fn with_name(mut self, name: impl Into<String>) -> Self {
        self.name = Some(name.into());
        self
    }

    /// Set task args
    pub fn with_args(mut self, args: impl Into<String>) -> Self {
        self.args = Some(args.into());
        self
    }

    /// Set task kwargs
    pub fn with_kwargs(mut self, kwargs: impl Into<String>) -> Self {
        self.kwargs = Some(kwargs.into());
        self
    }

    /// Set hostname
    pub fn with_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.base.hostname = Some(hostname.into());
        self
    }

    /// Set queue
    pub fn with_queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Set parent task ID
    pub fn with_parent(mut self, parent_id: Uuid) -> Self {
        self.parent_id = Some(parent_id);
        self
    }

    /// Set root task ID
    pub fn with_root(mut self, root_id: Uuid) -> Self {
        self.root_id = Some(root_id);
        self
    }

    /// Serialize to JSON bytes
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes
    pub fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

/// Worker-specific event data
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct WorkerEvent {
    /// Base event
    #[serde(flatten)]
    pub base: EventMessage,

    /// Software name/version (e.g., "celers-worker v0.1.0")
    #[serde(rename = "sw_ident", skip_serializing_if = "Option::is_none")]
    pub software_identity: Option<String>,

    /// Software version
    #[serde(rename = "sw_ver", skip_serializing_if = "Option::is_none")]
    pub software_version: Option<String>,

    /// Software system (e.g., "Linux")
    #[serde(rename = "sw_sys", skip_serializing_if = "Option::is_none")]
    pub software_system: Option<String>,

    /// Number of active tasks
    #[serde(skip_serializing_if = "Option::is_none")]
    pub active: Option<u32>,

    /// Number of processed tasks
    #[serde(skip_serializing_if = "Option::is_none")]
    pub processed: Option<u64>,

    /// Load average (1, 5, 15 minutes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub loadavg: Option<[f64; 3]>,

    /// Frequency of heartbeats (seconds)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub freq: Option<f64>,
}

impl WorkerEvent {
    /// Create a new worker event
    pub fn new(event_type: EventType) -> Self {
        Self {
            base: EventMessage::new(event_type),
            software_identity: None,
            software_version: None,
            software_system: None,
            active: None,
            processed: None,
            loadavg: None,
            freq: None,
        }
    }

    /// Create a worker-online event
    pub fn online(hostname: &str) -> Self {
        Self {
            base: EventMessage::new(EventType::WorkerOnline).with_hostname(hostname),
            ..Self::new(EventType::WorkerOnline)
        }
    }

    /// Create a worker-offline event
    pub fn offline(hostname: &str) -> Self {
        Self {
            base: EventMessage::new(EventType::WorkerOffline).with_hostname(hostname),
            ..Self::new(EventType::WorkerOffline)
        }
    }

    /// Create a worker-heartbeat event
    pub fn heartbeat(hostname: &str, active: u32, processed: u64) -> Self {
        Self {
            base: EventMessage::new(EventType::WorkerHeartbeat).with_hostname(hostname),
            active: Some(active),
            processed: Some(processed),
            ..Self::new(EventType::WorkerHeartbeat)
        }
    }

    /// Set software identity
    pub fn with_software(mut self, identity: &str, version: &str, system: &str) -> Self {
        self.software_identity = Some(identity.to_string());
        self.software_version = Some(version.to_string());
        self.software_system = Some(system.to_string());
        self
    }

    /// Set load average
    pub fn with_loadavg(mut self, loadavg: [f64; 3]) -> Self {
        self.loadavg = Some(loadavg);
        self
    }

    /// Set heartbeat frequency
    pub fn with_freq(mut self, freq: f64) -> Self {
        self.freq = Some(freq);
        self
    }

    /// Serialize to JSON bytes
    pub fn to_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize from JSON bytes
    pub fn from_json(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_event_type_as_str() {
        assert_eq!(EventType::TaskSent.as_str(), "task-sent");
        assert_eq!(EventType::TaskSucceeded.as_str(), "task-succeeded");
        assert_eq!(EventType::WorkerOnline.as_str(), "worker-online");
    }

    #[test]
    fn test_event_type_is_task_event() {
        assert!(EventType::TaskSent.is_task_event());
        assert!(EventType::TaskFailed.is_task_event());
        assert!(!EventType::WorkerOnline.is_task_event());
    }

    #[test]
    fn test_event_type_is_worker_event() {
        assert!(EventType::WorkerOnline.is_worker_event());
        assert!(EventType::WorkerHeartbeat.is_worker_event());
        assert!(!EventType::TaskSent.is_worker_event());
    }

    #[test]
    fn test_event_type_display() {
        assert_eq!(EventType::TaskStarted.to_string(), "task-started");
    }

    #[test]
    fn test_event_type_from_str() {
        use std::str::FromStr;

        assert_eq!(
            EventType::from_str("task-sent").unwrap(),
            EventType::TaskSent
        );
        assert_eq!(
            EventType::from_str("task-received").unwrap(),
            EventType::TaskReceived
        );
        assert_eq!(
            EventType::from_str("task-started").unwrap(),
            EventType::TaskStarted
        );
        assert_eq!(
            EventType::from_str("task-succeeded").unwrap(),
            EventType::TaskSucceeded
        );
        assert_eq!(
            EventType::from_str("task-failed").unwrap(),
            EventType::TaskFailed
        );
        assert_eq!(
            EventType::from_str("task-rejected").unwrap(),
            EventType::TaskRejected
        );
        assert_eq!(
            EventType::from_str("task-revoked").unwrap(),
            EventType::TaskRevoked
        );
        assert_eq!(
            EventType::from_str("task-retried").unwrap(),
            EventType::TaskRetried
        );
        assert_eq!(
            EventType::from_str("worker-online").unwrap(),
            EventType::WorkerOnline
        );
        assert_eq!(
            EventType::from_str("worker-offline").unwrap(),
            EventType::WorkerOffline
        );
        assert_eq!(
            EventType::from_str("worker-heartbeat").unwrap(),
            EventType::WorkerHeartbeat
        );

        // Test custom event type
        match EventType::from_str("custom-event").unwrap() {
            EventType::Custom(s) => assert_eq!(s, "custom-event"),
            _ => panic!("Expected Custom variant"),
        }
    }

    #[test]
    fn test_event_message_creation() {
        let event = EventMessage::new(EventType::TaskSent).with_hostname("worker-1");

        assert_eq!(event.event_type, "task-sent");
        assert_eq!(event.hostname, Some("worker-1".to_string()));
        assert!(event.timestamp > 0.0);
    }

    #[test]
    fn test_event_message_json() {
        let event = EventMessage::new(EventType::TaskStarted)
            .with_hostname("host-1")
            .with_pid(12345)
            .with_clock(100);

        let json_bytes = event.to_json().unwrap();
        let decoded = EventMessage::from_json(&json_bytes).unwrap();

        assert_eq!(decoded.event_type, "task-started");
        assert_eq!(decoded.hostname, Some("host-1".to_string()));
        assert_eq!(decoded.pid, Some(12345));
        assert_eq!(decoded.clock, Some(100));
    }

    #[test]
    fn test_task_event_sent() {
        let task_id = Uuid::new_v4();
        let event = TaskEvent::sent(task_id, "tasks.add")
            .with_args("[1, 2]")
            .with_kwargs("{}")
            .with_hostname("worker-1")
            .with_queue("celery");

        assert_eq!(event.base.event_type, "task-sent");
        assert_eq!(event.uuid, task_id);
        assert_eq!(event.name, Some("tasks.add".to_string()));
        assert_eq!(event.args, Some("[1, 2]".to_string()));
        assert_eq!(event.queue, Some("celery".to_string()));
    }

    #[test]
    fn test_task_event_succeeded() {
        let task_id = Uuid::new_v4();
        let event = TaskEvent::succeeded(task_id, json!(42), 0.123);

        assert_eq!(event.base.event_type, "task-succeeded");
        assert_eq!(event.result, Some(json!(42)));
        assert_eq!(event.runtime, Some(0.123));
    }

    #[test]
    fn test_task_event_failed() {
        let task_id = Uuid::new_v4();
        let event = TaskEvent::failed(task_id, "ValueError: bad input", Some("traceback..."));

        assert_eq!(event.base.event_type, "task-failed");
        assert_eq!(event.exception, Some("ValueError: bad input".to_string()));
        assert_eq!(event.traceback, Some("traceback...".to_string()));
    }

    #[test]
    fn test_task_event_retried() {
        let task_id = Uuid::new_v4();
        let event = TaskEvent::retried(task_id, "Timeout", 3);

        assert_eq!(event.base.event_type, "task-retried");
        assert_eq!(event.exception, Some("Timeout".to_string()));
        assert_eq!(event.retries, Some(3));
    }

    #[test]
    fn test_task_event_revoked() {
        let task_id = Uuid::new_v4();
        let event = TaskEvent::revoked(task_id, true, Some(9));

        assert_eq!(event.base.event_type, "task-revoked");
        assert_eq!(event.base.fields.get("terminated"), Some(&json!(true)));
        assert_eq!(event.base.fields.get("signum"), Some(&json!(9)));
    }

    #[test]
    fn test_task_event_json_round_trip() {
        let task_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();

        let event = TaskEvent::started(task_id, "tasks.process")
            .with_hostname("worker-2")
            .with_parent(parent_id)
            .with_queue("high-priority");

        let json_bytes = event.to_json().unwrap();
        let decoded = TaskEvent::from_json(&json_bytes).unwrap();

        assert_eq!(decoded.uuid, task_id);
        assert_eq!(decoded.name, Some("tasks.process".to_string()));
        assert_eq!(decoded.parent_id, Some(parent_id));
    }

    #[test]
    fn test_worker_event_online() {
        let event =
            WorkerEvent::online("worker@host").with_software("celers-worker", "0.1.0", "Linux");

        assert_eq!(event.base.event_type, "worker-online");
        assert_eq!(event.base.hostname, Some("worker@host".to_string()));
        assert_eq!(event.software_identity, Some("celers-worker".to_string()));
        assert_eq!(event.software_version, Some("0.1.0".to_string()));
        assert_eq!(event.software_system, Some("Linux".to_string()));
    }

    #[test]
    fn test_worker_event_heartbeat() {
        let event = WorkerEvent::heartbeat("worker@host", 5, 1000)
            .with_loadavg([0.5, 0.7, 0.9])
            .with_freq(2.0);

        assert_eq!(event.base.event_type, "worker-heartbeat");
        assert_eq!(event.active, Some(5));
        assert_eq!(event.processed, Some(1000));
        assert_eq!(event.loadavg, Some([0.5, 0.7, 0.9]));
        assert_eq!(event.freq, Some(2.0));
    }

    #[test]
    fn test_worker_event_json_round_trip() {
        let event = WorkerEvent::heartbeat("worker-1", 3, 500);

        let json_bytes = event.to_json().unwrap();
        let decoded = WorkerEvent::from_json(&json_bytes).unwrap();

        assert_eq!(decoded.base.event_type, "worker-heartbeat");
        assert_eq!(decoded.active, Some(3));
        assert_eq!(decoded.processed, Some(500));
    }

    #[test]
    fn test_event_message_from_event_type() {
        let event: EventMessage = EventType::TaskSent.into();
        assert_eq!(event.event_type, "task-sent");
        assert!(event.timestamp > 0.0);

        let event2: EventMessage = EventType::WorkerOnline.into();
        assert_eq!(event2.event_type, "worker-online");
    }

    #[test]
    fn test_event_message_equality() {
        let event1 = EventMessage::new(EventType::TaskSent)
            .with_hostname("host-1")
            .with_pid(123);
        let event3 = EventMessage::new(EventType::TaskSent)
            .with_hostname("host-2")
            .with_pid(123);

        // Test that the same event equals itself (cloning preserves all fields including timestamp)
        assert_eq!(event1, event1.clone());

        // Verify different hostnames result in different events
        assert_ne!(event1.hostname, event3.hostname);
    }

    #[test]
    fn test_task_event_equality() {
        let task_id = Uuid::new_v4();
        let event1 = TaskEvent::sent(task_id, "tasks.add")
            .with_hostname("worker-1")
            .with_queue("celery");
        let event2 = event1.clone();

        assert_eq!(event1, event2);
        assert_eq!(event1.uuid, event2.uuid);
    }

    #[test]
    fn test_worker_event_equality() {
        let event1 =
            WorkerEvent::online("worker@host").with_software("celers-worker", "0.1.0", "Linux");
        let event2 = event1.clone();

        assert_eq!(event1, event2);
        assert_eq!(event1.software_identity, event2.software_identity);
    }
}
