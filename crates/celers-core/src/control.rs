//! Worker Control Commands
//!
//! This module provides the protocol for remote worker control and inspection.
//! It enables clients to query worker state, inspect running tasks, and send
//! control commands to workers.
//!
//! # Example
//!
//! ```rust
//! use celers_core::control::{ControlCommand, InspectCommand, InspectResponse};
//!
//! // Create an inspect active tasks command
//! let cmd = ControlCommand::Inspect(InspectCommand::Active);
//!
//! // Serialize for sending to worker
//! let json = serde_json::to_string(&cmd).unwrap();
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Control commands that can be sent to workers
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "args")]
pub enum ControlCommand {
    /// Ping the worker to check if it's alive
    Ping {
        /// Unique ID for this ping request
        reply_to: String,
    },

    /// Inspect worker state
    Inspect(InspectCommand),

    /// Shutdown the worker gracefully
    Shutdown {
        /// Optional timeout in seconds
        timeout: Option<u64>,
    },

    /// Revoke a task
    Revoke {
        /// Task ID to revoke
        task_id: Uuid,
        /// Whether to terminate the task if already running
        terminate: bool,
        /// Whether to send SIGKILL (true) or SIGTERM (false)
        signal: Option<String>,
    },

    /// Set rate limit for a task type
    RateLimit {
        /// Task name to rate limit
        task_name: String,
        /// Maximum tasks per second (None to remove limit)
        rate: Option<f64>,
    },

    /// Set time limit for a task type
    TimeLimit {
        /// Task name to limit
        task_name: String,
        /// Soft time limit in seconds (warning)
        soft: Option<u64>,
        /// Hard time limit in seconds (kill)
        hard: Option<u64>,
    },

    /// Add consumer for a queue
    AddConsumer {
        /// Queue name to consume from
        queue: String,
    },

    /// Cancel consumer for a queue
    CancelConsumer {
        /// Queue name to stop consuming
        queue: String,
    },

    /// Queue control commands
    Queue(QueueCommand),

    /// Bulk revoke tasks
    BulkRevoke {
        /// Task IDs to revoke
        task_ids: Vec<Uuid>,
        /// Whether to terminate running tasks
        terminate: bool,
    },

    /// Revoke tasks matching a pattern
    RevokeByPattern {
        /// Pattern to match task names (glob format)
        pattern: String,
        /// Whether to terminate running tasks
        terminate: bool,
    },
}

/// Queue control sub-commands
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action")]
pub enum QueueCommand {
    /// Purge all messages from a queue
    Purge {
        /// Queue name to purge
        queue: String,
    },

    /// Get the number of messages in a queue
    Length {
        /// Queue name to check
        queue: String,
    },

    /// Delete a queue
    Delete {
        /// Queue name to delete
        queue: String,
        /// Only delete if queue is empty
        if_empty: bool,
        /// Only delete if queue has no consumers
        if_unused: bool,
    },

    /// Bind a queue to an exchange (AMQP)
    Bind {
        /// Queue name
        queue: String,
        /// Exchange name
        exchange: String,
        /// Routing key
        routing_key: String,
    },

    /// Unbind a queue from an exchange (AMQP)
    Unbind {
        /// Queue name
        queue: String,
        /// Exchange name
        exchange: String,
        /// Routing key
        routing_key: String,
    },

    /// Declare a new queue
    Declare {
        /// Queue name
        queue: String,
        /// Whether the queue should survive broker restart
        durable: bool,
        /// Whether the queue is exclusive to this connection
        exclusive: bool,
        /// Whether the queue should be auto-deleted when unused
        auto_delete: bool,
    },
}

/// Inspection sub-commands
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "method")]
pub enum InspectCommand {
    /// List currently executing tasks
    Active,

    /// List scheduled (ETA/countdown) tasks
    Scheduled,

    /// List reserved (prefetched) tasks
    Reserved,

    /// List revoked task IDs
    Revoked,

    /// Get registered task types
    Registered,

    /// Get worker statistics
    Stats,

    /// Get task queue lengths
    QueueInfo,

    /// Report current state
    Report,

    /// Get configuration
    Conf,
}

/// Response to a control command
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "payload")]
pub enum ControlResponse {
    /// Pong response
    Pong {
        /// Worker hostname
        hostname: String,
        /// UTC timestamp
        timestamp: f64,
    },

    /// Inspection results
    Inspect(Box<InspectResponse>),

    /// Acknowledgement (for shutdown, revoke, etc.)
    Ack {
        /// Success status
        ok: bool,
        /// Optional message
        message: Option<String>,
    },

    /// Error response
    Error {
        /// Error message
        error: String,
    },

    /// Queue operation response
    Queue(QueueResponse),
}

/// Response to queue control commands
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "queue_type", content = "result")]
pub enum QueueResponse {
    /// Purge result - number of messages deleted
    Purged {
        /// Queue name
        queue: String,
        /// Number of messages purged
        message_count: u64,
    },

    /// Queue length result
    Length {
        /// Queue name
        queue: String,
        /// Number of messages in queue
        message_count: u64,
    },

    /// Queue deleted
    Deleted {
        /// Queue name
        queue: String,
    },

    /// Queue bound to exchange
    Bound {
        /// Queue name
        queue: String,
        /// Exchange name
        exchange: String,
        /// Routing key
        routing_key: String,
    },

    /// Queue unbound from exchange
    Unbound {
        /// Queue name
        queue: String,
        /// Exchange name
        exchange: String,
        /// Routing key
        routing_key: String,
    },

    /// Queue declared
    Declared {
        /// Queue name
        queue: String,
        /// Number of messages already in queue
        message_count: u64,
        /// Number of consumers
        consumer_count: u32,
    },
}

/// Response data from inspect commands
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "inspect_type", content = "data")]
pub enum InspectResponse {
    /// Active tasks
    Active(Vec<ActiveTaskInfo>),

    /// Scheduled tasks
    Scheduled(Vec<ScheduledTaskInfo>),

    /// Reserved tasks
    Reserved(Vec<ReservedTaskInfo>),

    /// Revoked task IDs
    Revoked(Vec<Uuid>),

    /// Registered task names
    Registered(Vec<String>),

    /// Worker statistics
    Stats(WorkerStats),

    /// Queue information
    QueueInfo(HashMap<String, QueueStats>),

    /// Worker report
    Report(WorkerReport),

    /// Worker configuration
    Conf(WorkerConf),
}

/// Information about an actively executing task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveTaskInfo {
    /// Task ID
    pub id: Uuid,
    /// Task name
    pub name: String,
    /// Task arguments (JSON)
    pub args: String,
    /// Task keyword arguments (JSON)
    pub kwargs: String,
    /// When the task started (Unix timestamp)
    pub started: f64,
    /// Worker hostname executing the task
    pub hostname: String,
    /// Worker process ID
    pub worker_pid: Option<u32>,
    /// Delivery info (queue, routing key, etc.)
    pub delivery_info: Option<DeliveryInfo>,
}

/// Information about a scheduled task (with ETA or countdown)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduledTaskInfo {
    /// Task ID
    pub id: Uuid,
    /// Task name
    pub name: String,
    /// Task arguments (JSON)
    pub args: String,
    /// Task keyword arguments (JSON)
    pub kwargs: String,
    /// Scheduled execution time (Unix timestamp)
    pub eta: f64,
    /// Priority
    pub priority: Option<u8>,
    /// Request info
    pub request: Option<RequestInfo>,
}

/// Information about a reserved (prefetched) task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReservedTaskInfo {
    /// Task ID
    pub id: Uuid,
    /// Task name
    pub name: String,
    /// Task arguments (JSON)
    pub args: String,
    /// Task keyword arguments (JSON)
    pub kwargs: String,
    /// Delivery info
    pub delivery_info: Option<DeliveryInfo>,
}

/// Delivery information for a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryInfo {
    /// Source queue
    pub queue: String,
    /// Routing key
    pub routing_key: Option<String>,
    /// Exchange name
    pub exchange: Option<String>,
    /// Delivery tag
    pub delivery_tag: Option<String>,
    /// Redelivered flag
    pub redelivered: bool,
}

/// Request information for a task
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestInfo {
    /// Correlation ID
    pub correlation_id: Option<String>,
    /// Reply-to queue
    pub reply_to: Option<String>,
    /// Message priority
    pub priority: Option<u8>,
}

/// Worker statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerStats {
    /// Total tasks processed
    pub total_tasks: u64,
    /// Tasks currently active
    pub active_tasks: u32,
    /// Total successful tasks
    pub succeeded: u64,
    /// Total failed tasks
    pub failed: u64,
    /// Total retried tasks
    pub retried: u64,
    /// Worker uptime in seconds
    pub uptime: f64,
    /// System load average (1, 5, 15 min)
    pub loadavg: Option<[f64; 3]>,
    /// Process memory usage in bytes
    pub memory_usage: Option<u64>,
    /// Pool information
    pub pool: Option<PoolStats>,
    /// Broker connection stats
    pub broker: Option<BrokerStats>,
    /// Clock offset from server
    pub clock: Option<f64>,
}

/// Worker pool statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolStats {
    /// Pool type (prefork, solo, threads, etc.)
    pub pool_type: String,
    /// Maximum concurrency
    pub max_concurrency: u32,
    /// Current pool size
    pub pool_size: u32,
    /// Available workers in pool
    pub available: u32,
    /// Worker process IDs
    pub processes: Vec<u32>,
}

/// Broker connection statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerStats {
    /// Broker URL (sanitized, no credentials)
    pub url: String,
    /// Connection status
    pub connected: bool,
    /// Heartbeat interval
    pub heartbeat: Option<u64>,
    /// Transport type
    pub transport: String,
}

/// Queue statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueueStats {
    /// Queue name
    pub name: String,
    /// Number of messages in queue
    pub messages: u64,
    /// Number of consumers
    pub consumers: u32,
}

/// Worker report (comprehensive status)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerReport {
    /// Worker hostname
    pub hostname: String,
    /// Software version
    pub sw_ver: String,
    /// Software system (e.g., "celers")
    pub sw_sys: String,
    /// Worker statistics
    pub stats: WorkerStats,
    /// Active tasks
    pub active: Vec<ActiveTaskInfo>,
    /// Scheduled tasks
    pub scheduled: Vec<ScheduledTaskInfo>,
    /// Reserved tasks
    pub reserved: Vec<ReservedTaskInfo>,
    /// Registered tasks
    pub registered: Vec<String>,
}

/// Worker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConf {
    /// Broker URL (sanitized)
    pub broker_url: String,
    /// Result backend URL (sanitized)
    pub result_backend: Option<String>,
    /// Default queue
    pub default_queue: String,
    /// Prefetch multiplier
    pub prefetch_multiplier: u32,
    /// Concurrency
    pub concurrency: u32,
    /// Task soft time limit
    pub task_soft_time_limit: Option<u64>,
    /// Task time limit
    pub task_time_limit: Option<u64>,
    /// Task acks late
    pub task_acks_late: bool,
    /// Task reject on worker lost
    pub task_reject_on_worker_lost: bool,
    /// Worker hostname
    pub hostname: String,
}

impl ControlCommand {
    /// Create a ping command
    #[inline]
    pub fn ping(reply_to: impl Into<String>) -> Self {
        Self::Ping {
            reply_to: reply_to.into(),
        }
    }

    /// Create an inspect active command
    #[inline]
    #[must_use]
    pub fn inspect_active() -> Self {
        Self::Inspect(InspectCommand::Active)
    }

    /// Create an inspect scheduled command
    #[inline]
    #[must_use]
    pub fn inspect_scheduled() -> Self {
        Self::Inspect(InspectCommand::Scheduled)
    }

    /// Create an inspect reserved command
    #[inline]
    #[must_use]
    pub fn inspect_reserved() -> Self {
        Self::Inspect(InspectCommand::Reserved)
    }

    /// Create an inspect revoked command
    #[inline]
    #[must_use]
    pub fn inspect_revoked() -> Self {
        Self::Inspect(InspectCommand::Revoked)
    }

    /// Create an inspect registered command
    #[inline]
    #[must_use]
    pub fn inspect_registered() -> Self {
        Self::Inspect(InspectCommand::Registered)
    }

    /// Create an inspect stats command
    #[inline]
    #[must_use]
    pub fn inspect_stats() -> Self {
        Self::Inspect(InspectCommand::Stats)
    }

    /// Create an inspect queue info command
    #[inline]
    #[must_use]
    pub fn inspect_queue_info() -> Self {
        Self::Inspect(InspectCommand::QueueInfo)
    }

    /// Create a shutdown command
    #[inline]
    #[must_use]
    pub fn shutdown(timeout: Option<u64>) -> Self {
        Self::Shutdown { timeout }
    }

    /// Create a revoke command
    #[inline]
    #[must_use]
    pub fn revoke(task_id: Uuid, terminate: bool) -> Self {
        Self::Revoke {
            task_id,
            terminate,
            signal: None,
        }
    }

    /// Create a bulk revoke command
    #[inline]
    #[must_use]
    pub fn bulk_revoke(task_ids: Vec<Uuid>, terminate: bool) -> Self {
        Self::BulkRevoke {
            task_ids,
            terminate,
        }
    }

    /// Create a revoke by pattern command
    #[inline]
    pub fn revoke_by_pattern(pattern: impl Into<String>, terminate: bool) -> Self {
        Self::RevokeByPattern {
            pattern: pattern.into(),
            terminate,
        }
    }

    /// Create a queue purge command
    #[inline]
    pub fn queue_purge(queue: impl Into<String>) -> Self {
        Self::Queue(QueueCommand::Purge {
            queue: queue.into(),
        })
    }

    /// Create a queue length command
    #[inline]
    pub fn queue_length(queue: impl Into<String>) -> Self {
        Self::Queue(QueueCommand::Length {
            queue: queue.into(),
        })
    }

    /// Create a queue delete command
    #[inline]
    pub fn queue_delete(queue: impl Into<String>, if_empty: bool, if_unused: bool) -> Self {
        Self::Queue(QueueCommand::Delete {
            queue: queue.into(),
            if_empty,
            if_unused,
        })
    }

    /// Create a queue bind command
    #[inline]
    pub fn queue_bind(
        queue: impl Into<String>,
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
    ) -> Self {
        Self::Queue(QueueCommand::Bind {
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
        })
    }

    /// Create a queue unbind command
    #[inline]
    pub fn queue_unbind(
        queue: impl Into<String>,
        exchange: impl Into<String>,
        routing_key: impl Into<String>,
    ) -> Self {
        Self::Queue(QueueCommand::Unbind {
            queue: queue.into(),
            exchange: exchange.into(),
            routing_key: routing_key.into(),
        })
    }

    /// Create a queue declare command
    #[inline]
    pub fn queue_declare(
        queue: impl Into<String>,
        durable: bool,
        exclusive: bool,
        auto_delete: bool,
    ) -> Self {
        Self::Queue(QueueCommand::Declare {
            queue: queue.into(),
            durable,
            exclusive,
            auto_delete,
        })
    }
}

impl QueueCommand {
    /// Get the queue name this command operates on
    #[inline]
    #[must_use]
    pub fn queue_name(&self) -> &str {
        match self {
            Self::Purge { queue }
            | Self::Length { queue }
            | Self::Delete { queue, .. }
            | Self::Bind { queue, .. }
            | Self::Unbind { queue, .. }
            | Self::Declare { queue, .. } => queue,
        }
    }
}

impl ControlResponse {
    /// Create a pong response
    #[inline]
    pub fn pong(hostname: impl Into<String>) -> Self {
        Self::Pong {
            hostname: hostname.into(),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
        }
    }

    /// Create an ack response
    #[inline]
    #[must_use]
    pub fn ack(ok: bool, message: Option<String>) -> Self {
        Self::Ack { ok, message }
    }

    /// Create an error response
    #[inline]
    pub fn error(error: impl Into<String>) -> Self {
        Self::Error {
            error: error.into(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_control_command_serialization() {
        let cmd = ControlCommand::ping("test-reply");
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("Ping"));
        assert!(json.contains("test-reply"));

        let cmd: ControlCommand = serde_json::from_str(&json).unwrap();
        matches!(cmd, ControlCommand::Ping { reply_to } if reply_to == "test-reply");
    }

    #[test]
    fn test_inspect_command_serialization() {
        let cmd = ControlCommand::inspect_active();
        let json = serde_json::to_string(&cmd).unwrap();
        assert!(json.contains("Inspect"));
        assert!(json.contains("Active"));
    }

    #[test]
    fn test_control_response_serialization() {
        let resp = ControlResponse::pong("worker-1");
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("Pong"));
        assert!(json.contains("worker-1"));
    }

    #[test]
    fn test_inspect_response_serialization() {
        let stats = WorkerStats {
            total_tasks: 100,
            active_tasks: 2,
            succeeded: 95,
            failed: 3,
            retried: 2,
            uptime: 3600.0,
            loadavg: Some([0.5, 0.6, 0.7]),
            memory_usage: Some(1024 * 1024 * 100),
            pool: None,
            broker: None,
            clock: None,
        };
        let resp = InspectResponse::Stats(stats);
        let json = serde_json::to_string(&resp).unwrap();
        assert!(json.contains("Stats"));
        assert!(json.contains("100"));
    }

    #[test]
    fn test_active_task_info() {
        let info = ActiveTaskInfo {
            id: Uuid::new_v4(),
            name: "tasks.add".to_string(),
            args: "[1, 2]".to_string(),
            kwargs: "{}".to_string(),
            started: 1_234_567_890.0,
            hostname: "worker-1".to_string(),
            worker_pid: Some(12345),
            delivery_info: Some(DeliveryInfo {
                queue: "celery".to_string(),
                routing_key: Some("tasks.add".to_string()),
                exchange: Some(String::new()),
                delivery_tag: Some("1".to_string()),
                redelivered: false,
            }),
        };
        let json = serde_json::to_string(&info).unwrap();
        assert!(json.contains("tasks.add"));
    }
}
