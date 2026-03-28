//! Periodic task scheduler (Celery Beat equivalent)
//!
//! This crate provides scheduled task execution with various schedule types
//! and persistent state management.
//!
//! # Schedule Types
//!
//! - **Interval**: Execute every N seconds
//! - **Crontab**: Execute based on cron expression (requires `cron` feature)
//! - **Solar**: Execute at solar events (sunrise, sunset) (requires `solar` feature)
//! - **OneTime**: Execute once at a specific timestamp (auto-cleanup after execution)
//!
//! # Persistence
//!
//! The scheduler supports automatic state persistence to JSON files, preserving
//! schedules and execution history across restarts:
//!
//! ```no_run
//! use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
//!
//! // Load scheduler from file (or create new if file doesn't exist)
//! let mut scheduler = BeatScheduler::load_from_file("schedules.json").unwrap();
//!
//! // Add tasks - automatically saved to file
//! let task = ScheduledTask::new("send_report".to_string(), Schedule::interval(60));
//! scheduler.add_task(task).unwrap();
//!
//! // State persists across restarts
//! ```
//!
//! # Basic Example
//!
//! ```ignore
//! use celers_beat::{Schedule, ScheduledTask};
//!
//! let schedule = Schedule::interval(60);  // Every 60 seconds
//! let task = ScheduledTask::new("send_report".to_string(), schedule);
//! ```

use std::sync::Arc;

pub mod alert;
pub mod config;
pub mod heartbeat;
pub mod history;
pub mod lock;
pub mod schedule;
pub mod schedule_ext;
pub mod scheduler;
pub mod scheduler_ext;
pub mod task;
pub mod wfq;

#[cfg(test)]
mod tests;

// Re-export main types
pub use alert::*;
pub use config::*;
pub use heartbeat::{BeatHeartbeat, BeatRole, HeartbeatConfig, HeartbeatInfo, HeartbeatStats};
pub use history::*;
pub use lock::*;
pub use schedule::*;
pub use schedule_ext::*;
pub use scheduler::*;
pub use scheduler_ext::*;
pub use task::*;
pub use wfq::*;

/// Failure notification callback type
///
/// Called when a task execution fails. Receives the task name and error message.
pub type FailureCallback = Arc<dyn Fn(&str, &str) + Send + Sync>;
