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
pub mod calendar;
pub mod catchup;
pub mod config;
pub mod conflict;
pub mod dispatch_lock;
pub mod heartbeat;
pub mod history;
pub mod jitter;
pub mod lock;
pub mod registry;
pub mod schedule;
pub mod schedule_ext;
pub mod scheduler;
pub mod scheduler_catchup;
pub mod scheduler_ext;
pub mod scheduler_persistence;
pub mod task;
#[cfg(feature = "cron")]
pub mod timezone_schedule;
pub mod wfq;

#[cfg(test)]
mod tests;

// Re-export main types
pub use alert::*;
pub use calendar::{date_of, CalendarHoliday, WorkingCalendar};
// `catchup::CatchupPolicy` deliberately shadows nothing: the long-standing
// `history::CatchupPolicy` (interval-tick replay) keeps the crate-root name, so
// the occurrence-list policy here is reached as `catchup::CatchupPolicy`. The
// remaining catch-up helpers carry unique names and are re-exported directly.
pub use catchup::{
    catch_up, compute_missed, compute_missed_occurrences, MissedOccurrences, MAX_MISSED_OCCURRENCES,
};
pub use config::*;
pub use conflict::{
    detect_named_schedule_conflicts, detect_schedule_conflicts, enumerate_next_occurrences,
    enumerate_occurrences_in_window, has_schedule_conflicts, OccurrenceConflict,
};
pub use dispatch_lock::{dispatch_lock_key, DEFAULT_DISPATCH_LOCK_TTL_SECS};
pub use heartbeat::{BeatHeartbeat, BeatRole, HeartbeatConfig, HeartbeatInfo, HeartbeatStats};
pub use history::*;
pub use jitter::{apply_jitter, bounded_jitter_offset};
pub use lock::*;
pub use registry::{AddOutcome, ScheduleRegistry};
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
