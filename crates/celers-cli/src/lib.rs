//! `CeleRS` CLI library.
//!
//! This library provides command implementations for the `CeleRS` command-line interface.
//! While primarily used as a binary (`celers`), the library exports command functions
//! for programmatic use and examples.
//!
//! # Features
//!
//! - Worker management (start, stop, pause, resume, scale)
//! - Queue operations (list, stats, move, export, import)
//! - Task management (inspect, cancel, retry, logs)
//! - DLQ operations (inspect, clear, replay)
//! - Monitoring and diagnostics (metrics, health, reports)
//! - Database operations (connection testing, migrations)
//! - Auto-scaling and alerting
//!
//! # Examples
//!
//! See the `examples/` directory for comprehensive usage examples:
//! - `basic_workflow.rs` - Common CLI operations
//! - `monitoring_and_diagnostics.rs` - Production monitoring
//! - `queue_management.rs` - Queue operations
//! - `worker_management.rs` - Worker lifecycle management
//! - `task_and_dlq_management.rs` - Task and DLQ operations

pub mod backup;
pub mod command_utils;
pub mod commands;
pub mod config;
pub mod database;
pub mod interactive;

// Re-export commonly used items
pub use config::Config;
