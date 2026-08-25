//! CLI command implementations for `CeleRS` distributed task queue management.
//!
//! This module provides all the command implementations for the `CeleRS` CLI tool.
//! Commands are organized into the following categories:
//!
//! - **Worker Management**: Starting, stopping, pausing, scaling workers
//! - **Queue Operations**: Listing, purging, moving, importing/exporting queues
//! - **Task Management**: Inspecting, canceling, retrying, and monitoring tasks
//! - **DLQ Operations**: Managing failed tasks in the Dead Letter Queue
//! - **Scheduling**: Managing scheduled/periodic tasks with cron expressions
//! - **Monitoring**: Metrics, dashboard, health checks, and diagnostics
//! - **Database**: Connection testing, health checks, migrations
//! - **Configuration**: Validation, initialization, and profile management
//!
//! # Error Handling
//!
//! All functions return `anyhow::Result<()>` for consistent error handling.
//! Errors are user-friendly and provide actionable feedback.
//!
//! # Examples
//!
//! ```no_run
//! use celers_cli::commands;
//! use celers_cli::commands::WorkerStartupOptions;
//!
//! #[tokio::main]
//! async fn main() -> anyhow::Result<()> {
//!     // Start a worker
//!     commands::start_worker(
//!         "redis://localhost:6379",
//!         "my_queue",
//!         "fifo",
//!         4,
//!         3,
//!         300,
//!         &WorkerStartupOptions::default(),
//!     ).await?;
//!     Ok(())
//! }
//! ```

mod config_cmds;
pub mod control;
mod database;
pub mod depgraph;
mod dlq;
pub mod loadtest_cmds;
pub mod metrics_cmds;
mod monitoring;
// `pub(crate)` (rather than a bare private `mod`), matching the existing
// `task`/`utils` modules below: `queue::queue_names` needs to be callable
// from `crate::interactive` (outside this module tree) for the REPL's
// `use <queue>` "did you mean" suggestion, while everything else in `queue`
// stays reachable only through the curated `pub use queue::{...}`
// re-exports right below.
pub(crate) mod queue;
pub mod replay_cmds;
mod schedule;
pub(crate) mod task;
pub(crate) mod utils;
pub mod wizard;
mod worker;
mod worker_demo_tasks;

// Re-export all public functions to preserve the flat API

// Remote worker control and inspection (control protocol over Redis Pub/Sub)
pub use control::{ping_workers, revoke_tasks, run_control, run_inspect, ControlOptions};

// Worker management
pub use worker::{
    drain_worker, list_workers, pause_worker, resume_worker, scale_workers, start_worker,
    stop_worker, worker_stats, WorkerStartupOptions,
};

// Queue operations
pub use queue::{
    export_queue, import_queue, list_queues, move_queue, pause_queue, purge_queue, queue_stats,
    resume_queue, show_status,
};

// Task management
pub use task::{cancel_task, inspect_task, requeue_task, retry_task, show_task_result};

// DLQ operations
pub use dlq::{clear_dlq, inspect_dlq, replay_task};

// Task replay (re-execute failed / DLQ tasks).
//
// The thin broker-facing entry point and its selector enum are re-exported at
// the `commands` root for convenience (the binary uses these). The pure
// planning API (`plan_replay`, `glob_match`, `ReplayCandidate`, `ReplayPlan`)
// remains available via the public `commands::replay_cmds` module.
pub use replay_cmds::{replay_dlq, ReplayFilter};

// Task simulation / load testing.
//
// The thin broker-facing entry point (`run_loadtest`) is re-exported at the
// `commands` root for the binary. The pure planning API (`plan_loadtest`,
// `LoadTestConfig`, `LoadPlan`, `ArrivalPattern`, ...) remains available via
// the public `commands::loadtest_cmds` module.
pub use loadtest_cmds::{run_loadtest, ArrivalPattern, LoadTestConfig};

// Scheduling
pub use schedule::{
    add_schedule, list_schedules, pause_schedule, remove_schedule, resume_schedule,
    schedule_history, trigger_schedule,
};

// Monitoring, diagnostics, and reporting
//
// `report_weekly` (the original table-only, stdout-only sibling of
// `report_weekly_formatted`) had no caller anywhere in the crate and was
// removed; only `report_weekly_formatted` remains. `report_history`/
// `report_workers`/`report_queues` and `profile_task`/`profile_worker`/
// `profile_resources` are new report/profiling subcommands not yet wired
// into `cli::types`/`cli::dispatch`.
pub use monitoring::{
    alert_start, alert_test, analyze_bottlenecks, analyze_failures, autoscale_start,
    autoscale_status, debug_task, debug_worker, doctor, health_check, profile_resources,
    profile_task, profile_worker, report_daily_formatted, report_history, report_queues,
    report_weekly_formatted, report_workers, show_metrics, show_task_logs, worker_logs,
};

// `report_daily` keeps its original table-only, stdout-only signature (see
// `commands::monitoring::report` for why the function itself is
// `#[allow(dead_code)]`: `cli::dispatch`'s `Report::Daily` arm calls only
// `report_daily_formatted`, re-exported above). This re-export is likewise
// unreachable from this crate's own `bin`/test targets -- hence
// `#[allow(unused_imports)]` here too -- but it is genuine public library
// API, called directly by `examples/monitoring_and_diagnostics.rs` as
// `commands::report_daily`.
#[allow(unused_imports)]
pub use monitoring::report_daily;

// Remote Prometheus scraping and live monitoring
pub use metrics_cmds::{run_metrics, run_monitor};

// Database operations
pub use database::{db_health, db_migrate, db_pool_stats, db_test_connection, run_dashboard};

// Configuration
pub use config_cmds::{init_config, validate_config};

// Interactive configuration wizard (`celers init --wizard`).
//
// `init_config_wizard` lives in `config_cmds` (it is the wizard's
// counterpart to `init_config`, both writing a `Config` to disk) and is
// re-exported here on its own line so the pre-existing `config_cmds`
// re-export above stays untouched. The thin, I/O-performing entry point
// (`run_wizard`) has no caller of its own at the `commands` root -- unlike
// `loadtest_cmds::run_loadtest`/`replay_cmds::replay_dlq` above, `celers
// init --wizard` drives the wizard through `init_config_wizard` (which
// calls `wizard::run_wizard` internally), not a top-level `run_wizard`
// re-export -- so that redundant re-export was removed. `run_wizard` itself
// is not deleted: it remains available, along with the pure, fully
// unit-tested assembly API (`WizardAnswers`, `build_config`,
// `resolve_wizard_output_path`), via the public `commands::wizard` module.
pub use config_cmds::init_config_wizard;

// Task dependency graph visualization (`celers deps`).
//
// The thin, file-facing entry point (`run_deps`) is re-exported at the
// `commands` root, matching the `loadtest_cmds`/`replay_cmds` pattern
// above (see the `wizard`/`init_config_wizard` note above for a case where
// this pattern does *not* apply, since `run_wizard` has no root re-export).
// The pure, fully unit-tested rendering/graph-building API (`render_ascii`,
// `render_dot`, `dag_from_tasks`, `InteractiveSession`, ...) remains
// available via the public `commands::depgraph` module.
pub use depgraph::run_deps;

#[cfg(test)]
mod tests {
    #[test]
    fn test_task_id_parsing() {
        // Valid UUID
        let valid_id = "550e8400-e29b-41d4-a716-446655440000";
        assert!(valid_id.parse::<uuid::Uuid>().is_ok());

        // Invalid UUID
        let invalid_id = "not-a-valid-uuid";
        assert!(invalid_id.parse::<uuid::Uuid>().is_err());
    }

    #[test]
    fn test_worker_id_extraction() {
        let key = "celers:worker:worker-123:heartbeat";
        let parts: Vec<&str> = key.split(':').collect();
        assert_eq!(parts.len(), 4);
        assert_eq!(parts[2], "worker-123");
    }

    #[test]
    fn test_log_level_matching() {
        let levels = vec![
            "ERROR", "error", "WARN", "warn", "INFO", "info", "DEBUG", "debug",
        ];

        for level in levels {
            let _colored = match level {
                "ERROR" | "error" => level,
                "WARN" | "warn" => level,
                "DEBUG" | "debug" => level,
                _ => level,
            };
            // Just testing the pattern matching logic
            assert!(!level.is_empty());
        }
    }

    #[test]
    fn test_redis_key_formatting() {
        let task_id = uuid::Uuid::new_v4();
        let logs_key = format!("celers:task:{}:logs", task_id);
        assert!(logs_key.starts_with("celers:task:"));
        assert!(logs_key.ends_with(":logs"));

        let worker_id = "worker-123";
        let heartbeat_key = format!("celers:worker:{}:heartbeat", worker_id);
        assert_eq!(heartbeat_key, "celers:worker:worker-123:heartbeat");

        let pause_key = format!("celers:worker:{}:paused", worker_id);
        assert_eq!(pause_key, "celers:worker:worker-123:paused");
    }

    /// Regression test for idx 312: this test used to assert that a queue's
    /// Redis keys carry a `celers:` prefix (`format!("celers:{queue}")` and
    /// friends) -- the exact key-namespace bug `crate::keys` exists to
    /// eliminate, since a real `RedisBroker` queue-family key carries no
    /// shared prefix at all (see `crate::keys`'s module docs). Left
    /// unnoticed, this test actively enshrined the wrong scheme as
    /// "correct" even after every real call site was fixed to use
    /// `crate::keys`. It must assert against `crate::keys`'s constructors
    /// (mirroring `crate::keys::tests::queue_family_keys_match_redis_broker_scheme`)
    /// instead of a hand-rolled, `celers:`-prefixed format string.
    #[test]
    fn test_queue_key_formatting() {
        let queue = "test-queue";
        let queue_key = crate::keys::main(queue);
        assert_eq!(queue_key, "test-queue");

        let dlq_key = crate::keys::dlq(queue);
        assert_eq!(dlq_key, "test-queue:dlq");

        let delayed_key = crate::keys::delayed(queue);
        assert_eq!(delayed_key, "test-queue:delayed");

        let processing_key = crate::keys::processing(queue);
        assert_eq!(processing_key, "test-queue:processing");
    }

    #[test]
    fn test_json_log_parsing() {
        let valid_log =
            r#"{"timestamp":"2026-01-04T10:00:00Z","level":"INFO","message":"Test message"}"#;
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(valid_log);
        assert!(parsed.is_ok());

        if let Ok(log_json) = parsed {
            assert_eq!(log_json.get("level").and_then(|v| v.as_str()), Some("INFO"));
            assert_eq!(
                log_json.get("message").and_then(|v| v.as_str()),
                Some("Test message")
            );
        }

        let invalid_log = "not json";
        let parsed: Result<serde_json::Value, _> = serde_json::from_str(invalid_log);
        assert!(parsed.is_err());
    }

    #[test]
    fn test_limit_range_calculation() {
        let limit = 50;
        let log_count = 100;

        // Redis LRANGE with negative indices
        let start_idx = -(limit as isize);
        let end_idx = -1;

        assert_eq!(start_idx, -50);
        assert_eq!(end_idx, -1);

        // Should get last 50 items
        assert!(log_count as usize > limit);
    }

    #[test]
    fn test_diagnostic_thresholds() {
        // DLQ threshold
        let dlq_size = 15;
        assert!(dlq_size > 10, "Should trigger warning when DLQ > 10");

        // Queue backlog threshold
        let queue_size = 1500;
        assert!(
            queue_size > 1000,
            "Should trigger warning when queue > 1000"
        );

        // No workers scenario
        let worker_count = 0;
        let pending_tasks = 50;
        assert!(
            worker_count == 0 && pending_tasks > 0,
            "Should trigger error when no workers but tasks pending"
        );
    }

    #[test]
    fn test_shutdown_channel_naming() {
        let worker_id = "worker-123";

        let graceful_channel = format!("celers:worker:{}:shutdown_graceful", worker_id);
        assert_eq!(
            graceful_channel,
            "celers:worker:worker-123:shutdown_graceful"
        );

        let immediate_channel = format!("celers:worker:{}:shutdown", worker_id);
        assert_eq!(immediate_channel, "celers:worker:worker-123:shutdown");
    }

    #[test]
    fn test_timestamp_formatting() {
        let timestamp = chrono::Utc::now().to_rfc3339();
        assert!(timestamp.contains('T'));
        assert!(timestamp.contains('Z') || timestamp.contains('+'));
    }

    #[test]
    fn test_mask_password() {
        use super::utils::mask_password;

        // Test PostgreSQL URL
        let pg_url = "postgres://user:password123@localhost:5432/dbname";
        let masked = mask_password(pg_url);
        assert!(masked.contains("postgres://user:****@localhost"));
        assert!(!masked.contains("password123"));

        // Test MySQL URL
        let mysql_url = "mysql://admin:secret@127.0.0.1:3306/db";
        let masked = mask_password(mysql_url);
        assert!(masked.contains("mysql://admin:****@127.0.0.1"));
        assert!(!masked.contains("secret"));

        // Test URL without password
        let no_pass_url = "redis://localhost:6379";
        let masked = mask_password(no_pass_url);
        assert_eq!(masked, no_pass_url);
    }

    #[test]
    fn test_format_bytes() {
        use super::utils::format_bytes;

        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }

    #[test]
    fn test_format_duration() {
        use super::utils::format_duration;

        assert_eq!(format_duration(0), "0s");
        assert_eq!(format_duration(30), "30s");
        assert_eq!(format_duration(60), "1m");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(3600), "1h");
        assert_eq!(format_duration(3660), "1h 1m");
        assert_eq!(format_duration(86400), "1d");
        assert_eq!(format_duration(90000), "1d 1h");
    }

    #[test]
    fn test_validate_task_id() {
        use super::utils::validate_task_id;

        // Valid UUID
        let valid = "550e8400-e29b-41d4-a716-446655440000";
        assert!(validate_task_id(valid).is_ok());

        // Invalid UUIDs
        assert!(validate_task_id("not-a-uuid").is_err());
        assert!(validate_task_id("").is_err());
        assert!(validate_task_id("12345").is_err());
    }

    #[test]
    fn test_validate_queue_name() {
        use super::utils::validate_queue_name;

        // Valid queue names
        assert!(validate_queue_name("default").is_ok());
        assert!(validate_queue_name("high-priority").is_ok());
        assert!(validate_queue_name("queue_1").is_ok());

        // Invalid queue names
        assert!(validate_queue_name("").is_err()); // Empty
        assert!(validate_queue_name("queue name").is_err()); // Whitespace
        assert!(validate_queue_name(&"x".repeat(256)).is_err()); // Too long
    }

    #[test]
    fn test_calculate_percentage() {
        use super::utils::calculate_percentage;

        assert_eq!(calculate_percentage(0, 100), 0.0);
        assert_eq!(calculate_percentage(50, 100), 50.0);
        assert_eq!(calculate_percentage(100, 100), 100.0);
        assert_eq!(calculate_percentage(25, 100), 25.0);

        // Edge case: division by zero
        assert_eq!(calculate_percentage(10, 0), 0.0);
    }
}
