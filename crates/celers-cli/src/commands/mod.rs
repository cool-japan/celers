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
//!         300
//!     ).await?;
//!     Ok(())
//! }
//! ```

mod config_cmds;
mod database;
mod dlq;
mod monitoring;
mod queue;
mod schedule;
pub(crate) mod task;
pub(crate) mod utils;
mod worker;

// Re-export all public functions to preserve the flat API

// Worker management
pub use worker::{
    drain_worker, list_workers, pause_worker, resume_worker, scale_workers, start_worker,
    stop_worker, worker_stats,
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

// Scheduling
pub use schedule::{
    add_schedule, list_schedules, pause_schedule, remove_schedule, resume_schedule,
    schedule_history, trigger_schedule,
};

// Monitoring, diagnostics, and reporting
pub use monitoring::{
    alert_start, alert_test, analyze_bottlenecks, analyze_failures, autoscale_start,
    autoscale_status, debug_task, debug_worker, doctor, health_check, report_daily, report_weekly,
    show_metrics, show_task_logs, worker_logs,
};

// Database operations
pub use database::{db_health, db_migrate, db_pool_stats, db_test_connection, run_dashboard};

// Configuration
pub use config_cmds::{init_config, validate_config};

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

    #[test]
    fn test_queue_key_formatting() {
        let queue = "test-queue";
        let queue_key = format!("celers:{}", queue);
        assert_eq!(queue_key, "celers:test-queue");

        let dlq_key = format!("{}:dlq", queue_key);
        assert_eq!(dlq_key, "celers:test-queue:dlq");

        let delayed_key = format!("{}:delayed", queue_key);
        assert_eq!(delayed_key, "celers:test-queue:delayed");
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
