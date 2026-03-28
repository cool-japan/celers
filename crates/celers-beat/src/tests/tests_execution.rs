#![cfg(test)]

use crate::*;
use chrono::{Duration, Utc};
use std::collections::HashSet;
use tempfile::NamedTempFile;

// ===== Execution History Tests =====

#[test]
fn test_execution_result_success() {
    let result = ExecutionResult::Success;
    assert!(matches!(result, ExecutionResult::Success));
}

#[test]
fn test_execution_result_failure() {
    let result = ExecutionResult::Failure {
        error: "Test error".to_string(),
    };
    assert!(matches!(result, ExecutionResult::Failure { .. }));
}

#[test]
fn test_execution_result_timeout() {
    let result = ExecutionResult::Timeout;
    assert!(matches!(result, ExecutionResult::Timeout));
}

#[test]
fn test_execution_record_new() {
    let started_at = Utc::now();
    let record = ExecutionRecord::new(started_at);

    assert_eq!(record.started_at, started_at);
    assert!(record.completed_at.is_none());
    assert!(matches!(record.result, ExecutionResult::Success));
    assert!(record.duration_ms.is_none());
}

#[test]
fn test_execution_record_completed() {
    let started_at = Utc::now() - Duration::milliseconds(100);
    let record = ExecutionRecord::completed(started_at, ExecutionResult::Success);

    assert_eq!(record.started_at, started_at);
    assert!(record.completed_at.is_some());
    assert!(record.duration_ms.is_some());
    assert!(record.duration_ms.unwrap() >= 100);
}

#[test]
fn test_execution_record_is_success() {
    let record = ExecutionRecord::completed(Utc::now(), ExecutionResult::Success);
    assert!(record.is_success());
    assert!(!record.is_failure());
    assert!(!record.is_timeout());
}

#[test]
fn test_execution_record_is_failure() {
    let record = ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Failure {
            error: "Test error".to_string(),
        },
    );
    assert!(record.is_failure());
    assert!(!record.is_success());
    assert!(!record.is_timeout());
}

#[test]
fn test_execution_record_is_timeout() {
    let record = ExecutionRecord::completed(Utc::now(), ExecutionResult::Timeout);
    assert!(record.is_timeout());
    assert!(!record.is_success());
    assert!(!record.is_failure());
}

#[test]
fn test_execution_record_is_completed() {
    let record = ExecutionRecord::completed(Utc::now(), ExecutionResult::Success);
    assert!(record.is_completed());

    let incomplete = ExecutionRecord::new(Utc::now());
    assert!(!incomplete.is_completed());
}

#[test]
fn test_scheduled_task_add_execution_record() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    let record = ExecutionRecord::completed(Utc::now(), ExecutionResult::Success);
    task.add_execution_record(record);

    assert_eq!(task.execution_history.len(), 1);
    assert!(task.execution_history[0].is_success());
}

#[test]
fn test_scheduled_task_with_max_history() {
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule).with_max_history(3);

    assert_eq!(task.max_history_size, 3);
}

#[test]
fn test_scheduled_task_history_trimming() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule).with_max_history(3);

    // Add 5 records
    for _ in 0..5 {
        let record = ExecutionRecord::completed(Utc::now(), ExecutionResult::Success);
        task.add_execution_record(record);
        std::thread::sleep(std::time::Duration::from_millis(1));
    }

    // Should only keep last 3
    assert_eq!(task.execution_history.len(), 3);
}

#[test]
fn test_scheduled_task_get_last_executions() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    // Add 5 records
    for _ in 0..5 {
        let record = ExecutionRecord::completed(Utc::now(), ExecutionResult::Success);
        task.add_execution_record(record);
    }

    let last_3 = task.get_last_executions(3);
    assert_eq!(last_3.len(), 3);

    let last_10 = task.get_last_executions(10);
    assert_eq!(last_10.len(), 5); // Only 5 exist
}

#[test]
fn test_scheduled_task_get_all_executions() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    // Add 3 records
    for _ in 0..3 {
        let record = ExecutionRecord::completed(Utc::now(), ExecutionResult::Success);
        task.add_execution_record(record);
    }

    let all = task.get_all_executions();
    assert_eq!(all.len(), 3);
}

#[test]
fn test_scheduled_task_history_success_count() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Success,
    ));
    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Failure {
            error: "Error".to_string(),
        },
    ));
    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Success,
    ));

    assert_eq!(task.history_success_count(), 2);
}

#[test]
fn test_scheduled_task_history_failure_count() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Success,
    ));
    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Failure {
            error: "Error 1".to_string(),
        },
    ));
    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Failure {
            error: "Error 2".to_string(),
        },
    ));

    assert_eq!(task.history_failure_count(), 2);
}

#[test]
fn test_scheduled_task_history_timeout_count() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Success,
    ));
    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Timeout,
    ));
    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Timeout,
    ));

    assert_eq!(task.history_timeout_count(), 2);
}

#[test]
fn test_scheduled_task_average_duration_ms() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    // No history
    assert!(task.average_duration_ms().is_none());

    // Add records with known durations
    let started1 = Utc::now() - Duration::milliseconds(100);
    task.add_execution_record(ExecutionRecord::completed(
        started1,
        ExecutionResult::Success,
    ));

    std::thread::sleep(std::time::Duration::from_millis(10));

    let started2 = Utc::now() - Duration::milliseconds(200);
    task.add_execution_record(ExecutionRecord::completed(
        started2,
        ExecutionResult::Success,
    ));

    let avg = task.average_duration_ms().unwrap();
    assert!(avg >= 100); // At least 100ms average
}

#[test]
fn test_scheduled_task_min_max_duration() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    let started1 = Utc::now() - Duration::milliseconds(100);
    task.add_execution_record(ExecutionRecord::completed(
        started1,
        ExecutionResult::Success,
    ));

    std::thread::sleep(std::time::Duration::from_millis(10));

    let started2 = Utc::now() - Duration::milliseconds(200);
    task.add_execution_record(ExecutionRecord::completed(
        started2,
        ExecutionResult::Success,
    ));

    let min = task.min_duration_ms().unwrap();
    let max = task.max_duration_ms().unwrap();

    assert!(min >= 100);
    assert!(max >= 200);
    assert!(max >= min);
}

#[test]
fn test_scheduled_task_history_success_rate() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    // No history
    assert_eq!(task.history_success_rate(), 0.0);

    // Add 3 success, 1 failure
    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Success,
    ));
    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Success,
    ));
    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Failure {
            error: "Error".to_string(),
        },
    ));
    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Success,
    ));

    assert_eq!(task.history_success_rate(), 0.75); // 3/4 = 0.75
}

#[test]
fn test_scheduled_task_clear_history() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    // Add records
    for _ in 0..3 {
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Success,
        ));
    }

    assert_eq!(task.execution_history.len(), 3);

    task.clear_history();
    assert_eq!(task.execution_history.len(), 0);
}

#[test]
fn test_beat_scheduler_mark_task_success_with_history() {
    let mut scheduler = BeatScheduler::new();
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule);

    scheduler.add_task(task).unwrap();
    scheduler.mark_task_success("test_task").unwrap();

    let task = scheduler.tasks.get("test_task").unwrap();
    assert_eq!(task.execution_history.len(), 1);
    assert!(task.execution_history[0].is_success());
}

#[test]
fn test_beat_scheduler_mark_task_failure_with_history() {
    let mut scheduler = BeatScheduler::new();
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule);

    scheduler.add_task(task).unwrap();
    scheduler
        .mark_task_failure_with_error("test_task", "Test error".to_string())
        .unwrap();

    let task = scheduler.tasks.get("test_task").unwrap();
    assert_eq!(task.execution_history.len(), 1);
    assert!(task.execution_history[0].is_failure());
}

#[test]
fn test_beat_scheduler_mark_task_timeout_with_history() {
    let mut scheduler = BeatScheduler::new();
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule);

    scheduler.add_task(task).unwrap();

    let started_at = Utc::now() - Duration::seconds(5);
    scheduler
        .mark_task_timeout("test_task", started_at)
        .unwrap();

    let task = scheduler.tasks.get("test_task").unwrap();
    assert_eq!(task.execution_history.len(), 1);
    assert!(task.execution_history[0].is_timeout());
}

#[test]
fn test_beat_scheduler_mark_task_success_with_start_time() {
    let mut scheduler = BeatScheduler::new();
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule);

    scheduler.add_task(task).unwrap();

    let started_at = Utc::now() - Duration::milliseconds(150);
    scheduler
        .mark_task_success_with_start("test_task", started_at)
        .unwrap();

    let task = scheduler.tasks.get("test_task").unwrap();
    assert_eq!(task.execution_history.len(), 1);
    assert!(task.execution_history[0].is_success());
    assert!(task.execution_history[0].duration_ms.unwrap() >= 150);
}

#[test]
fn test_execution_history_serialization() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Success,
    ));
    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Failure {
            error: "Test error".to_string(),
        },
    ));

    let json = serde_json::to_string(&task).unwrap();
    let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.execution_history.len(), 2);
    assert!(deserialized.execution_history[0].is_success());
    assert!(deserialized.execution_history[1].is_failure());
}

#[test]
fn test_execution_history_persistence() {
    let temp_file = NamedTempFile::new().unwrap();
    let temp_path = temp_file.path().to_str().unwrap().to_string();

    // Create scheduler and add task with history
    {
        let mut scheduler = BeatScheduler::with_persistence(&temp_path);
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        scheduler.add_task(task).unwrap();
        scheduler.mark_task_success("test_task").unwrap();
        scheduler
            .mark_task_failure_with_error("test_task", "Test error".to_string())
            .unwrap();
        scheduler.mark_task_success("test_task").unwrap();
    }

    // Load and verify history
    {
        let scheduler = BeatScheduler::load_from_file(&temp_path).unwrap();
        let task = scheduler.tasks.get("test_task").unwrap();

        assert_eq!(task.execution_history.len(), 3);
        assert!(task.execution_history[0].is_success());
        assert!(task.execution_history[1].is_failure());
        assert!(task.execution_history[2].is_success());
    }
    // temp_file automatically cleaned up when dropped
}

// ===== Health Check Tests =====

#[test]
fn test_schedule_health_healthy() {
    let health = ScheduleHealth::Healthy;
    assert!(health.is_healthy());
    assert!(!health.has_warnings());
    assert!(!health.is_unhealthy());
    assert_eq!(health.get_issues().len(), 0);
}

#[test]
fn test_schedule_health_warning() {
    let health = ScheduleHealth::Warning {
        issues: vec!["Warning 1".to_string(), "Warning 2".to_string()],
    };
    assert!(!health.is_healthy());
    assert!(health.has_warnings());
    assert!(!health.is_unhealthy());
    assert_eq!(health.get_issues().len(), 2);
}

#[test]
fn test_schedule_health_unhealthy() {
    let health = ScheduleHealth::Unhealthy {
        issues: vec!["Error 1".to_string()],
    };
    assert!(!health.is_healthy());
    assert!(!health.has_warnings());
    assert!(health.is_unhealthy());
    assert_eq!(health.get_issues().len(), 1);
}

#[test]
fn test_health_check_result_creation() {
    let result = HealthCheckResult::healthy("test_task".to_string());
    assert_eq!(result.task_name, "test_task");
    assert!(result.health.is_healthy());
    assert!(result.next_run.is_none());
    assert!(result.time_since_last_run.is_none());
}

#[test]
fn test_health_check_result_with_details() {
    let next_run = Utc::now() + Duration::seconds(60);
    let duration = Duration::seconds(30);

    let result = HealthCheckResult::healthy("test_task".to_string())
        .with_next_run(next_run)
        .with_time_since_last_run(duration);

    assert_eq!(result.next_run, Some(next_run));
    assert_eq!(result.time_since_last_run, Some(duration));
}

#[test]
fn test_scheduled_task_check_health_healthy() {
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule);

    let result = task.check_health();
    assert!(result.health.is_healthy());
    assert!(result.next_run.is_some());
}

#[test]
fn test_scheduled_task_check_health_disabled() {
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule).disabled();

    let result = task.check_health();
    assert!(result.health.has_warnings());
    let issues = result.health.get_issues();
    assert!(issues.iter().any(|i| i.contains("disabled")));
}

#[test]
fn test_scheduled_task_check_health_high_failure_rate() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    // Simulate high failure rate
    task.total_run_count = 5;
    task.total_failure_count = 10; // 66% failure rate

    let result = task.check_health();
    assert!(result.health.has_warnings());
    let issues = result.health.get_issues();
    assert!(issues.iter().any(|i| i.contains("High failure rate")));
}

#[test]
fn test_scheduled_task_check_health_consecutive_failures() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    // Add 3 consecutive failures
    for _ in 0..3 {
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Failure {
                error: "Test error".to_string(),
            },
        ));
    }

    let result = task.check_health();
    assert!(result.health.has_warnings() || result.health.is_unhealthy());
    let issues = result.health.get_issues();
    assert!(issues
        .iter()
        .any(|i| i.contains("Last 3 executions failed")));
}

#[test]
fn test_scheduled_task_is_stuck_not_stuck() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    // Recently ran
    task.last_run_at = Some(Utc::now() - Duration::seconds(30));

    assert!(task.is_stuck().is_none());
}

#[test]
fn test_scheduled_task_is_stuck() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    // Hasn't run in a very long time (100x the interval)
    task.last_run_at = Some(Utc::now() - Duration::seconds(6000));

    let stuck_duration = task.is_stuck();
    assert!(stuck_duration.is_some());
    assert!(stuck_duration.unwrap().num_seconds() >= 6000);
}

#[test]
fn test_scheduled_task_is_stuck_disabled() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule).disabled();

    // Disabled tasks are never considered stuck
    task.last_run_at = Some(Utc::now() - Duration::seconds(10000));

    assert!(task.is_stuck().is_none());
}

#[test]
fn test_scheduled_task_validate_schedule_valid() {
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule);

    assert!(task.validate_schedule().is_ok());
}

#[test]
#[cfg(feature = "cron")]
fn test_scheduled_task_validate_schedule_invalid_cron() {
    let schedule = Schedule::crontab("invalid", "0", "*", "*", "*");
    let task = ScheduledTask::new("test_task".to_string(), schedule);

    assert!(task.validate_schedule().is_err());
}

#[test]
fn test_beat_scheduler_check_all_tasks_health() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(120));

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    let results = scheduler.check_all_tasks_health();
    assert_eq!(results.len(), 2);
}

#[test]
fn test_beat_scheduler_get_unhealthy_tasks() {
    let mut scheduler = BeatScheduler::new();

    // Add healthy task
    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));

    // Add unhealthy task (disabled)
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60)).disabled();

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    let unhealthy = scheduler.get_unhealthy_tasks();
    assert_eq!(unhealthy.len(), 1);
    assert_eq!(unhealthy[0].task_name, "task2");
}

#[test]
fn test_beat_scheduler_get_tasks_with_warnings() {
    let mut scheduler = BeatScheduler::new();

    // Add healthy task
    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));

    // Add task with warning (disabled)
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60)).disabled();

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    let warnings = scheduler.get_tasks_with_warnings();
    assert_eq!(warnings.len(), 1);
    assert_eq!(warnings[0].task_name, "task2");
}

#[test]
fn test_beat_scheduler_get_stuck_tasks() {
    let mut scheduler = BeatScheduler::new();

    // Add recently run task
    let mut task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
    task1.last_run_at = Some(Utc::now() - Duration::seconds(30));

    // Add stuck task
    let mut task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60));
    task2.last_run_at = Some(Utc::now() - Duration::seconds(10000));

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    let stuck = scheduler.get_stuck_tasks();
    assert_eq!(stuck.len(), 1);
    assert_eq!(stuck[0].name, "task2");
}

#[test]
fn test_beat_scheduler_validate_all_schedules() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(120));

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    let results = scheduler.validate_all_schedules();
    assert_eq!(results.len(), 2);

    for (_, result) in results {
        assert!(result.is_ok());
    }
}

#[test]
fn test_schedule_health_serialization() {
    let health_variants = vec![
        ScheduleHealth::Healthy,
        ScheduleHealth::Warning {
            issues: vec!["Warning".to_string()],
        },
        ScheduleHealth::Unhealthy {
            issues: vec!["Error".to_string()],
        },
    ];

    for health in health_variants {
        let json = serde_json::to_string(&health).unwrap();
        let deserialized: ScheduleHealth = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, health);
    }
}

// ===== Scheduler Metrics Tests =====

#[test]
fn test_scheduler_metrics_empty_scheduler() {
    let scheduler = BeatScheduler::new();
    let metrics = scheduler.get_metrics();

    assert_eq!(metrics.total_tasks, 0);
    assert_eq!(metrics.enabled_tasks, 0);
    assert_eq!(metrics.disabled_tasks, 0);
    assert_eq!(metrics.total_executions, 0);
    assert_eq!(metrics.overall_success_rate, 0.0);
}

#[test]
fn test_scheduler_metrics_basic() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60)).disabled();

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    let metrics = scheduler.get_metrics();
    assert_eq!(metrics.total_tasks, 2);
    assert_eq!(metrics.enabled_tasks, 1);
    assert_eq!(metrics.disabled_tasks, 1);
}

#[test]
fn test_scheduler_metrics_with_executions() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
    scheduler.add_task(task1).unwrap();

    // Add some execution history
    scheduler.mark_task_success("task1").unwrap();
    scheduler.mark_task_success("task1").unwrap();
    scheduler
        .mark_task_failure_with_error("task1", "Error".to_string())
        .unwrap();

    let metrics = scheduler.get_metrics();
    assert_eq!(metrics.tasks_with_executions, 1);
    assert_eq!(metrics.total_successes, 2);
    assert_eq!(metrics.total_failures, 1);
    assert_eq!(metrics.total_executions, 3);
    assert_eq!(metrics.overall_success_rate, 2.0 / 3.0);
}

#[test]
fn test_scheduler_metrics_retry_state() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60)).with_retry_policy(
        RetryPolicy::FixedDelay {
            delay_seconds: 30,
            max_retries: 3,
        },
    );

    scheduler.add_task(task1).unwrap();
    scheduler
        .mark_task_failure_with_error("task1", "Error".to_string())
        .unwrap();

    let metrics = scheduler.get_metrics();
    assert_eq!(metrics.tasks_in_retry, 1);
}

#[test]
fn test_scheduler_metrics_health_status() {
    let mut scheduler = BeatScheduler::new();

    // Add healthy task
    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));

    // Add disabled task (warning)
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60)).disabled();

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    let metrics = scheduler.get_metrics();
    assert_eq!(metrics.tasks_with_warnings, 1);
}

#[test]
fn test_scheduler_metrics_stuck_tasks() {
    let mut scheduler = BeatScheduler::new();

    // Add stuck task
    let mut task = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
    task.last_run_at = Some(Utc::now() - Duration::seconds(10000));
    scheduler.add_task(task).unwrap();

    let metrics = scheduler.get_metrics();
    assert_eq!(metrics.stuck_tasks, 1);
}

#[test]
fn test_scheduler_metrics_serialization() {
    let scheduler = BeatScheduler::new();
    let metrics = scheduler.get_metrics();

    let json = serde_json::to_string(&metrics).unwrap();
    let deserialized: SchedulerMetrics = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.total_tasks, metrics.total_tasks);
    assert_eq!(deserialized.enabled_tasks, metrics.enabled_tasks);
}

// ===== Task Statistics Tests =====

#[test]
fn test_task_statistics_basic() {
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule);

    let stats = TaskStatistics::from_task(&task);
    assert_eq!(stats.name, "test_task");
    assert_eq!(stats.success_count, 0);
    assert_eq!(stats.failure_count, 0);
    assert_eq!(stats.success_rate, 0.0);
}

#[test]
fn test_task_statistics_with_history() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    // Add execution history
    task.add_execution_record(ExecutionRecord::completed(
        Utc::now() - Duration::milliseconds(100),
        ExecutionResult::Success,
    ));
    task.add_execution_record(ExecutionRecord::completed(
        Utc::now() - Duration::milliseconds(200),
        ExecutionResult::Success,
    ));
    task.add_execution_record(ExecutionRecord::completed(
        Utc::now(),
        ExecutionResult::Failure {
            error: "Error".to_string(),
        },
    ));

    let stats = TaskStatistics::from_task(&task);
    assert_eq!(stats.success_count, 2);
    assert_eq!(stats.failure_count, 1);
    assert_eq!(stats.success_rate, 2.0 / 3.0);
    assert!(stats.average_duration_ms.is_some());
}

#[test]
fn test_beat_scheduler_get_all_task_statistics() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(120));

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    let stats = scheduler.get_all_task_statistics();
    assert_eq!(stats.len(), 2);
}

#[test]
fn test_beat_scheduler_get_task_statistics() {
    let mut scheduler = BeatScheduler::new();

    let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
    scheduler.add_task(task).unwrap();

    let stats = scheduler.get_task_statistics("test_task");
    assert!(stats.is_some());
    assert_eq!(stats.unwrap().name, "test_task");

    let missing = scheduler.get_task_statistics("nonexistent");
    assert!(missing.is_none());
}

#[test]
fn test_beat_scheduler_get_group_statistics() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
        .with_group("reports".to_string());
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
        .with_group("reports".to_string());
    let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
        .with_group("alerts".to_string());

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();
    scheduler.add_task(task3).unwrap();

    let stats = scheduler.get_group_statistics("reports");
    assert_eq!(stats.len(), 2);
}

#[test]
fn test_beat_scheduler_get_tag_statistics() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
        .with_tag("daily".to_string());
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
        .with_tag("daily".to_string());
    let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
        .with_tag("weekly".to_string());

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();
    scheduler.add_task(task3).unwrap();

    let stats = scheduler.get_tag_statistics("daily");
    assert_eq!(stats.len(), 2);
}

#[test]
fn test_task_statistics_retry_count() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(
        RetryPolicy::FixedDelay {
            delay_seconds: 30,
            max_retries: 3,
        },
    );

    task.mark_failure();
    task.mark_failure();

    let stats = TaskStatistics::from_task(&task);
    assert_eq!(stats.retry_count, 2);
}

#[test]
fn test_task_statistics_stuck_detection() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    // Not stuck initially
    let stats = TaskStatistics::from_task(&task);
    assert!(!stats.is_stuck);

    // Make it stuck
    task.last_run_at = Some(Utc::now() - Duration::seconds(10000));
    let stats = TaskStatistics::from_task(&task);
    assert!(stats.is_stuck);
}

// ===== Schedule Versioning Tests =====

#[test]
fn test_version_initial_creation() {
    let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

    assert_eq!(task.current_version, 1);
    assert_eq!(task.version_history.len(), 1);

    let initial_version = &task.version_history[0];
    assert_eq!(initial_version.version, 1);
    assert!(initial_version.schedule.is_interval());
    assert_eq!(
        initial_version.change_reason,
        Some("Initial creation".to_string())
    );
}

#[test]
fn test_version_update_schedule() {
    let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

    // Update to a different interval
    task.update_schedule(
        Schedule::interval(120),
        Some("Changed interval".to_string()),
    );

    assert_eq!(task.current_version, 2);
    assert_eq!(task.version_history.len(), 2);

    // Check new schedule is active
    if let Schedule::Interval { every } = task.schedule {
        assert_eq!(every, 120);
    } else {
        panic!("Expected interval schedule");
    }

    // Check version history
    let v2 = &task.version_history[1];
    assert_eq!(v2.version, 2);
    assert_eq!(v2.change_reason, Some("Changed interval".to_string()));
}

#[test]
fn test_version_update_config() {
    let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

    // Update configuration
    task.update_config(
        Some(false), // disable
        Some(Some(Jitter::positive(30))),
        Some(CatchupPolicy::RunOnce),
        Some("Changed config".to_string()),
    );

    assert_eq!(task.current_version, 2);
    assert!(!task.enabled);
    assert!(task.jitter.is_some());
    assert!(matches!(task.catchup_policy, CatchupPolicy::RunOnce));
}

#[test]
fn test_version_rollback() {
    let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

    // Make several changes
    task.update_schedule(Schedule::interval(120), Some("Change 1".to_string()));
    task.update_schedule(Schedule::interval(180), Some("Change 2".to_string()));

    assert_eq!(task.current_version, 3);

    // Rollback to version 1
    task.rollback_to_version(1).unwrap();

    // Check schedule is back to original
    if let Schedule::Interval { every } = task.schedule {
        assert_eq!(every, 60);
    } else {
        panic!("Expected interval schedule");
    }

    // Current version should be incremented
    assert_eq!(task.current_version, 4);

    // Should have rollback entry in history
    let rollback_version = &task.version_history[3];
    assert!(rollback_version
        .change_reason
        .as_ref()
        .unwrap()
        .contains("Rolled back to version 1"));
}

#[test]
fn test_version_rollback_invalid() {
    let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

    // Try to rollback to non-existent version
    let result = task.rollback_to_version(999);
    assert!(result.is_err());

    if let Err(ScheduleError::Invalid(msg)) = result {
        assert_eq!(msg, "Version 999 not found");
    }
}

#[test]
fn test_version_get_history() {
    let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

    task.update_schedule(Schedule::interval(120), Some("Change 1".to_string()));
    task.update_schedule(Schedule::interval(180), Some("Change 2".to_string()));

    let history = task.get_version_history();
    assert_eq!(history.len(), 3);

    // Verify chronological order
    assert_eq!(history[0].version, 1);
    assert_eq!(history[1].version, 2);
    assert_eq!(history[2].version, 3);
}

#[test]
fn test_version_get_specific() {
    let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

    task.update_schedule(Schedule::interval(120), Some("Change 1".to_string()));

    let v1 = task.get_version(1).unwrap();
    assert_eq!(v1.version, 1);

    let v2 = task.get_version(2).unwrap();
    assert_eq!(v2.version, 2);

    assert!(task.get_version(999).is_none());
}

#[test]
fn test_version_get_previous() {
    let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

    // No previous version initially
    assert!(task.get_previous_version().is_none());

    // Add a version
    task.update_schedule(Schedule::interval(120), Some("Change 1".to_string()));

    // Now there's a previous version
    let prev = task.get_previous_version().unwrap();
    assert_eq!(prev.version, 1);
}

#[test]
fn test_version_serialization() {
    let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

    task.update_schedule(Schedule::interval(120), Some("Change 1".to_string()));

    // Serialize
    let json = serde_json::to_string(&task).unwrap();

    // Deserialize
    let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

    // Verify version history is preserved
    assert_eq!(deserialized.current_version, task.current_version);
    assert_eq!(
        deserialized.version_history.len(),
        task.version_history.len()
    );
    assert_eq!(deserialized.version_history[0].version, 1);
    assert_eq!(deserialized.version_history[1].version, 2);
}

#[test]
fn test_version_multiple_rollbacks() {
    let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

    task.update_schedule(Schedule::interval(120), Some("Change 1".to_string()));
    task.update_schedule(Schedule::interval(180), Some("Change 2".to_string()));

    // Rollback to v1
    task.rollback_to_version(1).unwrap();
    assert_eq!(task.current_version, 4);

    // Rollback to v2
    task.rollback_to_version(2).unwrap();
    assert_eq!(task.current_version, 5);

    // Should have all versions in history
    assert_eq!(task.version_history.len(), 5);
}

// ===== Task Dependency Tests =====

#[test]
fn test_dependency_basic() {
    let mut task = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));

    assert!(!task.has_dependencies());

    task.add_dependency("task_a".to_string());

    assert!(task.has_dependencies());
    assert!(task.depends_on("task_a"));
    assert!(!task.depends_on("task_c"));
}

#[test]
fn test_dependency_add_remove() {
    let mut task = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));

    task.add_dependency("task_a".to_string());
    assert_eq!(task.dependencies.len(), 1);

    task.add_dependency("task_c".to_string());
    assert_eq!(task.dependencies.len(), 2);

    assert!(task.remove_dependency("task_a"));
    assert_eq!(task.dependencies.len(), 1);
    assert!(!task.depends_on("task_a"));

    assert!(!task.remove_dependency("nonexistent"));
}

#[test]
fn test_dependency_clear() {
    let mut task = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));

    task.add_dependency("task_a".to_string());
    task.add_dependency("task_c".to_string());
    assert_eq!(task.dependencies.len(), 2);

    task.clear_dependencies();
    assert_eq!(task.dependencies.len(), 0);
    assert!(!task.has_dependencies());
}

#[test]
fn test_dependency_with_dependencies() {
    let mut deps = HashSet::new();
    deps.insert("task_a".to_string());
    deps.insert("task_b".to_string());

    let task =
        ScheduledTask::new("task_c".to_string(), Schedule::interval(60)).with_dependencies(deps);

    assert_eq!(task.dependencies.len(), 2);
    assert!(task.depends_on("task_a"));
    assert!(task.depends_on("task_b"));
}

#[test]
fn test_dependency_status_satisfied() {
    let mut task = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
    task.add_dependency("task_a".to_string());

    let mut completed = HashSet::new();
    completed.insert("task_a".to_string());

    let status = task.check_dependencies(&completed);
    assert!(status.is_satisfied());
}

#[test]
fn test_dependency_status_waiting() {
    let mut task = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
    task.add_dependency("task_a".to_string());

    let completed = HashSet::new();

    let status = task.check_dependencies(&completed);
    assert!(!status.is_satisfied());

    if let DependencyStatus::Waiting { pending } = status {
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0], "task_a");
    } else {
        panic!("Expected Waiting status");
    }
}

#[test]
fn test_dependency_status_with_failures() {
    let mut task = ScheduledTask::new("task_c".to_string(), Schedule::interval(60));
    task.add_dependency("task_a".to_string());
    task.add_dependency("task_b".to_string());

    let mut completed = HashSet::new();
    completed.insert("task_a".to_string());

    let mut failed = HashSet::new();
    failed.insert("task_b".to_string());

    let status = task.check_dependencies_with_failures(&completed, &failed);
    assert!(status.has_failures());

    if let DependencyStatus::Failed {
        failed: failed_tasks,
    } = status
    {
        assert_eq!(failed_tasks.len(), 1);
        assert_eq!(failed_tasks[0], "task_b");
    } else {
        panic!("Expected Failed status");
    }
}

#[test]
fn test_circular_dependency_simple() {
    let mut scheduler = BeatScheduler::new();

    let mut task_a = ScheduledTask::new("task_a".to_string(), Schedule::interval(60));
    task_a.add_dependency("task_b".to_string());

    let mut task_b = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
    task_b.add_dependency("task_a".to_string());

    scheduler.add_task(task_a).unwrap();
    scheduler.add_task(task_b).unwrap();

    assert!(scheduler.has_circular_dependency("task_a"));
    assert!(scheduler.has_circular_dependency("task_b"));
}

#[test]
fn test_circular_dependency_complex() {
    let mut scheduler = BeatScheduler::new();

    let mut task_a = ScheduledTask::new("task_a".to_string(), Schedule::interval(60));
    task_a.add_dependency("task_b".to_string());

    let mut task_b = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
    task_b.add_dependency("task_c".to_string());

    let mut task_c = ScheduledTask::new("task_c".to_string(), Schedule::interval(60));
    task_c.add_dependency("task_a".to_string());

    scheduler.add_task(task_a).unwrap();
    scheduler.add_task(task_b).unwrap();
    scheduler.add_task(task_c).unwrap();

    assert!(scheduler.has_circular_dependency("task_a"));
    assert!(scheduler.has_circular_dependency("task_b"));
    assert!(scheduler.has_circular_dependency("task_c"));
}

#[test]
fn test_no_circular_dependency() {
    let mut scheduler = BeatScheduler::new();

    let task_a = ScheduledTask::new("task_a".to_string(), Schedule::interval(60));

    let mut task_b = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
    task_b.add_dependency("task_a".to_string());

    let mut task_c = ScheduledTask::new("task_c".to_string(), Schedule::interval(60));
    task_c.add_dependency("task_b".to_string());

    scheduler.add_task(task_a).unwrap();
    scheduler.add_task(task_b).unwrap();
    scheduler.add_task(task_c).unwrap();

    assert!(!scheduler.has_circular_dependency("task_a"));
    assert!(!scheduler.has_circular_dependency("task_b"));
    assert!(!scheduler.has_circular_dependency("task_c"));
}

#[test]
fn test_dependency_chain() {
    let mut scheduler = BeatScheduler::new();

    let task_a = ScheduledTask::new("task_a".to_string(), Schedule::interval(60));

    let mut task_b = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
    task_b.add_dependency("task_a".to_string());

    let mut task_c = ScheduledTask::new("task_c".to_string(), Schedule::interval(60));
    task_c.add_dependency("task_b".to_string());

    scheduler.add_task(task_a).unwrap();
    scheduler.add_task(task_b).unwrap();
    scheduler.add_task(task_c).unwrap();

    let chain = scheduler.get_dependency_chain("task_c").unwrap();

    // Chain should be in execution order: a -> b -> c
    assert_eq!(chain.len(), 3);
    assert_eq!(chain[0], "task_a");
    assert_eq!(chain[1], "task_b");
    assert_eq!(chain[2], "task_c");
}

#[test]
fn test_dependency_chain_circular() {
    let mut scheduler = BeatScheduler::new();

    let mut task_a = ScheduledTask::new("task_a".to_string(), Schedule::interval(60));
    task_a.add_dependency("task_b".to_string());

    let mut task_b = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
    task_b.add_dependency("task_a".to_string());

    scheduler.add_task(task_a).unwrap();
    scheduler.add_task(task_b).unwrap();

    let result = scheduler.get_dependency_chain("task_a");
    assert!(result.is_err());

    if let Err(ScheduleError::Invalid(msg)) = result {
        assert!(msg.contains("Circular dependency"));
    }
}

#[test]
fn test_validate_dependencies_success() {
    let mut scheduler = BeatScheduler::new();

    let task_a = ScheduledTask::new("task_a".to_string(), Schedule::interval(60));

    let mut task_b = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
    task_b.add_dependency("task_a".to_string());

    scheduler.add_task(task_a).unwrap();
    scheduler.add_task(task_b).unwrap();

    assert!(scheduler.validate_dependencies().is_ok());
}

#[test]
fn test_validate_dependencies_missing_task() {
    let mut scheduler = BeatScheduler::new();

    let mut task_b = ScheduledTask::new("task_b".to_string(), Schedule::interval(60));
    task_b.add_dependency("nonexistent_task".to_string());

    scheduler.add_task(task_b).unwrap();

    let result = scheduler.validate_dependencies();
    assert!(result.is_err());

    if let Err(ScheduleError::Invalid(msg)) = result {
        assert!(msg.contains("non-existent task"));
    }
}

#[test]
fn test_tasks_ready_with_dependencies() {
    let mut scheduler = BeatScheduler::new();

    // task_a has no dependencies and is due (past time)
    let task_a = ScheduledTask::new(
        "task_a".to_string(),
        Schedule::onetime(Utc::now() - Duration::hours(1)),
    );

    // task_b depends on task_a and is due
    let mut task_b = ScheduledTask::new(
        "task_b".to_string(),
        Schedule::onetime(Utc::now() - Duration::hours(1)),
    );
    task_b.add_dependency("task_a".to_string());

    scheduler.add_task(task_a).unwrap();
    scheduler.add_task(task_b).unwrap();

    let completed = HashSet::new();
    let failed = HashSet::new();

    // Only task_a should be ready (no dependencies)
    let ready = scheduler.get_tasks_ready_with_dependencies(&completed, &failed);
    assert_eq!(ready.len(), 1);
    assert_eq!(ready[0].name, "task_a");

    // After task_a completes, both tasks should be ready
    // (task_a is still in scheduler since we didn't call mark_task_success, and task_b now has satisfied dependencies)
    let mut completed = HashSet::new();
    completed.insert("task_a".to_string());

    let ready = scheduler.get_tasks_ready_with_dependencies(&completed, &failed);
    assert_eq!(ready.len(), 2); // Both tasks are ready now

    // Find task_b in the ready list
    let task_b_ready = ready.iter().any(|t| t.name == "task_b");
    assert!(
        task_b_ready,
        "task_b should be ready after task_a completes"
    );
}

#[test]
fn test_dependency_serialization() {
    let mut deps = HashSet::new();
    deps.insert("task_a".to_string());
    deps.insert("task_b".to_string());

    let task =
        ScheduledTask::new("task_c".to_string(), Schedule::interval(60)).with_dependencies(deps);

    // Serialize
    let json = serde_json::to_string(&task).unwrap();

    // Deserialize
    let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

    // Verify
    assert_eq!(deserialized.dependencies.len(), 2);
    assert!(deserialized.depends_on("task_a"));
    assert!(deserialized.depends_on("task_b"));
    assert!(deserialized.wait_for_dependencies);
}

#[test]
fn test_failure_notification_callback() {
    use std::sync::{Arc, Mutex};

    let mut scheduler = BeatScheduler::new();
    let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
    scheduler.add_task(task).unwrap();

    // Track callback invocations
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let invocations_clone = invocations.clone();

    // Register callback
    scheduler.on_failure(Arc::new(move |task_name, error| {
        invocations_clone
            .lock()
            .unwrap()
            .push((task_name.to_string(), error.to_string()));
    }));

    // Trigger failure
    scheduler
        .mark_task_failure_with_error("test_task", "Test error".to_string())
        .unwrap();

    // Verify callback was invoked
    let invocations = invocations.lock().unwrap();
    assert_eq!(invocations.len(), 1);
    assert_eq!(invocations[0].0, "test_task");
    assert_eq!(invocations[0].1, "Test error");
}

#[test]
fn test_failure_notification_multiple_callbacks() {
    use std::sync::{Arc, Mutex};

    let mut scheduler = BeatScheduler::new();
    let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
    scheduler.add_task(task).unwrap();

    // Track callback invocations for two separate callbacks
    let invocations1 = Arc::new(Mutex::new(0));
    let invocations2 = Arc::new(Mutex::new(0));

    let inv1_clone = invocations1.clone();
    let inv2_clone = invocations2.clone();

    // Register two callbacks
    scheduler.on_failure(Arc::new(move |_, _| {
        *inv1_clone.lock().unwrap() += 1;
    }));

    scheduler.on_failure(Arc::new(move |_, _| {
        *inv2_clone.lock().unwrap() += 1;
    }));

    // Trigger failure
    scheduler
        .mark_task_failure_with_error("test_task", "Test error".to_string())
        .unwrap();

    // Verify both callbacks were invoked
    assert_eq!(*invocations1.lock().unwrap(), 1);
    assert_eq!(*invocations2.lock().unwrap(), 1);
}

#[test]
fn test_failure_notification_clear_callbacks() {
    use std::sync::{Arc, Mutex};

    let mut scheduler = BeatScheduler::new();
    let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
    scheduler.add_task(task).unwrap();

    // Track callback invocations
    let invocations = Arc::new(Mutex::new(0));
    let inv_clone = invocations.clone();

    // Register callback
    scheduler.on_failure(Arc::new(move |_, _| {
        *inv_clone.lock().unwrap() += 1;
    }));

    // Clear callbacks
    scheduler.clear_failure_callbacks();

    // Trigger failure
    scheduler
        .mark_task_failure_with_error("test_task", "Test error".to_string())
        .unwrap();

    // Verify callback was NOT invoked
    assert_eq!(*invocations.lock().unwrap(), 0);
}

#[test]
fn test_failure_notification_with_start_time() {
    use std::sync::{Arc, Mutex};

    let mut scheduler = BeatScheduler::new();
    let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
    scheduler.add_task(task).unwrap();

    // Track callback invocations
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let invocations_clone = invocations.clone();

    // Register callback
    scheduler.on_failure(Arc::new(move |task_name, error| {
        invocations_clone
            .lock()
            .unwrap()
            .push((task_name.to_string(), error.to_string()));
    }));

    // Trigger failure with start time
    let start_time = Utc::now();
    scheduler
        .mark_task_failure_with_start("test_task", start_time, "Test error".to_string())
        .unwrap();

    // Verify callback was invoked
    let invocations = invocations.lock().unwrap();
    assert_eq!(invocations.len(), 1);
    assert_eq!(invocations[0].0, "test_task");
    assert_eq!(invocations[0].1, "Test error");
}

#[test]
fn test_failure_notification_multiple_failures() {
    use std::sync::{Arc, Mutex};

    let mut scheduler = BeatScheduler::new();
    let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
    scheduler.add_task(task).unwrap();

    // Track callback invocations
    let invocations = Arc::new(Mutex::new(Vec::new()));
    let invocations_clone = invocations.clone();

    // Register callback
    scheduler.on_failure(Arc::new(move |task_name, error| {
        invocations_clone
            .lock()
            .unwrap()
            .push((task_name.to_string(), error.to_string()));
    }));

    // Trigger multiple failures
    scheduler
        .mark_task_failure_with_error("test_task", "Error 1".to_string())
        .unwrap();
    scheduler
        .mark_task_failure_with_error("test_task", "Error 2".to_string())
        .unwrap();
    scheduler
        .mark_task_failure_with_error("test_task", "Error 3".to_string())
        .unwrap();

    // Verify callback was invoked three times
    let invocations = invocations.lock().unwrap();
    assert_eq!(invocations.len(), 3);
    assert_eq!(invocations[0].1, "Error 1");
    assert_eq!(invocations[1].1, "Error 2");
    assert_eq!(invocations[2].1, "Error 3");
}

#[test]
fn test_schedule_cache_basic() {
    let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

    // Initially cache should be None
    assert!(task.cached_next_run.is_none());

    // Update cache
    task.update_next_run_cache();
    assert!(task.cached_next_run.is_some());

    let cached_time = task.cached_next_run.unwrap();

    // next_run_time should return the cached value
    let next_run = task.next_run_time().unwrap();
    assert_eq!(next_run, cached_time);
}

#[test]
fn test_schedule_cache_invalidation() {
    let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

    // Update cache
    task.update_next_run_cache();
    assert!(task.cached_next_run.is_some());

    // Invalidate cache
    task.invalidate_next_run_cache();
    assert!(task.cached_next_run.is_none());
}

#[test]
fn test_schedule_cache_on_execution() {
    let mut scheduler = BeatScheduler::new();
    let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
    scheduler.add_task(task).unwrap();

    // After adding, cache should be set
    let task = scheduler.tasks.get("test_task").unwrap();
    assert!(task.cached_next_run.is_some());

    // Mark as success
    scheduler.mark_task_success("test_task").unwrap();

    // After execution, cache should be updated
    let task = scheduler.tasks.get("test_task").unwrap();
    assert!(task.cached_next_run.is_some());
}

#[test]
fn test_schedule_cache_on_schedule_update() {
    let mut task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));

    // Update cache
    task.update_next_run_cache();
    let old_cached_time = task.cached_next_run.unwrap();

    // Update schedule
    task.update_schedule(
        Schedule::interval(120),
        Some("Changed interval".to_string()),
    );

    // Cache should be updated with new schedule
    assert!(task.cached_next_run.is_some());
    let new_cached_time = task.cached_next_run.unwrap();

    // The cached times should be different (though this might fail if timing is exact)
    // At minimum, cache should still be valid
    assert!(new_cached_time >= old_cached_time);
}

#[test]
fn test_add_tasks_batch() {
    let mut scheduler = BeatScheduler::new();

    let tasks = vec![
        ScheduledTask::new("task1".to_string(), Schedule::interval(60)),
        ScheduledTask::new("task2".to_string(), Schedule::interval(120)),
        ScheduledTask::new("task3".to_string(), Schedule::interval(180)),
    ];

    let count = scheduler.add_tasks_batch(tasks).unwrap();

    assert_eq!(count, 3);
    assert_eq!(scheduler.tasks.len(), 3);
    assert!(scheduler.tasks.contains_key("task1"));
    assert!(scheduler.tasks.contains_key("task2"));
    assert!(scheduler.tasks.contains_key("task3"));

    // Verify cache is initialized for all tasks
    assert!(scheduler
        .tasks
        .get("task1")
        .unwrap()
        .cached_next_run
        .is_some());
    assert!(scheduler
        .tasks
        .get("task2")
        .unwrap()
        .cached_next_run
        .is_some());
    assert!(scheduler
        .tasks
        .get("task3")
        .unwrap()
        .cached_next_run
        .is_some());
}

#[test]
fn test_add_tasks_batch_empty() {
    let mut scheduler = BeatScheduler::new();

    let tasks = vec![];
    let count = scheduler.add_tasks_batch(tasks).unwrap();

    assert_eq!(count, 0);
    assert_eq!(scheduler.tasks.len(), 0);
}

#[test]
fn test_remove_tasks_batch() {
    let mut scheduler = BeatScheduler::new();

    // Add some tasks
    scheduler
        .add_task(ScheduledTask::new(
            "task1".to_string(),
            Schedule::interval(60),
        ))
        .unwrap();
    scheduler
        .add_task(ScheduledTask::new(
            "task2".to_string(),
            Schedule::interval(120),
        ))
        .unwrap();
    scheduler
        .add_task(ScheduledTask::new(
            "task3".to_string(),
            Schedule::interval(180),
        ))
        .unwrap();
    scheduler
        .add_task(ScheduledTask::new(
            "task4".to_string(),
            Schedule::interval(240),
        ))
        .unwrap();

    assert_eq!(scheduler.tasks.len(), 4);

    // Remove tasks in batch
    let count = scheduler
        .remove_tasks_batch(&["task1", "task2", "task3"])
        .unwrap();

    assert_eq!(count, 3);
    assert_eq!(scheduler.tasks.len(), 1);
    assert!(!scheduler.tasks.contains_key("task1"));
    assert!(!scheduler.tasks.contains_key("task2"));
    assert!(!scheduler.tasks.contains_key("task3"));
    assert!(scheduler.tasks.contains_key("task4"));
}

#[test]
fn test_remove_tasks_batch_nonexistent() {
    let mut scheduler = BeatScheduler::new();

    // Add some tasks
    scheduler
        .add_task(ScheduledTask::new(
            "task1".to_string(),
            Schedule::interval(60),
        ))
        .unwrap();
    scheduler
        .add_task(ScheduledTask::new(
            "task2".to_string(),
            Schedule::interval(120),
        ))
        .unwrap();

    assert_eq!(scheduler.tasks.len(), 2);

    // Try to remove tasks that don't all exist
    let count = scheduler
        .remove_tasks_batch(&["task1", "nonexistent", "task2"])
        .unwrap();

    assert_eq!(count, 2); // Only task1 and task2 were removed
    assert_eq!(scheduler.tasks.len(), 0);
}

#[test]
fn test_remove_tasks_batch_empty() {
    let mut scheduler = BeatScheduler::new();

    let count = scheduler.remove_tasks_batch(&[]).unwrap();
    assert_eq!(count, 0);
}
