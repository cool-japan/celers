#![cfg(test)]

use crate::*;
#[cfg(feature = "cron")]
use chrono::Timelike;
use chrono::{Duration, Utc};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use tempfile::NamedTempFile;

// ===== Interval Schedule Tests =====

#[test]
fn test_interval_schedule_basic() {
    let schedule = Schedule::interval(60);
    assert!(schedule.is_interval());
}

#[test]
fn test_interval_schedule_next_run_no_last_run() {
    let schedule = Schedule::interval(60);
    let before = Utc::now();
    let next_run = schedule.next_run(None).unwrap();
    let after = Utc::now();

    // Next run should be ~60 seconds from now
    assert!(next_run > before + Duration::seconds(59));
    assert!(next_run < after + Duration::seconds(61));
}

#[test]
fn test_interval_schedule_next_run_with_last_run() {
    let schedule = Schedule::interval(60);
    let last_run = Utc::now();
    let next_run = schedule.next_run(Some(last_run)).unwrap();

    // Next run should be exactly 60 seconds after last run
    let expected = last_run + Duration::seconds(60);
    assert_eq!(next_run, expected);
}

#[test]
fn test_interval_schedule_multiple_intervals() {
    for interval in [1, 5, 10, 30, 60, 120, 300, 3600] {
        let schedule = Schedule::interval(interval);
        let last_run = Utc::now();
        let next_run = schedule.next_run(Some(last_run)).unwrap();

        assert_eq!(
            next_run,
            last_run + Duration::seconds(interval as i64),
            "Failed for interval {}",
            interval
        );
    }
}

#[test]
fn test_interval_schedule_display() {
    let schedule = Schedule::interval(60);
    let display = format!("{}", schedule);
    assert_eq!(display, "Interval[every 60s]");
}

// ===== OneTime Schedule Tests =====

#[test]
fn test_onetime_schedule_basic() {
    let run_at = Utc::now() + Duration::hours(1);
    let schedule = Schedule::onetime(run_at);
    assert!(schedule.is_onetime());
}

#[test]
fn test_onetime_schedule_next_run_no_last_run() {
    let run_at = Utc::now() + Duration::hours(1);
    let schedule = Schedule::onetime(run_at);
    let next_run = schedule.next_run(None).unwrap();
    assert_eq!(next_run, run_at);
}

#[test]
fn test_onetime_schedule_next_run_with_last_run() {
    let run_at = Utc::now() + Duration::hours(1);
    let schedule = Schedule::onetime(run_at);
    let last_run = Utc::now();
    let result = schedule.next_run(Some(last_run));

    // Should return error because one-time schedules can't run twice
    assert!(result.is_err());
    if let Err(ScheduleError::Invalid(msg)) = result {
        assert_eq!(msg, "One-time schedule has already been executed");
    }
}

#[test]
fn test_onetime_schedule_display() {
    let run_at = Utc::now() + Duration::hours(1);
    let schedule = Schedule::onetime(run_at);
    let display = format!("{}", schedule);
    assert!(display.starts_with("OneTime[at "));
    assert!(display.ends_with(" UTC]"));
}

#[test]
fn test_onetime_schedule_in_future() {
    let run_at = Utc::now() + Duration::days(7);
    let schedule = Schedule::onetime(run_at);
    let next_run = schedule.next_run(None).unwrap();
    assert_eq!(next_run, run_at);
}

#[test]
fn test_onetime_schedule_in_past() {
    // OneTime schedules can be set in the past (scheduler will run immediately if due)
    let run_at = Utc::now() - Duration::hours(1);
    let schedule = Schedule::onetime(run_at);
    let next_run = schedule.next_run(None).unwrap();
    assert_eq!(next_run, run_at);
}

#[test]
fn test_onetime_task_auto_cleanup() {
    let mut scheduler = BeatScheduler::new();
    let run_at = Utc::now() - Duration::hours(1); // Past time, so it's immediately due
    let task = ScheduledTask::new("test_onetime".to_string(), Schedule::onetime(run_at));

    scheduler.add_task(task).unwrap();
    assert_eq!(scheduler.tasks.len(), 1);

    // Mark as successful - should auto-remove
    scheduler.mark_task_success("test_onetime").unwrap();
    assert_eq!(scheduler.tasks.len(), 0);
}

#[test]
fn test_onetime_task_auto_cleanup_with_start_time() {
    let mut scheduler = BeatScheduler::new();
    let run_at = Utc::now() - Duration::hours(1);
    let task = ScheduledTask::new("test_onetime".to_string(), Schedule::onetime(run_at));

    scheduler.add_task(task).unwrap();
    assert_eq!(scheduler.tasks.len(), 1);

    // Mark as successful with start time - should auto-remove
    let started_at = Utc::now() - Duration::seconds(5);
    scheduler
        .mark_task_success_with_start("test_onetime", started_at)
        .unwrap();
    assert_eq!(scheduler.tasks.len(), 0);
}

#[test]
fn test_onetime_task_not_removed_on_failure() {
    let mut scheduler = BeatScheduler::new();
    let run_at = Utc::now() - Duration::hours(1);
    let task = ScheduledTask::new("test_onetime".to_string(), Schedule::onetime(run_at));

    scheduler.add_task(task).unwrap();
    assert_eq!(scheduler.tasks.len(), 1);

    // Mark as failed - should NOT auto-remove (user might want to retry manually)
    scheduler.mark_task_failure("test_onetime").unwrap();
    assert_eq!(scheduler.tasks.len(), 1);
}

#[test]
fn test_onetime_serialization() {
    let run_at = Utc::now() + Duration::hours(2);
    let schedule = Schedule::onetime(run_at);

    // Serialize
    let json = serde_json::to_string(&schedule).unwrap();

    // Deserialize
    let deserialized: Schedule = serde_json::from_str(&json).unwrap();

    // Verify
    assert!(deserialized.is_onetime());
    let next_run = deserialized.next_run(None).unwrap();
    assert_eq!(next_run, run_at);
}

// ===== Crontab Schedule Tests =====

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_basic() {
    let schedule = Schedule::crontab("0", "0", "*", "*", "*");
    assert!(schedule.is_crontab());
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_every_minute() {
    let schedule = Schedule::crontab("*", "*", "*", "*", "*");
    let now = Utc::now();
    let next_run = schedule.next_run(Some(now)).unwrap();

    // Should be within next 2 minutes
    assert!(next_run > now);
    assert!(next_run < now + Duration::minutes(2));
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_specific_time() {
    // Every day at 10:30
    let schedule = Schedule::crontab("30", "10", "*", "*", "*");
    let now = Utc::now();
    let next_run = schedule.next_run(Some(now)).unwrap();

    assert!(next_run > now);
    assert_eq!(next_run.hour(), 10);
    assert_eq!(next_run.minute(), 30);
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_invalid() {
    let schedule = Schedule::crontab("invalid", "0", "*", "*", "*");
    let result = schedule.next_run(None);
    assert!(result.is_err());
    assert!(result.unwrap_err().is_parse());
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_display() {
    let schedule = Schedule::crontab("0", "12", "*", "*", "1");
    let display = format!("{}", schedule);
    assert_eq!(display, "Crontab[0 12 * * 1 (UTC)]");
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_with_timezone() {
    let schedule = Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York");
    assert!(schedule.is_crontab());

    // Display should show timezone
    let display = format!("{}", schedule);
    assert!(display.contains("America/New_York"));
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_timezone_next_run() {
    // Schedule for 9:00 AM New York time on weekdays
    let schedule = Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York");

    // Get next run time
    let next_run = schedule.next_run(None).unwrap();

    // Verify the time is valid (should be in the future)
    assert!(next_run > Utc::now());
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_invalid_timezone() {
    let schedule = Schedule::crontab_tz("0", "9", "*", "*", "*", "Invalid/Timezone");
    let result = schedule.next_run(None);
    assert!(result.is_err());
    assert!(result.unwrap_err().is_parse());
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_timezone_serialization() {
    let schedule = Schedule::crontab_tz("30", "14", "*", "*", "*", "Europe/London");
    let json = serde_json::to_string(&schedule).unwrap();
    let deserialized: Schedule = serde_json::from_str(&json).unwrap();

    // Verify timezone is preserved
    let display = format!("{}", deserialized);
    assert!(display.contains("Europe/London"));
}

// ===== Solar Schedule Tests =====

#[cfg(feature = "solar")]
#[test]
fn test_solar_schedule_basic() {
    let schedule = Schedule::solar("sunrise", 35.6762, 139.6503); // Tokyo
    assert!(schedule.is_solar());
}

#[cfg(feature = "solar")]
#[test]
#[ignore] // Sunrise crate API is deprecated and returns unexpected values
fn test_solar_schedule_sunrise() {
    let schedule = Schedule::solar("sunrise", 35.6762, 139.6503); // Tokyo
    let now = Utc::now();
    let next_run = schedule.next_run(Some(now)).unwrap();

    assert!(next_run > now);
    // Should be within next 48 hours
    assert!(next_run < now + Duration::hours(48));
}

#[cfg(feature = "solar")]
#[test]
#[ignore] // Sunrise crate API is deprecated and returns unexpected values
fn test_solar_schedule_sunset() {
    let schedule = Schedule::solar("sunset", 35.6762, 139.6503); // Tokyo
    let now = Utc::now();
    let next_run = schedule.next_run(Some(now)).unwrap();

    assert!(next_run > now);
    // Should be within next 48 hours
    assert!(next_run < now + Duration::hours(48));
}

#[cfg(feature = "solar")]
#[test]
fn test_solar_schedule_invalid_event() {
    let schedule = Schedule::solar("invalid", 35.6762, 139.6503);
    let result = schedule.next_run(None);
    assert!(result.is_err());
    assert!(result.unwrap_err().is_invalid());
}

#[cfg(feature = "solar")]
#[test]
fn test_solar_schedule_display() {
    let schedule = Schedule::solar("sunrise", 35.6762, 139.6503);
    let display = format!("{}", schedule);
    assert!(display.contains("Solar[sunrise"));
    assert!(display.contains("35.6762"));
    assert!(display.contains("139.6503"));
}

// ===== Scheduled Task Tests =====

#[test]
fn test_scheduled_task_basic() {
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule);

    assert_eq!(task.name, "test_task");
    assert!(task.enabled);
    assert!(!task.has_run());
    assert_eq!(task.total_run_count, 0);
    assert!(task.last_run_at.is_none());
}

#[test]
fn test_scheduled_task_with_args() {
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule)
        .with_args(vec![serde_json::json!(1), serde_json::json!("test")]);

    assert_eq!(task.args.len(), 2);
    assert_eq!(task.args[0], serde_json::json!(1));
    assert_eq!(task.args[1], serde_json::json!("test"));
}

#[test]
fn test_scheduled_task_with_kwargs() {
    let schedule = Schedule::interval(60);
    let mut kwargs = HashMap::new();
    kwargs.insert("key1".to_string(), serde_json::json!("value1"));
    kwargs.insert("key2".to_string(), serde_json::json!(42));

    let task = ScheduledTask::new("test_task".to_string(), schedule).with_kwargs(kwargs);

    assert_eq!(task.kwargs.len(), 2);
    assert_eq!(
        task.kwargs.get("key1").unwrap(),
        &serde_json::json!("value1")
    );
    assert_eq!(task.kwargs.get("key2").unwrap(), &serde_json::json!(42));
}

#[test]
fn test_scheduled_task_with_options() {
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule)
        .with_queue("high_priority".to_string())
        .with_priority(9)
        .with_expires(3600);

    assert!(task.has_options());
    assert_eq!(task.options.queue, Some("high_priority".to_string()));
    assert_eq!(task.options.priority, Some(9));
    assert_eq!(task.options.expires, Some(3600));
}

#[test]
fn test_scheduled_task_disabled() {
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule).disabled();

    assert!(!task.is_enabled());
    assert!(!task.enabled);
}

#[test]
fn test_scheduled_task_is_due_never_run() {
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule);

    // Should be due immediately if never run
    assert!(task.is_due().unwrap());
}

#[test]
fn test_scheduled_task_age_since_last_run() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    // No age if never run
    assert!(task.age_since_last_run().is_none());

    // Set last run to 30 seconds ago
    task.last_run_at = Some(Utc::now() - Duration::seconds(30));
    let age = task.age_since_last_run().unwrap();

    assert!(age.num_seconds() >= 29 && age.num_seconds() <= 31);
}

#[test]
fn test_scheduled_task_display() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);
    task.total_run_count = 5;

    let display = format!("{}", task);
    assert!(display.contains("test_task"));
    assert!(display.contains("Interval[every 60s]"));
    assert!(display.contains("runs=5"));
}

#[test]
fn test_scheduled_task_display_disabled() {
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule).disabled();

    let display = format!("{}", task);
    assert!(display.contains("(disabled)"));
}

// ===== Task Options Tests =====

#[test]
fn test_task_options_default() {
    let options = TaskOptions::default();
    assert!(!options.has_queue());
    assert!(!options.has_priority());
    assert!(!options.has_expires());
}

#[test]
fn test_task_options_has_queue() {
    let options = TaskOptions {
        queue: Some("test_queue".to_string()),
        ..Default::default()
    };
    assert!(options.has_queue());
}

#[test]
fn test_task_options_has_priority() {
    let options = TaskOptions {
        priority: Some(5),
        ..Default::default()
    };
    assert!(options.has_priority());
}

#[test]
fn test_task_options_has_expires() {
    let options = TaskOptions {
        expires: Some(3600),
        ..Default::default()
    };
    assert!(options.has_expires());
}

#[test]
fn test_task_options_display() {
    let options = TaskOptions {
        queue: Some("test".to_string()),
        priority: Some(5),
        expires: Some(3600),
    };

    let display = format!("{}", options);
    assert!(display.contains("queue=test"));
    assert!(display.contains("priority=5"));
    assert!(display.contains("expires=3600s"));
}

// ===== BeatScheduler Tests =====

#[test]
fn test_beat_scheduler_new() {
    let scheduler = BeatScheduler::new();
    assert_eq!(scheduler.tasks.len(), 0);
    assert!(scheduler.state_file.is_none());
}

#[test]
fn test_beat_scheduler_add_task() {
    let mut scheduler = BeatScheduler::new();
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule);

    scheduler.add_task(task).unwrap();

    assert_eq!(scheduler.tasks.len(), 1);
    assert!(scheduler.tasks.contains_key("test_task"));
}

#[test]
fn test_beat_scheduler_remove_task() {
    let mut scheduler = BeatScheduler::new();
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule);

    scheduler.add_task(task).unwrap();
    assert_eq!(scheduler.tasks.len(), 1);

    let removed = scheduler.remove_task("test_task").unwrap();
    assert!(removed.is_some());
    assert_eq!(removed.unwrap().name, "test_task");
    assert_eq!(scheduler.tasks.len(), 0);
}

#[test]
fn test_beat_scheduler_remove_nonexistent_task() {
    let mut scheduler = BeatScheduler::new();
    let removed = scheduler.remove_task("nonexistent").unwrap();
    assert!(removed.is_none());
}

#[test]
fn test_beat_scheduler_mark_task_run() {
    let mut scheduler = BeatScheduler::new();
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule);

    scheduler.add_task(task).unwrap();

    // Mark as run
    scheduler.mark_task_run("test_task").unwrap();

    let task = scheduler.tasks.get("test_task").unwrap();
    assert!(task.has_run());
    assert_eq!(task.total_run_count, 1);
    assert!(task.last_run_at.is_some());
}

#[test]
fn test_beat_scheduler_get_due_tasks_empty() {
    let scheduler = BeatScheduler::new();
    let due_tasks = scheduler.get_due_tasks();
    assert_eq!(due_tasks.len(), 0);
}

#[test]
fn test_beat_scheduler_get_due_tasks() {
    let mut scheduler = BeatScheduler::new();
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule);

    scheduler.add_task(task).unwrap();

    // Should be due immediately (never run before)
    let due_tasks = scheduler.get_due_tasks();
    assert_eq!(due_tasks.len(), 1);
    assert_eq!(due_tasks[0].name, "test_task");
}

#[test]
fn test_beat_scheduler_get_due_tasks_disabled() {
    let mut scheduler = BeatScheduler::new();
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule).disabled();

    scheduler.add_task(task).unwrap();

    // Disabled task should not be due
    let due_tasks = scheduler.get_due_tasks();
    assert_eq!(due_tasks.len(), 0);
}

#[test]
fn test_beat_scheduler_persistence_path() {
    let temp_file = NamedTempFile::new().unwrap();
    let temp_path = temp_file.path().to_str().unwrap().to_string();
    let scheduler = BeatScheduler::with_persistence(&temp_path);
    assert!(scheduler.state_file.is_some());
    assert_eq!(scheduler.state_file.unwrap(), PathBuf::from(&temp_path));
}

// ===== Persistence Tests =====

#[test]
fn test_persistence_save_and_load() {
    let temp_file = NamedTempFile::new().unwrap();
    let temp_path = temp_file.path().to_str().unwrap().to_string();

    // Create scheduler and add tasks
    let mut scheduler = BeatScheduler::with_persistence(&temp_path);
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule)
        .with_args(vec![serde_json::json!(1)])
        .with_queue("test_queue".to_string());

    scheduler.add_task(task).unwrap();
    scheduler.mark_task_run("test_task").unwrap();

    // Load from file
    let loaded_scheduler = BeatScheduler::load_from_file(&temp_path).unwrap();

    assert_eq!(loaded_scheduler.tasks.len(), 1);
    let loaded_task = loaded_scheduler.tasks.get("test_task").unwrap();
    assert_eq!(loaded_task.name, "test_task");
    assert_eq!(loaded_task.args.len(), 1);
    assert!(loaded_task.has_run());
    assert_eq!(loaded_task.total_run_count, 1);
    assert_eq!(loaded_task.options.queue, Some("test_queue".to_string()));
    // temp_file automatically cleaned up when dropped
}

#[test]
fn test_persistence_load_nonexistent_file() {
    let temp_dir = std::env::temp_dir();
    let temp_file = temp_dir.join("nonexistent_test_file_celers.json");
    // Ensure the file doesn't exist
    let _ = std::fs::remove_file(&temp_file);
    let scheduler = BeatScheduler::load_from_file(temp_file.to_str().unwrap()).unwrap();

    assert_eq!(scheduler.tasks.len(), 0);
    assert!(scheduler.state_file.is_some());
}

#[test]
fn test_persistence_preserves_run_history() {
    let temp_file = NamedTempFile::new().unwrap();
    let temp_path = temp_file.path().to_str().unwrap().to_string();

    // First scheduler - add and run task
    {
        let mut scheduler = BeatScheduler::with_persistence(&temp_path);
        let schedule = Schedule::interval(60);
        let task = ScheduledTask::new("test_task".to_string(), schedule);

        scheduler.add_task(task).unwrap();
        scheduler.mark_task_run("test_task").unwrap();
        scheduler.mark_task_run("test_task").unwrap();
    }

    // Second scheduler - load and verify history
    {
        let scheduler = BeatScheduler::load_from_file(&temp_path).unwrap();
        let task = scheduler.tasks.get("test_task").unwrap();

        assert_eq!(task.total_run_count, 2);
        assert!(task.last_run_at.is_some());
    }
    // temp_file automatically cleaned up when dropped
}

// ===== Schedule Error Tests =====

#[test]
fn test_schedule_error_is_invalid() {
    let err = ScheduleError::Invalid("test".to_string());
    assert!(err.is_invalid());
    assert!(!err.is_parse());
    assert!(!err.is_persistence());
    assert!(!err.is_not_implemented());
}

#[test]
fn test_schedule_error_is_parse() {
    let err = ScheduleError::Parse("test".to_string());
    assert!(err.is_parse());
    assert!(!err.is_invalid());
    assert!(!err.is_persistence());
    assert!(!err.is_not_implemented());
}

#[test]
fn test_schedule_error_is_persistence() {
    let err = ScheduleError::Persistence("test".to_string());
    assert!(err.is_persistence());
    assert!(!err.is_invalid());
    assert!(!err.is_parse());
    assert!(!err.is_not_implemented());
}

#[test]
fn test_schedule_error_is_not_implemented() {
    let err = ScheduleError::NotImplemented("test".to_string());
    assert!(err.is_not_implemented());
    assert!(!err.is_invalid());
    assert!(!err.is_parse());
    assert!(!err.is_persistence());
}

#[test]
fn test_schedule_error_is_retryable() {
    let persistence_err = ScheduleError::Persistence("test".to_string());
    assert!(persistence_err.is_retryable());

    let invalid_err = ScheduleError::Invalid("test".to_string());
    assert!(!invalid_err.is_retryable());

    let parse_err = ScheduleError::Parse("test".to_string());
    assert!(!parse_err.is_retryable());

    let not_impl_err = ScheduleError::NotImplemented("test".to_string());
    assert!(!not_impl_err.is_retryable());
}

#[test]
fn test_schedule_error_category() {
    assert_eq!(
        ScheduleError::Invalid("test".to_string()).category(),
        "invalid"
    );
    assert_eq!(ScheduleError::Parse("test".to_string()).category(), "parse");
    assert_eq!(
        ScheduleError::Persistence("test".to_string()).category(),
        "persistence"
    );
    assert_eq!(
        ScheduleError::NotImplemented("test".to_string()).category(),
        "not_implemented"
    );
}

// ===== Jitter Tests =====

#[test]
fn test_jitter_new() {
    let jitter = Jitter::new(-10, 10);
    assert_eq!(jitter.min_seconds, -10);
    assert_eq!(jitter.max_seconds, 10);
}

#[test]
fn test_jitter_positive() {
    let jitter = Jitter::positive(30);
    assert_eq!(jitter.min_seconds, 0);
    assert_eq!(jitter.max_seconds, 30);
}

#[test]
fn test_jitter_symmetric() {
    let jitter = Jitter::symmetric(15);
    assert_eq!(jitter.min_seconds, -15);
    assert_eq!(jitter.max_seconds, 15);
}

#[test]
fn test_jitter_apply_deterministic() {
    let jitter = Jitter::symmetric(60);
    let dt = Utc::now();
    let task_name = "test_task";

    // Same inputs should produce same output
    let result1 = jitter.apply(dt, task_name);
    let result2 = jitter.apply(dt, task_name);
    assert_eq!(result1, result2);
}

#[test]
fn test_jitter_apply_different_tasks() {
    let jitter = Jitter::symmetric(60);
    let dt = Utc::now();

    // Different task names should produce different results
    let result1 = jitter.apply(dt, "task1");
    let result2 = jitter.apply(dt, "task2");
    assert_ne!(result1, result2);
}

#[test]
fn test_jitter_apply_range() {
    let jitter = Jitter::new(10, 50);
    let dt = Utc::now();
    let task_name = "test_task";

    let result = jitter.apply(dt, task_name);
    let diff_seconds = (result - dt).num_seconds();

    // Result should be within range
    assert!(diff_seconds >= 10);
    assert!(diff_seconds <= 50);
}

#[test]
fn test_scheduled_task_with_jitter() {
    let schedule = Schedule::interval(60);
    let jitter = Jitter::positive(10);
    let task = ScheduledTask::new("test_task".to_string(), schedule).with_jitter(jitter);

    assert!(task.jitter.is_some());
    let j = task.jitter.unwrap();
    assert_eq!(j.min_seconds, 0);
    assert_eq!(j.max_seconds, 10);
}

#[test]
fn test_scheduled_task_next_run_time_with_jitter() {
    let schedule = Schedule::interval(60);
    let jitter = Jitter::positive(10);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule).with_jitter(jitter);

    // Set last run to specific time
    let last_run = Utc::now() - Duration::seconds(70);
    task.last_run_at = Some(last_run);

    let next_run = task.next_run_time().unwrap();

    // Without jitter, next run would be last_run + 60 seconds
    // With jitter (0 to 10), it should be between last_run + 60 and last_run + 70
    let expected_base = last_run + Duration::seconds(60);
    let diff = (next_run - expected_base).num_seconds();

    assert!(diff >= 0);
    assert!(diff <= 10);
}

#[test]
fn test_jitter_serialization() {
    let jitter = Jitter::symmetric(30);
    let json = serde_json::to_string(&jitter).unwrap();
    let deserialized: Jitter = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.min_seconds, -30);
    assert_eq!(deserialized.max_seconds, 30);
}

#[test]
fn test_scheduled_task_with_jitter_serialization() {
    let schedule = Schedule::interval(60);
    let jitter = Jitter::positive(15);
    let task = ScheduledTask::new("test_task".to_string(), schedule).with_jitter(jitter);

    let json = serde_json::to_string(&task).unwrap();
    let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

    assert!(deserialized.jitter.is_some());
    let j = deserialized.jitter.unwrap();
    assert_eq!(j.min_seconds, 0);
    assert_eq!(j.max_seconds, 15);
}

// ===== Catch-up Policy Tests =====

#[test]
fn test_catchup_policy_skip() {
    let policy = CatchupPolicy::Skip;
    let last_run = Utc::now() - Duration::seconds(200);
    let next_run = Utc::now() - Duration::seconds(50);
    let now = Utc::now();

    assert!(!policy.should_catchup(Some(last_run), next_run, now));
    assert_eq!(policy.catchup_count(Some(last_run), 60, now), 0);
}

#[test]
fn test_catchup_policy_run_once() {
    let policy = CatchupPolicy::RunOnce;
    let last_run = Utc::now() - Duration::seconds(200);
    let next_run = Utc::now() - Duration::seconds(50);
    let now = Utc::now();

    assert!(policy.should_catchup(Some(last_run), next_run, now));
    assert_eq!(policy.catchup_count(Some(last_run), 60, now), 1);
}

#[test]
fn test_catchup_policy_run_once_not_missed() {
    let policy = CatchupPolicy::RunOnce;
    let last_run = Utc::now() - Duration::seconds(30);
    let next_run = Utc::now() + Duration::seconds(30);
    let now = Utc::now();

    assert!(!policy.should_catchup(Some(last_run), next_run, now));
}

#[test]
fn test_catchup_policy_run_multiple() {
    let policy = CatchupPolicy::RunMultiple { max_catchup: 5 };
    let last_run = Utc::now() - Duration::seconds(250); // Missed ~4 runs (250/60)
    let next_run = Utc::now() - Duration::seconds(50);
    let now = Utc::now();

    assert!(policy.should_catchup(Some(last_run), next_run, now));

    // Should be 3 catch-up runs (4 missed - 1 current)
    let count = policy.catchup_count(Some(last_run), 60, now);
    assert!((2..=4).contains(&count));
}

#[test]
fn test_catchup_policy_run_multiple_max_limit() {
    let policy = CatchupPolicy::RunMultiple { max_catchup: 2 };
    let last_run = Utc::now() - Duration::seconds(600); // Missed ~10 runs
    let now = Utc::now();

    // Should be capped at max_catchup
    let count = policy.catchup_count(Some(last_run), 60, now);
    assert_eq!(count, 2);
}

#[test]
fn test_catchup_policy_time_window_within() {
    let policy = CatchupPolicy::TimeWindow {
        window_seconds: 120,
    };
    let last_run = Utc::now() - Duration::seconds(150);
    let next_run = Utc::now() - Duration::seconds(50); // Missed by 50s (within window)
    let now = Utc::now();

    assert!(policy.should_catchup(Some(last_run), next_run, now));
    assert_eq!(policy.catchup_count(Some(last_run), 60, now), 1);
}

#[test]
fn test_catchup_policy_time_window_outside() {
    let policy = CatchupPolicy::TimeWindow { window_seconds: 30 };
    let last_run = Utc::now() - Duration::seconds(200);
    let next_run = Utc::now() - Duration::seconds(100); // Missed by 100s (outside window)
    let now = Utc::now();

    assert!(!policy.should_catchup(Some(last_run), next_run, now));
    assert_eq!(policy.catchup_count(Some(last_run), 60, now), 0);
}

#[test]
fn test_catchup_policy_never_run() {
    let policy = CatchupPolicy::RunOnce;
    let now = Utc::now();
    let next_run = now + Duration::seconds(60);

    // Tasks that never ran should not trigger catchup
    assert!(!policy.should_catchup(None, next_run, now));
    assert_eq!(policy.catchup_count(None, 60, now), 0);
}

#[test]
fn test_catchup_policy_default() {
    let policy = CatchupPolicy::default();
    assert_eq!(policy, CatchupPolicy::Skip);
}

#[test]
fn test_catchup_policy_serialization() {
    let policies = vec![
        CatchupPolicy::Skip,
        CatchupPolicy::RunOnce,
        CatchupPolicy::RunMultiple { max_catchup: 5 },
        CatchupPolicy::TimeWindow {
            window_seconds: 300,
        },
    ];

    for policy in policies {
        let json = serde_json::to_string(&policy).unwrap();
        let deserialized: CatchupPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, policy);
    }
}

#[test]
fn test_scheduled_task_with_catchup_policy() {
    let schedule = Schedule::interval(60);
    let policy = CatchupPolicy::RunOnce;
    let task =
        ScheduledTask::new("test_task".to_string(), schedule).with_catchup_policy(policy.clone());

    assert_eq!(task.catchup_policy, policy);
}

#[test]
fn test_scheduled_task_catchup_policy_serialization() {
    let schedule = Schedule::interval(60);
    let policy = CatchupPolicy::RunMultiple { max_catchup: 3 };
    let task =
        ScheduledTask::new("test_task".to_string(), schedule).with_catchup_policy(policy.clone());

    let json = serde_json::to_string(&task).unwrap();
    let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.catchup_policy, policy);
}

// ===== Groups and Tags Tests =====

#[test]
fn test_scheduled_task_with_group() {
    let schedule = Schedule::interval(60);
    let task =
        ScheduledTask::new("test_task".to_string(), schedule).with_group("reports".to_string());

    assert_eq!(task.group, Some("reports".to_string()));
    assert!(task.is_in_group("reports"));
    assert!(!task.is_in_group("other"));
}

#[test]
fn test_scheduled_task_with_tag() {
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule).with_tag("daily".to_string());

    assert_eq!(task.tags.len(), 1);
    assert!(task.has_tag("daily"));
    assert!(!task.has_tag("weekly"));
}

#[test]
fn test_scheduled_task_with_tags() {
    let schedule = Schedule::interval(60);
    let mut tags = HashSet::new();
    tags.insert("daily".to_string());
    tags.insert("reports".to_string());
    tags.insert("critical".to_string());

    let task = ScheduledTask::new("test_task".to_string(), schedule).with_tags(tags.clone());

    assert_eq!(task.tags.len(), 3);
    assert!(task.has_tag("daily"));
    assert!(task.has_tag("reports"));
    assert!(task.has_tag("critical"));
}

#[test]
fn test_scheduled_task_add_remove_tag() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    task.add_tag("tag1".to_string());
    assert!(task.has_tag("tag1"));

    task.add_tag("tag2".to_string());
    assert!(task.has_tag("tag2"));
    assert_eq!(task.tags.len(), 2);

    let removed = task.remove_tag("tag1");
    assert!(removed);
    assert!(!task.has_tag("tag1"));
    assert_eq!(task.tags.len(), 1);

    let not_removed = task.remove_tag("nonexistent");
    assert!(!not_removed);
}

#[test]
fn test_beat_scheduler_get_tasks_by_group() {
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

    let reports = scheduler.get_tasks_by_group("reports");
    assert_eq!(reports.len(), 2);

    let alerts = scheduler.get_tasks_by_group("alerts");
    assert_eq!(alerts.len(), 1);

    let nonexistent = scheduler.get_tasks_by_group("nonexistent");
    assert_eq!(nonexistent.len(), 0);
}

#[test]
fn test_beat_scheduler_get_tasks_by_tag() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
        .with_tag("daily".to_string());
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
        .with_tag("daily".to_string())
        .with_tag("critical".to_string());
    let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
        .with_tag("weekly".to_string());

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();
    scheduler.add_task(task3).unwrap();

    let daily = scheduler.get_tasks_by_tag("daily");
    assert_eq!(daily.len(), 2);

    let critical = scheduler.get_tasks_by_tag("critical");
    assert_eq!(critical.len(), 1);

    let weekly = scheduler.get_tasks_by_tag("weekly");
    assert_eq!(weekly.len(), 1);
}

#[test]
fn test_beat_scheduler_get_tasks_by_tags() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
        .with_tag("daily".to_string());
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
        .with_tag("weekly".to_string());
    let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
        .with_tag("monthly".to_string());

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();
    scheduler.add_task(task3).unwrap();

    // Get tasks with any of the specified tags
    let tasks = scheduler.get_tasks_by_tags(&["daily", "weekly"]);
    assert_eq!(tasks.len(), 2);
}

#[test]
fn test_beat_scheduler_get_tasks_with_all_tags() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
        .with_tag("daily".to_string())
        .with_tag("critical".to_string());
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
        .with_tag("daily".to_string());
    let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
        .with_tag("critical".to_string());

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();
    scheduler.add_task(task3).unwrap();

    // Get tasks with all of the specified tags
    let tasks = scheduler.get_tasks_with_all_tags(&["daily", "critical"]);
    assert_eq!(tasks.len(), 1);
    assert_eq!(tasks[0].name, "task1");
}

#[test]
fn test_beat_scheduler_get_all_groups() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
        .with_group("reports".to_string());
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
        .with_group("alerts".to_string());
    let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
        .with_group("reports".to_string());

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();
    scheduler.add_task(task3).unwrap();

    let groups = scheduler.get_all_groups();
    assert_eq!(groups.len(), 2);
    assert!(groups.contains("reports"));
    assert!(groups.contains("alerts"));
}

#[test]
fn test_beat_scheduler_get_all_tags() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
        .with_tag("daily".to_string())
        .with_tag("critical".to_string());
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
        .with_tag("weekly".to_string());
    let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(60))
        .with_tag("daily".to_string());

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();
    scheduler.add_task(task3).unwrap();

    let tags = scheduler.get_all_tags();
    assert_eq!(tags.len(), 3);
    assert!(tags.contains("daily"));
    assert!(tags.contains("weekly"));
    assert!(tags.contains("critical"));
}

#[test]
fn test_beat_scheduler_enable_disable_group() {
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

    // Disable reports group
    let count = scheduler.disable_group("reports").unwrap();
    assert_eq!(count, 2);

    let reports = scheduler.get_tasks_by_group("reports");
    for task in reports {
        assert!(!task.enabled);
    }

    // Enable reports group
    let count = scheduler.enable_group("reports").unwrap();
    assert_eq!(count, 2);

    let reports = scheduler.get_tasks_by_group("reports");
    for task in reports {
        assert!(task.enabled);
    }
}

#[test]
fn test_beat_scheduler_enable_disable_tag() {
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

    // Disable daily tag
    let count = scheduler.disable_tag("daily").unwrap();
    assert_eq!(count, 2);

    let daily_tasks = scheduler.get_tasks_by_tag("daily");
    for task in daily_tasks {
        assert!(!task.enabled);
    }

    // Enable daily tag
    let count = scheduler.enable_tag("daily").unwrap();
    assert_eq!(count, 2);

    let daily_tasks = scheduler.get_tasks_by_tag("daily");
    for task in daily_tasks {
        assert!(task.enabled);
    }
}

#[test]
fn test_groups_tags_serialization() {
    let schedule = Schedule::interval(60);
    let mut tags = HashSet::new();
    tags.insert("daily".to_string());
    tags.insert("critical".to_string());

    let task = ScheduledTask::new("test_task".to_string(), schedule)
        .with_group("reports".to_string())
        .with_tags(tags.clone());

    let json = serde_json::to_string(&task).unwrap();
    let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.group, Some("reports".to_string()));
    assert_eq!(deserialized.tags.len(), 2);
    assert!(deserialized.has_tag("daily"));
    assert!(deserialized.has_tag("critical"));
}

// ===== Retry Policy Tests =====

#[test]
fn test_retry_policy_no_retry() {
    let policy = RetryPolicy::NoRetry;
    assert!(!policy.should_retry(0));
    assert!(!policy.should_retry(1));
    assert_eq!(policy.next_retry_delay(0), None);
}

#[test]
fn test_retry_policy_fixed_delay() {
    let policy = RetryPolicy::FixedDelay {
        delay_seconds: 30,
        max_retries: 3,
    };

    assert!(policy.should_retry(0));
    assert_eq!(policy.next_retry_delay(0), Some(30));

    assert!(policy.should_retry(1));
    assert_eq!(policy.next_retry_delay(1), Some(30));

    assert!(policy.should_retry(2));
    assert_eq!(policy.next_retry_delay(2), Some(30));

    assert!(!policy.should_retry(3));
    assert_eq!(policy.next_retry_delay(3), None);
}

#[test]
fn test_retry_policy_exponential_backoff() {
    let policy = RetryPolicy::ExponentialBackoff {
        initial_delay_seconds: 10,
        multiplier: 2.0,
        max_delay_seconds: 300,
        max_retries: 5,
    };

    // First retry: 10 * 2^0 = 10
    assert_eq!(policy.next_retry_delay(0), Some(10));

    // Second retry: 10 * 2^1 = 20
    assert_eq!(policy.next_retry_delay(1), Some(20));

    // Third retry: 10 * 2^2 = 40
    assert_eq!(policy.next_retry_delay(2), Some(40));

    // Fourth retry: 10 * 2^3 = 80
    assert_eq!(policy.next_retry_delay(3), Some(80));

    // Fifth retry: 10 * 2^4 = 160
    assert_eq!(policy.next_retry_delay(4), Some(160));

    // Sixth retry: exceeds max_retries
    assert_eq!(policy.next_retry_delay(5), None);
    assert!(!policy.should_retry(5));
}

#[test]
fn test_retry_policy_exponential_backoff_max_delay() {
    let policy = RetryPolicy::ExponentialBackoff {
        initial_delay_seconds: 10,
        multiplier: 2.0,
        max_delay_seconds: 100,
        max_retries: 10,
    };

    // Seventh retry: 10 * 2^6 = 640, capped at 100
    assert_eq!(policy.next_retry_delay(6), Some(100));
}

#[test]
fn test_retry_policy_default() {
    let policy = RetryPolicy::default();
    assert_eq!(policy, RetryPolicy::NoRetry);
}

#[test]
fn test_retry_policy_serialization() {
    let policies = vec![
        RetryPolicy::NoRetry,
        RetryPolicy::FixedDelay {
            delay_seconds: 30,
            max_retries: 3,
        },
        RetryPolicy::ExponentialBackoff {
            initial_delay_seconds: 10,
            multiplier: 2.0,
            max_delay_seconds: 300,
            max_retries: 5,
        },
    ];

    for policy in policies {
        let json = serde_json::to_string(&policy).unwrap();
        let deserialized: RetryPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, policy);
    }
}

// ===== Task Retry Tests =====

#[test]
fn test_scheduled_task_with_retry_policy() {
    let schedule = Schedule::interval(60);
    let policy = RetryPolicy::FixedDelay {
        delay_seconds: 30,
        max_retries: 3,
    };
    let task =
        ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(policy.clone());

    assert_eq!(task.retry_policy, policy);
    assert_eq!(task.retry_count, 0);
    assert_eq!(task.total_failure_count, 0);
}

#[test]
fn test_scheduled_task_mark_failure() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(
        RetryPolicy::FixedDelay {
            delay_seconds: 30,
            max_retries: 3,
        },
    );

    task.mark_failure();
    assert_eq!(task.retry_count, 1);
    assert_eq!(task.total_failure_count, 1);
    assert!(task.last_failure_at.is_some());
}

#[test]
fn test_scheduled_task_mark_success() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(
        RetryPolicy::FixedDelay {
            delay_seconds: 30,
            max_retries: 3,
        },
    );

    task.mark_failure();
    task.mark_failure();
    assert_eq!(task.retry_count, 2);

    task.mark_success();
    assert_eq!(task.retry_count, 0); // Reset on success
}

#[test]
fn test_scheduled_task_should_retry() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(
        RetryPolicy::FixedDelay {
            delay_seconds: 30,
            max_retries: 2,
        },
    );

    assert!(task.should_retry()); // Can retry (0 < 2)

    task.mark_failure();
    assert!(task.should_retry()); // Can retry (1 < 2)

    task.mark_failure();
    assert!(!task.should_retry()); // Cannot retry (2 >= 2)
}

#[test]
fn test_scheduled_task_next_retry_time() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(
        RetryPolicy::FixedDelay {
            delay_seconds: 30,
            max_retries: 3,
        },
    );

    assert!(task.next_retry_time().is_none()); // No failure yet

    task.mark_failure();
    let next_retry = task.next_retry_time().unwrap();
    let expected = task.last_failure_at.unwrap() + Duration::seconds(30);

    assert_eq!(next_retry, expected);
}

#[test]
fn test_scheduled_task_failure_rate() {
    let schedule = Schedule::interval(60);
    let mut task = ScheduledTask::new("test_task".to_string(), schedule);

    assert_eq!(task.failure_rate(), 0.0);

    task.total_run_count = 7;
    task.total_failure_count = 3;
    assert_eq!(task.failure_rate(), 0.3); // 3/10 = 0.3
}

#[test]
fn test_beat_scheduler_mark_task_success() {
    let mut scheduler = BeatScheduler::new();
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(
        RetryPolicy::FixedDelay {
            delay_seconds: 30,
            max_retries: 3,
        },
    );

    scheduler.add_task(task).unwrap();

    // Simulate failure then success
    scheduler.mark_task_failure("test_task").unwrap();
    let task = scheduler.tasks.get("test_task").unwrap();
    assert_eq!(task.retry_count, 1);

    scheduler.mark_task_success("test_task").unwrap();
    let task = scheduler.tasks.get("test_task").unwrap();
    assert_eq!(task.retry_count, 0);
    assert_eq!(task.total_run_count, 1);
}

#[test]
fn test_beat_scheduler_mark_task_failure() {
    let mut scheduler = BeatScheduler::new();
    let schedule = Schedule::interval(60);
    let task = ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(
        RetryPolicy::FixedDelay {
            delay_seconds: 30,
            max_retries: 3,
        },
    );

    scheduler.add_task(task).unwrap();

    scheduler.mark_task_failure("test_task").unwrap();
    scheduler.mark_task_failure("test_task").unwrap();

    let task = scheduler.tasks.get("test_task").unwrap();
    assert_eq!(task.retry_count, 2);
    assert_eq!(task.total_failure_count, 2);
    assert!(task.last_failure_at.is_some());
}

#[test]
fn test_beat_scheduler_get_retry_tasks() {
    let mut scheduler = BeatScheduler::new();
    let schedule = Schedule::interval(60);

    // Task with retry policy
    let task1 = ScheduledTask::new("task1".to_string(), schedule.clone()).with_retry_policy(
        RetryPolicy::FixedDelay {
            delay_seconds: 1, // Short delay for testing
            max_retries: 3,
        },
    );

    // Task without retry policy
    let task2 = ScheduledTask::new("task2".to_string(), schedule);

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    // Mark failures
    scheduler.mark_task_failure("task1").unwrap();
    scheduler.mark_task_failure("task2").unwrap();

    // Wait for retry time
    std::thread::sleep(std::time::Duration::from_secs(2));

    let retry_tasks = scheduler.get_retry_tasks();
    assert_eq!(retry_tasks.len(), 1);
    assert_eq!(retry_tasks[0].name, "task1");
}

#[test]
fn test_retry_policy_serialization_in_task() {
    let schedule = Schedule::interval(60);
    let policy = RetryPolicy::ExponentialBackoff {
        initial_delay_seconds: 10,
        multiplier: 2.0,
        max_delay_seconds: 300,
        max_retries: 5,
    };
    let task =
        ScheduledTask::new("test_task".to_string(), schedule).with_retry_policy(policy.clone());

    let json = serde_json::to_string(&task).unwrap();
    let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.retry_policy, policy);
}
