#![cfg(test)]

use crate::*;
#[cfg(feature = "cron")]
use chrono::Timelike;
use chrono::{Duration, Utc};
use tempfile::NamedTempFile;

// ===== Lock Manager Tests =====

#[test]
fn test_schedule_lock_basic() {
    let lock = ScheduleLock::new("task1".to_string(), "owner1".to_string(), 300);

    assert_eq!(lock.task_name, "task1");
    assert_eq!(lock.owner, "owner1");
    assert!(!lock.is_expired());
    assert!(lock.is_owned_by("owner1"));
    assert!(!lock.is_owned_by("owner2"));
    assert_eq!(lock.renewal_count, 0);
}

#[test]
fn test_schedule_lock_ttl() {
    let lock = ScheduleLock::new("task1".to_string(), "owner1".to_string(), 300);

    let ttl = lock.ttl();
    assert!(ttl.num_seconds() > 290);
    assert!(ttl.num_seconds() <= 300);
}

#[test]
fn test_schedule_lock_renew() {
    let mut lock = ScheduleLock::new("task1".to_string(), "owner1".to_string(), 1);

    // Wait a bit
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Renew before expiration
    let result = lock.renew(300);
    assert!(result.is_ok());
    assert_eq!(lock.renewal_count, 1);
    assert!(!lock.is_expired());
}

#[test]
fn test_lock_manager_acquire_release() {
    let mut manager = LockManager::new(300);

    // Acquire lock
    let acquired = manager.try_acquire("task1", "owner1", None).unwrap();
    assert!(acquired);
    assert!(manager.is_locked("task1"));

    // Try to acquire again with different owner
    let acquired = manager.try_acquire("task1", "owner2", None).unwrap();
    assert!(!acquired);

    // Release lock
    let released = manager.release("task1", "owner1").unwrap();
    assert!(released);
    assert!(!manager.is_locked("task1"));
}

#[test]
fn test_lock_manager_acquire_same_owner() {
    let mut manager = LockManager::new(300);

    // Acquire lock
    let acquired = manager.try_acquire("task1", "owner1", None).unwrap();
    assert!(acquired);

    // Try to acquire again with same owner (should succeed)
    let acquired = manager.try_acquire("task1", "owner1", None).unwrap();
    assert!(acquired);
}

#[test]
fn test_lock_manager_renew() {
    let mut manager = LockManager::new(300);

    // Acquire lock
    manager.try_acquire("task1", "owner1", None).unwrap();

    // Renew lock
    let renewed = manager.renew("task1", "owner1", Some(600)).unwrap();
    assert!(renewed);

    // Check lock info
    let lock = manager.get_lock("task1").unwrap();
    assert_eq!(lock.renewal_count, 1);
}

#[test]
fn test_lock_manager_cleanup_expired() {
    let mut manager = LockManager::new(1);

    // Acquire lock with 1 second TTL
    manager.try_acquire("task1", "owner1", Some(1)).unwrap();
    assert!(manager.is_locked("task1"));

    // Wait for expiration
    std::thread::sleep(std::time::Duration::from_millis(1100));

    // Cleanup
    manager.cleanup_expired();
    assert!(!manager.is_locked("task1"));
}

#[test]
fn test_lock_manager_get_active_locks() {
    let mut manager = LockManager::new(300);

    manager.try_acquire("task1", "owner1", None).unwrap();
    manager.try_acquire("task2", "owner2", None).unwrap();

    let active_locks = manager.get_active_locks();
    assert_eq!(active_locks.len(), 2);
}

#[test]
fn test_lock_manager_release_all() {
    let mut manager = LockManager::new(300);

    manager.try_acquire("task1", "owner1", None).unwrap();
    manager.try_acquire("task2", "owner2", None).unwrap();

    assert_eq!(manager.get_active_locks().len(), 2);

    manager.release_all();
    assert_eq!(manager.get_active_locks().len(), 0);
}

#[test]
fn test_scheduler_lock_acquire_release() {
    let mut scheduler = BeatScheduler::new();

    // Acquire lock
    let acquired = scheduler.try_acquire_lock("task1", None).unwrap();
    assert!(acquired);
    assert!(scheduler.is_task_locked("task1"));

    // Release lock
    let released = scheduler.release_lock("task1").unwrap();
    assert!(released);
    assert!(!scheduler.is_task_locked("task1"));
}

#[test]
fn test_scheduler_lock_multiple_instances() {
    let mut scheduler1 = BeatScheduler::new();
    let mut scheduler2 = BeatScheduler::new();

    // Note: Each scheduler has its own in-memory lock manager
    // For distributed locking, you would need external state (Redis, DB, etc.)

    // Scheduler 1 acquires lock in its own lock manager
    let acquired = scheduler1.try_acquire_lock("task1", None).unwrap();
    assert!(acquired);

    // Scheduler 2 can also acquire the same task name in its own lock manager
    // because they don't share state (this is in-memory locking)
    let acquired = scheduler2.try_acquire_lock("task1", None).unwrap();
    assert!(acquired); // Both can acquire independently

    // Each scheduler maintains its own locks
    assert!(scheduler1.is_task_locked("task1"));
    assert!(scheduler2.is_task_locked("task1"));

    // Releasing in scheduler1 doesn't affect scheduler2
    scheduler1.release_lock("task1").unwrap();
    assert!(!scheduler1.is_task_locked("task1"));
    assert!(scheduler2.is_task_locked("task1"));
}

#[test]
fn test_scheduler_execute_with_lock() {
    let mut scheduler = BeatScheduler::new();
    let mut executed = false;

    // Execute with lock
    let result = scheduler.execute_with_lock("task1", None, || {
        executed = true;
        Ok(())
    });

    assert!(result.is_ok());
    assert!(result.unwrap());
    assert!(executed);

    // Lock should be released after execution
    assert!(!scheduler.is_task_locked("task1"));
}

#[test]
fn test_scheduler_instance_id() {
    let scheduler1 = BeatScheduler::new();
    let scheduler2 = BeatScheduler::new();

    // Each scheduler should have a unique instance ID
    assert_ne!(scheduler1.instance_id(), scheduler2.instance_id());
}

#[test]
fn test_scheduler_set_custom_instance_id() {
    let mut scheduler = BeatScheduler::new();

    scheduler.set_instance_id("custom-id-123".to_string());
    assert_eq!(scheduler.instance_id(), "custom-id-123");
}

#[test]
fn test_lock_manager_serialization() {
    let mut manager = LockManager::new(300);
    manager.try_acquire("task1", "owner1", None).unwrap();

    let json = serde_json::to_string(&manager).unwrap();
    let deserialized: LockManager = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.default_ttl, 300);
    // Note: locks are serialized, so they should be present
    assert!(deserialized.is_locked("task1"));
}

// ===== Conflict Detection Tests =====

#[test]
fn test_schedule_conflict_basic() {
    let conflict = ScheduleConflict::new(
        "task1".to_string(),
        "task2".to_string(),
        ConflictSeverity::High,
        120,
        "Overlapping execution".to_string(),
    );

    assert_eq!(conflict.task1, "task1");
    assert_eq!(conflict.task2, "task2");
    assert_eq!(conflict.severity, ConflictSeverity::High);
    assert_eq!(conflict.overlap_seconds, 120);
    assert!(conflict.is_high_severity());
    assert!(!conflict.is_medium_severity());
    assert!(!conflict.is_low_severity());
}

#[test]
fn test_schedule_conflict_with_resolution() {
    let conflict = ScheduleConflict::new(
        "task1".to_string(),
        "task2".to_string(),
        ConflictSeverity::Medium,
        60,
        "Partial overlap".to_string(),
    )
    .with_resolution("Add jitter".to_string());

    assert!(conflict.resolution.is_some());
    assert_eq!(conflict.resolution.unwrap(), "Add jitter");
}

#[test]
fn test_schedule_conflict_severity() {
    let low = ScheduleConflict::new(
        "t1".to_string(),
        "t2".to_string(),
        ConflictSeverity::Low,
        10,
        "Low conflict".to_string(),
    );

    let medium = ScheduleConflict::new(
        "t1".to_string(),
        "t2".to_string(),
        ConflictSeverity::Medium,
        30,
        "Medium conflict".to_string(),
    );

    let high = ScheduleConflict::new(
        "t1".to_string(),
        "t2".to_string(),
        ConflictSeverity::High,
        60,
        "High conflict".to_string(),
    );

    assert!(low.is_low_severity());
    assert!(medium.is_medium_severity());
    assert!(high.is_high_severity());
}

#[test]
fn test_detect_conflicts_no_conflict() {
    let mut scheduler = BeatScheduler::new();

    // Add tasks with different schedules that don't overlap
    scheduler
        .add_task(ScheduledTask::new(
            "task1".to_string(),
            Schedule::interval(3600),
        ))
        .unwrap();
    scheduler
        .add_task(ScheduledTask::new(
            "task2".to_string(),
            Schedule::interval(7200),
        ))
        .unwrap();

    let conflicts = scheduler.detect_conflicts(60, 30);
    assert_eq!(conflicts.len(), 0);
}

#[test]
fn test_detect_conflicts_with_overlap() {
    let mut scheduler = BeatScheduler::new();

    // Add two tasks with the same interval (will overlap)
    scheduler
        .add_task(ScheduledTask::new(
            "task1".to_string(),
            Schedule::interval(60),
        ))
        .unwrap();
    scheduler
        .add_task(ScheduledTask::new(
            "task2".to_string(),
            Schedule::interval(60),
        ))
        .unwrap();

    // Check for conflicts in 1 hour window with 30 second estimated duration
    let conflicts = scheduler.detect_conflicts(3600, 30);

    // Should detect conflicts since both run every 60 seconds
    assert!(!conflicts.is_empty());
}

#[test]
fn test_detect_conflicts_disabled_tasks() {
    let mut scheduler = BeatScheduler::new();

    // Add two tasks with same schedule, one disabled
    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60));
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60)).disabled();

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    // Should not detect conflicts because task2 is disabled
    let conflicts = scheduler.detect_conflicts(3600, 30);
    assert_eq!(conflicts.len(), 0);
}

#[test]
fn test_get_high_severity_conflicts() {
    let mut scheduler = BeatScheduler::new();

    // Add multiple tasks with the same interval
    scheduler
        .add_task(ScheduledTask::new(
            "task1".to_string(),
            Schedule::interval(60),
        ))
        .unwrap();
    scheduler
        .add_task(ScheduledTask::new(
            "task2".to_string(),
            Schedule::interval(60),
        ))
        .unwrap();
    scheduler
        .add_task(ScheduledTask::new(
            "task3".to_string(),
            Schedule::interval(120),
        ))
        .unwrap();

    // Get high severity conflicts with long duration (60s)
    let high_conflicts = scheduler.get_high_severity_conflicts(3600, 60);

    // May have high severity conflicts depending on overlap
    // Just verify the method works
    assert!(high_conflicts.len() <= scheduler.conflict_count(3600, 60));
}

#[test]
fn test_has_conflicts() {
    let mut scheduler = BeatScheduler::new();

    // Initially no conflicts
    assert!(!scheduler.has_conflicts(3600, 30));

    // Add tasks with same interval
    scheduler
        .add_task(ScheduledTask::new(
            "task1".to_string(),
            Schedule::interval(60),
        ))
        .unwrap();
    scheduler
        .add_task(ScheduledTask::new(
            "task2".to_string(),
            Schedule::interval(60),
        ))
        .unwrap();

    // Now should have conflicts
    assert!(scheduler.has_conflicts(3600, 30));
}

#[test]
fn test_conflict_count() {
    let mut scheduler = BeatScheduler::new();

    assert_eq!(scheduler.conflict_count(3600, 30), 0);

    scheduler
        .add_task(ScheduledTask::new(
            "task1".to_string(),
            Schedule::interval(60),
        ))
        .unwrap();
    scheduler
        .add_task(ScheduledTask::new(
            "task2".to_string(),
            Schedule::interval(60),
        ))
        .unwrap();
    scheduler
        .add_task(ScheduledTask::new(
            "task3".to_string(),
            Schedule::interval(60),
        ))
        .unwrap();

    let count = scheduler.conflict_count(3600, 30);
    assert!(count > 0);
}

#[test]
fn test_schedule_conflict_display() {
    let conflict = ScheduleConflict::new(
        "task1".to_string(),
        "task2".to_string(),
        ConflictSeverity::High,
        120,
        "Test conflict".to_string(),
    );

    let display = format!("{}", conflict);
    assert!(display.contains("task1"));
    assert!(display.contains("task2"));
    assert!(display.contains("120s"));
}

#[test]
fn test_schedule_conflict_serialization() {
    let conflict = ScheduleConflict::new(
        "task1".to_string(),
        "task2".to_string(),
        ConflictSeverity::Medium,
        60,
        "Test".to_string(),
    );

    let json = serde_json::to_string(&conflict).unwrap();
    let deserialized: ScheduleConflict = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.task1, "task1");
    assert_eq!(deserialized.task2, "task2");
    assert_eq!(deserialized.severity, ConflictSeverity::Medium);
    assert_eq!(deserialized.overlap_seconds, 60);
}

// ===== Timezone Tests =====

#[cfg(feature = "cron")]
#[test]
fn test_crontab_timezone_utc() {
    // Test crontab with UTC (default)
    let schedule = Schedule::crontab("0", "12", "*", "*", "*");
    let now = Utc::now();
    let next_run = schedule.next_run(Some(now)).unwrap();

    // Should be at noon UTC
    assert_eq!(next_run.hour(), 12);
    assert_eq!(next_run.minute(), 0);
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_timezone_new_york() {
    // Test crontab with New York timezone (runs every day, not just weekdays)
    let schedule = Schedule::crontab_tz("0", "9", "*", "*", "*", "America/New_York");
    let now = Utc::now();
    let next_run = schedule.next_run(Some(now)).unwrap();

    // Verify we got a valid future time
    assert!(next_run > now);
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_timezone_london() {
    // Test crontab with London timezone
    let schedule = Schedule::crontab_tz("30", "14", "*", "*", "*", "Europe/London");
    let now = Utc::now();
    let next_run = schedule.next_run(Some(now)).unwrap();

    // Verify we got a valid future time
    assert!(next_run > now);
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_timezone_tokyo() {
    // Test crontab with Tokyo timezone (runs every day, not just weekdays)
    let schedule = Schedule::crontab_tz("0", "18", "*", "*", "*", "Asia/Tokyo");
    let now = Utc::now();
    let next_run = schedule.next_run(Some(now)).unwrap();

    // Verify we got a valid future time
    assert!(next_run > now);
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_timezone_invalid() {
    // Test with invalid timezone
    let schedule = Schedule::crontab_tz("0", "12", "*", "*", "*", "Invalid/Timezone");
    let now = Utc::now();
    let result = schedule.next_run(Some(now));

    // Should return error for invalid timezone
    assert!(result.is_err());
    if let Err(ScheduleError::Parse(msg)) = result {
        assert!(msg.contains("Invalid timezone"));
    }
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_timezone_serialization() {
    // Test that timezone is preserved through serialization
    let schedule = Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York");
    let json = serde_json::to_string(&schedule).unwrap();
    let deserialized: Schedule = serde_json::from_str(&json).unwrap();

    if let Schedule::Crontab { timezone, .. } = deserialized {
        assert_eq!(timezone, Some("America/New_York".to_string()));
    } else {
        panic!("Expected Crontab schedule");
    }
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_timezone_display() {
    // Test that timezone appears in display string
    let schedule = Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York");
    let display = format!("{}", schedule);

    assert!(display.contains("America/New_York"));
    assert!(display.contains("Crontab"));
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_timezone_consistency() {
    // Test that same schedule in different timezones produces different UTC times
    let ny_schedule = Schedule::crontab_tz("9", "0", "*", "*", "*", "America/New_York");
    let london_schedule = Schedule::crontab_tz("9", "0", "*", "*", "*", "Europe/London");

    let now = Utc::now();
    let ny_next = ny_schedule.next_run(Some(now)).unwrap();
    let london_next = london_schedule.next_run(Some(now)).unwrap();

    // 9 AM in New York and 9 AM in London are different UTC times
    // (typically 5-6 hour difference depending on DST)
    assert_ne!(ny_next.hour(), london_next.hour());
}

#[cfg(feature = "cron")]
#[test]
fn test_scheduled_task_timezone_persistence() {
    // Test that timezone-aware task persists correctly
    let schedule = Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York");
    let task = ScheduledTask::new("timezone_task".to_string(), schedule);

    // Serialize and deserialize
    let json = serde_json::to_string(&task).unwrap();
    let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

    // Verify timezone is preserved
    if let Schedule::Crontab { timezone, .. } = deserialized.schedule {
        assert_eq!(timezone, Some("America/New_York".to_string()));
    } else {
        panic!("Expected Crontab schedule");
    }
}

// ===== Scheduler Loop Tests =====

#[test]
fn test_scheduler_get_due_tasks_empty() {
    let scheduler = BeatScheduler::new();
    let due_tasks = scheduler.get_due_tasks();

    // New scheduler should have no due tasks
    assert_eq!(due_tasks.len(), 0);
}

#[test]
fn test_scheduler_get_due_tasks_with_due_task() {
    let mut scheduler = BeatScheduler::new();

    // Add task with very short interval (already due)
    let task = ScheduledTask::new("due_task".to_string(), Schedule::interval(1));
    scheduler.add_task(task).unwrap();

    // Set last run in the past manually
    if let Some(task) = scheduler.tasks.get_mut("due_task") {
        task.last_run_at = Some(Utc::now() - Duration::seconds(10));
    }

    // Should be due now
    let due_tasks = scheduler.get_due_tasks();
    assert_eq!(due_tasks.len(), 1);
    assert_eq!(due_tasks[0].name, "due_task");
}

#[test]
fn test_scheduler_get_due_tasks_with_future_task() {
    let mut scheduler = BeatScheduler::new();

    // Add task with long interval (not due yet)
    let task = ScheduledTask::new("future_task".to_string(), Schedule::interval(3600));
    scheduler.add_task(task).unwrap();

    // Mark as run now
    scheduler.mark_task_run("future_task").unwrap();

    // Should not be due yet
    let due_tasks = scheduler.get_due_tasks();
    assert_eq!(due_tasks.len(), 0);
}

#[test]
fn test_scheduler_get_due_tasks_mixed() {
    let mut scheduler = BeatScheduler::new();

    // Add due task
    let task1 = ScheduledTask::new("due_task".to_string(), Schedule::interval(1));
    scheduler.add_task(task1).unwrap();
    // Set last run in the past
    if let Some(task) = scheduler.tasks.get_mut("due_task") {
        task.last_run_at = Some(Utc::now() - Duration::seconds(10));
    }

    // Add future task
    let task2 = ScheduledTask::new("future_task".to_string(), Schedule::interval(3600));
    scheduler.add_task(task2).unwrap();
    scheduler.mark_task_run("future_task").unwrap();

    // Should only get the due task
    let due_tasks = scheduler.get_due_tasks();
    assert_eq!(due_tasks.len(), 1);
    assert_eq!(due_tasks[0].name, "due_task");
}

#[test]
fn test_scheduler_get_due_tasks_disabled() {
    let mut scheduler = BeatScheduler::new();

    // Add due but disabled task
    let task = ScheduledTask::new("disabled_task".to_string(), Schedule::interval(1)).disabled();
    scheduler.add_task(task).unwrap();
    // Set last run in the past
    if let Some(task) = scheduler.tasks.get_mut("disabled_task") {
        task.last_run_at = Some(Utc::now() - Duration::seconds(10));
    }

    // Disabled tasks should not be returned as due
    let due_tasks = scheduler.get_due_tasks();
    assert_eq!(due_tasks.len(), 0);
}

#[test]
fn test_scheduler_mark_task_run_updates_timestamp() {
    let mut scheduler = BeatScheduler::new();
    let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
    scheduler.add_task(task).unwrap();

    let before = Utc::now();
    std::thread::sleep(std::time::Duration::from_millis(10));
    scheduler.mark_task_run("test_task").unwrap();

    let task = scheduler.tasks.get("test_task").unwrap();
    assert!(task.last_run_at.is_some());
    assert!(task.last_run_at.unwrap() >= before);
}

#[test]
fn test_scheduler_mark_task_run_increments_count() {
    let mut scheduler = BeatScheduler::new();
    let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60));
    scheduler.add_task(task).unwrap();

    let initial_count = scheduler.tasks.get("test_task").unwrap().total_run_count;

    scheduler.mark_task_run("test_task").unwrap();
    let count_after_1 = scheduler.tasks.get("test_task").unwrap().total_run_count;
    assert_eq!(count_after_1, initial_count + 1);

    scheduler.mark_task_run("test_task").unwrap();
    let count_after_2 = scheduler.tasks.get("test_task").unwrap().total_run_count;
    assert_eq!(count_after_2, initial_count + 2);
}

#[test]
fn test_scheduler_multiple_due_tasks() {
    let mut scheduler = BeatScheduler::new();

    // Add 5 tasks with short intervals
    for i in 0..5 {
        let task = ScheduledTask::new(format!("task_{}", i), Schedule::interval(1));
        scheduler.add_task(task).unwrap();
        // Set last run in the past
        if let Some(task) = scheduler.tasks.get_mut(&format!("task_{}", i)) {
            task.last_run_at = Some(Utc::now() - Duration::seconds(10));
        }
    }

    // All 5 should be due
    let due_tasks = scheduler.get_due_tasks();
    assert_eq!(due_tasks.len(), 5);
}

#[test]
fn test_scheduler_task_prioritization() {
    let mut scheduler = BeatScheduler::new();

    // Add tasks with different intervals
    let task1 = ScheduledTask::new("task_60".to_string(), Schedule::interval(60));
    let task2 = ScheduledTask::new("task_120".to_string(), Schedule::interval(120));
    let task3 = ScheduledTask::new("task_30".to_string(), Schedule::interval(30));

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();
    scheduler.add_task(task3).unwrap();

    // Verify all tasks were added
    assert_eq!(scheduler.tasks.len(), 3);

    // All tasks should initially be due (never run)
    let due_tasks = scheduler.get_due_tasks();
    assert_eq!(due_tasks.len(), 3);
}

#[test]
fn test_scheduler_task_lifecycle() {
    let mut scheduler = BeatScheduler::new();

    // 1. Add task
    let task = ScheduledTask::new("lifecycle_task".to_string(), Schedule::interval(1));
    scheduler.add_task(task).unwrap();
    assert_eq!(scheduler.tasks.len(), 1);

    // 2. Task is due (short interval, never run)
    let due_tasks = scheduler.get_due_tasks();
    assert_eq!(due_tasks.len(), 1);

    // 3. Mark as run
    scheduler.mark_task_run("lifecycle_task").unwrap();

    // 4. Task should not be due immediately
    let due_tasks = scheduler.get_due_tasks();
    assert_eq!(due_tasks.len(), 0);

    // 5. Disable task
    if let Some(task) = scheduler.tasks.get_mut("lifecycle_task") {
        task.enabled = false;
    }
    let task = scheduler.tasks.get("lifecycle_task").unwrap();
    assert!(!task.enabled);

    // 6. Re-enable task
    if let Some(task) = scheduler.tasks.get_mut("lifecycle_task") {
        task.enabled = true;
    }
    let task = scheduler.tasks.get("lifecycle_task").unwrap();
    assert!(task.enabled);

    // 7. Remove task
    scheduler.remove_task("lifecycle_task").unwrap();
    assert_eq!(scheduler.tasks.len(), 0);
}

#[test]
fn test_scheduler_persistence_preserves_state() {
    let temp_file = NamedTempFile::new().unwrap();
    let state_file = temp_file.path().to_str().unwrap().to_string();

    // Create scheduler and add tasks
    let mut scheduler1 = BeatScheduler::with_persistence(state_file.clone());
    let task = ScheduledTask::new("persistent_task".to_string(), Schedule::interval(60));
    scheduler1.add_task(task).unwrap();
    scheduler1.mark_task_run("persistent_task").unwrap();
    scheduler1.save_state().unwrap();

    // Load scheduler from file
    let scheduler2 = BeatScheduler::load_from_file(&state_file).unwrap();

    // Verify state was preserved
    assert_eq!(scheduler2.tasks.len(), 1);
    let task = scheduler2.tasks.get("persistent_task").unwrap();
    assert_eq!(task.name, "persistent_task");
    assert!(task.last_run_at.is_some());
    assert_eq!(task.total_run_count, 1);
    // temp_file automatically cleaned up when dropped
}

#[test]
fn test_scheduler_loop_simulation() {
    let mut scheduler = BeatScheduler::new();

    // Add task with 1 second interval
    let task = ScheduledTask::new("loop_task".to_string(), Schedule::interval(1));
    scheduler.add_task(task).unwrap();

    // Simulate 5 iterations of scheduler loop
    let mut executions = 0;
    for _ in 0..5 {
        // Collect task names first to avoid borrow conflicts
        let task_names: Vec<String> = scheduler
            .get_due_tasks()
            .into_iter()
            .map(|t| t.name.clone())
            .collect();

        for task_name in task_names {
            scheduler.mark_task_run(&task_name).unwrap();
            executions += 1;
        }
        // Simulate time passing
        std::thread::sleep(std::time::Duration::from_millis(100));
    }

    // Should have executed at least once
    assert!(executions > 0);

    let task = scheduler.tasks.get("loop_task").unwrap();
    assert_eq!(task.total_run_count as usize, executions);
}

// ===== Weighted Fair Queuing Tests =====

#[test]
fn test_wfq_task_weight_validation() {
    let task = ScheduledTask::new("test".to_string(), Schedule::interval(60))
        .with_wfq_weight(5.0)
        .unwrap();
    assert_eq!(task.wfq_weight(), 5.0);

    // Test invalid weights
    let result =
        ScheduledTask::new("test".to_string(), Schedule::interval(60)).with_wfq_weight(0.05); // Too low
    assert!(result.is_err());

    let result =
        ScheduledTask::new("test".to_string(), Schedule::interval(60)).with_wfq_weight(15.0); // Too high
    assert!(result.is_err());
}

#[test]
fn test_wfq_basic_scheduling() {
    let mut scheduler = BeatScheduler::new();

    // Add tasks with different weights
    let task1 = ScheduledTask::new("low_weight".to_string(), Schedule::interval(1))
        .with_wfq_weight(0.5)
        .unwrap();
    let task2 = ScheduledTask::new("high_weight".to_string(), Schedule::interval(1))
        .with_wfq_weight(5.0)
        .unwrap();

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    // Mark tasks as run to set last_run_at
    std::thread::sleep(std::time::Duration::from_millis(1100));

    // Get WFQ tasks
    let wfq_tasks = scheduler.get_due_tasks_wfq();
    assert_eq!(wfq_tasks.len(), 2);

    // Both should have initial virtual finish time of 0
    for task in &wfq_tasks {
        assert_eq!(task.virtual_finish_time, 0.0);
    }
}

#[test]
fn test_wfq_virtual_time_update() {
    let mut scheduler = BeatScheduler::new();

    let task = ScheduledTask::new("test_task".to_string(), Schedule::interval(60))
        .with_wfq_weight(2.0)
        .unwrap();
    scheduler.add_task(task).unwrap();

    // Simulate execution
    scheduler
        .update_wfq_after_execution("test_task", 10.0)
        .unwrap();

    // Check virtual time was updated
    let task = scheduler.tasks.get("test_task").unwrap();
    assert!(task.wfq_state.is_some());

    let wfq_state = task.wfq_state.as_ref().unwrap();
    // With weight 2.0 and execution time 10.0:
    // virtual_finish_time = 0 + (10.0 / 2.0) = 5.0
    assert_eq!(wfq_state.virtual_finish_time, 5.0);
    assert_eq!(wfq_state.total_execution_time, 10.0);
}

#[test]
fn test_wfq_fairness() {
    let mut scheduler = BeatScheduler::new();

    // Create three tasks with different weights
    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(1))
        .with_wfq_weight(1.0)
        .unwrap();
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(1))
        .with_wfq_weight(2.0)
        .unwrap();
    let task3 = ScheduledTask::new("task3".to_string(), Schedule::interval(1))
        .with_wfq_weight(5.0)
        .unwrap();

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();
    scheduler.add_task(task3).unwrap();

    // Simulate executions with same duration
    scheduler.update_wfq_after_execution("task1", 10.0).unwrap();
    scheduler.update_wfq_after_execution("task2", 10.0).unwrap();
    scheduler.update_wfq_after_execution("task3", 10.0).unwrap();

    // Check virtual finish times are computed correctly based on weights
    let t1 = scheduler.tasks.get("task1").unwrap();
    let t2 = scheduler.tasks.get("task2").unwrap();
    let t3 = scheduler.tasks.get("task3").unwrap();

    let vft1 = t1.wfq_finish_time();
    let vft2 = t2.wfq_finish_time();
    let vft3 = t3.wfq_finish_time();

    // All tasks should have virtual finish times reflecting their execution
    // Task with higher weight should finish sooner (smaller vft increment)
    assert!(vft1 > 0.0);
    assert!(vft2 > 0.0);
    assert!(vft3 > 0.0);

    // The actual ordering depends on execution order and global virtual time
    // But weights should affect the virtual time increments
    let t1_state = t1.wfq_state.as_ref().unwrap();
    let t2_state = t2.wfq_state.as_ref().unwrap();
    let t3_state = t3.wfq_state.as_ref().unwrap();

    // Virtual time increment should be inversely proportional to weight
    // weight=1.0: increment = 10.0/1.0 = 10.0
    // weight=2.0: increment = 10.0/2.0 = 5.0
    // weight=5.0: increment = 10.0/5.0 = 2.0
    let increment1 = t1_state.virtual_finish_time - t1_state.virtual_start_time;
    let increment2 = t2_state.virtual_finish_time - t2_state.virtual_start_time;
    let increment3 = t3_state.virtual_finish_time - t3_state.virtual_start_time;

    assert_eq!(increment1, 10.0);
    assert_eq!(increment2, 5.0);
    assert_eq!(increment3, 2.0);
}

#[test]
fn test_wfq_task_ordering() {
    let mut scheduler = BeatScheduler::new();

    // Create tasks with different weights and execution histories
    let task1 = ScheduledTask::new("heavy_task".to_string(), Schedule::interval(1))
        .with_wfq_weight(1.0)
        .unwrap();
    let task2 = ScheduledTask::new("light_task".to_string(), Schedule::interval(1))
        .with_wfq_weight(5.0)
        .unwrap();

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    // Simulate first execution - both tasks run
    scheduler
        .update_wfq_after_execution("heavy_task", 10.0)
        .unwrap();
    scheduler
        .update_wfq_after_execution("light_task", 10.0)
        .unwrap();

    std::thread::sleep(std::time::Duration::from_millis(1100));

    // Get tasks - ordering is based on virtual finish time
    let wfq_tasks = scheduler.get_due_tasks_wfq();
    assert_eq!(wfq_tasks.len(), 2);

    // Verify weights are preserved
    let heavy = wfq_tasks.iter().find(|t| t.name == "heavy_task").unwrap();
    let light = wfq_tasks.iter().find(|t| t.name == "light_task").unwrap();
    assert_eq!(heavy.weight, 1.0);
    assert_eq!(light.weight, 5.0);
}

#[test]
fn test_wfq_stats() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
        .with_wfq_weight(2.0)
        .unwrap();
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
        .with_wfq_weight(3.0)
        .unwrap();

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    let stats = scheduler.get_wfq_stats();
    assert_eq!(stats.total_tasks, 2);
    assert_eq!(stats.tasks_with_wfq_config, 2);
    assert_eq!(stats.total_weight, 5.0);
    assert_eq!(stats.average_weight, 2.5);
    assert_eq!(stats.global_virtual_time, 0.0);
}

#[test]
fn test_wfq_stats_display() {
    let mut scheduler = BeatScheduler::new();

    let task = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
        .with_wfq_weight(2.0)
        .unwrap();
    scheduler.add_task(task).unwrap();

    let stats = scheduler.get_wfq_stats();
    let display = format!("{}", stats);
    assert!(display.contains("WFQ Stats"));
    assert!(display.contains("1/1 tasks configured"));
}

#[test]
fn test_wfq_with_disabled_tasks() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("enabled_task".to_string(), Schedule::interval(1))
        .with_wfq_weight(2.0)
        .unwrap();

    let mut task2 = ScheduledTask::new("disabled_task".to_string(), Schedule::interval(1))
        .with_wfq_weight(5.0)
        .unwrap();
    task2.enabled = false;

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    std::thread::sleep(std::time::Duration::from_millis(1100));

    // Only enabled task should be returned
    let wfq_tasks = scheduler.get_due_tasks_wfq();
    assert_eq!(wfq_tasks.len(), 1);
    assert_eq!(wfq_tasks[0].name, "enabled_task");
}

#[test]
fn test_wfq_global_virtual_time() {
    let mut scheduler = BeatScheduler::new();

    let task1 = ScheduledTask::new("task1".to_string(), Schedule::interval(60))
        .with_wfq_weight(1.0)
        .unwrap();
    let task2 = ScheduledTask::new("task2".to_string(), Schedule::interval(60))
        .with_wfq_weight(2.0)
        .unwrap();

    scheduler.add_task(task1).unwrap();
    scheduler.add_task(task2).unwrap();

    // Simulate executions
    scheduler.update_wfq_after_execution("task1", 10.0).unwrap();
    scheduler.update_wfq_after_execution("task2", 10.0).unwrap();

    // Global virtual time should be the max of all virtual finish times
    let stats = scheduler.get_wfq_stats();
    // task1 executes first: vft1 = 0 + 10.0/1.0 = 10.0
    // task2 executes second: vstart2 = max(0, 10.0) = 10.0
    //                        vft2 = 10.0 + 10.0/2.0 = 15.0
    // global = max(10.0, 15.0) = 15.0
    assert_eq!(stats.global_virtual_time, 15.0);
}

#[test]
fn test_wfq_multiple_executions() {
    let mut scheduler = BeatScheduler::new();

    let task = ScheduledTask::new("task".to_string(), Schedule::interval(60))
        .with_wfq_weight(2.0)
        .unwrap();
    scheduler.add_task(task).unwrap();

    // First execution
    scheduler.update_wfq_after_execution("task", 10.0).unwrap();
    let task_state = scheduler.tasks.get("task").unwrap();
    let vft1 = task_state.wfq_finish_time();
    assert_eq!(vft1, 5.0); // 10.0 / 2.0

    // Second execution - virtual time should continue from previous
    scheduler.update_wfq_after_execution("task", 6.0).unwrap();
    let task_state = scheduler.tasks.get("task").unwrap();
    let vft2 = task_state.wfq_finish_time();
    assert_eq!(vft2, 8.0); // 5.0 + (6.0 / 2.0)
    assert_eq!(
        task_state.wfq_state.as_ref().unwrap().total_execution_time,
        16.0
    );
}

#[test]
fn test_wfq_task_weight_default() {
    let task = ScheduledTask::new("task".to_string(), Schedule::interval(60));
    assert_eq!(task.wfq_weight(), 1.0); // Default weight
}

#[test]
fn test_wfq_serialization() {
    let task = ScheduledTask::new("task".to_string(), Schedule::interval(60))
        .with_wfq_weight(3.5)
        .unwrap();

    let json = serde_json::to_string(&task).unwrap();
    let deserialized: ScheduledTask = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.wfq_weight(), 3.5);
}

// ===== Heartbeat-Aware Scheduler Tests =====

#[tokio::test]
async fn test_tick_without_heartbeat() {
    // Single instance mode - tick returns due tasks
    let mut scheduler = BeatScheduler::new();
    scheduler
        .add_task(ScheduledTask::new(
            "test_task".to_string(),
            Schedule::interval(1),
        ))
        .expect("failed to add task");
    // Wait for task to become due
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
    let due = scheduler.tick().await.expect("tick failed");
    assert!(
        !due.is_empty(),
        "single-instance tick should return due tasks"
    );
}

#[tokio::test]
async fn test_tick_as_leader() {
    use crate::heartbeat::{BeatHeartbeat, HeartbeatConfig};
    use crate::lock::InMemoryLockBackend;

    let lock_backend = Arc::new(InMemoryLockBackend::new());
    let hb = BeatHeartbeat::new(
        "instance-1".to_string(),
        lock_backend,
        HeartbeatConfig::new(),
    );

    // Become leader first
    let became_leader = hb
        .try_become_leader()
        .await
        .expect("leader election failed");
    assert!(became_leader);

    let mut scheduler = BeatScheduler::new();
    scheduler.with_heartbeat(hb);
    scheduler
        .add_task(ScheduledTask::new(
            "test_task".to_string(),
            Schedule::interval(1),
        ))
        .expect("failed to add task");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let due = scheduler.tick().await.expect("tick failed");
    assert!(!due.is_empty(), "leader tick should return due tasks");
}

#[tokio::test]
async fn test_tick_as_standby_returns_empty() {
    use crate::heartbeat::{BeatHeartbeat, HeartbeatConfig};
    use crate::lock::InMemoryLockBackend;

    let lock_backend = Arc::new(InMemoryLockBackend::new());

    // Instance 1 becomes leader
    let hb1 = BeatHeartbeat::new(
        "instance-1".to_string(),
        lock_backend.clone(),
        HeartbeatConfig::new(),
    );
    let became_leader = hb1
        .try_become_leader()
        .await
        .expect("leader election failed");
    assert!(became_leader);

    // Instance 2 is standby
    let hb2 = BeatHeartbeat::new(
        "instance-2".to_string(),
        lock_backend,
        HeartbeatConfig::new(),
    );
    let became_leader2 = hb2
        .try_become_leader()
        .await
        .expect("leader election failed");
    assert!(!became_leader2); // Should fail, becomes standby

    let mut scheduler = BeatScheduler::new();
    scheduler.with_heartbeat(hb2);
    scheduler
        .add_task(ScheduledTask::new(
            "test_task".to_string(),
            Schedule::interval(1),
        ))
        .expect("failed to add task");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let due = scheduler.tick().await.expect("tick failed");
    assert!(due.is_empty(), "standby tick should not return tasks");
}

#[tokio::test]
async fn test_shutdown_graceful() {
    use crate::heartbeat::{BeatHeartbeat, HeartbeatConfig};
    use crate::lock::InMemoryLockBackend;

    let lock_backend = Arc::new(InMemoryLockBackend::new());
    let hb = BeatHeartbeat::new(
        "instance-1".to_string(),
        lock_backend.clone(),
        HeartbeatConfig::new(),
    );
    let became_leader = hb
        .try_become_leader()
        .await
        .expect("leader election failed");
    assert!(became_leader);

    let mut scheduler = BeatScheduler::new();
    scheduler.with_heartbeat(hb);

    scheduler
        .shutdown_graceful()
        .await
        .expect("shutdown failed");
    assert!(
        !scheduler.is_leader().await,
        "should no longer be leader after shutdown"
    );
}

#[tokio::test]
async fn test_heartbeat_tick_without_heartbeat() {
    let scheduler = BeatScheduler::new();
    let role = scheduler
        .heartbeat_tick()
        .await
        .expect("heartbeat_tick failed");
    assert!(role.is_none(), "no heartbeat should return None");
}

#[tokio::test]
async fn test_heartbeat_tick_as_leader() {
    use crate::heartbeat::{BeatHeartbeat, BeatRole, HeartbeatConfig};
    use crate::lock::InMemoryLockBackend;

    let lock_backend = Arc::new(InMemoryLockBackend::new());
    let hb = BeatHeartbeat::new(
        "instance-1".to_string(),
        lock_backend,
        HeartbeatConfig::new(),
    );
    let _ = hb
        .try_become_leader()
        .await
        .expect("leader election failed");

    let mut scheduler = BeatScheduler::new();
    scheduler.with_heartbeat(hb);

    let role = scheduler
        .heartbeat_tick()
        .await
        .expect("heartbeat_tick failed");
    assert_eq!(role, Some(BeatRole::Leader));
}

#[tokio::test]
async fn test_get_leader_due_tasks_single_instance() {
    let mut scheduler = BeatScheduler::new();
    scheduler
        .add_task(ScheduledTask::new(
            "task1".to_string(),
            Schedule::interval(1),
        ))
        .expect("failed to add task");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let due = scheduler.get_leader_due_tasks().await;
    assert!(!due.is_empty(), "single-instance should return due tasks");
}

#[tokio::test]
async fn test_get_leader_due_tasks_standby() {
    use crate::heartbeat::{BeatHeartbeat, HeartbeatConfig};
    use crate::lock::InMemoryLockBackend;

    let lock_backend = Arc::new(InMemoryLockBackend::new());

    // Instance 1 takes leadership
    let hb1 = BeatHeartbeat::new(
        "instance-1".to_string(),
        lock_backend.clone(),
        HeartbeatConfig::new(),
    );
    let _ = hb1
        .try_become_leader()
        .await
        .expect("leader election failed");

    // Instance 2 is standby
    let hb2 = BeatHeartbeat::new(
        "instance-2".to_string(),
        lock_backend,
        HeartbeatConfig::new(),
    );
    let _ = hb2
        .try_become_leader()
        .await
        .expect("leader election failed");

    let mut scheduler = BeatScheduler::new();
    scheduler.with_heartbeat(hb2);
    scheduler
        .add_task(ScheduledTask::new(
            "task1".to_string(),
            Schedule::interval(1),
        ))
        .expect("failed to add task");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let due = scheduler.get_leader_due_tasks().await;
    assert!(due.is_empty(), "standby should not return due tasks");
}

#[tokio::test]
async fn test_update_heartbeat_info_updates_metadata() {
    use crate::heartbeat::{BeatHeartbeat, HeartbeatConfig};
    use crate::lock::InMemoryLockBackend;

    let lock_backend = Arc::new(InMemoryLockBackend::new());
    let hb = BeatHeartbeat::new(
        "instance-1".to_string(),
        lock_backend,
        HeartbeatConfig::new(),
    );
    let _ = hb
        .try_become_leader()
        .await
        .expect("leader election failed");

    let mut scheduler = BeatScheduler::new();
    scheduler
        .add_task(ScheduledTask::new(
            "my_task".to_string(),
            Schedule::interval(60),
        ))
        .expect("failed to add task");
    scheduler.with_heartbeat(hb);

    scheduler.update_heartbeat_info().await;

    let info = scheduler
        .heartbeat()
        .expect("heartbeat should be set")
        .info()
        .await;
    assert_eq!(info.schedule_count, 1);
}

#[tokio::test]
async fn test_update_heartbeat_info_noop_without_heartbeat() {
    let scheduler = BeatScheduler::new();
    // Should not panic
    scheduler.update_heartbeat_info().await;
}

// ===== Dynamic Registry & Timezone Schedule Tests =====
//
// These tests cover the runtime add/remove/update story (both the dedicated
// thread-safe `ScheduleRegistry` and the scheduler-level `*_entry` API) and the
// timezone-aware crontab evaluation including a DST spring-forward case. All
// assertions use fixed dates so they are deterministic.

use chrono::TimeZone;

#[test]
fn test_scheduler_entry_api_add_list_update_remove() {
    let mut scheduler = BeatScheduler::new();

    scheduler
        .add_entry(ScheduledTask::new(
            "alpha".to_string(),
            Schedule::interval(60),
        ))
        .unwrap();
    scheduler
        .add_entry(ScheduledTask::new(
            "beta".to_string(),
            Schedule::interval(120),
        ))
        .unwrap();

    let mut names: Vec<String> = scheduler
        .list_entries()
        .into_iter()
        .map(|t| t.name)
        .collect();
    names.sort();
    assert_eq!(names, vec!["alpha".to_string(), "beta".to_string()]);

    // Update in place.
    let updated = scheduler
        .update_entry("alpha", |task| {
            task.options.priority = Some(9);
            task.enabled = false;
        })
        .unwrap();
    assert!(updated);
    let alpha = scheduler.get_task("alpha").unwrap();
    assert_eq!(alpha.options.priority, Some(9));
    assert!(!alpha.enabled);

    // Updating a missing entry returns false.
    assert!(!scheduler.update_entry("ghost", |_| {}).unwrap());

    // Remove.
    let removed = scheduler.remove_entry("beta").unwrap();
    assert!(removed.is_some());
    assert_eq!(scheduler.list_entries().len(), 1);
}

#[test]
fn test_scheduler_to_registry_bridge_is_independent() {
    let mut scheduler = BeatScheduler::new();
    scheduler
        .add_entry(ScheduledTask::new(
            "seed".to_string(),
            Schedule::interval(60),
        ))
        .unwrap();

    let registry = scheduler.to_registry();
    assert_eq!(registry.len().unwrap(), 1);

    // Mutating the registry must not affect the scheduler (snapshot semantics).
    registry
        .add_entry(ScheduledTask::new(
            "registry_only".to_string(),
            Schedule::interval(60),
        ))
        .unwrap();
    assert_eq!(registry.len().unwrap(), 2);
    assert_eq!(scheduler.list_entries().len(), 1);
}

#[test]
fn test_registry_runtime_update_picked_up_between_ticks() {
    // Model a beat loop: snapshot due entries, "execute" them by marking run,
    // and verify that a runtime schedule change made between ticks takes effect.
    let registry = ScheduleRegistry::new();
    let base = Utc.with_ymd_and_hms(2026, 2, 1, 0, 0, 0).unwrap();

    let mut task = ScheduledTask::new("hourly".to_string(), Schedule::interval(3600));
    task.last_run_at = Some(base);
    registry.add_entry(task).unwrap();

    // Tick 1 at +30s: not due yet (interval 3600s).
    let tick1 = base + Duration::seconds(30);
    assert!(registry.due_entries_at(tick1).unwrap().is_empty());

    // Between ticks, a control plane shortens the interval to 10s.
    assert!(registry
        .update_schedule("hourly", Schedule::interval(10))
        .unwrap());

    // Tick 2 at +30s: now due because the new interval (10s) has elapsed.
    let due = registry.due_entries_at(tick1).unwrap();
    assert_eq!(due.len(), 1);
    assert_eq!(due[0].name, "hourly");

    // Execute it (advance last_run), then it is no longer due at the same tick.
    assert!(registry.mark_run_at("hourly", tick1).unwrap());
    assert!(registry.due_entries_at(tick1).unwrap().is_empty());

    // Due again 11s later.
    let tick3 = tick1 + Duration::seconds(11);
    assert_eq!(registry.due_entries_at(tick3).unwrap().len(), 1);
}

#[test]
fn test_registry_add_remove_at_runtime_changes_due_set() {
    let registry = ScheduleRegistry::new();
    let now = Utc.with_ymd_and_hms(2026, 2, 1, 12, 0, 0).unwrap();

    // Add two never-run (immediately due) tasks.
    registry
        .add_entry(ScheduledTask::new("a".to_string(), Schedule::interval(60)))
        .unwrap();
    registry
        .add_entry(ScheduledTask::new("b".to_string(), Schedule::interval(60)))
        .unwrap();
    assert_eq!(registry.due_entries_at(now).unwrap().len(), 2);

    // Remove one at runtime; due set shrinks.
    registry.remove_entry("a").unwrap();
    let due: Vec<String> = registry
        .due_entries_at(now)
        .unwrap()
        .into_iter()
        .map(|t| t.name)
        .collect();
    assert_eq!(due, vec!["b".to_string()]);

    // Add a third at runtime; due set grows.
    registry
        .add_entry(ScheduledTask::new("c".to_string(), Schedule::interval(60)))
        .unwrap();
    assert_eq!(registry.due_entries_at(now).unwrap().len(), 2);
}

#[cfg(feature = "cron")]
#[test]
fn test_task_with_timezone_schedule_next_run() {
    // A ScheduledTask carrying a timezone-aware crontab computes next_run in
    // local wall-clock terms. 09:00 Tokyo (UTC+9) maps to 00:00 UTC.
    use chrono_tz::Asia::Tokyo;
    let schedule = Schedule::crontab("0", "9", "*", "*", "*").with_timezone(Tokyo);
    let mut task = ScheduledTask::new("tokyo_job".to_string(), schedule);
    task.last_run_at = Some(Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap());

    let next = task.next_run_time().unwrap();
    let local = next.with_timezone(&Tokyo);
    assert_eq!(local.hour(), 9);
    assert_eq!(local.minute(), 0);
}

#[cfg(feature = "cron")]
#[test]
fn test_task_timezone_dst_spring_forward_gap() {
    // 2026-03-08 New York spring-forward: 02:00 -> 03:00, so the 02:30 daily
    // run cannot occur that day and must roll to 2026-03-09 02:30 EDT == 06:30
    // UTC. This is the DST gap case requested by the roadmap.
    use chrono::Datelike;
    use chrono_tz::America::New_York;

    let schedule = Schedule::crontab("30", "2", "*", "*", "*").with_timezone(New_York);
    let mut task = ScheduledTask::new("dst_job".to_string(), schedule);
    // last_run = 2026-03-08 01:00 New York (EST, UTC-5) == 06:00 UTC.
    task.last_run_at = Some(
        New_York
            .with_ymd_and_hms(2026, 3, 8, 1, 0, 0)
            .unwrap()
            .with_timezone(&Utc),
    );

    let next = task.next_run_time().unwrap();
    let local = next.with_timezone(&New_York);
    assert_ne!(
        local.day(),
        8,
        "02:30 does not exist on the spring-forward day"
    );
    assert_eq!(local.day(), 9);
    assert_eq!(local.hour(), 2);
    assert_eq!(local.minute(), 30);
    assert_eq!(next, Utc.with_ymd_and_hms(2026, 3, 9, 6, 30, 0).unwrap());
}

#[cfg(feature = "cron")]
#[test]
fn test_task_timezone_persists_through_scheduler() {
    // Adding a timezone-aware task to the scheduler preserves the timezone and
    // it survives a JSON round-trip via the entry API.
    use chrono_tz::America::New_York;

    let mut scheduler = BeatScheduler::new();
    let schedule = Schedule::crontab("0", "9", "1-5", "*", "*").with_timezone(New_York);
    scheduler
        .add_entry(ScheduledTask::new("tz_task".to_string(), schedule))
        .unwrap();

    let json = scheduler.export_state().unwrap();
    assert!(json.contains("America/New_York"));

    let stored = scheduler.get_task("tz_task").unwrap();
    assert_eq!(stored.schedule.timezone_name(), Some("America/New_York"));
}

// ===== Dispatch Lock Tests (duplicate-execution prevention) =====

// `chrono::TimeZone` is already imported above for the timezone tests.

/// A fixed, deterministic scheduled instant used for lock-key assertions so the
/// tests never depend on the wall clock.
fn fixed_instant(second: u32) -> chrono::DateTime<Utc> {
    Utc.with_ymd_and_hms(2026, 6, 13, 12, 0, second)
        .single()
        .expect("fixed instant is valid")
}

#[test]
fn test_dispatch_lock_key_is_deterministic_and_distinct() {
    let i0 = fixed_instant(0);
    let i1 = fixed_instant(1);

    // Same (entry, instant) => identical key.
    assert_eq!(
        dispatch_lock_key("entry", i0),
        dispatch_lock_key("entry", i0)
    );
    // Different instant => different key.
    assert_ne!(
        dispatch_lock_key("entry", i0),
        dispatch_lock_key("entry", i1)
    );
    // Different entry => different key.
    assert_ne!(dispatch_lock_key("a", i0), dispatch_lock_key("b", i0));
    // Exact key shape is stable (RFC 3339, timezone-explicit).
    assert_eq!(
        dispatch_lock_key("entry", i0),
        "celers-beat:dispatch:entry@2026-06-13T12:00:00+00:00"
    );
}

#[tokio::test]
async fn test_dispatch_lock_blocks_second_instance_same_instant() {
    // Two schedulers share one in-memory lock backend; the per-fire lock must
    // be mutually exclusive for the same (entry, instant).
    let backend = Arc::new(InMemoryLockBackend::new());

    let mut beat_a = BeatScheduler::new();
    beat_a.with_lock_backend(backend.clone());
    let mut beat_b = BeatScheduler::new();
    beat_b.with_lock_backend(backend.clone());

    let instant = fixed_instant(0);

    // First instance acquires the fire.
    let a = beat_a
        .try_acquire_dispatch_lock("shared_entry", instant, DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("acquire should not error");
    assert!(a, "first instance must win the fire lock");

    // Second instance is blocked on the same fire.
    let b = beat_b
        .try_acquire_dispatch_lock("shared_entry", instant, DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("acquire should not error");
    assert!(!b, "second instance must be blocked for the same instant");
}

#[tokio::test]
async fn test_dispatch_lock_release_allows_next_instant() {
    let backend = Arc::new(InMemoryLockBackend::new());

    let mut beat_a = BeatScheduler::new();
    beat_a.with_lock_backend(backend.clone());
    let mut beat_b = BeatScheduler::new();
    beat_b.with_lock_backend(backend.clone());

    let instant0 = fixed_instant(0);
    let instant1 = fixed_instant(1);

    // A wins instant0.
    assert!(beat_a
        .try_acquire_dispatch_lock("entry", instant0, DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("acquire"));

    // While A holds instant0, B cannot dispatch instant0...
    assert!(!beat_b
        .try_acquire_dispatch_lock("entry", instant0, DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("acquire"));
    // ...but the *next* instant is a different key, so B can take it.
    assert!(beat_b
        .try_acquire_dispatch_lock("entry", instant1, DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("acquire"));

    // A releases instant0; a fresh attempt on instant0 now succeeds.
    let released = beat_a
        .release_dispatch_lock("entry", instant0)
        .await
        .expect("release");
    assert!(released, "owner should release its own fire lock");

    assert!(beat_b
        .try_acquire_dispatch_lock("entry", instant0, DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("acquire after release"));
}

#[tokio::test]
async fn test_tick_with_locks_single_instance_dispatches() {
    // Without a shared backend a single instance dispatches its due entry.
    let mut scheduler = BeatScheduler::new();
    scheduler.with_lock_backend(Arc::new(InMemoryLockBackend::new()));
    scheduler
        .add_task(ScheduledTask::new(
            "solo".to_string(),
            Schedule::interval(1),
        ))
        .expect("add task");

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let dispatched = scheduler
        .tick_with_locks(DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("tick_with_locks");
    assert_eq!(dispatched, vec!["solo".to_string()]);
}

#[tokio::test]
async fn test_tick_with_locks_dedupes_across_instances() {
    // Two instances sharing a lock backend each consider the same entry due for
    // the same fire instant; exactly one of them dispatches it.
    let backend = Arc::new(InMemoryLockBackend::new());

    let mut beat_a = BeatScheduler::new();
    beat_a.with_lock_backend(backend.clone());
    beat_a
        .add_task(ScheduledTask::new("dup".to_string(), Schedule::interval(1)))
        .expect("add to a");

    let mut beat_b = BeatScheduler::new();
    beat_b.with_lock_backend(backend.clone());
    beat_b
        .add_task(ScheduledTask::new("dup".to_string(), Schedule::interval(1)))
        .expect("add to b");

    // Make both consider the entry due for the SAME instant by pinning an
    // identical last_run_at, then waiting until that fire is in the past.
    let last = Utc::now() - chrono::Duration::seconds(5);
    if let Some(task) = beat_a.tasks.get_mut("dup") {
        task.last_run_at = Some(last);
        task.update_next_run_cache();
    }
    if let Some(task) = beat_b.tasks.get_mut("dup") {
        task.last_run_at = Some(last);
        task.update_next_run_cache();
    }

    let from_a = beat_a
        .tick_with_locks(DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("a tick");
    let from_b = beat_b
        .tick_with_locks(DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("b tick");

    let total = from_a.len() + from_b.len();
    assert_eq!(
        total, 1,
        "exactly one instance should dispatch the shared fire (a={from_a:?}, b={from_b:?})"
    );
}

#[tokio::test]
async fn test_tick_with_locks_standby_dispatches_nothing() {
    use crate::heartbeat::{BeatHeartbeat, HeartbeatConfig};

    let lock_backend = Arc::new(InMemoryLockBackend::new());

    // Instance 1 becomes leader.
    let hb1 = BeatHeartbeat::new(
        "leader".to_string(),
        lock_backend.clone(),
        HeartbeatConfig::new(),
    );
    assert!(hb1.try_become_leader().await.expect("election"));

    // Instance 2 is standby.
    let hb2 = BeatHeartbeat::new(
        "standby".to_string(),
        lock_backend.clone(),
        HeartbeatConfig::new(),
    );
    assert!(!hb2.try_become_leader().await.expect("election"));

    let mut standby = BeatScheduler::new();
    standby.with_heartbeat(hb2);
    standby.with_lock_backend(lock_backend);
    standby
        .add_task(ScheduledTask::new(
            "should_not_run".to_string(),
            Schedule::interval(1),
        ))
        .expect("add task");
    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    let dispatched = standby
        .tick_with_locks(DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("standby tick");
    assert!(
        dispatched.is_empty(),
        "standby instance must not dispatch any fire"
    );
}
