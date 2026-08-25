//! Regression tests for beat scheduler correctness defects.
//!
//! Every test here pins a specific behaviour that was previously wrong:
//! colliding lock-owner identities, a dispatch lock keyed on a clock-relative
//! instant, catch-up configuration that was accepted and ignored, non-atomic
//! state writes, an unstable jitter hash, and unbounded history growth.
//!
//! All of them are deterministic: instants are injected rather than slept for.

use crate::catchup::CatchupPolicy as OccurrenceCatchupPolicy;
use crate::dispatch_lock::{dispatch_lock_key, DEFAULT_DISPATCH_LOCK_TTL_SECS};
use crate::history::{CatchupPolicy, ExecutionRecord, ExecutionResult, Jitter};
use crate::jitter::bounded_jitter_offset;
use crate::lock::InMemoryLockBackend;
use crate::schedule::{
    BusinessCalendar, BusinessHours, DayOfWeek, Holiday, HolidayCalendar, Schedule,
};
use crate::scheduler::BeatScheduler;
use crate::task::{ScheduledTask, DEFAULT_MAX_HISTORY_SIZE, MAX_VERSION_HISTORY};
use chrono::{DateTime, Datelike, Duration, TimeZone, Timelike, Utc};
use std::sync::Arc;

fn at(y: i32, mo: u32, d: u32, h: u32, mi: u32) -> DateTime<Utc> {
    Utc.with_ymd_and_hms(y, mo, d, h, mi, 0)
        .single()
        .expect("valid timestamp")
}

/// Unique temp path inside the system temp dir (no fixed names, no `/tmp`
/// hardcoding).
fn temp_state_path(tag: &str) -> std::path::PathBuf {
    let unique = format!(
        "celers-beat-{}-{}-{}.json",
        tag,
        std::process::id(),
        uuid::Uuid::new_v4()
    );
    std::env::temp_dir().join(unique)
}

// ---------------------------------------------------------------------------
// Instance identity (lock ownership)
// ---------------------------------------------------------------------------

/// Regression: every scheduler in a process was named `scheduler-0`,
/// `scheduler-1`, ... so two beat processes both called themselves
/// `scheduler-0`. Lock backends treat an owner match as a successful
/// re-entrant acquire, which meant the "distributed" lock excluded nobody.
#[test]
fn instance_ids_are_unique_per_scheduler() {
    let a = BeatScheduler::new();
    let b = BeatScheduler::new();
    let c = BeatScheduler::with_persistence(temp_state_path("ids"));

    assert_ne!(a.instance_id(), b.instance_id());
    assert_ne!(a.instance_id(), c.instance_id());
    assert_ne!(b.instance_id(), c.instance_id());

    // Not the old process-local counter form.
    assert!(!a.instance_id().starts_with("scheduler-"));
    // Carries the pid so two processes on one host cannot collide either.
    assert!(a.instance_id().contains(&std::process::id().to_string()));
}

/// Regression: `lock_manager` was `#[serde(default)]` and therefore persisted,
/// so a restarted scheduler re-adopted the previous run's in-memory locks as
/// its own.
#[test]
fn lock_manager_is_not_persisted() {
    let path = temp_state_path("locks");
    let mut scheduler = BeatScheduler::with_persistence(&path);
    scheduler
        .add_task(ScheduledTask::new(
            "task".to_string(),
            Schedule::interval(60),
        ))
        .expect("add task");
    assert!(scheduler
        .try_acquire_lock("task", Some(300))
        .expect("acquire lock"));
    assert!(scheduler.is_task_locked("task"));
    scheduler.save_state().expect("save state");

    let json = std::fs::read_to_string(&path).expect("read state");
    assert!(
        !json.contains("lock_manager"),
        "live lock state must not be written to the state file"
    );

    let reloaded = BeatScheduler::load_from_file(&path).expect("reload");
    assert!(
        !reloaded.is_task_locked("task"),
        "a restarted scheduler must not re-adopt stale locks"
    );
    assert_ne!(reloaded.instance_id(), scheduler.instance_id());

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.bak", path.display()));
}

// ---------------------------------------------------------------------------
// Per-fire dispatch locking
// ---------------------------------------------------------------------------

/// Regression: `Schedule::Interval::next_run(None)` evaluated `Utc::now() +
/// every`, so a never-run task produced a *different* lock key on every
/// evaluation. Two instances therefore never contended for a task's first fire
/// and both dispatched it.
#[test]
fn first_fire_instant_is_deterministic_across_evaluations() {
    let mut task = ScheduledTask::new("report".to_string(), Schedule::interval(60));
    task.created_at = at(2026, 6, 13, 12, 0);
    task.invalidate_next_run_cache();

    let first = task.next_run_time().expect("next run");
    for _ in 0..100 {
        assert_eq!(task.next_run_time().expect("next run"), first);
    }
    assert_eq!(first, at(2026, 6, 13, 12, 1));

    // Two independently constructed replicas of the same entry agree, which is
    // what makes the derived lock key contended rather than disjoint.
    let mut replica = ScheduledTask::new("report".to_string(), Schedule::interval(60));
    replica.created_at = task.created_at;
    replica.invalidate_next_run_cache();
    assert_eq!(
        dispatch_lock_key("report", replica.next_run_time().expect("next run")),
        dispatch_lock_key("report", first)
    );
}

/// Regression: the module's core guarantee — exactly one instance wins a given
/// `(entry, instant)` — did not hold for a task's *first* fire.
#[tokio::test]
async fn two_instances_dedupe_a_never_run_task_first_fire() {
    let backend = Arc::new(InMemoryLockBackend::new());
    let created = Utc::now() - Duration::seconds(120);

    let build = |name: &str| {
        let mut scheduler = BeatScheduler::new();
        scheduler.with_lock_backend(backend.clone());
        let mut task = ScheduledTask::new(name.to_string(), Schedule::interval(60));
        // Never run, but registered long enough ago that the first occurrence
        // has already elapsed.
        task.created_at = created;
        task.invalidate_next_run_cache();
        scheduler.add_task(task).expect("add task");
        scheduler
    };

    let mut instance_a = build("first_fire");
    let mut instance_b = build("first_fire");
    assert_ne!(instance_a.instance_id(), instance_b.instance_id());

    let dispatched_a = instance_a
        .tick_with_locks(DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("tick a");
    let dispatched_b = instance_b
        .tick_with_locks(DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("tick b");

    assert_eq!(
        dispatched_a.len() + dispatched_b.len(),
        1,
        "exactly one instance must dispatch the first fire (a={dispatched_a:?}, b={dispatched_b:?})"
    );
}

/// Regression: `mark_task_run` advanced `last_run_at` but left
/// `cached_next_run` holding the previous fire's instant, so the *next* fire
/// recomputed an already-claimed lock key and was silently skipped forever.
#[test]
fn marking_a_run_invalidates_the_cached_next_run() {
    let mut scheduler = BeatScheduler::new();
    let mut task = ScheduledTask::new("cached".to_string(), Schedule::interval(60));
    task.created_at = Utc::now() - Duration::seconds(120);
    scheduler.add_task(task).expect("add task");

    let before = scheduler
        .get_task("cached")
        .expect("task")
        .next_run_time()
        .expect("next run");

    scheduler.mark_task_run("cached").expect("mark run");

    let after = scheduler
        .get_task("cached")
        .expect("task")
        .next_run_time()
        .expect("next run");

    assert_ne!(
        before, after,
        "next run must advance after a fire, otherwise the dispatch lock key repeats"
    );
    assert!(after > before);
    assert_ne!(
        dispatch_lock_key("cached", before),
        dispatch_lock_key("cached", after)
    );
}

// ---------------------------------------------------------------------------
// Catch-up
// ---------------------------------------------------------------------------

fn catchup_task(name: &str, policy: CatchupPolicy, minutes_behind: i64) -> ScheduledTask {
    let mut task =
        ScheduledTask::new(name.to_string(), Schedule::interval(60)).with_catchup_policy(policy);
    task.last_run_at = Some(Utc::now() - Duration::minutes(minutes_behind));
    task.invalidate_next_run_cache();
    task
}

/// Regression: `catchup_policy` was settable, persisted and versioned, but no
/// code path ever read it — a scheduler down for hours replayed nothing
/// regardless of configuration.
#[test]
fn catchup_policy_controls_how_many_missed_occurrences_replay() {
    let now = Utc::now();
    let scheduler = BeatScheduler::new();

    // Five whole intervals elapsed => the current fire plus four misses.
    let skip = catchup_task("skip", CatchupPolicy::Skip, 5);
    let once = catchup_task("once", CatchupPolicy::RunOnce, 5);
    let multiple = catchup_task("multiple", CatchupPolicy::RunMultiple { max_catchup: 2 }, 5);

    let skip_fires = scheduler.planned_fires(&skip, now).expect("skip fires");
    let once_fires = scheduler.planned_fires(&once, now).expect("once fires");
    let multiple_fires = scheduler
        .planned_fires(&multiple, now)
        .expect("multiple fires");

    assert_eq!(
        skip_fires.len(),
        1,
        "Skip must drop every missed occurrence"
    );
    assert_eq!(once_fires.len(), 2, "RunOnce replays a single miss");
    assert_eq!(
        multiple_fires.len(),
        3,
        "RunMultiple{{max_catchup: 2}} replays two misses"
    );

    // Instants are ascending and all in the past.
    for fires in [&skip_fires, &once_fires, &multiple_fires] {
        assert!(fires.windows(2).all(|w| w[0] < w[1]));
        assert!(fires.iter().all(|t| *t <= now));
    }
}

#[test]
fn catchup_time_window_policy_filters_by_age() {
    let now = Utc::now();
    let scheduler = BeatScheduler::new();

    // Ten intervals behind, but only misses within the last 3 minutes replay.
    let task = catchup_task(
        "windowed",
        CatchupPolicy::TimeWindow {
            window_seconds: 180,
        },
        10,
    );
    let fires = scheduler.planned_fires(&task, now).expect("fires");

    assert!(fires.len() > 1, "some misses should replay");
    let cutoff = now - Duration::seconds(180);
    assert!(
        fires.iter().all(|t| *t >= cutoff),
        "no fire may be older than the configured window"
    );
}

/// The per-tick rate cap is what keeps a replaying policy from bursting an
/// unbounded number of dispatches after a long outage.
#[test]
fn catchup_fires_are_rate_capped_per_tick() {
    let now = Utc::now();
    let mut scheduler = BeatScheduler::new();
    scheduler.with_max_catchup_fires_per_tick(5);

    let task = catchup_task(
        "flood",
        CatchupPolicy::RunMultiple {
            max_catchup: 10_000,
        },
        600,
    );
    let fires = scheduler.planned_fires(&task, now).expect("fires");

    assert_eq!(fires.len(), 5);
    // The cap keeps the most recent occurrences, not the oldest.
    assert!(fires.iter().all(|t| *t > now - Duration::minutes(10)));
}

/// A tick returns one entry per fire, so a caller driving dispatch from it
/// actually performs the catch-up runs.
#[tokio::test]
async fn tick_returns_one_entry_per_catchup_fire() {
    let mut scheduler = BeatScheduler::new();
    scheduler
        .add_task(catchup_task(
            "replay",
            CatchupPolicy::RunMultiple { max_catchup: 3 },
            5,
        ))
        .expect("add task");

    let dispatched = scheduler.tick().await.expect("tick");
    assert_eq!(dispatched.len(), 4, "3 replays + the current fire");
    assert!(dispatched.iter().all(|name| name == "replay"));

    let task = scheduler.get_task("replay").expect("task");
    assert_eq!(task.total_run_count, 4);
    // `last_run_at` must land on the schedule grid, not on the wall clock.
    assert!(task.last_run_at.expect("last run") <= Utc::now());
}

#[test]
fn history_catchup_policy_maps_onto_the_occurrence_policy() {
    assert_eq!(
        CatchupPolicy::Skip.occurrence_policy(),
        OccurrenceCatchupPolicy::Skip
    );
    assert_eq!(
        CatchupPolicy::RunOnce.occurrence_policy(),
        OccurrenceCatchupPolicy::FireLatestOnly
    );
    assert_eq!(
        CatchupPolicy::RunMultiple { max_catchup: 3 }.occurrence_policy(),
        OccurrenceCatchupPolicy::FireAll
    );
    assert_eq!(
        CatchupPolicy::TimeWindow { window_seconds: 5 }.occurrence_policy(),
        OccurrenceCatchupPolicy::FireAll
    );
}

/// A task that is not yet due produces no fires at all.
#[test]
fn planned_fires_is_empty_for_a_task_that_is_not_due() {
    let scheduler = BeatScheduler::new();
    let task = ScheduledTask::new("fresh".to_string(), Schedule::interval(3600));
    assert!(scheduler
        .planned_fires(&task, Utc::now())
        .expect("fires")
        .is_empty());
}

// ---------------------------------------------------------------------------
// State persistence durability
// ---------------------------------------------------------------------------

/// Regression: `save_state` used `std::fs::write`, which truncates the target
/// before writing — a crash mid-write left a partial file and the operator lost
/// every registered schedule.
#[test]
fn state_file_is_written_atomically_with_a_backup() {
    let path = temp_state_path("atomic");
    let mut scheduler = BeatScheduler::with_persistence(&path);
    scheduler
        .add_task(ScheduledTask::new(
            "one".to_string(),
            Schedule::interval(60),
        ))
        .expect("add task");

    // First save: no previous file to rotate.
    scheduler.save_state().expect("first save");
    let backup = std::path::PathBuf::from(format!("{}.bak", path.display()));
    assert!(path.exists());

    scheduler
        .add_task(ScheduledTask::new(
            "two".to_string(),
            Schedule::interval(60),
        ))
        .expect("add task");
    scheduler.save_state().expect("second save");

    // The previous good copy is retained, and no temp file is left behind.
    assert!(backup.exists(), "previous state should be rotated to .bak");
    let leftovers: Vec<_> = std::fs::read_dir(std::env::temp_dir())
        .expect("read temp dir")
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().into_owned();
            name.contains("celers-beat-atomic") && name.ends_with(".tmp")
        })
        .collect();
    assert!(
        leftovers.is_empty(),
        "temp file not cleaned up: {leftovers:?}"
    );

    let reloaded = BeatScheduler::load_from_file(&path).expect("reload");
    assert_eq!(reloaded.list_tasks().len(), 2);
    assert_eq!(scheduler.persistence_error_count(), 0);

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&backup);
}

/// Regression: a corrupt primary made `load_from_file` fail hard and the
/// operator lost every schedule; and a primary missing between the two renames
/// silently produced an empty scheduler.
#[test]
fn load_recovers_from_the_backup_when_the_primary_is_unusable() {
    let path = temp_state_path("recover");
    let backup = std::path::PathBuf::from(format!("{}.bak", path.display()));

    let mut scheduler = BeatScheduler::with_persistence(&path);
    scheduler
        .add_task(ScheduledTask::new(
            "kept".to_string(),
            Schedule::interval(60),
        ))
        .expect("add task");
    scheduler.save_state().expect("save 1");
    scheduler.save_state().expect("save 2 (rotates a backup)");
    assert!(backup.exists());

    // Corrupt the primary the way a truncated write would.
    std::fs::write(&path, b"{\"tasks\": {\"kept\":").expect("corrupt primary");
    let recovered = BeatScheduler::load_from_file(&path).expect("recover from backup");
    assert!(recovered.get_task("kept").is_some());

    // And with the primary missing entirely (crash between the two renames).
    std::fs::remove_file(&path).expect("remove primary");
    let recovered = BeatScheduler::load_from_file(&path).expect("recover from backup");
    assert!(recovered.get_task("kept").is_some());

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&backup);
}

/// Regression: a permanently failing write was swallowed by `let _ =`, so beat
/// ran indefinitely with no persistence and no log line.
#[tokio::test]
async fn persistence_failures_are_counted() {
    // A path whose parent does not exist can never be written.
    let bad_path = std::env::temp_dir()
        .join(format!("celers-beat-missing-{}", uuid::Uuid::new_v4()))
        .join("state.json");

    let mut scheduler = BeatScheduler::with_persistence(&bad_path);
    assert!(scheduler
        .add_task(ScheduledTask::new(
            "task".to_string(),
            Schedule::interval(60)
        ))
        .is_err());
    assert!(scheduler.persistence_error_count() >= 1);

    let before = scheduler.persistence_error_count();
    assert!(scheduler.save_state_async().await.is_err());
    assert!(scheduler.persistence_error_count() > before);
}

/// The async save keeps the blocking file I/O off the runtime worker thread.
#[tokio::test]
async fn save_state_async_round_trips() {
    let path = temp_state_path("async");
    let mut scheduler = BeatScheduler::with_persistence(&path);
    scheduler
        .add_task(ScheduledTask::new(
            "async_task".to_string(),
            Schedule::interval(60),
        ))
        .expect("add task");

    scheduler.save_state_async().await.expect("async save");
    let reloaded = BeatScheduler::load_from_file(&path).expect("reload");
    assert!(reloaded.get_task("async_task").is_some());

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.bak", path.display()));
}

// ---------------------------------------------------------------------------
// Jitter stability
// ---------------------------------------------------------------------------

/// Regression: the jitter actually used by the scheduler hashed with
/// `DefaultHasher`, whose output is not stable across Rust releases or
/// platforms — so two instances computed different fire instants for the same
/// entry and never contended for the same dispatch lock.
#[test]
fn jitter_apply_uses_the_stable_hash() {
    let fire = at(2026, 6, 13, 12, 0);
    for window in [1i64, 5, 30, 600] {
        let expected = bounded_jitter_offset("entry", fire, window as u64);
        let applied = Jitter::symmetric(window).apply(fire, "entry");
        assert_eq!(
            (applied - fire).num_seconds(),
            expected,
            "Jitter::apply must agree with the crate's stable jitter primitive"
        );
    }
}

/// Regression: the offset range was `hash % (max - min) + min`, i.e.
/// `[min, max - 1]` — the configured maximum was unreachable.
#[test]
fn jitter_range_is_inclusive_of_max() {
    let jitter = Jitter::new(0, 1);
    let mut seen_max = false;
    let mut seen_min = false;
    for i in 0..200 {
        let fire = at(2026, 6, 13, 0, 0) + Duration::seconds(i);
        match (jitter.apply(fire, "coverage") - fire).num_seconds() {
            0 => seen_min = true,
            1 => seen_max = true,
            other => panic!("offset {other} outside [0, 1]"),
        }
    }
    assert!(seen_min && seen_max, "both bounds must be reachable");
}

/// Regression: `Jitter::new(100, 0)` computed `(max - min) as u64`, wrapping to
/// ~1.8e19 and producing an enormous offset that overflowed the datetime add.
#[test]
fn inverted_jitter_range_is_normalised_not_wrapped() {
    let jitter = Jitter::new(100, 0);
    assert!(jitter.is_valid());
    assert_eq!((jitter.min_seconds, jitter.max_seconds), (0, 100));

    let fire = at(2026, 6, 13, 12, 0);
    let delta = (jitter.apply(fire, "inverted") - fire).num_seconds();
    assert!(
        (0..=100).contains(&delta),
        "offset {delta} outside [0, 100]"
    );
}

#[test]
fn jitter_never_panics_on_extreme_bounds() {
    let jitter = Jitter::new(i64::MIN, i64::MAX);
    // Must return an instant rather than overflowing.
    let fire = at(2026, 6, 13, 12, 0);
    let _ = jitter.apply(fire, "extreme");
}

// ---------------------------------------------------------------------------
// Calendars
// ---------------------------------------------------------------------------

/// Regression: `with_business_calendar` / `with_holiday_calendar` bound their
/// argument to `_calendar` and returned `self` unchanged, so a task configured
/// with a holiday calendar ran on Christmas Day exactly as if the call had
/// never been made.
#[test]
fn holiday_calendar_is_stored_and_honoured() {
    let mut holidays = HolidayCalendar::new();
    holidays.add_holiday(Holiday::new("Christmas Day", 2026, 12, 25));

    let mut task = ScheduledTask::new("payroll".to_string(), Schedule::interval(3600))
        .with_holiday_calendar(holidays);
    assert!(task.holiday_calendar().is_some());

    // Last run at 23:30 on Christmas Eve => raw next fire is 00:30 on the 25th,
    // which the calendar must push past the holiday.
    task.last_run_at = Some(at(2026, 12, 24, 23, 30));
    task.invalidate_next_run_cache();

    let next = task.next_run_time().expect("next run");
    assert_ne!(next.day(), 25, "fired on a configured holiday");
    assert_eq!((next.month(), next.day()), (12, 26));
}

#[test]
fn business_calendar_is_stored_and_honoured() {
    let calendar = BusinessCalendar::new(
        BusinessHours::new(9, 17),
        vec![
            DayOfWeek::Monday,
            DayOfWeek::Tuesday,
            DayOfWeek::Wednesday,
            DayOfWeek::Thursday,
            DayOfWeek::Friday,
        ],
    );

    let mut task = ScheduledTask::new("reconcile".to_string(), Schedule::interval(3600))
        .with_business_calendar(calendar);
    assert!(task.business_calendar().is_some());

    // 2026-06-13 is a Saturday: the raw next fire is out of hours and out of
    // working days, so it must be advanced into Monday business hours.
    task.last_run_at = Some(at(2026, 6, 13, 3, 0));
    task.invalidate_next_run_cache();

    let next = task.next_run_time().expect("next run");
    assert_eq!(next.weekday(), chrono::Weekday::Mon);
    assert!(
        (9..17).contains(&next.hour()),
        "fired outside business hours"
    );
}

// ---------------------------------------------------------------------------
// History bounds
// ---------------------------------------------------------------------------

/// Regression: `max_history_size` defaulted to 0 ("unlimited"), so a minutely
/// task accumulated hundreds of thousands of records, all re-serialized into
/// the state file on every save.
#[test]
fn execution_history_is_bounded_by_default() {
    let mut task = ScheduledTask::new("chatty".to_string(), Schedule::interval(60));
    assert_eq!(task.max_history_size, DEFAULT_MAX_HISTORY_SIZE);

    for _ in 0..(DEFAULT_MAX_HISTORY_SIZE * 3) {
        task.add_execution_record(ExecutionRecord::completed(
            Utc::now(),
            ExecutionResult::Success,
        ));
    }

    assert_eq!(task.execution_history.len(), DEFAULT_MAX_HISTORY_SIZE);
    // Unlimited retention remains available as an explicit opt-in.
    let unlimited =
        ScheduledTask::new("keep_all".to_string(), Schedule::interval(60)).with_max_history(0);
    assert_eq!(unlimited.max_history_size, 0);
}

/// Regression: `version_history` had no trim site at all, and each entry clones
/// the whole schedule + jitter + catch-up policy.
#[test]
fn version_history_is_bounded() {
    let mut task = ScheduledTask::new("versioned".to_string(), Schedule::interval(60));
    for i in 0..(MAX_VERSION_HISTORY * 2) {
        task.update_schedule(Schedule::interval(60 + i as u64), None);
    }
    assert_eq!(task.version_history.len(), MAX_VERSION_HISTORY);
    // The most recent versions are the ones retained.
    assert_eq!(
        task.version_history
            .last()
            .expect("at least one version")
            .version,
        task.current_version
    );
}

// ---------------------------------------------------------------------------
// Schedule evaluation error surfacing
// ---------------------------------------------------------------------------

/// Regression: `is_due().unwrap_or(false)` mapped any evaluation error to "not
/// due", so a task with a broken stored crontab was silently never scheduled —
/// no log, no metric, no alert.
#[cfg(feature = "cron")]
#[tokio::test]
async fn schedule_evaluation_errors_are_counted_and_alerted() {
    let mut scheduler = BeatScheduler::new();

    // A crontab that survives construction but not the cron parser.
    let broken = ScheduledTask::new(
        "broken".to_string(),
        Schedule::crontab("not-a-minute", "*", "*", "*", "*"),
    );
    scheduler.add_task(broken).expect("add task");

    assert_eq!(scheduler.schedule_eval_error_count(), 0);
    assert!(scheduler.get_due_tasks().is_empty());
    assert!(
        scheduler.schedule_eval_error_count() > 0,
        "evaluation failure must be counted, not swallowed"
    );

    // A tick raises an alert so the inert task is visible to operators.
    let dispatched = scheduler.tick().await.expect("tick");
    assert!(dispatched.is_empty());
    let alerts = scheduler.get_task_alerts("broken");
    assert!(
        !alerts.is_empty(),
        "a task that can never run should raise an alert"
    );
}

// ---------------------------------------------------------------------------
// Priority ordering
// ---------------------------------------------------------------------------

/// Regression: the tie-break branch recomputed `schedule.next_run` inside the
/// comparator (O(n log n) evaluations) and used `Utc::now()` as the error
/// fallback, which can order the same pair differently on two comparisons.
#[test]
fn due_tasks_by_priority_is_stable_and_correctly_ordered() {
    let mut scheduler = BeatScheduler::new();
    let created = Utc::now() - Duration::seconds(600);

    for (name, priority, every) in [
        ("low", 1u8, 60u64),
        ("high_late", 9, 120),
        ("high_early", 9, 30),
        ("mid", 5, 60),
    ] {
        let mut task = ScheduledTask::new(name.to_string(), Schedule::interval(every));
        task.options.priority = Some(priority);
        task.created_at = created;
        task.invalidate_next_run_cache();
        scheduler.add_task(task).expect("add task");
    }

    let order: Vec<String> = scheduler
        .get_due_tasks_by_priority()
        .iter()
        .map(|t| t.name.clone())
        .collect();

    // Highest priority first; within a priority, the earlier next-run first.
    assert_eq!(order[0], "high_early");
    assert_eq!(order[1], "high_late");
    assert_eq!(order[2], "mid");
    assert_eq!(order[3], "low");

    // Repeated calls agree (no clock read inside the comparator).
    for _ in 0..20 {
        let again: Vec<String> = scheduler
            .get_due_tasks_by_priority()
            .iter()
            .map(|t| t.name.clone())
            .collect();
        assert_eq!(again, order);
    }
}

// ---------------------------------------------------------------------------
// Cron compilation cache
// ---------------------------------------------------------------------------

/// The memoised cron compilation must not change any result: repeated and
/// interleaved evaluations of several expressions stay correct.
#[cfg(feature = "cron")]
#[test]
fn cached_cron_compilation_preserves_results() {
    let hourly = Schedule::crontab("0", "*", "*", "*", "*");
    let nightly = Schedule::crontab("30", "3", "*", "*", "*");
    let after = at(2026, 6, 13, 1, 15);

    let hourly_first = hourly.next_run(Some(after)).expect("hourly");
    let nightly_first = nightly.next_run(Some(after)).expect("nightly");
    assert_eq!(hourly_first, at(2026, 6, 13, 2, 0));
    assert_eq!(nightly_first, at(2026, 6, 13, 3, 30));

    for _ in 0..200 {
        assert_eq!(hourly.next_run(Some(after)).expect("hourly"), hourly_first);
        assert_eq!(
            nightly.next_run(Some(after)).expect("nightly"),
            nightly_first
        );
    }

    // An invalid expression must still be reported as an error, not cached as
    // a success from a neighbouring expression.
    let broken = Schedule::crontab("bogus", "*", "*", "*", "*");
    assert!(broken.next_run(Some(after)).is_err());
}

// ---------------------------------------------------------------------------
// Startup fires and completed one-time tasks
// ---------------------------------------------------------------------------

/// The `run_on_startup` opt-in must still produce a *deterministic* fire
/// instant, otherwise its dispatch-lock key differs on every evaluation and two
/// instances both dispatch the startup run.
#[tokio::test]
async fn run_on_startup_fire_is_deterministic_and_deduped() {
    let scheduler = BeatScheduler::new();
    // Freshly registered: the schedule has not produced an occurrence yet.
    let created = Utc::now();
    let mut task = ScheduledTask::new("startup".to_string(), Schedule::interval(3600))
        .with_run_on_startup(true);
    task.created_at = created;

    let first = scheduler.planned_fires(&task, Utc::now()).expect("fires");
    let second = scheduler.planned_fires(&task, Utc::now()).expect("fires");
    assert_eq!(first, vec![created]);
    assert_eq!(first, second);

    // And two instances contend for the same key.
    let backend = Arc::new(InMemoryLockBackend::new());
    let build = || {
        let mut scheduler = BeatScheduler::new();
        scheduler.with_lock_backend(backend.clone());
        let mut task = ScheduledTask::new("startup".to_string(), Schedule::interval(3600))
            .with_run_on_startup(true);
        task.created_at = created;
        scheduler.add_task(task).expect("add task");
        scheduler
    };

    let mut a = build();
    let mut b = build();
    let da = a
        .tick_with_locks(DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("tick a");
    let db = b
        .tick_with_locks(DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("tick b");
    assert_eq!(da.len() + db.len(), 1);
}

/// Regression/compat: a state file written before `created_at` existed has no
/// such key in its JSON. `#[serde(default = "Utc::now")]` fills it in at load
/// time, so a restored never-run task is evaluated from "now" rather than from
/// its original registration instant. That still means it is *not* due on
/// load: an `Interval { every: 86400 }` task waits a full interval past load
/// time before its next fire, where a pre-`created_at` version of this crate
/// would have fired it immediately. `run_on_startup: true` opts a task back
/// into that immediate-fire behaviour.
#[test]
fn legacy_state_file_without_created_at_does_not_fire_immediately() {
    // No `created_at` (and no `run_on_startup`, `last_run_at`) key at all -
    // simulates a state file written by a pre-`created_at` version of this
    // crate.
    let legacy_json = r#"{
        "name": "daily_report",
        "schedule": { "type": "Interval", "every": 86400 }
    }"#;

    let before_load = Utc::now();
    let task: ScheduledTask =
        serde_json::from_str(legacy_json).expect("legacy state must still deserialize");
    let after_load = Utc::now();

    assert!(
        task.created_at >= before_load && task.created_at <= after_load,
        "a missing `created_at` must default to load time, got {:?} outside [{before_load:?}, {after_load:?}]",
        task.created_at
    );
    assert!(task.last_run_at.is_none());
    assert!(
        !task.run_on_startup,
        "run_on_startup must also default to false for legacy state"
    );

    // Not due immediately: the first occurrence is a full interval after
    // `created_at`, not "now" - unlike the old (pre-`created_at`) behaviour.
    assert!(
        !task.is_due().expect("interval schedule always evaluates"),
        "a restored never-run task must wait for its schedule-derived occurrence, \
         not fire on load"
    );
    let next = task.next_run_time().expect("next run");
    assert_eq!((next - task.created_at).num_seconds(), 86400);

    // The opt-in restores the pre-`created_at` "due on load" behaviour.
    let mut startup_task: ScheduledTask =
        serde_json::from_str(legacy_json).expect("legacy state must still deserialize");
    startup_task.run_on_startup = true;
    assert!(
        startup_task
            .is_due()
            .expect("interval schedule always evaluates"),
        "run_on_startup=true must restore immediate-fire behaviour for a never-run task"
    );
}

/// A one-time schedule that has already fired is complete, not broken: it must
/// not be reported as a schedule evaluation failure on every subsequent tick.
#[tokio::test]
async fn completed_onetime_task_is_not_an_evaluation_error() {
    let mut scheduler = BeatScheduler::new();
    let mut task = ScheduledTask::new(
        "one_shot".to_string(),
        Schedule::onetime(Utc::now() - Duration::minutes(10)),
    );
    task.last_run_at = Some(Utc::now() - Duration::minutes(9));
    task.invalidate_next_run_cache();
    scheduler.add_task(task).expect("add task");

    for _ in 0..3 {
        let dispatched = scheduler.tick().await.expect("tick");
        assert!(dispatched.is_empty());
    }
    assert_eq!(
        scheduler.schedule_eval_error_count(),
        0,
        "a completed one-time task must not be counted as broken"
    );
    assert!(scheduler.get_task_alerts("one_shot").is_empty());
}

/// Each catch-up fire takes its own dispatch lock, so replays are individually
/// deduplicated across instances rather than collapsing onto one key.
#[tokio::test]
async fn catchup_fires_take_distinct_dispatch_locks() {
    let backend = Arc::new(InMemoryLockBackend::new());
    let last_run = Utc::now() - Duration::minutes(4);

    let build = || {
        let mut scheduler = BeatScheduler::new();
        scheduler.with_lock_backend(backend.clone());
        let mut task = ScheduledTask::new("replayed".to_string(), Schedule::interval(60))
            .with_catchup_policy(CatchupPolicy::RunMultiple { max_catchup: 5 });
        task.last_run_at = Some(last_run);
        task.invalidate_next_run_cache();
        scheduler.add_task(task).expect("add task");
        scheduler
    };

    let mut a = build();
    let mut b = build();

    let dispatched_a = a
        .tick_with_locks(DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("tick a");
    let dispatched_b = b
        .tick_with_locks(DEFAULT_DISPATCH_LOCK_TTL_SECS)
        .await
        .expect("tick b");

    // The first instance claims every missed occurrence; the second finds them
    // all taken and dispatches nothing.
    assert!(
        dispatched_a.len() >= 3,
        "expected replays, got {dispatched_a:?}"
    );
    assert!(
        dispatched_b.is_empty(),
        "second instance duplicated catch-up fires: {dispatched_b:?}"
    );
}

// ---------------------------------------------------------------------------
// Jitter must smear the dispatch moment, never re-anchor the schedule grid
// ---------------------------------------------------------------------------

/// A fire records the *schedule-grid occurrence*, not the jittered dispatch
/// moment. Recording the jittered instant would re-anchor the grid on every
/// fire, turning a bounded ±window into accumulating interval drift.
#[test]
fn jitter_does_not_drift_the_interval_grid() {
    let scheduler = BeatScheduler::new();
    let base = Utc::now() - Duration::seconds(600);

    // The window has to be *strictly* narrower than half the interval, and the
    // sampling instant has to match it. With the ±30 s window this used to
    // pair with a 60 s interval, occurrence k's latest dispatch (k*60+30) is
    // exactly occurrence k+1's earliest (k+1)*60-30 — so at `now = base+60k+30`
    // the *next* occurrence could also be eligible, `fires.last()` returned it,
    // and the walk skipped a step and desynchronised (observed as an
    // intermittent "no fire at step N"). At ±20 s the windows are disjoint:
    // occurrence k is always eligible at base+60k+20, and occurrence k+1 never
    // is (its earliest dispatch is base+60k+40). The property under test — a
    // fire records the grid occurrence, never the jittered instant — is
    // unchanged.
    let mut task = ScheduledTask::new("smeared".to_string(), Schedule::interval(60))
        .with_jitter(Jitter::symmetric(20));
    task.last_run_at = Some(base);
    task.invalidate_next_run_cache();

    // Walk ten fires, always recording what `planned_fires` hands back.
    let mut recorded = Vec::new();
    for step in 1..=10 {
        let now = base + Duration::seconds(60 * step + 20);
        let fires = scheduler.planned_fires(&task, now).expect("fires");
        assert_eq!(
            fires.len(),
            1,
            "exactly one occurrence is eligible per step; got {fires:?} at step {step}"
        );
        let occurrence = *fires.last().expect("at least one fire");
        task.mark_run_at(occurrence);
        recorded.push(occurrence);
    }

    // Every recorded occurrence sits exactly on the 60 s grid anchored at base.
    for (i, occurrence) in recorded.iter().enumerate() {
        let expected = base + Duration::seconds(60 * (i as i64 + 1));
        assert_eq!(
            *occurrence, expected,
            "fire {i} drifted off the grid (jitter must not re-anchor it)"
        );
    }
}

/// Regression shape for the cron case: with a negative jitter offset, recording
/// the jittered instant put `last_run_at` *before* the slot, so the very same
/// slot was recomputed and re-fired on every subsequent tick.
#[cfg(feature = "cron")]
#[test]
fn jittered_cron_slot_fires_exactly_once() {
    let scheduler = BeatScheduler::new();
    // Daily at 03:00, with a wide symmetric window so the offset is very
    // unlikely to be exactly zero.
    let mut task = ScheduledTask::new(
        "nightly".to_string(),
        Schedule::crontab("0", "3", "*", "*", "*"),
    )
    .with_jitter(Jitter::symmetric(600));
    task.last_run_at = Some(at(2026, 6, 12, 3, 0));
    task.invalidate_next_run_cache();

    // Well past the 2026-06-13 03:00 slot, but before the next day's.
    let now = at(2026, 6, 13, 12, 0);

    let fires = scheduler.planned_fires(&task, now).expect("fires");
    assert_eq!(fires, vec![at(2026, 6, 13, 3, 0)]);
    task.mark_run_at(*fires.last().expect("one fire"));

    // The same slot must not come back on the following ticks.
    for _ in 0..5 {
        assert!(
            scheduler
                .planned_fires(&task, now)
                .expect("fires")
                .is_empty(),
            "a jittered cron slot re-fired after being recorded"
        );
    }

    // The next slot is the following day.
    let tomorrow = at(2026, 6, 14, 12, 0);
    assert_eq!(
        scheduler.planned_fires(&task, tomorrow).expect("fires"),
        vec![at(2026, 6, 14, 3, 0)]
    );
}

/// A negative jitter offset must be able to fire an occurrence *early*; the
/// occurrence enumeration has to reach ahead by the early half of the window or
/// the task is silently skipped for that tick.
#[test]
fn negative_jitter_can_fire_before_the_grid_instant() {
    let scheduler = BeatScheduler::new();

    // `Jitter::new(-60, -60)` is a fixed −60 s offset: deterministic and always
    // early, with no reliance on which offset the hash happens to pick.
    let base = Utc::now() - Duration::seconds(300);
    let mut task = ScheduledTask::new("early".to_string(), Schedule::interval(120))
        .with_jitter(Jitter::new(-60, -60));
    task.last_run_at = Some(base);
    task.invalidate_next_run_cache();

    // 90 s in: the grid occurrence (base + 120) has not arrived, but its
    // dispatch moment (base + 60) has.
    let now = base + Duration::seconds(90);
    let fires = scheduler.planned_fires(&task, now).expect("fires");
    assert_eq!(
        fires,
        vec![base + Duration::seconds(120)],
        "an early-jittered occurrence must fire, and must be recorded on the grid"
    );

    // Consistent with `is_due`/`next_run_time`, which report the (early)
    // dispatch moment rather than the grid occurrence.
    assert_eq!(
        task.next_run_time().expect("next run"),
        base + Duration::seconds(60)
    );
    assert!(task.is_due().expect("is_due"));
}

// ---------------------------------------------------------------------------
// BusinessHours round-trip
// ---------------------------------------------------------------------------

/// Anything `BusinessHours::new` accepts must survive a state-file round trip:
/// a scheduler that writes a file it cannot read back is worse than one that
/// rejects the value up front.
#[test]
fn business_hours_new_values_round_trip_through_the_state_file() {
    // Inverted window: inert, but representable — and `new` accepts it.
    let hours = BusinessHours::new(17, 9);
    assert!(!hours.is_valid());

    let json = serde_json::to_string(&hours).expect("serialize");
    let parsed: BusinessHours = serde_json::from_str(&json).expect("inverted hours must reload");
    assert_eq!((parsed.start_hour, parsed.end_hour), (17, 9));

    // ... and through the full scheduler state file.
    let path = temp_state_path("bizhours");
    let mut scheduler = BeatScheduler::with_persistence(&path);
    let calendar = BusinessCalendar::new(BusinessHours::new(17, 9), vec![DayOfWeek::Monday]);
    scheduler
        .add_task(
            ScheduledTask::new("inverted".to_string(), Schedule::interval(60))
                .with_business_calendar(calendar),
        )
        .expect("add task");
    scheduler.save_state().expect("save");

    let reloaded = BeatScheduler::load_from_file(&path).expect("state file must reload");
    assert!(reloaded
        .get_task("inverted")
        .expect("task")
        .business_calendar()
        .is_some());

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(format!("{}.bak", path.display()));
}

// ---------------------------------------------------------------------------
// Leader lease recovery
// ---------------------------------------------------------------------------

/// A leader that self-demoted because its lease slipped inside the safety
/// margin — while still holding the backend lock — must renew and resume, not
/// idle until the lock expires and then wait out `failover_timeout`.
#[tokio::test]
async fn stalled_leader_reclaims_its_own_still_held_lock() {
    use crate::heartbeat::{BeatHeartbeat, BeatRole, HeartbeatConfig};

    let backend = Arc::new(InMemoryLockBackend::new());
    let hb = BeatHeartbeat::new(
        "instance-1".to_string(),
        backend.clone(),
        HeartbeatConfig::new(),
    );

    assert!(hb.try_become_leader().await.is_ok_and(|v| v));

    // Simulate a stall that outlived the lease while the backend lock (30 s
    // TTL) is still held by us.
    hb.set_lease_expires_at(Some(Utc::now() - Duration::minutes(1)))
        .await;
    assert!(!hb.is_leader().await);
    assert_eq!(hb.cached_role().await, BeatRole::Standby);
    assert!(hb.check_leader_health().await.is_ok_and(|v| v));

    // But the backend lock is still owned by us, so a tick must reclaim it
    // rather than starting a failover countdown.
    hb.tick().await.expect("tick");
    assert_eq!(hb.cached_role().await, BeatRole::Leader);
    assert!(hb.leader_missing_since().await.is_none());
}
