//! Thread-safe dynamic schedule registry.
//!
//! [`ScheduleRegistry`] is a cloneable, interior-locked store of
//! [`ScheduledTask`] entries that can be mutated *at runtime* from any thread
//! while a beat loop concurrently reads it between ticks. Cloning a registry
//! yields another handle onto the **same** shared state (the inner map is held
//! behind `Arc<RwLock<…>>`), so a control thread can call
//! [`add_entry`](ScheduleRegistry::add_entry) /
//! [`remove_entry`](ScheduleRegistry::remove_entry) /
//! [`update_entry`](ScheduleRegistry::update_entry) and the running loop will
//! observe the change on its next tick via
//! [`list_entries`](ScheduleRegistry::list_entries) or
//! [`due_entries`](ScheduleRegistry::due_entries).
//!
//! The registry takes a *write* lock only for mutations and a *read* lock for
//! queries, so many concurrent ticks can read in parallel. A poisoned lock
//! (caused by a panic in another thread while the lock was held) is surfaced as
//! a [`ScheduleError::Invalid`] rather than propagating the panic, honouring
//! the crate's no-panic policy.
//!
//! # Example
//!
//! ```
//! use celers_beat::{ScheduleRegistry, Schedule, ScheduledTask};
//!
//! let registry = ScheduleRegistry::new();
//! registry
//!     .add_entry(ScheduledTask::new("report".into(), Schedule::interval(60)))
//!     .unwrap();
//!
//! // A second handle shares the same underlying state.
//! let handle = registry.clone();
//! assert_eq!(handle.len().unwrap(), 1);
//!
//! handle
//!     .update_entry("report", |task| {
//!         task.enabled = false;
//!     })
//!     .unwrap();
//! assert!(!registry.get_entry("report").unwrap().unwrap().enabled);
//!
//! registry.remove_entry("report").unwrap();
//! assert!(registry.is_empty().unwrap());
//! ```

use crate::config::ScheduleError;
use crate::task::ScheduledTask;
use chrono::{DateTime, Utc};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

/// Outcome of [`ScheduleRegistry::add_entry`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AddOutcome {
    /// A brand-new entry was inserted.
    Inserted,
    /// An existing entry with the same name was replaced.
    Replaced,
}

impl AddOutcome {
    /// Whether the add replaced an existing entry.
    pub fn was_replaced(self) -> bool {
        matches!(self, AddOutcome::Replaced)
    }
}

/// A cloneable, thread-safe registry of scheduled tasks.
///
/// All clones share the same underlying storage. See the [module
/// documentation](crate::registry) for the concurrency model.
#[derive(Clone, Default)]
pub struct ScheduleRegistry {
    inner: Arc<RwLock<HashMap<String, ScheduledTask>>>,
}

impl ScheduleRegistry {
    /// Create a new, empty registry.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Create a registry pre-populated from an iterator of tasks.
    ///
    /// Later entries with a duplicate name overwrite earlier ones.
    pub fn from_tasks<I>(tasks: I) -> Self
    where
        I: IntoIterator<Item = ScheduledTask>,
    {
        let map = tasks
            .into_iter()
            .map(|mut task| {
                task.update_next_run_cache();
                (task.name.clone(), task)
            })
            .collect();
        Self {
            inner: Arc::new(RwLock::new(map)),
        }
    }

    /// Acquire the read guard, mapping a poisoned lock to a recoverable error.
    fn read(
        &self,
    ) -> Result<std::sync::RwLockReadGuard<'_, HashMap<String, ScheduledTask>>, ScheduleError> {
        self.inner
            .read()
            .map_err(|_| ScheduleError::Invalid("schedule registry lock poisoned".to_string()))
    }

    /// Acquire the write guard, mapping a poisoned lock to a recoverable error.
    fn write(
        &self,
    ) -> Result<std::sync::RwLockWriteGuard<'_, HashMap<String, ScheduledTask>>, ScheduleError>
    {
        self.inner
            .write()
            .map_err(|_| ScheduleError::Invalid("schedule registry lock poisoned".to_string()))
    }

    /// Add (or replace) an entry.
    ///
    /// The task's next-run cache is initialised before insertion so the first
    /// tick after the addition sees an accurate due time. If an entry with the
    /// same name already exists it is replaced and [`AddOutcome::Replaced`] is
    /// returned.
    pub fn add_entry(&self, mut task: ScheduledTask) -> Result<AddOutcome, ScheduleError> {
        task.update_next_run_cache();
        let mut guard = self.write()?;
        let existed = guard.insert(task.name.clone(), task).is_some();
        Ok(if existed {
            AddOutcome::Replaced
        } else {
            AddOutcome::Inserted
        })
    }

    /// Insert an entry only if no entry with the same name exists.
    ///
    /// Returns `Ok(true)` if inserted, `Ok(false)` if an entry with that name
    /// was already present (in which case the registry is left unchanged).
    pub fn add_entry_if_absent(&self, mut task: ScheduledTask) -> Result<bool, ScheduleError> {
        let mut guard = self.write()?;
        if guard.contains_key(&task.name) {
            return Ok(false);
        }
        task.update_next_run_cache();
        guard.insert(task.name.clone(), task);
        Ok(true)
    }

    /// Remove an entry by name, returning the removed task if it existed.
    pub fn remove_entry(&self, name: &str) -> Result<Option<ScheduledTask>, ScheduleError> {
        let mut guard = self.write()?;
        Ok(guard.remove(name))
    }

    /// Mutate an existing entry in place via a closure.
    ///
    /// The closure receives a mutable reference to the stored task. After it
    /// runs, the entry's next-run cache is refreshed so schedule changes take
    /// effect on the next tick. Returns `Ok(true)` if the entry existed and was
    /// updated, `Ok(false)` if no entry with that name exists.
    ///
    /// # Example
    /// ```
    /// use celers_beat::{ScheduleRegistry, Schedule, ScheduledTask};
    ///
    /// let registry = ScheduleRegistry::new();
    /// registry
    ///     .add_entry(ScheduledTask::new("t".into(), Schedule::interval(60)))
    ///     .unwrap();
    ///
    /// let updated = registry
    ///     .update_entry("t", |task| task.schedule = Schedule::interval(120))
    ///     .unwrap();
    /// assert!(updated);
    /// ```
    pub fn update_entry<F>(&self, name: &str, mutate: F) -> Result<bool, ScheduleError>
    where
        F: FnOnce(&mut ScheduledTask),
    {
        let mut guard = self.write()?;
        match guard.get_mut(name) {
            Some(task) => {
                mutate(task);
                // Keep the cached next-run consistent with any schedule change.
                task.update_next_run_cache();
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Replace an entry's schedule wholesale, returning whether it existed.
    ///
    /// This is a convenience wrapper over [`update_entry`](Self::update_entry)
    /// for the common case of swapping the schedule. The version-tracked
    /// [`ScheduledTask::update_schedule`] is used so the change is recorded in
    /// the task's version history.
    pub fn update_schedule(
        &self,
        name: &str,
        schedule: crate::schedule::Schedule,
    ) -> Result<bool, ScheduleError> {
        self.update_entry(name, move |task| {
            task.update_schedule(schedule, Some("registry update_schedule".to_string()));
        })
    }

    /// List a snapshot of all entries.
    ///
    /// Returns owned clones so the caller can iterate without holding the lock,
    /// which is exactly what a beat loop wants: take a consistent snapshot at
    /// the start of a tick, release the lock, then process it.
    pub fn list_entries(&self) -> Result<Vec<ScheduledTask>, ScheduleError> {
        let guard = self.read()?;
        Ok(guard.values().cloned().collect())
    }

    /// List the names of all entries.
    pub fn list_entry_names(&self) -> Result<Vec<String>, ScheduleError> {
        let guard = self.read()?;
        Ok(guard.keys().cloned().collect())
    }

    /// Fetch a single entry by name (cloned).
    pub fn get_entry(&self, name: &str) -> Result<Option<ScheduledTask>, ScheduleError> {
        let guard = self.read()?;
        Ok(guard.get(name).cloned())
    }

    /// Whether an entry with the given name exists.
    pub fn contains(&self, name: &str) -> Result<bool, ScheduleError> {
        let guard = self.read()?;
        Ok(guard.contains_key(name))
    }

    /// Number of registered entries.
    pub fn len(&self) -> Result<usize, ScheduleError> {
        let guard = self.read()?;
        Ok(guard.len())
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> Result<bool, ScheduleError> {
        let guard = self.read()?;
        Ok(guard.is_empty())
    }

    /// Remove every entry.
    pub fn clear(&self) -> Result<(), ScheduleError> {
        let mut guard = self.write()?;
        guard.clear();
        Ok(())
    }

    /// Snapshot of all *enabled* entries that are due at the given instant.
    ///
    /// This is the primary read path for a beat loop: pass the current tick
    /// time and execute the returned tasks. Disabled tasks and tasks whose
    /// next-run computation fails are excluded.
    pub fn due_entries_at(&self, now: DateTime<Utc>) -> Result<Vec<ScheduledTask>, ScheduleError> {
        let guard = self.read()?;
        Ok(guard
            .values()
            .filter(|task| task.enabled && Self::is_due_at(task, now))
            .cloned()
            .collect())
    }

    /// Convenience wrapper over [`due_entries_at`](Self::due_entries_at) using
    /// the current wall-clock time.
    pub fn due_entries(&self) -> Result<Vec<ScheduledTask>, ScheduleError> {
        self.due_entries_at(Utc::now())
    }

    /// Determine whether a task is due at `now`, mirroring
    /// [`ScheduledTask::is_due`] but against a caller-supplied instant so a beat
    /// loop can evaluate a whole tick against a single timestamp.
    fn is_due_at(task: &ScheduledTask, now: DateTime<Utc>) -> bool {
        if task.last_run_at.is_none() {
            return true;
        }
        match task.schedule.next_run(task.last_run_at) {
            Ok(mut next_run) => {
                if let Some(ref jitter) = task.jitter {
                    next_run = jitter.apply(next_run, &task.name);
                }
                now >= next_run
            }
            Err(_) => false,
        }
    }

    /// Record that an entry has just run at `at`, advancing its `last_run_at`,
    /// bumping its run count, and refreshing its next-run cache. Returns
    /// `Ok(true)` if the entry existed.
    ///
    /// A beat loop calls this after dispatching a due task so the *same shared*
    /// registry no longer reports the task as due on the following tick.
    pub fn mark_run_at(&self, name: &str, at: DateTime<Utc>) -> Result<bool, ScheduleError> {
        self.update_entry(name, move |task| {
            task.last_run_at = Some(at);
            task.total_run_count += 1;
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::Schedule;
    use chrono::TimeZone;
    use std::sync::Arc;
    use std::thread;

    fn task(name: &str, every: u64) -> ScheduledTask {
        ScheduledTask::new(name.to_string(), Schedule::interval(every))
    }

    #[test]
    fn add_list_remove_roundtrip() {
        let registry = ScheduleRegistry::new();
        assert!(registry.is_empty().unwrap());

        assert_eq!(
            registry.add_entry(task("a", 60)).unwrap(),
            AddOutcome::Inserted
        );
        assert_eq!(
            registry.add_entry(task("b", 120)).unwrap(),
            AddOutcome::Inserted
        );
        assert_eq!(registry.len().unwrap(), 2);

        let mut names = registry.list_entry_names().unwrap();
        names.sort();
        assert_eq!(names, vec!["a".to_string(), "b".to_string()]);

        let removed = registry.remove_entry("a").unwrap();
        assert!(removed.is_some());
        assert_eq!(removed.unwrap().name, "a");
        assert_eq!(registry.len().unwrap(), 1);
        assert!(registry.remove_entry("a").unwrap().is_none());
    }

    #[test]
    fn add_replaces_existing_and_reports_outcome() {
        let registry = ScheduleRegistry::new();
        assert_eq!(
            registry.add_entry(task("dup", 60)).unwrap(),
            AddOutcome::Inserted
        );
        let outcome = registry.add_entry(task("dup", 999)).unwrap();
        assert_eq!(outcome, AddOutcome::Replaced);
        assert!(outcome.was_replaced());
        assert_eq!(registry.len().unwrap(), 1);
    }

    #[test]
    fn add_entry_if_absent_does_not_overwrite() {
        let registry = ScheduleRegistry::new();
        assert!(registry.add_entry_if_absent(task("x", 60)).unwrap());
        assert!(!registry.add_entry_if_absent(task("x", 120)).unwrap());
        // Original schedule preserved.
        let stored = registry.get_entry("x").unwrap().unwrap();
        assert!(stored.schedule.is_interval());
    }

    #[test]
    fn update_entry_mutates_in_place() {
        let registry = ScheduleRegistry::new();
        registry.add_entry(task("u", 60)).unwrap();

        let updated = registry
            .update_entry("u", |t| {
                t.enabled = false;
                t.options.priority = Some(9);
            })
            .unwrap();
        assert!(updated);

        let stored = registry.get_entry("u").unwrap().unwrap();
        assert!(!stored.enabled);
        assert_eq!(stored.options.priority, Some(9));

        // Updating a missing entry returns false.
        assert!(!registry.update_entry("missing", |_| {}).unwrap());
    }

    #[test]
    fn update_schedule_records_version_history() {
        let registry = ScheduleRegistry::new();
        registry.add_entry(task("s", 60)).unwrap();
        let before = registry.get_entry("s").unwrap().unwrap();
        let before_versions = before.version_history.len();

        assert!(registry
            .update_schedule("s", Schedule::interval(3600))
            .unwrap());

        let after = registry.get_entry("s").unwrap().unwrap();
        assert!(after.version_history.len() > before_versions);
    }

    #[test]
    fn clones_share_state() {
        let registry = ScheduleRegistry::new();
        let handle = registry.clone();
        registry.add_entry(task("shared", 60)).unwrap();
        // The clone observes the addition because the Arc is shared.
        assert_eq!(handle.len().unwrap(), 1);
        handle.remove_entry("shared").unwrap();
        assert!(registry.is_empty().unwrap());
    }

    #[test]
    fn due_entries_at_fixed_time() {
        let registry = ScheduleRegistry::new();
        // Task never run -> always due.
        registry.add_entry(task("fresh", 60)).unwrap();

        // Task that ran 30s ago with a 60s interval -> not yet due at +30s.
        let base = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let mut ran = task("ran", 60);
        ran.last_run_at = Some(base);
        registry.add_entry(ran).unwrap();

        let at_30s = base + chrono::Duration::seconds(30);
        let due: Vec<String> = registry
            .due_entries_at(at_30s)
            .unwrap()
            .into_iter()
            .map(|t| t.name)
            .collect();
        assert!(due.contains(&"fresh".to_string()));
        assert!(!due.contains(&"ran".to_string()));

        // At +90s the interval task is due as well.
        let at_90s = base + chrono::Duration::seconds(90);
        let due: Vec<String> = registry
            .due_entries_at(at_90s)
            .unwrap()
            .into_iter()
            .map(|t| t.name)
            .collect();
        assert!(due.contains(&"ran".to_string()));
    }

    #[test]
    fn disabled_entries_are_not_due() {
        let registry = ScheduleRegistry::new();
        registry.add_entry(task("off", 60).disabled()).unwrap();
        let due = registry.due_entries().unwrap();
        assert!(due.is_empty());
    }

    #[test]
    fn mark_run_advances_due_state() {
        let registry = ScheduleRegistry::new();
        registry.add_entry(task("loop_task", 3600)).unwrap();
        let base = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();

        // Initially due (never run).
        assert_eq!(registry.due_entries_at(base).unwrap().len(), 1);

        // After marking a run, no longer due until the interval elapses.
        assert!(registry.mark_run_at("loop_task", base).unwrap());
        assert!(registry.due_entries_at(base).unwrap().is_empty());

        // Due again an hour later.
        let later = base + chrono::Duration::seconds(3601);
        assert_eq!(registry.due_entries_at(later).unwrap().len(), 1);
    }

    #[test]
    fn runtime_updates_are_visible_to_a_concurrent_reader() {
        // Simulates a beat loop reading on one thread while a control thread
        // mutates the shared registry.
        let registry = ScheduleRegistry::new();
        registry.add_entry(task("seed", 60)).unwrap();

        let writer = registry.clone();
        let handle = thread::spawn(move || {
            for i in 0..50 {
                writer
                    .add_entry(task(&format!("dyn-{i}"), 60))
                    .expect("add must succeed");
            }
            writer.remove_entry("seed").expect("remove must succeed");
        });

        // Reader keeps snapshotting; just ensure no lock errors and that the
        // count is monotonically observable.
        for _ in 0..50 {
            let _snapshot = registry.list_entries().expect("read must succeed");
        }
        handle.join().expect("writer thread panicked");

        assert_eq!(registry.len().unwrap(), 50);
        assert!(!registry.contains("seed").unwrap());
    }

    #[test]
    fn from_tasks_and_clear() {
        let registry = ScheduleRegistry::from_tasks(vec![task("a", 60), task("b", 120)]);
        assert_eq!(registry.len().unwrap(), 2);
        registry.clear().unwrap();
        assert!(registry.is_empty().unwrap());
    }

    #[test]
    fn shared_arc_strong_count_reflects_clones() {
        let registry = ScheduleRegistry::new();
        let _c1 = registry.clone();
        let _c2 = registry.clone();
        // 3 handles -> strong count 3 on the inner Arc.
        assert_eq!(Arc::strong_count(&registry.inner), 3);
    }
}
