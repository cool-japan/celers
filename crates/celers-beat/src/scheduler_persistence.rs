//! Scheduler state persistence
//!
//! Durable, crash-safe persistence of [`BeatScheduler`] state: atomic writes
//! (temp file + `fsync` + `rename`), a rotated previous-good copy the loader
//! can fall back to, an async variant that keeps the blocking file I/O off the
//! runtime worker threads, and failure counters so a scheduler running without
//! durable state is observable rather than silent.
//!
//! This is the *local-file* mechanism specifically. [`crate::schedule_store`]
//! wraps the same durability guarantee behind a pluggable
//! [`ScheduleStore`](crate::schedule_store::ScheduleStore) trait — set one
//! via [`BeatScheduler::with_schedule_store`]/[`BeatScheduler::load_from_store`]
//! and [`BeatScheduler::save_state_async`] below writes through it instead,
//! which is what makes a shared (e.g. Redis-backed) schedule catalog possible
//! across more than one beat instance. Sync [`BeatScheduler::save_state`]
//! always uses the local file, regardless of whether a store is configured.

use crate::config::ScheduleError;
use crate::scheduler::{default_instance_id, BeatScheduler};
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::Arc;

impl BeatScheduler {
    /// Load scheduler state from file
    ///
    /// Creates a new scheduler with tasks loaded from the specified file.
    /// If the file doesn't exist or can't be read, returns an empty scheduler.
    ///
    /// # Arguments
    /// * `path` - Path to the state file
    ///
    /// # Returns
    /// Scheduler loaded from file, or empty scheduler if file doesn't exist
    pub fn load_from_file<P: Into<PathBuf>>(path: P) -> Result<Self, ScheduleError> {
        let path = path.into();
        let backup = Self::backup_path(&path);

        // The primary is written atomically (temp file + rename), so it is
        // never partial. It can still be missing for a moment if the process
        // died between the "rotate to .bak" and "rename temp into place"
        // renames, and it can be corrupt if edited by hand — fall back to the
        // last known good copy in both cases rather than silently starting
        // with an empty schedule set.
        let primary = Self::try_load_path(&path)?;
        if let Some(mut scheduler) = primary {
            scheduler.state_file = Some(path);
            scheduler.ensure_instance_id();
            return Ok(scheduler);
        }

        if let Some(mut scheduler) = Self::try_load_path(&backup)? {
            tracing::warn!(
                path = %path.display(),
                backup = %backup.display(),
                "state file missing or unreadable; recovered from backup"
            );
            scheduler.state_file = Some(path);
            scheduler.ensure_instance_id();
            return Ok(scheduler);
        }

        if path.exists() {
            // The file is present but neither it nor the backup parsed: this is
            // real data loss, not a fresh start, so surface it.
            return Err(ScheduleError::Persistence(format!(
                "Failed to parse state file {} and no usable backup at {}",
                path.display(),
                backup.display()
            )));
        }

        Ok(Self::with_state_file(Some(path)))
    }

    /// Read and parse one candidate state file.
    ///
    /// `Ok(None)` means "not usable" (absent, unreadable, or unparsable), which
    /// the caller resolves by trying the next candidate.
    fn try_load_path(path: &Path) -> Result<Option<Self>, ScheduleError> {
        if !path.exists() {
            return Ok(None);
        }

        let content = match std::fs::read_to_string(path) {
            Ok(content) => content,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "failed to read state file");
                return Ok(None);
            }
        };

        match serde_json::from_str::<BeatScheduler>(&content) {
            Ok(scheduler) => Ok(Some(scheduler)),
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "failed to parse state file");
                Ok(None)
            }
        }
    }

    /// Give a deserialized scheduler a process-unique instance id.
    fn ensure_instance_id(&mut self) {
        if self.instance_id.is_empty() {
            self.instance_id = default_instance_id();
        }
    }

    /// Path of the rotated previous-good copy of a state file.
    fn backup_path(path: &Path) -> PathBuf {
        let mut name = path.as_os_str().to_os_string();
        name.push(".bak");
        PathBuf::from(name)
    }

    /// Save scheduler state to file
    ///
    /// Persists the current scheduler state (all tasks and their run history)
    /// to the configured state file. If no state file is configured, this is a no-op.
    ///
    /// # Returns
    /// Ok(()) if successful or no state file configured
    pub fn save_state(&self) -> Result<(), ScheduleError> {
        let Some(path) = self.state_file.clone() else {
            return Ok(());
        };

        let bytes = self.serialize_state()?;
        match Self::write_state_atomically(&path, &bytes) {
            Ok(()) => Ok(()),
            Err(e) => {
                self.persistence_errors.fetch_add(1, Ordering::Relaxed);
                Err(e)
            }
        }
    }

    /// Persist scheduler state without blocking the async runtime.
    ///
    /// `save_state` performs synchronous file I/O; calling it directly from an
    /// async task stalls a runtime worker thread for the duration of the write.
    /// This variant serializes on the caller (cheap, needs `&self`) and hands
    /// the write to `spawn_blocking` — except when a
    /// [`ScheduleStore`](crate::schedule_store::ScheduleStore) is configured
    /// ([`BeatScheduler::with_schedule_store`]/[`BeatScheduler::load_from_store`]),
    /// in which case it writes through that store instead and `state_file` is
    /// not consulted at all. Sync [`Self::save_state`] is unaffected either
    /// way — it always uses `state_file`, never the store — see
    /// `schedule_store.rs`'s module doc for why a store-aware save needed to
    /// be async-only.
    pub async fn save_state_async(&self) -> Result<(), ScheduleError> {
        if let Some(store) = self.schedule_store.clone() {
            let bytes = self.serialize_state()?;
            let result = store.save(&bytes).await;
            if result.is_err() {
                self.persistence_errors.fetch_add(1, Ordering::Relaxed);
            }
            return result;
        }

        let Some(path) = self.state_file.clone() else {
            return Ok(());
        };

        let bytes = self.serialize_state()?;
        let errors = Arc::clone(&self.persistence_errors);

        let result =
            tokio::task::spawn_blocking(move || Self::write_state_atomically(&path, &bytes))
                .await
                .map_err(|e| {
                    ScheduleError::Persistence(format!("State write task failed to join: {}", e))
                })?;

        if result.is_err() {
            errors.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    /// Serialize the scheduler into the compact on-disk form.
    ///
    /// Compact rather than pretty: the state file is machine-read, and pretty
    /// printing roughly doubles both the serialization cost and the bytes
    /// written on every save.
    fn serialize_state(&self) -> Result<Vec<u8>, ScheduleError> {
        serde_json::to_vec(&self)
            .map_err(|e| ScheduleError::Persistence(format!("Failed to serialize state: {}", e)))
    }

    /// Write `bytes` to `path` such that the file is never observed partially
    /// written.
    ///
    /// Writes a sibling temp file, `fsync`s it, rotates any existing state file
    /// to `<path>.bak`, then `rename`s the temp file into place. `rename`
    /// within a directory is atomic on POSIX and on Windows, so a crash or
    /// power loss at any point leaves either the previous state file or the new
    /// one — never a truncated mixture.
    fn write_state_atomically(path: &Path, bytes: &[u8]) -> Result<(), ScheduleError> {
        use std::io::Write;

        let dir = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        };

        let file_name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "celers-beat-state.json".to_string());

        // PID plus a random suffix keeps two writers pointed at the same path —
        // whether two processes or two schedulers in one process — from
        // clobbering each other's temp file mid-write.
        let temp_path = dir.join(format!(
            ".{}.{}.{}.tmp",
            file_name,
            std::process::id(),
            uuid::Uuid::new_v4()
        ));

        let write_temp = || -> std::io::Result<()> {
            let mut file = std::fs::File::create(&temp_path)?;
            file.write_all(bytes)?;
            file.sync_all()?;
            Ok(())
        };

        if let Err(e) = write_temp() {
            let _ = std::fs::remove_file(&temp_path);
            return Err(ScheduleError::Persistence(format!(
                "Failed to write temporary state file {}: {}",
                temp_path.display(),
                e
            )));
        }

        if path.exists() {
            let backup = Self::backup_path(path);
            if let Err(e) = std::fs::rename(path, &backup) {
                tracing::warn!(
                    path = %path.display(),
                    error = %e,
                    "failed to rotate previous state file to backup"
                );
            }
        }

        if let Err(e) = std::fs::rename(&temp_path, path) {
            let _ = std::fs::remove_file(&temp_path);
            return Err(ScheduleError::Persistence(format!(
                "Failed to install state file {}: {}",
                path.display(),
                e
            )));
        }

        Ok(())
    }

    /// Number of state-file writes that have failed in this process.
    ///
    /// A persistently non-zero, growing value means the scheduler is running
    /// without durable state (full disk, read-only mount, bad path).
    pub fn persistence_error_count(&self) -> u64 {
        self.persistence_errors.load(Ordering::Relaxed)
    }

    /// Number of schedule evaluations that have failed in this process.
    ///
    /// A task whose schedule cannot be evaluated is never dispatched, so a
    /// non-zero value here means at least one registered task is silently
    /// inert; [`BeatScheduler::validate_all_schedules`] identifies which.
    pub fn schedule_eval_error_count(&self) -> u64 {
        self.schedule_eval_errors.load(Ordering::Relaxed)
    }

    /// Persist state, logging rather than propagating a failure.
    ///
    /// Used on the tick path, where a persistence failure must be visible but
    /// must not abort the dispatch that already happened.
    pub(crate) async fn persist_after_tick(&self) {
        if let Err(e) = self.save_state_async().await {
            tracing::error!(
                error = %e,
                failures = self.persistence_error_count(),
                "failed to persist beat scheduler state"
            );
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::Schedule;
    use crate::schedule_store::{FileScheduleStore, ScheduleStore};
    use crate::task::ScheduledTask;
    use std::sync::Arc;

    fn temp_store_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "celers-beat-scheduler-persistence-store-test-{}-{}.json",
            std::process::id(),
            name
        ))
    }

    #[tokio::test]
    async fn save_state_async_prefers_the_store_over_the_file_when_both_are_set() {
        let store_path = temp_store_path("prefers_store");
        let file_path = temp_store_path("prefers_store_unused_file");
        let store = Arc::new(FileScheduleStore::new(&store_path));

        let mut scheduler = BeatScheduler::with_state_file(Some(file_path.clone()));
        scheduler.with_schedule_store(store.clone());
        scheduler
            .add_task(ScheduledTask::new(
                "via_store".to_string(),
                Schedule::interval(60),
            ))
            .expect("add_task");

        // `add_task` itself calls the *synchronous* `save_state` (scheduler.rs,
        // unrelated to and unchanged by the ScheduleStore work), which is
        // documented and separately tested
        // (`sync_save_state_still_uses_the_file_even_with_a_store_configured`,
        // below) to always target `state_file` regardless of any configured
        // store — sync code cannot await the store's async I/O. So `file_path`
        // legitimately exists at this point; that is not what this test is
        // about. Clear it before exercising `save_state_async` so the
        // assertion below isolates that one method's own behaviour rather
        // than being contaminated by `add_task`'s unrelated, already-correct
        // side effect.
        let _ = std::fs::remove_file(&file_path);

        scheduler.save_state_async().await.expect("save via store");

        assert!(
            store.load().await.expect("store load").is_some(),
            "save_state_async must write through the configured store"
        );
        assert!(
            !file_path.exists(),
            "save_state_async itself must not touch state_file once a schedule_store is configured"
        );

        store.remove().await.expect("cleanup");
    }

    #[tokio::test]
    async fn load_from_store_round_trips_registered_tasks() {
        let store_path = temp_store_path("round_trip");
        let store = Arc::new(FileScheduleStore::new(&store_path));

        let mut original = BeatScheduler::with_state_file(None);
        original.with_schedule_store(store.clone());
        original
            .add_task(ScheduledTask::new(
                "roundtrip_task".to_string(),
                Schedule::interval(30),
            ))
            .expect("add_task");
        original.save_state_async().await.expect("save");

        let restored = BeatScheduler::load_from_store(store.clone())
            .await
            .expect("load_from_store");

        assert!(restored.get_task("roundtrip_task").is_some());
        assert!(
            restored.schedule_store().is_some(),
            "load_from_store must leave the scheduler wired to the same store"
        );

        store.remove().await.expect("cleanup");
    }

    #[tokio::test]
    async fn load_from_store_yields_a_fresh_scheduler_when_nothing_was_saved() {
        let store_path = temp_store_path("fresh");
        let store = Arc::new(FileScheduleStore::new(&store_path));

        let scheduler = BeatScheduler::load_from_store(store.clone())
            .await
            .expect("load_from_store");

        assert!(scheduler.get_task("anything").is_none());
        assert!(!scheduler.instance_id.is_empty());
    }

    #[tokio::test]
    async fn sync_save_state_still_uses_the_file_even_with_a_store_configured() {
        let store_path = temp_store_path("sync_uses_file");
        let file_path = temp_store_path("sync_uses_file_target");
        let store = Arc::new(FileScheduleStore::new(&store_path));

        let mut scheduler = BeatScheduler::with_state_file(Some(file_path.clone()));
        scheduler.with_schedule_store(store.clone());

        scheduler.save_state().expect("sync save_state");

        assert!(
            file_path.exists(),
            "sync save_state must be unaffected by schedule_store, per its own doc"
        );
        assert!(
            store.load().await.expect("store load").is_none(),
            "sync save_state must not touch the store"
        );

        let _ = std::fs::remove_file(&file_path);
    }
}
