//! [`FileScheduleStore`]: the local-file [`ScheduleStore`] implementation.
//!
//! Independent of (does not share code with) `scheduler_persistence.rs`'s
//! `BeatScheduler::save_state`/`load_from_file`: those methods keep their
//! existing, separately-tested behaviour completely unchanged.
//! `FileScheduleStore` is the same *kind* of mechanism — atomic write
//! (temp file + `fsync` + `rename`), rotated `.bak` fallback — reimplemented
//! behind the generic, schema-agnostic [`ScheduleStore`] trait so a caller
//! can opt into the trait-based API (`BeatScheduler::load_from_store`/the
//! `schedule_store`-aware `save_state_async`) while still landing on disk by
//! default.

use async_trait::async_trait;
use std::path::{Path, PathBuf};

use super::ScheduleStore;
use crate::config::ScheduleError;

/// A [`ScheduleStore`] backed by a single local file, with the same
/// atomic-write-plus-backup durability as the scheduler's built-in file
/// persistence.
#[derive(Debug, Clone)]
pub struct FileScheduleStore {
    path: PathBuf,
}

impl FileScheduleStore {
    /// Store state at `path` (and its rotated backup, `<path>.bak`).
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    /// The primary file this store reads from and writes to.
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn backup_path(&self) -> PathBuf {
        let mut name = self.path.as_os_str().to_os_string();
        name.push(".bak");
        PathBuf::from(name)
    }

    /// Read and sanity-check one candidate file. `Ok(None)` means "not
    /// usable" (absent, unreadable, or not valid JSON) — the caller falls
    /// back to the next candidate. Validity is checked generically (does
    /// this parse as *any* JSON document), not against `BeatScheduler`'s
    /// specific shape: this store has no dependency on that type.
    fn try_read(path: &Path) -> Option<Vec<u8>> {
        if !path.exists() {
            return None;
        }
        let bytes = match std::fs::read(path) {
            Ok(bytes) => bytes,
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "failed to read state file");
                return None;
            }
        };
        match serde_json::from_slice::<serde_json::Value>(&bytes) {
            Ok(_) => Some(bytes),
            Err(e) => {
                tracing::warn!(path = %path.display(), error = %e, "failed to parse state file");
                None
            }
        }
    }

    /// Write `bytes` to `path` such that the file is never observed
    /// partially written: a sibling temp file, `fsync`d, then an atomic
    /// `rename` into place, rotating any existing file to `.bak` first.
    fn write_atomically(path: &Path, backup: &Path, bytes: &[u8]) -> Result<(), ScheduleError> {
        use std::io::Write;

        let dir = match path.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => parent.to_path_buf(),
            _ => PathBuf::from("."),
        };
        let file_name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "celers-beat-schedule-store.json".to_string());
        // PID plus a random suffix keeps two writers pointed at the same
        // path from clobbering each other's temp file mid-write.
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
            if let Err(e) = std::fs::rename(path, backup) {
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
}

#[async_trait]
impl ScheduleStore for FileScheduleStore {
    async fn load(&self) -> Result<Option<Vec<u8>>, ScheduleError> {
        let path = self.path.clone();
        let backup = self.backup_path();
        tokio::task::spawn_blocking(move || {
            if let Some(bytes) = Self::try_read(&path) {
                return Ok(Some(bytes));
            }
            if let Some(bytes) = Self::try_read(&backup) {
                tracing::warn!(
                    path = %path.display(),
                    backup = %backup.display(),
                    "state file missing or unreadable; recovered from backup"
                );
                return Ok(Some(bytes));
            }
            if path.exists() {
                return Err(ScheduleError::Persistence(format!(
                    "Failed to parse state file {} and no usable backup at {}",
                    path.display(),
                    backup.display()
                )));
            }
            Ok(None)
        })
        .await
        .map_err(|e| ScheduleError::Persistence(format!("State read task failed to join: {e}")))?
    }

    async fn save(&self, bytes: &[u8]) -> Result<(), ScheduleError> {
        let path = self.path.clone();
        let backup = self.backup_path();
        let bytes = bytes.to_vec();
        tokio::task::spawn_blocking(move || Self::write_atomically(&path, &backup, &bytes))
            .await
            .map_err(|e| {
                ScheduleError::Persistence(format!("State write task failed to join: {e}"))
            })?
    }

    async fn remove(&self) -> Result<(), ScheduleError> {
        let path = self.path.clone();
        let backup = self.backup_path();
        tokio::task::spawn_blocking(move || {
            for candidate in [&path, &backup] {
                if candidate.exists() {
                    std::fs::remove_file(candidate).map_err(|e| {
                        ScheduleError::Persistence(format!(
                            "Failed to remove {}: {}",
                            candidate.display(),
                            e
                        ))
                    })?;
                }
            }
            Ok(())
        })
        .await
        .map_err(|e| ScheduleError::Persistence(format!("State remove task failed to join: {e}")))?
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(name: &str) -> PathBuf {
        std::env::temp_dir().join(format!(
            "celers-beat-schedule-store-test-{}-{}.json",
            std::process::id(),
            name
        ))
    }

    #[tokio::test]
    async fn load_is_none_when_nothing_was_ever_saved() {
        let store = FileScheduleStore::new(temp_path("never_saved"));
        assert!(store.load().await.expect("load").is_none());
    }

    #[tokio::test]
    async fn save_then_load_round_trips_the_exact_bytes() {
        let path = temp_path("round_trip");
        let store = FileScheduleStore::new(&path);
        store.save(b"{\"tasks\":{}}").await.expect("save");

        let loaded = store.load().await.expect("load").expect("bytes present");
        assert_eq!(loaded, b"{\"tasks\":{}}");

        store.remove().await.expect("cleanup");
        let _ = std::fs::remove_file(&path);
    }

    #[tokio::test]
    async fn a_second_save_replaces_the_first_and_rotates_a_backup() {
        let path = temp_path("rotation");
        let store = FileScheduleStore::new(&path);
        store.save(b"{\"v\":1}").await.expect("first save");
        store.save(b"{\"v\":2}").await.expect("second save");

        let loaded = store.load().await.expect("load").expect("bytes present");
        assert_eq!(loaded, b"{\"v\":2}");
        assert!(
            store.backup_path().exists(),
            "the first save must have been rotated to .bak"
        );

        store.remove().await.expect("cleanup");
    }

    #[tokio::test]
    async fn remove_deletes_both_the_primary_and_the_backup() {
        let path = temp_path("remove");
        let store = FileScheduleStore::new(&path);
        store.save(b"{\"v\":1}").await.expect("first save");
        store.save(b"{\"v\":2}").await.expect("second save");
        assert!(store.backup_path().exists());

        store.remove().await.expect("remove");

        assert!(store.load().await.expect("load").is_none());
        assert!(!path.exists());
        assert!(!store.backup_path().exists());
    }

    #[tokio::test]
    async fn a_corrupt_primary_falls_back_to_the_backup() {
        let path = temp_path("corrupt_fallback");
        let store = FileScheduleStore::new(&path);
        store.save(b"{\"v\":1}").await.expect("first save");
        store.save(b"{\"v\":2}").await.expect("second save");

        // Corrupt the primary directly, leaving the rotated backup intact.
        std::fs::write(&path, b"not json at all").expect("corrupt the primary");

        let loaded = store.load().await.expect("load").expect("backup used");
        assert_eq!(loaded, b"{\"v\":1}", "must recover the rotated backup");

        store.remove().await.expect("cleanup");
    }
}
