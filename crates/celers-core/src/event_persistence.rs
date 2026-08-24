//! Event persistence for audit trails and replay
//!
//! Provides persistent event storage implementations:
//! - `FileEventPersister`: JSONL file-based storage with rotation
//! - `EventPersister` trait: Query and cleanup interface

use crate::event::{Event, EventEmitter};
use async_trait::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::AsyncWriteExt;
use tokio::sync::RwLock;

/// Trait for persistent event storage with query and cleanup capabilities
#[async_trait]
pub trait EventPersister: EventEmitter {
    /// Query events within a time range, optionally filtering by event type
    async fn query_events(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        event_type_filter: Option<&str>,
    ) -> crate::Result<Vec<Event>>;

    /// Count events, optionally filtering by event type
    async fn count_events(&self, event_type: Option<&str>) -> crate::Result<u64>;

    /// Remove events older than the given duration, returning the count of removed files
    async fn cleanup(&self, older_than: chrono::Duration) -> crate::Result<u64>;

    /// Flush any buffered data to persistent storage
    async fn flush(&self) -> crate::Result<()>;
}

/// Policy for rotating event log files
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RotationPolicy {
    /// Rotate daily (new file each calendar day)
    Daily,
    /// Rotate when file exceeds max_bytes
    SizeBased {
        /// Maximum file size in bytes before rotation
        max_bytes: u64,
    },
    /// Rotate on either daily boundary or size threshold
    Both {
        /// Maximum file size in bytes before rotation
        max_bytes: u64,
    },
}

/// Configuration for [`FileEventPersister`]
#[derive(Debug, Clone)]
pub struct FileEventPersisterConfig {
    /// Directory where event JSONL files are stored
    pub directory: PathBuf,
    /// Maximum file size in bytes (default: 100MB)
    pub max_file_size_bytes: u64,
    /// File rotation policy
    pub rotation: RotationPolicy,
    /// Number of days to retain event files (default: 30)
    ///
    /// Enforced by the persister's background maintenance task (see
    /// [`FileEventPersisterConfig::background_maintenance`]); a value of `0`
    /// disables automatic retention.
    pub retention_days: u32,
    /// How often to flush buffered writes (default: 1s)
    ///
    /// A background task flushes the write buffer at this cadence so a single
    /// `emit` is durable within one interval instead of sitting in an in-process
    /// buffer until the next batch/rotation. A value of zero disables the
    /// background flusher.
    pub flush_interval: Duration,
    /// How often the background task applies `retention_days` (default: 1h)
    pub retention_check_interval: Duration,
    /// Whether to run the background flush/retention task (default: true)
    ///
    /// When disabled, callers must drive [`EventPersister::flush`] and
    /// [`EventPersister::cleanup`] themselves.
    pub background_maintenance: bool,
    /// Whether event persistence is enabled
    pub enabled: bool,
}

impl Default for FileEventPersisterConfig {
    fn default() -> Self {
        Self {
            directory: PathBuf::from("./events"),
            max_file_size_bytes: 104_857_600, // 100MB
            rotation: RotationPolicy::Both {
                max_bytes: 104_857_600,
            },
            retention_days: 30,
            flush_interval: Duration::from_secs(1),
            retention_check_interval: Duration::from_secs(3600),
            background_maintenance: true,
            enabled: true,
        }
    }
}

impl FileEventPersisterConfig {
    /// Set the directory for event files
    #[must_use]
    pub fn with_directory(mut self, directory: impl Into<PathBuf>) -> Self {
        self.directory = directory.into();
        self
    }

    /// Set the maximum file size in bytes
    #[must_use]
    pub fn with_max_file_size(mut self, max_bytes: u64) -> Self {
        self.max_file_size_bytes = max_bytes;
        self
    }

    /// Set the rotation policy
    #[must_use]
    pub fn with_rotation(mut self, rotation: RotationPolicy) -> Self {
        self.rotation = rotation;
        self
    }

    /// Set the retention period in days
    #[must_use]
    pub fn with_retention_days(mut self, days: u32) -> Self {
        self.retention_days = days;
        self
    }

    /// Set the flush interval
    #[must_use]
    pub fn with_flush_interval(mut self, interval: Duration) -> Self {
        self.flush_interval = interval;
        self
    }

    /// Set how often retention is applied by the background task
    #[must_use]
    pub fn with_retention_check_interval(mut self, interval: Duration) -> Self {
        self.retention_check_interval = interval;
        self
    }

    /// Enable or disable the background flush/retention task
    #[must_use]
    pub fn with_background_maintenance(mut self, enabled: bool) -> Self {
        self.background_maintenance = enabled;
        self
    }

    /// Set whether persistence is enabled
    #[must_use]
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

/// File-based event persister using JSONL format with rotation
///
/// Events are written to date-partitioned JSONL files in the configured directory.
/// Files are named `events-YYYY-MM-DD.jsonl` with optional index suffix for
/// size-based rotation: `events-YYYY-MM-DD.N.jsonl`.
pub struct FileEventPersister {
    config: FileEventPersisterConfig,
    current_file: Arc<RwLock<Option<tokio::io::BufWriter<tokio::fs::File>>>>,
    current_file_size: Arc<AtomicU64>,
    current_file_date: Arc<RwLock<NaiveDate>>,
    events_written: Arc<AtomicU64>,
    /// Number of flushes performed by the background maintenance task.
    background_flushes: Arc<AtomicU64>,
    /// Number of retention passes performed by the background task.
    background_cleanups: Arc<AtomicU64>,
    /// Handle of the background maintenance task, aborted on drop.
    maintenance: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for FileEventPersister {
    fn drop(&mut self) {
        if let Some(handle) = self.maintenance.take() {
            handle.abort();
        }
    }
}

impl FileEventPersister {
    /// Create a new file-based event persister
    ///
    /// Creates the configured directory if it does not exist and opens
    /// the initial event file for writing.
    ///
    /// Unless [`FileEventPersisterConfig::background_maintenance`] is disabled,
    /// this also spawns a background task that flushes buffered writes every
    /// `flush_interval` and applies `retention_days` every
    /// `retention_check_interval`. The task is aborted when the persister is
    /// dropped.
    pub async fn new(config: FileEventPersisterConfig) -> crate::Result<Self> {
        tokio::fs::create_dir_all(&config.directory).await?;

        let today = Utc::now().date_naive();
        let mut persister = Self {
            config,
            current_file: Arc::new(RwLock::new(None)),
            current_file_size: Arc::new(AtomicU64::new(0)),
            current_file_date: Arc::new(RwLock::new(today)),
            events_written: Arc::new(AtomicU64::new(0)),
            background_flushes: Arc::new(AtomicU64::new(0)),
            background_cleanups: Arc::new(AtomicU64::new(0)),
            maintenance: None,
        };

        persister.ensure_file().await?;
        persister.start_background_maintenance();
        Ok(persister)
    }

    /// Spawn the background flush/retention task if it is configured and not
    /// already running.
    fn start_background_maintenance(&mut self) {
        if self.maintenance.is_some() {
            return;
        }
        if !self.config.background_maintenance
            || !self.config.enabled
            || self.config.flush_interval.is_zero()
        {
            return;
        }

        let current_file = Arc::clone(&self.current_file);
        let flushes = Arc::clone(&self.background_flushes);
        let cleanups = Arc::clone(&self.background_cleanups);
        let flush_interval = self.config.flush_interval;
        let retention_interval = self.config.retention_check_interval;
        let retention_days = self.config.retention_days;
        let directory = self.config.directory.clone();

        self.maintenance = Some(tokio::spawn(async move {
            let mut since_retention = Duration::ZERO;
            loop {
                tokio::time::sleep(flush_interval).await;

                if let Err(e) = Self::flush_writer(&current_file).await {
                    tracing::warn!(error = %e, "Background event flush failed");
                }
                flushes.fetch_add(1, Ordering::Relaxed);

                if retention_days == 0 || retention_interval.is_zero() {
                    continue;
                }
                since_retention = since_retention.saturating_add(flush_interval);
                if since_retention < retention_interval {
                    continue;
                }
                since_retention = Duration::ZERO;
                let older_than = chrono::Duration::days(i64::from(retention_days));
                match Self::cleanup_directory(&directory, older_than).await {
                    Ok(removed) => {
                        if removed > 0 {
                            tracing::info!(
                                removed,
                                retention_days,
                                "Removed expired event files during retention pass"
                            );
                        }
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "Background event retention pass failed");
                    }
                }
                cleanups.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    /// Get the total number of events written since creation
    #[must_use]
    pub fn events_written(&self) -> u64 {
        self.events_written.load(Ordering::Relaxed)
    }

    /// Number of flushes performed by the background maintenance task.
    #[must_use]
    pub fn background_flushes(&self) -> u64 {
        self.background_flushes.load(Ordering::Relaxed)
    }

    /// Number of retention passes performed by the background maintenance task.
    #[must_use]
    pub fn background_cleanups(&self) -> u64 {
        self.background_cleanups.load(Ordering::Relaxed)
    }

    /// Returns `true` if the background maintenance task is running.
    #[must_use]
    pub fn background_maintenance_running(&self) -> bool {
        self.maintenance
            .as_ref()
            .is_some_and(|handle| !handle.is_finished())
    }

    /// Flush the buffered writer behind `current_file`, if one is open.
    async fn flush_writer(
        current_file: &Arc<RwLock<Option<tokio::io::BufWriter<tokio::fs::File>>>>,
    ) -> crate::Result<()> {
        let mut file_guard = current_file.write().await;
        if let Some(writer) = file_guard.as_mut() {
            writer.flush().await?;
        }
        Ok(())
    }

    /// Remove event files older than `older_than` from `directory`.
    async fn cleanup_directory(
        directory: &Path,
        older_than: chrono::Duration,
    ) -> crate::Result<u64> {
        let cutoff = Utc::now()
            .date_naive()
            .checked_sub_signed(older_than)
            .ok_or_else(|| {
                crate::CelersError::Other("Invalid duration for cleanup cutoff".to_string())
            })?;

        let files = Self::list_event_files_in(directory).await?;
        let mut removed = 0u64;

        for file_path in &files {
            if let Some(file_date) = Self::parse_file_date(file_path) {
                if file_date < cutoff {
                    tokio::fs::remove_file(file_path).await?;
                    removed += 1;
                }
            }
        }

        Ok(removed)
    }

    /// Ensure a file is open and ready for writing, rotating if necessary
    async fn ensure_file(&self) -> crate::Result<()> {
        let today = Utc::now().date_naive();
        let mut file_guard = self.current_file.write().await;
        let mut date_guard = self.current_file_date.write().await;

        let needs_new_file = file_guard.is_none() || *date_guard != today;

        if needs_new_file {
            // Flush and drop old file
            if let Some(ref mut writer) = *file_guard {
                writer.flush().await?;
            }
            *file_guard = None;

            *date_guard = today;
            self.current_file_size.store(0, Ordering::Relaxed);

            let path = self.file_path_for_date(today);
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .await?;

            let metadata = file.metadata().await?;
            self.current_file_size
                .store(metadata.len(), Ordering::Relaxed);

            *file_guard = Some(tokio::io::BufWriter::new(file));
        }

        Ok(())
    }

    /// Check if rotation is needed based on the configured policy and rotate if so
    async fn rotate_if_needed(&self) -> crate::Result<()> {
        let needs_rotation = match &self.config.rotation {
            RotationPolicy::Daily => {
                let today = Utc::now().date_naive();
                let date_guard = self.current_file_date.read().await;
                *date_guard != today
            }
            RotationPolicy::SizeBased { max_bytes } => {
                self.current_file_size.load(Ordering::Relaxed) >= *max_bytes
            }
            RotationPolicy::Both { max_bytes } => {
                let today = Utc::now().date_naive();
                let date_guard = self.current_file_date.read().await;
                *date_guard != today || self.current_file_size.load(Ordering::Relaxed) >= *max_bytes
            }
        };

        if needs_rotation {
            self.perform_rotation().await?;
        }

        Ok(())
    }

    /// Perform the actual file rotation
    async fn perform_rotation(&self) -> crate::Result<()> {
        let mut file_guard = self.current_file.write().await;

        // Flush and close current file
        if let Some(ref mut writer) = *file_guard {
            writer.flush().await?;
        }
        *file_guard = None;

        let today = Utc::now().date_naive();
        let mut date_guard = self.current_file_date.write().await;
        *date_guard = today;

        // Find the next available index for today
        let mut index = 0u32;
        loop {
            let path = if index == 0 {
                self.file_path_for_date(today)
            } else {
                self.file_path_with_index(today, index)
            };

            if !path.exists() {
                let file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .await?;
                self.current_file_size.store(0, Ordering::Relaxed);
                *file_guard = Some(tokio::io::BufWriter::new(file));
                return Ok(());
            }

            // Check if existing file is under size limit
            let metadata = tokio::fs::metadata(&path).await?;
            if metadata.len() < self.config.max_file_size_bytes {
                let file = tokio::fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .await?;
                self.current_file_size
                    .store(metadata.len(), Ordering::Relaxed);
                *file_guard = Some(tokio::io::BufWriter::new(file));
                return Ok(());
            }

            index = index.saturating_add(1);
            if index > 10_000 {
                return Err(crate::CelersError::Other(
                    "Too many rotation files for a single day".to_string(),
                ));
            }
        }
    }

    /// Get the file path for a given date (primary file)
    fn file_path_for_date(&self, date: NaiveDate) -> PathBuf {
        self.config
            .directory
            .join(format!("events-{}.jsonl", date.format("%Y-%m-%d")))
    }

    /// Get the file path for a given date with a rotation index
    fn file_path_with_index(&self, date: NaiveDate, index: u32) -> PathBuf {
        self.config.directory.join(format!(
            "events-{}.{}.jsonl",
            date.format("%Y-%m-%d"),
            index
        ))
    }

    /// List all event files in the persister's directory
    async fn list_event_files(&self) -> crate::Result<Vec<PathBuf>> {
        Self::list_event_files_in(&self.config.directory).await
    }

    /// List all event files in `directory`
    async fn list_event_files_in(directory: &Path) -> crate::Result<Vec<PathBuf>> {
        let mut files = Vec::new();
        let mut entries = tokio::fs::read_dir(directory).await?;

        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                if name.starts_with("events-") && name.ends_with(".jsonl") {
                    files.push(path);
                }
            }
        }

        files.sort();
        Ok(files)
    }

    /// Parse the date from an event file name
    fn parse_file_date(path: &Path) -> Option<NaiveDate> {
        let name = path.file_name()?.to_str()?;
        // Format: events-YYYY-MM-DD.jsonl or events-YYYY-MM-DD.N.jsonl
        let date_str = name.strip_prefix("events-")?;
        let date_part = if let Some(pos) = date_str.find('.') {
            &date_str[..pos]
        } else {
            date_str
        };
        NaiveDate::parse_from_str(date_part, "%Y-%m-%d").ok()
    }

    /// Read and parse events from a single JSONL file
    async fn read_events_from_file(path: &Path) -> crate::Result<Vec<Event>> {
        let content = tokio::fs::read_to_string(path).await?;
        let mut events = Vec::new();

        for line in content.lines() {
            let trimmed = line.trim();
            if trimmed.is_empty() {
                continue;
            }
            match serde_json::from_str::<Event>(trimmed) {
                Ok(event) => events.push(event),
                Err(e) => {
                    tracing::warn!("Failed to parse event line in {:?}: {}", path, e);
                }
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl EventEmitter for FileEventPersister {
    async fn emit(&self, event: Event) -> crate::Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        self.ensure_file().await?;

        let json = serde_json::to_string(&event).map_err(|e| {
            crate::CelersError::Serialization(format!("Failed to serialize event: {}", e))
        })?;

        let line = format!("{}\n", json);
        let line_bytes = line.as_bytes();

        {
            let mut file_guard = self.current_file.write().await;
            if let Some(ref mut writer) = *file_guard {
                writer.write_all(line_bytes).await?;
                self.current_file_size
                    .fetch_add(line_bytes.len() as u64, Ordering::Relaxed);
                self.events_written.fetch_add(1, Ordering::Relaxed);
            }
        }

        self.rotate_if_needed().await?;

        Ok(())
    }

    async fn emit_batch(&self, events: Vec<Event>) -> crate::Result<()> {
        if !self.config.enabled {
            return Ok(());
        }

        self.ensure_file().await?;

        let mut total_bytes = 0u64;
        let mut lines = String::new();

        for event in &events {
            let json = serde_json::to_string(event).map_err(|e| {
                crate::CelersError::Serialization(format!("Failed to serialize event: {}", e))
            })?;
            lines.push_str(&json);
            lines.push('\n');
        }

        let line_bytes = lines.as_bytes();
        total_bytes += line_bytes.len() as u64;

        {
            let mut file_guard = self.current_file.write().await;
            if let Some(ref mut writer) = *file_guard {
                writer.write_all(line_bytes).await?;
                writer.flush().await?;
            }
        }

        self.current_file_size
            .fetch_add(total_bytes, Ordering::Relaxed);
        self.events_written
            .fetch_add(events.len() as u64, Ordering::Relaxed);

        self.rotate_if_needed().await?;

        Ok(())
    }

    fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

#[async_trait]
impl EventPersister for FileEventPersister {
    async fn query_events(
        &self,
        from: DateTime<Utc>,
        to: DateTime<Utc>,
        event_type_filter: Option<&str>,
    ) -> crate::Result<Vec<Event>> {
        // Flush before querying to ensure all buffered data is written
        self.flush().await?;

        let from_date = from.date_naive();
        let to_date = to.date_naive();

        let files = self.list_event_files().await?;
        let mut result = Vec::new();

        for file_path in &files {
            // Filter by file date
            if let Some(file_date) = Self::parse_file_date(file_path) {
                if file_date < from_date || file_date > to_date {
                    continue;
                }
            }

            let events = Self::read_events_from_file(file_path).await?;

            for event in events {
                let ts = event.timestamp();
                if ts >= from && ts <= to {
                    if let Some(filter) = event_type_filter {
                        if event.event_type() == filter {
                            result.push(event);
                        }
                    } else {
                        result.push(event);
                    }
                }
            }
        }

        Ok(result)
    }

    async fn count_events(&self, event_type: Option<&str>) -> crate::Result<u64> {
        self.flush().await?;

        let files = self.list_event_files().await?;
        let mut count = 0u64;

        for file_path in &files {
            let events = Self::read_events_from_file(file_path).await?;
            for event in &events {
                if let Some(filter) = event_type {
                    if event.event_type() == filter {
                        count += 1;
                    }
                } else {
                    count += 1;
                }
            }
        }

        Ok(count)
    }

    async fn cleanup(&self, older_than: chrono::Duration) -> crate::Result<u64> {
        Self::cleanup_directory(&self.config.directory, older_than).await
    }

    async fn flush(&self) -> crate::Result<()> {
        Self::flush_writer(&self.current_file).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::event::{TaskEvent, WorkerEvent};
    use uuid::Uuid;

    fn make_task_event(name: &str) -> Event {
        Event::Task(TaskEvent::Started {
            task_id: Uuid::new_v4(),
            task_name: name.to_string(),
            hostname: "test-worker".to_string(),
            timestamp: Utc::now(),
            pid: 1234,
        })
    }

    fn make_worker_event() -> Event {
        Event::Worker(WorkerEvent::Online {
            hostname: "test-worker".to_string(),
            timestamp: Utc::now(),
            sw_ident: "celers".to_string(),
            sw_ver: "0.2.0".to_string(),
            sw_sys: "linux".to_string(),
        })
    }

    #[test]
    fn test_file_persister_config_defaults() {
        let config = FileEventPersisterConfig::default();
        assert_eq!(config.max_file_size_bytes, 104_857_600);
        assert_eq!(config.retention_days, 30);
        assert_eq!(config.flush_interval, Duration::from_secs(1));
        assert_eq!(config.retention_check_interval, Duration::from_secs(3600));
        assert!(config.background_maintenance);
        assert!(config.enabled);
        assert_eq!(config.directory, PathBuf::from("./events"));
    }

    /// Drive the (paused) clock forward in `step` increments, letting the
    /// spawned maintenance task run between advances, until `check` holds or the
    /// bounded budget runs out.
    ///
    /// Advancing repeatedly rather than once matters: the background task may
    /// not have registered its first timer yet when the test starts driving the
    /// clock. No wall-clock sleeping is involved.
    async fn advance_until(step: Duration, mut check: impl FnMut() -> bool) -> bool {
        for _ in 0..100 {
            for _ in 0..64 {
                if check() {
                    return true;
                }
                tokio::task::yield_now().await;
            }
            tokio::time::advance(step).await;
        }
        check()
    }

    #[tokio::test(start_paused = true)]
    async fn test_background_flusher_makes_single_emits_durable() {
        // Regression: `flush_interval` was dead config. A single `emit` sat in
        // an 8 KiB in-process BufWriter for an unbounded time and was lost on
        // crash, even though the persister documents a 1s durability window.
        let dir = std::env::temp_dir().join(format!("celers_test_flush_{}", Uuid::new_v4()));
        // A long interval keeps the paused clock's auto-advance from reaching
        // the first tick on its own, so the flush below is the one this test
        // triggers explicitly.
        let config = FileEventPersisterConfig::default()
            .with_directory(&dir)
            .with_flush_interval(Duration::from_secs(3600));

        let persister = FileEventPersister::new(config)
            .await
            .expect("Failed to create persister");
        assert!(persister.background_maintenance_running());

        persister
            .emit(make_task_event("buffered"))
            .await
            .expect("emit failed");

        let path = persister.file_path_for_date(Utc::now().date_naive());

        // One flush interval later the background task has flushed it.
        assert!(
            advance_until(Duration::from_secs(3601), || persister.background_flushes()
                > 0)
            .await,
            "background flusher never ran"
        );

        let after = tokio::fs::read_to_string(&path).await.unwrap_or_default();
        assert!(
            after.contains("buffered"),
            "event was not flushed to disk: {after:?}"
        );

        drop(persister);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test(start_paused = true)]
    async fn test_background_retention_removes_expired_files() {
        // Regression: `retention_days` was dead config, so a default-configured
        // persister grew without bound forever.
        let dir = std::env::temp_dir().join(format!("celers_test_retain_{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir).await.unwrap();

        // An event file well past the retention horizon.
        let stale = dir.join("events-2000-01-01.jsonl");
        tokio::fs::write(&stale, b"{}\n").await.unwrap();

        let config = FileEventPersisterConfig::default()
            .with_directory(&dir)
            .with_flush_interval(Duration::from_secs(3600))
            .with_retention_days(7)
            .with_retention_check_interval(Duration::from_secs(3600));

        let persister = FileEventPersister::new(config)
            .await
            .expect("Failed to create persister");

        assert!(
            advance_until(Duration::from_secs(3601), || persister
                .background_cleanups()
                > 0)
            .await,
            "background retention pass never ran"
        );

        assert!(
            !stale.exists(),
            "expired event file was not removed by retention"
        );

        drop(persister);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_background_maintenance_can_be_disabled() {
        let dir = std::env::temp_dir().join(format!("celers_test_nobg_{}", Uuid::new_v4()));
        let config = FileEventPersisterConfig::default()
            .with_directory(&dir)
            .with_background_maintenance(false);

        let persister = FileEventPersister::new(config)
            .await
            .expect("Failed to create persister");
        assert!(!persister.background_maintenance_running());

        drop(persister);
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_file_persister_write_and_read() {
        let dir = std::env::temp_dir().join(format!("celers_test_wr_{}", Uuid::new_v4()));
        let config = FileEventPersisterConfig::default()
            .with_directory(&dir)
            .with_enabled(true);

        let persister = FileEventPersister::new(config)
            .await
            .expect("Failed to create persister");

        let event = make_task_event("test_task");
        persister.emit(event.clone()).await.expect("emit failed");
        persister.flush().await.expect("flush failed");

        let from = Utc::now() - chrono::Duration::hours(1);
        let to = Utc::now() + chrono::Duration::hours(1);
        let events = persister
            .query_events(from, to, None)
            .await
            .expect("query failed");

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].event_type(), "task-started");

        // Cleanup temp directory
        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_file_persister_rotation_by_size() {
        let dir = std::env::temp_dir().join(format!("celers_test_rot_{}", Uuid::new_v4()));
        let config = FileEventPersisterConfig::default()
            .with_directory(&dir)
            .with_max_file_size(100) // very small to trigger rotation
            .with_rotation(RotationPolicy::SizeBased { max_bytes: 100 });

        let persister = FileEventPersister::new(config)
            .await
            .expect("Failed to create persister");

        // Emit enough events to trigger rotation
        for i in 0..20 {
            let event = make_task_event(&format!("task_{}", i));
            persister.emit(event).await.expect("emit failed");
        }
        persister.flush().await.expect("flush failed");

        // Check that multiple files were created
        let files = persister
            .list_event_files()
            .await
            .expect("list files failed");
        assert!(
            files.len() > 1,
            "Expected multiple files after rotation, got {}",
            files.len()
        );

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_file_persister_cleanup() {
        let dir = std::env::temp_dir().join(format!("celers_test_cl_{}", Uuid::new_v4()));
        tokio::fs::create_dir_all(&dir)
            .await
            .expect("create dir failed");

        // Create an old event file manually
        let old_date = NaiveDate::from_ymd_opt(2020, 1, 1).expect("invalid date");
        let old_file = dir.join(format!("events-{}.jsonl", old_date.format("%Y-%m-%d")));
        tokio::fs::write(&old_file, "{}\n")
            .await
            .expect("write old file failed");

        let config = FileEventPersisterConfig::default()
            .with_directory(&dir)
            .with_enabled(true);

        let persister = FileEventPersister::new(config)
            .await
            .expect("Failed to create persister");

        // Cleanup anything older than 1 day
        let removed = persister
            .cleanup(chrono::Duration::days(1))
            .await
            .expect("cleanup failed");

        assert!(removed >= 1, "Expected at least 1 file removed");

        // Verify old file is gone
        assert!(!old_file.exists());

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_file_persister_query_with_filter() {
        let dir = std::env::temp_dir().join(format!("celers_test_filt_{}", Uuid::new_v4()));
        let config = FileEventPersisterConfig::default()
            .with_directory(&dir)
            .with_enabled(true);

        let persister = FileEventPersister::new(config)
            .await
            .expect("Failed to create persister");

        // Emit task and worker events
        persister
            .emit(make_task_event("my_task"))
            .await
            .expect("emit task failed");
        persister
            .emit(make_worker_event())
            .await
            .expect("emit worker failed");
        persister
            .emit(make_task_event("another_task"))
            .await
            .expect("emit task2 failed");
        persister.flush().await.expect("flush failed");

        let from = Utc::now() - chrono::Duration::hours(1);
        let to = Utc::now() + chrono::Duration::hours(1);

        // Filter for task-started only
        let task_events = persister
            .query_events(from, to, Some("task-started"))
            .await
            .expect("query failed");
        assert_eq!(task_events.len(), 2);

        // Filter for worker-online only
        let worker_events = persister
            .query_events(from, to, Some("worker-online"))
            .await
            .expect("query failed");
        assert_eq!(worker_events.len(), 1);

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_file_persister_count() {
        let dir = std::env::temp_dir().join(format!("celers_test_cnt_{}", Uuid::new_v4()));
        let config = FileEventPersisterConfig::default()
            .with_directory(&dir)
            .with_enabled(true);

        let persister = FileEventPersister::new(config)
            .await
            .expect("Failed to create persister");

        persister
            .emit(make_task_event("t1"))
            .await
            .expect("emit failed");
        persister
            .emit(make_task_event("t2"))
            .await
            .expect("emit failed");
        persister
            .emit(make_worker_event())
            .await
            .expect("emit failed");
        persister.flush().await.expect("flush failed");

        let total = persister
            .count_events(None)
            .await
            .expect("count all failed");
        assert_eq!(total, 3);

        let task_count = persister
            .count_events(Some("task-started"))
            .await
            .expect("count task failed");
        assert_eq!(task_count, 2);

        let worker_count = persister
            .count_events(Some("worker-online"))
            .await
            .expect("count worker failed");
        assert_eq!(worker_count, 1);

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_file_persister_batch() {
        let dir = std::env::temp_dir().join(format!("celers_test_batch_{}", Uuid::new_v4()));
        let config = FileEventPersisterConfig::default()
            .with_directory(&dir)
            .with_enabled(true);

        let persister = FileEventPersister::new(config)
            .await
            .expect("Failed to create persister");

        let events = vec![
            make_task_event("batch_1"),
            make_task_event("batch_2"),
            make_worker_event(),
        ];

        persister
            .emit_batch(events)
            .await
            .expect("emit_batch failed");

        let total = persister.count_events(None).await.expect("count failed");
        assert_eq!(total, 3);
        assert_eq!(persister.events_written(), 3);

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }

    #[tokio::test]
    async fn test_file_persister_disabled() {
        let dir = std::env::temp_dir().join(format!("celers_test_dis_{}", Uuid::new_v4()));
        let config = FileEventPersisterConfig::default()
            .with_directory(&dir)
            .with_enabled(false);

        let persister = FileEventPersister::new(config)
            .await
            .expect("Failed to create persister");

        assert!(!persister.is_enabled());

        // Emitting should be a no-op
        persister
            .emit(make_task_event("ignored"))
            .await
            .expect("emit failed");
        assert_eq!(persister.events_written(), 0);

        let _ = tokio::fs::remove_dir_all(&dir).await;
    }
}
