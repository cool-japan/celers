//! Checkpoint and resume support for long-running tasks
//!
//! This module provides mechanisms to checkpoint task state and resume
//! execution from saved checkpoints, enabling fault-tolerant processing
//! of long-running tasks.
//!
//! # Features
//!
//! - State checkpoint creation and restoration
//! - Configurable checkpoint intervals
//! - Multiple storage backends (in-memory, file-based) via the pluggable
//!   [`CheckpointStore`] trait
//! - Automatic checkpoint cleanup
//! - Progress tracking
//!
//! # Durability
//!
//! [`CheckpointManager::new`] stores checkpoints purely in process memory:
//! they do **not** survive a crash or restart, and exist mainly for
//! testing or for callers with no durability requirement.
//!
//! [`CheckpointManager::new_with_file_backend`] persists every checkpoint
//! to a JSON file per task under a configurable directory, written
//! atomically (temp file + rename) so a crash mid-write can never corrupt
//! a previously-good checkpoint. A **new** `CheckpointManager` constructed
//! against the same directory after a restart can immediately
//! [`load_checkpoint`](CheckpointManager::load_checkpoint) whatever the
//! previous process last saved -- this is what makes checkpointing
//! actually useful for resuming long-running work after a crash.
//!
//! # Example
//!
//! ```
//! use celers_worker::{CheckpointManager, CheckpointConfig, Checkpoint};
//! use std::time::Duration;
//!
//! # async fn example() {
//! let config = CheckpointConfig::new()
//!     .with_interval(Duration::from_secs(60))
//!     .with_max_checkpoints(5);
//!
//! let manager = CheckpointManager::new(config);
//!
//! // Create a checkpoint
//! let checkpoint = Checkpoint::new("task-123".to_string(), vec![1, 2, 3, 4]);
//! manager.save_checkpoint(checkpoint).await.unwrap();
//!
//! // Resume from checkpoint
//! if let Some(checkpoint) = manager.load_checkpoint("task-123").await {
//!     println!("Resuming from checkpoint with {} bytes", checkpoint.data.len());
//! }
//! # }
//! ```
//!
//! For durable, crash-surviving checkpoints instead:
//!
//! ```
//! use celers_worker::checkpoint::{CheckpointManager, CheckpointConfig};
//!
//! # async fn example(dir: std::path::PathBuf) {
//! let manager = CheckpointManager::new_with_file_backend(CheckpointConfig::new(), dir);
//! # }
//! ```

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::{Mutex, RwLock};
use tracing::{debug, error, info};

/// Checkpoint data for a task
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Checkpoint {
    /// Task ID
    pub task_id: String,
    /// Checkpoint data (serialized state)
    pub data: Vec<u8>,
    /// When the checkpoint was created
    pub created_at: SystemTime,
    /// Checkpoint version/sequence number
    pub version: u64,
    /// Optional metadata
    pub metadata: HashMap<String, String>,
    /// Progress percentage (0-100)
    pub progress: u8,
}

impl Checkpoint {
    /// Create a new checkpoint
    pub fn new(task_id: String, data: Vec<u8>) -> Self {
        Self {
            task_id,
            data,
            created_at: SystemTime::now(),
            version: 1,
            metadata: HashMap::new(),
            progress: 0,
        }
    }

    /// Set checkpoint version
    pub fn with_version(mut self, version: u64) -> Self {
        self.version = version;
        self
    }

    /// Add metadata
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Set progress percentage
    pub fn with_progress(mut self, progress: u8) -> Self {
        self.progress = progress.min(100);
        self
    }

    /// Check if checkpoint has expired
    pub fn is_expired(&self, ttl: Duration) -> bool {
        if let Ok(age) = SystemTime::now().duration_since(self.created_at) {
            age >= ttl
        } else {
            false
        }
    }

    /// Get checkpoint size in bytes
    pub fn size(&self) -> usize {
        self.data.len()
    }
}

/// Checkpoint storage strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointStrategy {
    /// Keep only the latest checkpoint
    LatestOnly,
    /// Keep all checkpoints within TTL
    KeepAll,
    /// Keep last N checkpoints
    KeepLast(usize),
    /// Keep checkpoints at intervals
    Interval(Duration),
}

impl Default for CheckpointStrategy {
    fn default() -> Self {
        Self::KeepLast(5)
    }
}

/// Checkpoint configuration
#[derive(Clone)]
pub struct CheckpointConfig {
    /// Checkpoint interval
    pub checkpoint_interval: Duration,
    /// Maximum number of checkpoints to keep per task
    pub max_checkpoints: usize,
    /// Checkpoint TTL (time to live)
    pub checkpoint_ttl: Duration,
    /// Storage strategy
    pub strategy: CheckpointStrategy,
    /// Enable automatic cleanup
    pub auto_cleanup: bool,
    /// Cleanup interval
    pub cleanup_interval: Duration,
}

impl CheckpointConfig {
    /// Create a new checkpoint configuration
    pub fn new() -> Self {
        Self {
            checkpoint_interval: Duration::from_secs(300), // 5 minutes
            max_checkpoints: 10,
            checkpoint_ttl: Duration::from_secs(86400), // 24 hours
            strategy: CheckpointStrategy::default(),
            auto_cleanup: true,
            cleanup_interval: Duration::from_secs(3600), // 1 hour
        }
    }

    /// Set checkpoint interval
    pub fn with_interval(mut self, interval: Duration) -> Self {
        self.checkpoint_interval = interval;
        self
    }

    /// Set maximum checkpoints
    pub fn with_max_checkpoints(mut self, max: usize) -> Self {
        self.max_checkpoints = max;
        self
    }

    /// Set checkpoint TTL
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.checkpoint_ttl = ttl;
        self
    }

    /// Set storage strategy
    pub fn with_strategy(mut self, strategy: CheckpointStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Enable or disable auto cleanup
    pub fn with_auto_cleanup(mut self, enable: bool) -> Self {
        self.auto_cleanup = enable;
        self
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.max_checkpoints == 0 {
            return Err("Maximum checkpoints must be greater than 0".to_string());
        }
        if self.checkpoint_interval.is_zero() {
            return Err("Checkpoint interval must be greater than 0".to_string());
        }
        Ok(())
    }
}

impl Default for CheckpointConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Checkpoint statistics
#[derive(Clone, Debug, Default)]
pub struct CheckpointStats {
    /// Total checkpoints created
    pub total_created: usize,
    /// Total checkpoints restored
    pub total_restored: usize,
    /// Total checkpoints deleted
    pub total_deleted: usize,
    /// Current number of checkpoints
    pub current_count: usize,
    /// Total bytes stored
    pub total_bytes: usize,
}

/// Errors from a [`CheckpointStore`] operation.
#[derive(Debug, thiserror::Error)]
pub enum CheckpointStoreError {
    /// The underlying filesystem operation failed.
    #[error("checkpoint I/O error: {0}")]
    Io(#[from] std::io::Error),
    /// The checkpoint payload could not be serialized/deserialized.
    #[error("checkpoint serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
}

/// Pluggable checkpoint persistence backend.
///
/// A store holds, for each task id, an ordered history of checkpoints
/// (oldest first -- the last element is "latest"). Implementations must be
/// safe to share behind an `Arc` and to call concurrently.
/// [`CheckpointManager`] additionally serializes its own read-modify-write
/// sequences (e.g. "load history, apply retention strategy, write back")
/// with an internal lock, so a store does not need to implement
/// per-task transactions itself.
#[async_trait]
pub trait CheckpointStore: Send + Sync {
    /// Load the full checkpoint history for `task_id`, oldest first. An
    /// empty `Vec` (not an error) means the task has no checkpoints.
    async fn load_all(&self, task_id: &str) -> Result<Vec<Checkpoint>, CheckpointStoreError>;

    /// Replace `task_id`'s stored history with exactly `checkpoints`.
    /// Passing an empty `Vec` removes the task entirely (equivalent to
    /// [`delete_task`](Self::delete_task)).
    async fn replace_all(
        &self,
        task_id: &str,
        checkpoints: Vec<Checkpoint>,
    ) -> Result<(), CheckpointStoreError>;

    /// Remove all stored checkpoints for `task_id`. Not an error if the
    /// task had none.
    async fn delete_task(&self, task_id: &str) -> Result<(), CheckpointStoreError>;

    /// List every task id with at least one stored checkpoint. Used to
    /// rehydrate an index (e.g. statistics) after a restart.
    async fn list_task_ids(&self) -> Result<Vec<String>, CheckpointStoreError>;

    /// Remove every stored checkpoint for every task.
    async fn clear(&self) -> Result<(), CheckpointStoreError>;
}

/// In-memory [`CheckpointStore`].
///
/// Data does not survive a process restart -- use [`FileCheckpointStore`]
/// (via [`CheckpointManager::new_with_file_backend`]) for that.
#[derive(Default)]
pub struct MemoryCheckpointStore {
    data: RwLock<HashMap<String, Vec<Checkpoint>>>,
}

impl MemoryCheckpointStore {
    /// Create an empty in-memory store.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl CheckpointStore for MemoryCheckpointStore {
    async fn load_all(&self, task_id: &str) -> Result<Vec<Checkpoint>, CheckpointStoreError> {
        Ok(self
            .data
            .read()
            .await
            .get(task_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn replace_all(
        &self,
        task_id: &str,
        checkpoints: Vec<Checkpoint>,
    ) -> Result<(), CheckpointStoreError> {
        let mut data = self.data.write().await;
        if checkpoints.is_empty() {
            data.remove(task_id);
        } else {
            data.insert(task_id.to_string(), checkpoints);
        }
        Ok(())
    }

    async fn delete_task(&self, task_id: &str) -> Result<(), CheckpointStoreError> {
        self.data.write().await.remove(task_id);
        Ok(())
    }

    async fn list_task_ids(&self) -> Result<Vec<String>, CheckpointStoreError> {
        Ok(self.data.read().await.keys().cloned().collect())
    }

    async fn clear(&self) -> Result<(), CheckpointStoreError> {
        self.data.write().await.clear();
        Ok(())
    }
}

/// On-disk representation of one task's checkpoint history: the task id
/// is stored alongside the checkpoints (rather than relied upon from the
/// file name) so that listing/rehydration never depends on reversing the
/// filename encoding.
#[derive(Serialize, Deserialize)]
struct TaskCheckpointFile {
    task_id: String,
    checkpoints: Vec<Checkpoint>,
}

/// File-based [`CheckpointStore`]: one JSON file per task under a
/// configurable root directory, written atomically (temp file + rename)
/// so a crash mid-write can never leave a corrupt/partial file in place
/// of a valid one -- the rename is same-filesystem (the temp file lives
/// alongside its destination), so it is a single atomic filesystem
/// operation.
pub struct FileCheckpointStore {
    root_dir: PathBuf,
}

impl FileCheckpointStore {
    /// Create a store rooted at `root_dir`. Performs no I/O itself: the
    /// directory is created lazily on first write.
    pub fn new(root_dir: impl Into<PathBuf>) -> Self {
        Self {
            root_dir: root_dir.into(),
        }
    }

    /// Root directory this store persists into.
    pub fn root_dir(&self) -> &std::path::Path {
        &self.root_dir
    }

    /// Path of the file backing `task_id`'s checkpoint history.
    ///
    /// `task_id` is hex-encoded rather than used directly as a path
    /// component so that a task id containing `/`, `..`, or other
    /// filesystem-meaningful characters can never escape `root_dir` or
    /// collide with another task's file. The *original* task id is still
    /// recorded inside the file's own content ([`TaskCheckpointFile`]),
    /// so nothing is lost and listing never needs to reverse the
    /// encoding.
    fn task_file_path(&self, task_id: &str) -> PathBuf {
        let mut encoded = String::with_capacity(task_id.len() * 2 + 5);
        for byte in task_id.as_bytes() {
            encoded.push_str(&format!("{byte:02x}"));
        }
        encoded.push_str(".json");
        self.root_dir.join(encoded)
    }

    async fn read_task_file(
        &self,
        path: &std::path::Path,
    ) -> Result<Option<TaskCheckpointFile>, CheckpointStoreError> {
        match tokio::fs::read(path).await {
            Ok(bytes) => Ok(Some(serde_json::from_slice(&bytes)?)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn write_task_file(
        &self,
        task_id: &str,
        checkpoints: &[Checkpoint],
    ) -> Result<(), CheckpointStoreError> {
        tokio::fs::create_dir_all(&self.root_dir).await?;
        let path = self.task_file_path(task_id);
        let payload = TaskCheckpointFile {
            task_id: task_id.to_string(),
            checkpoints: checkpoints.to_vec(),
        };
        let bytes = serde_json::to_vec(&payload)?;

        // Atomic write: write to a uniquely-named temp file in the same
        // directory, then rename over the destination. A crash at any
        // point leaves either the old file intact or the new file fully
        // written -- never a truncated/partial one.
        let tmp_name = format!(
            ".{}.tmp-{}",
            path.file_name()
                .and_then(|f| f.to_str())
                .unwrap_or("checkpoint.json"),
            uuid::Uuid::new_v4()
        );
        let tmp_path = self.root_dir.join(tmp_name);
        tokio::fs::write(&tmp_path, &bytes).await?;
        let renamed = tokio::fs::rename(&tmp_path, &path).await;
        if renamed.is_err() {
            // Best-effort cleanup of the temp file if the rename itself
            // failed; the primary error is returned below regardless.
            let _ = tokio::fs::remove_file(&tmp_path).await;
        }
        renamed?;
        Ok(())
    }
}

#[async_trait]
impl CheckpointStore for FileCheckpointStore {
    async fn load_all(&self, task_id: &str) -> Result<Vec<Checkpoint>, CheckpointStoreError> {
        let path = self.task_file_path(task_id);
        Ok(self
            .read_task_file(&path)
            .await?
            .map(|f| f.checkpoints)
            .unwrap_or_default())
    }

    async fn replace_all(
        &self,
        task_id: &str,
        checkpoints: Vec<Checkpoint>,
    ) -> Result<(), CheckpointStoreError> {
        if checkpoints.is_empty() {
            self.delete_task(task_id).await
        } else {
            self.write_task_file(task_id, &checkpoints).await
        }
    }

    async fn delete_task(&self, task_id: &str) -> Result<(), CheckpointStoreError> {
        let path = self.task_file_path(task_id);
        match tokio::fs::remove_file(&path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn list_task_ids(&self) -> Result<Vec<String>, CheckpointStoreError> {
        let mut ids = Vec::new();
        let mut dir = match tokio::fs::read_dir(&self.root_dir).await {
            Ok(dir) => dir,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(ids),
            Err(e) => return Err(e.into()),
        };
        while let Some(entry) = dir.next_entry().await? {
            let path = entry.path();
            if path.extension().and_then(|e| e.to_str()) != Some("json") {
                // Skips in-progress `.{name}.tmp-{uuid}` files too: their
                // final path component's extension is the random suffix,
                // never literally "json".
                continue;
            }
            if let Some(file) = self.read_task_file(&path).await? {
                ids.push(file.task_id);
            }
        }
        Ok(ids)
    }

    async fn clear(&self) -> Result<(), CheckpointStoreError> {
        match tokio::fs::remove_dir_all(&self.root_dir).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
}

/// Checkpoint manager
pub struct CheckpointManager {
    /// Configuration
    config: CheckpointConfig,
    /// Persistence backend (in-memory by default; see
    /// [`new_with_file_backend`](Self::new_with_file_backend) for a
    /// durable one).
    store: Arc<dyn CheckpointStore>,
    /// Serializes read-modify-write sequences (load history, apply
    /// retention strategy, write back) across all tasks, mirroring the
    /// effective serialization a single `RwLock<HashMap<..>>` gave the
    /// original in-memory-only implementation. Without this, two
    /// concurrent `save_checkpoint` calls could race: both load the same
    /// pre-save history, and the second `replace_all` would silently
    /// discard the first save.
    write_lock: Mutex<()>,
    /// Statistics
    stats: Arc<RwLock<CheckpointStats>>,
}

impl CheckpointManager {
    /// Create a new checkpoint manager backed by in-memory storage only.
    ///
    /// Checkpoints created through this manager do **not** survive a
    /// process crash or restart -- see
    /// [`new_with_file_backend`](Self::new_with_file_backend).
    pub fn new(config: CheckpointConfig) -> Self {
        Self::with_store(config, Arc::new(MemoryCheckpointStore::new()))
    }

    /// Create a checkpoint manager backed by durable file storage rooted
    /// at `dir`.
    ///
    /// Checkpoints written through this manager survive a process crash
    /// or restart: a **new** `CheckpointManager` constructed with the
    /// same directory can immediately load whatever the previous process
    /// last saved. Call [`rehydrate_stats`](Self::rehydrate_stats)
    /// afterward if you also want [`get_stats`](Self::get_stats) to
    /// reflect what is already on disk rather than only activity through
    /// this instance.
    pub fn new_with_file_backend(config: CheckpointConfig, dir: impl Into<PathBuf>) -> Self {
        Self::with_store(config, Arc::new(FileCheckpointStore::new(dir)))
    }

    /// Create a checkpoint manager backed by an arbitrary
    /// [`CheckpointStore`] implementation.
    pub fn with_store(config: CheckpointConfig, store: Arc<dyn CheckpointStore>) -> Self {
        Self {
            config,
            store,
            write_lock: Mutex::new(()),
            stats: Arc::new(RwLock::new(CheckpointStats::default())),
        }
    }

    /// Save a checkpoint for a task.
    ///
    /// Applies the configured [`CheckpointStrategy`] immediately, so the
    /// persisted history (in memory, or on disk for the file backend)
    /// never grows past what the strategy allows.
    pub async fn save_checkpoint(
        &self,
        checkpoint: Checkpoint,
    ) -> Result<(), CheckpointStoreError> {
        let task_id = checkpoint.task_id.clone();
        let checkpoint_size = checkpoint.size();

        let (count_before, count_after) = {
            let _guard = self.write_lock.lock().await;
            let mut task_checkpoints = self.store.load_all(&task_id).await?;
            let count_before = task_checkpoints.len();
            task_checkpoints.push(checkpoint);
            self.apply_strategy(&mut task_checkpoints);
            let count_after = task_checkpoints.len();
            self.store.replace_all(&task_id, task_checkpoints).await?;
            (count_before, count_after)
        };

        let mut stats = self.stats.write().await;
        stats.total_created += 1;
        stats.current_count = if count_after >= count_before {
            stats
                .current_count
                .saturating_add(count_after - count_before)
        } else {
            stats
                .current_count
                .saturating_sub(count_before - count_after)
        };
        stats.total_bytes = stats.total_bytes.saturating_add(checkpoint_size);

        info!(
            "Saved checkpoint for task {} (size: {} bytes)",
            task_id, checkpoint_size
        );
        Ok(())
    }

    /// Load the latest checkpoint for a task.
    pub async fn load_checkpoint(&self, task_id: &str) -> Option<Checkpoint> {
        let mut checkpoints = self.load_all_or_log(task_id).await?;
        let checkpoint = checkpoints.pop()?;

        let mut stats = self.stats.write().await;
        stats.total_restored += 1;

        debug!(
            "Loaded checkpoint for task {} (version: {})",
            task_id, checkpoint.version
        );
        Some(checkpoint)
    }

    /// Load a specific checkpoint version.
    pub async fn load_checkpoint_version(&self, task_id: &str, version: u64) -> Option<Checkpoint> {
        let checkpoints = self.load_all_or_log(task_id).await?;
        let checkpoint = checkpoints.into_iter().find(|c| c.version == version)?;

        debug!("Loaded checkpoint version {} for task {}", version, task_id);
        Some(checkpoint)
    }

    /// Delete all checkpoints for a task.
    pub async fn delete_checkpoints(&self, task_id: &str) -> usize {
        let existing = {
            let _guard = self.write_lock.lock().await;
            let existing = match self.store.load_all(task_id).await {
                Ok(list) => list,
                Err(e) => {
                    error!("failed to load checkpoints for task {task_id} before delete: {e}");
                    return 0;
                }
            };
            if existing.is_empty() {
                return 0;
            }
            if let Err(e) = self.store.delete_task(task_id).await {
                error!("failed to delete checkpoints for task {task_id}: {e}");
                return 0;
            }
            existing
        };

        let count = existing.len();
        let total_size: usize = existing.iter().map(|c| c.size()).sum();

        let mut stats = self.stats.write().await;
        stats.total_deleted += count;
        stats.current_count = stats.current_count.saturating_sub(count);
        stats.total_bytes = stats.total_bytes.saturating_sub(total_size);

        info!("Deleted {} checkpoints for task {}", count, task_id);
        count
    }

    /// Get all checkpoints for a task.
    pub async fn get_checkpoints(&self, task_id: &str) -> Vec<Checkpoint> {
        self.load_all_or_log(task_id).await.unwrap_or_default()
    }

    /// Get checkpoint count for a task.
    pub async fn checkpoint_count(&self, task_id: &str) -> usize {
        self.get_checkpoints(task_id).await.len()
    }

    /// Check if a task has checkpoints.
    pub async fn has_checkpoint(&self, task_id: &str) -> bool {
        self.checkpoint_count(task_id).await > 0
    }

    /// Get task progress from latest checkpoint.
    pub async fn get_progress(&self, task_id: &str) -> Option<u8> {
        self.get_checkpoints(task_id)
            .await
            .last()
            .map(|c| c.progress)
    }

    /// Cleanup expired checkpoints across every task.
    pub async fn cleanup_expired(&self) -> usize {
        let (total_removed, total_size_removed) = {
            let _guard = self.write_lock.lock().await;
            let task_ids = match self.store.list_task_ids().await {
                Ok(ids) => ids,
                Err(e) => {
                    error!("failed to list tasks during checkpoint cleanup: {e}");
                    return 0;
                }
            };

            let mut total_removed = 0usize;
            let mut total_size_removed = 0usize;

            for task_id in task_ids {
                let mut task_checkpoints = match self.store.load_all(&task_id).await {
                    Ok(list) => list,
                    Err(e) => {
                        error!("failed to load checkpoints for task {task_id} during cleanup: {e}");
                        continue;
                    }
                };
                let original_len = task_checkpoints.len();
                let removed_size: usize = task_checkpoints
                    .iter()
                    .filter(|c| c.is_expired(self.config.checkpoint_ttl))
                    .map(|c| c.size())
                    .sum();
                task_checkpoints.retain(|c| !c.is_expired(self.config.checkpoint_ttl));
                let removed = original_len - task_checkpoints.len();

                if removed > 0 {
                    if let Err(e) = self.store.replace_all(&task_id, task_checkpoints).await {
                        error!("failed to persist pruned checkpoints for task {task_id}: {e}");
                        continue;
                    }
                    total_removed += removed;
                    total_size_removed += removed_size;
                    debug!(
                        "Removed {} expired checkpoints for task {}",
                        removed, task_id
                    );
                }
            }

            (total_removed, total_size_removed)
        };

        if total_removed > 0 {
            let mut stats = self.stats.write().await;
            stats.total_deleted += total_removed;
            stats.current_count = stats.current_count.saturating_sub(total_removed);
            stats.total_bytes = stats.total_bytes.saturating_sub(total_size_removed);
            info!("Cleaned up {} expired checkpoints", total_removed);
        }

        total_removed
    }

    /// Get checkpoint statistics.
    pub async fn get_stats(&self) -> CheckpointStats {
        self.stats.read().await.clone()
    }

    /// Recompute `current_count`/`total_bytes` from what is actually
    /// stored right now.
    ///
    /// Not required for the correctness of
    /// [`load_checkpoint`](Self::load_checkpoint)/[`save_checkpoint`](Self::save_checkpoint)
    /// (which always consult the store directly rather than a cached
    /// index), but useful after constructing
    /// [`new_with_file_backend`](Self::new_with_file_backend) against a
    /// directory containing checkpoints from a previous process, so that
    /// [`get_stats`](Self::get_stats) reflects reality immediately
    /// instead of only activity through this instance.
    pub async fn rehydrate_stats(&self) -> Result<(), CheckpointStoreError> {
        let task_ids = self.store.list_task_ids().await?;
        let mut current_count = 0usize;
        let mut total_bytes = 0usize;
        for task_id in &task_ids {
            let checkpoints = self.store.load_all(task_id).await?;
            current_count += checkpoints.len();
            total_bytes += checkpoints.iter().map(|c| c.size()).sum::<usize>();
        }

        let mut stats = self.stats.write().await;
        stats.current_count = current_count;
        stats.total_bytes = total_bytes;
        Ok(())
    }

    /// Clear all checkpoints for every task.
    pub async fn clear_all(&self) {
        {
            let _guard = self.write_lock.lock().await;
            if let Err(e) = self.store.clear().await {
                error!("failed to clear checkpoint store: {e}");
                return;
            }
        }

        let mut stats = self.stats.write().await;
        *stats = CheckpointStats::default();

        info!("Cleared all checkpoints");
    }

    /// `store.load_all`, logging and returning `None` on a store error
    /// rather than propagating it -- reads degrade gracefully so an
    /// infrastructure hiccup on the file backend doesn't turn every
    /// accessor into a `Result`-returning API.
    async fn load_all_or_log(&self, task_id: &str) -> Option<Vec<Checkpoint>> {
        match self.store.load_all(task_id).await {
            Ok(list) => Some(list),
            Err(e) => {
                error!("failed to load checkpoints for task {task_id}: {e}");
                None
            }
        }
    }

    /// Apply the configured storage strategy to one task's (already
    /// loaded, already includes the newest entry) checkpoint list.
    fn apply_strategy(&self, checkpoints: &mut Vec<Checkpoint>) {
        match self.config.strategy {
            CheckpointStrategy::LatestOnly => {
                if checkpoints.len() > 1 {
                    checkpoints.drain(0..checkpoints.len() - 1);
                }
            }
            CheckpointStrategy::KeepAll => {
                // Remove expired checkpoints only
                checkpoints.retain(|c| !c.is_expired(self.config.checkpoint_ttl));
            }
            CheckpointStrategy::KeepLast(n) => {
                if checkpoints.len() > n {
                    checkpoints.drain(0..checkpoints.len() - n);
                }
            }
            CheckpointStrategy::Interval(_interval) => {
                // Keep checkpoints at specific intervals
                // For simplicity, keep last N checkpoints
                if checkpoints.len() > self.config.max_checkpoints {
                    checkpoints.drain(0..checkpoints.len() - self.config.max_checkpoints);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checkpoint_creation() {
        let checkpoint = Checkpoint::new("task-1".to_string(), vec![1, 2, 3, 4, 5]);
        assert_eq!(checkpoint.task_id, "task-1");
        assert_eq!(checkpoint.data.len(), 5);
        assert_eq!(checkpoint.version, 1);
        assert_eq!(checkpoint.progress, 0);
    }

    #[test]
    fn test_checkpoint_with_metadata() {
        let checkpoint = Checkpoint::new("task-1".to_string(), vec![1, 2, 3])
            .with_version(2)
            .with_progress(50)
            .with_metadata("key", "value");

        assert_eq!(checkpoint.version, 2);
        assert_eq!(checkpoint.progress, 50);
        assert_eq!(checkpoint.metadata.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_checkpoint_size() {
        let checkpoint = Checkpoint::new("task-1".to_string(), vec![1, 2, 3, 4, 5]);
        assert_eq!(checkpoint.size(), 5);
    }

    #[test]
    fn test_checkpoint_config_default() {
        let config = CheckpointConfig::default();
        assert_eq!(config.checkpoint_interval, Duration::from_secs(300));
        assert_eq!(config.max_checkpoints, 10);
        assert!(config.auto_cleanup);
    }

    #[test]
    fn test_checkpoint_config_builder() {
        let config = CheckpointConfig::new()
            .with_interval(Duration::from_secs(60))
            .with_max_checkpoints(5)
            .with_ttl(Duration::from_secs(3600))
            .with_auto_cleanup(false);

        assert_eq!(config.checkpoint_interval, Duration::from_secs(60));
        assert_eq!(config.max_checkpoints, 5);
        assert_eq!(config.checkpoint_ttl, Duration::from_secs(3600));
        assert!(!config.auto_cleanup);
    }

    #[test]
    fn test_checkpoint_config_validation() {
        let invalid_config = CheckpointConfig::new().with_max_checkpoints(0);
        assert!(invalid_config.validate().is_err());

        let invalid_config2 = CheckpointConfig::new().with_interval(Duration::ZERO);
        assert!(invalid_config2.validate().is_err());

        let valid_config = CheckpointConfig::new();
        assert!(valid_config.validate().is_ok());
    }

    #[tokio::test]
    async fn test_checkpoint_manager_save_load() {
        let config = CheckpointConfig::default();
        let manager = CheckpointManager::new(config);

        let checkpoint = Checkpoint::new("task-1".to_string(), vec![1, 2, 3, 4, 5]);
        manager.save_checkpoint(checkpoint).await.unwrap();

        let loaded = manager.load_checkpoint("task-1").await;
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().data, vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_checkpoint_manager_versions() {
        let config = CheckpointConfig::default();
        let manager = CheckpointManager::new(config);

        let checkpoint1 = Checkpoint::new("task-1".to_string(), vec![1]).with_version(1);
        let checkpoint2 = Checkpoint::new("task-1".to_string(), vec![2]).with_version(2);

        manager.save_checkpoint(checkpoint1).await.unwrap();
        manager.save_checkpoint(checkpoint2).await.unwrap();

        let loaded = manager.load_checkpoint_version("task-1", 1).await;
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().data, vec![1]);
    }

    #[tokio::test]
    async fn test_checkpoint_manager_delete() {
        let config = CheckpointConfig::default();
        let manager = CheckpointManager::new(config);

        let checkpoint = Checkpoint::new("task-1".to_string(), vec![1, 2, 3]);
        manager.save_checkpoint(checkpoint).await.unwrap();

        assert!(manager.has_checkpoint("task-1").await);

        let deleted = manager.delete_checkpoints("task-1").await;
        assert_eq!(deleted, 1);
        assert!(!manager.has_checkpoint("task-1").await);
    }

    #[tokio::test]
    async fn test_checkpoint_strategy_latest_only() {
        let config = CheckpointConfig::new().with_strategy(CheckpointStrategy::LatestOnly);
        let manager = CheckpointManager::new(config);

        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![1]))
            .await
            .unwrap();
        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![2]))
            .await
            .unwrap();
        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![3]))
            .await
            .unwrap();

        let count = manager.checkpoint_count("task-1").await;
        assert_eq!(count, 1);

        let latest = manager.load_checkpoint("task-1").await.unwrap();
        assert_eq!(latest.data, vec![3]);
    }

    #[tokio::test]
    async fn test_checkpoint_strategy_keep_last() {
        let config = CheckpointConfig::new().with_strategy(CheckpointStrategy::KeepLast(2));
        let manager = CheckpointManager::new(config);

        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![1]))
            .await
            .unwrap();
        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![2]))
            .await
            .unwrap();
        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![3]))
            .await
            .unwrap();

        let count = manager.checkpoint_count("task-1").await;
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_checkpoint_progress() {
        let config = CheckpointConfig::default();
        let manager = CheckpointManager::new(config);

        let checkpoint = Checkpoint::new("task-1".to_string(), vec![1]).with_progress(75);
        manager.save_checkpoint(checkpoint).await.unwrap();

        let progress = manager.get_progress("task-1").await;
        assert_eq!(progress, Some(75));
    }

    #[tokio::test]
    async fn test_checkpoint_stats() {
        let config = CheckpointConfig::default();
        let manager = CheckpointManager::new(config);

        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![1, 2, 3]))
            .await
            .unwrap();
        manager
            .save_checkpoint(Checkpoint::new("task-2".to_string(), vec![4, 5]))
            .await
            .unwrap();

        let stats = manager.get_stats().await;
        assert_eq!(stats.total_created, 2);
        assert_eq!(stats.current_count, 2);
        assert_eq!(stats.total_bytes, 5);
    }

    #[tokio::test]
    async fn test_checkpoint_clear_all() {
        let config = CheckpointConfig::default();
        let manager = CheckpointManager::new(config);

        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![1]))
            .await
            .unwrap();
        manager
            .save_checkpoint(Checkpoint::new("task-2".to_string(), vec![2]))
            .await
            .unwrap();

        manager.clear_all().await;

        assert!(!manager.has_checkpoint("task-1").await);
        assert!(!manager.has_checkpoint("task-2").await);
    }

    #[test]
    fn test_checkpoint_strategy_default() {
        let strategy = CheckpointStrategy::default();
        assert!(matches!(strategy, CheckpointStrategy::KeepLast(5)));
    }

    // --- Regression tests: file-based durability (idx 181) ----------------

    /// A scratch directory under `std::env::temp_dir()`, unique per call,
    /// removed on drop.
    struct ScratchDir(PathBuf);

    impl ScratchDir {
        fn new(label: &str) -> Self {
            let dir = std::env::temp_dir().join(format!(
                "celers-checkpoint-test-{label}-{}",
                uuid::Uuid::new_v4()
            ));
            Self(dir)
        }
    }

    impl Drop for ScratchDir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    /// The central regression test for this fix: a checkpoint saved by
    /// one `CheckpointManager` instance must be loadable by a brand new
    /// instance pointed at the same directory -- i.e. it survives
    /// exactly the kind of crash/restart checkpointing exists for. The
    /// previous implementation was pure `HashMap` storage, so a fresh
    /// manager always started empty.
    #[tokio::test]
    async fn test_file_backend_checkpoint_survives_new_manager_instance() {
        let scratch = ScratchDir::new("survives-restart");

        {
            let manager =
                CheckpointManager::new_with_file_backend(CheckpointConfig::default(), &scratch.0);
            let checkpoint = Checkpoint::new("task-1".to_string(), vec![9, 8, 7, 6])
                .with_progress(42)
                .with_metadata("stage", "reduce");
            manager.save_checkpoint(checkpoint).await.unwrap();
            // `manager` is dropped here, simulating process exit.
        }

        // A completely new manager, new in-memory state, same directory.
        let restarted =
            CheckpointManager::new_with_file_backend(CheckpointConfig::default(), &scratch.0);
        let loaded = restarted
            .load_checkpoint("task-1")
            .await
            .expect("checkpoint must survive across manager instances");
        assert_eq!(loaded.data, vec![9, 8, 7, 6]);
        assert_eq!(loaded.progress, 42);
        assert_eq!(loaded.metadata.get("stage"), Some(&"reduce".to_string()));
    }

    #[tokio::test]
    async fn test_file_backend_multiple_tasks_are_independent() {
        let scratch = ScratchDir::new("multi-task");
        let manager =
            CheckpointManager::new_with_file_backend(CheckpointConfig::default(), &scratch.0);

        manager
            .save_checkpoint(Checkpoint::new("task-a".to_string(), vec![1]))
            .await
            .unwrap();
        manager
            .save_checkpoint(Checkpoint::new("task-b".to_string(), vec![2]))
            .await
            .unwrap();

        assert_eq!(
            manager.load_checkpoint("task-a").await.unwrap().data,
            vec![1]
        );
        assert_eq!(
            manager.load_checkpoint("task-b").await.unwrap().data,
            vec![2]
        );
    }

    /// A task id containing filesystem-meaningful characters must not
    /// escape the store's root directory or collide with another task.
    #[tokio::test]
    async fn test_file_backend_handles_unusual_task_ids_safely() {
        let scratch = ScratchDir::new("unusual-ids");
        let manager =
            CheckpointManager::new_with_file_backend(CheckpointConfig::default(), &scratch.0);

        let tricky_id = "../../etc/passwd:weird/id";
        manager
            .save_checkpoint(Checkpoint::new(tricky_id.to_string(), vec![42]))
            .await
            .unwrap();

        let loaded = manager.load_checkpoint(tricky_id).await;
        assert_eq!(loaded.unwrap().data, vec![42]);

        // Nothing escaped the scratch directory.
        assert!(scratch.0.exists());
        let parent = scratch.0.parent().unwrap();
        for entry in std::fs::read_dir(parent).unwrap().flatten() {
            let name = entry.file_name();
            let name = name.to_string_lossy();
            // No stray "etc" or "passwd" directory/file must appear
            // alongside our scratch dir.
            assert!(!name.contains("passwd"));
        }
    }

    /// Retention strategies must prune the *persisted* history, not just
    /// an in-memory copy -- otherwise a file-backed manager would
    /// accumulate checkpoint history on disk forever.
    #[tokio::test]
    async fn test_file_backend_retention_strategy_prunes_disk_state() {
        let scratch = ScratchDir::new("retention");
        let config = CheckpointConfig::new().with_strategy(CheckpointStrategy::KeepLast(2));
        let manager = CheckpointManager::new_with_file_backend(config, &scratch.0);

        for i in 0..5u8 {
            manager
                .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![i]))
                .await
                .unwrap();
        }

        assert_eq!(manager.checkpoint_count("task-1").await, 2);

        // Verify against a *fresh* manager instance too, proving the cap
        // was actually written to disk and not just held in memory.
        let restarted = CheckpointManager::new_with_file_backend(
            CheckpointConfig::new().with_strategy(CheckpointStrategy::KeepLast(2)),
            &scratch.0,
        );
        let remaining = restarted.get_checkpoints("task-1").await;
        assert_eq!(remaining.len(), 2);
        assert_eq!(remaining.last().unwrap().data, vec![4]);
    }

    /// `delete_checkpoints` on a file-backed manager must actually remove
    /// the file from disk, not just forget about it in memory.
    #[tokio::test]
    async fn test_file_backend_delete_removes_data_from_disk() {
        let scratch = ScratchDir::new("delete");
        let manager =
            CheckpointManager::new_with_file_backend(CheckpointConfig::default(), &scratch.0);

        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![1, 2, 3]))
            .await
            .unwrap();
        assert_eq!(manager.delete_checkpoints("task-1").await, 1);

        // A fresh instance against the same directory must also see
        // nothing -- proves the deletion reached disk.
        let restarted =
            CheckpointManager::new_with_file_backend(CheckpointConfig::default(), &scratch.0);
        assert!(!restarted.has_checkpoint("task-1").await);
    }

    /// A successful save must not leave a temporary file behind: the
    /// atomic-write implementation renames the temp file over the
    /// destination rather than copying + separately deleting it.
    #[tokio::test]
    async fn test_file_backend_atomic_write_leaves_no_temp_file() {
        let scratch = ScratchDir::new("atomic-write");
        let manager =
            CheckpointManager::new_with_file_backend(CheckpointConfig::default(), &scratch.0);

        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![1, 2, 3]))
            .await
            .unwrap();

        let entries: Vec<_> = std::fs::read_dir(&scratch.0)
            .unwrap()
            .flatten()
            .map(|e| e.file_name().to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            entries.len(),
            1,
            "expected exactly one file, got {entries:?}"
        );
        assert!(entries[0].ends_with(".json"));
        assert!(!entries[0].contains(".tmp-"));
    }

    #[tokio::test]
    async fn test_file_backend_rehydrate_stats_after_restart() {
        let scratch = ScratchDir::new("rehydrate");

        {
            let manager =
                CheckpointManager::new_with_file_backend(CheckpointConfig::default(), &scratch.0);
            manager
                .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![1, 2, 3]))
                .await
                .unwrap();
            manager
                .save_checkpoint(Checkpoint::new("task-2".to_string(), vec![4, 5]))
                .await
                .unwrap();
        }

        let restarted =
            CheckpointManager::new_with_file_backend(CheckpointConfig::default(), &scratch.0);
        // Before rehydration, a fresh instance's own counters start at 0.
        assert_eq!(restarted.get_stats().await.current_count, 0);

        restarted.rehydrate_stats().await.unwrap();
        let stats = restarted.get_stats().await;
        assert_eq!(stats.current_count, 2);
        assert_eq!(stats.total_bytes, 5);
    }

    #[tokio::test]
    async fn test_file_backend_cleanup_expired_prunes_disk_state() {
        let scratch = ScratchDir::new("cleanup-expired");
        let config = CheckpointConfig::new().with_ttl(Duration::from_millis(0));
        let manager = CheckpointManager::new_with_file_backend(config, &scratch.0);

        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![1]))
            .await
            .unwrap();

        // TTL of 0 means the checkpoint is immediately expired the moment
        // `is_expired` is evaluated against "now".
        let removed = manager.cleanup_expired().await;
        assert_eq!(removed, 1);
        assert!(!manager.has_checkpoint("task-1").await);

        let restarted = CheckpointManager::new_with_file_backend(
            CheckpointConfig::new().with_ttl(Duration::from_millis(0)),
            &scratch.0,
        );
        assert!(!restarted.has_checkpoint("task-1").await);
    }

    #[tokio::test]
    async fn test_file_backend_clear_all_removes_directory_contents() {
        let scratch = ScratchDir::new("clear-all");
        let manager =
            CheckpointManager::new_with_file_backend(CheckpointConfig::default(), &scratch.0);

        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![1]))
            .await
            .unwrap();
        manager
            .save_checkpoint(Checkpoint::new("task-2".to_string(), vec![2]))
            .await
            .unwrap();

        manager.clear_all().await;

        assert!(!manager.has_checkpoint("task-1").await);
        assert!(!manager.has_checkpoint("task-2").await);
        // The directory itself may or may not still exist, but it must
        // contain no checkpoint files.
        if scratch.0.exists() {
            let remaining: Vec<_> = std::fs::read_dir(&scratch.0).unwrap().flatten().collect();
            assert!(remaining.is_empty());
        }
    }

    #[tokio::test]
    async fn test_memory_store_used_by_default_does_not_touch_disk() {
        // `CheckpointManager::new` must remain purely in-memory: this is
        // a behavioral contract (documented in the module docs), not
        // just an implementation detail.
        let manager = CheckpointManager::new(CheckpointConfig::default());
        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![1]))
            .await
            .unwrap();
        // No directory was ever specified, so there is nothing to assert
        // against on disk -- the meaningful assertion is simply that this
        // all works without any filesystem configuration.
        assert!(manager.has_checkpoint("task-1").await);
    }
}
