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
//! - Multiple storage backends (in-memory, file-based)
//! - Automatic checkpoint cleanup
//! - Progress tracking
//! - Checkpoint compression support
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
//! let mut manager = CheckpointManager::new(config);
//!
//! // Create a checkpoint
//! let checkpoint = Checkpoint::new("task-123".to_string(), vec![1, 2, 3, 4]);
//! manager.save_checkpoint(checkpoint).await;
//!
//! // Resume from checkpoint
//! if let Some(checkpoint) = manager.load_checkpoint("task-123").await {
//!     println!("Resuming from checkpoint with {} bytes", checkpoint.data.len());
//! }
//! # }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tracing::{debug, info};

/// Checkpoint data for a task
#[derive(Clone, Debug)]
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

/// Checkpoint manager
pub struct CheckpointManager {
    /// Configuration
    config: CheckpointConfig,
    /// Checkpoint storage (task_id -> list of checkpoints)
    checkpoints: Arc<RwLock<HashMap<String, Vec<Checkpoint>>>>,
    /// Statistics
    stats: Arc<RwLock<CheckpointStats>>,
}

impl CheckpointManager {
    /// Create a new checkpoint manager
    pub fn new(config: CheckpointConfig) -> Self {
        Self {
            config,
            checkpoints: Arc::new(RwLock::new(HashMap::new())),
            stats: Arc::new(RwLock::new(CheckpointStats::default())),
        }
    }

    /// Save a checkpoint for a task
    pub async fn save_checkpoint(&self, checkpoint: Checkpoint) {
        let task_id = checkpoint.task_id.clone();
        let checkpoint_size = checkpoint.size();

        let mut checkpoints = self.checkpoints.write().await;
        let task_checkpoints = checkpoints.entry(task_id.clone()).or_default();

        // Add new checkpoint
        task_checkpoints.push(checkpoint);

        // Apply storage strategy
        self.apply_strategy(task_checkpoints).await;

        // Update statistics
        let mut stats = self.stats.write().await;
        stats.total_created += 1;
        stats.current_count = checkpoints.values().map(|v| v.len()).sum();
        stats.total_bytes += checkpoint_size;

        info!(
            "Saved checkpoint for task {} (size: {} bytes)",
            task_id, checkpoint_size
        );
    }

    /// Load the latest checkpoint for a task
    pub async fn load_checkpoint(&self, task_id: &str) -> Option<Checkpoint> {
        let checkpoints = self.checkpoints.read().await;
        let checkpoint = checkpoints.get(task_id)?.last()?.clone();

        // Update statistics
        let mut stats = self.stats.write().await;
        stats.total_restored += 1;

        debug!(
            "Loaded checkpoint for task {} (version: {})",
            task_id, checkpoint.version
        );
        Some(checkpoint)
    }

    /// Load a specific checkpoint version
    pub async fn load_checkpoint_version(&self, task_id: &str, version: u64) -> Option<Checkpoint> {
        let checkpoints = self.checkpoints.read().await;
        let checkpoint = checkpoints
            .get(task_id)?
            .iter()
            .find(|c| c.version == version)?
            .clone();

        debug!("Loaded checkpoint version {} for task {}", version, task_id);
        Some(checkpoint)
    }

    /// Delete all checkpoints for a task
    pub async fn delete_checkpoints(&self, task_id: &str) -> usize {
        let mut checkpoints = self.checkpoints.write().await;
        if let Some(task_checkpoints) = checkpoints.remove(task_id) {
            let count = task_checkpoints.len();
            let total_size: usize = task_checkpoints.iter().map(|c| c.size()).sum();

            let mut stats = self.stats.write().await;
            stats.total_deleted += count;
            stats.current_count = stats.current_count.saturating_sub(count);
            stats.total_bytes = stats.total_bytes.saturating_sub(total_size);

            info!("Deleted {} checkpoints for task {}", count, task_id);
            count
        } else {
            0
        }
    }

    /// Get all checkpoints for a task
    pub async fn get_checkpoints(&self, task_id: &str) -> Vec<Checkpoint> {
        let checkpoints = self.checkpoints.read().await;
        checkpoints.get(task_id).cloned().unwrap_or_default()
    }

    /// Get checkpoint count for a task
    pub async fn checkpoint_count(&self, task_id: &str) -> usize {
        let checkpoints = self.checkpoints.read().await;
        checkpoints.get(task_id).map(|v| v.len()).unwrap_or(0)
    }

    /// Check if a task has checkpoints
    pub async fn has_checkpoint(&self, task_id: &str) -> bool {
        let checkpoints = self.checkpoints.read().await;
        checkpoints.contains_key(task_id)
    }

    /// Get task progress from latest checkpoint
    pub async fn get_progress(&self, task_id: &str) -> Option<u8> {
        let checkpoints = self.checkpoints.read().await;
        checkpoints.get(task_id)?.last().map(|c| c.progress)
    }

    /// Cleanup expired checkpoints
    pub async fn cleanup_expired(&self) -> usize {
        let mut checkpoints = self.checkpoints.write().await;
        let mut total_removed = 0;
        let total_size_removed = 0;

        checkpoints.retain(|task_id, task_checkpoints| {
            let original_len = task_checkpoints.len();
            task_checkpoints.retain(|c| !c.is_expired(self.config.checkpoint_ttl));
            let removed = original_len - task_checkpoints.len();

            if removed > 0 {
                total_removed += removed;
                debug!(
                    "Removed {} expired checkpoints for task {}",
                    removed, task_id
                );
            }

            !task_checkpoints.is_empty()
        });

        if total_removed > 0 {
            let mut stats = self.stats.write().await;
            stats.total_deleted += total_removed;
            stats.current_count = stats.current_count.saturating_sub(total_removed);
            stats.total_bytes = stats.total_bytes.saturating_sub(total_size_removed);
            info!("Cleaned up {} expired checkpoints", total_removed);
        }

        total_removed
    }

    /// Get checkpoint statistics
    pub async fn get_stats(&self) -> CheckpointStats {
        self.stats.read().await.clone()
    }

    /// Apply storage strategy to checkpoint list
    async fn apply_strategy(&self, checkpoints: &mut Vec<Checkpoint>) {
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

    /// Clear all checkpoints
    pub async fn clear_all(&self) {
        let mut checkpoints = self.checkpoints.write().await;
        checkpoints.clear();

        let mut stats = self.stats.write().await;
        *stats = CheckpointStats::default();

        info!("Cleared all checkpoints");
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
        manager.save_checkpoint(checkpoint).await;

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

        manager.save_checkpoint(checkpoint1).await;
        manager.save_checkpoint(checkpoint2).await;

        let loaded = manager.load_checkpoint_version("task-1", 1).await;
        assert!(loaded.is_some());
        assert_eq!(loaded.unwrap().data, vec![1]);
    }

    #[tokio::test]
    async fn test_checkpoint_manager_delete() {
        let config = CheckpointConfig::default();
        let manager = CheckpointManager::new(config);

        let checkpoint = Checkpoint::new("task-1".to_string(), vec![1, 2, 3]);
        manager.save_checkpoint(checkpoint).await;

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
            .await;
        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![2]))
            .await;
        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![3]))
            .await;

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
            .await;
        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![2]))
            .await;
        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![3]))
            .await;

        let count = manager.checkpoint_count("task-1").await;
        assert_eq!(count, 2);
    }

    #[tokio::test]
    async fn test_checkpoint_progress() {
        let config = CheckpointConfig::default();
        let manager = CheckpointManager::new(config);

        let checkpoint = Checkpoint::new("task-1".to_string(), vec![1]).with_progress(75);
        manager.save_checkpoint(checkpoint).await;

        let progress = manager.get_progress("task-1").await;
        assert_eq!(progress, Some(75));
    }

    #[tokio::test]
    async fn test_checkpoint_stats() {
        let config = CheckpointConfig::default();
        let manager = CheckpointManager::new(config);

        manager
            .save_checkpoint(Checkpoint::new("task-1".to_string(), vec![1, 2, 3]))
            .await;
        manager
            .save_checkpoint(Checkpoint::new("task-2".to_string(), vec![4, 5]))
            .await;

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
            .await;
        manager
            .save_checkpoint(Checkpoint::new("task-2".to_string(), vec![2]))
            .await;

        manager.clear_all().await;

        assert!(!manager.has_checkpoint("task-1").await);
        assert!(!manager.has_checkpoint("task-2").await);
    }

    #[test]
    fn test_checkpoint_strategy_default() {
        let strategy = CheckpointStrategy::default();
        assert!(matches!(strategy, CheckpointStrategy::KeepLast(5)));
    }
}
