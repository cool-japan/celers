//! Dead Letter Queue (DLQ) support for handling poison messages
//!
//! This module provides functionality to handle tasks that fail permanently
//! after exhausting all retry attempts. Instead of discarding failed tasks,
//! they are moved to a Dead Letter Queue for later investigation and manual reprocessing.
//!
//! # Example
//!
//! ```rust
//! use celers_worker::dlq::{DlqConfig, DlqHandler};
//!
//! let config = DlqConfig {
//!     enabled: true,
//!     max_dlq_size: Some(10000),
//!     ttl_seconds: Some(604800), // 7 days
//!     queue_name: "my_app_dlq".to_string(),
//! };
//!
//! let handler = DlqHandler::new(config);
//! assert!(handler.is_enabled());
//! ```

use celers_core::{CelersError, Result, SerializedTask, TaskId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::SystemTime;
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Configuration for Dead Letter Queue
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqConfig {
    /// Enable DLQ functionality
    pub enabled: bool,

    /// Maximum number of messages in DLQ (None = unlimited)
    pub max_dlq_size: Option<usize>,

    /// Time-to-live for DLQ messages in seconds (None = never expire)
    pub ttl_seconds: Option<u64>,

    /// Name of the DLQ queue
    pub queue_name: String,
}

impl Default for DlqConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            max_dlq_size: None,
            ttl_seconds: Some(604800), // 7 days by default
            queue_name: "celery_dlq".to_string(),
        }
    }
}

impl DlqConfig {
    /// Create a new DLQ configuration
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            ..Default::default()
        }
    }

    /// Set the maximum DLQ size
    pub fn with_max_size(mut self, max_size: usize) -> Self {
        self.max_dlq_size = Some(max_size);
        self
    }

    /// Set the TTL for DLQ messages
    pub fn with_ttl(mut self, ttl_seconds: u64) -> Self {
        self.ttl_seconds = Some(ttl_seconds);
        self
    }

    /// Set the DLQ queue name
    pub fn with_queue_name(mut self, name: impl Into<String>) -> Self {
        self.queue_name = name.into();
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.enabled && self.queue_name.is_empty() {
            return Err("DLQ queue name cannot be empty when DLQ is enabled".to_string());
        }
        Ok(())
    }

    /// Check if DLQ is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Check if DLQ has size limit
    pub fn has_size_limit(&self) -> bool {
        self.max_dlq_size.is_some()
    }

    /// Check if DLQ has TTL configured
    pub fn has_ttl(&self) -> bool {
        self.ttl_seconds.is_some()
    }
}

/// Dead Letter Queue entry containing task and failure metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqEntry {
    /// The failed task
    pub task: SerializedTask,

    /// Task ID
    pub task_id: TaskId,

    /// Number of retry attempts made
    pub retry_count: u32,

    /// Error message from the last failure
    pub error_message: String,

    /// Timestamp when the task was moved to DLQ (Unix timestamp)
    pub dlq_timestamp: u64,

    /// Original enqueue timestamp (Unix timestamp)
    pub original_timestamp: u64,

    /// Worker hostname that processed the task
    pub worker_hostname: String,

    /// Additional metadata
    pub metadata: std::collections::HashMap<String, String>,
}

impl DlqEntry {
    /// Create a new DLQ entry
    pub fn new(
        task: SerializedTask,
        task_id: TaskId,
        retry_count: u32,
        error_message: String,
        worker_hostname: String,
    ) -> Self {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            task,
            task_id,
            retry_count,
            error_message,
            dlq_timestamp: now,
            original_timestamp: now,
            worker_hostname,
            metadata: std::collections::HashMap::new(),
        }
    }

    /// Add metadata to the entry
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Get the age of this entry in seconds
    pub fn age_seconds(&self) -> u64 {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now.saturating_sub(self.dlq_timestamp)
    }

    /// Check if this entry has expired based on TTL
    pub fn is_expired(&self, ttl_seconds: u64) -> bool {
        self.age_seconds() > ttl_seconds
    }
}

/// Statistics for the Dead Letter Queue
#[derive(Debug, Default, Clone)]
pub struct DlqStats {
    /// Total number of messages in DLQ
    pub total_messages: usize,

    /// Number of messages added since last reset
    pub messages_added: usize,

    /// Number of messages reprocessed successfully
    pub messages_reprocessed: usize,

    /// Number of messages removed (expired or manually deleted)
    pub messages_removed: usize,

    /// Number of messages that failed reprocessing
    pub reprocess_failures: usize,
}

impl DlqStats {
    /// Create new DLQ statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Reset counters (except total_messages)
    pub fn reset_counters(&mut self) {
        self.messages_added = 0;
        self.messages_reprocessed = 0;
        self.messages_removed = 0;
        self.reprocess_failures = 0;
    }
}

/// Dead Letter Queue handler
pub struct DlqHandler {
    config: DlqConfig,
    entries: Arc<RwLock<Vec<DlqEntry>>>,
    stats: Arc<RwLock<DlqStats>>,
}

impl DlqHandler {
    /// Create a new DLQ handler
    pub fn new(config: DlqConfig) -> Self {
        Self {
            config,
            entries: Arc::new(RwLock::new(Vec::new())),
            stats: Arc::new(RwLock::new(DlqStats::new())),
        }
    }

    /// Check if DLQ is enabled
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Add a failed task to the DLQ
    pub async fn add_entry(&self, entry: DlqEntry) -> Result<()> {
        if !self.config.enabled {
            debug!("DLQ is disabled, skipping entry for task {}", entry.task_id);
            return Ok(());
        }

        let mut entries = self.entries.write().await;
        let mut stats = self.stats.write().await;

        // Check size limit
        if let Some(max_size) = self.config.max_dlq_size {
            if entries.len() >= max_size {
                warn!(
                    "DLQ size limit ({}) reached, removing oldest entry",
                    max_size
                );
                entries.remove(0);
                stats.messages_removed += 1;
            }
        }

        info!(
            "Adding task {} to DLQ after {} retries: {}",
            entry.task_id, entry.retry_count, entry.error_message
        );

        entries.push(entry);
        stats.total_messages = entries.len();
        stats.messages_added += 1;

        Ok(())
    }

    /// Get all DLQ entries
    pub async fn get_entries(&self) -> Vec<DlqEntry> {
        self.entries.read().await.clone()
    }

    /// Get a specific DLQ entry by task ID
    pub async fn get_entry(&self, task_id: &TaskId) -> Option<DlqEntry> {
        let entries = self.entries.read().await;
        entries.iter().find(|e| &e.task_id == task_id).cloned()
    }

    /// Remove a specific entry by task ID
    pub async fn remove_entry(&self, task_id: &TaskId) -> Result<bool> {
        let mut entries = self.entries.write().await;
        let mut stats = self.stats.write().await;

        if let Some(pos) = entries.iter().position(|e| &e.task_id == task_id) {
            entries.remove(pos);
            stats.total_messages = entries.len();
            stats.messages_removed += 1;
            info!("Removed task {} from DLQ", task_id);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Clean up expired entries based on TTL
    pub async fn cleanup_expired(&self) -> Result<usize> {
        if let Some(ttl) = self.config.ttl_seconds {
            let mut entries = self.entries.write().await;
            let mut stats = self.stats.write().await;

            let before_count = entries.len();
            entries.retain(|e| !e.is_expired(ttl));
            let after_count = entries.len();
            let removed = before_count - after_count;

            if removed > 0 {
                info!("Removed {} expired entries from DLQ", removed);
                stats.total_messages = after_count;
                stats.messages_removed += removed;
            }

            Ok(removed)
        } else {
            Ok(0)
        }
    }

    /// Clear all DLQ entries
    pub async fn clear(&self) -> Result<usize> {
        let mut entries = self.entries.write().await;
        let mut stats = self.stats.write().await;

        let count = entries.len();
        entries.clear();
        stats.total_messages = 0;
        stats.messages_removed += count;

        info!("Cleared {} entries from DLQ", count);
        Ok(count)
    }

    /// Get DLQ statistics
    pub async fn get_stats(&self) -> DlqStats {
        self.stats.read().await.clone()
    }

    /// Get the current size of the DLQ
    pub async fn size(&self) -> usize {
        self.entries.read().await.len()
    }

    /// Record a successful reprocess
    pub async fn record_reprocess_success(&self) {
        let mut stats = self.stats.write().await;
        stats.messages_reprocessed += 1;
    }

    /// Record a failed reprocess
    pub async fn record_reprocess_failure(&self) {
        let mut stats = self.stats.write().await;
        stats.reprocess_failures += 1;
    }

    /// Get entries older than a certain age (in seconds)
    pub async fn get_entries_older_than(&self, age_seconds: u64) -> Vec<DlqEntry> {
        let entries = self.entries.read().await;
        entries
            .iter()
            .filter(|e| e.age_seconds() > age_seconds)
            .cloned()
            .collect()
    }

    /// Get entries for a specific task type
    pub async fn get_entries_by_task_name(&self, task_name: &str) -> Vec<DlqEntry> {
        let entries = self.entries.read().await;
        entries
            .iter()
            .filter(|e| e.task.metadata.name == task_name)
            .cloned()
            .collect()
    }

    /// Export DLQ entries to JSON
    pub async fn export_json(&self) -> Result<String> {
        let entries = self.entries.read().await;
        serde_json::to_string_pretty(&*entries)
            .map_err(|e| CelersError::Other(format!("Failed to serialize DLQ entries: {}", e)))
    }

    /// Import DLQ entries from JSON
    pub async fn import_json(&self, json: &str) -> Result<usize> {
        let imported: Vec<DlqEntry> = serde_json::from_str(json)
            .map_err(|e| CelersError::Other(format!("Failed to deserialize DLQ entries: {}", e)))?;

        let count = imported.len();
        let mut entries = self.entries.write().await;
        let mut stats = self.stats.write().await;

        entries.extend(imported);
        stats.total_messages = entries.len();

        info!("Imported {} entries into DLQ", count);
        Ok(count)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::TaskMetadata;

    fn create_test_task() -> SerializedTask {
        SerializedTask {
            metadata: TaskMetadata::new("test_task".to_string()),
            payload: vec![],
        }
    }

    #[test]
    fn test_dlq_config_default() {
        let config = DlqConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.queue_name, "celery_dlq");
        assert_eq!(config.ttl_seconds, Some(604800));
        assert_eq!(config.max_dlq_size, None);
    }

    #[test]
    fn test_dlq_config_builder() {
        let config = DlqConfig::new(true)
            .with_max_size(1000)
            .with_ttl(86400)
            .with_queue_name("my_dlq");

        assert!(config.enabled);
        assert_eq!(config.max_dlq_size, Some(1000));
        assert_eq!(config.ttl_seconds, Some(86400));
        assert_eq!(config.queue_name, "my_dlq");
    }

    #[test]
    fn test_dlq_config_validation() {
        let config = DlqConfig {
            enabled: true,
            queue_name: "".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = DlqConfig {
            enabled: true,
            queue_name: "valid_name".to_string(),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_dlq_config_predicates() {
        let config = DlqConfig {
            enabled: true,
            max_dlq_size: Some(100),
            ttl_seconds: Some(3600),
            ..Default::default()
        };

        assert!(config.is_enabled());
        assert!(config.has_size_limit());
        assert!(config.has_ttl());
    }

    #[test]
    fn test_dlq_entry_creation() {
        let task = create_test_task();
        let task_id = task.metadata.id;

        let entry = DlqEntry::new(
            task,
            task_id,
            3,
            "Test error".to_string(),
            "worker-1".to_string(),
        );

        assert_eq!(entry.task_id, task_id);
        assert_eq!(entry.retry_count, 3);
        assert_eq!(entry.error_message, "Test error");
        assert_eq!(entry.worker_hostname, "worker-1");
        assert!(entry.metadata.is_empty());
    }

    #[test]
    fn test_dlq_entry_with_metadata() {
        let task = create_test_task();
        let task_id = task.metadata.id;

        let entry = DlqEntry::new(
            task,
            task_id,
            3,
            "Test error".to_string(),
            "worker-1".to_string(),
        )
        .with_metadata("reason", "timeout")
        .with_metadata("duration", "300");

        assert_eq!(entry.metadata.len(), 2);
        assert_eq!(entry.metadata.get("reason"), Some(&"timeout".to_string()));
        assert_eq!(entry.metadata.get("duration"), Some(&"300".to_string()));
    }

    #[tokio::test]
    async fn test_dlq_handler_disabled() {
        let config = DlqConfig::new(false);
        let handler = DlqHandler::new(config);

        assert!(!handler.is_enabled());

        let task = create_test_task();
        let task_id = task.metadata.id;
        let entry = DlqEntry::new(
            task,
            task_id,
            3,
            "Error".to_string(),
            "worker-1".to_string(),
        );

        // Should succeed but not add entry
        assert!(handler.add_entry(entry).await.is_ok());
        assert_eq!(handler.size().await, 0);
    }

    #[tokio::test]
    async fn test_dlq_handler_add_entry() {
        let config = DlqConfig::new(true);
        let handler = DlqHandler::new(config);

        let task = create_test_task();
        let task_id = task.metadata.id;
        let entry = DlqEntry::new(
            task,
            task_id,
            3,
            "Error".to_string(),
            "worker-1".to_string(),
        );

        assert!(handler.add_entry(entry).await.is_ok());
        assert_eq!(handler.size().await, 1);

        let stats = handler.get_stats().await;
        assert_eq!(stats.total_messages, 1);
        assert_eq!(stats.messages_added, 1);
    }

    #[tokio::test]
    async fn test_dlq_handler_max_size() {
        let config = DlqConfig::new(true).with_max_size(2);
        let handler = DlqHandler::new(config);

        // Add 3 entries, oldest should be removed
        for i in 0..3 {
            let task = create_test_task();
            let task_id = task.metadata.id;
            let entry = DlqEntry::new(
                task,
                task_id,
                3,
                format!("Error {}", i),
                "worker-1".to_string(),
            );
            handler.add_entry(entry).await.unwrap();
        }

        assert_eq!(handler.size().await, 2);
        let stats = handler.get_stats().await;
        assert_eq!(stats.total_messages, 2);
        assert_eq!(stats.messages_added, 3);
        assert_eq!(stats.messages_removed, 1);
    }

    #[tokio::test]
    async fn test_dlq_handler_get_entry() {
        let config = DlqConfig::new(true);
        let handler = DlqHandler::new(config);

        let task = create_test_task();
        let task_id = task.metadata.id;
        let entry = DlqEntry::new(
            task,
            task_id,
            3,
            "Error".to_string(),
            "worker-1".to_string(),
        );

        handler.add_entry(entry).await.unwrap();

        let retrieved = handler.get_entry(&task_id).await;
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().task_id, task_id);

        let non_existent = handler.get_entry(&uuid::Uuid::new_v4()).await;
        assert!(non_existent.is_none());
    }

    #[tokio::test]
    async fn test_dlq_handler_remove_entry() {
        let config = DlqConfig::new(true);
        let handler = DlqHandler::new(config);

        let task = create_test_task();
        let task_id = task.metadata.id;
        let entry = DlqEntry::new(
            task,
            task_id,
            3,
            "Error".to_string(),
            "worker-1".to_string(),
        );

        handler.add_entry(entry).await.unwrap();
        assert_eq!(handler.size().await, 1);

        let removed = handler.remove_entry(&task_id).await.unwrap();
        assert!(removed);
        assert_eq!(handler.size().await, 0);

        let removed_again = handler.remove_entry(&task_id).await.unwrap();
        assert!(!removed_again);
    }

    #[tokio::test]
    async fn test_dlq_handler_clear() {
        let config = DlqConfig::new(true);
        let handler = DlqHandler::new(config);

        // Add 5 entries
        for _ in 0..5 {
            let task = create_test_task();
            let task_id = task.metadata.id;
            let entry = DlqEntry::new(
                task,
                task_id,
                3,
                "Error".to_string(),
                "worker-1".to_string(),
            );
            handler.add_entry(entry).await.unwrap();
        }

        assert_eq!(handler.size().await, 5);

        let cleared = handler.clear().await.unwrap();
        assert_eq!(cleared, 5);
        assert_eq!(handler.size().await, 0);
    }

    #[tokio::test]
    async fn test_dlq_handler_reprocess_tracking() {
        let config = DlqConfig::new(true);
        let handler = DlqHandler::new(config);

        handler.record_reprocess_success().await;
        handler.record_reprocess_success().await;
        handler.record_reprocess_failure().await;

        let stats = handler.get_stats().await;
        assert_eq!(stats.messages_reprocessed, 2);
        assert_eq!(stats.reprocess_failures, 1);
    }

    #[tokio::test]
    async fn test_dlq_handler_export_import() {
        let config = DlqConfig::new(true);
        let handler = DlqHandler::new(config);

        // Add entries
        for i in 0..3 {
            let task = create_test_task();
            let task_id = task.metadata.id;
            let entry = DlqEntry::new(
                task,
                task_id,
                3,
                format!("Error {}", i),
                "worker-1".to_string(),
            );
            handler.add_entry(entry).await.unwrap();
        }

        // Export
        let json = handler.export_json().await.unwrap();
        assert!(!json.is_empty());

        // Clear and import
        handler.clear().await.unwrap();
        assert_eq!(handler.size().await, 0);

        let imported = handler.import_json(&json).await.unwrap();
        assert_eq!(imported, 3);
        assert_eq!(handler.size().await, 3);
    }
}
