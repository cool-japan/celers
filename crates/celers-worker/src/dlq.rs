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

use celers_core::{Broker, CelersError, Result, SerializedTask, TaskId};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime};
use tokio::sync::RwLock;
use tokio::time;
use tracing::{debug, error, info, warn};

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

    /// Get entries that are candidates for automatic reprocessing
    ///
    /// Returns entries that:
    /// - Are not expired
    /// - Have age greater than min_age_seconds
    /// - Have retry count less than max_retries
    pub async fn get_reprocessable_entries(
        &self,
        min_age_seconds: u64,
        max_retries: u32,
    ) -> Vec<DlqEntry> {
        let entries = self.entries.read().await;
        let ttl = self.config.ttl_seconds;

        entries
            .iter()
            .filter(|e| {
                // Not expired
                if let Some(ttl_secs) = ttl {
                    if e.is_expired(ttl_secs) {
                        return false;
                    }
                }
                // Old enough to reprocess
                e.age_seconds() >= min_age_seconds && e.retry_count < max_retries
            })
            .cloned()
            .collect()
    }

    /// Remove entries in batch by task IDs
    pub async fn remove_entries_batch(&self, task_ids: &[TaskId]) -> Result<usize> {
        let mut entries = self.entries.write().await;
        let mut stats = self.stats.write().await;

        let before_count = entries.len();
        entries.retain(|e| !task_ids.contains(&e.task_id));
        let after_count = entries.len();
        let removed = before_count - after_count;

        if removed > 0 {
            stats.total_messages = after_count;
            stats.messages_removed += removed;
            info!("Removed {} entries from DLQ in batch", removed);
        }

        Ok(removed)
    }

    /// Get aggregated statistics by task name
    pub async fn get_stats_by_task_name(&self) -> std::collections::HashMap<String, TaskStats> {
        use std::collections::HashMap;

        let entries = self.entries.read().await;
        let mut task_stats: HashMap<String, TaskStats> = HashMap::new();

        for entry in entries.iter() {
            let name = entry.task.metadata.name.clone();
            let stats = task_stats.entry(name).or_default();

            stats.count += 1;
            stats.total_retries += entry.retry_count as usize;
            if entry.retry_count > stats.max_retries {
                stats.max_retries = entry.retry_count;
            }

            let age = entry.age_seconds();
            if age < stats.min_age_seconds {
                stats.min_age_seconds = age;
            }
            if age > stats.max_age_seconds {
                stats.max_age_seconds = age;
            }
        }

        task_stats
    }

    /// Check if DLQ is approaching capacity
    pub async fn is_near_capacity(&self, threshold_percent: f64) -> bool {
        if let Some(max_size) = self.config.max_dlq_size {
            let current_size = self.entries.read().await.len();
            let threshold = (max_size as f64 * threshold_percent).ceil() as usize;
            current_size >= threshold
        } else {
            false
        }
    }

    /// Get the oldest entry
    pub async fn get_oldest_entry(&self) -> Option<DlqEntry> {
        let entries = self.entries.read().await;
        entries.first().cloned()
    }

    /// Get the newest entry
    pub async fn get_newest_entry(&self) -> Option<DlqEntry> {
        let entries = self.entries.read().await;
        entries.last().cloned()
    }

    /// Compact the DLQ by removing duplicate task IDs, keeping only the newest
    pub async fn compact_duplicates(&self) -> Result<usize> {
        use std::collections::HashSet;

        let mut entries = self.entries.write().await;
        let mut stats = self.stats.write().await;
        let mut seen_ids = HashSet::new();
        let before_count = entries.len();

        // Keep only the last occurrence of each task ID
        entries.reverse();
        entries.retain(|e| seen_ids.insert(e.task_id));
        entries.reverse();

        let after_count = entries.len();
        let removed = before_count - after_count;

        if removed > 0 {
            stats.total_messages = after_count;
            stats.messages_removed += removed;
            info!("Compacted DLQ, removed {} duplicate entries", removed);
        }

        Ok(removed)
    }
}

/// Statistics for a specific task type in the DLQ
#[derive(Debug, Clone, Default)]
pub struct TaskStats {
    /// Number of entries for this task type
    pub count: usize,
    /// Total retry attempts across all entries
    pub total_retries: usize,
    /// Maximum retry count for any entry
    pub max_retries: u32,
    /// Minimum age in seconds
    pub min_age_seconds: u64,
    /// Maximum age in seconds
    pub max_age_seconds: u64,
}

impl TaskStats {
    /// Get the average retry count
    pub fn avg_retries(&self) -> f64 {
        if self.count > 0 {
            self.total_retries as f64 / self.count as f64
        } else {
            0.0
        }
    }

    /// Get the age range
    pub fn age_range_seconds(&self) -> u64 {
        self.max_age_seconds.saturating_sub(self.min_age_seconds)
    }
}

/// Configuration for automatic DLQ reprocessing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DlqReprocessConfig {
    /// Enable automatic reprocessing
    pub enabled: bool,

    /// Minimum age (in seconds) before a DLQ entry can be reprocessed
    pub min_age_seconds: u64,

    /// Maximum number of reprocessing attempts
    pub max_reprocess_attempts: u32,

    /// Interval (in seconds) between reprocessing checks
    pub check_interval_seconds: u64,

    /// Maximum number of entries to reprocess per batch
    pub batch_size: usize,

    /// Backoff multiplier for retry delays (exponential backoff)
    pub backoff_multiplier: f64,

    /// Maximum delay (in seconds) between reprocessing attempts
    pub max_delay_seconds: u64,
}

impl Default for DlqReprocessConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            min_age_seconds: 300, // 5 minutes
            max_reprocess_attempts: 3,
            check_interval_seconds: 60, // 1 minute
            batch_size: 10,
            backoff_multiplier: 2.0,
            max_delay_seconds: 3600, // 1 hour
        }
    }
}

impl DlqReprocessConfig {
    /// Create a new reprocess configuration
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            ..Default::default()
        }
    }

    /// Set the minimum age for reprocessing
    pub fn with_min_age(mut self, min_age_seconds: u64) -> Self {
        self.min_age_seconds = min_age_seconds;
        self
    }

    /// Set the maximum reprocess attempts
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_reprocess_attempts = max_attempts;
        self
    }

    /// Set the check interval
    pub fn with_check_interval(mut self, interval_seconds: u64) -> Self {
        self.check_interval_seconds = interval_seconds;
        self
    }

    /// Set the batch size
    pub fn with_batch_size(mut self, batch_size: usize) -> Self {
        self.batch_size = batch_size;
        self
    }

    /// Set the backoff multiplier
    pub fn with_backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.backoff_multiplier = multiplier;
        self
    }

    /// Set the maximum delay
    pub fn with_max_delay(mut self, max_delay_seconds: u64) -> Self {
        self.max_delay_seconds = max_delay_seconds;
        self
    }

    /// Validate the configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.enabled {
            if self.min_age_seconds == 0 {
                return Err("min_age_seconds must be greater than 0".to_string());
            }
            if self.max_reprocess_attempts == 0 {
                return Err("max_reprocess_attempts must be greater than 0".to_string());
            }
            if self.check_interval_seconds == 0 {
                return Err("check_interval_seconds must be greater than 0".to_string());
            }
            if self.batch_size == 0 {
                return Err("batch_size must be greater than 0".to_string());
            }
            if self.backoff_multiplier <= 0.0 {
                return Err("backoff_multiplier must be greater than 0".to_string());
            }
            if self.max_delay_seconds == 0 {
                return Err("max_delay_seconds must be greater than 0".to_string());
            }
        }
        Ok(())
    }

    /// Check if reprocessing is enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Calculate the delay before next reprocessing attempt
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        let base_delay = self.min_age_seconds as f64;
        let delay = base_delay * self.backoff_multiplier.powi(attempt as i32);
        let delay_secs = delay.min(self.max_delay_seconds as f64) as u64;
        Duration::from_secs(delay_secs)
    }
}

/// Automatic DLQ reprocessor
pub struct DlqReprocessor<B: Broker> {
    config: DlqReprocessConfig,
    handler: Arc<DlqHandler>,
    broker: Arc<B>,
    running: Arc<RwLock<bool>>,
}

impl<B: Broker + 'static> DlqReprocessor<B> {
    /// Create a new DLQ reprocessor
    pub fn new(config: DlqReprocessConfig, handler: Arc<DlqHandler>, broker: Arc<B>) -> Self {
        Self {
            config,
            handler,
            broker,
            running: Arc::new(RwLock::new(false)),
        }
    }

    /// Start the automatic reprocessing loop
    pub async fn start(&self) -> Result<()> {
        if !self.config.enabled {
            debug!("DLQ reprocessing is disabled");
            return Ok(());
        }

        let mut running = self.running.write().await;
        if *running {
            warn!("DLQ reprocessor is already running");
            return Ok(());
        }
        *running = true;
        drop(running);

        info!(
            "Starting DLQ reprocessor with check interval: {}s, batch size: {}",
            self.config.check_interval_seconds, self.config.batch_size
        );

        let config = self.config.clone();
        let handler = self.handler.clone();
        let broker = self.broker.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            let mut interval = time::interval(Duration::from_secs(config.check_interval_seconds));

            loop {
                interval.tick().await;

                if !*running.read().await {
                    info!("DLQ reprocessor stopped");
                    break;
                }

                // Get reprocessable entries
                let entries = handler
                    .get_reprocessable_entries(
                        config.min_age_seconds,
                        config.max_reprocess_attempts,
                    )
                    .await;

                if entries.is_empty() {
                    debug!("No DLQ entries to reprocess");
                    continue;
                }

                info!(
                    "Found {} DLQ entries eligible for reprocessing",
                    entries.len()
                );

                // Process in batches
                let batch_size = config.batch_size;
                for chunk in entries.chunks(batch_size) {
                    for entry in chunk {
                        match Self::reprocess_entry(&broker, &handler, entry, &config).await {
                            Ok(true) => {
                                info!("Successfully reprocessed task {}", entry.task_id);
                                handler.record_reprocess_success().await;
                                // Remove from DLQ on success
                                let _ = handler.remove_entry(&entry.task_id).await;
                            }
                            Ok(false) => {
                                debug!("Task {} requeued for later retry", entry.task_id);
                            }
                            Err(e) => {
                                error!("Failed to reprocess task {}: {}", entry.task_id, e);
                                handler.record_reprocess_failure().await;
                            }
                        }
                    }

                    // Small delay between batches to avoid overwhelming the system
                    if !*running.read().await {
                        break;
                    }
                    time::sleep(Duration::from_millis(100)).await;
                }
            }
        });

        Ok(())
    }

    /// Stop the automatic reprocessing loop
    pub async fn stop(&self) {
        let mut running = self.running.write().await;
        *running = false;
        info!("Stopping DLQ reprocessor");
    }

    /// Check if the reprocessor is running
    pub async fn is_running(&self) -> bool {
        *self.running.read().await
    }

    /// Reprocess a single DLQ entry
    async fn reprocess_entry(
        broker: &Arc<B>,
        handler: &Arc<DlqHandler>,
        entry: &DlqEntry,
        config: &DlqReprocessConfig,
    ) -> Result<bool> {
        // Check if entry has exceeded max reprocess attempts
        if entry.retry_count >= config.max_reprocess_attempts {
            warn!(
                "Task {} has exceeded max reprocess attempts ({})",
                entry.task_id, config.max_reprocess_attempts
            );
            return Ok(false);
        }

        // Calculate backoff delay
        let delay = config.calculate_delay(entry.retry_count);
        let age = Duration::from_secs(entry.age_seconds());

        if age < delay {
            debug!(
                "Task {} not ready for reprocessing yet (age: {:?}, required: {:?})",
                entry.task_id, age, delay
            );
            return Ok(false);
        }

        info!(
            "Reprocessing task {} (attempt {}/{})",
            entry.task_id,
            entry.retry_count + 1,
            config.max_reprocess_attempts
        );

        // Re-enqueue the task to the broker
        match broker.enqueue(entry.task.clone()).await {
            Ok(_) => {
                info!("Task {} successfully re-enqueued", entry.task_id);
                Ok(true)
            }
            Err(e) => {
                error!("Failed to re-enqueue task {}: {}", entry.task_id, e);

                // Update the entry with new retry count and error
                let mut updated_entry = entry.clone();
                updated_entry.retry_count += 1;
                updated_entry.error_message = format!("Reprocess failed: {}", e);

                // Remove old entry and add updated one
                let _ = handler.remove_entry(&entry.task_id).await;
                let _ = handler.add_entry(updated_entry).await;

                Err(e)
            }
        }
    }

    /// Manually trigger a reprocessing check
    pub async fn trigger_reprocess(&self) -> Result<usize> {
        if !self.config.enabled {
            return Err(CelersError::Other(
                "DLQ reprocessing is disabled".to_string(),
            ));
        }

        let entries = self
            .handler
            .get_reprocessable_entries(
                self.config.min_age_seconds,
                self.config.max_reprocess_attempts,
            )
            .await;

        let total = entries.len();
        let mut reprocessed = 0;

        for entry in entries {
            match Self::reprocess_entry(&self.broker, &self.handler, &entry, &self.config).await {
                Ok(true) => {
                    reprocessed += 1;
                    self.handler.record_reprocess_success().await;
                    let _ = self.handler.remove_entry(&entry.task_id).await;
                }
                Ok(false) => {}
                Err(_) => {
                    self.handler.record_reprocess_failure().await;
                }
            }
        }

        info!(
            "Manual reprocess triggered: {}/{} tasks successfully reprocessed",
            reprocessed, total
        );
        Ok(reprocessed)
    }

    /// Get the reprocessing configuration
    pub fn config(&self) -> &DlqReprocessConfig {
        &self.config
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

    #[test]
    fn test_dlq_reprocess_config_default() {
        let config = DlqReprocessConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.min_age_seconds, 300);
        assert_eq!(config.max_reprocess_attempts, 3);
        assert_eq!(config.check_interval_seconds, 60);
        assert_eq!(config.batch_size, 10);
        assert_eq!(config.backoff_multiplier, 2.0);
        assert_eq!(config.max_delay_seconds, 3600);
    }

    #[test]
    fn test_dlq_reprocess_config_builder() {
        let config = DlqReprocessConfig::new(true)
            .with_min_age(600)
            .with_max_attempts(5)
            .with_check_interval(120)
            .with_batch_size(20)
            .with_backoff_multiplier(1.5)
            .with_max_delay(7200);

        assert!(config.enabled);
        assert_eq!(config.min_age_seconds, 600);
        assert_eq!(config.max_reprocess_attempts, 5);
        assert_eq!(config.check_interval_seconds, 120);
        assert_eq!(config.batch_size, 20);
        assert_eq!(config.backoff_multiplier, 1.5);
        assert_eq!(config.max_delay_seconds, 7200);
    }

    #[test]
    fn test_dlq_reprocess_config_validation() {
        let mut config = DlqReprocessConfig::new(true);
        assert!(config.validate().is_ok());

        config.min_age_seconds = 0;
        assert!(config.validate().is_err());

        config = DlqReprocessConfig::new(true);
        config.max_reprocess_attempts = 0;
        assert!(config.validate().is_err());

        config = DlqReprocessConfig::new(true);
        config.check_interval_seconds = 0;
        assert!(config.validate().is_err());

        config = DlqReprocessConfig::new(true);
        config.batch_size = 0;
        assert!(config.validate().is_err());

        config = DlqReprocessConfig::new(true);
        config.backoff_multiplier = 0.0;
        assert!(config.validate().is_err());

        config = DlqReprocessConfig::new(true);
        config.max_delay_seconds = 0;
        assert!(config.validate().is_err());

        // Disabled config should always be valid
        let config = DlqReprocessConfig::new(false);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_dlq_reprocess_config_calculate_delay() {
        let config = DlqReprocessConfig::new(true)
            .with_min_age(100)
            .with_backoff_multiplier(2.0)
            .with_max_delay(1000);

        // First attempt: 100 * 2^0 = 100
        let delay = config.calculate_delay(0);
        assert_eq!(delay.as_secs(), 100);

        // Second attempt: 100 * 2^1 = 200
        let delay = config.calculate_delay(1);
        assert_eq!(delay.as_secs(), 200);

        // Third attempt: 100 * 2^2 = 400
        let delay = config.calculate_delay(2);
        assert_eq!(delay.as_secs(), 400);

        // Fourth attempt: 100 * 2^3 = 800
        let delay = config.calculate_delay(3);
        assert_eq!(delay.as_secs(), 800);

        // Fifth attempt: 100 * 2^4 = 1600, capped at 1000
        let delay = config.calculate_delay(4);
        assert_eq!(delay.as_secs(), 1000);
    }

    #[test]
    fn test_dlq_reprocess_config_predicates() {
        let config = DlqReprocessConfig::new(true);
        assert!(config.is_enabled());

        let config = DlqReprocessConfig::new(false);
        assert!(!config.is_enabled());
    }
}
