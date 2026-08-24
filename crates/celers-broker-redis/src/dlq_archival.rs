//! DLQ Archival and Retention
//!
//! Provides automatic archival and retention management for Dead Letter Queue (DLQ) tasks:
//! - Archive old DLQ items to cheaper storage
//! - Configurable retention policies
//! - DLQ compression for long-term storage
//! - Archive search and retrieval
//!
//! # Features
//!
//! - **Multiple Storage Backends**: File-based, Redis-based with compression, or external storage
//! - **Retention Policies**: Age-based, count-based, or storage size-based retention
//! - **Automatic Cleanup**: Scheduled cleanup based on policies
//! - **Archive Search**: Query archived tasks
//! - **Archive Restoration**: Restore archived tasks back to DLQ
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::dlq_archival::{
//!     DLQArchivalManager, ArchivalConfig, RetentionPolicy, StorageBackend
//! };
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Configure archival with file-based storage
//! let config = ArchivalConfig::default()
//!     .with_backend(StorageBackend::FileSystem {
//!         path: "/var/lib/celers/dlq_archive".to_string()
//!     })
//!     .with_retention_policy(RetentionPolicy::ByAge {
//!         max_age_secs: 30 * 24 * 3600, // 30 days
//!     })
//!     .with_compression(true);
//!
//! let manager = DLQArchivalManager::new("redis://localhost:6379", "my_queue", config).await?;
//!
//! // Archive tasks older than 7 days
//! let archived = manager.archive_old_tasks(7 * 24 * 3600).await?;
//! println!("Archived {} tasks", archived);
//!
//! // Apply retention policy to clean up old archives
//! let deleted = manager.apply_retention_policy().await?;
//! println!("Deleted {} old archive entries", deleted);
//! # Ok(())
//! # }
//! ```

use crate::connection::RedisClientExt;
use celers_core::{CelersError, Result, SerializedTask};
use oxihttp_client::{Client as HttpClient, HttpsClient};
use redis::{AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use tracing::{debug, info, warn};

/// DLQ archival and retention manager
#[derive(Debug, Clone)]
pub struct DLQArchivalManager {
    client: Client,
    queue_name: String,
    dlq_key: String,
    config: ArchivalConfig,
    http_client: HttpsClient,
}

/// Archival configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivalConfig {
    /// Storage backend for archives
    pub backend: StorageBackend,
    /// Retention policy
    pub retention_policy: RetentionPolicy,
    /// Enable compression for archived tasks
    pub enable_compression: bool,
    /// Archive key prefix in Redis
    pub archive_prefix: String,
    /// Maximum tasks per archive batch
    pub batch_size: usize,
}

/// Storage backend for archived tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageBackend {
    /// File system storage
    FileSystem {
        /// Base path for archive files
        path: String,
    },
    /// Redis-based storage with compression
    Redis {
        /// Archive key prefix
        key_prefix: String,
    },
    /// External storage (S3-compatible, etc.)
    External {
        /// Storage endpoint URL
        endpoint: String,
        /// Bucket/container name
        bucket: String,
    },
}

/// Retention policy for archives
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RetentionPolicy {
    /// Retain archives for a maximum age
    ByAge {
        /// Maximum age in seconds
        max_age_secs: u64,
    },
    /// Retain a maximum number of archived tasks
    ByCount {
        /// Maximum number of tasks
        max_count: usize,
    },
    /// Retain archives up to a maximum storage size
    BySize {
        /// Maximum size in bytes
        max_size_bytes: u64,
    },
    /// Combined policy (all conditions must be met)
    Combined {
        /// Maximum age in seconds
        max_age_secs: Option<u64>,
        /// Maximum number of tasks
        max_count: Option<usize>,
        /// Maximum size in bytes
        max_size_bytes: Option<u64>,
    },
    /// No retention limits (archive forever)
    Unlimited,
}

/// Archived task metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchivedTask {
    /// Original task
    pub task: SerializedTask,
    /// Archive timestamp (Unix timestamp in seconds)
    pub archived_at: i64,
    /// Archive ID
    pub archive_id: String,
    /// Compressed size (if compression was used)
    pub compressed_size: Option<usize>,
    /// Original DLQ timestamp
    pub dlq_timestamp: Option<i64>,
}

/// Archive statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveStats {
    /// Total archived tasks
    pub total_tasks: usize,
    /// Total storage size in bytes
    pub total_size_bytes: u64,
    /// Oldest archive timestamp
    pub oldest_timestamp: Option<i64>,
    /// Newest archive timestamp
    pub newest_timestamp: Option<i64>,
    /// Compression ratio (if compression enabled)
    pub compression_ratio: Option<f64>,
}

/// Archive search criteria
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ArchiveSearchCriteria {
    /// Task name pattern
    pub task_name: Option<String>,
    /// Minimum archive timestamp
    pub min_timestamp: Option<i64>,
    /// Maximum archive timestamp
    pub max_timestamp: Option<i64>,
    /// Archive ID
    pub archive_id: Option<String>,
    /// Maximum results to return
    pub limit: Option<usize>,
}

impl Default for ArchivalConfig {
    fn default() -> Self {
        Self {
            backend: StorageBackend::Redis {
                key_prefix: "archive".to_string(),
            },
            retention_policy: RetentionPolicy::ByAge {
                max_age_secs: 30 * 24 * 3600, // 30 days
            },
            enable_compression: true,
            archive_prefix: "archive".to_string(),
            batch_size: 100,
        }
    }
}

impl ArchivalConfig {
    /// Set storage backend
    pub fn with_backend(mut self, backend: StorageBackend) -> Self {
        self.backend = backend;
        self
    }

    /// Set retention policy
    pub fn with_retention_policy(mut self, policy: RetentionPolicy) -> Self {
        self.retention_policy = policy;
        self
    }

    /// Enable or disable compression
    pub fn with_compression(mut self, enable: bool) -> Self {
        self.enable_compression = enable;
        self
    }

    /// Set archive key prefix
    pub fn with_archive_prefix(mut self, prefix: &str) -> Self {
        self.archive_prefix = prefix.to_string();
        self
    }

    /// Set batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.batch_size = size;
        self
    }
}

impl DLQArchivalManager {
    /// Create a new DLQ archival manager
    pub async fn new(redis_url: &str, queue_name: &str, config: ArchivalConfig) -> Result<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to Redis: {}", e)))?;

        let http_client = HttpClient::builder()
            .with_webpki_roots()
            .build_https()
            .map_err(|e| CelersError::Broker(format!("Failed to build HTTP client: {}", e)))?;

        Ok(Self {
            client,
            queue_name: queue_name.to_string(),
            dlq_key: format!("{}:dlq", queue_name),
            config,
            http_client,
        })
    }

    /// Archive tasks older than the specified age (in seconds).
    ///
    /// A task's age is measured from `task.metadata.updated_at` — the
    /// closest real, non-fabricated timestamp available on a DLQ entry.
    /// (`RedisBroker::reject()` currently pushes the task's existing
    /// serialized handle onto the DLQ verbatim, without stamping a dedicated
    /// "entered DLQ" timestamp — see the followup recorded against this
    /// function for the ideal fix. Registering
    /// [`crate::hooks::TimestampEnrichmentHook`] on the enqueue path keeps
    /// `updated_at` fresh, which improves this proxy's accuracy.) Tasks
    /// younger than `min_age_secs` are left untouched in the DLQ.
    ///
    /// Archiving one task and removing it from the DLQ are two separate
    /// Redis calls and cannot be made atomic without a DLQ-side Lua script
    /// (out of scope for a per-backend archiver that may write to the
    /// filesystem or an external HTTP store). Instead this loop is made
    /// *idempotent* and *fault-tolerant*: `archive_id` is derived
    /// deterministically from the task id and its DLQ-entry timestamp, so
    /// re-archiving the same entry after a partial failure overwrites the
    /// same backend record rather than creating a duplicate, and a failure
    /// on any single entry is logged and skipped rather than aborting the
    /// whole batch (leaving already-processed entries duplicated or lost).
    pub async fn archive_old_tasks(&self, min_age_secs: u64) -> Result<usize> {
        let mut conn = self
            .client
            .celers_multiplexed_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Connection error: {}", e)))?;

        // Get all tasks from DLQ
        let tasks: Vec<String> = conn
            .lrange(&self.dlq_key, 0, -1)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to read DLQ: {}", e)))?;

        let cutoff_timestamp = chrono::Utc::now().timestamp() - min_age_secs as i64;
        let mut archived_count = 0;
        let mut error_count = 0;

        for task_data in tasks {
            let task = match serde_json::from_str::<SerializedTask>(&task_data) {
                Ok(task) => task,
                Err(e) => {
                    warn!("Skipping unparseable DLQ entry during archival: {}", e);
                    continue;
                }
            };

            // The task's own `updated_at` is the best real signal available
            // for "when did this enter the DLQ" (see the doc comment above).
            let entry_timestamp = task.metadata.updated_at.timestamp();
            if entry_timestamp > cutoff_timestamp {
                // Too young: leave it in the DLQ for a future run.
                continue;
            }

            let archive_id = self.deterministic_archive_id(&task, entry_timestamp);
            let archived_task = ArchivedTask {
                task: task.clone(),
                archived_at: chrono::Utc::now().timestamp(),
                archive_id: archive_id.clone(),
                compressed_size: None,
                dlq_timestamp: Some(entry_timestamp),
            };

            // Store to backend
            if let Err(e) = self.store_to_backend(&archived_task).await {
                warn!(
                    "Failed to archive DLQ task {} (archive_id {}): {} — leaving it in the DLQ, will retry next run",
                    task.metadata.id, archive_id, e
                );
                error_count += 1;
                continue;
            }

            // Remove from DLQ. If this fails, the entry stays in the DLQ but
            // the archive record is already written under a deterministic
            // `archive_id`, so the next run's `store_to_backend` overwrites
            // the same record instead of creating a duplicate archive.
            match conn.lrem::<_, _, i64>(&self.dlq_key, 1, &task_data).await {
                Ok(_) => archived_count += 1,
                Err(e) => {
                    warn!(
                        "Archived DLQ task {} (archive_id {}) but failed to remove it from the DLQ: {} — safe to retry",
                        task.metadata.id, archive_id, e
                    );
                    error_count += 1;
                }
            }
        }

        if error_count > 0 {
            warn!(
                "archive_old_tasks for DLQ '{}' completed with {} error(s); {} tasks archived successfully",
                self.queue_name, error_count, archived_count
            );
        }

        info!(
            "Archived {} tasks from DLQ '{}' older than {} seconds",
            archived_count, self.queue_name, min_age_secs
        );

        Ok(archived_count)
    }

    /// Apply retention policy to clean up old archives
    pub async fn apply_retention_policy(&self) -> Result<usize> {
        match &self.config.retention_policy {
            RetentionPolicy::ByAge { max_age_secs } => {
                self.delete_archives_by_age(*max_age_secs).await
            }
            RetentionPolicy::ByCount { max_count } => {
                self.delete_archives_by_count(*max_count).await
            }
            RetentionPolicy::BySize { max_size_bytes } => {
                self.delete_archives_by_size(*max_size_bytes).await
            }
            RetentionPolicy::Combined {
                max_age_secs,
                max_count,
                max_size_bytes,
            } => {
                let mut total_deleted = 0;
                if let Some(age) = max_age_secs {
                    total_deleted += self.delete_archives_by_age(*age).await?;
                }
                if let Some(count) = max_count {
                    total_deleted += self.delete_archives_by_count(*count).await?;
                }
                if let Some(size) = max_size_bytes {
                    total_deleted += self.delete_archives_by_size(*size).await?;
                }
                Ok(total_deleted)
            }
            RetentionPolicy::Unlimited => Ok(0),
        }
    }

    /// Get archive statistics
    pub async fn get_archive_stats(&self) -> Result<ArchiveStats> {
        match &self.config.backend {
            StorageBackend::Redis { key_prefix } => self.get_redis_archive_stats(key_prefix).await,
            StorageBackend::FileSystem { path } => self.get_filesystem_archive_stats(path).await,
            StorageBackend::External { endpoint, bucket } => {
                let objects = self.list_external_objects(endpoint, bucket).await?;
                let total_tasks = objects.len();
                let total_size_bytes: u64 = objects.iter().map(|o| o.size).sum();
                let timestamps: Vec<i64> = objects
                    .iter()
                    .filter_map(|o| o.last_modified.as_deref())
                    .filter_map(|ts| chrono::DateTime::parse_from_rfc3339(ts).ok())
                    .map(|dt| dt.timestamp())
                    .collect();
                let oldest_timestamp = timestamps.iter().copied().min();
                let newest_timestamp = timestamps.iter().copied().max();
                Ok(ArchiveStats {
                    total_tasks,
                    total_size_bytes,
                    oldest_timestamp,
                    newest_timestamp,
                    compression_ratio: None,
                })
            }
        }
    }

    /// Search archived tasks
    pub async fn search_archives(
        &self,
        criteria: ArchiveSearchCriteria,
    ) -> Result<Vec<ArchivedTask>> {
        match &self.config.backend {
            StorageBackend::Redis { key_prefix } => {
                self.search_redis_archives(key_prefix, &criteria).await
            }
            StorageBackend::FileSystem { path } => {
                self.search_filesystem_archives(path, &criteria).await
            }
            StorageBackend::External { endpoint, bucket } => {
                let objects = self.list_external_objects(endpoint, bucket).await?;
                let limit = criteria.limit.unwrap_or(usize::MAX);
                let mut results = Vec::new();
                for obj in &objects {
                    if results.len() >= limit {
                        break;
                    }
                    // Key format: "{queue_name}/{archive_id}.json"
                    let archive_id = obj
                        .key
                        .strip_prefix(&format!("{}/", self.queue_name))
                        .and_then(|s| s.strip_suffix(".json"))
                        .unwrap_or("")
                        .to_string();
                    if archive_id.is_empty() {
                        continue;
                    }
                    // Short-circuit on archive_id filter before fetching the object
                    if let Some(ref target_id) = criteria.archive_id {
                        if archive_id != *target_id {
                            continue;
                        }
                    }
                    let obj_url =
                        format!("{}/{}/{}", endpoint.trim_end_matches('/'), bucket, obj.key);
                    let request = match self.http_client.get(&obj_url) {
                        Ok(r) => r,
                        Err(e) => {
                            warn!("Failed to build request for archive {}: {}", archive_id, e);
                            continue;
                        }
                    };
                    let resp = match request.send().await {
                        Ok(r) => r,
                        Err(e) => {
                            warn!("Failed to fetch external archive {}: {}", archive_id, e);
                            continue;
                        }
                    };
                    let archived_task: ArchivedTask = match resp.body_json().await {
                        Ok(t) => t,
                        Err(e) => {
                            warn!(
                                "Failed to deserialise external archive {}: {}",
                                archive_id, e
                            );
                            continue;
                        }
                    };
                    if self.matches_criteria(&archived_task, &criteria) {
                        results.push(archived_task);
                    }
                }
                Ok(results)
            }
        }
    }

    /// Restore archived tasks back to DLQ
    pub async fn restore_from_archive(&self, archive_ids: Vec<String>) -> Result<usize> {
        let mut conn = self
            .client
            .celers_multiplexed_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Connection error: {}", e)))?;

        let mut restored_count = 0;

        for archive_id in archive_ids {
            // Search for the archived task
            let criteria = ArchiveSearchCriteria {
                task_name: None,
                min_timestamp: None,
                max_timestamp: None,
                archive_id: Some(archive_id.clone()),
                limit: Some(1),
            };

            let results = self.search_archives(criteria).await?;

            if let Some(archived_task) = results.first() {
                // Serialize task
                let task_data = serde_json::to_string(&archived_task.task)
                    .map_err(|e| CelersError::Serialization(e.to_string()))?;

                // Push back to DLQ
                let _: i64 = conn
                    .lpush(&self.dlq_key, task_data)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to restore to DLQ: {}", e)))?;

                restored_count += 1;
            } else {
                warn!("Archive ID {} not found", archive_id);
            }
        }

        info!(
            "Restored {} tasks from archive to DLQ '{}'",
            restored_count, self.queue_name
        );

        Ok(restored_count)
    }

    /// Delete archives by age
    async fn delete_archives_by_age(&self, max_age_secs: u64) -> Result<usize> {
        let cutoff_timestamp = chrono::Utc::now().timestamp() - max_age_secs as i64;

        let criteria = ArchiveSearchCriteria {
            task_name: None,
            min_timestamp: None,
            max_timestamp: Some(cutoff_timestamp),
            archive_id: None,
            limit: None,
        };

        let archives = self.search_archives(criteria).await?;
        let count = archives.len();

        for archived_task in archives {
            self.delete_from_backend(&archived_task.archive_id).await?;
        }

        debug!(
            "Deleted {} archives older than {} seconds",
            count, max_age_secs
        );
        Ok(count)
    }

    /// Delete archives by count (keep only the newest max_count archives)
    async fn delete_archives_by_count(&self, max_count: usize) -> Result<usize> {
        let all_archives = self
            .search_archives(ArchiveSearchCriteria {
                task_name: None,
                min_timestamp: None,
                max_timestamp: None,
                archive_id: None,
                limit: None,
            })
            .await?;

        if all_archives.len() <= max_count {
            return Ok(0);
        }

        // Sort by timestamp (oldest first)
        let mut sorted_archives = all_archives;
        sorted_archives.sort_by_key(|a| a.archived_at);

        // Delete oldest archives
        let to_delete = sorted_archives.len() - max_count;
        let mut deleted_count = 0;

        for archived_task in sorted_archives.iter().take(to_delete) {
            self.delete_from_backend(&archived_task.archive_id).await?;
            deleted_count += 1;
        }

        debug!(
            "Deleted {} oldest archives to maintain count limit",
            deleted_count
        );
        Ok(deleted_count)
    }

    /// Delete archives by size
    async fn delete_archives_by_size(&self, max_size_bytes: u64) -> Result<usize> {
        let stats = self.get_archive_stats().await?;

        if stats.total_size_bytes <= max_size_bytes {
            return Ok(0);
        }

        // Get all archives sorted by age (oldest first)
        let all_archives = self
            .search_archives(ArchiveSearchCriteria {
                task_name: None,
                min_timestamp: None,
                max_timestamp: None,
                archive_id: None,
                limit: None,
            })
            .await?;

        let mut sorted_archives = all_archives;
        sorted_archives.sort_by_key(|a| a.archived_at);

        let mut current_size = stats.total_size_bytes;
        let mut deleted_count = 0;

        for archived_task in sorted_archives {
            if current_size <= max_size_bytes {
                break;
            }

            let task_size = archived_task.compressed_size.unwrap_or(1024); // Estimate 1KB if unknown
            self.delete_from_backend(&archived_task.archive_id).await?;
            current_size = current_size.saturating_sub(task_size as u64);
            deleted_count += 1;
        }

        debug!("Deleted {} archives to maintain size limit", deleted_count);
        Ok(deleted_count)
    }

    /// Store archived task to backend
    async fn store_to_backend(&self, archived_task: &ArchivedTask) -> Result<()> {
        match &self.config.backend {
            StorageBackend::Redis { key_prefix } => {
                let mut conn = self
                    .client
                    .celers_multiplexed_connection()
                    .await
                    .map_err(|e| CelersError::Broker(format!("Connection error: {}", e)))?;

                let key = format!(
                    "{}:{}:{}",
                    self.queue_name, key_prefix, archived_task.archive_id
                );
                let data = serde_json::to_string(archived_task)
                    .map_err(|e| CelersError::Serialization(e.to_string()))?;

                let _: () = conn
                    .set(&key, data)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to store archive: {}", e)))?;

                Ok(())
            }
            StorageBackend::FileSystem { path } => {
                let file_path = PathBuf::from(path)
                    .join(&self.queue_name)
                    .join(format!("{}.json", archived_task.archive_id));

                // Create directory if it doesn't exist
                if let Some(parent) = file_path.parent() {
                    std::fs::create_dir_all(parent).map_err(|e| {
                        CelersError::Broker(format!("Failed to create archive directory: {}", e))
                    })?;
                }

                let data = serde_json::to_string_pretty(archived_task)
                    .map_err(|e| CelersError::Serialization(e.to_string()))?;

                std::fs::write(&file_path, data).map_err(|e| {
                    CelersError::Broker(format!("Failed to write archive file: {}", e))
                })?;

                Ok(())
            }
            StorageBackend::External { endpoint, bucket } => {
                let key = format!("{}/{}.json", self.queue_name, archived_task.archive_id);
                let url = format!("{}/{}/{}", endpoint.trim_end_matches('/'), bucket, key);
                let data = serde_json::to_string(archived_task)
                    .map_err(|e| CelersError::Serialization(e.to_string()))?;
                self.http_client
                    .put(&url)
                    .map_err(|e| {
                        CelersError::Broker(format!(
                            "External PUT request build failed for {}: {}",
                            url, e
                        ))
                    })?
                    .header("Content-Type", "application/json")
                    .map_err(|e| {
                        CelersError::Broker(format!(
                            "External PUT header failed for {}: {}",
                            url, e
                        ))
                    })?
                    .body(data)
                    .send()
                    .await
                    .map_err(|e| {
                        CelersError::Broker(format!("External PUT failed for {}: {}", url, e))
                    })?
                    .error_for_status()
                    .map_err(|e| {
                        CelersError::Broker(format!("External PUT status error for {}: {}", url, e))
                    })?;
                Ok(())
            }
        }
    }

    /// Delete archived task from backend
    async fn delete_from_backend(&self, archive_id: &str) -> Result<()> {
        match &self.config.backend {
            StorageBackend::Redis { key_prefix } => {
                let mut conn = self
                    .client
                    .celers_multiplexed_connection()
                    .await
                    .map_err(|e| CelersError::Broker(format!("Connection error: {}", e)))?;

                let key = format!("{}:{}:{}", self.queue_name, key_prefix, archive_id);
                let _: i64 = conn
                    .del(&key)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to delete archive: {}", e)))?;

                Ok(())
            }
            StorageBackend::FileSystem { path } => {
                let file_path = PathBuf::from(path)
                    .join(&self.queue_name)
                    .join(format!("{}.json", archive_id));

                if file_path.exists() {
                    std::fs::remove_file(&file_path).map_err(|e| {
                        CelersError::Broker(format!("Failed to delete archive file: {}", e))
                    })?;
                }

                Ok(())
            }
            StorageBackend::External { endpoint, bucket } => {
                let key = format!("{}/{}.json", self.queue_name, archive_id);
                let url = format!("{}/{}/{}", endpoint.trim_end_matches('/'), bucket, key);
                let resp = self
                    .http_client
                    .delete(&url)
                    .map_err(|e| {
                        CelersError::Broker(format!(
                            "External DELETE request build failed for {}: {}",
                            url, e
                        ))
                    })?
                    .send()
                    .await
                    .map_err(|e| {
                        CelersError::Broker(format!("External DELETE failed for {}: {}", url, e))
                    })?;
                // 204 No Content = success; 404 = already gone (treat as ok)
                let status = resp.status();
                if !status.is_success() && status.as_u16() != 404 {
                    return Err(CelersError::Broker(format!(
                        "External DELETE returned {} for {}",
                        status, url
                    )));
                }
                Ok(())
            }
        }
    }

    /// Get archive statistics from Redis backend
    async fn get_redis_archive_stats(&self, key_prefix: &str) -> Result<ArchiveStats> {
        let mut conn = self
            .client
            .celers_multiplexed_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Connection error: {}", e)))?;

        let pattern = format!("{}:{}:*", self.queue_name, key_prefix);
        let keys: Vec<String> = conn
            .keys(&pattern)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get archive keys: {}", e)))?;

        let total_tasks = keys.len();
        let mut total_size_bytes = 0u64;
        let mut oldest_timestamp: Option<i64> = None;
        let mut newest_timestamp: Option<i64> = None;

        for key in keys {
            let data: String = conn
                .get(&key)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to get archive: {}", e)))?;

            total_size_bytes += data.len() as u64;

            if let Ok(archived_task) = serde_json::from_str::<ArchivedTask>(&data) {
                oldest_timestamp = Some(oldest_timestamp.map_or(archived_task.archived_at, |t| {
                    t.min(archived_task.archived_at)
                }));
                newest_timestamp = Some(newest_timestamp.map_or(archived_task.archived_at, |t| {
                    t.max(archived_task.archived_at)
                }));
            }
        }

        Ok(ArchiveStats {
            total_tasks,
            total_size_bytes,
            oldest_timestamp,
            newest_timestamp,
            compression_ratio: None,
        })
    }

    /// Get archive statistics from filesystem backend
    async fn get_filesystem_archive_stats(&self, path: &str) -> Result<ArchiveStats> {
        let dir_path = PathBuf::from(path).join(&self.queue_name);

        if !dir_path.exists() {
            return Ok(ArchiveStats {
                total_tasks: 0,
                total_size_bytes: 0,
                oldest_timestamp: None,
                newest_timestamp: None,
                compression_ratio: None,
            });
        }

        let mut total_tasks = 0;
        let mut total_size_bytes = 0u64;
        let mut oldest_timestamp: Option<i64> = None;
        let mut newest_timestamp: Option<i64> = None;

        let entries = std::fs::read_dir(&dir_path)
            .map_err(|e| CelersError::Broker(format!("Failed to read archive directory: {}", e)))?;

        for entry in entries.flatten() {
            if let Ok(metadata) = entry.metadata() {
                total_size_bytes += metadata.len();
                total_tasks += 1;

                if let Ok(data) = std::fs::read_to_string(entry.path()) {
                    if let Ok(archived_task) = serde_json::from_str::<ArchivedTask>(&data) {
                        oldest_timestamp =
                            Some(oldest_timestamp.map_or(archived_task.archived_at, |t| {
                                t.min(archived_task.archived_at)
                            }));
                        newest_timestamp =
                            Some(newest_timestamp.map_or(archived_task.archived_at, |t| {
                                t.max(archived_task.archived_at)
                            }));
                    }
                }
            }
        }

        Ok(ArchiveStats {
            total_tasks,
            total_size_bytes,
            oldest_timestamp,
            newest_timestamp,
            compression_ratio: None,
        })
    }

    /// Search Redis archives
    async fn search_redis_archives(
        &self,
        key_prefix: &str,
        criteria: &ArchiveSearchCriteria,
    ) -> Result<Vec<ArchivedTask>> {
        let mut conn = self
            .client
            .celers_multiplexed_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Connection error: {}", e)))?;

        let pattern = format!("{}:{}:*", self.queue_name, key_prefix);
        let keys: Vec<String> = conn
            .keys(&pattern)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get archive keys: {}", e)))?;

        let mut results = Vec::new();

        for key in keys {
            let data: String = conn
                .get(&key)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to get archive: {}", e)))?;

            if let Ok(archived_task) = serde_json::from_str::<ArchivedTask>(&data) {
                if self.matches_criteria(&archived_task, criteria) {
                    results.push(archived_task);

                    if let Some(limit) = criteria.limit {
                        if results.len() >= limit {
                            break;
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Search filesystem archives
    async fn search_filesystem_archives(
        &self,
        path: &str,
        criteria: &ArchiveSearchCriteria,
    ) -> Result<Vec<ArchivedTask>> {
        let dir_path = PathBuf::from(path).join(&self.queue_name);

        if !dir_path.exists() {
            return Ok(Vec::new());
        }

        let mut results = Vec::new();

        let entries = std::fs::read_dir(&dir_path)
            .map_err(|e| CelersError::Broker(format!("Failed to read archive directory: {}", e)))?;

        for entry in entries.flatten() {
            if let Ok(data) = std::fs::read_to_string(entry.path()) {
                if let Ok(archived_task) = serde_json::from_str::<ArchivedTask>(&data) {
                    if self.matches_criteria(&archived_task, criteria) {
                        results.push(archived_task);

                        if let Some(limit) = criteria.limit {
                            if results.len() >= limit {
                                break;
                            }
                        }
                    }
                }
            }
        }

        Ok(results)
    }

    /// Check if archived task matches search criteria
    fn matches_criteria(
        &self,
        archived_task: &ArchivedTask,
        criteria: &ArchiveSearchCriteria,
    ) -> bool {
        if let Some(ref task_name) = criteria.task_name {
            if !archived_task.task.metadata.name.contains(task_name) {
                return false;
            }
        }

        if let Some(min_ts) = criteria.min_timestamp {
            if archived_task.archived_at < min_ts {
                return false;
            }
        }

        if let Some(max_ts) = criteria.max_timestamp {
            if archived_task.archived_at > max_ts {
                return false;
            }
        }

        if let Some(ref archive_id) = criteria.archive_id {
            if &archived_task.archive_id != archive_id {
                return false;
            }
        }

        true
    }

    /// List objects in the External S3-compatible backend for this queue.
    ///
    /// Issues an S3 ListObjectsV2 GET request (`list-type=2`) and parses the
    /// XML response. Works with any S3-compatible store (MinIO, Ceph RGW, GCS
    /// interop layer) that implements the standard ListObjectsV2 schema.
    async fn list_external_objects(
        &self,
        endpoint: &str,
        bucket: &str,
    ) -> Result<Vec<ExternalObjectInfo>> {
        let url = format!(
            "{}/{}?list-type=2&prefix={}/",
            endpoint.trim_end_matches('/'),
            bucket,
            self.queue_name
        );
        let xml = self
            .http_client
            .get(&url)
            .map_err(|e| CelersError::Broker(format!("External LIST request build failed: {}", e)))?
            .send()
            .await
            .map_err(|e| CelersError::Broker(format!("External LIST failed: {}", e)))?
            .error_for_status()
            .map_err(|e| CelersError::Broker(format!("External LIST error: {}", e)))?
            .body_text()
            .await
            .map_err(|e| {
                CelersError::Broker(format!("Failed to read LIST response body: {}", e))
            })?;

        // Parse S3 ListObjectsV2 XML — each <Contents> block describes one object.
        let mut objects = Vec::new();
        let mut offset = 0;
        while let Some(rel_start) = xml[offset..].find("<Contents>") {
            let abs_start = offset + rel_start + "<Contents>".len();
            let Some(rel_end) = xml[abs_start..].find("</Contents>") else {
                break;
            };
            let block = &xml[abs_start..abs_start + rel_end];
            offset = abs_start + rel_end + "</Contents>".len();

            let key = extract_xml_tag(block, "Key")
                .unwrap_or_default()
                .to_string();
            let size = extract_xml_tag(block, "Size")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
            let last_modified = extract_xml_tag(block, "LastModified").map(str::to_string);

            objects.push(ExternalObjectInfo {
                key,
                size,
                last_modified,
            });
        }
        Ok(objects)
    }

    /// Derive a stable archive ID from a task's identity and its DLQ-entry
    /// timestamp, so archiving the same DLQ entry more than once (e.g. after
    /// a `store_to_backend` succeeded but the following `LREM` failed, so the
    /// entry is picked up again on the next run) overwrites the same backend
    /// record instead of creating a duplicate.
    fn deterministic_archive_id(&self, task: &SerializedTask, entry_timestamp: i64) -> String {
        format!("{}-{}", task.metadata.id, entry_timestamp)
    }
}

/// Metadata for a single object returned by `list_external_objects`.
struct ExternalObjectInfo {
    key: String,
    size: u64,
    last_modified: Option<String>,
}

/// Extract the text content of the first `<tag>…</tag>` element found in `xml`.
///
/// Handles simple `<Tag>value</Tag>` patterns as found in S3 XML responses.
/// Does not handle namespaced tags, attributes, or CDATA.
fn extract_xml_tag<'a>(xml: &'a str, tag: &str) -> Option<&'a str> {
    let open = format!("<{}>", tag);
    let close = format!("</{}>", tag);
    let start = xml.find(open.as_str())? + open.len();
    let end = xml[start..].find(close.as_str())?;
    Some(&xml[start..start + end])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `archive_old_tasks` must honor its age parameter: only entries whose
    /// real (`updated_at`-derived) DLQ timestamp is older than the cutoff
    /// are archived and removed, and the archived record must carry that
    /// real timestamp rather than a value derived from the cutoff/`now`.
    #[tokio::test]
    async fn test_archive_old_tasks_filters_by_entry_age_and_records_real_timestamp() {
        let queue_name = format!("test-archival-age-{}", uuid::Uuid::new_v4());
        let manager = DLQArchivalManager::new(
            "redis://127.0.0.1:6379",
            &queue_name,
            ArchivalConfig::default(),
        )
        .await
        .unwrap();

        let mut conn = Client::open("redis://127.0.0.1:6379")
            .unwrap()
            .celers_multiplexed_connection()
            .await
            .unwrap();
        let dlq_key = format!("{}:dlq", queue_name);

        // A fresh task (just failed) must NOT be archived.
        let mut fresh_task = SerializedTask::new("fresh_job".to_string(), vec![1, 2, 3]);
        fresh_task.metadata.updated_at = chrono::Utc::now();
        let fresh_data = serde_json::to_string(&fresh_task).unwrap();

        // A task that entered the DLQ 10 days ago SHOULD be archived when
        // sweeping for anything older than 7 days.
        let mut old_task = SerializedTask::new("old_job".to_string(), vec![4, 5, 6]);
        old_task.metadata.updated_at = chrono::Utc::now() - chrono::Duration::days(10);
        let old_data = serde_json::to_string(&old_task).unwrap();

        let _: () = conn.rpush(&dlq_key, &fresh_data).await.unwrap();
        let _: () = conn.rpush(&dlq_key, &old_data).await.unwrap();

        let archived = manager.archive_old_tasks(7 * 24 * 3600).await.unwrap();
        assert_eq!(archived, 1, "only the 10-day-old entry should be archived");

        // The fresh entry must still be sitting in the live DLQ.
        let remaining: Vec<String> = conn.lrange(&dlq_key, 0, -1).await.unwrap();
        assert_eq!(remaining, vec![fresh_data.clone()]);

        // The archived record must carry the task's *real* entry timestamp,
        // not a fabricated cutoff-derived value.
        let archives = manager
            .search_archives(ArchiveSearchCriteria {
                task_name: Some("old_job".to_string()),
                min_timestamp: None,
                max_timestamp: None,
                archive_id: None,
                limit: None,
            })
            .await
            .unwrap();
        assert_eq!(archives.len(), 1);
        assert_eq!(
            archives[0].dlq_timestamp,
            Some(old_task.metadata.updated_at.timestamp())
        );

        // Re-running immediately is a safe no-op: the only remaining DLQ
        // entry (the fresh one) is still too young to archive.
        let archived_again = manager.archive_old_tasks(7 * 24 * 3600).await.unwrap();
        assert_eq!(archived_again, 0);

        // Cleanup.
        manager
            .delete_from_backend(&archives[0].archive_id)
            .await
            .unwrap();
        let _: () = conn.del(&dlq_key).await.unwrap();
    }

    #[test]
    fn test_archival_config_builder() {
        let archive_path = std::env::temp_dir()
            .join("archives")
            .to_string_lossy()
            .to_string();
        let config = ArchivalConfig::default()
            .with_backend(StorageBackend::FileSystem { path: archive_path })
            .with_retention_policy(RetentionPolicy::ByAge {
                max_age_secs: 86400,
            })
            .with_compression(true)
            .with_batch_size(50);

        assert!(config.enable_compression);
        assert_eq!(config.batch_size, 50);
    }

    #[test]
    fn test_retention_policy_variants() {
        let by_age = RetentionPolicy::ByAge { max_age_secs: 3600 };
        let by_count = RetentionPolicy::ByCount { max_count: 1000 };
        let by_size = RetentionPolicy::BySize {
            max_size_bytes: 1024 * 1024,
        };
        let unlimited = RetentionPolicy::Unlimited;

        // Just verify they can be constructed
        assert!(matches!(by_age, RetentionPolicy::ByAge { .. }));
        assert!(matches!(by_count, RetentionPolicy::ByCount { .. }));
        assert!(matches!(by_size, RetentionPolicy::BySize { .. }));
        assert!(matches!(unlimited, RetentionPolicy::Unlimited));
    }

    #[test]
    fn test_storage_backend_variants() {
        let fs = StorageBackend::FileSystem {
            path: "/tmp".to_string(),
        };
        let redis = StorageBackend::Redis {
            key_prefix: "archive".to_string(),
        };
        let external = StorageBackend::External {
            endpoint: "https://s3.amazonaws.com".to_string(),
            bucket: "my-bucket".to_string(),
        };

        assert!(matches!(fs, StorageBackend::FileSystem { .. }));
        assert!(matches!(redis, StorageBackend::Redis { .. }));
        assert!(matches!(external, StorageBackend::External { .. }));
    }

    #[test]
    fn test_archive_search_criteria() {
        let criteria = ArchiveSearchCriteria {
            task_name: Some("test".to_string()),
            min_timestamp: Some(1000),
            max_timestamp: Some(2000),
            archive_id: None,
            limit: Some(10),
        };

        assert_eq!(criteria.task_name, Some("test".to_string()));
        assert_eq!(criteria.limit, Some(10));
    }

    #[test]
    fn test_archive_stats_default() {
        let stats = ArchiveStats {
            total_tasks: 0,
            total_size_bytes: 0,
            oldest_timestamp: None,
            newest_timestamp: None,
            compression_ratio: None,
        };

        assert_eq!(stats.total_tasks, 0);
        assert_eq!(stats.total_size_bytes, 0);
    }

    #[test]
    fn test_extract_xml_tag_basic() {
        let xml = "<Key>my-queue/abc123.json</Key><Size>1024</Size>";
        assert_eq!(extract_xml_tag(xml, "Key"), Some("my-queue/abc123.json"));
        assert_eq!(extract_xml_tag(xml, "Size"), Some("1024"));
        assert_eq!(extract_xml_tag(xml, "Missing"), None);
    }

    #[test]
    fn test_extract_xml_tag_in_s3_block() {
        let block = "\n    <Key>tasks/abc-def.json</Key>\n    <Size>2048</Size>\n    <LastModified>2024-01-15T10:00:00.000Z</LastModified>\n";
        assert_eq!(extract_xml_tag(block, "Key"), Some("tasks/abc-def.json"));
        assert_eq!(extract_xml_tag(block, "Size"), Some("2048"));
        assert_eq!(
            extract_xml_tag(block, "LastModified"),
            Some("2024-01-15T10:00:00.000Z")
        );
    }

    #[test]
    fn test_extract_xml_tag_empty_value() {
        let xml = "<Prefix></Prefix>";
        assert_eq!(extract_xml_tag(xml, "Prefix"), Some(""));
    }

    #[test]
    fn test_s3_list_xml_parsing_via_extract() {
        // Simulate an S3 ListObjectsV2 XML response fragment
        let xml = r#"<ListBucketResult>
  <KeyCount>2</KeyCount>
  <Contents>
    <Key>queue/id1.json</Key><Size>512</Size><LastModified>2024-03-01T00:00:00.000Z</LastModified>
  </Contents>
  <Contents>
    <Key>queue/id2.json</Key><Size>768</Size><LastModified>2024-03-02T00:00:00.000Z</LastModified>
  </Contents>
</ListBucketResult>"#;

        // Verify that our parser correctly locates individual <Contents> blocks
        let mut count = 0;
        let mut total_size = 0u64;
        let mut offset = 0;
        while let Some(rel_start) = xml[offset..].find("<Contents>") {
            let abs_start = offset + rel_start + "<Contents>".len();
            let Some(rel_end) = xml[abs_start..].find("</Contents>") else {
                break;
            };
            let block = &xml[abs_start..abs_start + rel_end];
            offset = abs_start + rel_end + "</Contents>".len();
            count += 1;
            total_size += extract_xml_tag(block, "Size")
                .and_then(|s| s.parse::<u64>().ok())
                .unwrap_or(0);
        }
        assert_eq!(count, 2);
        assert_eq!(total_size, 1280);
    }
}
