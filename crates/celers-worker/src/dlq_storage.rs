//! Storage backends for Dead Letter Queue persistence
//!
//! This module provides different storage backends for DLQ entries:
//! - **Memory**: In-memory storage (default, non-persistent)
//! - **Redis**: Persistent storage using Redis with TTL support
//! - **PostgreSQL**: Persistent storage using PostgreSQL with advanced querying
//!
//! # Example
//!
//! ```rust
//! use celers_worker::dlq_storage::{DlqStorage, MemoryDlqStorage};
//!
//! # async fn example() -> celers_core::Result<()> {
//! let storage = MemoryDlqStorage::new();
//! # Ok(())
//! # }
//! ```

use crate::dlq::DlqEntry;
use async_trait::async_trait;
#[allow(unused_imports)]
use celers_core::{CelersError, Result, TaskId};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
#[allow(unused_imports)]
use tracing::{debug, error, info, warn};

/// Trait for DLQ storage backends
#[async_trait]
pub trait DlqStorage: Send + Sync {
    /// Add a DLQ entry to storage
    async fn add(&self, entry: DlqEntry) -> Result<()>;

    /// Get a specific DLQ entry by task ID
    async fn get(&self, task_id: &TaskId) -> Result<Option<DlqEntry>>;

    /// Get all DLQ entries
    async fn get_all(&self) -> Result<Vec<DlqEntry>>;

    /// Remove a specific entry by task ID
    async fn remove(&self, task_id: &TaskId) -> Result<bool>;

    /// Remove multiple entries by task IDs
    async fn remove_batch(&self, task_ids: &[TaskId]) -> Result<usize>;

    /// Clear all DLQ entries
    async fn clear(&self) -> Result<usize>;

    /// Get the number of entries in storage
    async fn size(&self) -> Result<usize>;

    /// Get entries older than a certain age (in seconds)
    async fn get_older_than(&self, age_seconds: u64) -> Result<Vec<DlqEntry>>;

    /// Get entries for a specific task type
    async fn get_by_task_name(&self, task_name: &str) -> Result<Vec<DlqEntry>>;

    /// Clean up expired entries based on TTL
    async fn cleanup_expired(&self, ttl_seconds: u64) -> Result<usize>;

    /// Get aggregated statistics by task name
    async fn get_stats_by_task_name(&self) -> Result<HashMap<String, TaskStats>>;

    /// Check storage health
    async fn health_check(&self) -> Result<bool>;
}

/// Statistics for a specific task type in the DLQ
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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

/// In-memory DLQ storage (non-persistent)
pub struct MemoryDlqStorage {
    entries: Arc<RwLock<Vec<DlqEntry>>>,
}

impl MemoryDlqStorage {
    /// Create a new in-memory storage
    pub fn new() -> Self {
        Self {
            entries: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

impl Default for MemoryDlqStorage {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DlqStorage for MemoryDlqStorage {
    async fn add(&self, entry: DlqEntry) -> Result<()> {
        let mut entries = self.entries.write().await;
        entries.push(entry);
        Ok(())
    }

    async fn get(&self, task_id: &TaskId) -> Result<Option<DlqEntry>> {
        let entries = self.entries.read().await;
        Ok(entries.iter().find(|e| &e.task_id == task_id).cloned())
    }

    async fn get_all(&self) -> Result<Vec<DlqEntry>> {
        Ok(self.entries.read().await.clone())
    }

    async fn remove(&self, task_id: &TaskId) -> Result<bool> {
        let mut entries = self.entries.write().await;
        if let Some(pos) = entries.iter().position(|e| &e.task_id == task_id) {
            entries.remove(pos);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn remove_batch(&self, task_ids: &[TaskId]) -> Result<usize> {
        let mut entries = self.entries.write().await;
        let before_count = entries.len();
        entries.retain(|e| !task_ids.contains(&e.task_id));
        let removed = before_count - entries.len();
        Ok(removed)
    }

    async fn clear(&self) -> Result<usize> {
        let mut entries = self.entries.write().await;
        let count = entries.len();
        entries.clear();
        Ok(count)
    }

    async fn size(&self) -> Result<usize> {
        Ok(self.entries.read().await.len())
    }

    async fn get_older_than(&self, age_seconds: u64) -> Result<Vec<DlqEntry>> {
        let entries = self.entries.read().await;
        Ok(entries
            .iter()
            .filter(|e| e.age_seconds() > age_seconds)
            .cloned()
            .collect())
    }

    async fn get_by_task_name(&self, task_name: &str) -> Result<Vec<DlqEntry>> {
        let entries = self.entries.read().await;
        Ok(entries
            .iter()
            .filter(|e| e.task.metadata.name == task_name)
            .cloned()
            .collect())
    }

    async fn cleanup_expired(&self, ttl_seconds: u64) -> Result<usize> {
        let mut entries = self.entries.write().await;
        let before_count = entries.len();
        entries.retain(|e| !e.is_expired(ttl_seconds));
        let removed = before_count - entries.len();
        if removed > 0 {
            info!("Removed {} expired entries from memory DLQ", removed);
        }
        Ok(removed)
    }

    async fn get_stats_by_task_name(&self) -> Result<HashMap<String, TaskStats>> {
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
            if stats.count == 1 || age < stats.min_age_seconds {
                stats.min_age_seconds = age;
            }
            if age > stats.max_age_seconds {
                stats.max_age_seconds = age;
            }
        }

        Ok(task_stats)
    }

    async fn health_check(&self) -> Result<bool> {
        // Memory storage is always healthy
        Ok(true)
    }
}

#[cfg(feature = "redis")]
/// Redis-based DLQ storage with persistence and TTL support
pub struct RedisDlqStorage {
    client: redis::Client,
    key_prefix: String,
}

#[cfg(feature = "redis")]
impl RedisDlqStorage {
    /// Create a new Redis DLQ storage
    ///
    /// # Arguments
    /// * `redis_url` - Redis connection URL (e.g., "redis://127.0.0.1:6379")
    /// * `key_prefix` - Prefix for Redis keys (default: "celery:dlq")
    pub fn new(redis_url: &str, key_prefix: Option<String>) -> Result<Self> {
        let client = redis::Client::open(redis_url).map_err(|e| {
            celers_core::CelersError::Other(format!("Redis connection error: {}", e))
        })?;

        Ok(Self {
            client,
            key_prefix: key_prefix.unwrap_or_else(|| "celery:dlq".to_string()),
        })
    }

    /// Get the Redis key for a specific task ID
    fn task_key(&self, task_id: &TaskId) -> String {
        format!("{}:entry:{}", self.key_prefix, task_id)
    }

    /// Get the Redis key for the task ID index
    fn index_key(&self) -> String {
        format!("{}:index", self.key_prefix)
    }

    /// Get the Redis key for task name index
    fn task_name_index_key(&self, task_name: &str) -> String {
        format!("{}:by_task:{}", self.key_prefix, task_name)
    }
}

#[cfg(feature = "redis")]
#[async_trait]
impl DlqStorage for RedisDlqStorage {
    async fn add(&self, entry: DlqEntry) -> Result<()> {
        use redis::AsyncCommands;

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Redis connection error: {}", e))
            })?;

        let task_id = entry.task_id;
        let task_key = self.task_key(&task_id);
        let index_key = self.index_key();
        let task_name_index = self.task_name_index_key(&entry.task.metadata.name);

        // Serialize entry to JSON
        let json = serde_json::to_string(&entry)
            .map_err(|e| celers_core::CelersError::Other(format!("Serialization error: {}", e)))?;

        // Store entry with TTL if specified
        let _: () = conn
            .set(&task_key, json)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Redis set error: {}", e)))?;

        // Add to index
        let _: () = conn
            .sadd(&index_key, task_id.to_string())
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Redis sadd error: {}", e)))?;

        // Add to task name index
        let _: () = conn
            .sadd(&task_name_index, task_id.to_string())
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Redis sadd error: {}", e)))?;

        debug!("Added task {} to Redis DLQ", task_id);
        Ok(())
    }

    async fn get(&self, task_id: &TaskId) -> Result<Option<DlqEntry>> {
        use redis::AsyncCommands;

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Redis connection error: {}", e))
            })?;

        let task_key = self.task_key(task_id);
        let json: Option<String> = conn
            .get(&task_key)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Redis get error: {}", e)))?;

        match json {
            Some(json_str) => {
                let entry: DlqEntry = serde_json::from_str(&json_str).map_err(|e| {
                    celers_core::CelersError::Other(format!("Deserialization error: {}", e))
                })?;
                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    async fn get_all(&self) -> Result<Vec<DlqEntry>> {
        use redis::AsyncCommands;

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Redis connection error: {}", e))
            })?;

        let index_key = self.index_key();
        let task_ids: Vec<String> = conn
            .smembers(&index_key)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Redis smembers error: {}", e)))?;

        let mut entries = Vec::new();
        for task_id_str in task_ids {
            if let Ok(task_id) = uuid::Uuid::parse_str(&task_id_str) {
                if let Ok(Some(entry)) = self.get(&task_id).await {
                    entries.push(entry);
                }
            }
        }

        Ok(entries)
    }

    async fn remove(&self, task_id: &TaskId) -> Result<bool> {
        use redis::AsyncCommands;

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Redis connection error: {}", e))
            })?;

        // Get entry first to know the task name
        let entry = self.get(task_id).await?;
        if entry.is_none() {
            return Ok(false);
        }

        let task_name = entry.unwrap().task.metadata.name;
        let task_key = self.task_key(task_id);
        let index_key = self.index_key();
        let task_name_index = self.task_name_index_key(&task_name);

        // Remove entry
        let deleted: i32 = conn
            .del(&task_key)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Redis del error: {}", e)))?;

        // Remove from indices
        let _: () = conn
            .srem(&index_key, task_id.to_string())
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Redis srem error: {}", e)))?;

        let _: () = conn
            .srem(&task_name_index, task_id.to_string())
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Redis srem error: {}", e)))?;

        debug!("Removed task {} from Redis DLQ", task_id);
        Ok(deleted > 0)
    }

    async fn remove_batch(&self, task_ids: &[TaskId]) -> Result<usize> {
        let mut removed = 0;
        for task_id in task_ids {
            if self.remove(task_id).await? {
                removed += 1;
            }
        }
        Ok(removed)
    }

    async fn clear(&self) -> Result<usize> {
        let entries = self.get_all().await?;
        let count = entries.len();

        for entry in entries {
            let _ = self.remove(&entry.task_id).await;
        }

        info!("Cleared {} entries from Redis DLQ", count);
        Ok(count)
    }

    async fn size(&self) -> Result<usize> {
        use redis::AsyncCommands;

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Redis connection error: {}", e))
            })?;

        let index_key = self.index_key();
        let size: usize = conn
            .scard(&index_key)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Redis scard error: {}", e)))?;

        Ok(size)
    }

    async fn get_older_than(&self, age_seconds: u64) -> Result<Vec<DlqEntry>> {
        let entries = self.get_all().await?;
        Ok(entries
            .into_iter()
            .filter(|e| e.age_seconds() > age_seconds)
            .collect())
    }

    async fn get_by_task_name(&self, task_name: &str) -> Result<Vec<DlqEntry>> {
        use redis::AsyncCommands;

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Redis connection error: {}", e))
            })?;

        let task_name_index = self.task_name_index_key(task_name);
        let task_ids: Vec<String> = conn
            .smembers(&task_name_index)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Redis smembers error: {}", e)))?;

        let mut entries = Vec::new();
        for task_id_str in task_ids {
            if let Ok(task_id) = uuid::Uuid::parse_str(&task_id_str) {
                if let Ok(Some(entry)) = self.get(&task_id).await {
                    entries.push(entry);
                }
            }
        }

        Ok(entries)
    }

    async fn cleanup_expired(&self, ttl_seconds: u64) -> Result<usize> {
        let entries = self.get_all().await?;
        let mut removed = 0;

        for entry in entries {
            if entry.is_expired(ttl_seconds) && self.remove(&entry.task_id).await? {
                removed += 1;
            }
        }

        if removed > 0 {
            info!("Removed {} expired entries from Redis DLQ", removed);
        }

        Ok(removed)
    }

    async fn get_stats_by_task_name(&self) -> Result<HashMap<String, TaskStats>> {
        let entries = self.get_all().await?;
        let mut task_stats: HashMap<String, TaskStats> = HashMap::new();

        for entry in entries {
            let name = entry.task.metadata.name.clone();
            let stats = task_stats.entry(name).or_default();

            stats.count += 1;
            stats.total_retries += entry.retry_count as usize;
            if entry.retry_count > stats.max_retries {
                stats.max_retries = entry.retry_count;
            }

            let age = entry.age_seconds();
            if stats.count == 1 || age < stats.min_age_seconds {
                stats.min_age_seconds = age;
            }
            if age > stats.max_age_seconds {
                stats.max_age_seconds = age;
            }
        }

        Ok(task_stats)
    }

    async fn health_check(&self) -> Result<bool> {
        use redis::AsyncCommands;

        match self.client.get_multiplexed_async_connection().await {
            Ok(mut conn) => {
                // Try a simple ping
                match conn.get::<_, Option<String>>("__health_check__").await {
                    Ok(_) => Ok(true),
                    Err(e) => {
                        warn!("Redis health check failed: {}", e);
                        Ok(false)
                    }
                }
            }
            Err(e) => {
                warn!("Redis connection failed during health check: {}", e);
                Ok(false)
            }
        }
    }
}

#[cfg(feature = "postgres")]
/// PostgreSQL-based DLQ storage with persistence and advanced querying
pub struct PostgresDlqStorage {
    pool: sqlx::PgPool,
    table_name: String,
}

#[cfg(feature = "postgres")]
impl PostgresDlqStorage {
    /// Create a new PostgreSQL DLQ storage
    ///
    /// # Arguments
    /// * `database_url` - PostgreSQL connection URL
    /// * `table_name` - Name of the DLQ table (default: "celery_dlq")
    pub async fn new(database_url: &str, table_name: Option<String>) -> Result<Self> {
        let pool = sqlx::PgPool::connect(database_url).await.map_err(|e| {
            celers_core::CelersError::Other(format!("PostgreSQL connection error: {}", e))
        })?;

        let table_name = table_name.unwrap_or_else(|| "celery_dlq".to_string());

        let storage = Self { pool, table_name };

        // Create table if it doesn't exist
        storage.create_table().await?;

        Ok(storage)
    }

    /// Create the DLQ table if it doesn't exist
    async fn create_table(&self) -> Result<()> {
        let query = format!(
            r#"
            CREATE TABLE IF NOT EXISTS {} (
                task_id UUID PRIMARY KEY,
                task_data JSONB NOT NULL,
                task_name VARCHAR(255) NOT NULL,
                retry_count INTEGER NOT NULL,
                error_message TEXT NOT NULL,
                dlq_timestamp BIGINT NOT NULL,
                original_timestamp BIGINT NOT NULL,
                worker_hostname VARCHAR(255) NOT NULL,
                metadata JSONB NOT NULL,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_{}_task_name ON {} (task_name);
            CREATE INDEX IF NOT EXISTS idx_{}_dlq_timestamp ON {} (dlq_timestamp);
            "#,
            self.table_name, self.table_name, self.table_name, self.table_name, self.table_name
        );

        sqlx::query(&query).execute(&self.pool).await.map_err(|e| {
            celers_core::CelersError::Other(format!("Failed to create DLQ table: {}", e))
        })?;

        debug!("Created or verified DLQ table: {}", self.table_name);
        Ok(())
    }
}

#[cfg(feature = "postgres")]
#[async_trait]
impl DlqStorage for PostgresDlqStorage {
    async fn add(&self, entry: DlqEntry) -> Result<()> {
        let task_json = serde_json::to_value(&entry.task)
            .map_err(|e| celers_core::CelersError::Other(format!("Serialization error: {}", e)))?;

        let metadata_json = serde_json::to_value(&entry.metadata)
            .map_err(|e| celers_core::CelersError::Other(format!("Serialization error: {}", e)))?;

        let query = format!(
            r#"
            INSERT INTO {} (
                task_id, task_data, task_name, retry_count, error_message,
                dlq_timestamp, original_timestamp, worker_hostname, metadata
            )
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (task_id) DO UPDATE SET
                retry_count = $4,
                error_message = $5,
                dlq_timestamp = $6,
                metadata = $9
            "#,
            self.table_name
        );

        sqlx::query(&query)
            .bind(entry.task_id)
            .bind(task_json)
            .bind(&entry.task.metadata.name)
            .bind(entry.retry_count as i32)
            .bind(&entry.error_message)
            .bind(entry.dlq_timestamp as i64)
            .bind(entry.original_timestamp as i64)
            .bind(&entry.worker_hostname)
            .bind(metadata_json)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Failed to insert DLQ entry: {}", e))
            })?;

        debug!("Added task {} to PostgreSQL DLQ", entry.task_id);
        Ok(())
    }

    async fn get(&self, task_id: &TaskId) -> Result<Option<DlqEntry>> {
        let query = format!(
            "SELECT task_id, task_data, retry_count, error_message, dlq_timestamp, original_timestamp, worker_hostname, metadata FROM {} WHERE task_id = $1",
            self.table_name
        );

        let row: Option<(
            uuid::Uuid,
            sqlx::types::JsonValue,
            i32,
            String,
            i64,
            i64,
            String,
            sqlx::types::JsonValue,
        )> = sqlx::query_as(&query)
            .bind(task_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Failed to fetch DLQ entry: {}", e))
            })?;

        match row {
            Some((
                task_id,
                task_data,
                retry_count,
                error_message,
                dlq_timestamp,
                original_timestamp,
                worker_hostname,
                metadata,
            )) => {
                let task: celers_core::SerializedTask =
                    serde_json::from_value(task_data).map_err(|e| {
                        celers_core::CelersError::Other(format!("Deserialization error: {}", e))
                    })?;

                let metadata_map: HashMap<String, String> = serde_json::from_value(metadata)
                    .map_err(|e| {
                        celers_core::CelersError::Other(format!("Deserialization error: {}", e))
                    })?;

                let entry = DlqEntry {
                    task,
                    task_id,
                    retry_count: retry_count as u32,
                    error_message,
                    dlq_timestamp: dlq_timestamp as u64,
                    original_timestamp: original_timestamp as u64,
                    worker_hostname,
                    metadata: metadata_map,
                };

                Ok(Some(entry))
            }
            None => Ok(None),
        }
    }

    async fn get_all(&self) -> Result<Vec<DlqEntry>> {
        let query = format!(
            "SELECT task_id, task_data, retry_count, error_message, dlq_timestamp, original_timestamp, worker_hostname, metadata FROM {} ORDER BY dlq_timestamp DESC",
            self.table_name
        );

        let rows: Vec<(
            uuid::Uuid,
            sqlx::types::JsonValue,
            i32,
            String,
            i64,
            i64,
            String,
            sqlx::types::JsonValue,
        )> = sqlx::query_as(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Failed to fetch DLQ entries: {}", e))
            })?;

        let mut entries = Vec::new();
        for (
            task_id,
            task_data,
            retry_count,
            error_message,
            dlq_timestamp,
            original_timestamp,
            worker_hostname,
            metadata,
        ) in rows
        {
            let task: celers_core::SerializedTask = serde_json::from_value(task_data)
                .unwrap_or_else(|_| {
                    error!("Failed to deserialize task data for {}", task_id);
                    celers_core::SerializedTask {
                        metadata: celers_core::TaskMetadata::new("unknown".to_string()),
                        payload: vec![],
                    }
                });

            let metadata_map: HashMap<String, String> =
                serde_json::from_value(metadata).unwrap_or_default();

            entries.push(DlqEntry {
                task,
                task_id,
                retry_count: retry_count as u32,
                error_message,
                dlq_timestamp: dlq_timestamp as u64,
                original_timestamp: original_timestamp as u64,
                worker_hostname,
                metadata: metadata_map,
            });
        }

        Ok(entries)
    }

    async fn remove(&self, task_id: &TaskId) -> Result<bool> {
        let query = format!("DELETE FROM {} WHERE task_id = $1", self.table_name);

        let result = sqlx::query(&query)
            .bind(task_id)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Failed to delete DLQ entry: {}", e))
            })?;

        debug!("Removed task {} from PostgreSQL DLQ", task_id);
        Ok(result.rows_affected() > 0)
    }

    async fn remove_batch(&self, task_ids: &[TaskId]) -> Result<usize> {
        if task_ids.is_empty() {
            return Ok(0);
        }

        // Build a query with IN clause
        let placeholders: Vec<String> = (1..=task_ids.len()).map(|i| format!("${}", i)).collect();
        let query = format!(
            "DELETE FROM {} WHERE task_id IN ({})",
            self.table_name,
            placeholders.join(", ")
        );

        let mut query_builder = sqlx::query(&query);
        for task_id in task_ids {
            query_builder = query_builder.bind(task_id);
        }

        let result = query_builder.execute(&self.pool).await.map_err(|e| {
            celers_core::CelersError::Other(format!("Failed to delete DLQ entries: {}", e))
        })?;

        let removed = result.rows_affected() as usize;
        info!("Removed {} entries from PostgreSQL DLQ in batch", removed);
        Ok(removed)
    }

    async fn clear(&self) -> Result<usize> {
        let query = format!("DELETE FROM {}", self.table_name);

        let result = sqlx::query(&query)
            .execute(&self.pool)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Failed to clear DLQ: {}", e)))?;

        let count = result.rows_affected() as usize;
        info!("Cleared {} entries from PostgreSQL DLQ", count);
        Ok(count)
    }

    async fn size(&self) -> Result<usize> {
        let query = format!("SELECT COUNT(*) FROM {}", self.table_name);

        let (count,): (i64,) = sqlx::query_as(&query)
            .fetch_one(&self.pool)
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Failed to count DLQ entries: {}", e))
            })?;

        Ok(count as usize)
    }

    async fn get_older_than(&self, age_seconds: u64) -> Result<Vec<DlqEntry>> {
        use std::time::{SystemTime, UNIX_EPOCH};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let cutoff = now.saturating_sub(age_seconds);

        let query = format!(
            "SELECT task_id, task_data, retry_count, error_message, dlq_timestamp, original_timestamp, worker_hostname, metadata FROM {} WHERE dlq_timestamp < $1",
            self.table_name
        );

        let rows: Vec<(
            uuid::Uuid,
            sqlx::types::JsonValue,
            i32,
            String,
            i64,
            i64,
            String,
            sqlx::types::JsonValue,
        )> = sqlx::query_as(&query)
            .bind(cutoff as i64)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Failed to fetch old DLQ entries: {}", e))
            })?;

        let mut entries = Vec::new();
        for (
            task_id,
            task_data,
            retry_count,
            error_message,
            dlq_timestamp,
            original_timestamp,
            worker_hostname,
            metadata,
        ) in rows
        {
            let task: celers_core::SerializedTask = serde_json::from_value(task_data)
                .unwrap_or_else(|_| {
                    error!("Failed to deserialize task data for {}", task_id);
                    celers_core::SerializedTask {
                        metadata: celers_core::TaskMetadata::new("unknown".to_string()),
                        payload: vec![],
                    }
                });

            let metadata_map: HashMap<String, String> =
                serde_json::from_value(metadata).unwrap_or_default();

            entries.push(DlqEntry {
                task,
                task_id,
                retry_count: retry_count as u32,
                error_message,
                dlq_timestamp: dlq_timestamp as u64,
                original_timestamp: original_timestamp as u64,
                worker_hostname,
                metadata: metadata_map,
            });
        }

        Ok(entries)
    }

    async fn get_by_task_name(&self, task_name: &str) -> Result<Vec<DlqEntry>> {
        let query = format!(
            "SELECT task_id, task_data, retry_count, error_message, dlq_timestamp, original_timestamp, worker_hostname, metadata FROM {} WHERE task_name = $1",
            self.table_name
        );

        let rows: Vec<(
            uuid::Uuid,
            sqlx::types::JsonValue,
            i32,
            String,
            i64,
            i64,
            String,
            sqlx::types::JsonValue,
        )> = sqlx::query_as(&query)
            .bind(task_name)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!(
                    "Failed to fetch DLQ entries by task name: {}",
                    e
                ))
            })?;

        let mut entries = Vec::new();
        for (
            task_id,
            task_data,
            retry_count,
            error_message,
            dlq_timestamp,
            original_timestamp,
            worker_hostname,
            metadata,
        ) in rows
        {
            let task: celers_core::SerializedTask = serde_json::from_value(task_data)
                .unwrap_or_else(|_| {
                    error!("Failed to deserialize task data for {}", task_id);
                    celers_core::SerializedTask {
                        metadata: celers_core::TaskMetadata::new("unknown".to_string()),
                        payload: vec![],
                    }
                });

            let metadata_map: HashMap<String, String> =
                serde_json::from_value(metadata).unwrap_or_default();

            entries.push(DlqEntry {
                task,
                task_id,
                retry_count: retry_count as u32,
                error_message,
                dlq_timestamp: dlq_timestamp as u64,
                original_timestamp: original_timestamp as u64,
                worker_hostname,
                metadata: metadata_map,
            });
        }

        Ok(entries)
    }

    async fn cleanup_expired(&self, ttl_seconds: u64) -> Result<usize> {
        use std::time::{SystemTime, UNIX_EPOCH};

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let cutoff = now.saturating_sub(ttl_seconds);

        let query = format!("DELETE FROM {} WHERE dlq_timestamp < $1", self.table_name);

        let result = sqlx::query(&query)
            .bind(cutoff as i64)
            .execute(&self.pool)
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Failed to cleanup expired entries: {}", e))
            })?;

        let removed = result.rows_affected() as usize;
        if removed > 0 {
            info!("Removed {} expired entries from PostgreSQL DLQ", removed);
        }

        Ok(removed)
    }

    async fn get_stats_by_task_name(&self) -> Result<HashMap<String, TaskStats>> {
        let query = format!(
            r#"
            SELECT
                task_name,
                COUNT(*) as count,
                SUM(retry_count) as total_retries,
                MAX(retry_count) as max_retries,
                MIN(dlq_timestamp) as min_timestamp,
                MAX(dlq_timestamp) as max_timestamp
            FROM {}
            GROUP BY task_name
            "#,
            self.table_name
        );

        let rows: Vec<(
            String,
            i64,
            Option<i64>,
            Option<i32>,
            Option<i64>,
            Option<i64>,
        )> = sqlx::query_as(&query)
            .fetch_all(&self.pool)
            .await
            .map_err(|e| {
                celers_core::CelersError::Other(format!("Failed to fetch DLQ stats: {}", e))
            })?;

        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut task_stats = HashMap::new();
        for (task_name, count, total_retries, max_retries, min_timestamp, max_timestamp) in rows {
            let stats = TaskStats {
                count: count as usize,
                total_retries: total_retries.unwrap_or(0) as usize,
                max_retries: max_retries.unwrap_or(0) as u32,
                min_age_seconds: now.saturating_sub(max_timestamp.unwrap_or(now as i64) as u64),
                max_age_seconds: now.saturating_sub(min_timestamp.unwrap_or(now as i64) as u64),
            };
            task_stats.insert(task_name, stats);
        }

        Ok(task_stats)
    }

    async fn health_check(&self) -> Result<bool> {
        match sqlx::query("SELECT 1").fetch_one(&self.pool).await {
            Ok(_) => Ok(true),
            Err(e) => {
                warn!("PostgreSQL health check failed: {}", e);
                Ok(false)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::TaskMetadata;

    fn create_test_entry() -> DlqEntry {
        let task = celers_core::SerializedTask {
            metadata: TaskMetadata::new("test_task".to_string()),
            payload: vec![],
        };

        let task_id = task.metadata.id;

        DlqEntry::new(
            task,
            task_id,
            3,
            "Test error".to_string(),
            "worker-1".to_string(),
        )
    }

    #[tokio::test]
    async fn test_memory_storage_add_and_get() {
        let storage = MemoryDlqStorage::new();
        let entry = create_test_entry();
        let task_id = entry.task_id;

        storage.add(entry.clone()).await.unwrap();

        let retrieved = storage.get(&task_id).await.unwrap();
        assert!(retrieved.is_some());
        assert_eq!(retrieved.unwrap().task_id, task_id);
    }

    #[tokio::test]
    async fn test_memory_storage_remove() {
        let storage = MemoryDlqStorage::new();
        let entry = create_test_entry();
        let task_id = entry.task_id;

        storage.add(entry).await.unwrap();
        assert_eq!(storage.size().await.unwrap(), 1);

        let removed = storage.remove(&task_id).await.unwrap();
        assert!(removed);
        assert_eq!(storage.size().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_memory_storage_clear() {
        let storage = MemoryDlqStorage::new();

        for _ in 0..5 {
            storage.add(create_test_entry()).await.unwrap();
        }

        assert_eq!(storage.size().await.unwrap(), 5);

        let cleared = storage.clear().await.unwrap();
        assert_eq!(cleared, 5);
        assert_eq!(storage.size().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn test_memory_storage_get_by_task_name() {
        let storage = MemoryDlqStorage::new();
        storage.add(create_test_entry()).await.unwrap();

        let entries = storage.get_by_task_name("test_task").await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].task.metadata.name, "test_task");
    }

    #[tokio::test]
    async fn test_memory_storage_health_check() {
        let storage = MemoryDlqStorage::new();
        assert!(storage.health_check().await.unwrap());
    }
}
