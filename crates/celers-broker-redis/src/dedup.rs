//! Task deduplication implementation
//!
//! Provides mechanisms to prevent duplicate task processing:
//! - Content-based deduplication (hash of task payload)
//! - Time-window deduplication (prevent re-enqueue within window)
//! - Custom deduplication keys

use celers_core::{CelersError, Result, SerializedTask, TaskId};
use redis::AsyncCommands;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

/// Deduplication key suffix
const DEDUP_KEY_PREFIX: &str = "dedup:";

/// Deduplication strategy
#[derive(Debug, Clone)]
pub enum DedupStrategy {
    /// Deduplicate by task ID only
    ById,
    /// Deduplicate by task name and arguments (content hash)
    ByContent,
    /// Deduplicate using a custom key
    ByKey(String),
}

impl DedupStrategy {
    /// Generate deduplication key for a task
    pub fn generate_key(&self, task: &SerializedTask, queue_name: &str) -> String {
        let base = match self {
            DedupStrategy::ById => task.metadata.id.to_string(),
            DedupStrategy::ByContent => {
                let mut hasher = DefaultHasher::new();
                task.metadata.name.hash(&mut hasher);
                task.payload.hash(&mut hasher);
                format!("{:x}", hasher.finish())
            }
            DedupStrategy::ByKey(key) => key.clone(),
        };

        format!("{}{}:{}", DEDUP_KEY_PREFIX, queue_name, base)
    }
}

/// Result of deduplication check
#[derive(Debug, Clone)]
pub enum DedupResult {
    /// Task is new, not a duplicate
    New,
    /// Task is a duplicate
    Duplicate {
        /// The existing task ID
        existing_task_id: Option<TaskId>,
        /// When the duplicate was first seen
        first_seen_secs: Option<i64>,
    },
}

impl DedupResult {
    /// Check if task is a duplicate
    pub fn is_duplicate(&self) -> bool {
        matches!(self, DedupResult::Duplicate { .. })
    }
}

/// Task deduplicator for preventing duplicate task processing
pub struct Deduplicator {
    client: redis::Client,
    queue_name: String,
    /// Time window for deduplication (how long to remember tasks)
    ttl: Duration,
    /// Deduplication strategy
    strategy: DedupStrategy,
}

impl Deduplicator {
    /// Create a new deduplicator with default settings
    pub fn new(client: redis::Client, queue_name: &str) -> Self {
        Self {
            client,
            queue_name: queue_name.to_string(),
            ttl: Duration::from_secs(3600), // 1 hour default
            strategy: DedupStrategy::ByContent,
        }
    }

    /// Set the TTL for deduplication entries
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    /// Set the deduplication strategy
    pub fn with_strategy(mut self, strategy: DedupStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Check if a task is a duplicate
    pub async fn check(&self, task: &SerializedTask) -> Result<DedupResult> {
        let key = self.strategy.generate_key(task, &self.queue_name);

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        let existing: Option<String> = conn
            .get(&key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to check dedup key: {}", e)))?;

        match existing {
            Some(value) => {
                // Parse existing value (format: "task_id:timestamp")
                let parts: Vec<&str> = value.split(':').collect();
                let existing_task_id = parts.first().and_then(|s| s.parse().ok());
                let first_seen_secs = parts.get(1).and_then(|s| s.parse().ok());

                Ok(DedupResult::Duplicate {
                    existing_task_id,
                    first_seen_secs,
                })
            }
            None => Ok(DedupResult::New),
        }
    }

    /// Mark a task as seen (for deduplication)
    pub async fn mark(&self, task: &SerializedTask) -> Result<()> {
        let key = self.strategy.generate_key(task, &self.queue_name);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CelersError::Other(format!("Time error: {}", e)))?
            .as_secs();

        let value = format!("{}:{}", task.metadata.id, now);

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        conn.set_ex::<_, _, ()>(&key, &value, self.ttl.as_secs())
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to mark task: {}", e)))?;

        Ok(())
    }

    /// Check and mark atomically (returns true if new, false if duplicate)
    pub async fn check_and_mark(&self, task: &SerializedTask) -> Result<bool> {
        let key = self.strategy.generate_key(task, &self.queue_name);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CelersError::Other(format!("Time error: {}", e)))?
            .as_secs();

        let value = format!("{}:{}", task.metadata.id, now);

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        // Use SETNX (SET if Not eXists) for atomic check-and-set
        let set: bool = conn
            .set_nx(&key, &value)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to check and mark: {}", e)))?;

        if set {
            // Set the TTL
            conn.expire::<_, ()>(&key, self.ttl.as_secs() as i64)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to set TTL: {}", e)))?;
        }

        Ok(set)
    }

    /// Remove deduplication entry for a task
    pub async fn unmark(&self, task: &SerializedTask) -> Result<bool> {
        let key = self.strategy.generate_key(task, &self.queue_name);

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        let deleted: i64 = conn
            .del(&key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to unmark task: {}", e)))?;

        Ok(deleted > 0)
    }

    /// Clear all deduplication entries for this queue
    pub async fn clear(&self) -> Result<usize> {
        let pattern = format!("{}{}:*", DEDUP_KEY_PREFIX, self.queue_name);

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        // Use SCAN to find all matching keys
        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to scan keys: {}", e)))?;

        if keys.is_empty() {
            return Ok(0);
        }

        let count = keys.len();

        // Delete all keys
        conn.del::<_, ()>(&keys)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to delete keys: {}", e)))?;

        Ok(count)
    }

    /// Get the number of tracked dedup entries
    pub async fn count(&self) -> Result<usize> {
        let pattern = format!("{}{}:*", DEDUP_KEY_PREFIX, self.queue_name);

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))?;

        let keys: Vec<String> = redis::cmd("KEYS")
            .arg(&pattern)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to count keys: {}", e)))?;

        Ok(keys.len())
    }
}

/// Generate content hash for a task
pub fn content_hash(task: &SerializedTask) -> u64 {
    let mut hasher = DefaultHasher::new();
    task.metadata.name.hash(&mut hasher);
    task.payload.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_task(name: &str, payload: &str) -> SerializedTask {
        SerializedTask::new(name.to_string(), payload.as_bytes().to_vec())
    }

    #[test]
    fn test_dedup_strategy_by_id() {
        let task = create_test_task("test_task", r#"{"arg": 1}"#);
        let strategy = DedupStrategy::ById;

        let key = strategy.generate_key(&task, "my_queue");
        assert!(key.starts_with("dedup:my_queue:"));
        assert!(key.contains(&task.metadata.id.to_string()));
    }

    #[test]
    fn test_dedup_strategy_by_content() {
        let task1 = create_test_task("test_task", r#"{"arg": 1}"#);
        let task2 = create_test_task("test_task", r#"{"arg": 1}"#);
        let task3 = create_test_task("test_task", r#"{"arg": 2}"#);

        let strategy = DedupStrategy::ByContent;

        let key1 = strategy.generate_key(&task1, "my_queue");
        let key2 = strategy.generate_key(&task2, "my_queue");
        let key3 = strategy.generate_key(&task3, "my_queue");

        // Same content should produce same key
        assert_eq!(key1, key2);
        // Different content should produce different key
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_dedup_strategy_by_key() {
        let task = create_test_task("test_task", r#"{"arg": 1}"#);
        let strategy = DedupStrategy::ByKey("custom_key".to_string());

        let key = strategy.generate_key(&task, "my_queue");
        assert_eq!(key, "dedup:my_queue:custom_key");
    }

    #[test]
    fn test_dedup_result() {
        let new = DedupResult::New;
        assert!(!new.is_duplicate());

        let dup = DedupResult::Duplicate {
            existing_task_id: None,
            first_seen_secs: None,
        };
        assert!(dup.is_duplicate());
    }

    #[test]
    fn test_content_hash() {
        let task1 = create_test_task("test_task", r#"{"arg": 1}"#);
        let task2 = create_test_task("test_task", r#"{"arg": 1}"#);
        let task3 = create_test_task("test_task", r#"{"arg": 2}"#);

        assert_eq!(content_hash(&task1), content_hash(&task2));
        assert_ne!(content_hash(&task1), content_hash(&task3));
    }
}
