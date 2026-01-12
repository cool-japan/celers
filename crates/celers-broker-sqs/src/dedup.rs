// Copyright (c) 2025 CeleRS Contributors
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Message deduplication utilities for AWS SQS.
//!
//! This module provides advanced deduplication features beyond SQS's native
//! FIFO queue deduplication, including:
//! - Content-based deduplication for standard queues
//! - In-memory deduplication cache
//! - Time-window based deduplication
//! - Custom deduplication strategies
//!
//! # Examples
//!
//! ```
//! use celers_broker_sqs::dedup::{DeduplicationCache, DeduplicationStrategy};
//! use std::time::Duration;
//!
//! // Create a deduplication cache with 5-minute window
//! let mut cache = DeduplicationCache::new(Duration::from_secs(300));
//!
//! // Check if message is duplicate
//! let message_id = "msg-123";
//! if cache.is_duplicate(message_id, DeduplicationStrategy::MessageId) {
//!     println!("Duplicate message detected!");
//! } else {
//!     cache.mark_processed(message_id);
//!     println!("Processing message...");
//! }
//! ```

use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// Deduplication strategy for identifying duplicate messages
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeduplicationStrategy {
    /// Use message ID for deduplication
    MessageId,
    /// Use content hash for deduplication (same content = duplicate)
    ContentHash,
    /// Use correlation ID for deduplication
    CorrelationId,
    /// Use task name + args hash for deduplication
    TaskSignature,
}

/// Deduplication cache entry
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Timestamp when the message was first seen
    timestamp: SystemTime,
    /// Number of times this message was seen
    seen_count: usize,
}

/// In-memory deduplication cache with time-based expiration
///
/// This cache tracks processed messages and prevents duplicate processing
/// within a configurable time window.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::dedup::DeduplicationCache;
/// use std::time::Duration;
///
/// let mut cache = DeduplicationCache::new(Duration::from_secs(300));
/// assert_eq!(cache.size(), 0);
/// ```
#[derive(Debug, Clone)]
pub struct DeduplicationCache {
    /// Internal cache storage
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
    /// Time window for deduplication
    window: Duration,
    /// Maximum cache size (for memory management)
    max_size: usize,
}

impl DeduplicationCache {
    /// Create a new deduplication cache with the specified time window
    ///
    /// # Arguments
    ///
    /// * `window` - Time window for keeping deduplication entries
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_broker_sqs::dedup::DeduplicationCache;
    /// use std::time::Duration;
    ///
    /// // 5-minute deduplication window
    /// let cache = DeduplicationCache::new(Duration::from_secs(300));
    /// ```
    pub fn new(window: Duration) -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            window,
            max_size: 10_000,
        }
    }

    /// Create a new deduplication cache with custom max size
    ///
    /// # Arguments
    ///
    /// * `window` - Time window for keeping deduplication entries
    /// * `max_size` - Maximum number of entries to keep in cache
    pub fn with_max_size(window: Duration, max_size: usize) -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::new())),
            window,
            max_size,
        }
    }

    /// Check if a message is a duplicate
    ///
    /// Returns `true` if the message was seen within the deduplication window.
    ///
    /// # Arguments
    ///
    /// * `key` - Deduplication key (e.g., message ID, content hash)
    /// * `_strategy` - Deduplication strategy (for future use)
    pub fn is_duplicate(&mut self, key: &str, _strategy: DeduplicationStrategy) -> bool {
        self.cleanup_expired();

        let mut cache = self.cache.lock().unwrap();
        if let Some(entry) = cache.get_mut(key) {
            entry.seen_count += 1;
            true
        } else {
            false
        }
    }

    /// Mark a message as processed
    ///
    /// Adds the message to the deduplication cache.
    ///
    /// # Arguments
    ///
    /// * `key` - Deduplication key to mark as processed
    pub fn mark_processed(&mut self, key: &str) {
        let mut cache = self.cache.lock().unwrap();

        // Enforce max size by removing oldest entries
        if cache.len() >= self.max_size {
            self.evict_oldest(&mut cache);
        }

        cache.insert(
            key.to_string(),
            CacheEntry {
                timestamp: SystemTime::now(),
                seen_count: 1,
            },
        );
    }

    /// Remove expired entries from the cache
    fn cleanup_expired(&mut self) {
        let mut cache = self.cache.lock().unwrap();
        let now = SystemTime::now();

        cache.retain(|_, entry| {
            if let Ok(elapsed) = now.duration_since(entry.timestamp) {
                elapsed < self.window
            } else {
                false
            }
        });
    }

    /// Evict oldest entries when cache is full
    fn evict_oldest(&self, cache: &mut HashMap<String, CacheEntry>) {
        if cache.is_empty() {
            return;
        }

        // Find the oldest entry
        if let Some(oldest_key) = cache
            .iter()
            .min_by_key(|(_, entry)| entry.timestamp)
            .map(|(k, _)| k.clone())
        {
            cache.remove(&oldest_key);
        }
    }

    /// Get the current cache size
    pub fn size(&self) -> usize {
        self.cache.lock().unwrap().len()
    }

    /// Clear all entries from the cache
    pub fn clear(&mut self) {
        self.cache.lock().unwrap().clear();
    }

    /// Get statistics about duplicate detections
    ///
    /// Returns the total number of duplicate detections
    pub fn duplicate_count(&self) -> usize {
        self.cache
            .lock()
            .unwrap()
            .values()
            .filter(|entry| entry.seen_count > 1)
            .count()
    }
}

/// Generate a content hash for deduplication
///
/// Creates a deterministic hash of message content for content-based deduplication.
///
/// # Arguments
///
/// * `task_name` - Name of the task
/// * `args` - Task arguments as JSON
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::dedup::generate_content_hash;
/// use serde_json::json;
///
/// let hash1 = generate_content_hash("tasks.process", &json!({"user_id": 123}));
/// let hash2 = generate_content_hash("tasks.process", &json!({"user_id": 123}));
/// assert_eq!(hash1, hash2);  // Same content = same hash
/// ```
pub fn generate_content_hash(task_name: &str, args: &Value) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    task_name.hash(&mut hasher);
    args.to_string().hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

/// Generate a task signature for deduplication
///
/// Creates a unique signature based on task name and normalized arguments.
/// This is useful for preventing duplicate task executions with the same parameters.
///
/// # Arguments
///
/// * `task_name` - Name of the task
/// * `args` - Task arguments as JSON
/// * `kwargs` - Task keyword arguments as JSON
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::dedup::generate_task_signature;
/// use serde_json::json;
///
/// let sig = generate_task_signature(
///     "tasks.send_email",
///     &json!([]),
///     &json!({"to": "user@example.com", "subject": "Hello"})
/// );
/// println!("Task signature: {}", sig);
/// ```
pub fn generate_task_signature(task_name: &str, args: &Value, kwargs: &Value) -> String {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    task_name.hash(&mut hasher);

    // Sort kwargs keys for deterministic hashing
    if let Value::Object(map) = kwargs {
        let mut sorted_keys: Vec<_> = map.keys().collect();
        sorted_keys.sort();
        for key in sorted_keys {
            key.hash(&mut hasher);
            map[key].to_string().hash(&mut hasher);
        }
    }

    args.to_string().hash(&mut hasher);
    format!("{:x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_deduplication_cache_new() {
        let cache = DeduplicationCache::new(Duration::from_secs(300));
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_deduplication_cache_mark_processed() {
        let mut cache = DeduplicationCache::new(Duration::from_secs(300));
        cache.mark_processed("msg-1");
        assert_eq!(cache.size(), 1);
    }

    #[test]
    fn test_deduplication_cache_is_duplicate() {
        let mut cache = DeduplicationCache::new(Duration::from_secs(300));

        // First time - not a duplicate
        assert!(!cache.is_duplicate("msg-1", DeduplicationStrategy::MessageId));
        cache.mark_processed("msg-1");

        // Second time - is a duplicate
        assert!(cache.is_duplicate("msg-1", DeduplicationStrategy::MessageId));
    }

    #[test]
    fn test_deduplication_cache_clear() {
        let mut cache = DeduplicationCache::new(Duration::from_secs(300));
        cache.mark_processed("msg-1");
        cache.mark_processed("msg-2");
        assert_eq!(cache.size(), 2);

        cache.clear();
        assert_eq!(cache.size(), 0);
    }

    #[test]
    fn test_deduplication_cache_max_size() {
        let mut cache = DeduplicationCache::with_max_size(Duration::from_secs(300), 3);

        cache.mark_processed("msg-1");
        cache.mark_processed("msg-2");
        cache.mark_processed("msg-3");
        assert_eq!(cache.size(), 3);

        // Adding 4th message should evict the oldest
        cache.mark_processed("msg-4");
        assert_eq!(cache.size(), 3);
    }

    #[test]
    fn test_duplicate_count() {
        let mut cache = DeduplicationCache::new(Duration::from_secs(300));

        cache.mark_processed("msg-1");
        assert_eq!(cache.duplicate_count(), 0);

        cache.is_duplicate("msg-1", DeduplicationStrategy::MessageId);
        assert_eq!(cache.duplicate_count(), 1);
    }

    #[test]
    fn test_generate_content_hash() {
        let hash1 = generate_content_hash("tasks.process", &json!({"user_id": 123}));
        let hash2 = generate_content_hash("tasks.process", &json!({"user_id": 123}));
        let hash3 = generate_content_hash("tasks.process", &json!({"user_id": 456}));

        assert_eq!(hash1, hash2); // Same content = same hash
        assert_ne!(hash1, hash3); // Different content = different hash
    }

    #[test]
    fn test_generate_task_signature() {
        let sig1 = generate_task_signature(
            "tasks.send_email",
            &json!([]),
            &json!({"to": "user@example.com", "subject": "Hello"}),
        );
        let sig2 = generate_task_signature(
            "tasks.send_email",
            &json!([]),
            &json!({"subject": "Hello", "to": "user@example.com"}), // Different order
        );
        let sig3 = generate_task_signature(
            "tasks.send_email",
            &json!([]),
            &json!({"to": "other@example.com", "subject": "Hello"}),
        );

        assert_eq!(sig1, sig2); // Same kwargs (different order) = same signature
        assert_ne!(sig1, sig3); // Different kwargs = different signature
    }

    #[test]
    fn test_deduplication_strategy_variants() {
        assert_eq!(
            DeduplicationStrategy::MessageId,
            DeduplicationStrategy::MessageId
        );
        assert_ne!(
            DeduplicationStrategy::MessageId,
            DeduplicationStrategy::ContentHash
        );
    }

    #[test]
    fn test_cache_entry_seen_count() {
        let mut cache = DeduplicationCache::new(Duration::from_secs(300));

        cache.mark_processed("msg-1");
        cache.is_duplicate("msg-1", DeduplicationStrategy::MessageId);
        cache.is_duplicate("msg-1", DeduplicationStrategy::MessageId);

        // Verify seen count is tracked (3 times: 1 mark + 2 is_duplicate)
        let cache_data = cache.cache.lock().unwrap();
        assert_eq!(cache_data.get("msg-1").unwrap().seen_count, 3);
    }
}
