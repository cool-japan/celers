//! Message deduplication utilities
//!
//! This module provides utilities for preventing duplicate message processing
//! through various deduplication strategies.

use crate::Message;
use std::collections::{HashMap, HashSet, VecDeque};
use std::hash::{Hash, Hasher};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// Deduplication key for a message
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum DedupKey {
    /// Use the message task ID
    TaskId(Uuid),
    /// Use the task name and arguments hash
    ContentHash(u64),
    /// Custom key
    Custom(String),
}

impl DedupKey {
    /// Create a dedup key from a message's task ID
    pub fn from_task_id(message: &Message) -> Self {
        Self::TaskId(message.headers.id)
    }

    /// Create a dedup key from message content hash
    pub fn from_content(message: &Message) -> Self {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        message.headers.task.hash(&mut hasher);
        message.body.hash(&mut hasher);
        Self::ContentHash(hasher.finish())
    }

    /// Create a custom dedup key
    pub fn custom(key: impl Into<String>) -> Self {
        Self::Custom(key.into())
    }
}

/// Entry in the deduplication cache with expiry
#[derive(Debug, Clone)]
struct DedupEntry {
    inserted_at: Instant,
}

/// Message deduplication cache
#[derive(Debug, Clone)]
pub struct DedupCache {
    entries: HashMap<DedupKey, DedupEntry>,
    max_size: usize,
    ttl: Duration,
    insertion_order: VecDeque<DedupKey>,
}

impl DedupCache {
    /// Create a new deduplication cache
    ///
    /// # Arguments
    ///
    /// * `max_size` - Maximum number of entries to cache
    /// * `ttl` - Time-to-live for each entry
    pub fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            entries: HashMap::new(),
            max_size,
            ttl,
            insertion_order: VecDeque::new(),
        }
    }

    /// Create a cache with default settings (10000 entries, 1 hour TTL)
    pub fn with_defaults() -> Self {
        Self::new(10000, Duration::from_secs(3600))
    }

    /// Check if a key has been seen recently
    pub fn contains(&mut self, key: &DedupKey) -> bool {
        self.cleanup_expired();
        self.entries.contains_key(key)
    }

    /// Insert a key into the cache
    ///
    /// Returns `true` if the key was newly inserted, `false` if it already existed
    pub fn insert(&mut self, key: DedupKey) -> bool {
        self.cleanup_expired();

        if self.entries.contains_key(&key) {
            return false;
        }

        // Evict oldest entry if at capacity
        if self.entries.len() >= self.max_size {
            if let Some(oldest_key) = self.insertion_order.pop_front() {
                self.entries.remove(&oldest_key);
            }
        }

        let entry = DedupEntry {
            inserted_at: Instant::now(),
        };

        self.entries.insert(key.clone(), entry);
        self.insertion_order.push_back(key);
        true
    }

    /// Check if a message is a duplicate
    ///
    /// Returns `true` if the message has been seen before, `false` otherwise
    pub fn is_duplicate(&mut self, message: &Message, use_content_hash: bool) -> bool {
        let key = if use_content_hash {
            DedupKey::from_content(message)
        } else {
            DedupKey::from_task_id(message)
        };

        self.contains(&key)
    }

    /// Mark a message as seen
    ///
    /// Returns `true` if this is the first time seeing this message, `false` if duplicate
    pub fn mark_seen(&mut self, message: &Message, use_content_hash: bool) -> bool {
        let key = if use_content_hash {
            DedupKey::from_content(message)
        } else {
            DedupKey::from_task_id(message)
        };

        self.insert(key)
    }

    /// Remove expired entries from the cache
    fn cleanup_expired(&mut self) {
        let now = Instant::now();
        let ttl = self.ttl;

        // Remove expired entries
        self.entries
            .retain(|_, entry| now.duration_since(entry.inserted_at) < ttl);

        // Clean up insertion order
        self.insertion_order
            .retain(|key| self.entries.contains_key(key));
    }

    /// Clear all entries from the cache
    pub fn clear(&mut self) {
        self.entries.clear();
        self.insertion_order.clear();
    }

    /// Get the number of entries in the cache
    #[inline]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the cache is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
}

/// Simple deduplication using a HashSet of task IDs
#[derive(Debug, Clone)]
pub struct SimpleDedupSet {
    seen_ids: HashSet<Uuid>,
    max_size: usize,
}

impl SimpleDedupSet {
    /// Create a new simple deduplication set
    pub fn new(max_size: usize) -> Self {
        Self {
            seen_ids: HashSet::new(),
            max_size,
        }
    }

    /// Check if a message ID has been seen
    pub fn contains(&self, message: &Message) -> bool {
        self.seen_ids.contains(&message.headers.id)
    }

    /// Mark a message ID as seen
    ///
    /// Returns `true` if newly inserted, `false` if already seen
    pub fn mark_seen(&mut self, message: &Message) -> bool {
        if self.seen_ids.len() >= self.max_size {
            // Simple eviction: clear half the set
            let to_remove: Vec<_> = self
                .seen_ids
                .iter()
                .take(self.max_size / 2)
                .copied()
                .collect();
            for id in to_remove {
                self.seen_ids.remove(&id);
            }
        }

        self.seen_ids.insert(message.headers.id)
    }

    /// Clear all seen IDs
    pub fn clear(&mut self) {
        self.seen_ids.clear();
    }

    /// Get the number of seen IDs
    #[inline]
    pub fn len(&self) -> usize {
        self.seen_ids.len()
    }

    /// Check if the set is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.seen_ids.is_empty()
    }
}

/// Filter out duplicate messages from a collection
pub fn filter_duplicates(messages: Vec<Message>) -> Vec<Message> {
    let mut seen = HashSet::new();
    messages
        .into_iter()
        .filter(|msg| seen.insert(msg.headers.id))
        .collect()
}

/// Filter duplicates based on content hash
pub fn filter_duplicates_by_content(messages: Vec<Message>) -> Vec<Message> {
    let mut seen = HashSet::new();
    messages
        .into_iter()
        .filter(|msg| {
            let key = DedupKey::from_content(msg);
            seen.insert(key)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::MessageBuilder;

    fn create_test_message(task: &str) -> Message {
        MessageBuilder::new(task).build().unwrap()
    }

    #[test]
    fn test_dedup_key_from_task_id() {
        let msg = create_test_message("task1");
        let key = DedupKey::from_task_id(&msg);

        match key {
            DedupKey::TaskId(id) => assert_eq!(id, msg.headers.id),
            _ => panic!("Expected TaskId"),
        }
    }

    #[test]
    fn test_dedup_key_from_content() {
        let msg1 = MessageBuilder::new("task1")
            .args(vec![serde_json::json!(42)])
            .build()
            .unwrap();

        let msg2 = MessageBuilder::new("task1")
            .args(vec![serde_json::json!(42)])
            .build()
            .unwrap();

        let key1 = DedupKey::from_content(&msg1);
        let key2 = DedupKey::from_content(&msg2);

        // Same content should produce same hash
        assert_eq!(key1, key2);
    }

    #[test]
    fn test_dedup_cache_insert() {
        let mut cache = DedupCache::new(3, Duration::from_secs(60));
        let msg1 = create_test_message("task1");
        let msg2 = create_test_message("task2");

        assert!(cache.mark_seen(&msg1, false));
        assert!(!cache.mark_seen(&msg1, false)); // Duplicate
        assert!(cache.mark_seen(&msg2, false));
    }

    #[test]
    fn test_dedup_cache_is_duplicate() {
        let mut cache = DedupCache::new(3, Duration::from_secs(60));
        let msg = create_test_message("task1");

        assert!(!cache.is_duplicate(&msg, false));
        cache.mark_seen(&msg, false);
        assert!(cache.is_duplicate(&msg, false));
    }

    #[test]
    fn test_dedup_cache_eviction() {
        let mut cache = DedupCache::new(2, Duration::from_secs(60));
        let msg1 = create_test_message("task1");
        let msg2 = create_test_message("task2");
        let msg3 = create_test_message("task3");

        cache.mark_seen(&msg1, false);
        cache.mark_seen(&msg2, false);
        assert_eq!(cache.len(), 2);

        // Should evict oldest (msg1)
        cache.mark_seen(&msg3, false);
        assert_eq!(cache.len(), 2);
    }

    #[test]
    fn test_dedup_cache_content_hash() {
        let mut cache = DedupCache::new(10, Duration::from_secs(60));

        let msg1 = MessageBuilder::new("task1")
            .args(vec![serde_json::json!(1)])
            .build()
            .unwrap();

        let msg2 = MessageBuilder::new("task1")
            .args(vec![serde_json::json!(1)])
            .build()
            .unwrap();

        assert!(cache.mark_seen(&msg1, true));
        assert!(!cache.mark_seen(&msg2, true)); // Same content, different ID
    }

    #[test]
    fn test_simple_dedup_set() {
        let mut dedup = SimpleDedupSet::new(10);
        let msg1 = create_test_message("task1");
        let msg2 = create_test_message("task2");

        assert!(dedup.mark_seen(&msg1));
        assert!(!dedup.mark_seen(&msg1)); // Duplicate
        assert!(dedup.mark_seen(&msg2));

        assert!(dedup.contains(&msg1));
        assert!(dedup.contains(&msg2));
    }

    #[test]
    fn test_filter_duplicates() {
        let msg1 = create_test_message("task1");
        let msg2 = create_test_message("task2");
        let msg1_dup = msg1.clone();

        let messages = vec![msg1, msg2, msg1_dup];
        let filtered = filter_duplicates(messages);

        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_filter_duplicates_by_content() {
        let msg1 = MessageBuilder::new("task1")
            .args(vec![serde_json::json!(1)])
            .build()
            .unwrap();

        let msg2 = MessageBuilder::new("task1")
            .args(vec![serde_json::json!(1)])
            .build()
            .unwrap();

        let msg3 = MessageBuilder::new("task2")
            .args(vec![serde_json::json!(2)])
            .build()
            .unwrap();

        let messages = vec![msg1, msg2, msg3];
        let filtered = filter_duplicates_by_content(messages);

        // msg1 and msg2 have same content, should be deduplicated
        assert_eq!(filtered.len(), 2);
    }

    #[test]
    fn test_dedup_cache_clear() {
        let mut cache = DedupCache::new(10, Duration::from_secs(60));
        let msg = create_test_message("task1");

        cache.mark_seen(&msg, false);
        assert_eq!(cache.len(), 1);

        cache.clear();
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());
    }

    #[test]
    fn test_simple_dedup_set_eviction() {
        let mut dedup = SimpleDedupSet::new(4);

        for i in 0..6 {
            let msg = create_test_message(&format!("task{}", i));
            dedup.mark_seen(&msg);
        }

        // Should have evicted some entries
        assert!(dedup.len() <= 4);
    }
}
