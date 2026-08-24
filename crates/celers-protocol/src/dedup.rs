//! Message deduplication utilities
//!
//! This module provides utilities for preventing duplicate message processing
//! through various deduplication strategies.

use crate::Message;
use std::collections::{HashMap, HashSet, VecDeque};
use std::time::{Duration, Instant};
use uuid::Uuid;

/// FNV-1a (Fowler-Noll-Vo) 64-bit hash.
///
/// A small, dependency-free, non-cryptographic hash with a fixed, versioned
/// definition -- unlike `std::collections::hash_map::DefaultHasher`, whose
/// algorithm is explicitly documented as *not* guaranteed to be stable
/// across Rust releases, FNV-1a's definition never changes. That stability
/// matters here because the digest becomes part of a message's
/// deduplication identity ([`DedupKey::ContentHash`]): a toolchain upgrade
/// must never silently change every content hash, which would make
/// previously-seen messages look brand new. See
/// <http://www.isthe.com/chongo/tech/comp/fnv/> for the reference algorithm
/// and constants.
///
/// `celers-protocol` cannot depend on `celers-kombu` (the dependency runs
/// the other way), so this is a deliberate duplicate of the identical
/// `celers_kombu::utils::fnv1a_hash`.
fn fnv1a_hash(bytes: &[u8]) -> u64 {
    const FNV_OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const FNV_PRIME: u64 = 0x0000_0100_0000_01b3;

    let mut hash = FNV_OFFSET_BASIS;
    for &byte in bytes {
        hash ^= byte as u64;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

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
    ///
    /// Uses [`fnv1a_hash`] rather than `DefaultHasher` so the digest -- and
    /// therefore a message's deduplication identity -- stays stable across
    /// Rust toolchain upgrades. `task` is length-prefixed before being
    /// concatenated with `body` so that distinct `(task, body)` pairs which
    /// would otherwise concatenate to the same byte stream (e.g. task="ab",
    /// body=b"cd" vs. task="abc", body=b"d") can never collide.
    pub fn from_content(message: &Message) -> Self {
        let task_bytes = message.headers.task.as_bytes();
        let body_bytes = message.body.as_slice();
        let mut buf = Vec::with_capacity(task_bytes.len() + body_bytes.len() + 8);
        buf.extend_from_slice(&(task_bytes.len() as u64).to_le_bytes());
        buf.extend_from_slice(task_bytes);
        buf.extend_from_slice(body_bytes);
        Self::ContentHash(fnv1a_hash(&buf))
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

    /// Remove expired entries from the cache.
    ///
    /// `insertion_order` is maintained in strict insertion order and every
    /// entry shares the same fixed `ttl`, so the entry closest to expiring
    /// is always at the front. Popping from the front while it is expired
    /// is therefore equivalent to (and far cheaper than) scanning every
    /// entry on every call: amortised O(1) rather than the O(n) full sweep
    /// this used to perform via `HashMap::retain` on both `entries` and
    /// `insertion_order` - on every single `contains`/`insert` call.
    fn cleanup_expired(&mut self) {
        let now = Instant::now();
        let ttl = self.ttl;

        loop {
            let should_pop = match self.insertion_order.front() {
                Some(key) => match self.entries.get(key) {
                    Some(entry) => now.duration_since(entry.inserted_at) >= ttl,
                    // `insertion_order` and `entries` should always stay in
                    // sync, but if a stale key were ever left behind, drop
                    // it and keep scanning rather than getting stuck.
                    None => true,
                },
                None => false,
            };

            if !should_pop {
                break;
            }

            if let Some(key) = self.insertion_order.pop_front() {
                self.entries.remove(&key);
            }
        }
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
///
/// Pairs the `HashSet` with a [`VecDeque`] recording true insertion order,
/// mirroring `celers_kombu::DeduplicationMiddleware`'s `DedupState`. A bare
/// `HashSet` has no ordering, so evicting via `seen_ids.iter().take(n)` (the
/// previous approach) removed an arbitrary bucket-order prefix that had no
/// relationship to insertion recency -- it could evict an entry seen a
/// moment ago while one seen long before survived indefinitely.
#[derive(Debug, Clone)]
pub struct SimpleDedupSet {
    seen_ids: HashSet<Uuid>,
    insertion_order: VecDeque<Uuid>,
    max_size: usize,
}

impl SimpleDedupSet {
    /// Create a new simple deduplication set
    pub fn new(max_size: usize) -> Self {
        Self {
            seen_ids: HashSet::new(),
            insertion_order: VecDeque::new(),
            max_size,
        }
    }

    /// Check if a message ID has been seen
    pub fn contains(&self, message: &Message) -> bool {
        self.seen_ids.contains(&message.headers.id)
    }

    /// Mark a message ID as seen
    ///
    /// Returns `true` if newly inserted, `false` if already seen.
    ///
    /// Checking membership *before* evicting (rather than evicting
    /// unconditionally once at capacity, as the previous implementation
    /// did) also fixes a correctness bug: at capacity, re-marking an
    /// already-seen id used to evict entries regardless, which could evict
    /// that very id and make the subsequent `insert` report it as newly
    /// seen -- a duplicate silently reported as novel.
    pub fn mark_seen(&mut self, message: &Message) -> bool {
        let id = message.headers.id;

        if self.seen_ids.contains(&id) {
            return false;
        }

        self.seen_ids.insert(id);
        self.insertion_order.push_back(id);

        // Evict true FIFO (oldest-inserted first) once the set exceeds
        // capacity.
        while self.insertion_order.len() > self.max_size {
            if let Some(oldest) = self.insertion_order.pop_front() {
                self.seen_ids.remove(&oldest);
            } else {
                break;
            }
        }

        true
    }

    /// Clear all seen IDs
    pub fn clear(&mut self) {
        self.seen_ids.clear();
        self.insertion_order.clear();
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
    fn test_dedup_cache_cleanup_expires_only_the_expired_prefix() {
        // Regression: cleanup must lazily pop only the (already-expired)
        // front of insertion_order rather than doing a full scan, and must
        // still expire exactly the right entries - no more, no less.
        let ttl = Duration::from_millis(50);
        let mut cache = DedupCache::new(100, ttl);

        // Insert three entries in order, then deterministically backdate
        // the first two past the TTL (no real sleeping involved).
        for i in 0..3 {
            cache.insert(DedupKey::custom(format!("k{i}")));
        }
        for i in 0..2 {
            let key = DedupKey::custom(format!("k{i}"));
            if let Some(entry) = cache.entries.get_mut(&key) {
                entry.inserted_at = Instant::now() - Duration::from_secs(10);
            }
        }
        assert_eq!(cache.len(), 3);

        // Triggering cleanup (via `contains`) must expire only the two
        // backdated entries and stop as soon as it reaches the fresh one.
        assert!(cache.contains(&DedupKey::custom("k2")));
        assert_eq!(cache.len(), 1);
        assert!(!cache.entries.contains_key(&DedupKey::custom("k0")));
        assert!(!cache.entries.contains_key(&DedupKey::custom("k1")));
        assert!(cache.entries.contains_key(&DedupKey::custom("k2")));
        // insertion_order must have been kept in sync with entries.
        assert_eq!(cache.insertion_order.len(), 1);
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

    #[test]
    fn test_simple_dedup_set_eviction_is_true_fifo() {
        // Regression: eviction must remove the actual oldest-inserted ids,
        // not an arbitrary bucket-order prefix of the underlying HashSet.
        let mut dedup = SimpleDedupSet::new(3);
        let messages: Vec<Message> = (0..3)
            .map(|i| create_test_message(&format!("task{}", i)))
            .collect();

        for msg in &messages {
            assert!(dedup.mark_seen(msg));
        }
        assert_eq!(dedup.len(), 3);

        // A 4th distinct id must evict messages[0] specifically (the
        // oldest), never messages[1] or messages[2].
        let msg4 = create_test_message("task3");
        assert!(dedup.mark_seen(&msg4));

        assert!(
            !dedup.contains(&messages[0]),
            "the oldest entry must be the one evicted"
        );
        assert!(
            dedup.contains(&messages[1]),
            "newer entries must survive eviction"
        );
        assert!(
            dedup.contains(&messages[2]),
            "newer entries must survive eviction"
        );
        assert!(dedup.contains(&msg4));
        assert_eq!(dedup.len(), 3);
    }

    #[test]
    fn test_simple_dedup_set_duplicate_at_capacity_does_not_evict_or_report_novel() {
        // Regression: at capacity, re-marking an *already-seen* id used to
        // evict entries unconditionally before checking for the duplicate,
        // which could evict that very id and make the reinsertion below
        // report it as newly seen -- a duplicate silently treated as novel.
        let mut dedup = SimpleDedupSet::new(2);
        let msg1 = create_test_message("task1");
        let msg2 = create_test_message("task2");

        assert!(dedup.mark_seen(&msg1));
        assert!(dedup.mark_seen(&msg2));
        assert_eq!(dedup.len(), 2);

        // Re-marking msg1 (already seen, set is at capacity) must report a
        // duplicate and must not disturb membership.
        assert!(!dedup.mark_seen(&msg1));
        assert_eq!(dedup.len(), 2);
        assert!(dedup.contains(&msg1));
        assert!(dedup.contains(&msg2));
    }

    #[test]
    fn test_dedup_key_from_content_length_prefix_avoids_ambiguous_concatenation() {
        // Without a length prefix on `task`, ("ab", b"cd") and ("abc", b"d")
        // would concatenate to the identical byte stream "abcd" and hash
        // equal even though they are different (task, body) pairs.
        let msg1 = Message::new("ab".to_string(), Uuid::new_v4(), b"cd".to_vec());
        let msg2 = Message::new("abc".to_string(), Uuid::new_v4(), b"d".to_vec());

        let key1 = DedupKey::from_content(&msg1);
        let key2 = DedupKey::from_content(&msg2);

        assert_ne!(
            key1, key2,
            "length-prefixing task must prevent this collision"
        );
    }
}
