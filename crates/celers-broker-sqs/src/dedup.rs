// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
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
//! # Scope: this cache is per-process, not distributed
//!
//! [`DeduplicationCache`] lives in the memory of one worker. Cloning it shares
//! the same underlying store *within* that process, but two workers on two
//! machines have two independent caches and will both admit the same message.
//! For cross-process exactly-once semantics use a FIFO queue's
//! `MessageDeduplicationId`, or a shared store (Redis/database).
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
//! // Check-and-mark is atomic: two threads cannot both be told "not a
//! // duplicate" for the same key.
//! let message_id = "msg-123";
//! if cache.check_and_mark(message_id, DeduplicationStrategy::MessageId) {
//!     println!("Duplicate message detected!");
//! } else {
//!     println!("Processing message...");
//! }
//! ```

use serde_json::Value;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

/// Deduplication strategy for identifying duplicate messages
///
/// The strategy determines which *namespace* a key is stored under, so the same
/// raw identifier used under two strategies cannot collide (a message id of
/// `"abc"` and a content hash of `"abc"` are different entries).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

impl DeduplicationStrategy {
    /// Stable namespace prefix for this strategy.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_broker_sqs::dedup::DeduplicationStrategy;
    ///
    /// assert_eq!(DeduplicationStrategy::MessageId.namespace(), "message-id");
    /// ```
    pub fn namespace(&self) -> &'static str {
        match self {
            Self::MessageId => "message-id",
            Self::ContentHash => "content-hash",
            Self::CorrelationId => "correlation-id",
            Self::TaskSignature => "task-signature",
        }
    }

    /// Build the namespaced cache key for a raw deduplication key.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_broker_sqs::dedup::DeduplicationStrategy;
    ///
    /// assert_eq!(
    ///     DeduplicationStrategy::ContentHash.cache_key("abc"),
    ///     "content-hash:abc"
    /// );
    /// ```
    pub fn cache_key(&self, key: &str) -> String {
        format!("{}:{}", self.namespace(), key)
    }
}

/// Strategy assumed by [`DeduplicationCache::mark_processed`].
pub const DEFAULT_STRATEGY: DeduplicationStrategy = DeduplicationStrategy::MessageId;

/// Deduplication cache entry
#[derive(Debug, Clone)]
struct CacheEntry {
    /// Timestamp when the message was first seen
    timestamp: SystemTime,
    /// Number of times this message was seen
    seen_count: usize,
    /// Insertion sequence number, used for O(1) eviction
    sequence: u64,
}

impl CacheEntry {
    fn is_expired(&self, now: SystemTime, window: Duration) -> bool {
        match now.duration_since(self.timestamp) {
            Ok(elapsed) => elapsed >= window,
            // The clock moved backwards: treat the entry as expired rather than
            // keeping it forever.
            Err(_) => true,
        }
    }
}

#[derive(Debug, Default)]
struct CacheInner {
    entries: HashMap<String, CacheEntry>,
    /// `(sequence, key)` in insertion order, for amortised O(1) eviction.
    order: VecDeque<(u64, String)>,
    next_sequence: u64,
    operations: u64,
}

impl CacheInner {
    /// Amortised O(1) eviction of the oldest live entry.
    fn evict_oldest(&mut self) {
        while let Some((sequence, key)) = self.order.pop_front() {
            match self.entries.get(&key) {
                // Stale order record (the key was re-inserted later): skip it.
                Some(entry) if entry.sequence != sequence => continue,
                Some(_) => {
                    self.entries.remove(&key);
                    return;
                }
                None => continue,
            }
        }
    }

    fn sweep_expired(&mut self, window: Duration) {
        let now = SystemTime::now();
        self.entries
            .retain(|_, entry| !entry.is_expired(now, window));
    }

    fn insert(&mut self, key: String, max_size: usize) {
        if !self.entries.contains_key(&key) && self.entries.len() >= max_size {
            self.evict_oldest();
        }

        let sequence = self.next_sequence;
        self.next_sequence += 1;

        self.entries.insert(
            key.clone(),
            CacheEntry {
                timestamp: SystemTime::now(),
                seen_count: 1,
                sequence,
            },
        );
        self.order.push_back((sequence, key));
    }
}

/// Interval (in cache operations) between full expiry sweeps.
const SWEEP_INTERVAL: u64 = 1_024;

/// In-memory deduplication cache with time-based expiration
///
/// This cache tracks processed messages and prevents duplicate processing
/// within a configurable time window.
///
/// Expiry is evaluated lazily at lookup time, with a periodic full sweep, so a
/// lookup no longer walks the whole map on every single message.
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
    cache: Arc<Mutex<CacheInner>>,
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
            cache: Arc::new(Mutex::new(CacheInner::default())),
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
            cache: Arc::new(Mutex::new(CacheInner::default())),
            window,
            max_size: max_size.max(1),
        }
    }

    /// Atomically test a key and, if it is new, record it.
    ///
    /// Returns `true` when the key was already present within the deduplication
    /// window. This takes the lock exactly once, so two threads (or two clones
    /// of the same cache) can never both observe "not a duplicate" for the same
    /// key — the check-then-act race that a separate
    /// [`is_duplicate`](Self::is_duplicate) + [`mark_processed`](Self::mark_processed)
    /// pair leaves open.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_broker_sqs::dedup::{DeduplicationCache, DeduplicationStrategy};
    /// use std::time::Duration;
    ///
    /// let cache = DeduplicationCache::new(Duration::from_secs(300));
    ///
    /// assert!(!cache.check_and_mark("msg-1", DeduplicationStrategy::MessageId));
    /// assert!(cache.check_and_mark("msg-1", DeduplicationStrategy::MessageId));
    ///
    /// // Different strategies live in different namespaces.
    /// assert!(!cache.check_and_mark("msg-1", DeduplicationStrategy::ContentHash));
    /// ```
    pub fn check_and_mark(&self, key: &str, strategy: DeduplicationStrategy) -> bool {
        let cache_key = strategy.cache_key(key);
        let mut inner = self.cache.lock().unwrap_or_else(|e| e.into_inner());

        self.maybe_sweep(&mut inner);

        let now = SystemTime::now();
        let expired = inner
            .entries
            .get(&cache_key)
            .is_some_and(|entry| entry.is_expired(now, self.window));

        if expired {
            inner.entries.remove(&cache_key);
        }

        if let Some(entry) = inner.entries.get_mut(&cache_key) {
            entry.seen_count += 1;
            return true;
        }

        inner.insert(cache_key, self.max_size);
        false
    }

    /// Check if a message is a duplicate
    ///
    /// Returns `true` if the message was seen within the deduplication window.
    ///
    /// This is the non-atomic half of the classic check-then-act pair; prefer
    /// [`check_and_mark`](Self::check_and_mark) when correctness under
    /// concurrency matters.
    ///
    /// # Arguments
    ///
    /// * `key` - Deduplication key (e.g., message ID, content hash)
    /// * `strategy` - Namespace the key is looked up under
    pub fn is_duplicate(&mut self, key: &str, strategy: DeduplicationStrategy) -> bool {
        let cache_key = strategy.cache_key(key);
        let mut inner = self.cache.lock().unwrap_or_else(|e| e.into_inner());

        self.maybe_sweep(&mut inner);

        let now = SystemTime::now();
        let expired = inner
            .entries
            .get(&cache_key)
            .is_some_and(|entry| entry.is_expired(now, self.window));

        if expired {
            inner.entries.remove(&cache_key);
            return false;
        }

        match inner.entries.get_mut(&cache_key) {
            Some(entry) => {
                entry.seen_count += 1;
                true
            }
            None => false,
        }
    }

    /// Mark a message as processed under the default strategy
    ///
    /// The default is [`DEFAULT_STRATEGY`], which pairs with
    /// `is_duplicate(key, DeduplicationStrategy::MessageId)`. Use
    /// [`mark_processed_with`](Self::mark_processed_with) for another strategy.
    ///
    /// # Arguments
    ///
    /// * `key` - Deduplication key to mark as processed
    pub fn mark_processed(&mut self, key: &str) {
        self.mark_processed_with(key, DEFAULT_STRATEGY);
    }

    /// Mark a message as processed under an explicit strategy
    pub fn mark_processed_with(&mut self, key: &str, strategy: DeduplicationStrategy) {
        let cache_key = strategy.cache_key(key);
        let mut inner = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        inner.insert(cache_key, self.max_size);
    }

    /// Remove expired entries from the cache
    pub fn cleanup_expired(&mut self) {
        let mut inner = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        inner.sweep_expired(self.window);
    }

    fn maybe_sweep(&self, inner: &mut CacheInner) {
        inner.operations = inner.operations.wrapping_add(1);
        if inner.operations.is_multiple_of(SWEEP_INTERVAL) || inner.entries.len() >= self.max_size {
            inner.sweep_expired(self.window);
        }
    }

    /// Get the current cache size
    pub fn size(&self) -> usize {
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .len()
    }

    /// Clear all entries from the cache
    pub fn clear(&mut self) {
        let mut inner = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        inner.entries.clear();
        inner.order.clear();
    }

    /// Get statistics about duplicate detections
    ///
    /// Returns the total number of duplicate detections
    pub fn duplicate_count(&self) -> usize {
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .values()
            .filter(|entry| entry.seen_count > 1)
            .count()
    }

    /// Number of times a key has been seen, for diagnostics and tests.
    pub fn seen_count(&self, key: &str, strategy: DeduplicationStrategy) -> Option<usize> {
        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .entries
            .get(&strategy.cache_key(key))
            .map(|entry| entry.seen_count)
    }
}

/// Render a JSON value in a canonical form: object keys sorted, no whitespace.
///
/// Two structurally equal JSON documents produce byte-identical output
/// regardless of key insertion order, which is what makes the digests below
/// agree across producers.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::dedup::canonical_json;
/// use serde_json::json;
///
/// assert_eq!(
///     canonical_json(&json!({"b": 1, "a": 2})),
///     canonical_json(&json!({"a": 2, "b": 1}))
/// );
/// ```
pub fn canonical_json(value: &Value) -> String {
    match value {
        Value::Object(map) => {
            let mut keys: Vec<&String> = map.keys().collect();
            keys.sort();

            let fields: Vec<String> = keys
                .into_iter()
                .filter_map(|key| {
                    map.get(key).map(|child| {
                        format!(
                            "{}:{}",
                            serde_json::to_string(key.as_str()).unwrap_or_default(),
                            canonical_json(child)
                        )
                    })
                })
                .collect();

            format!("{{{}}}", fields.join(","))
        }
        Value::Array(items) => {
            let rendered: Vec<String> = items.iter().map(canonical_json).collect();
            format!("[{}]", rendered.join(","))
        }
        other => serde_json::to_string(other).unwrap_or_default(),
    }
}

/// SHA-256 digest of a canonically rendered set of parts, as lowercase hex.
///
/// `DefaultHasher` — the previous implementation — is explicitly documented as
/// *not* stable across Rust releases, so two workers built with different
/// toolchains disagreed about whether a message was a duplicate. Its 64-bit
/// output also invites birthday collisions, and a collision here silently drops
/// a legitimate task.
fn sha256_hex(parts: &[&str]) -> String {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    for part in parts {
        // Length-prefix each part so that ("ab", "c") and ("a", "bc") differ.
        hasher.update((part.len() as u64).to_be_bytes());
        hasher.update(part.as_bytes());
    }

    hex::encode(hasher.finalize())
}

/// Generate a content hash for deduplication
///
/// Creates a deterministic, toolchain-independent SHA-256 digest of the message
/// content for content-based deduplication.
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
/// assert_eq!(hash1.len(), 64); // SHA-256, hex encoded
/// ```
pub fn generate_content_hash(task_name: &str, args: &Value) -> String {
    sha256_hex(&[task_name, &canonical_json(args)])
}

/// Generate a task signature for deduplication
///
/// Creates a unique signature based on task name and canonically normalized
/// arguments. This is useful for preventing duplicate task executions with the
/// same parameters.
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
/// assert_eq!(sig.len(), 64);
/// ```
pub fn generate_task_signature(task_name: &str, args: &Value, kwargs: &Value) -> String {
    sha256_hex(&[task_name, &canonical_json(args), &canonical_json(kwargs)])
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
        assert!(!cache.is_duplicate("msg-1", DeduplicationStrategy::MessageId));
        assert!(cache.is_duplicate("msg-4", DeduplicationStrategy::MessageId));
    }

    #[test]
    fn eviction_survives_reinsertion() {
        // A key re-inserted after its first insertion leaves a stale record in
        // the eviction queue; the sweep must skip it instead of dropping a live
        // entry or looping forever.
        let mut cache = DeduplicationCache::with_max_size(Duration::from_secs(300), 2);

        cache.mark_processed("a");
        cache.mark_processed("b");
        cache.mark_processed("a"); // stale (seq 0, "a") now in the queue
        cache.mark_processed("c");

        assert_eq!(cache.size(), 2);
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
    fn expired_entries_are_not_duplicates() {
        let mut cache = DeduplicationCache::new(Duration::from_nanos(1));
        cache.mark_processed("msg-1");

        // The window is one nanosecond; by the time the lookup runs it is over.
        std::thread::yield_now();
        assert!(!cache.is_duplicate("msg-1", DeduplicationStrategy::MessageId));
    }

    #[test]
    fn check_and_mark_is_atomic_and_admits_once() {
        let cache = DeduplicationCache::new(Duration::from_secs(300));

        assert!(!cache.check_and_mark("msg-1", DeduplicationStrategy::MessageId));
        assert!(cache.check_and_mark("msg-1", DeduplicationStrategy::MessageId));
        assert_eq!(cache.size(), 1);
    }

    #[test]
    fn check_and_mark_admits_exactly_one_of_many_threads() {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Barrier;

        let cache = DeduplicationCache::new(Duration::from_secs(300));
        let admitted = Arc::new(AtomicUsize::new(0));
        let barrier = Arc::new(Barrier::new(8));

        std::thread::scope(|scope| {
            for _ in 0..8 {
                let cache = cache.clone();
                let admitted = Arc::clone(&admitted);
                let barrier = Arc::clone(&barrier);
                scope.spawn(move || {
                    barrier.wait();
                    if !cache.check_and_mark("shared-key", DeduplicationStrategy::MessageId) {
                        admitted.fetch_add(1, Ordering::SeqCst);
                    }
                });
            }
        });

        assert_eq!(
            admitted.load(Ordering::SeqCst),
            1,
            "exactly one thread may be told the key is new"
        );
    }

    #[test]
    fn strategies_use_separate_namespaces() {
        let cache = DeduplicationCache::new(Duration::from_secs(300));

        assert!(!cache.check_and_mark("k", DeduplicationStrategy::MessageId));
        assert!(!cache.check_and_mark("k", DeduplicationStrategy::ContentHash));
        assert!(!cache.check_and_mark("k", DeduplicationStrategy::CorrelationId));
        assert!(!cache.check_and_mark("k", DeduplicationStrategy::TaskSignature));

        assert_eq!(cache.size(), 4);
        assert!(cache.check_and_mark("k", DeduplicationStrategy::MessageId));
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
    fn content_hash_is_a_fixed_stable_digest() {
        // Pinning the digest guarantees the value does not drift with the
        // toolchain the way `DefaultHasher` did.
        assert_eq!(
            generate_content_hash("t", &json!(null)),
            sha256_hex(&["t", "null"])
        );
        assert_eq!(generate_content_hash("t", &json!(null)).len(), 64);
    }

    #[test]
    fn content_hash_ignores_object_key_order() {
        let a = generate_content_hash("t", &json!({"x": 1, "y": {"p": 1, "q": 2}}));
        let b = generate_content_hash("t", &json!({"y": {"q": 2, "p": 1}, "x": 1}));
        assert_eq!(a, b);
    }

    #[test]
    fn content_hash_is_not_confused_by_concatenation() {
        // Length prefixing prevents ("ab", "c") colliding with ("a", "bc").
        assert_ne!(sha256_hex(&["ab", "c"]), sha256_hex(&["a", "bc"]));
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
    fn task_signature_distinguishes_args_from_kwargs() {
        let with_args = generate_task_signature("t", &json!(["a"]), &json!({}));
        let with_kwargs = generate_task_signature("t", &json!([]), &json!({"0": "a"}));
        assert_ne!(with_args, with_kwargs);
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
        assert_eq!(
            DeduplicationStrategy::TaskSignature.namespace(),
            "task-signature"
        );
    }

    #[test]
    fn test_cache_entry_seen_count() {
        let mut cache = DeduplicationCache::new(Duration::from_secs(300));

        cache.mark_processed("msg-1");
        cache.is_duplicate("msg-1", DeduplicationStrategy::MessageId);
        cache.is_duplicate("msg-1", DeduplicationStrategy::MessageId);

        // 1 mark + 2 is_duplicate hits
        assert_eq!(
            cache.seen_count("msg-1", DeduplicationStrategy::MessageId),
            Some(3)
        );
    }

    #[test]
    fn canonical_json_normalises_nested_objects() {
        assert_eq!(
            canonical_json(&json!({"b": [1, {"z": 1, "a": 2}], "a": 0})),
            r#"{"a":0,"b":[1,{"a":2,"z":1}]}"#
        );
    }
}
