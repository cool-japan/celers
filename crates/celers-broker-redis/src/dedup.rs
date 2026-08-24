//! Task deduplication implementation
//!
//! Provides mechanisms to prevent duplicate task processing:
//! - Content-based deduplication (hash of task payload)
//! - Time-window deduplication (prevent re-enqueue within window)
//! - Custom deduplication keys

use celers_core::{CelersError, Result, SerializedTask, TaskId};
use redis::{aio::ConnectionManager, AsyncCommands};
use std::time::Duration;
use tokio::sync::OnceCell;

/// Deduplication key suffix
const DEDUP_KEY_PREFIX: &str = "dedup:";

/// How many keys one `SCAN` iteration asks Redis to examine.
const SCAN_BATCH: usize = 500;

/// Frame a task's identity as bytes for hashing.
///
/// The name is length-prefixed so that `("ab", "c")` and `("a", "bc")` cannot
/// hash to the same digest — with plain concatenation they would, and two
/// unrelated tasks would then deduplicate each other.
fn identity_bytes(task: &SerializedTask) -> Vec<u8> {
    let name = task.metadata.name.as_bytes();
    let mut framed = Vec::with_capacity(8 + name.len() + task.payload.len());
    framed.extend_from_slice(&(name.len() as u64).to_le_bytes());
    framed.extend_from_slice(name);
    framed.extend_from_slice(&task.payload);
    framed
}

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
    ///
    /// Content keys use XXH3-128, a specified algorithm with a reference
    /// implementation, so the same payload maps to the same key on every
    /// rustc version, every host and every language binding. (The standard
    /// library's `DefaultHasher` explicitly does not promise that, so a fleet
    /// running mixed toolchains would compute different keys for identical
    /// payloads and deduplicate nothing.) 128 bits keeps the odds of two
    /// distinct payloads colliding — which would silently *drop* a task —
    /// negligible for any realistic keyspace.
    pub fn generate_key(&self, task: &SerializedTask, queue_name: &str) -> String {
        let base = match self {
            DedupStrategy::ById => task.metadata.id.to_string(),
            DedupStrategy::ByContent => {
                format!(
                    "{:032x}",
                    twox_hash::XxHash3_128::oneshot(&identity_bytes(task))
                )
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
    /// Long-lived multiplexed connection, shared by every call.
    ///
    /// Opening a fresh connection per operation costs a TCP handshake, an
    /// AUTH/SELECT round trip and a socket left in `TIME_WAIT` — per task.
    conn: OnceCell<ConnectionManager>,
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
            conn: OnceCell::new(),
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

    /// Reuse an existing connection manager instead of opening one lazily.
    ///
    /// Lets a caller that already holds a multiplexed connection (such as
    /// [`crate::RedisBroker`]) share it rather than paying for a second one.
    pub fn with_connection_manager(self, manager: ConnectionManager) -> Self {
        let conn = OnceCell::new();
        // Only fails if the cell is already initialised, which it is not.
        let _ = conn.set(manager);
        Self { conn, ..self }
    }

    /// Get the shared connection, establishing it on first use.
    async fn connection(&self) -> Result<ConnectionManager> {
        self.conn
            .get_or_try_init(|| async {
                self.client
                    .get_connection_manager()
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to connect: {}", e)))
            })
            .await
            .cloned()
    }

    /// Check if a task is a duplicate
    pub async fn check(&self, task: &SerializedTask) -> Result<DedupResult> {
        let key = self.strategy.generate_key(task, &self.queue_name);

        let mut conn = self.connection().await?;

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

        let mut conn = self.connection().await?;

        conn.set_ex::<_, _, ()>(&key, &value, self.ttl.as_secs())
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to mark task: {}", e)))?;

        Ok(())
    }

    /// Check and mark atomically (returns true if new, false if duplicate)
    ///
    /// Uses a single `SET key value NX EX <ttl>`. Splitting this into `SETNX`
    /// followed by `EXPIRE` leaves a TTL-less key behind whenever the process
    /// dies, the connection drops or the `EXPIRE` errors in between — that
    /// task's content hash is then blacklisted forever, with no operator-
    /// visible symptom and no way for the key to be reclaimed.
    pub async fn check_and_mark(&self, task: &SerializedTask) -> Result<bool> {
        let key = self.strategy.generate_key(task, &self.queue_name);
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|e| CelersError::Other(format!("Time error: {}", e)))?
            .as_secs();

        let value = format!("{}:{}", task.metadata.id, now);

        let mut conn = self.connection().await?;

        let stored: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg(&value)
            .arg("NX")
            .arg("EX")
            .arg(self.ttl.as_secs().max(1))
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to check and mark: {}", e)))?;

        // `SET ... NX` replies with OK when it stored the value and with a
        // nil bulk string when the key already existed.
        Ok(stored.is_some())
    }

    /// Remove deduplication entry for a task
    pub async fn unmark(&self, task: &SerializedTask) -> Result<bool> {
        let key = self.strategy.generate_key(task, &self.queue_name);

        let mut conn = self.connection().await?;

        let deleted: i64 = conn
            .del(&key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to unmark task: {}", e)))?;

        Ok(deleted > 0)
    }

    /// The key pattern covering every dedup entry of this queue.
    fn key_pattern(&self) -> String {
        format!("{}{}:*", DEDUP_KEY_PREFIX, self.queue_name)
    }

    /// Clear all deduplication entries for this queue
    ///
    /// Iterates with `SCAN` and reclaims in `UNLINK` batches. `KEYS <pattern>`
    /// would be O(total keyspace) and blocks the single-threaded server for
    /// its whole duration, stalling every other client on the instance.
    pub async fn clear(&self) -> Result<usize> {
        let pattern = self.key_pattern();
        let mut conn = self.connection().await?;

        let mut cursor: u64 = 0;
        let mut removed = 0usize;

        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SCAN_BATCH)
                .query_async(&mut conn)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to scan keys: {}", e)))?;

            if !keys.is_empty() {
                // UNLINK reclaims memory on a background thread and returns
                // how many keys it actually removed, so a key SCAN reports
                // twice (which it may) is not double-counted.
                let unlinked: usize = redis::cmd("UNLINK")
                    .arg(&keys)
                    .query_async(&mut conn)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Failed to delete keys: {}", e)))?;
                removed += unlinked;
            }

            cursor = next;
            if cursor == 0 {
                break;
            }
        }

        Ok(removed)
    }

    /// Get the number of tracked dedup entries
    ///
    /// Uses `SCAN` rather than `KEYS`; see [`Self::clear`].
    pub async fn count(&self) -> Result<usize> {
        let pattern = self.key_pattern();
        let mut conn = self.connection().await?;

        let mut cursor: u64 = 0;
        // SCAN guarantees keys are returned at least once, not exactly once,
        // so deduplicate before counting.
        let mut seen = std::collections::HashSet::new();

        loop {
            let (next, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(SCAN_BATCH)
                .query_async(&mut conn)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to count keys: {}", e)))?;

            seen.extend(keys);

            cursor = next;
            if cursor == 0 {
                break;
            }
        }

        Ok(seen.len())
    }
}

/// Generate content hash for a task
///
/// XXH3-64 over the framed `(name, payload)` identity: a specified algorithm
/// with a reference implementation, so the digest is reproducible across
/// rustc versions and hosts.
pub fn content_hash(task: &SerializedTask) -> u64 {
    twox_hash::XxHash3_64::oneshot(&identity_bytes(task))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::connection::RedisClientExt;

    const TEST_REDIS_URL: &str = "redis://127.0.0.1:6379";

    fn create_test_task(name: &str, payload: &str) -> SerializedTask {
        SerializedTask::new(name.to_string(), payload.as_bytes().to_vec())
    }

    /// `check_and_mark` must leave a key that *always* expires. With the old
    /// `SETNX` + separate `EXPIRE` a crash in between left a TTL-less key,
    /// permanently blacklisting that content hash.
    #[tokio::test]
    async fn test_check_and_mark_is_atomic_and_always_expires() {
        let client = redis::Client::open(TEST_REDIS_URL).expect("client");
        let queue = format!("test-dedup-{}", uuid::Uuid::new_v4());
        let dedup = Deduplicator::new(client.clone(), &queue).with_ttl(Duration::from_secs(60));
        let task = create_test_task("ttl_probe", r#"{"arg":1}"#);

        assert!(dedup.check_and_mark(&task).await.expect("first mark"));
        assert!(
            !dedup.check_and_mark(&task).await.expect("second mark"),
            "the same content must be reported as a duplicate"
        );

        let key = DedupStrategy::ByContent.generate_key(&task, &queue);
        let mut conn = client
            .celers_multiplexed_connection()
            .await
            .expect("connection");
        let ttl: i64 = redis::cmd("TTL")
            .arg(&key)
            .query_async(&mut conn)
            .await
            .expect("ttl");
        assert!(
            ttl > 0 && ttl <= 60,
            "dedup key must carry a TTL, got {ttl} (-1 means it never expires)"
        );

        // SCAN-based bookkeeping must see and reclaim the key.
        assert_eq!(dedup.count().await.expect("count"), 1);
        assert_eq!(dedup.clear().await.expect("clear"), 1);
        assert_eq!(dedup.count().await.expect("count after clear"), 0);
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

    /// Concatenating `name` and `payload` without a length prefix makes
    /// `("ab", "c")` and `("a", "bc")` hash identically, so two unrelated
    /// tasks would deduplicate each other and one would silently vanish.
    #[test]
    fn test_identity_framing_prevents_boundary_collisions() {
        let ab_c = create_test_task("ab", "c");
        let a_bc = create_test_task("a", "bc");

        assert_ne!(content_hash(&ab_c), content_hash(&a_bc));
        assert_ne!(
            DedupStrategy::ByContent.generate_key(&ab_c, "q"),
            DedupStrategy::ByContent.generate_key(&a_bc, "q")
        );
    }

    /// Dedup keys are shared across a fleet, so the digest must be a
    /// specified algorithm rather than whatever hash the current rustc ships.
    /// These vectors pin XXH3 for the exact bytes the framing produces.
    #[test]
    fn test_content_hash_is_version_stable() {
        let task = create_test_task("test_task", r#"{"arg": 1}"#);
        let framed = identity_bytes(&task);

        assert_eq!(content_hash(&task), twox_hash::XxHash3_64::oneshot(&framed));
        assert_eq!(
            DedupStrategy::ByContent.generate_key(&task, "q"),
            format!("dedup:q:{:032x}", twox_hash::XxHash3_128::oneshot(&framed))
        );

        // Framing layout: 8-byte little-endian name length, name, payload.
        let mut expected = Vec::new();
        expected.extend_from_slice(&9u64.to_le_bytes());
        expected.extend_from_slice(b"test_task");
        expected.extend_from_slice(br#"{"arg": 1}"#);
        assert_eq!(framed, expected);
    }

    /// The generated key must be a fixed-width hex digest -- a `{:x}` format
    /// of a leading-zero digest would otherwise shorten and could alias.
    #[test]
    fn test_content_key_is_fixed_width() {
        for i in 0..32u8 {
            let task = create_test_task("t", &format!("payload-{i}"));
            let key = DedupStrategy::ByContent.generate_key(&task, "q");
            let digest = key.rsplit(':').next().expect("digest segment");
            assert_eq!(digest.len(), 32, "digest must be 128 bits of hex: {key}");
            assert!(digest.chars().all(|c| c.is_ascii_hexdigit()));
        }
    }
}
