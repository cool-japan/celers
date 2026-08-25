//! Data integrity verification for task messages
//!
//! Provides checksum validation and ordered delivery guarantees to ensure
//! data integrity during task transmission and processing.
//!
//! Ordered-delivery checking (`IntegrityValidator::check_sequence`) works in
//! two modes:
//!
//! - **Process-local** (the default, via [`IntegrityValidator::new`]): the
//!   sequence watermark lives in an in-process map. This only detects
//!   reordering within the single process that both wrapped and checked the
//!   task — useful for tests and single-process pipelines, but it gives no
//!   guarantee across a real producer/consumer deployment.
//! - **Redis-backed** (via [`IntegrityValidator::with_redis`]): the sequence
//!   counter and the consumed watermark are stored in Redis, so ordering is
//!   verified across process boundaries — a separate producer process and
//!   consumer process share the same watermark.

use crate::connection::RedisClientExt;
use celers_core::{CelersError, Result, SerializedTask};
use redis::{AsyncCommands, Client, Script};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Checksum algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumAlgorithm {
    /// CRC32 checksum (fast, good for error detection)
    Crc32,
    /// `XXH3-64` (very fast, good distribution, spec-stable across
    /// toolchains — implemented via the `twox-hash` crate)
    XxHash,
    /// SHA-256 (cryptographically secure, slower)
    ///
    /// Computed with the `sha2` crate and rendered as 64 lowercase hex
    /// characters. Pick this when the checksum has to resist deliberate
    /// tampering rather than only accidental corruption.
    Sha256,
}

impl ChecksumAlgorithm {
    /// Get the algorithm name
    pub fn name(&self) -> &'static str {
        match self {
            ChecksumAlgorithm::Crc32 => "crc32",
            ChecksumAlgorithm::XxHash => "xxhash",
            ChecksumAlgorithm::Sha256 => "sha256",
        }
    }

    /// Compute checksum for data.
    ///
    /// Every variant is implemented, so this currently never returns `Err`;
    /// the `Result` is kept because the return type is part of the public API
    /// and a future algorithm may need to fail.
    ///
    /// The output is lowercase hex in all cases: 8 characters for
    /// [`Crc32`](ChecksumAlgorithm::Crc32), 16 for
    /// [`XxHash`](ChecksumAlgorithm::XxHash) and 64 for
    /// [`Sha256`](ChecksumAlgorithm::Sha256).
    pub fn compute(&self, data: &[u8]) -> Result<String> {
        match self {
            ChecksumAlgorithm::Crc32 => {
                let checksum = crc32fast::hash(data);
                Ok(format!("{:08x}", checksum))
            }
            ChecksumAlgorithm::XxHash => {
                // XXH3-64: a real, specified, non-cryptographic hash with a
                // fixed output for a given input across Rust toolchains
                // (unlike `std::collections::hash_map::DefaultHasher`, whose
                // output is explicitly *not* stable across releases).
                let hash = twox_hash::XxHash3_64::oneshot(data);
                Ok(format!("{:016x}", hash))
            }
            ChecksumAlgorithm::Sha256 => {
                // A real SHA-256 digest via the `sha2` crate. Rendering it as
                // lowercase hex keeps the checksum field a plain ASCII string,
                // matching the other two algorithms.
                use sha2::Digest;
                let mut hasher = sha2::Sha256::new();
                hasher.update(data);
                Ok(hex::encode(hasher.finalize()))
            }
        }
    }
}

/// Wrapped task with integrity metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IntegrityWrappedTask {
    /// The original serialized task
    pub task: SerializedTask,
    /// Checksum of the task payload
    pub checksum: String,
    /// Checksum algorithm used
    pub algorithm: String,
    /// Sequence number for ordered delivery (optional)
    pub sequence: Option<u64>,
}

/// Result of an ordered-delivery sequence check ([`IntegrityValidator::check_sequence`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SequenceCheck {
    /// No sequence number was present on the task, or this is the first
    /// sequence ever observed for this task name (process-local mode only —
    /// there is no watermark yet to compare against).
    Unknown,
    /// Arrived in order: at or ahead of the watermark, within the allowed
    /// out-of-order window.
    InOrder,
    /// Arrived ahead of the watermark by more than the allowed window,
    /// meaning one or more sequence numbers between `expected` and `got`
    /// were never observed.
    Gap {
        /// The next sequence number that was expected.
        expected: u64,
        /// The sequence number actually observed.
        got: u64,
    },
    /// Arrived at or behind the current watermark — a stale retransmit or a
    /// true duplicate delivery.
    Duplicate,
}

impl SequenceCheck {
    /// True for [`SequenceCheck::InOrder`] or [`SequenceCheck::Unknown`] —
    /// i.e. nothing anomalous was detected.
    pub fn is_ok(&self) -> bool {
        matches!(self, SequenceCheck::InOrder | SequenceCheck::Unknown)
    }
}

/// Redis-backed shared sequence state for [`IntegrityValidator`].
///
/// Kept separate from the process-local `HashMap`s so a single validator
/// instance can be used purely locally (no Redis connection attempted)
/// unless [`IntegrityValidator::with_redis`] was used to construct it.
#[derive(Clone)]
struct RedisSequenceBackend {
    client: Client,
    key_prefix: String,
}

/// Atomically compare-and-advance a per-task-name sequence watermark in Redis.
///
/// `KEYS[1]` = watermark key, `ARGV[1]` = observed sequence number, `ARGV[2]`
/// = out-of-order window. Returns `{classification, previous_watermark}`
/// where `classification` is `0` = Duplicate, `1` = InOrder, `2` = Gap. A
/// missing key is treated as watermark `0`.
const SEQUENCE_WATERMARK_SCRIPT: &str = r#"
local highest = tonumber(redis.call('GET', KEYS[1]) or '0')
local got = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
if got <= highest then
    return {0, highest}
end
redis.call('SET', KEYS[1], got)
if got > highest + window + 1 then
    return {2, highest}
else
    return {1, highest}
end
"#;

/// Integrity validator for tasks
pub struct IntegrityValidator {
    algorithm: ChecksumAlgorithm,
    /// Producer-side: next sequence number to allocate per task name.
    /// Written only by `wrap()`.
    sequences: Arc<RwLock<HashMap<String, u64>>>,
    /// Consumer-side: highest sequence number observed per task name.
    /// Written only by the process-local fallback path of `check_sequence()`.
    /// Kept separate from `sequences` so a validator used for both wrapping
    /// and checking in the same process (e.g. in tests) doesn't have its
    /// producer counter corrupted by consumer-side watermark advances.
    consumed_watermarks: Arc<RwLock<HashMap<String, u64>>>,
    /// Statistics
    validated_count: Arc<RwLock<u64>>,
    failed_count: Arc<RwLock<u64>>,
    /// When set, sequence allocation and checking are backed by Redis
    /// instead of the process-local maps above, giving real cross-process
    /// ordering guarantees.
    redis: Option<RedisSequenceBackend>,
    /// Out-of-order window: a sequence number up to this many slots ahead of
    /// the watermark is still considered in-order. Applies to both the
    /// process-local and Redis-backed paths.
    out_of_order_window: u64,
}

impl IntegrityValidator {
    /// Create a new integrity validator with process-local sequence tracking.
    pub fn new(algorithm: ChecksumAlgorithm) -> Self {
        Self {
            algorithm,
            sequences: Arc::new(RwLock::new(HashMap::new())),
            consumed_watermarks: Arc::new(RwLock::new(HashMap::new())),
            validated_count: Arc::new(RwLock::new(0)),
            failed_count: Arc::new(RwLock::new(0)),
            redis: None,
            out_of_order_window: 10,
        }
    }

    /// Create a validator whose sequence allocation and ordering watermark
    /// are shared via Redis under `key_prefix`, so [`Self::check_sequence`]
    /// gives real guarantees across process boundaries instead of only
    /// working within the process that called [`Self::wrap`].
    pub fn with_redis(
        algorithm: ChecksumAlgorithm,
        redis_url: &str,
        key_prefix: &str,
    ) -> Result<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to Redis: {}", e)))?;
        let mut validator = Self::new(algorithm);
        validator.redis = Some(RedisSequenceBackend {
            client,
            key_prefix: key_prefix.to_string(),
        });
        Ok(validator)
    }

    /// Set the out-of-order window used by [`Self::check_sequence`] (default: 10).
    pub fn with_out_of_order_window(mut self, window: u64) -> Self {
        self.out_of_order_window = window;
        self
    }

    /// Wrap a task with integrity metadata
    pub async fn wrap(&self, task: SerializedTask) -> Result<IntegrityWrappedTask> {
        let payload_bytes = serde_json::to_vec(&task.payload)
            .map_err(|e| CelersError::Serialization(e.to_string()))?;

        let checksum = self.algorithm.compute(&payload_bytes)?;

        let sequence = if let Some(backend) = &self.redis {
            self.next_sequence_redis(backend, &task.metadata.name)
                .await?
        } else {
            let mut sequences = self.sequences.write().await;
            let next = sequences
                .entry(task.metadata.name.clone())
                .and_modify(|s| *s += 1)
                .or_insert(1);
            *next
        };

        Ok(IntegrityWrappedTask {
            task,
            checksum,
            algorithm: self.algorithm.name().to_string(),
            sequence: Some(sequence),
        })
    }

    /// Allocate the next sequence number for `task_name` via Redis `INCR`,
    /// shared across every producer process using the same `key_prefix`.
    async fn next_sequence_redis(
        &self,
        backend: &RedisSequenceBackend,
        task_name: &str,
    ) -> Result<u64> {
        let mut conn = backend
            .client
            .celers_multiplexed_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Connection error: {}", e)))?;

        let key = format!("{}:seq:{}", backend.key_prefix, task_name);
        let seq: u64 = conn
            .incr(&key, 1u64)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to allocate sequence: {}", e)))?;
        Ok(seq)
    }

    /// Validate a wrapped task
    pub async fn validate(&self, wrapped: &IntegrityWrappedTask) -> Result<bool> {
        let payload_bytes = serde_json::to_vec(&wrapped.task.payload)
            .map_err(|e| CelersError::Serialization(e.to_string()))?;

        let computed_checksum = self.algorithm.compute(&payload_bytes)?;

        let is_valid = computed_checksum == wrapped.checksum;

        if is_valid {
            let mut count = self.validated_count.write().await;
            *count += 1;
        } else {
            let mut count = self.failed_count.write().await;
            *count += 1;
        }

        Ok(is_valid)
    }

    /// Check whether a wrapped task's sequence number arrived in order.
    ///
    /// See the module documentation for the difference between process-local
    /// (default) and Redis-backed ([`Self::with_redis`]) checking.
    pub async fn check_sequence(&self, wrapped: &IntegrityWrappedTask) -> Result<SequenceCheck> {
        let Some(seq) = wrapped.sequence else {
            return Ok(SequenceCheck::Unknown);
        };
        let task_name = &wrapped.task.metadata.name;

        if let Some(backend) = &self.redis {
            return self.check_sequence_redis(backend, task_name, seq).await;
        }

        let mut watermarks = self.consumed_watermarks.write().await;
        match watermarks.get_mut(task_name) {
            Some(highest) => {
                if seq <= *highest {
                    Ok(SequenceCheck::Duplicate)
                } else if seq > *highest + self.out_of_order_window + 1 {
                    let expected = *highest + 1;
                    *highest = seq;
                    Ok(SequenceCheck::Gap { expected, got: seq })
                } else {
                    *highest = seq;
                    Ok(SequenceCheck::InOrder)
                }
            }
            None => {
                watermarks.insert(task_name.clone(), seq);
                Ok(SequenceCheck::Unknown)
            }
        }
    }

    /// Redis-backed sequence check: atomically compares and advances the
    /// shared watermark via [`SEQUENCE_WATERMARK_SCRIPT`].
    async fn check_sequence_redis(
        &self,
        backend: &RedisSequenceBackend,
        task_name: &str,
        seq: u64,
    ) -> Result<SequenceCheck> {
        let mut conn = backend
            .client
            .celers_multiplexed_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Connection error: {}", e)))?;

        let key = format!("{}:watermark:{}", backend.key_prefix, task_name);
        let script = Script::new(SEQUENCE_WATERMARK_SCRIPT);
        let (classification, previous): (i64, u64) = script
            .key(&key)
            .arg(seq)
            .arg(self.out_of_order_window)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| {
                CelersError::Broker(format!("Failed to check sequence watermark: {}", e))
            })?;

        Ok(match classification {
            0 => SequenceCheck::Duplicate,
            2 => SequenceCheck::Gap {
                expected: previous + 1,
                got: seq,
            },
            _ => SequenceCheck::InOrder,
        })
    }

    /// Get validation statistics
    pub async fn stats(&self) -> IntegrityStats {
        IntegrityStats {
            validated_count: *self.validated_count.read().await,
            failed_count: *self.failed_count.read().await,
            algorithm: self.algorithm.name().to_string(),
        }
    }

    /// Reset statistics
    pub async fn reset_stats(&self) {
        let mut validated = self.validated_count.write().await;
        let mut failed = self.failed_count.write().await;
        *validated = 0;
        *failed = 0;
    }

    /// Reset producer-side sequence allocation (process-local mode only).
    pub async fn reset_sequences(&self) {
        let mut sequences = self.sequences.write().await;
        sequences.clear();
    }

    /// Reset consumer-side sequence watermarks (process-local mode only).
    pub async fn reset_consumed_watermarks(&self) {
        let mut watermarks = self.consumed_watermarks.write().await;
        watermarks.clear();
    }
}

/// Integrity validation statistics
#[derive(Debug, Clone)]
pub struct IntegrityStats {
    /// Number of successfully validated tasks
    pub validated_count: u64,
    /// Number of failed validations
    pub failed_count: u64,
    /// Checksum algorithm used
    pub algorithm: String,
}

impl IntegrityStats {
    /// Get the validation success rate (0.0 to 1.0)
    pub fn success_rate(&self) -> f64 {
        let total = self.validated_count + self.failed_count;
        if total == 0 {
            1.0
        } else {
            self.validated_count as f64 / total as f64
        }
    }

    /// Check if validation is healthy (success rate > threshold)
    pub fn is_healthy(&self, threshold: f64) -> bool {
        self.success_rate() >= threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_test_task() -> SerializedTask {
        let payload_json = json!({"key": "value"});
        let payload_bytes = serde_json::to_vec(&payload_json).unwrap();

        let task_json = json!({
            "metadata": {
                "id": "550e8400-e29b-41d4-a716-446655440000",
                "name": "test.task",
                "priority": 5,
                "state": "Pending",
                "retries": 0,
                "max_retries": 3,
                "eta": null,
                "expires": null,
                "created_at": "2024-01-01T00:00:00Z",
                "updated_at": "2024-01-01T00:00:00Z"
            },
            "payload": payload_bytes
        });

        serde_json::from_value(task_json).unwrap()
    }

    /// Redis connection URL used by the integration-style tests below. A
    /// local Redis is expected to be reachable in this crate's test
    /// environment (see the crate's other Redis-backed modules).
    const TEST_REDIS_URL: &str = "redis://127.0.0.1:6379";

    #[test]
    fn test_checksum_algorithm_name() {
        assert_eq!(ChecksumAlgorithm::Crc32.name(), "crc32");
        assert_eq!(ChecksumAlgorithm::XxHash.name(), "xxhash");
        assert_eq!(ChecksumAlgorithm::Sha256.name(), "sha256");
    }

    #[test]
    fn test_checksum_compute() {
        let data = b"test data";
        let crc32 = ChecksumAlgorithm::Crc32.compute(data).unwrap();
        let xxhash = ChecksumAlgorithm::XxHash.compute(data).unwrap();
        let sha256 = ChecksumAlgorithm::Sha256.compute(data).unwrap();

        assert!(!crc32.is_empty());
        assert!(!xxhash.is_empty());
        assert!(!sha256.is_empty());

        // Same data should produce same checksum
        assert_eq!(crc32, ChecksumAlgorithm::Crc32.compute(data).unwrap());
        assert_eq!(xxhash, ChecksumAlgorithm::XxHash.compute(data).unwrap());
        assert_eq!(sha256, ChecksumAlgorithm::Sha256.compute(data).unwrap());
    }

    #[test]
    fn test_sha256_matches_known_nist_vectors() {
        // FIPS 180-4 reference digests. These pin that the "sha256" label
        // really denotes SHA-256 and not some other hash rendered under that
        // name — the whole point of offering a cryptographic option.
        assert_eq!(
            ChecksumAlgorithm::Sha256.compute(b"").unwrap(),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
        assert_eq!(
            ChecksumAlgorithm::Sha256.compute(b"abc").unwrap(),
            "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
        );
        assert_eq!(
            ChecksumAlgorithm::Sha256
                .compute(b"abcdbcdecdefdefgefghfghighijhijkijkljklmklmnlmnomnopnopq")
                .unwrap(),
            "248d6a61d20638b8e5c026930c3e6039a33ce45964ff2167f6ecedd419db06c1"
        );
    }

    #[test]
    fn test_sha256_output_shape_and_determinism() {
        let a = ChecksumAlgorithm::Sha256.compute(b"test data").unwrap();
        let b = ChecksumAlgorithm::Sha256.compute(b"test data").unwrap();
        assert_eq!(a, b, "the same input must produce the same digest");
        assert_eq!(a.len(), 64, "SHA-256 renders as 64 lowercase hex chars");
        assert!(a
            .chars()
            .all(|c| c.is_ascii_hexdigit() && !c.is_uppercase()));
        assert_ne!(a, ChecksumAlgorithm::Sha256.compute(b"test datb").unwrap());
        // And it must not collide with the other algorithms' labels/outputs.
        assert_ne!(a, ChecksumAlgorithm::Crc32.compute(b"test data").unwrap());
        assert_ne!(a, ChecksumAlgorithm::XxHash.compute(b"test data").unwrap());
    }

    #[test]
    fn test_xxhash_matches_known_xxh3_64_vectors() {
        // XXH3-64 (seed 0) reference values, independently computed against
        // the `twox-hash` crate — pins both correctness and cross-toolchain
        // stability (unlike `DefaultHasher`, XXH3 has a fixed, specified
        // output for a given input).
        assert_eq!(
            ChecksumAlgorithm::XxHash.compute(b"").unwrap(),
            "2d06800538d394c2"
        );
        assert_eq!(
            ChecksumAlgorithm::XxHash.compute(b"abc").unwrap(),
            "78af5f94892f3950"
        );
        assert_eq!(
            ChecksumAlgorithm::XxHash.compute(b"test data").unwrap(),
            "8f0fa94a1fe96cc4"
        );
    }

    #[tokio::test]
    async fn test_wrap_task() {
        let validator = IntegrityValidator::new(ChecksumAlgorithm::Crc32);
        let task = create_test_task();

        let wrapped = validator.wrap(task.clone()).await.unwrap();
        assert_eq!(wrapped.task.metadata.id, task.metadata.id);
        assert_eq!(wrapped.algorithm, "crc32");
        assert_eq!(wrapped.sequence, Some(1));
        assert!(!wrapped.checksum.is_empty());
    }

    #[tokio::test]
    async fn test_wrap_and_validate_round_trip_with_sha256() {
        let validator = IntegrityValidator::new(ChecksumAlgorithm::Sha256);
        let task = create_test_task();

        let wrapped = validator.wrap(task.clone()).await.unwrap();
        assert_eq!(wrapped.task.metadata.id, task.metadata.id);
        assert_eq!(wrapped.algorithm, "sha256");
        assert_eq!(wrapped.checksum.len(), 64);
        assert!(validator.validate(&wrapped).await.unwrap());

        // Tampering with the payload must be detected by the digest.
        let mut tampered = wrapped;
        tampered.task.payload.push(0xff);
        assert!(!validator.validate(&tampered).await.unwrap());
    }

    #[tokio::test]
    async fn test_validate_task() {
        let validator = IntegrityValidator::new(ChecksumAlgorithm::Crc32);
        let task = create_test_task();

        let wrapped = validator.wrap(task).await.unwrap();
        let is_valid = validator.validate(&wrapped).await.unwrap();
        assert!(is_valid);

        let stats = validator.stats().await;
        assert_eq!(stats.validated_count, 1);
        assert_eq!(stats.failed_count, 0);
    }

    #[tokio::test]
    async fn test_validate_corrupted_task() {
        let validator = IntegrityValidator::new(ChecksumAlgorithm::Crc32);
        let task = create_test_task();

        let mut wrapped = validator.wrap(task).await.unwrap();
        // Corrupt the checksum
        wrapped.checksum = "00000000".to_string();

        let is_valid = validator.validate(&wrapped).await.unwrap();
        assert!(!is_valid);

        let stats = validator.stats().await;
        assert_eq!(stats.validated_count, 0);
        assert_eq!(stats.failed_count, 1);
    }

    #[tokio::test]
    async fn test_sequence_tracking() {
        let validator = IntegrityValidator::new(ChecksumAlgorithm::Crc32);
        let task = create_test_task();

        let wrapped1 = validator.wrap(task.clone()).await.unwrap();
        assert_eq!(wrapped1.sequence, Some(1));

        let wrapped2 = validator.wrap(task.clone()).await.unwrap();
        assert_eq!(wrapped2.sequence, Some(2));

        let wrapped3 = validator.wrap(task).await.unwrap();
        assert_eq!(wrapped3.sequence, Some(3));
    }

    #[tokio::test]
    async fn test_check_sequence_local_first_is_unknown_then_in_order() {
        let validator = IntegrityValidator::new(ChecksumAlgorithm::Crc32);
        let task = create_test_task();

        let w1 = validator.wrap(task.clone()).await.unwrap();
        // First observation for this task name: no watermark yet.
        assert_eq!(
            validator.check_sequence(&w1).await.unwrap(),
            SequenceCheck::Unknown
        );

        let w2 = validator.wrap(task.clone()).await.unwrap();
        assert_eq!(
            validator.check_sequence(&w2).await.unwrap(),
            SequenceCheck::InOrder
        );
    }

    #[tokio::test]
    async fn test_check_sequence_local_detects_duplicate_and_gap() {
        let validator =
            IntegrityValidator::new(ChecksumAlgorithm::Crc32).with_out_of_order_window(2);
        let task = create_test_task();

        // Establish a watermark at sequence 1.
        let w1 = validator.wrap(task.clone()).await.unwrap();
        validator.check_sequence(&w1).await.unwrap();

        // Re-delivering the same (or an older) sequence is a duplicate.
        assert_eq!(
            validator.check_sequence(&w1).await.unwrap(),
            SequenceCheck::Duplicate
        );

        // Jumping far ahead (beyond the window) is reported as a gap, not
        // silently accepted as in-order.
        let mut w_far = w1.clone();
        w_far.sequence = Some(100);
        match validator.check_sequence(&w_far).await.unwrap() {
            SequenceCheck::Gap { expected, got } => {
                assert_eq!(expected, 2);
                assert_eq!(got, 100);
            }
            other => panic!("expected Gap, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_check_sequence_none_is_unknown() {
        let validator = IntegrityValidator::new(ChecksumAlgorithm::Crc32);
        let task = create_test_task();
        let mut wrapped = validator.wrap(task).await.unwrap();
        wrapped.sequence = None;

        assert_eq!(
            validator.check_sequence(&wrapped).await.unwrap(),
            SequenceCheck::Unknown
        );
    }

    /// Cross-process ordering: two independent `IntegrityValidator`
    /// instances (simulating a producer process and a consumer process)
    /// sharing the same Redis-backed watermark must agree on ordering,
    /// which is exactly what the process-local map cannot provide.
    #[tokio::test]
    async fn test_check_sequence_redis_backed_cross_process() {
        let key_prefix = format!("test:integrity:{}", uuid::Uuid::new_v4());

        let producer =
            IntegrityValidator::with_redis(ChecksumAlgorithm::Crc32, TEST_REDIS_URL, &key_prefix)
                .unwrap();
        let consumer =
            IntegrityValidator::with_redis(ChecksumAlgorithm::Crc32, TEST_REDIS_URL, &key_prefix)
                .unwrap();

        let task = create_test_task();

        // Producer allocates sequence numbers via Redis INCR.
        let w1 = producer.wrap(task.clone()).await.unwrap();
        let w2 = producer.wrap(task.clone()).await.unwrap();
        assert_eq!(w1.sequence, Some(1));
        assert_eq!(w2.sequence, Some(2));

        // A *different* validator instance (the "consumer") sees the same
        // watermark through Redis, not through any in-process state.
        assert_eq!(
            consumer.check_sequence(&w1).await.unwrap(),
            SequenceCheck::InOrder
        );
        assert_eq!(
            consumer.check_sequence(&w2).await.unwrap(),
            SequenceCheck::InOrder
        );
        // Re-consuming w1 now reads as a duplicate/stale delivery.
        assert_eq!(
            consumer.check_sequence(&w1).await.unwrap(),
            SequenceCheck::Duplicate
        );
    }

    #[tokio::test]
    async fn test_stats() {
        let validator = IntegrityValidator::new(ChecksumAlgorithm::Crc32);
        let task = create_test_task();

        let wrapped = validator.wrap(task).await.unwrap();
        validator.validate(&wrapped).await.unwrap();

        let stats = validator.stats().await;
        assert_eq!(stats.validated_count, 1);
        assert_eq!(stats.algorithm, "crc32");
        assert_eq!(stats.success_rate(), 1.0);
        assert!(stats.is_healthy(0.95));
    }

    #[tokio::test]
    async fn test_reset_stats() {
        let validator = IntegrityValidator::new(ChecksumAlgorithm::Crc32);
        let task = create_test_task();

        let wrapped = validator.wrap(task).await.unwrap();
        validator.validate(&wrapped).await.unwrap();

        validator.reset_stats().await;

        let stats = validator.stats().await;
        assert_eq!(stats.validated_count, 0);
        assert_eq!(stats.failed_count, 0);
    }

    #[tokio::test]
    async fn test_reset_sequences() {
        let validator = IntegrityValidator::new(ChecksumAlgorithm::Crc32);
        let task = create_test_task();

        let wrapped1 = validator.wrap(task.clone()).await.unwrap();
        assert_eq!(wrapped1.sequence, Some(1));

        validator.reset_sequences().await;

        let wrapped2 = validator.wrap(task).await.unwrap();
        assert_eq!(wrapped2.sequence, Some(1));
    }

    #[tokio::test]
    async fn test_reset_consumed_watermarks() {
        let validator = IntegrityValidator::new(ChecksumAlgorithm::Crc32);
        let task = create_test_task();

        let w1 = validator.wrap(task.clone()).await.unwrap();
        validator.check_sequence(&w1).await.unwrap();
        // Now a duplicate.
        assert_eq!(
            validator.check_sequence(&w1).await.unwrap(),
            SequenceCheck::Duplicate
        );

        validator.reset_consumed_watermarks().await;

        // Watermark cleared: this "duplicate" now reads as first-ever again.
        assert_eq!(
            validator.check_sequence(&w1).await.unwrap(),
            SequenceCheck::Unknown
        );
    }

    #[test]
    fn test_integrity_stats_success_rate() {
        let stats = IntegrityStats {
            validated_count: 90,
            failed_count: 10,
            algorithm: "crc32".to_string(),
        };
        assert_eq!(stats.success_rate(), 0.9);
        assert!(stats.is_healthy(0.85));
        assert!(!stats.is_healthy(0.95));

        let empty_stats = IntegrityStats {
            validated_count: 0,
            failed_count: 0,
            algorithm: "crc32".to_string(),
        };
        assert_eq!(empty_stats.success_rate(), 1.0);
    }
}
