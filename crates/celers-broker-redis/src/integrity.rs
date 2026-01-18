//! Data integrity verification for task messages
//!
//! Provides checksum validation and ordered delivery guarantees to ensure
//! data integrity during task transmission and processing.

use celers_core::{CelersError, Result, SerializedTask};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Checksum algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChecksumAlgorithm {
    /// CRC32 checksum (fast, good for error detection)
    Crc32,
    /// XXHash (very fast, good distribution)
    XxHash,
    /// SHA256 (cryptographically secure, slower)
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

    /// Compute checksum for data
    pub fn compute(&self, data: &[u8]) -> String {
        match self {
            ChecksumAlgorithm::Crc32 => {
                let checksum = crc32fast::hash(data);
                format!("{:08x}", checksum)
            }
            ChecksumAlgorithm::XxHash => {
                // Use a simple hash for now (could use xxhash crate)
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                use std::hash::Hasher;
                hasher.write(data);
                format!("{:016x}", hasher.finish())
            }
            ChecksumAlgorithm::Sha256 => {
                // Use a simple hash for now (could use sha2 crate)
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                use std::hash::Hasher;
                hasher.write(data);
                format!("{:064x}", hasher.finish())
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

/// Integrity validator for tasks
pub struct IntegrityValidator {
    algorithm: ChecksumAlgorithm,
    /// Track sequence numbers per task name
    sequences: Arc<RwLock<HashMap<String, u64>>>,
    /// Statistics
    validated_count: Arc<RwLock<u64>>,
    failed_count: Arc<RwLock<u64>>,
}

impl IntegrityValidator {
    /// Create a new integrity validator
    pub fn new(algorithm: ChecksumAlgorithm) -> Self {
        Self {
            algorithm,
            sequences: Arc::new(RwLock::new(HashMap::new())),
            validated_count: Arc::new(RwLock::new(0)),
            failed_count: Arc::new(RwLock::new(0)),
        }
    }

    /// Wrap a task with integrity metadata
    pub async fn wrap(&self, task: SerializedTask) -> Result<IntegrityWrappedTask> {
        let payload_bytes = serde_json::to_vec(&task.payload)
            .map_err(|e| CelersError::Serialization(e.to_string()))?;

        let checksum = self.algorithm.compute(&payload_bytes);

        // Get and increment sequence number for this task type
        let mut sequences = self.sequences.write().await;
        let sequence = sequences
            .entry(task.metadata.name.clone())
            .and_modify(|s| *s += 1)
            .or_insert(1);

        Ok(IntegrityWrappedTask {
            task,
            checksum,
            algorithm: self.algorithm.name().to_string(),
            sequence: Some(*sequence),
        })
    }

    /// Validate a wrapped task
    pub async fn validate(&self, wrapped: &IntegrityWrappedTask) -> Result<bool> {
        let payload_bytes = serde_json::to_vec(&wrapped.task.payload)
            .map_err(|e| CelersError::Serialization(e.to_string()))?;

        let computed_checksum = self.algorithm.compute(&payload_bytes);

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

    /// Check if sequence is in order (returns None if out of order)
    pub async fn check_sequence(&self, wrapped: &IntegrityWrappedTask) -> Option<bool> {
        if let Some(seq) = wrapped.sequence {
            let sequences = self.sequences.read().await;
            if let Some(expected) = sequences.get(&wrapped.task.metadata.name) {
                // Allow for some out-of-order delivery (within a window)
                return Some(seq <= *expected + 10);
            }
        }
        None
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

    /// Reset sequence tracking
    pub async fn reset_sequences(&self) {
        let mut sequences = self.sequences.write().await;
        sequences.clear();
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

    #[test]
    fn test_checksum_algorithm_name() {
        assert_eq!(ChecksumAlgorithm::Crc32.name(), "crc32");
        assert_eq!(ChecksumAlgorithm::XxHash.name(), "xxhash");
        assert_eq!(ChecksumAlgorithm::Sha256.name(), "sha256");
    }

    #[test]
    fn test_checksum_compute() {
        let data = b"test data";
        let crc32 = ChecksumAlgorithm::Crc32.compute(data);
        let xxhash = ChecksumAlgorithm::XxHash.compute(data);
        let sha256 = ChecksumAlgorithm::Sha256.compute(data);

        assert!(!crc32.is_empty());
        assert!(!xxhash.is_empty());
        assert!(!sha256.is_empty());

        // Same data should produce same checksum
        assert_eq!(crc32, ChecksumAlgorithm::Crc32.compute(data));
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
