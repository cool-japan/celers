//! Result chunking for large payloads
//!
//! Automatically splits large task results across multiple Redis keys
//! and reassembles them transparently on read. Uses CRC32 checksums
//! for data integrity verification.

use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicU64, Ordering};

/// Default threshold: 512KB
const DEFAULT_THRESHOLD_BYTES: usize = 524_288;

/// Default chunk size: 256KB
const DEFAULT_CHUNK_SIZE_BYTES: usize = 262_144;

/// Sentinel prefix used to identify chunked values stored in Redis
const CHUNKED_SENTINEL: &[u8] = b"CELERS_CHUNKED:";

/// Configuration for result chunking behavior
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkingConfig {
    /// Whether chunking is enabled
    pub enabled: bool,
    /// Minimum payload size (bytes) before chunking kicks in
    pub threshold_bytes: usize,
    /// Maximum size of each individual chunk (bytes)
    pub chunk_size_bytes: usize,
    /// Whether CRC32 checksums are computed and verified
    pub checksum_enabled: bool,
}

impl Default for ChunkingConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold_bytes: DEFAULT_THRESHOLD_BYTES,
            chunk_size_bytes: DEFAULT_CHUNK_SIZE_BYTES,
            checksum_enabled: true,
        }
    }
}

impl ChunkingConfig {
    /// Create a new chunking config with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a config with chunking disabled
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            ..Self::default()
        }
    }

    /// Set the threshold in bytes above which chunking is applied
    pub fn with_threshold(mut self, bytes: usize) -> Self {
        self.threshold_bytes = bytes;
        self
    }

    /// Set the maximum chunk size in bytes
    pub fn with_chunk_size(mut self, bytes: usize) -> Self {
        self.chunk_size_bytes = bytes;
        self
    }

    /// Enable or disable CRC32 checksum verification
    pub fn with_checksum(mut self, enabled: bool) -> Self {
        self.checksum_enabled = enabled;
        self
    }
}

/// Metadata describing a chunked result stored across multiple Redis keys
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMetadata {
    /// Number of chunks the data was split into
    pub total_chunks: usize,
    /// Size of each chunk (last chunk may be smaller)
    pub chunk_size: usize,
    /// Total size of the original data in bytes
    pub total_size: usize,
    /// CRC32 checksum of the original data (if checksum was enabled)
    pub checksum: Option<u32>,
    /// Unix timestamp when the chunked result was created
    pub created_at: u64,
}

/// Thread-safe counters for tracking chunking operations
#[derive(Debug)]
pub struct ChunkingStats {
    /// Total number of chunk store operations
    chunks_stored: AtomicU64,
    /// Total number of chunk load operations
    chunks_loaded: AtomicU64,
    /// Total bytes that were chunked (original size)
    bytes_chunked: AtomicU64,
    /// Number of checksum verification failures
    checksum_failures: AtomicU64,
}

impl Default for ChunkingStats {
    fn default() -> Self {
        Self::new()
    }
}

impl ChunkingStats {
    /// Create new zeroed stats
    pub fn new() -> Self {
        Self {
            chunks_stored: AtomicU64::new(0),
            chunks_loaded: AtomicU64::new(0),
            bytes_chunked: AtomicU64::new(0),
            checksum_failures: AtomicU64::new(0),
        }
    }

    /// Record a store operation
    pub fn record_store(&self, num_chunks: u64, total_bytes: u64) {
        self.chunks_stored.fetch_add(num_chunks, Ordering::Relaxed);
        self.bytes_chunked.fetch_add(total_bytes, Ordering::Relaxed);
    }

    /// Record a load operation
    pub fn record_load(&self, num_chunks: u64) {
        self.chunks_loaded.fetch_add(num_chunks, Ordering::Relaxed);
    }

    /// Record a checksum failure
    pub fn record_checksum_failure(&self) {
        self.checksum_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Get total chunks stored
    pub fn chunks_stored(&self) -> u64 {
        self.chunks_stored.load(Ordering::Relaxed)
    }

    /// Get total chunks loaded
    pub fn chunks_loaded(&self) -> u64 {
        self.chunks_loaded.load(Ordering::Relaxed)
    }

    /// Get total bytes chunked
    pub fn bytes_chunked(&self) -> u64 {
        self.bytes_chunked.load(Ordering::Relaxed)
    }

    /// Get total checksum failures
    pub fn checksum_failures(&self) -> u64 {
        self.checksum_failures.load(Ordering::Relaxed)
    }

    /// Reset all counters to zero
    pub fn reset(&self) {
        self.chunks_stored.store(0, Ordering::Relaxed);
        self.chunks_loaded.store(0, Ordering::Relaxed);
        self.bytes_chunked.store(0, Ordering::Relaxed);
        self.checksum_failures.store(0, Ordering::Relaxed);
    }
}

impl Clone for ResultChunker {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            stats: ChunkingStats::new(),
        }
    }
}

/// Core chunking engine that splits and reassembles large payloads
pub struct ResultChunker {
    config: ChunkingConfig,
    stats: ChunkingStats,
}

impl ResultChunker {
    /// Create a new chunker with the given configuration
    pub fn new(config: ChunkingConfig) -> Self {
        Self {
            config,
            stats: ChunkingStats::new(),
        }
    }

    /// Check if the given data exceeds the chunking threshold
    pub fn needs_chunking(&self, data: &[u8]) -> bool {
        self.config.enabled && data.len() > self.config.threshold_bytes
    }

    /// Create a sentinel value that encodes the chunk metadata.
    ///
    /// This sentinel is stored at the primary Redis key so that readers
    /// can detect that the real data lives in chunk keys.
    pub fn create_sentinel(&self, metadata: &ChunkMetadata) -> Vec<u8> {
        let mut sentinel = CHUNKED_SENTINEL.to_vec();
        if let Ok(json) = serde_json::to_vec(metadata) {
            sentinel.extend_from_slice(&json);
        }
        sentinel
    }

    /// Check whether a Redis value is a chunked sentinel
    pub fn is_chunked(data: &[u8]) -> bool {
        data.len() >= CHUNKED_SENTINEL.len() && data[..CHUNKED_SENTINEL.len()] == *CHUNKED_SENTINEL
    }

    /// Parse chunk metadata from a sentinel value
    pub fn parse_sentinel(data: &[u8]) -> Result<ChunkMetadata, std::io::Error> {
        if !Self::is_chunked(data) {
            return Err(std::io::Error::other("not a chunked sentinel"));
        }
        let json = &data[CHUNKED_SENTINEL.len()..];
        serde_json::from_slice(json).map_err(|e| std::io::Error::other(e.to_string()))
    }

    /// Split data into chunks, returning metadata and the chunk payloads
    pub fn split_chunks(&self, data: &[u8]) -> (ChunkMetadata, Vec<Vec<u8>>) {
        let chunk_size = if self.config.chunk_size_bytes == 0 {
            // Guard against zero-size chunks
            DEFAULT_CHUNK_SIZE_BYTES
        } else {
            self.config.chunk_size_bytes
        };

        let total_chunks = if data.is_empty() {
            1
        } else {
            data.len().div_ceil(chunk_size)
        };

        let checksum = if self.config.checksum_enabled {
            Some(crc32fast::hash(data))
        } else {
            None
        };

        let created_at = chrono::Utc::now().timestamp() as u64;

        let metadata = ChunkMetadata {
            total_chunks,
            chunk_size,
            total_size: data.len(),
            checksum,
            created_at,
        };

        let chunks: Vec<Vec<u8>> = if data.is_empty() {
            vec![Vec::new()]
        } else {
            data.chunks(chunk_size).map(|c| c.to_vec()).collect()
        };

        self.stats
            .record_store(total_chunks as u64, data.len() as u64);

        (metadata, chunks)
    }

    /// Reassemble chunks back into the original data, verifying integrity
    pub fn reassemble_chunks(
        &self,
        metadata: &ChunkMetadata,
        chunks: &[Vec<u8>],
    ) -> Result<Vec<u8>, std::io::Error> {
        if chunks.len() != metadata.total_chunks {
            return Err(std::io::Error::other(format!(
                "chunk count mismatch: expected {}, got {}",
                metadata.total_chunks,
                chunks.len()
            )));
        }

        let mut result = Vec::with_capacity(metadata.total_size);
        for chunk in chunks {
            result.extend_from_slice(chunk);
        }

        if result.len() != metadata.total_size {
            return Err(std::io::Error::other(format!(
                "reassembled size mismatch: expected {}, got {}",
                metadata.total_size,
                result.len()
            )));
        }

        // Verify CRC32 checksum if present
        if let Some(expected) = metadata.checksum {
            let actual = crc32fast::hash(&result);
            if actual != expected {
                self.stats.record_checksum_failure();
                return Err(std::io::Error::other(format!(
                    "checksum mismatch: expected {expected}, got {actual}"
                )));
            }
        }

        self.stats.record_load(metadata.total_chunks as u64);

        Ok(result)
    }

    /// Generate the Redis keys where individual chunks are stored
    pub fn chunk_keys(base_key: &str, total_chunks: usize) -> Vec<String> {
        (0..total_chunks)
            .map(|i| format!("{base_key}:chunk:{i}"))
            .collect()
    }

    /// Generate the Redis key for the chunk metadata
    pub fn metadata_key(base_key: &str) -> String {
        format!("{base_key}:chunks")
    }

    /// Get a reference to the chunking configuration
    pub fn config(&self) -> &ChunkingConfig {
        &self.config
    }

    /// Get a reference to the chunking statistics
    pub fn stats(&self) -> &ChunkingStats {
        &self.stats
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_chunking_config_defaults() {
        let config = ChunkingConfig::default();
        assert!(config.enabled);
        assert_eq!(config.threshold_bytes, 524_288);
        assert_eq!(config.chunk_size_bytes, 262_144);
        assert!(config.checksum_enabled);
    }

    #[test]
    fn test_needs_chunking() {
        let chunker = ResultChunker::new(
            ChunkingConfig::new()
                .with_threshold(100)
                .with_chunk_size(50),
        );

        // Below threshold
        let small = vec![0u8; 50];
        assert!(!chunker.needs_chunking(&small));

        // Exactly at threshold
        let exact = vec![0u8; 100];
        assert!(!chunker.needs_chunking(&exact));

        // Above threshold
        let large = vec![0u8; 101];
        assert!(chunker.needs_chunking(&large));

        // Disabled chunking
        let disabled_chunker = ResultChunker::new(ChunkingConfig::disabled());
        let huge = vec![0u8; 1_000_000];
        assert!(!disabled_chunker.needs_chunking(&huge));
    }

    #[test]
    fn test_split_and_reassemble() {
        let chunker =
            ResultChunker::new(ChunkingConfig::new().with_threshold(10).with_chunk_size(50));

        // Various sizes
        for size in [0, 1, 49, 50, 51, 100, 150, 255, 1000] {
            let data: Vec<u8> = (0..size).map(|i| (i % 256) as u8).collect();
            let (metadata, chunks) = chunker.split_chunks(&data);
            let reassembled = chunker
                .reassemble_chunks(&metadata, &chunks)
                .expect("reassemble failed");
            assert_eq!(reassembled, data, "roundtrip failed for size {size}");
        }
    }

    #[test]
    fn test_split_exact_boundary() {
        let chunk_size = 64;
        let chunker = ResultChunker::new(
            ChunkingConfig::new()
                .with_threshold(0)
                .with_chunk_size(chunk_size),
        );

        // Exactly 3 chunks
        let data = vec![0xABu8; chunk_size * 3];
        let (metadata, chunks) = chunker.split_chunks(&data);
        assert_eq!(metadata.total_chunks, 3);
        assert_eq!(chunks.len(), 3);
        for chunk in &chunks {
            assert_eq!(chunk.len(), chunk_size);
        }
        let reassembled = chunker
            .reassemble_chunks(&metadata, &chunks)
            .expect("reassemble failed");
        assert_eq!(reassembled, data);
    }

    #[test]
    fn test_split_one_byte_over() {
        let chunk_size = 64;
        let chunker = ResultChunker::new(
            ChunkingConfig::new()
                .with_threshold(0)
                .with_chunk_size(chunk_size),
        );

        let data = vec![0xCDu8; chunk_size * 2 + 1];
        let (metadata, chunks) = chunker.split_chunks(&data);
        assert_eq!(metadata.total_chunks, 3);
        assert_eq!(chunks.len(), 3);
        assert_eq!(chunks[0].len(), chunk_size);
        assert_eq!(chunks[1].len(), chunk_size);
        assert_eq!(chunks[2].len(), 1);

        let reassembled = chunker
            .reassemble_chunks(&metadata, &chunks)
            .expect("reassemble failed");
        assert_eq!(reassembled, data);
    }

    #[test]
    fn test_checksum_verification() {
        let chunker = ResultChunker::new(
            ChunkingConfig::new()
                .with_threshold(0)
                .with_chunk_size(50)
                .with_checksum(true),
        );

        let data = vec![0xFFu8; 100];
        let (metadata, mut chunks) = chunker.split_chunks(&data);

        // Corrupt the second chunk
        if let Some(byte) = chunks[1].first_mut() {
            *byte = byte.wrapping_add(1);
        }

        let result = chunker.reassemble_chunks(&metadata, &chunks);
        assert!(result.is_err());
        assert!(result
            .as_ref()
            .err()
            .is_some_and(|e| e.to_string().contains("checksum mismatch")));
        assert_eq!(chunker.stats().checksum_failures(), 1);
    }

    #[test]
    fn test_checksum_disabled() {
        let chunker = ResultChunker::new(
            ChunkingConfig::new()
                .with_threshold(0)
                .with_chunk_size(50)
                .with_checksum(false),
        );

        let data = vec![0xFFu8; 100];
        let (mut metadata, mut chunks) = chunker.split_chunks(&data);

        // Corrupt a chunk -- should pass because checksum is disabled
        if let Some(byte) = chunks[0].first_mut() {
            *byte = byte.wrapping_add(1);
        }
        // Ensure metadata has no checksum
        assert!(metadata.checksum.is_none());
        // Force total_size to match corrupted data length
        metadata.total_size = chunks.iter().map(|c| c.len()).sum();

        let result = chunker.reassemble_chunks(&metadata, &chunks);
        assert!(result.is_ok());
    }

    #[test]
    fn test_sentinel_roundtrip() {
        let chunker = ResultChunker::new(ChunkingConfig::new());
        let metadata = ChunkMetadata {
            total_chunks: 5,
            chunk_size: 256,
            total_size: 1234,
            checksum: Some(0xDEADBEEF),
            created_at: 1700000000,
        };

        let sentinel = chunker.create_sentinel(&metadata);
        assert!(ResultChunker::is_chunked(&sentinel));

        let parsed = ResultChunker::parse_sentinel(&sentinel).expect("parse sentinel failed");
        assert_eq!(parsed.total_chunks, 5);
        assert_eq!(parsed.chunk_size, 256);
        assert_eq!(parsed.total_size, 1234);
        assert_eq!(parsed.checksum, Some(0xDEADBEEF));
        assert_eq!(parsed.created_at, 1700000000);
    }

    #[test]
    fn test_sentinel_not_chunked() {
        let data = b"just regular data";
        assert!(!ResultChunker::is_chunked(data));

        let result = ResultChunker::parse_sentinel(data);
        assert!(result.is_err());
    }

    #[test]
    fn test_chunk_keys() {
        let keys = ResultChunker::chunk_keys("celery-task-meta-abc123", 3);
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], "celery-task-meta-abc123:chunk:0");
        assert_eq!(keys[1], "celery-task-meta-abc123:chunk:1");
        assert_eq!(keys[2], "celery-task-meta-abc123:chunk:2");
    }

    #[test]
    fn test_metadata_key() {
        let key = ResultChunker::metadata_key("celery-task-meta-abc123");
        assert_eq!(key, "celery-task-meta-abc123:chunks");
    }

    #[test]
    fn test_chunk_count_mismatch() {
        let chunker =
            ResultChunker::new(ChunkingConfig::new().with_threshold(0).with_chunk_size(50));

        let data = vec![0u8; 100];
        let (metadata, chunks) = chunker.split_chunks(&data);
        assert_eq!(chunks.len(), 2);

        // Provide only one chunk
        let result = chunker.reassemble_chunks(&metadata, &chunks[..1]);
        assert!(result.is_err());
        assert!(result
            .as_ref()
            .err()
            .is_some_and(|e| e.to_string().contains("chunk count mismatch")));
    }

    #[test]
    fn test_empty_data() {
        let chunker =
            ResultChunker::new(ChunkingConfig::new().with_threshold(0).with_chunk_size(50));

        let data: Vec<u8> = Vec::new();
        let (metadata, chunks) = chunker.split_chunks(&data);
        assert_eq!(metadata.total_chunks, 1);
        assert_eq!(metadata.total_size, 0);
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].is_empty());

        let reassembled = chunker
            .reassemble_chunks(&metadata, &chunks)
            .expect("reassemble failed");
        assert!(reassembled.is_empty());
    }

    #[test]
    fn test_stats_tracking() {
        let chunker = ResultChunker::new(
            ChunkingConfig::new()
                .with_threshold(0)
                .with_chunk_size(50)
                .with_checksum(true),
        );

        // Store operation
        let data = vec![0u8; 120];
        let (metadata, chunks) = chunker.split_chunks(&data);
        assert_eq!(chunker.stats().chunks_stored(), 3);
        assert_eq!(chunker.stats().bytes_chunked(), 120);

        // Load operation
        let _reassembled = chunker
            .reassemble_chunks(&metadata, &chunks)
            .expect("reassemble failed");
        assert_eq!(chunker.stats().chunks_loaded(), 3);

        // Reset
        chunker.stats().reset();
        assert_eq!(chunker.stats().chunks_stored(), 0);
        assert_eq!(chunker.stats().chunks_loaded(), 0);
        assert_eq!(chunker.stats().bytes_chunked(), 0);
        assert_eq!(chunker.stats().checksum_failures(), 0);
    }
}
