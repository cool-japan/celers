//! Result compression for large payloads
//!
//! This module provides compression utilities for task results to reduce
//! storage space and network bandwidth when storing large payloads in Redis.
//!
//! # Compression Strategy
//!
//! - Results smaller than threshold (default 1KB) are stored uncompressed
//! - Results larger than threshold are compressed with gzip or zstd
//! - Compression is automatic and transparent to the caller
//! - Compressed data is prefixed with a marker for automatic detection
//!
//! # Algorithms
//!
//! - **Gzip** (default): Widely supported, good compression ratio
//! - **Zstd** (feature-gated: `zstd-compression`): Better compression ratio and faster decompression

use oxiarc_deflate::{gzip_compress, gzip_decompress};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

/// Magic bytes to identify gzip-compressed data
const GZIP_MARKER: &[u8] = b"CELERS_GZIP:";

/// Magic bytes to identify zstd-compressed data
#[cfg(feature = "zstd-compression")]
const ZSTD_MARKER: &[u8] = b"CELERS_ZSTD:";

/// Default compression threshold (1KB)
const DEFAULT_COMPRESSION_THRESHOLD: usize = 1024;

/// Compression algorithm selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CompressionAlgorithm {
    /// Gzip compression (default)
    #[default]
    Gzip,
    /// Zstd compression (requires `zstd-compression` feature)
    #[cfg(feature = "zstd-compression")]
    Zstd,
}

/// Compression configuration
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Enable compression
    pub enabled: bool,

    /// Minimum size (bytes) to trigger compression
    pub threshold: usize,

    /// Compression level (0-9 for gzip, 1-22 for zstd)
    pub level: u32,

    /// Compression algorithm to use
    pub algorithm: CompressionAlgorithm,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold: DEFAULT_COMPRESSION_THRESHOLD,
            level: 6, // Default compression level
            algorithm: CompressionAlgorithm::default(),
        }
    }
}

impl CompressionConfig {
    /// Create new compression config with defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Disable compression
    pub fn disabled() -> Self {
        Self {
            enabled: false,
            threshold: DEFAULT_COMPRESSION_THRESHOLD,
            level: 6,
            algorithm: CompressionAlgorithm::default(),
        }
    }

    /// Set compression threshold
    pub fn with_threshold(mut self, threshold: usize) -> Self {
        self.threshold = threshold;
        self
    }

    /// Set compression level (0-9 for gzip, 1-22 for zstd)
    pub fn with_level(mut self, level: u32) -> Self {
        self.level = match self.algorithm {
            CompressionAlgorithm::Gzip => level.min(9),
            #[cfg(feature = "zstd-compression")]
            CompressionAlgorithm::Zstd => level.min(22),
        };
        self
    }

    /// Enable or disable compression
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Set the compression algorithm
    pub fn with_algorithm(mut self, algorithm: CompressionAlgorithm) -> Self {
        self.algorithm = algorithm;
        self
    }

    /// Create a config for Zstd compression (convenience method)
    #[cfg(feature = "zstd-compression")]
    pub fn zstd() -> Self {
        Self {
            enabled: true,
            threshold: DEFAULT_COMPRESSION_THRESHOLD,
            level: 3, // Zstd default level
            algorithm: CompressionAlgorithm::Zstd,
        }
    }
}

/// Compress data if it exceeds the threshold
///
/// Returns the compressed data with a marker prefix if compression was applied,
/// or the original data if it was below the threshold or compression is disabled.
pub fn maybe_compress(data: &[u8], config: &CompressionConfig) -> Result<Vec<u8>, std::io::Error> {
    // Skip compression if disabled or below threshold
    if !config.enabled || data.len() < config.threshold {
        return Ok(data.to_vec());
    }

    match config.algorithm {
        CompressionAlgorithm::Gzip => compress_gzip(data, config.level),
        #[cfg(feature = "zstd-compression")]
        CompressionAlgorithm::Zstd => compress_zstd(data, config.level),
    }
}

/// Compress data using gzip
fn compress_gzip(data: &[u8], level: u32) -> Result<Vec<u8>, std::io::Error> {
    let compressed = gzip_compress(data, level.min(9) as u8)
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    // Only use compression if it actually reduces size
    if compressed.len() < data.len() {
        let mut result = Vec::with_capacity(GZIP_MARKER.len() + compressed.len());
        result.extend_from_slice(GZIP_MARKER);
        result.extend_from_slice(&compressed);
        Ok(result)
    } else {
        Ok(data.to_vec())
    }
}

/// Compress data using zstd
#[cfg(feature = "zstd-compression")]
fn compress_zstd(data: &[u8], level: u32) -> Result<Vec<u8>, std::io::Error> {
    let compressed = oxiarc_zstd::compress_with_level(data, level.min(22) as i32)
        .map_err(|e| std::io::Error::other(e.to_string()))?;

    // Only use compression if it actually reduces size
    if compressed.len() < data.len() {
        let mut result = Vec::with_capacity(ZSTD_MARKER.len() + compressed.len());
        result.extend_from_slice(ZSTD_MARKER);
        result.extend_from_slice(&compressed);
        Ok(result)
    } else {
        Ok(data.to_vec())
    }
}

/// Decompress data if it has a compression marker
///
/// Automatically detects which compression algorithm was used (gzip or zstd)
/// and decompresses accordingly. Returns the original data if not compressed.
pub fn maybe_decompress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    // Check for gzip marker
    if data.len() >= GZIP_MARKER.len() && &data[..GZIP_MARKER.len()] == GZIP_MARKER {
        let compressed = &data[GZIP_MARKER.len()..];
        return gzip_decompress(compressed).map_err(|e| std::io::Error::other(e.to_string()));
    }

    // Check for zstd marker
    #[cfg(feature = "zstd-compression")]
    if data.len() >= ZSTD_MARKER.len() && &data[..ZSTD_MARKER.len()] == ZSTD_MARKER {
        let compressed = &data[ZSTD_MARKER.len()..];
        return oxiarc_zstd::decompress(compressed)
            .map_err(|e| std::io::Error::other(e.to_string()));
    }

    // Not compressed
    Ok(data.to_vec())
}

/// Calculate compression ratio (compressed size / original size)
pub fn compression_ratio(original_size: usize, compressed_size: usize) -> f64 {
    if original_size == 0 {
        return 1.0;
    }
    compressed_size as f64 / original_size as f64
}

/// Compression statistics tracker
///
/// Tracks original sizes, compressed sizes, and compression ratios
/// across multiple compression operations. Thread-safe via atomics.
#[derive(Debug)]
pub struct CompressionStats {
    /// Total number of compression operations
    operations: AtomicU64,
    /// Total original bytes before compression
    total_original_bytes: AtomicUsize,
    /// Total compressed bytes after compression
    total_compressed_bytes: AtomicUsize,
    /// Number of operations where compression was applied (data was smaller)
    compressions_applied: AtomicU64,
}

impl Default for CompressionStats {
    fn default() -> Self {
        Self::new()
    }
}

impl CompressionStats {
    /// Create a new empty stats tracker
    pub fn new() -> Self {
        Self {
            operations: AtomicU64::new(0),
            total_original_bytes: AtomicUsize::new(0),
            total_compressed_bytes: AtomicUsize::new(0),
            compressions_applied: AtomicU64::new(0),
        }
    }

    /// Record a compression operation
    pub fn record(&self, original_size: usize, compressed_size: usize) {
        self.operations.fetch_add(1, Ordering::Relaxed);
        self.total_original_bytes
            .fetch_add(original_size, Ordering::Relaxed);
        self.total_compressed_bytes
            .fetch_add(compressed_size, Ordering::Relaxed);
        if compressed_size < original_size {
            self.compressions_applied.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Get total number of compression operations
    pub fn operation_count(&self) -> u64 {
        self.operations.load(Ordering::Relaxed)
    }

    /// Get total original bytes processed
    pub fn total_original_bytes(&self) -> usize {
        self.total_original_bytes.load(Ordering::Relaxed)
    }

    /// Get total compressed bytes stored
    pub fn total_compressed_bytes(&self) -> usize {
        self.total_compressed_bytes.load(Ordering::Relaxed)
    }

    /// Get the overall compression ratio (0.0 to 1.0, lower is better)
    pub fn overall_ratio(&self) -> f64 {
        let original = self.total_original_bytes.load(Ordering::Relaxed);
        let compressed = self.total_compressed_bytes.load(Ordering::Relaxed);
        compression_ratio(original, compressed)
    }

    /// Get the number of operations where compression was actually applied
    pub fn compressions_applied(&self) -> u64 {
        self.compressions_applied.load(Ordering::Relaxed)
    }

    /// Get the total bytes saved by compression
    pub fn bytes_saved(&self) -> usize {
        let original = self.total_original_bytes.load(Ordering::Relaxed);
        let compressed = self.total_compressed_bytes.load(Ordering::Relaxed);
        original.saturating_sub(compressed)
    }

    /// Reset all statistics
    pub fn reset(&self) {
        self.operations.store(0, Ordering::Relaxed);
        self.total_original_bytes.store(0, Ordering::Relaxed);
        self.total_compressed_bytes.store(0, Ordering::Relaxed);
        self.compressions_applied.store(0, Ordering::Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_config_defaults() {
        let config = CompressionConfig::default();
        assert!(config.enabled);
        assert_eq!(config.threshold, 1024);
        assert_eq!(config.level, 6);
        assert_eq!(config.algorithm, CompressionAlgorithm::Gzip);
    }

    #[test]
    fn test_compression_config_builder() {
        let config = CompressionConfig::new()
            .with_threshold(2048)
            .with_level(9)
            .with_enabled(true);

        assert!(config.enabled);
        assert_eq!(config.threshold, 2048);
        assert_eq!(config.level, 9);
    }

    #[test]
    fn test_compression_disabled() {
        let config = CompressionConfig::disabled();
        assert!(!config.enabled);

        let data = b"test data that should not be compressed";
        let result = maybe_compress(data, &config).expect("compress failed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_compression_below_threshold() {
        let config = CompressionConfig::new().with_threshold(1000);
        let data = b"small"; // 5 bytes
        let result = maybe_compress(data, &config).expect("compress failed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_gzip_compression_and_decompression() {
        let config = CompressionConfig::new().with_threshold(10);

        // Create some compressible data
        let original = b"This is a test string that should compress well because it has repetition. \
                         This is a test string that should compress well because it has repetition. \
                         This is a test string that should compress well because it has repetition.";

        // Compress
        let compressed = maybe_compress(original, &config).expect("compress failed");

        // Should be compressed (marker + data)
        assert!(compressed.starts_with(GZIP_MARKER));
        assert!(compressed.len() < original.len());

        // Decompress
        let decompressed = maybe_decompress(&compressed).expect("decompress failed");
        assert_eq!(decompressed, original);
    }

    #[cfg(feature = "zstd-compression")]
    #[test]
    fn test_zstd_compression_and_decompression() {
        let config = CompressionConfig::zstd().with_threshold(10);

        // Create some compressible data
        let original = b"This is a test string that should compress well because it has repetition. \
                         This is a test string that should compress well because it has repetition. \
                         This is a test string that should compress well because it has repetition.";

        // Compress
        let compressed = maybe_compress(original, &config).expect("compress failed");

        // Should be compressed (marker + data)
        assert!(compressed.starts_with(ZSTD_MARKER));
        assert!(compressed.len() < original.len());

        // Decompress
        let decompressed = maybe_decompress(&compressed).expect("decompress failed");
        assert_eq!(decompressed, original);
    }

    #[cfg(feature = "zstd-compression")]
    #[test]
    fn test_auto_detect_gzip_vs_zstd() {
        let original = b"This is a test string that should compress well because it has repetition. \
                         This is a test string that should compress well because it has repetition. \
                         This is a test string that should compress well because it has repetition.";

        // Compress with gzip
        let gzip_config = CompressionConfig::new().with_threshold(10);
        let gzip_compressed = maybe_compress(original, &gzip_config).expect("gzip compress failed");
        assert!(gzip_compressed.starts_with(GZIP_MARKER));

        // Compress with zstd
        let zstd_config = CompressionConfig::zstd().with_threshold(10);
        let zstd_compressed = maybe_compress(original, &zstd_config).expect("zstd compress failed");
        assert!(zstd_compressed.starts_with(ZSTD_MARKER));

        // Both should decompress correctly via auto-detection
        let gzip_decompressed = maybe_decompress(&gzip_compressed).expect("gzip decompress failed");
        let zstd_decompressed = maybe_decompress(&zstd_compressed).expect("zstd decompress failed");

        assert_eq!(gzip_decompressed, original);
        assert_eq!(zstd_decompressed, original);
    }

    #[test]
    fn test_decompress_uncompressed_data() {
        let data = b"uncompressed data";
        let result = maybe_decompress(data).expect("decompress failed");
        assert_eq!(result, data);
    }

    #[test]
    fn test_compression_ratio_calculation() {
        assert_eq!(compression_ratio(1000, 500), 0.5);
        assert_eq!(compression_ratio(1000, 1000), 1.0);
        assert_eq!(compression_ratio(0, 0), 1.0);
    }

    #[test]
    fn test_compression_level_capping() {
        let config = CompressionConfig::new().with_level(999);
        assert_eq!(config.level, 9);
    }

    #[cfg(feature = "zstd-compression")]
    #[test]
    fn test_zstd_compression_level_capping() {
        let config = CompressionConfig::zstd().with_level(999);
        assert_eq!(config.level, 22);
    }

    #[test]
    fn test_compression_no_benefit() {
        let config = CompressionConfig::new().with_threshold(0);

        // Random data that won't compress well
        let data = b"abc123xyz";
        let result = maybe_compress(data, &config).expect("compress failed");

        // Should return original data if compression doesn't help
        if !result.starts_with(GZIP_MARKER) {
            assert_eq!(result, data);
        }
    }

    #[test]
    fn test_compression_stats() {
        let stats = CompressionStats::new();

        assert_eq!(stats.operation_count(), 0);
        assert_eq!(stats.total_original_bytes(), 0);
        assert_eq!(stats.total_compressed_bytes(), 0);
        assert_eq!(stats.compressions_applied(), 0);

        // Record some operations
        stats.record(1000, 500); // Compression helped
        stats.record(100, 100); // No compression benefit
        stats.record(2000, 800); // Compression helped

        assert_eq!(stats.operation_count(), 3);
        assert_eq!(stats.total_original_bytes(), 3100);
        assert_eq!(stats.total_compressed_bytes(), 1400);
        assert_eq!(stats.compressions_applied(), 2);
        assert_eq!(stats.bytes_saved(), 1700);

        let ratio = stats.overall_ratio();
        assert!((ratio - 1400.0 / 3100.0).abs() < 0.001);

        // Reset
        stats.reset();
        assert_eq!(stats.operation_count(), 0);
        assert_eq!(stats.total_original_bytes(), 0);
    }

    #[test]
    fn test_compression_algorithm_default() {
        assert_eq!(CompressionAlgorithm::default(), CompressionAlgorithm::Gzip);
    }
}
