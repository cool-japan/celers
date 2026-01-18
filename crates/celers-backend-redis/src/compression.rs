//! Result compression for large payloads
//!
//! This module provides compression utilities for task results to reduce
//! storage space and network bandwidth when storing large payloads in Redis.
//!
//! # Compression Strategy
//!
//! - Results smaller than threshold (default 1KB) are stored uncompressed
//! - Results larger than threshold are compressed with gzip
//! - Compression is automatic and transparent to the caller
//! - Compressed data is prefixed with a marker for automatic detection

use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use flate2::Compression;
use std::io::{Read, Write};

/// Magic bytes to identify compressed data
const COMPRESSION_MARKER: &[u8] = b"CELERS_GZIP:";

/// Default compression threshold (1KB)
const DEFAULT_COMPRESSION_THRESHOLD: usize = 1024;

/// Compression configuration
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Enable compression
    pub enabled: bool,

    /// Minimum size (bytes) to trigger compression
    pub threshold: usize,

    /// Compression level (0-9)
    pub level: u32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            threshold: DEFAULT_COMPRESSION_THRESHOLD,
            level: 6, // Default compression level
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
        }
    }

    /// Set compression threshold
    pub fn with_threshold(mut self, threshold: usize) -> Self {
        self.threshold = threshold;
        self
    }

    /// Set compression level (0-9, higher = better compression but slower)
    pub fn with_level(mut self, level: u32) -> Self {
        self.level = level.min(9); // Cap at 9
        self
    }

    /// Enable or disable compression
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
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

    // Compress the data
    let mut encoder = GzEncoder::new(Vec::new(), Compression::new(config.level));
    encoder.write_all(data)?;
    let compressed = encoder.finish()?;

    // Only use compression if it actually reduces size
    if compressed.len() < data.len() {
        // Add marker prefix
        let mut result = Vec::with_capacity(COMPRESSION_MARKER.len() + compressed.len());
        result.extend_from_slice(COMPRESSION_MARKER);
        result.extend_from_slice(&compressed);
        Ok(result)
    } else {
        // Compression didn't help, use original
        Ok(data.to_vec())
    }
}

/// Decompress data if it has the compression marker
///
/// Automatically detects if data is compressed and decompresses it,
/// or returns the original data if not compressed.
pub fn maybe_decompress(data: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    // Check for compression marker
    if data.len() < COMPRESSION_MARKER.len() {
        return Ok(data.to_vec());
    }

    if &data[..COMPRESSION_MARKER.len()] != COMPRESSION_MARKER {
        return Ok(data.to_vec());
    }

    // Data is compressed, decompress it
    let compressed = &data[COMPRESSION_MARKER.len()..];
    let mut decoder = GzDecoder::new(compressed);
    let mut decompressed = Vec::new();
    decoder.read_to_end(&mut decompressed)?;

    Ok(decompressed)
}

/// Calculate compression ratio (compressed size / original size)
pub fn compression_ratio(original_size: usize, compressed_size: usize) -> f64 {
    if original_size == 0 {
        return 1.0;
    }
    compressed_size as f64 / original_size as f64
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
        let result = maybe_compress(data, &config).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_compression_below_threshold() {
        let config = CompressionConfig::new().with_threshold(1000);
        let data = b"small"; // 5 bytes
        let result = maybe_compress(data, &config).unwrap();
        assert_eq!(result, data);
    }

    #[test]
    fn test_compression_and_decompression() {
        let config = CompressionConfig::new().with_threshold(10);

        // Create some compressible data
        let original = b"This is a test string that should compress well because it has repetition. \
                         This is a test string that should compress well because it has repetition. \
                         This is a test string that should compress well because it has repetition.";

        // Compress
        let compressed = maybe_compress(original, &config).unwrap();

        // Should be compressed (marker + data)
        assert!(compressed.starts_with(COMPRESSION_MARKER));
        assert!(compressed.len() < original.len());

        // Decompress
        let decompressed = maybe_decompress(&compressed).unwrap();
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_decompress_uncompressed_data() {
        let data = b"uncompressed data";
        let result = maybe_decompress(data).unwrap();
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

    #[test]
    fn test_compression_no_benefit() {
        let config = CompressionConfig::new().with_threshold(0);

        // Random data that won't compress well
        let data = b"abc123xyz";
        let result = maybe_compress(data, &config).unwrap();

        // Should return original data if compression doesn't help
        // (In this case, compressed size might be larger due to gzip overhead)
        if !result.starts_with(COMPRESSION_MARKER) {
            assert_eq!(result, data);
        }
    }
}
