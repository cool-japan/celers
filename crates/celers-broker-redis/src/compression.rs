//! Payload compression for memory optimization
//!
//! Automatically compresses large payloads to reduce Redis memory usage
//! and network bandwidth. Supports multiple compression algorithms.

use celers_core::{CelersError, Result};
use std::io::{Read, Write};

/// Compression algorithm
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompressionAlgorithm {
    /// No compression
    None,
    /// Gzip compression (good balance of speed and ratio)
    Gzip,
    /// Zlib compression (similar to gzip)
    Zlib,
}

impl CompressionAlgorithm {
    /// Get the algorithm identifier byte
    pub fn id(&self) -> u8 {
        match self {
            CompressionAlgorithm::None => 0,
            CompressionAlgorithm::Gzip => 1,
            CompressionAlgorithm::Zlib => 2,
        }
    }

    /// Get algorithm from identifier byte
    pub fn from_id(id: u8) -> Option<Self> {
        match id {
            0 => Some(CompressionAlgorithm::None),
            1 => Some(CompressionAlgorithm::Gzip),
            2 => Some(CompressionAlgorithm::Zlib),
            _ => None,
        }
    }

    /// Get the name of the algorithm
    pub fn name(&self) -> &'static str {
        match self {
            CompressionAlgorithm::None => "none",
            CompressionAlgorithm::Gzip => "gzip",
            CompressionAlgorithm::Zlib => "zlib",
        }
    }
}

impl std::fmt::Display for CompressionAlgorithm {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

/// Compression configuration
#[derive(Debug, Clone)]
pub struct CompressionConfig {
    /// Compression algorithm to use
    pub algorithm: CompressionAlgorithm,
    /// Minimum payload size to trigger compression (in bytes)
    pub threshold: usize,
    /// Compression level (0-9, where 9 is maximum compression)
    pub level: u32,
}

impl Default for CompressionConfig {
    fn default() -> Self {
        Self {
            algorithm: CompressionAlgorithm::Gzip,
            threshold: 1024, // 1 KB
            level: 6,        // Default compression level
        }
    }
}

impl CompressionConfig {
    /// Create a new compression configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set compression algorithm
    pub fn with_algorithm(mut self, algorithm: CompressionAlgorithm) -> Self {
        self.algorithm = algorithm;
        self
    }

    /// Set compression threshold
    pub fn with_threshold(mut self, threshold: usize) -> Self {
        self.threshold = threshold;
        self
    }

    /// Set compression level
    pub fn with_level(mut self, level: u32) -> Self {
        self.level = level.min(9);
        self
    }

    /// Disable compression
    pub fn disabled() -> Self {
        Self {
            algorithm: CompressionAlgorithm::None,
            threshold: usize::MAX,
            level: 0,
        }
    }
}

/// Compressor for payload compression
pub struct Compressor {
    config: CompressionConfig,
}

impl Compressor {
    /// Create a new compressor with default configuration
    pub fn new() -> Self {
        Self {
            config: CompressionConfig::default(),
        }
    }

    /// Create a compressor with custom configuration
    pub fn with_config(config: CompressionConfig) -> Self {
        Self { config }
    }

    /// Compress data if it exceeds the threshold
    ///
    /// Returns a tuple of (compressed_data, was_compressed)
    /// The first byte of compressed data indicates the algorithm used
    pub fn compress(&self, data: &[u8]) -> Result<(Vec<u8>, bool)> {
        // Skip compression if disabled or below threshold
        if self.config.algorithm == CompressionAlgorithm::None || data.len() < self.config.threshold
        {
            return Ok((data.to_vec(), false));
        }

        match self.config.algorithm {
            CompressionAlgorithm::None => Ok((data.to_vec(), false)),
            CompressionAlgorithm::Gzip => self.compress_gzip(data),
            CompressionAlgorithm::Zlib => self.compress_zlib(data),
        }
    }

    /// Decompress data
    ///
    /// Automatically detects the compression algorithm from the first byte
    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        if data.is_empty() {
            return Ok(Vec::new());
        }

        // Check if data is compressed (first byte is algorithm ID)
        let algorithm_id = data[0];
        let algorithm = match CompressionAlgorithm::from_id(algorithm_id) {
            Some(algo) => algo,
            None => {
                // Assume uncompressed data
                return Ok(data.to_vec());
            }
        };

        if algorithm == CompressionAlgorithm::None {
            // Not compressed, return as-is (skip first byte)
            return Ok(data[1..].to_vec());
        }

        let compressed_data = &data[1..]; // Skip algorithm ID byte

        match algorithm {
            CompressionAlgorithm::None => Ok(compressed_data.to_vec()),
            CompressionAlgorithm::Gzip => self.decompress_gzip(compressed_data),
            CompressionAlgorithm::Zlib => self.decompress_zlib(compressed_data),
        }
    }

    /// Compress using gzip
    fn compress_gzip(&self, data: &[u8]) -> Result<(Vec<u8>, bool)> {
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(self.config.level));
        encoder.write_all(data).map_err(|e| {
            CelersError::Serialization(format!("Failed to compress with gzip: {}", e))
        })?;

        let mut compressed = encoder.finish().map_err(|e| {
            CelersError::Serialization(format!("Failed to finish gzip compression: {}", e))
        })?;

        // Prepend algorithm ID
        let mut result = vec![CompressionAlgorithm::Gzip.id()];
        result.append(&mut compressed);

        Ok((result, true))
    }

    /// Decompress using gzip
    fn decompress_gzip(&self, data: &[u8]) -> Result<Vec<u8>> {
        use flate2::read::GzDecoder;

        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).map_err(|e| {
            CelersError::Deserialization(format!("Failed to decompress with gzip: {}", e))
        })?;

        Ok(decompressed)
    }

    /// Compress using zlib
    fn compress_zlib(&self, data: &[u8]) -> Result<(Vec<u8>, bool)> {
        use flate2::write::ZlibEncoder;
        use flate2::Compression;

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(self.config.level));
        encoder.write_all(data).map_err(|e| {
            CelersError::Serialization(format!("Failed to compress with zlib: {}", e))
        })?;

        let mut compressed = encoder.finish().map_err(|e| {
            CelersError::Serialization(format!("Failed to finish zlib compression: {}", e))
        })?;

        // Prepend algorithm ID
        let mut result = vec![CompressionAlgorithm::Zlib.id()];
        result.append(&mut compressed);

        Ok((result, true))
    }

    /// Decompress using zlib
    fn decompress_zlib(&self, data: &[u8]) -> Result<Vec<u8>> {
        use flate2::read::ZlibDecoder;

        let mut decoder = ZlibDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).map_err(|e| {
            CelersError::Deserialization(format!("Failed to decompress with zlib: {}", e))
        })?;

        Ok(decompressed)
    }

    /// Get compression statistics for data
    pub fn stats(&self, original: &[u8], compressed: &[u8]) -> CompressionStats {
        CompressionStats {
            original_size: original.len(),
            compressed_size: compressed.len(),
            compression_ratio: if original.is_empty() {
                0.0
            } else {
                compressed.len() as f64 / original.len() as f64
            },
            savings_bytes: original.len().saturating_sub(compressed.len()),
        }
    }
}

impl Default for Compressor {
    fn default() -> Self {
        Self::new()
    }
}

/// Compression statistics
#[derive(Debug, Clone)]
pub struct CompressionStats {
    /// Original size in bytes
    pub original_size: usize,
    /// Compressed size in bytes
    pub compressed_size: usize,
    /// Compression ratio (compressed / original)
    pub compression_ratio: f64,
    /// Savings in bytes
    pub savings_bytes: usize,
}

impl CompressionStats {
    /// Calculate savings percentage
    pub fn savings_percent(&self) -> f64 {
        if self.original_size == 0 {
            0.0
        } else {
            (1.0 - self.compression_ratio) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_algorithm_id() {
        assert_eq!(CompressionAlgorithm::None.id(), 0);
        assert_eq!(CompressionAlgorithm::Gzip.id(), 1);
        assert_eq!(CompressionAlgorithm::Zlib.id(), 2);
    }

    #[test]
    fn test_compression_algorithm_from_id() {
        assert_eq!(
            CompressionAlgorithm::from_id(0),
            Some(CompressionAlgorithm::None)
        );
        assert_eq!(
            CompressionAlgorithm::from_id(1),
            Some(CompressionAlgorithm::Gzip)
        );
        assert_eq!(
            CompressionAlgorithm::from_id(2),
            Some(CompressionAlgorithm::Zlib)
        );
        assert_eq!(CompressionAlgorithm::from_id(99), None);
    }

    #[test]
    fn test_compression_algorithm_name() {
        assert_eq!(CompressionAlgorithm::None.name(), "none");
        assert_eq!(CompressionAlgorithm::Gzip.name(), "gzip");
        assert_eq!(CompressionAlgorithm::Zlib.name(), "zlib");
    }

    #[test]
    fn test_compression_algorithm_display() {
        assert_eq!(CompressionAlgorithm::Gzip.to_string(), "gzip");
        assert_eq!(CompressionAlgorithm::Zlib.to_string(), "zlib");
    }

    #[test]
    fn test_compression_config_default() {
        let config = CompressionConfig::default();
        assert_eq!(config.algorithm, CompressionAlgorithm::Gzip);
        assert_eq!(config.threshold, 1024);
        assert_eq!(config.level, 6);
    }

    #[test]
    fn test_compression_config_builder() {
        let config = CompressionConfig::new()
            .with_algorithm(CompressionAlgorithm::Zlib)
            .with_threshold(2048)
            .with_level(9);

        assert_eq!(config.algorithm, CompressionAlgorithm::Zlib);
        assert_eq!(config.threshold, 2048);
        assert_eq!(config.level, 9);
    }

    #[test]
    fn test_compression_config_disabled() {
        let config = CompressionConfig::disabled();
        assert_eq!(config.algorithm, CompressionAlgorithm::None);
        assert_eq!(config.threshold, usize::MAX);
        assert_eq!(config.level, 0);
    }

    #[test]
    fn test_compress_below_threshold() {
        let compressor = Compressor::with_config(
            CompressionConfig::new()
                .with_threshold(100)
                .with_algorithm(CompressionAlgorithm::Gzip),
        );

        let data = b"small data";
        let (compressed, was_compressed) = compressor.compress(data).unwrap();

        assert!(!was_compressed);
        assert_eq!(compressed, data);
    }

    #[test]
    fn test_compress_gzip() {
        let compressor = Compressor::with_config(
            CompressionConfig::new()
                .with_threshold(10)
                .with_algorithm(CompressionAlgorithm::Gzip),
        );

        let data = b"This is a longer string that should be compressed to save space";
        let (compressed, was_compressed) = compressor.compress(data).unwrap();

        assert!(was_compressed);
        assert_eq!(compressed[0], CompressionAlgorithm::Gzip.id());

        // Decompress and verify
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_zlib() {
        let compressor = Compressor::with_config(
            CompressionConfig::new()
                .with_threshold(10)
                .with_algorithm(CompressionAlgorithm::Zlib),
        );

        let data = b"This is a longer string that should be compressed to save space";
        let (compressed, was_compressed) = compressor.compress(data).unwrap();

        assert!(was_compressed);
        assert_eq!(compressed[0], CompressionAlgorithm::Zlib.id());

        // Decompress and verify
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[test]
    fn test_compress_disabled() {
        let compressor = Compressor::with_config(CompressionConfig::disabled());

        let data = b"This data should not be compressed";
        let (compressed, was_compressed) = compressor.compress(data).unwrap();

        assert!(!was_compressed);
        assert_eq!(compressed, data);
    }

    #[test]
    fn test_compression_stats() {
        let compressor = Compressor::new();
        let original = vec![0u8; 1000];
        let compressed = vec![0u8; 100];

        let stats = compressor.stats(&original, &compressed);

        assert_eq!(stats.original_size, 1000);
        assert_eq!(stats.compressed_size, 100);
        assert_eq!(stats.compression_ratio, 0.1);
        assert_eq!(stats.savings_bytes, 900);
        assert_eq!(stats.savings_percent(), 90.0);
    }

    #[test]
    fn test_large_payload_compression() {
        let compressor = Compressor::with_config(
            CompressionConfig::new()
                .with_threshold(100)
                .with_algorithm(CompressionAlgorithm::Gzip),
        );

        // Create large repetitive payload
        let data = "Hello World! ".repeat(1000);
        let data_bytes = data.as_bytes();

        let (compressed, was_compressed) = compressor.compress(data_bytes).unwrap();

        assert!(was_compressed);
        assert!(compressed.len() < data_bytes.len());

        // Verify decompression
        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data_bytes);

        // Check stats
        let stats = compressor.stats(data_bytes, &compressed);
        assert!(stats.compression_ratio < 0.5); // Should compress well
        assert!(stats.savings_percent() > 50.0);
    }
}
