//! Compression support for message bodies
//!
//! This module provides compression and decompression utilities for
//! Celery message bodies. Compression can significantly reduce message
//! size for large payloads.
//!
//! `CompressionType` is the **single source of truth** for compression
//! algorithms across the entire celers workspace. Broker crates
//! (`celers-broker-redis`, `celers-broker-amqp`, etc.) should reference
//! this type rather than defining their own enum variants.
//!
//! # Supported Algorithms
//!
//! - **gzip** - Standard gzip compression (requires `gzip` feature)
//! - **zlib** - Zlib compression (requires `zlib` feature)
//! - **zstd** - Zstandard compression (requires `zstd-compression` feature)
//!
//! # Example
//!
//! ```ignore
//! use celers_protocol::compression::{Compressor, CompressionType};
//!
//! let compressor = Compressor::new(CompressionType::Gzip);
//! let data = b"Hello, World!".repeat(100);
//! let compressed = compressor.compress(&data).unwrap();
//! let decompressed = compressor.decompress(&compressed).unwrap();
//! assert_eq!(data, decompressed);
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;

// ---------------------------------------------------------------------------
// CompressionType -- the canonical enum
// ---------------------------------------------------------------------------

/// Compression algorithm type.
///
/// This is the **canonical** compression enum for the entire celers
/// workspace. All broker and backend crates should use (or convert to)
/// this type instead of maintaining their own enum.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CompressionType {
    /// No compression
    #[default]
    None,
    /// Gzip compression
    #[cfg(feature = "gzip")]
    Gzip,
    /// Zlib compression
    #[cfg(feature = "zlib")]
    Zlib,
    /// Zstandard compression
    #[cfg(feature = "zstd-compression")]
    Zstd,
}

impl CompressionType {
    /// Get the content encoding string for this compression type
    #[inline]
    pub fn as_encoding(&self) -> &'static str {
        match self {
            CompressionType::None => "utf-8",
            #[cfg(feature = "gzip")]
            CompressionType::Gzip => "gzip",
            #[cfg(feature = "zlib")]
            CompressionType::Zlib => "zlib",
            #[cfg(feature = "zstd-compression")]
            CompressionType::Zstd => "zstd",
        }
    }

    /// Parse from content encoding string
    pub fn from_encoding(encoding: &str) -> Option<Self> {
        match encoding.to_lowercase().as_str() {
            "utf-8" | "identity" | "" => Some(CompressionType::None),
            #[cfg(feature = "gzip")]
            "gzip" | "x-gzip" => Some(CompressionType::Gzip),
            #[cfg(feature = "zlib")]
            "zlib" | "deflate" => Some(CompressionType::Zlib),
            #[cfg(feature = "zstd-compression")]
            "zstd" | "zstandard" => Some(CompressionType::Zstd),
            _ => None,
        }
    }

    /// List available compression types based on enabled features
    pub fn available() -> Vec<CompressionType> {
        vec![
            CompressionType::None,
            #[cfg(feature = "gzip")]
            CompressionType::Gzip,
            #[cfg(feature = "zlib")]
            CompressionType::Zlib,
            #[cfg(feature = "zstd-compression")]
            CompressionType::Zstd,
        ]
    }

    /// Get a numeric identifier byte for this compression type.
    ///
    /// Useful for binary framing protocols that prefix compressed
    /// payloads with an algorithm tag.
    pub fn id(&self) -> u8 {
        match self {
            CompressionType::None => 0,
            #[cfg(feature = "gzip")]
            CompressionType::Gzip => 1,
            #[cfg(feature = "zlib")]
            CompressionType::Zlib => 2,
            #[cfg(feature = "zstd-compression")]
            CompressionType::Zstd => 3,
        }
    }

    /// Reconstruct a `CompressionType` from an identifier byte
    /// produced by [`CompressionType::id`].
    pub fn from_id(id: u8) -> Option<Self> {
        match id {
            0 => Some(CompressionType::None),
            #[cfg(feature = "gzip")]
            1 => Some(CompressionType::Gzip),
            #[cfg(feature = "zlib")]
            2 => Some(CompressionType::Zlib),
            #[cfg(feature = "zstd-compression")]
            3 => Some(CompressionType::Zstd),
            _ => None,
        }
    }

    /// Human-readable short name (lowercase).
    pub fn name(&self) -> &'static str {
        match self {
            CompressionType::None => "none",
            #[cfg(feature = "gzip")]
            CompressionType::Gzip => "gzip",
            #[cfg(feature = "zlib")]
            CompressionType::Zlib => "zlib",
            #[cfg(feature = "zstd-compression")]
            CompressionType::Zstd => "zstd",
        }
    }

    /// Returns `true` when this variant represents an actual
    /// compression algorithm (i.e. anything other than `None`).
    pub fn is_enabled(&self) -> bool {
        !matches!(self, CompressionType::None)
    }

    /// Parse from a short name (case-insensitive).
    pub fn from_name(s: &str) -> Option<Self> {
        match s.to_lowercase().as_str() {
            "none" => Some(CompressionType::None),
            #[cfg(feature = "gzip")]
            "gzip" => Some(CompressionType::Gzip),
            #[cfg(feature = "zlib")]
            "zlib" | "deflate" => Some(CompressionType::Zlib),
            #[cfg(feature = "zstd-compression")]
            "zstd" | "zstandard" => Some(CompressionType::Zstd),
            _ => None,
        }
    }
}

impl fmt::Display for CompressionType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_encoding())
    }
}

impl TryFrom<&str> for CompressionType {
    type Error = String;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        Self::from_encoding(s).ok_or_else(|| format!("Unknown compression type: {}", s))
    }
}

// ---------------------------------------------------------------------------
// CompressionRegistry
// ---------------------------------------------------------------------------

/// Registry that tracks which compression algorithms are available
/// at runtime and which one is the default.
#[derive(Debug, Clone)]
pub struct CompressionRegistry {
    default: CompressionType,
    available: Vec<CompressionType>,
}

impl CompressionRegistry {
    /// Create a new registry with all feature-enabled algorithms
    /// and `None` as the default.
    pub fn new() -> Self {
        Self {
            default: CompressionType::None,
            available: CompressionType::available(),
        }
    }

    /// Create a registry with a specific default algorithm.
    ///
    /// The available list is still populated from enabled features.
    /// Returns an error string if `algo` is not in the available set.
    pub fn with_default(algo: CompressionType) -> Result<Self, String> {
        let available = CompressionType::available();
        if !available.contains(&algo) {
            return Err(format!(
                "Compression type {:?} is not available (enabled features: {:?})",
                algo, available
            ));
        }
        Ok(Self {
            default: algo,
            available,
        })
    }

    /// The default compression type.
    pub fn default_type(&self) -> CompressionType {
        self.default
    }

    /// All compression types currently available.
    pub fn available_types(&self) -> &[CompressionType] {
        &self.available
    }

    /// Check whether a given compression type is available.
    pub fn is_available(&self, algo: &CompressionType) -> bool {
        self.available.contains(algo)
    }
}

impl Default for CompressionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// CompressionStats
// ---------------------------------------------------------------------------

/// Cumulative compression statistics.
///
/// Tracks totals across multiple compress operations so callers
/// can monitor compression effectiveness over time.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompressionStats {
    /// Total original (uncompressed) bytes seen.
    pub original_bytes: u64,
    /// Total compressed bytes produced.
    pub compressed_bytes: u64,
    /// Number of successful compress/decompress operations.
    pub operations: u64,
    /// Number of failed operations.
    pub failures: u64,
}

impl CompressionStats {
    /// Overall compression ratio (`compressed / original`).
    ///
    /// Returns `0.0` when no bytes have been recorded.
    pub fn ratio(&self) -> f64 {
        if self.original_bytes == 0 {
            return 0.0;
        }
        self.compressed_bytes as f64 / self.original_bytes as f64
    }

    /// Record a successful compression operation.
    pub fn record(&mut self, original: usize, compressed: usize) {
        self.original_bytes += original as u64;
        self.compressed_bytes += compressed as u64;
        self.operations += 1;
    }

    /// Record a failed compression/decompression attempt.
    pub fn record_failure(&mut self) {
        self.failures += 1;
    }

    /// Savings percentage (`(1 - ratio) * 100`).
    pub fn savings_percent(&self) -> f64 {
        if self.original_bytes == 0 {
            return 0.0;
        }
        (1.0 - self.ratio()) * 100.0
    }
}

// ---------------------------------------------------------------------------
// CompressionError
// ---------------------------------------------------------------------------

/// Compression error
#[derive(Debug)]
pub enum CompressionError {
    /// Compression failed
    Compress(String),
    /// Decompression failed
    Decompress(String),
    /// Unsupported compression type
    UnsupportedType(String),
}

impl fmt::Display for CompressionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompressionError::Compress(msg) => write!(f, "Compression error: {}", msg),
            CompressionError::Decompress(msg) => write!(f, "Decompression error: {}", msg),
            CompressionError::UnsupportedType(t) => {
                write!(f, "Unsupported compression type: {}", t)
            }
        }
    }
}

impl std::error::Error for CompressionError {}

/// Result type for compression operations
pub type CompressionResult<T> = Result<T, CompressionError>;

// ---------------------------------------------------------------------------
// Compressor
// ---------------------------------------------------------------------------

/// Compressor with configurable algorithm and level
#[derive(Debug, Clone)]
pub struct Compressor {
    /// Compression type
    pub compression_type: CompressionType,
    /// Compression level (1-9 for gzip/zlib, 1-22 for zstd)
    pub level: u32,
}

impl Default for Compressor {
    fn default() -> Self {
        Self {
            compression_type: CompressionType::None,
            level: 6,
        }
    }
}

impl Compressor {
    /// Create a new compressor with default level
    pub fn new(compression_type: CompressionType) -> Self {
        Self {
            compression_type,
            level: 6,
        }
    }

    /// Set compression level
    #[must_use]
    pub fn with_level(mut self, level: u32) -> Self {
        self.level = level;
        self
    }

    /// Compress data
    pub fn compress(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        match self.compression_type {
            CompressionType::None => Ok(data.to_vec()),
            #[cfg(feature = "gzip")]
            CompressionType::Gzip => self.compress_gzip(data),
            #[cfg(feature = "zlib")]
            CompressionType::Zlib => self.compress_zlib(data),
            #[cfg(feature = "zstd-compression")]
            CompressionType::Zstd => self.compress_zstd(data),
        }
    }

    /// Decompress data
    pub fn decompress(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        match self.compression_type {
            CompressionType::None => Ok(data.to_vec()),
            #[cfg(feature = "gzip")]
            CompressionType::Gzip => self.decompress_gzip(data),
            #[cfg(feature = "zlib")]
            CompressionType::Zlib => self.decompress_zlib(data),
            #[cfg(feature = "zstd-compression")]
            CompressionType::Zstd => self.decompress_zstd(data),
        }
    }

    /// Get the content encoding string
    pub fn content_encoding(&self) -> &'static str {
        self.compression_type.as_encoding()
    }

    #[cfg(feature = "gzip")]
    fn compress_gzip(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        let level = self.level.min(9) as u8;
        oxiarc_deflate::gzip_compress(data, level)
            .map_err(|e| CompressionError::Compress(e.to_string()))
    }

    #[cfg(feature = "gzip")]
    fn decompress_gzip(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        oxiarc_deflate::gzip_decompress(data)
            .map_err(|e| CompressionError::Decompress(e.to_string()))
    }

    #[cfg(feature = "zlib")]
    fn compress_zlib(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        let level = self.level.min(9) as u8;
        oxiarc_deflate::zlib_compress(data, level)
            .map_err(|e| CompressionError::Compress(e.to_string()))
    }

    #[cfg(feature = "zlib")]
    fn decompress_zlib(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        oxiarc_deflate::zlib_decompress(data)
            .map_err(|e| CompressionError::Decompress(e.to_string()))
    }

    #[cfg(feature = "zstd-compression")]
    fn compress_zstd(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        let level = self.level.min(22) as i32;
        oxiarc_zstd::encode_all(data, level).map_err(|e| CompressionError::Compress(e.to_string()))
    }

    #[cfg(feature = "zstd-compression")]
    fn decompress_zstd(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        oxiarc_zstd::decode_all(data).map_err(|e| CompressionError::Decompress(e.to_string()))
    }
}

/// Auto-detect compression type from data header
pub fn detect_compression(data: &[u8]) -> CompressionType {
    if data.len() < 2 {
        return CompressionType::None;
    }

    // Gzip magic number: 1f 8b
    #[cfg(feature = "gzip")]
    if data[0] == 0x1f && data[1] == 0x8b {
        return CompressionType::Gzip;
    }

    // Zlib header: first byte is typically 0x78 (CMF byte)
    // 0x78 0x01 = no/low compression, 0x78 0x9C = default, 0x78 0xDA = best
    #[cfg(feature = "zlib")]
    if data[0] == 0x78 && (data[1] == 0x01 || data[1] == 0x5E || data[1] == 0x9C || data[1] == 0xDA)
    {
        return CompressionType::Zlib;
    }

    // Zstd magic number: 28 b5 2f fd
    #[cfg(feature = "zstd-compression")]
    if data.len() >= 4 && data[0] == 0x28 && data[1] == 0xb5 && data[2] == 0x2f && data[3] == 0xfd {
        return CompressionType::Zstd;
    }

    CompressionType::None
}

/// Decompress data with auto-detection
pub fn auto_decompress(data: &[u8]) -> CompressionResult<Vec<u8>> {
    let compression_type = detect_compression(data);
    Compressor::new(compression_type).decompress(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compression_type_as_encoding() {
        assert_eq!(CompressionType::None.as_encoding(), "utf-8");
        #[cfg(feature = "gzip")]
        assert_eq!(CompressionType::Gzip.as_encoding(), "gzip");
        #[cfg(feature = "zlib")]
        assert_eq!(CompressionType::Zlib.as_encoding(), "zlib");
        #[cfg(feature = "zstd-compression")]
        assert_eq!(CompressionType::Zstd.as_encoding(), "zstd");
    }

    #[test]
    fn test_compression_type_from_encoding() {
        assert_eq!(
            CompressionType::from_encoding("utf-8"),
            Some(CompressionType::None)
        );
        assert_eq!(
            CompressionType::from_encoding("identity"),
            Some(CompressionType::None)
        );
        #[cfg(feature = "gzip")]
        assert_eq!(
            CompressionType::from_encoding("gzip"),
            Some(CompressionType::Gzip)
        );
        #[cfg(feature = "zlib")]
        assert_eq!(
            CompressionType::from_encoding("zlib"),
            Some(CompressionType::Zlib)
        );
        #[cfg(feature = "zstd-compression")]
        assert_eq!(
            CompressionType::from_encoding("zstd"),
            Some(CompressionType::Zstd)
        );
        assert_eq!(CompressionType::from_encoding("unknown"), None);
    }

    #[test]
    fn test_compression_type_default() {
        assert_eq!(CompressionType::default(), CompressionType::None);
    }

    #[test]
    fn test_compression_type_display() {
        assert_eq!(CompressionType::None.to_string(), "utf-8");
    }

    #[test]
    fn test_compression_type_id_roundtrip() {
        assert_eq!(
            CompressionType::from_id(CompressionType::None.id()),
            Some(CompressionType::None)
        );
        #[cfg(feature = "gzip")]
        assert_eq!(
            CompressionType::from_id(CompressionType::Gzip.id()),
            Some(CompressionType::Gzip)
        );
        #[cfg(feature = "zlib")]
        assert_eq!(
            CompressionType::from_id(CompressionType::Zlib.id()),
            Some(CompressionType::Zlib)
        );
        #[cfg(feature = "zstd-compression")]
        assert_eq!(
            CompressionType::from_id(CompressionType::Zstd.id()),
            Some(CompressionType::Zstd)
        );
        assert_eq!(CompressionType::from_id(255), None);
    }

    #[test]
    fn test_compression_type_name() {
        assert_eq!(CompressionType::None.name(), "none");
        #[cfg(feature = "gzip")]
        assert_eq!(CompressionType::Gzip.name(), "gzip");
        #[cfg(feature = "zlib")]
        assert_eq!(CompressionType::Zlib.name(), "zlib");
        #[cfg(feature = "zstd-compression")]
        assert_eq!(CompressionType::Zstd.name(), "zstd");
    }

    #[test]
    fn test_compression_type_is_enabled() {
        assert!(!CompressionType::None.is_enabled());
        #[cfg(feature = "gzip")]
        assert!(CompressionType::Gzip.is_enabled());
        #[cfg(feature = "zlib")]
        assert!(CompressionType::Zlib.is_enabled());
        #[cfg(feature = "zstd-compression")]
        assert!(CompressionType::Zstd.is_enabled());
    }

    #[test]
    fn test_compression_type_from_name() {
        assert_eq!(
            CompressionType::from_name("none"),
            Some(CompressionType::None)
        );
        #[cfg(feature = "gzip")]
        assert_eq!(
            CompressionType::from_name("gzip"),
            Some(CompressionType::Gzip)
        );
        #[cfg(feature = "zlib")]
        assert_eq!(
            CompressionType::from_name("zlib"),
            Some(CompressionType::Zlib)
        );
        #[cfg(feature = "zstd-compression")]
        assert_eq!(
            CompressionType::from_name("zstd"),
            Some(CompressionType::Zstd)
        );
        assert_eq!(CompressionType::from_name("invalid"), None);
    }

    #[test]
    fn test_compressor_no_compression() {
        let compressor = Compressor::new(CompressionType::None);
        let data = b"Hello, World!";

        let compressed = compressor.compress(data).expect("compress should succeed");
        assert_eq!(compressed, data);

        let decompressed = compressor
            .decompress(&compressed)
            .expect("decompress should succeed");
        assert_eq!(decompressed, data);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_compressor_gzip() {
        let compressor = Compressor::new(CompressionType::Gzip).with_level(6);
        let data = b"Hello, World!".repeat(100);

        let compressed = compressor
            .compress(&data)
            .expect("gzip compress should succeed");
        // Compressed should be smaller for repetitive data
        assert!(compressed.len() < data.len());

        let decompressed = compressor
            .decompress(&compressed)
            .expect("gzip decompress should succeed");
        assert_eq!(decompressed, data);
    }

    #[cfg(feature = "zlib")]
    #[test]
    fn test_compressor_zlib() {
        let compressor = Compressor::new(CompressionType::Zlib).with_level(6);
        let data = b"Hello, World!".repeat(100);

        let compressed = compressor
            .compress(&data)
            .expect("zlib compress should succeed");
        assert!(compressed.len() < data.len());

        let decompressed = compressor
            .decompress(&compressed)
            .expect("zlib decompress should succeed");
        assert_eq!(decompressed, data);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_detect_gzip() {
        let compressor = Compressor::new(CompressionType::Gzip);
        let data = b"Test data";
        let compressed = compressor.compress(data).expect("compress should succeed");

        assert_eq!(detect_compression(&compressed), CompressionType::Gzip);
    }

    #[cfg(feature = "zstd-compression")]
    #[test]
    fn test_compressor_zstd() {
        let compressor = Compressor::new(CompressionType::Zstd).with_level(3);
        let data = b"Hello, World!".repeat(100);

        let compressed = compressor
            .compress(&data)
            .expect("zstd compress should succeed");
        assert!(compressed.len() < data.len());

        let decompressed = compressor
            .decompress(&compressed)
            .expect("zstd decompress should succeed");
        assert_eq!(decompressed, data);
    }

    #[cfg(feature = "zstd-compression")]
    #[test]
    fn test_detect_zstd() {
        let compressor = Compressor::new(CompressionType::Zstd);
        let data = b"Test data";
        let compressed = compressor.compress(data).expect("compress should succeed");

        assert_eq!(detect_compression(&compressed), CompressionType::Zstd);
    }

    #[test]
    fn test_detect_no_compression() {
        let data = b"Plain text data";
        assert_eq!(detect_compression(data), CompressionType::None);
    }

    #[test]
    fn test_auto_decompress_plain() {
        let data = b"Plain text";
        let result = auto_decompress(data).expect("auto_decompress should succeed");
        assert_eq!(result, data);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_auto_decompress_gzip() {
        let compressor = Compressor::new(CompressionType::Gzip);
        let original = b"Test data for auto-decompress";
        let compressed = compressor
            .compress(original)
            .expect("compress should succeed");

        let decompressed = auto_decompress(&compressed).expect("auto_decompress should succeed");
        assert_eq!(decompressed, original);
    }

    #[test]
    fn test_compression_error_display() {
        let err = CompressionError::Compress("test error".to_string());
        assert_eq!(err.to_string(), "Compression error: test error");

        let err = CompressionError::Decompress("decode failed".to_string());
        assert_eq!(err.to_string(), "Decompression error: decode failed");

        let err = CompressionError::UnsupportedType("lz4".to_string());
        assert_eq!(err.to_string(), "Unsupported compression type: lz4");
    }

    #[test]
    fn test_compression_type_available() {
        let available = CompressionType::available();
        assert!(available.contains(&CompressionType::None));
    }

    #[test]
    fn test_compression_type_try_from() {
        use std::convert::TryFrom;

        assert_eq!(
            CompressionType::try_from("utf-8").expect("should parse utf-8"),
            CompressionType::None
        );
        assert_eq!(
            CompressionType::try_from("identity").expect("should parse identity"),
            CompressionType::None
        );

        #[cfg(feature = "gzip")]
        assert_eq!(
            CompressionType::try_from("gzip").expect("should parse gzip"),
            CompressionType::Gzip
        );

        #[cfg(feature = "zstd-compression")]
        assert_eq!(
            CompressionType::try_from("zstd").expect("should parse zstd"),
            CompressionType::Zstd
        );

        // Test error case
        assert!(CompressionType::try_from("unknown").is_err());
        assert!(CompressionType::try_from("lz4").is_err());
    }

    // --- CompressionRegistry tests ---

    #[test]
    fn test_registry_new() {
        let registry = CompressionRegistry::new();
        assert_eq!(registry.default_type(), CompressionType::None);
        assert!(registry.is_available(&CompressionType::None));
        assert!(!registry.available_types().is_empty());
    }

    #[test]
    fn test_registry_with_default_none() {
        let registry = CompressionRegistry::with_default(CompressionType::None)
            .expect("None is always available");
        assert_eq!(registry.default_type(), CompressionType::None);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_registry_with_default_gzip() {
        let registry = CompressionRegistry::with_default(CompressionType::Gzip)
            .expect("gzip should be available");
        assert_eq!(registry.default_type(), CompressionType::Gzip);
        assert!(registry.is_available(&CompressionType::Gzip));
    }

    #[test]
    fn test_registry_default_impl() {
        let registry = CompressionRegistry::default();
        assert_eq!(registry.default_type(), CompressionType::None);
    }

    // --- CompressionStats tests ---

    #[test]
    fn test_stats_default() {
        let stats = CompressionStats::default();
        assert_eq!(stats.original_bytes, 0);
        assert_eq!(stats.compressed_bytes, 0);
        assert_eq!(stats.operations, 0);
        assert_eq!(stats.failures, 0);
        assert_eq!(stats.ratio(), 0.0);
        assert_eq!(stats.savings_percent(), 0.0);
    }

    #[test]
    fn test_stats_record() {
        let mut stats = CompressionStats::default();
        stats.record(1000, 500);

        assert_eq!(stats.original_bytes, 1000);
        assert_eq!(stats.compressed_bytes, 500);
        assert_eq!(stats.operations, 1);
        assert_eq!(stats.failures, 0);
        assert_eq!(stats.ratio(), 0.5);
        assert_eq!(stats.savings_percent(), 50.0);
    }

    #[test]
    fn test_stats_multiple_records() {
        let mut stats = CompressionStats::default();
        stats.record(1000, 500);
        stats.record(2000, 1000);

        assert_eq!(stats.original_bytes, 3000);
        assert_eq!(stats.compressed_bytes, 1500);
        assert_eq!(stats.operations, 2);
        assert_eq!(stats.ratio(), 0.5);
    }

    #[test]
    fn test_stats_record_failure() {
        let mut stats = CompressionStats::default();
        stats.record_failure();
        stats.record_failure();

        assert_eq!(stats.failures, 2);
        assert_eq!(stats.operations, 0);
    }

    #[test]
    fn test_stats_serde_roundtrip() {
        let mut stats = CompressionStats::default();
        stats.record(1000, 400);
        stats.record_failure();

        let json = serde_json::to_string(&stats).expect("serialize should succeed");
        let deserialized: CompressionStats =
            serde_json::from_str(&json).expect("deserialize should succeed");

        assert_eq!(deserialized.original_bytes, stats.original_bytes);
        assert_eq!(deserialized.compressed_bytes, stats.compressed_bytes);
        assert_eq!(deserialized.operations, stats.operations);
        assert_eq!(deserialized.failures, stats.failures);
    }

    #[test]
    fn test_compression_type_serde_roundtrip() {
        let ct = CompressionType::None;
        let json = serde_json::to_string(&ct).expect("serialize should succeed");
        let deserialized: CompressionType =
            serde_json::from_str(&json).expect("deserialize should succeed");
        assert_eq!(deserialized, ct);
    }
}
