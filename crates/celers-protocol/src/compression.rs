//! Compression support for message bodies
//!
//! This module provides compression and decompression utilities for
//! Celery message bodies. Compression can significantly reduce message
//! size for large payloads.
//!
//! # Supported Algorithms
//!
//! - **gzip** - Standard gzip compression (requires `gzip` feature)
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

use std::fmt;

#[cfg(feature = "gzip")]
use std::io::{Read, Write};

/// Compression algorithm type
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
pub enum CompressionType {
    /// No compression
    #[default]
    None,
    /// Gzip compression
    #[cfg(feature = "gzip")]
    Gzip,
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
            #[cfg(feature = "zstd-compression")]
            "zstd" | "zstandard" => Some(CompressionType::Zstd),
            _ => None,
        }
    }

    /// List available compression types
    pub fn available() -> Vec<CompressionType> {
        vec![
            CompressionType::None,
            #[cfg(feature = "gzip")]
            CompressionType::Gzip,
            #[cfg(feature = "zstd-compression")]
            CompressionType::Zstd,
        ]
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

/// Compressor with configurable algorithm and level
#[derive(Debug, Clone)]
pub struct Compressor {
    /// Compression type
    pub compression_type: CompressionType,
    /// Compression level (1-9 for gzip, 1-22 for zstd)
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
        use flate2::write::GzEncoder;
        use flate2::Compression;

        let level = self.level.min(9);
        let mut encoder = GzEncoder::new(Vec::new(), Compression::new(level));
        encoder
            .write_all(data)
            .map_err(|e| CompressionError::Compress(e.to_string()))?;
        encoder
            .finish()
            .map_err(|e| CompressionError::Compress(e.to_string()))
    }

    #[cfg(feature = "gzip")]
    fn decompress_gzip(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        use flate2::read::GzDecoder;

        let mut decoder = GzDecoder::new(data);
        let mut decompressed = Vec::new();
        decoder
            .read_to_end(&mut decompressed)
            .map_err(|e| CompressionError::Decompress(e.to_string()))?;
        Ok(decompressed)
    }

    #[cfg(feature = "zstd-compression")]
    fn compress_zstd(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        let level = self.level.min(22) as i32;
        zstd::encode_all(data, level).map_err(|e| CompressionError::Compress(e.to_string()))
    }

    #[cfg(feature = "zstd-compression")]
    fn decompress_zstd(&self, data: &[u8]) -> CompressionResult<Vec<u8>> {
        zstd::decode_all(data).map_err(|e| CompressionError::Decompress(e.to_string()))
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
    fn test_compressor_no_compression() {
        let compressor = Compressor::new(CompressionType::None);
        let data = b"Hello, World!";

        let compressed = compressor.compress(data).unwrap();
        assert_eq!(compressed, data);

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_compressor_gzip() {
        let compressor = Compressor::new(CompressionType::Gzip).with_level(6);
        let data = b"Hello, World!".repeat(100);

        let compressed = compressor.compress(&data).unwrap();
        // Compressed should be smaller for repetitive data
        assert!(compressed.len() < data.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_detect_gzip() {
        let compressor = Compressor::new(CompressionType::Gzip);
        let data = b"Test data";
        let compressed = compressor.compress(data).unwrap();

        assert_eq!(detect_compression(&compressed), CompressionType::Gzip);
    }

    #[cfg(feature = "zstd-compression")]
    #[test]
    fn test_compressor_zstd() {
        let compressor = Compressor::new(CompressionType::Zstd).with_level(3);
        let data = b"Hello, World!".repeat(100);

        let compressed = compressor.compress(&data).unwrap();
        assert!(compressed.len() < data.len());

        let decompressed = compressor.decompress(&compressed).unwrap();
        assert_eq!(decompressed, data);
    }

    #[cfg(feature = "zstd-compression")]
    #[test]
    fn test_detect_zstd() {
        let compressor = Compressor::new(CompressionType::Zstd);
        let data = b"Test data";
        let compressed = compressor.compress(data).unwrap();

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
        let result = auto_decompress(data).unwrap();
        assert_eq!(result, data);
    }

    #[cfg(feature = "gzip")]
    #[test]
    fn test_auto_decompress_gzip() {
        let compressor = Compressor::new(CompressionType::Gzip);
        let original = b"Test data for auto-decompress";
        let compressed = compressor.compress(original).unwrap();

        let decompressed = auto_decompress(&compressed).unwrap();
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
            CompressionType::try_from("utf-8").unwrap(),
            CompressionType::None
        );
        assert_eq!(
            CompressionType::try_from("identity").unwrap(),
            CompressionType::None
        );

        #[cfg(feature = "gzip")]
        assert_eq!(
            CompressionType::try_from("gzip").unwrap(),
            CompressionType::Gzip
        );

        #[cfg(feature = "zstd-compression")]
        assert_eq!(
            CompressionType::try_from("zstd").unwrap(),
            CompressionType::Zstd
        );

        // Test error case
        assert!(CompressionType::try_from("unknown").is_err());
        assert!(CompressionType::try_from("lz4").is_err());
    }
}
