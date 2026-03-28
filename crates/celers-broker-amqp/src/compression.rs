//! Message compression support for AMQP
//!
//! This module provides compression utilities to reduce network overhead
//! and storage requirements for large messages.
//!
//! This module uses [`celers_protocol::compression::CompressionType`] as
//! the canonical compression enum. The legacy `CompressionCodec` type
//! alias is retained for backward compatibility but is deprecated.
//!
//! # Supported Compression Algorithms
//!
//! - **Gzip**: Standard compression, good balance of speed and ratio
//! - **Zstd**: Modern compression, faster with better ratios
//! - **Zlib**: Deflate-based compression
//! - **None**: No compression
//!
//! # Examples
//!
//! ```
//! use celers_broker_amqp::compression::{CompressionCodec, compress_message, decompress_message};
//!
//! # fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let data = b"Hello, World! This is a test message.";
//!
//! // Compress with gzip
//! let compressed = compress_message(data, CompressionCodec::Gzip)?;
//! println!("Original: {} bytes, Compressed: {} bytes",  data.len(), compressed.len());
//!
//! // Decompress
//! let decompressed = decompress_message(&compressed, CompressionCodec::Gzip)?;
//! assert_eq!(data, decompressed.as_slice());
//! # Ok(())
//! # }
//! ```

use celers_protocol::compression::CompressionType as ProtocolCompressionType;
use oxiarc_deflate::{gzip_compress, gzip_decompress};
use serde::{Deserialize, Serialize};

/// Deprecated -- use [`celers_protocol::compression::CompressionType`] directly.
///
/// This type alias maps the legacy `CompressionCodec` name to the
/// canonical `CompressionType` from `celers-protocol`.
#[deprecated(
    since = "0.2.0",
    note = "Use celers_protocol::compression::CompressionType instead"
)]
pub type CompressionCodec = ProtocolCompressionType;

/// Compression error
#[derive(Debug, thiserror::Error)]
pub enum CompressionError {
    /// IO error during compression/decompression
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Unsupported codec
    #[error("Unsupported compression codec: {0}")]
    UnsupportedCodec(String),

    /// Compression failed
    #[error("Compression failed: {0}")]
    CompressionFailed(String),

    /// Decompression failed
    #[error("Decompression failed: {0}")]
    DecompressionFailed(String),
}

/// Compress message data using the specified codec
///
/// # Arguments
///
/// * `data` - Data to compress
/// * `codec` - Compression codec to use
///
/// # Returns
///
/// Compressed data
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::compression::{compress_message, CompressionCodec};
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let data = b"Test message";
/// #[allow(deprecated)]
/// let compressed = compress_message(data, CompressionCodec::Gzip)?;
/// assert!(compressed.len() > 0);
/// # Ok(())
/// # }
/// ```
pub fn compress_message(
    data: &[u8],
    codec: ProtocolCompressionType,
) -> Result<Vec<u8>, CompressionError> {
    match codec {
        ProtocolCompressionType::None => Ok(data.to_vec()),
        ProtocolCompressionType::Gzip => compress_gzip(data),
        ProtocolCompressionType::Zlib => compress_zlib(data),
        ProtocolCompressionType::Zstd => compress_zstd(data),
    }
}

/// Decompress message data using the specified codec
///
/// # Arguments
///
/// * `data` - Compressed data
/// * `codec` - Compression codec used
///
/// # Returns
///
/// Decompressed data
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::compression::{compress_message, decompress_message, CompressionCodec};
///
/// # fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let original = b"Test message";
/// #[allow(deprecated)]
/// let compressed = compress_message(original, CompressionCodec::Gzip)?;
/// #[allow(deprecated)]
/// let decompressed = decompress_message(&compressed, CompressionCodec::Gzip)?;
/// assert_eq!(original, decompressed.as_slice());
/// # Ok(())
/// # }
/// ```
pub fn decompress_message(
    data: &[u8],
    codec: ProtocolCompressionType,
) -> Result<Vec<u8>, CompressionError> {
    match codec {
        ProtocolCompressionType::None => Ok(data.to_vec()),
        ProtocolCompressionType::Gzip => decompress_gzip(data),
        ProtocolCompressionType::Zlib => decompress_zlib(data),
        ProtocolCompressionType::Zstd => decompress_zstd(data),
    }
}

/// Compress data using gzip
fn compress_gzip(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    gzip_compress(data, 6).map_err(|e| CompressionError::CompressionFailed(e.to_string()))
}

/// Decompress data using gzip
fn decompress_gzip(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    gzip_decompress(data).map_err(|e| CompressionError::DecompressionFailed(e.to_string()))
}

/// Compress data using zlib
fn compress_zlib(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    oxiarc_deflate::zlib_compress(data, 6)
        .map_err(|e| CompressionError::CompressionFailed(e.to_string()))
}

/// Decompress data using zlib
fn decompress_zlib(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    oxiarc_deflate::zlib_decompress(data)
        .map_err(|e| CompressionError::DecompressionFailed(e.to_string()))
}

/// Compress data using zstd
fn compress_zstd(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    oxiarc_zstd::encode_all(data, 3).map_err(|e| CompressionError::CompressionFailed(e.to_string()))
}

/// Decompress data using zstd
fn decompress_zstd(data: &[u8]) -> Result<Vec<u8>, CompressionError> {
    oxiarc_zstd::decode_all(data).map_err(|e| CompressionError::DecompressionFailed(e.to_string()))
}

/// Calculate compression ratio
///
/// # Arguments
///
/// * `original_size` - Original data size in bytes
/// * `compressed_size` - Compressed data size in bytes
///
/// # Returns
///
/// Compression ratio (higher is better, e.g., 2.0 means 50% size reduction)
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::compression::calculate_compression_ratio;
///
/// let ratio = calculate_compression_ratio(1000, 500);
/// assert_eq!(ratio, 2.0);
/// ```
pub fn calculate_compression_ratio(original_size: usize, compressed_size: usize) -> f64 {
    if compressed_size == 0 {
        return 0.0;
    }
    original_size as f64 / compressed_size as f64
}

/// Calculate compression savings percentage
///
/// # Arguments
///
/// * `original_size` - Original data size in bytes
/// * `compressed_size` - Compressed data size in bytes
///
/// # Returns
///
/// Compression savings as percentage (0-100)
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::compression::calculate_compression_savings;
///
/// let savings = calculate_compression_savings(1000, 500);
/// assert_eq!(savings, 50.0);
/// ```
pub fn calculate_compression_savings(original_size: usize, compressed_size: usize) -> f64 {
    if original_size == 0 {
        return 0.0;
    }
    ((original_size - compressed_size) as f64 / original_size as f64) * 100.0
}

/// Determine if message should be compressed based on size
///
/// # Arguments
///
/// * `message_size` - Message size in bytes
/// * `threshold_bytes` - Minimum size threshold for compression
///
/// # Returns
///
/// True if message should be compressed
///
/// # Examples
///
/// ```
/// use celers_broker_amqp::compression::should_compress_message;
///
/// assert!(!should_compress_message(100, 1024)); // Too small
/// assert!(should_compress_message(2048, 1024)); // Large enough
/// ```
pub fn should_compress_message(message_size: usize, threshold_bytes: usize) -> bool {
    message_size >= threshold_bytes
}

/// Compression statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompressionStats {
    /// Total messages compressed
    pub messages_compressed: usize,
    /// Total original bytes
    pub original_bytes: usize,
    /// Total compressed bytes
    pub compressed_bytes: usize,
    /// Average compression ratio
    pub avg_compression_ratio: f64,
    /// Total bytes saved
    pub bytes_saved: usize,
}

impl CompressionStats {
    /// Create new compression statistics
    pub fn new() -> Self {
        Self {
            messages_compressed: 0,
            original_bytes: 0,
            compressed_bytes: 0,
            avg_compression_ratio: 0.0,
            bytes_saved: 0,
        }
    }

    /// Update statistics with new compression
    pub fn record_compression(&mut self, original_size: usize, compressed_size: usize) {
        self.messages_compressed += 1;
        self.original_bytes += original_size;
        self.compressed_bytes += compressed_size;
        self.bytes_saved = self.original_bytes.saturating_sub(self.compressed_bytes);
        self.avg_compression_ratio =
            calculate_compression_ratio(self.original_bytes, self.compressed_bytes);
    }

    /// Get compression savings percentage
    pub fn savings_percentage(&self) -> f64 {
        calculate_compression_savings(self.original_bytes, self.compressed_bytes)
    }
}

impl Default for CompressionStats {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gzip_compression() {
        // Use larger, repetitive data that compresses well
        let data = b"Hello, World! This is a test message that should compress well. ".repeat(100);
        let compressed =
            compress_message(&data, ProtocolCompressionType::Gzip).expect("gzip compress failed");
        let decompressed = decompress_message(&compressed, ProtocolCompressionType::Gzip)
            .expect("gzip decompress failed");

        assert_eq!(data, decompressed);
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_zstd_compression() {
        // Use larger, repetitive data that compresses well
        let data = b"Hello, World! This is a test message that should compress well. ".repeat(100);
        let compressed =
            compress_message(&data, ProtocolCompressionType::Zstd).expect("zstd compress failed");
        let decompressed = decompress_message(&compressed, ProtocolCompressionType::Zstd)
            .expect("zstd decompress failed");

        assert_eq!(data, decompressed);
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_zlib_compression() {
        let data = b"Hello, World! This is a test message that should compress well. ".repeat(100);
        let compressed =
            compress_message(&data, ProtocolCompressionType::Zlib).expect("zlib compress failed");
        let decompressed = decompress_message(&compressed, ProtocolCompressionType::Zlib)
            .expect("zlib decompress failed");

        assert_eq!(data, decompressed);
        assert!(compressed.len() < data.len());
    }

    #[test]
    fn test_no_compression() {
        let data = b"Test data";
        let compressed =
            compress_message(data, ProtocolCompressionType::None).expect("none compress failed");
        let decompressed = decompress_message(&compressed, ProtocolCompressionType::None)
            .expect("none decompress failed");

        assert_eq!(data, compressed.as_slice());
        assert_eq!(data, decompressed.as_slice());
    }

    #[test]
    fn test_compression_ratio() {
        let ratio = calculate_compression_ratio(1000, 500);
        assert_eq!(ratio, 2.0);

        let ratio = calculate_compression_ratio(1000, 250);
        assert_eq!(ratio, 4.0);
    }

    #[test]
    fn test_compression_savings() {
        let savings = calculate_compression_savings(1000, 500);
        assert_eq!(savings, 50.0);

        let savings = calculate_compression_savings(1000, 750);
        assert_eq!(savings, 25.0);
    }

    #[test]
    fn test_should_compress_message() {
        assert!(!should_compress_message(512, 1024));
        assert!(should_compress_message(1024, 1024));
        assert!(should_compress_message(2048, 1024));
    }

    #[test]
    fn test_compression_stats() {
        let mut stats = CompressionStats::new();

        stats.record_compression(1000, 500);
        assert_eq!(stats.messages_compressed, 1);
        assert_eq!(stats.original_bytes, 1000);
        assert_eq!(stats.compressed_bytes, 500);
        assert_eq!(stats.bytes_saved, 500);
        assert_eq!(stats.avg_compression_ratio, 2.0);
        assert_eq!(stats.savings_percentage(), 50.0);

        stats.record_compression(2000, 1000);
        assert_eq!(stats.messages_compressed, 2);
        assert_eq!(stats.original_bytes, 3000);
        assert_eq!(stats.compressed_bytes, 1500);
        assert_eq!(stats.bytes_saved, 1500);
    }

    #[test]
    fn test_codec_from_name() {
        assert_eq!(
            ProtocolCompressionType::from_name("none"),
            Some(ProtocolCompressionType::None)
        );
        assert_eq!(
            ProtocolCompressionType::from_name("gzip"),
            Some(ProtocolCompressionType::Gzip)
        );
        assert_eq!(
            ProtocolCompressionType::from_name("zstd"),
            Some(ProtocolCompressionType::Zstd)
        );
        assert_eq!(ProtocolCompressionType::from_name("invalid"), None);
    }

    #[test]
    fn test_codec_display() {
        assert_eq!(ProtocolCompressionType::None.to_string(), "utf-8");
        assert_eq!(ProtocolCompressionType::Gzip.to_string(), "gzip");
        assert_eq!(ProtocolCompressionType::Zstd.to_string(), "zstd");
    }

    #[test]
    fn test_codec_is_enabled() {
        assert!(!ProtocolCompressionType::None.is_enabled());
        assert!(ProtocolCompressionType::Gzip.is_enabled());
        assert!(ProtocolCompressionType::Zstd.is_enabled());
    }
}
