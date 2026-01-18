//! Zero-copy task passing for efficient data transfer
//!
//! This module provides zero-copy mechanisms for passing task data between
//! different components without unnecessary memory copies. Benefits include:
//! - Reduced memory allocations
//! - Lower CPU overhead
//! - Better cache utilization
//! - Improved throughput for large payloads
//!
//! # Example
//!
//! ```
//! use celers_worker::zero_copy::{ZeroCopyBuffer, SharedBuffer};
//!
//! // Create a zero-copy buffer
//! let data = vec![1, 2, 3, 4, 5];
//! let buffer = ZeroCopyBuffer::from(data);
//!
//! // Share without copying
//! let shared1 = buffer.share();
//! let shared2 = buffer.share();
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

/// Zero-copy buffer that uses reference counting for sharing
#[derive(Clone)]
pub struct ZeroCopyBuffer {
    data: Arc<Vec<u8>>,
    offset: usize,
    len: usize,
}

impl ZeroCopyBuffer {
    /// Create a new zero-copy buffer from a Vec
    pub fn new(data: Vec<u8>) -> Self {
        let len = data.len();
        Self {
            data: Arc::new(data),
            offset: 0,
            len,
        }
    }

    /// Create a zero-copy buffer from existing Arc
    pub fn from_arc(data: Arc<Vec<u8>>) -> Self {
        let len = data.len();
        Self {
            data,
            offset: 0,
            len,
        }
    }

    /// Create an empty buffer
    pub fn empty() -> Self {
        Self {
            data: Arc::new(Vec::new()),
            offset: 0,
            len: 0,
        }
    }

    /// Get the length of the buffer
    pub fn len(&self) -> usize {
        self.len
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Get a slice of the buffer data
    pub fn as_slice(&self) -> &[u8] {
        &self.data[self.offset..self.offset + self.len]
    }

    /// Share this buffer (creates a new reference without copying)
    pub fn share(&self) -> Self {
        Self {
            data: Arc::clone(&self.data),
            offset: self.offset,
            len: self.len,
        }
    }

    /// Create a view into a subset of this buffer (zero-copy slice)
    pub fn slice(&self, start: usize, end: usize) -> Result<Self, String> {
        if start > end {
            return Err("Start must be <= end".to_string());
        }
        if end > self.len {
            return Err(format!("End {} exceeds buffer length {}", end, self.len));
        }

        Ok(Self {
            data: Arc::clone(&self.data),
            offset: self.offset + start,
            len: end - start,
        })
    }

    /// Get the number of references to the underlying data
    pub fn ref_count(&self) -> usize {
        Arc::strong_count(&self.data)
    }

    /// Check if this is the only reference to the data
    pub fn is_unique(&self) -> bool {
        Arc::strong_count(&self.data) == 1
    }

    /// Try to get mutable access to the data (only works if this is the only reference)
    pub fn try_mut(&mut self) -> Option<&mut Vec<u8>> {
        Arc::get_mut(&mut self.data)
    }

    /// Convert to owned Vec (clones if there are other references)
    pub fn into_vec(self) -> Vec<u8> {
        match Arc::try_unwrap(self.data) {
            Ok(mut vec) => {
                if self.offset == 0 && self.len == vec.len() {
                    vec
                } else {
                    vec.drain(self.offset..self.offset + self.len).collect()
                }
            }
            Err(arc) => arc[self.offset..self.offset + self.len].to_vec(),
        }
    }

    /// Get memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        self.data.capacity()
    }
}

impl From<Vec<u8>> for ZeroCopyBuffer {
    fn from(data: Vec<u8>) -> Self {
        Self::new(data)
    }
}

impl From<&[u8]> for ZeroCopyBuffer {
    fn from(data: &[u8]) -> Self {
        Self::new(data.to_vec())
    }
}

impl AsRef<[u8]> for ZeroCopyBuffer {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl Deref for ZeroCopyBuffer {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl fmt::Debug for ZeroCopyBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZeroCopyBuffer")
            .field("len", &self.len)
            .field("offset", &self.offset)
            .field("ref_count", &self.ref_count())
            .finish()
    }
}

impl fmt::Display for ZeroCopyBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "ZeroCopyBuffer(len={}, refs={})",
            self.len,
            self.ref_count()
        )
    }
}

/// Shared buffer for zero-copy task data
#[derive(Clone)]
pub struct SharedBuffer {
    buffer: ZeroCopyBuffer,
    metadata: Arc<BufferMetadata>,
}

/// Buffer metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BufferMetadata {
    /// Content type (e.g., "application/json", "application/octet-stream")
    pub content_type: String,
    /// Encoding (e.g., "utf-8", "binary")
    pub encoding: String,
    /// Compression (e.g., "none", "gzip", "zstd")
    pub compression: String,
    /// Custom metadata
    pub custom: std::collections::HashMap<String, String>,
}

impl Default for BufferMetadata {
    fn default() -> Self {
        Self {
            content_type: "application/octet-stream".to_string(),
            encoding: "binary".to_string(),
            compression: "none".to_string(),
            custom: std::collections::HashMap::new(),
        }
    }
}

impl BufferMetadata {
    /// Create new buffer metadata
    pub fn new() -> Self {
        Self::default()
    }

    /// Set content type
    pub fn with_content_type(mut self, content_type: String) -> Self {
        self.content_type = content_type;
        self
    }

    /// Set encoding
    pub fn with_encoding(mut self, encoding: String) -> Self {
        self.encoding = encoding;
        self
    }

    /// Set compression
    pub fn with_compression(mut self, compression: String) -> Self {
        self.compression = compression;
        self
    }

    /// Add custom metadata
    pub fn with_custom(mut self, key: String, value: String) -> Self {
        self.custom.insert(key, value);
        self
    }

    /// Check if content is JSON
    pub fn is_json(&self) -> bool {
        self.content_type.contains("json")
    }

    /// Check if content is compressed
    pub fn is_compressed(&self) -> bool {
        self.compression != "none"
    }
}

impl SharedBuffer {
    /// Create a new shared buffer
    pub fn new(buffer: ZeroCopyBuffer) -> Self {
        Self {
            buffer,
            metadata: Arc::new(BufferMetadata::default()),
        }
    }

    /// Create with metadata
    pub fn with_metadata(buffer: ZeroCopyBuffer, metadata: BufferMetadata) -> Self {
        Self {
            buffer,
            metadata: Arc::new(metadata),
        }
    }

    /// Get the buffer
    pub fn buffer(&self) -> &ZeroCopyBuffer {
        &self.buffer
    }

    /// Get the metadata
    pub fn metadata(&self) -> &BufferMetadata {
        &self.metadata
    }

    /// Get buffer length
    pub fn len(&self) -> usize {
        self.buffer.len()
    }

    /// Check if buffer is empty
    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    /// Get buffer data as slice
    pub fn as_slice(&self) -> &[u8] {
        self.buffer.as_slice()
    }

    /// Share this buffer
    pub fn share(&self) -> Self {
        Self {
            buffer: self.buffer.share(),
            metadata: Arc::clone(&self.metadata),
        }
    }

    /// Slice the buffer
    pub fn slice(&self, start: usize, end: usize) -> Result<Self, String> {
        Ok(Self {
            buffer: self.buffer.slice(start, end)?,
            metadata: Arc::clone(&self.metadata),
        })
    }

    /// Get reference count
    pub fn ref_count(&self) -> usize {
        self.buffer.ref_count()
    }

    /// Convert to owned vector
    pub fn into_vec(self) -> Vec<u8> {
        self.buffer.into_vec()
    }
}

impl From<Vec<u8>> for SharedBuffer {
    fn from(data: Vec<u8>) -> Self {
        Self::new(ZeroCopyBuffer::from(data))
    }
}

impl From<ZeroCopyBuffer> for SharedBuffer {
    fn from(buffer: ZeroCopyBuffer) -> Self {
        Self::new(buffer)
    }
}

impl AsRef<[u8]> for SharedBuffer {
    fn as_ref(&self) -> &[u8] {
        self.as_slice()
    }
}

impl fmt::Debug for SharedBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SharedBuffer")
            .field("len", &self.len())
            .field("ref_count", &self.ref_count())
            .field("metadata", &self.metadata)
            .finish()
    }
}

impl fmt::Display for SharedBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "SharedBuffer(len={}, refs={}, type={})",
            self.len(),
            self.ref_count(),
            self.metadata.content_type
        )
    }
}

/// Zero-copy buffer pool for reusing buffers
pub struct ZeroCopyPool {
    buffers: Arc<tokio::sync::RwLock<Vec<ZeroCopyBuffer>>>,
    max_size: usize,
    buffer_size: usize,
}

impl ZeroCopyPool {
    /// Create a new zero-copy pool
    pub fn new(buffer_size: usize, max_size: usize) -> Self {
        Self {
            buffers: Arc::new(tokio::sync::RwLock::new(Vec::new())),
            max_size,
            buffer_size,
        }
    }

    /// Acquire a buffer from the pool
    pub async fn acquire(&self) -> ZeroCopyBuffer {
        let mut buffers = self.buffers.write().await;

        if let Some(buffer) = buffers.pop() {
            buffer
        } else {
            // Allocate new buffer
            ZeroCopyBuffer::new(vec![0; self.buffer_size])
        }
    }

    /// Return a buffer to the pool
    pub async fn release(&self, buffer: ZeroCopyBuffer) {
        // Only accept buffers of the correct size and with no other references
        if buffer.len() == self.buffer_size && buffer.is_unique() {
            let mut buffers = self.buffers.write().await;
            if buffers.len() < self.max_size {
                buffers.push(buffer);
            }
        }
    }

    /// Get current pool size
    pub async fn size(&self) -> usize {
        self.buffers.read().await.len()
    }

    /// Clear the pool
    pub async fn clear(&self) {
        self.buffers.write().await.clear();
    }
}

impl Clone for ZeroCopyPool {
    fn clone(&self) -> Self {
        Self {
            buffers: Arc::clone(&self.buffers),
            max_size: self.max_size,
            buffer_size: self.buffer_size,
        }
    }
}

impl fmt::Debug for ZeroCopyPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ZeroCopyPool")
            .field("buffer_size", &self.buffer_size)
            .field("max_size", &self.max_size)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_zero_copy_buffer_creation() {
        let data = vec![1, 2, 3, 4, 5];
        let buffer = ZeroCopyBuffer::new(data);
        assert_eq!(buffer.len(), 5);
        assert!(!buffer.is_empty());
        assert_eq!(buffer.ref_count(), 1);
    }

    #[test]
    fn test_zero_copy_buffer_share() {
        let data = vec![1, 2, 3, 4, 5];
        let buffer = ZeroCopyBuffer::new(data);

        let shared1 = buffer.share();
        let shared2 = buffer.share();

        assert_eq!(buffer.ref_count(), 3);
        assert_eq!(shared1.ref_count(), 3);
        assert_eq!(shared2.ref_count(), 3);

        assert_eq!(buffer.as_slice(), shared1.as_slice());
        assert_eq!(buffer.as_slice(), shared2.as_slice());
    }

    #[test]
    fn test_zero_copy_buffer_slice() {
        let data = vec![1, 2, 3, 4, 5];
        let buffer = ZeroCopyBuffer::new(data);

        let slice = buffer.slice(1, 4).unwrap();
        assert_eq!(slice.len(), 3);
        assert_eq!(slice.as_slice(), &[2, 3, 4]);
        assert_eq!(slice.ref_count(), 2);
    }

    #[test]
    fn test_zero_copy_buffer_slice_errors() {
        let data = vec![1, 2, 3, 4, 5];
        let buffer = ZeroCopyBuffer::new(data);

        assert!(buffer.slice(4, 2).is_err());
        assert!(buffer.slice(0, 10).is_err());
    }

    #[test]
    fn test_zero_copy_buffer_into_vec() {
        let data = vec![1, 2, 3, 4, 5];
        let buffer = ZeroCopyBuffer::new(data.clone());

        let vec = buffer.into_vec();
        assert_eq!(vec, data);
    }

    #[test]
    fn test_zero_copy_buffer_from_vec() {
        let data = vec![1, 2, 3, 4, 5];
        let buffer: ZeroCopyBuffer = data.clone().into();
        assert_eq!(buffer.as_slice(), &data[..]);
    }

    #[test]
    fn test_zero_copy_buffer_empty() {
        let buffer = ZeroCopyBuffer::empty();
        assert_eq!(buffer.len(), 0);
        assert!(buffer.is_empty());
    }

    #[test]
    fn test_buffer_metadata_default() {
        let metadata = BufferMetadata::default();
        assert_eq!(metadata.content_type, "application/octet-stream");
        assert_eq!(metadata.encoding, "binary");
        assert_eq!(metadata.compression, "none");
        assert!(!metadata.is_compressed());
    }

    #[test]
    fn test_buffer_metadata_builder() {
        let metadata = BufferMetadata::new()
            .with_content_type("application/json".to_string())
            .with_encoding("utf-8".to_string())
            .with_compression("gzip".to_string())
            .with_custom("key".to_string(), "value".to_string());

        assert_eq!(metadata.content_type, "application/json");
        assert_eq!(metadata.encoding, "utf-8");
        assert_eq!(metadata.compression, "gzip");
        assert!(metadata.is_json());
        assert!(metadata.is_compressed());
        assert_eq!(metadata.custom.get("key"), Some(&"value".to_string()));
    }

    #[test]
    fn test_shared_buffer_creation() {
        let data = vec![1, 2, 3, 4, 5];
        let buffer = SharedBuffer::from(data);
        assert_eq!(buffer.len(), 5);
        assert_eq!(buffer.ref_count(), 1);
    }

    #[test]
    fn test_shared_buffer_share() {
        let data = vec![1, 2, 3, 4, 5];
        let buffer = SharedBuffer::from(data);

        let shared = buffer.share();
        assert_eq!(buffer.ref_count(), 2);
        assert_eq!(shared.ref_count(), 2);
    }

    #[test]
    fn test_shared_buffer_with_metadata() {
        let data = vec![1, 2, 3, 4, 5];
        let metadata = BufferMetadata::new().with_content_type("application/json".to_string());

        let buffer = SharedBuffer::with_metadata(ZeroCopyBuffer::from(data), metadata);
        assert!(buffer.metadata().is_json());
    }

    #[test]
    fn test_shared_buffer_slice() {
        let data = vec![1, 2, 3, 4, 5];
        let buffer = SharedBuffer::from(data);

        let slice = buffer.slice(1, 4).unwrap();
        assert_eq!(slice.len(), 3);
        assert_eq!(slice.as_slice(), &[2, 3, 4]);
    }

    #[tokio::test]
    async fn test_zero_copy_pool_acquire() {
        let pool = ZeroCopyPool::new(1024, 10);

        let buffer = pool.acquire().await;
        assert_eq!(buffer.len(), 1024);
    }

    #[tokio::test]
    async fn test_zero_copy_pool_release() {
        let pool = ZeroCopyPool::new(1024, 10);

        let buffer = pool.acquire().await;
        pool.release(buffer).await;

        assert_eq!(pool.size().await, 1);
    }

    #[tokio::test]
    async fn test_zero_copy_pool_reuse() {
        let pool = ZeroCopyPool::new(1024, 10);

        let buffer1 = pool.acquire().await;
        let ptr1 = buffer1.as_slice().as_ptr();
        pool.release(buffer1).await;

        let buffer2 = pool.acquire().await;
        let ptr2 = buffer2.as_slice().as_ptr();

        // Should reuse the same buffer
        assert_eq!(ptr1, ptr2);
    }

    #[tokio::test]
    async fn test_zero_copy_pool_clear() {
        let pool = ZeroCopyPool::new(1024, 10);

        let buffer = pool.acquire().await;
        pool.release(buffer).await;
        assert_eq!(pool.size().await, 1);

        pool.clear().await;
        assert_eq!(pool.size().await, 0);
    }
}
