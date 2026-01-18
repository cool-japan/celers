//! Memory pooling for task data
//!
//! This module provides memory pooling to reduce allocation overhead and improve
//! performance for task data. It supports:
//! - Fixed-size buffer pools
//! - Dynamic buffer allocation with size classes
//! - Pool statistics and monitoring
//! - Automatic pool cleanup
//!
//! # Example
//!
//! ```
//! use celers_worker::memory_pool::{MemoryPool, PoolConfig};
//!
//! let config = PoolConfig::default()
//!     .with_buffer_size(4096)
//!     .with_max_buffers(100);
//!
//! let pool = MemoryPool::new(config);
//!
//! // Allocate a buffer from the pool
//! let buffer = pool.allocate();
//!
//! // Use the buffer...
//!
//! // Buffer is automatically returned to pool when dropped
//! ```

use serde::{Deserialize, Serialize};
use std::fmt;
use std::sync::Arc;
use tokio::sync::RwLock;

/// Size class for buffer allocation
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SizeClass {
    /// Tiny buffers (< 1KB)
    Tiny,
    /// Small buffers (1KB - 16KB)
    Small,
    /// Medium buffers (16KB - 256KB)
    Medium,
    /// Large buffers (256KB - 1MB)
    Large,
    /// Huge buffers (> 1MB)
    Huge,
}

impl SizeClass {
    /// Get the buffer size for this size class
    pub fn buffer_size(&self) -> usize {
        match self {
            Self::Tiny => 512,
            Self::Small => 4096,
            Self::Medium => 65536,
            Self::Large => 524288,
            Self::Huge => 1048576,
        }
    }

    /// Get the size class for a given size
    pub fn from_size(size: usize) -> Self {
        if size <= 1024 {
            Self::Tiny
        } else if size <= 16384 {
            Self::Small
        } else if size <= 262144 {
            Self::Medium
        } else if size <= 1048576 {
            Self::Large
        } else {
            Self::Huge
        }
    }
}

impl fmt::Display for SizeClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tiny => write!(f, "Tiny"),
            Self::Small => write!(f, "Small"),
            Self::Medium => write!(f, "Medium"),
            Self::Large => write!(f, "Large"),
            Self::Huge => write!(f, "Huge"),
        }
    }
}

/// Pool configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PoolConfig {
    /// Buffer size in bytes
    buffer_size: usize,
    /// Maximum number of buffers in the pool
    max_buffers: usize,
    /// Enable pool statistics
    enable_stats: bool,
    /// Enable size classes for dynamic allocation
    enable_size_classes: bool,
    /// Prewarm the pool by allocating initial buffers
    prewarm_count: usize,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            buffer_size: 4096,
            max_buffers: 100,
            enable_stats: true,
            enable_size_classes: false,
            prewarm_count: 0,
        }
    }
}

impl PoolConfig {
    /// Create a new pool configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the buffer size
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Set the maximum number of buffers
    pub fn with_max_buffers(mut self, count: usize) -> Self {
        self.max_buffers = count;
        self
    }

    /// Enable or disable statistics
    pub fn with_stats(mut self, enable: bool) -> Self {
        self.enable_stats = enable;
        self
    }

    /// Enable or disable size classes
    pub fn with_size_classes(mut self, enable: bool) -> Self {
        self.enable_size_classes = enable;
        self
    }

    /// Set the prewarm count
    pub fn with_prewarm_count(mut self, count: usize) -> Self {
        self.prewarm_count = count;
        self
    }

    /// Get the buffer size
    pub fn buffer_size(&self) -> usize {
        self.buffer_size
    }

    /// Get the maximum number of buffers
    pub fn max_buffers(&self) -> usize {
        self.max_buffers
    }

    /// Check if statistics are enabled
    pub fn is_stats_enabled(&self) -> bool {
        self.enable_stats
    }

    /// Check if size classes are enabled
    pub fn is_size_classes_enabled(&self) -> bool {
        self.enable_size_classes
    }

    /// Get the prewarm count
    pub fn prewarm_count(&self) -> usize {
        self.prewarm_count
    }

    /// Validate configuration
    pub fn is_valid(&self) -> bool {
        self.buffer_size > 0 && self.max_buffers > 0 && self.prewarm_count <= self.max_buffers
    }

    /// Create a configuration for small buffers
    pub fn small() -> Self {
        Self {
            buffer_size: 1024,
            max_buffers: 200,
            enable_stats: true,
            enable_size_classes: false,
            prewarm_count: 20,
        }
    }

    /// Create a configuration for medium buffers
    pub fn medium() -> Self {
        Self::default()
    }

    /// Create a configuration for large buffers
    pub fn large() -> Self {
        Self {
            buffer_size: 65536,
            max_buffers: 50,
            enable_stats: true,
            enable_size_classes: false,
            prewarm_count: 5,
        }
    }

    /// Create a configuration with size classes
    pub fn with_dynamic_sizing() -> Self {
        Self {
            buffer_size: 0, // Not used with size classes
            max_buffers: 200,
            enable_stats: true,
            enable_size_classes: true,
            prewarm_count: 0,
        }
    }
}

impl fmt::Display for PoolConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PoolConfig(buf_size={}, max={}, stats={}, size_classes={})",
            self.buffer_size, self.max_buffers, self.enable_stats, self.enable_size_classes
        )
    }
}

/// Pool statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoolStats {
    /// Total allocations from the pool
    total_allocations: u64,
    /// Total deallocations to the pool
    total_deallocations: u64,
    /// Current buffers in use
    buffers_in_use: usize,
    /// Current buffers available in pool
    buffers_available: usize,
    /// Total buffers allocated (in use + available)
    total_buffers: usize,
    /// Peak buffers in use
    peak_buffers_in_use: usize,
    /// Pool hits (allocated from pool)
    pool_hits: u64,
    /// Pool misses (allocated from heap)
    pool_misses: u64,
}

impl PoolStats {
    /// Create new pool statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Get total allocations
    pub fn total_allocations(&self) -> u64 {
        self.total_allocations
    }

    /// Get total deallocations
    pub fn total_deallocations(&self) -> u64 {
        self.total_deallocations
    }

    /// Get buffers in use
    pub fn buffers_in_use(&self) -> usize {
        self.buffers_in_use
    }

    /// Get buffers available
    pub fn buffers_available(&self) -> usize {
        self.buffers_available
    }

    /// Get total buffers
    pub fn total_buffers(&self) -> usize {
        self.total_buffers
    }

    /// Get peak buffers in use
    pub fn peak_buffers_in_use(&self) -> usize {
        self.peak_buffers_in_use
    }

    /// Get pool hits
    pub fn pool_hits(&self) -> u64 {
        self.pool_hits
    }

    /// Get pool misses
    pub fn pool_misses(&self) -> u64 {
        self.pool_misses
    }

    /// Calculate hit rate (0.0 - 1.0)
    pub fn hit_rate(&self) -> f64 {
        let total = self.pool_hits + self.pool_misses;
        if total == 0 {
            return 0.0;
        }
        self.pool_hits as f64 / total as f64
    }

    /// Calculate pool utilization (0.0 - 1.0)
    pub fn utilization(&self) -> f64 {
        if self.total_buffers == 0 {
            return 0.0;
        }
        self.buffers_in_use as f64 / self.total_buffers as f64
    }

    /// Reset statistics
    pub fn reset(&mut self) {
        self.total_allocations = 0;
        self.total_deallocations = 0;
        self.pool_hits = 0;
        self.pool_misses = 0;
        // Keep current state information
    }
}

impl fmt::Display for PoolStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PoolStats(in_use={}, available={}, hit_rate={:.2}%, utilization={:.2}%)",
            self.buffers_in_use,
            self.buffers_available,
            self.hit_rate() * 100.0,
            self.utilization() * 100.0
        )
    }
}

/// Buffer handle that returns to pool on drop
pub struct PooledBuffer {
    data: Vec<u8>,
    pool: Option<Arc<RwLock<PoolInner>>>,
}

impl PooledBuffer {
    /// Create a new pooled buffer
    fn new(data: Vec<u8>, pool: Arc<RwLock<PoolInner>>) -> Self {
        Self {
            data,
            pool: Some(pool),
        }
    }

    /// Create a non-pooled buffer (allocated from heap)
    fn non_pooled(size: usize) -> Self {
        Self {
            data: vec![0; size],
            pool: None,
        }
    }

    /// Get the buffer data
    pub fn as_slice(&self) -> &[u8] {
        &self.data
    }

    /// Get mutable buffer data
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        &mut self.data
    }

    /// Get the buffer size
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if the buffer is empty
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clear the buffer
    pub fn clear(&mut self) {
        self.data.fill(0);
    }

    /// Check if this buffer is from a pool
    pub fn is_pooled(&self) -> bool {
        self.pool.is_some()
    }
}

impl Drop for PooledBuffer {
    fn drop(&mut self) {
        if let Some(pool) = self.pool.take() {
            // Return buffer to pool
            let data = std::mem::take(&mut self.data);
            if let Ok(mut pool) = pool.try_write() {
                pool.return_buffer(data);
            }
        }
    }
}

impl AsRef<[u8]> for PooledBuffer {
    fn as_ref(&self) -> &[u8] {
        &self.data
    }
}

impl AsMut<[u8]> for PooledBuffer {
    fn as_mut(&mut self) -> &mut [u8] {
        &mut self.data
    }
}

/// Internal pool state
struct PoolInner {
    config: PoolConfig,
    stats: PoolStats,
    buffers: Vec<Vec<u8>>,
}

impl PoolInner {
    fn new(config: PoolConfig) -> Self {
        let mut inner = Self {
            config,
            stats: PoolStats::default(),
            buffers: Vec::new(),
        };

        // Prewarm the pool
        if inner.config.prewarm_count > 0 {
            for _ in 0..inner.config.prewarm_count {
                inner.buffers.push(vec![0; inner.config.buffer_size]);
            }
            inner.stats.buffers_available = inner.config.prewarm_count;
            inner.stats.total_buffers = inner.config.prewarm_count;
        }

        inner
    }

    fn allocate(&mut self, size: usize) -> PooledBuffer {
        self.stats.total_allocations += 1;

        // Try to get from pool
        if let Some(mut buffer) = self.buffers.pop() {
            self.stats.pool_hits += 1;
            self.stats.buffers_available = self.buffers.len();
            self.stats.buffers_in_use += 1;

            // Resize if needed
            if buffer.len() != size {
                buffer.resize(size, 0);
            }

            if self.stats.buffers_in_use > self.stats.peak_buffers_in_use {
                self.stats.peak_buffers_in_use = self.stats.buffers_in_use;
            }

            return PooledBuffer::new(
                buffer,
                Arc::new(RwLock::new(Self::new(self.config.clone()))),
            );
        }

        // Pool is empty, allocate from heap
        self.stats.pool_misses += 1;
        self.stats.buffers_in_use += 1;
        self.stats.total_buffers += 1;

        if self.stats.buffers_in_use > self.stats.peak_buffers_in_use {
            self.stats.peak_buffers_in_use = self.stats.buffers_in_use;
        }

        PooledBuffer::non_pooled(size)
    }

    fn return_buffer(&mut self, buffer: Vec<u8>) {
        self.stats.total_deallocations += 1;

        if self.stats.buffers_in_use > 0 {
            self.stats.buffers_in_use -= 1;
        }

        // Return to pool if not full
        if self.buffers.len() < self.config.max_buffers {
            self.buffers.push(buffer);
            self.stats.buffers_available = self.buffers.len();
        } else {
            // Pool is full, drop the buffer
            if self.stats.total_buffers > 0 {
                self.stats.total_buffers -= 1;
            }
        }
    }
}

/// Memory pool for task data
pub struct MemoryPool {
    inner: Arc<RwLock<PoolInner>>,
}

impl MemoryPool {
    /// Create a new memory pool
    pub fn new(config: PoolConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(PoolInner::new(config))),
        }
    }

    /// Allocate a buffer from the pool
    pub async fn allocate(&self) -> PooledBuffer {
        let size = {
            let inner = self.inner.read().await;
            inner.config.buffer_size
        };

        let mut inner = self.inner.write().await;
        inner.allocate(size)
    }

    /// Allocate a buffer with a specific size
    pub async fn allocate_sized(&self, size: usize) -> PooledBuffer {
        let mut inner = self.inner.write().await;
        inner.allocate(size)
    }

    /// Get pool statistics
    pub async fn stats(&self) -> PoolStats {
        let inner = self.inner.read().await;
        inner.stats.clone()
    }

    /// Reset pool statistics
    pub async fn reset_stats(&self) {
        let mut inner = self.inner.write().await;
        inner.stats.reset();
    }

    /// Clear all buffers from the pool
    pub async fn clear(&self) {
        let mut inner = self.inner.write().await;
        inner.buffers.clear();
        inner.stats.buffers_available = 0;
        inner.stats.total_buffers = inner.stats.buffers_in_use;
    }

    /// Get pool configuration
    pub async fn config(&self) -> PoolConfig {
        let inner = self.inner.read().await;
        inner.config.clone()
    }
}

impl Clone for MemoryPool {
    fn clone(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }
}

impl fmt::Debug for MemoryPool {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryPool").finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_size_class_from_size() {
        assert_eq!(SizeClass::from_size(512), SizeClass::Tiny);
        assert_eq!(SizeClass::from_size(2048), SizeClass::Small);
        assert_eq!(SizeClass::from_size(32768), SizeClass::Medium);
        assert_eq!(SizeClass::from_size(524288), SizeClass::Large);
        assert_eq!(SizeClass::from_size(2097152), SizeClass::Huge);
    }

    #[test]
    fn test_size_class_buffer_size() {
        assert_eq!(SizeClass::Tiny.buffer_size(), 512);
        assert_eq!(SizeClass::Small.buffer_size(), 4096);
        assert_eq!(SizeClass::Medium.buffer_size(), 65536);
    }

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.buffer_size(), 4096);
        assert_eq!(config.max_buffers(), 100);
        assert!(config.is_valid());
    }

    #[test]
    fn test_pool_config_builder() {
        let config = PoolConfig::new()
            .with_buffer_size(8192)
            .with_max_buffers(50)
            .with_stats(false);

        assert_eq!(config.buffer_size(), 8192);
        assert_eq!(config.max_buffers(), 50);
        assert!(!config.is_stats_enabled());
    }

    #[test]
    fn test_pool_config_presets() {
        let small = PoolConfig::small();
        assert_eq!(small.buffer_size(), 1024);

        let large = PoolConfig::large();
        assert_eq!(large.buffer_size(), 65536);

        let dynamic = PoolConfig::with_dynamic_sizing();
        assert!(dynamic.is_size_classes_enabled());
    }

    #[test]
    fn test_pool_stats_default() {
        let stats = PoolStats::default();
        assert_eq!(stats.total_allocations(), 0);
        assert_eq!(stats.buffers_in_use(), 0);
        assert_eq!(stats.hit_rate(), 0.0);
        assert_eq!(stats.utilization(), 0.0);
    }

    #[test]
    fn test_pool_stats_hit_rate() {
        let stats = PoolStats {
            pool_hits: 80,
            pool_misses: 20,
            ..Default::default()
        };

        assert_eq!(stats.hit_rate(), 0.8);
    }

    #[test]
    fn test_pool_stats_utilization() {
        let stats = PoolStats {
            total_buffers: 100,
            buffers_in_use: 75,
            ..Default::default()
        };

        assert_eq!(stats.utilization(), 0.75);
    }

    #[test]
    fn test_pooled_buffer_creation() {
        let buffer = PooledBuffer::non_pooled(1024);
        assert_eq!(buffer.len(), 1024);
        assert!(!buffer.is_pooled());
    }

    #[test]
    fn test_pooled_buffer_clear() {
        let mut buffer = PooledBuffer::non_pooled(100);
        buffer.as_mut_slice().fill(42);

        buffer.clear();
        assert!(buffer.as_slice().iter().all(|&b| b == 0));
    }

    #[tokio::test]
    async fn test_memory_pool_creation() {
        let config = PoolConfig::default();
        let pool = MemoryPool::new(config);

        let config = pool.config().await;
        assert_eq!(config.buffer_size(), 4096);
    }

    #[tokio::test]
    async fn test_memory_pool_allocation() {
        let config = PoolConfig::default().with_buffer_size(1024);
        let pool = MemoryPool::new(config);

        let buffer = pool.allocate().await;
        assert_eq!(buffer.len(), 1024);
    }

    #[tokio::test]
    async fn test_memory_pool_sized_allocation() {
        let config = PoolConfig::default();
        let pool = MemoryPool::new(config);

        let buffer = pool.allocate_sized(2048).await;
        assert_eq!(buffer.len(), 2048);
    }

    #[tokio::test]
    async fn test_memory_pool_stats() {
        let config = PoolConfig::default();
        let pool = MemoryPool::new(config);

        let _buffer = pool.allocate().await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_allocations(), 1);
    }

    #[tokio::test]
    async fn test_memory_pool_prewarm() {
        let config = PoolConfig::default().with_prewarm_count(10);
        let pool = MemoryPool::new(config);

        let stats = pool.stats().await;
        assert_eq!(stats.buffers_available(), 10);
    }

    #[tokio::test]
    async fn test_memory_pool_clear() {
        let config = PoolConfig::default().with_prewarm_count(5);
        let pool = MemoryPool::new(config);

        pool.clear().await;

        let stats = pool.stats().await;
        assert_eq!(stats.buffers_available(), 0);
    }

    #[tokio::test]
    async fn test_memory_pool_reset_stats() {
        let config = PoolConfig::default();
        let pool = MemoryPool::new(config);

        let _buffer = pool.allocate().await;
        pool.reset_stats().await;

        let stats = pool.stats().await;
        assert_eq!(stats.total_allocations(), 0);
    }
}
