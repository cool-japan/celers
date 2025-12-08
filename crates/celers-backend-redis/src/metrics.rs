//! Metrics and monitoring for Redis backend
//!
//! This module provides metrics collection for monitoring Redis backend operations.
//! Metrics include operation latency, data sizes, compression ratios, and connection stats.
//!
//! # Example
//!
//! ```
//! use celers_backend_redis::metrics::{BackendMetrics, OperationType};
//! use std::time::Instant;
//!
//! let metrics = BackendMetrics::new();
//!
//! // Record an operation
//! let start = Instant::now();
//! // ... perform operation ...
//! metrics.record_operation(OperationType::StoreResult, start.elapsed());
//!
//! // Record data sizes
//! metrics.record_data_size(1000, 500); // original: 1KB, stored: 500B
//!
//! // Get statistics
//! println!("Total operations: {}", metrics.total_operations());
//! println!("Average compression ratio: {:.2}", metrics.avg_compression_ratio());
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Operation types for tracking metrics
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    /// Store single result
    StoreResult,
    /// Get single result
    GetResult,
    /// Delete single result
    DeleteResult,
    /// Store batch results
    StoreBatch,
    /// Get batch results
    GetBatch,
    /// Delete batch results
    DeleteBatch,
    /// Chord operations
    ChordOperation,
    /// Set progress
    SetProgress,
    /// Get progress
    GetProgress,
}

impl OperationType {
    /// Get the operation name as a string
    pub fn as_str(&self) -> &'static str {
        match self {
            OperationType::StoreResult => "store_result",
            OperationType::GetResult => "get_result",
            OperationType::DeleteResult => "delete_result",
            OperationType::StoreBatch => "store_batch",
            OperationType::GetBatch => "get_batch",
            OperationType::DeleteBatch => "delete_batch",
            OperationType::ChordOperation => "chord_operation",
            OperationType::SetProgress => "set_progress",
            OperationType::GetProgress => "get_progress",
        }
    }
}

/// Atomic metrics storage
#[derive(Debug)]
struct AtomicMetrics {
    // Operation counts
    store_count: AtomicU64,
    get_count: AtomicU64,
    delete_count: AtomicU64,
    chord_count: AtomicU64,
    progress_count: AtomicU64,

    // Batch operation counts
    batch_store_count: AtomicU64,
    batch_get_count: AtomicU64,
    batch_delete_count: AtomicU64,

    // Latency tracking (microseconds)
    total_store_latency_us: AtomicU64,
    total_get_latency_us: AtomicU64,
    total_delete_latency_us: AtomicU64,

    // Data size tracking (bytes)
    total_original_bytes: AtomicU64,
    total_stored_bytes: AtomicU64,
    compressed_count: AtomicU64,
    uncompressed_count: AtomicU64,

    // Error tracking
    error_count: AtomicU64,
    compression_errors: AtomicU64,
    connection_errors: AtomicU64,
    serialization_errors: AtomicU64,

    // Cache metrics (for future use)
    cache_hits: AtomicU64,
    cache_misses: AtomicU64,
}

impl Default for AtomicMetrics {
    fn default() -> Self {
        Self {
            store_count: AtomicU64::new(0),
            get_count: AtomicU64::new(0),
            delete_count: AtomicU64::new(0),
            chord_count: AtomicU64::new(0),
            progress_count: AtomicU64::new(0),
            batch_store_count: AtomicU64::new(0),
            batch_get_count: AtomicU64::new(0),
            batch_delete_count: AtomicU64::new(0),
            total_store_latency_us: AtomicU64::new(0),
            total_get_latency_us: AtomicU64::new(0),
            total_delete_latency_us: AtomicU64::new(0),
            total_original_bytes: AtomicU64::new(0),
            total_stored_bytes: AtomicU64::new(0),
            compressed_count: AtomicU64::new(0),
            uncompressed_count: AtomicU64::new(0),
            error_count: AtomicU64::new(0),
            compression_errors: AtomicU64::new(0),
            connection_errors: AtomicU64::new(0),
            serialization_errors: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            cache_misses: AtomicU64::new(0),
        }
    }
}

/// Backend metrics collector
///
/// Thread-safe metrics collection for Redis backend operations.
#[derive(Debug, Clone)]
pub struct BackendMetrics {
    metrics: Arc<AtomicMetrics>,
    enabled: bool,
}

impl Default for BackendMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl BackendMetrics {
    /// Create a new metrics collector
    pub fn new() -> Self {
        Self {
            metrics: Arc::new(AtomicMetrics::default()),
            enabled: true,
        }
    }

    /// Create a disabled metrics collector (no-op)
    pub fn disabled() -> Self {
        Self {
            metrics: Arc::new(AtomicMetrics::default()),
            enabled: false,
        }
    }

    /// Check if metrics are enabled
    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    /// Record an operation with its latency
    pub fn record_operation(&self, op_type: OperationType, latency: Duration) {
        if !self.enabled {
            return;
        }

        let latency_us = latency.as_micros() as u64;

        match op_type {
            OperationType::StoreResult => {
                self.metrics.store_count.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .total_store_latency_us
                    .fetch_add(latency_us, Ordering::Relaxed);
            }
            OperationType::GetResult => {
                self.metrics.get_count.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .total_get_latency_us
                    .fetch_add(latency_us, Ordering::Relaxed);
            }
            OperationType::DeleteResult => {
                self.metrics.delete_count.fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .total_delete_latency_us
                    .fetch_add(latency_us, Ordering::Relaxed);
            }
            OperationType::StoreBatch => {
                self.metrics
                    .batch_store_count
                    .fetch_add(1, Ordering::Relaxed);
            }
            OperationType::GetBatch => {
                self.metrics.batch_get_count.fetch_add(1, Ordering::Relaxed);
            }
            OperationType::DeleteBatch => {
                self.metrics
                    .batch_delete_count
                    .fetch_add(1, Ordering::Relaxed);
            }
            OperationType::ChordOperation => {
                self.metrics.chord_count.fetch_add(1, Ordering::Relaxed);
            }
            OperationType::SetProgress | OperationType::GetProgress => {
                self.metrics.progress_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record data size (original and stored)
    pub fn record_data_size(&self, original_bytes: usize, stored_bytes: usize) {
        if !self.enabled {
            return;
        }

        self.metrics
            .total_original_bytes
            .fetch_add(original_bytes as u64, Ordering::Relaxed);
        self.metrics
            .total_stored_bytes
            .fetch_add(stored_bytes as u64, Ordering::Relaxed);

        if stored_bytes < original_bytes {
            self.metrics
                .compressed_count
                .fetch_add(1, Ordering::Relaxed);
        } else {
            self.metrics
                .uncompressed_count
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record an error
    pub fn record_error(&self, error_type: &str) {
        if !self.enabled {
            return;
        }

        self.metrics.error_count.fetch_add(1, Ordering::Relaxed);

        match error_type {
            "compression" => {
                self.metrics
                    .compression_errors
                    .fetch_add(1, Ordering::Relaxed);
            }
            "connection" => {
                self.metrics
                    .connection_errors
                    .fetch_add(1, Ordering::Relaxed);
            }
            "serialization" => {
                self.metrics
                    .serialization_errors
                    .fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Record a cache hit
    pub fn record_cache_hit(&self) {
        if !self.enabled {
            return;
        }
        self.metrics.cache_hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a cache miss
    pub fn record_cache_miss(&self) {
        if !self.enabled {
            return;
        }
        self.metrics.cache_misses.fetch_add(1, Ordering::Relaxed);
    }

    /// Get total number of operations
    pub fn total_operations(&self) -> u64 {
        self.metrics.store_count.load(Ordering::Relaxed)
            + self.metrics.get_count.load(Ordering::Relaxed)
            + self.metrics.delete_count.load(Ordering::Relaxed)
            + self.metrics.chord_count.load(Ordering::Relaxed)
            + self.metrics.progress_count.load(Ordering::Relaxed)
    }

    /// Get store operation count
    pub fn store_count(&self) -> u64 {
        self.metrics.store_count.load(Ordering::Relaxed)
    }

    /// Get get operation count
    pub fn get_count(&self) -> u64 {
        self.metrics.get_count.load(Ordering::Relaxed)
    }

    /// Get delete operation count
    pub fn delete_count(&self) -> u64 {
        self.metrics.delete_count.load(Ordering::Relaxed)
    }

    /// Get chord operation count
    pub fn chord_count(&self) -> u64 {
        self.metrics.chord_count.load(Ordering::Relaxed)
    }

    /// Get progress operation count
    pub fn progress_count(&self) -> u64 {
        self.metrics.progress_count.load(Ordering::Relaxed)
    }

    /// Get batch store count
    pub fn batch_store_count(&self) -> u64 {
        self.metrics.batch_store_count.load(Ordering::Relaxed)
    }

    /// Get batch get count
    pub fn batch_get_count(&self) -> u64 {
        self.metrics.batch_get_count.load(Ordering::Relaxed)
    }

    /// Get batch delete count
    pub fn batch_delete_count(&self) -> u64 {
        self.metrics.batch_delete_count.load(Ordering::Relaxed)
    }

    /// Get average store latency
    pub fn avg_store_latency(&self) -> Duration {
        let count = self.store_count();
        if count == 0 {
            return Duration::from_micros(0);
        }
        let total_us = self.metrics.total_store_latency_us.load(Ordering::Relaxed);
        Duration::from_micros(total_us / count)
    }

    /// Get average get latency
    pub fn avg_get_latency(&self) -> Duration {
        let count = self.get_count();
        if count == 0 {
            return Duration::from_micros(0);
        }
        let total_us = self.metrics.total_get_latency_us.load(Ordering::Relaxed);
        Duration::from_micros(total_us / count)
    }

    /// Get average delete latency
    pub fn avg_delete_latency(&self) -> Duration {
        let count = self.delete_count();
        if count == 0 {
            return Duration::from_micros(0);
        }
        let total_us = self.metrics.total_delete_latency_us.load(Ordering::Relaxed);
        Duration::from_micros(total_us / count)
    }

    /// Get total original data size
    pub fn total_original_bytes(&self) -> u64 {
        self.metrics.total_original_bytes.load(Ordering::Relaxed)
    }

    /// Get total stored data size
    pub fn total_stored_bytes(&self) -> u64 {
        self.metrics.total_stored_bytes.load(Ordering::Relaxed)
    }

    /// Get average compression ratio
    pub fn avg_compression_ratio(&self) -> f64 {
        let original = self.total_original_bytes();
        if original == 0 {
            return 1.0;
        }
        let stored = self.total_stored_bytes();
        stored as f64 / original as f64
    }

    /// Get compression savings in bytes
    pub fn compression_savings_bytes(&self) -> i64 {
        let original = self.total_original_bytes() as i64;
        let stored = self.total_stored_bytes() as i64;
        original - stored
    }

    /// Get compression savings percentage
    pub fn compression_savings_percent(&self) -> f64 {
        let original = self.total_original_bytes() as f64;
        if original == 0.0 {
            return 0.0;
        }
        let savings = self.compression_savings_bytes() as f64;
        (savings / original) * 100.0
    }

    /// Get number of compressed results
    pub fn compressed_count(&self) -> u64 {
        self.metrics.compressed_count.load(Ordering::Relaxed)
    }

    /// Get number of uncompressed results
    pub fn uncompressed_count(&self) -> u64 {
        self.metrics.uncompressed_count.load(Ordering::Relaxed)
    }

    /// Get total error count
    pub fn error_count(&self) -> u64 {
        self.metrics.error_count.load(Ordering::Relaxed)
    }

    /// Get compression error count
    pub fn compression_errors(&self) -> u64 {
        self.metrics.compression_errors.load(Ordering::Relaxed)
    }

    /// Get connection error count
    pub fn connection_errors(&self) -> u64 {
        self.metrics.connection_errors.load(Ordering::Relaxed)
    }

    /// Get serialization error count
    pub fn serialization_errors(&self) -> u64 {
        self.metrics.serialization_errors.load(Ordering::Relaxed)
    }

    /// Get cache hit count
    pub fn cache_hits(&self) -> u64 {
        self.metrics.cache_hits.load(Ordering::Relaxed)
    }

    /// Get cache miss count
    pub fn cache_misses(&self) -> u64 {
        self.metrics.cache_misses.load(Ordering::Relaxed)
    }

    /// Get cache hit rate (0.0 to 1.0)
    pub fn cache_hit_rate(&self) -> f64 {
        let hits = self.cache_hits();
        let misses = self.cache_misses();
        let total = hits + misses;
        if total == 0 {
            return 0.0;
        }
        hits as f64 / total as f64
    }

    /// Get a snapshot of all metrics
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            total_operations: self.total_operations(),
            store_count: self.store_count(),
            get_count: self.get_count(),
            delete_count: self.delete_count(),
            chord_count: self.chord_count(),
            progress_count: self.progress_count(),
            batch_store_count: self.batch_store_count(),
            batch_get_count: self.batch_get_count(),
            batch_delete_count: self.batch_delete_count(),
            avg_store_latency: self.avg_store_latency(),
            avg_get_latency: self.avg_get_latency(),
            avg_delete_latency: self.avg_delete_latency(),
            total_original_bytes: self.total_original_bytes(),
            total_stored_bytes: self.total_stored_bytes(),
            avg_compression_ratio: self.avg_compression_ratio(),
            compression_savings_bytes: self.compression_savings_bytes(),
            compression_savings_percent: self.compression_savings_percent(),
            compressed_count: self.compressed_count(),
            uncompressed_count: self.uncompressed_count(),
            error_count: self.error_count(),
            compression_errors: self.compression_errors(),
            connection_errors: self.connection_errors(),
            serialization_errors: self.serialization_errors(),
            cache_hits: self.cache_hits(),
            cache_misses: self.cache_misses(),
            cache_hit_rate: self.cache_hit_rate(),
        }
    }
}

/// Snapshot of metrics at a point in time
#[derive(Debug, Clone)]
pub struct MetricsSnapshot {
    pub total_operations: u64,
    pub store_count: u64,
    pub get_count: u64,
    pub delete_count: u64,
    pub chord_count: u64,
    pub progress_count: u64,
    pub batch_store_count: u64,
    pub batch_get_count: u64,
    pub batch_delete_count: u64,
    pub avg_store_latency: Duration,
    pub avg_get_latency: Duration,
    pub avg_delete_latency: Duration,
    pub total_original_bytes: u64,
    pub total_stored_bytes: u64,
    pub avg_compression_ratio: f64,
    pub compression_savings_bytes: i64,
    pub compression_savings_percent: f64,
    pub compressed_count: u64,
    pub uncompressed_count: u64,
    pub error_count: u64,
    pub compression_errors: u64,
    pub connection_errors: u64,
    pub serialization_errors: u64,
    pub cache_hits: u64,
    pub cache_misses: u64,
    pub cache_hit_rate: f64,
}

impl std::fmt::Display for MetricsSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "Redis Backend Metrics:")?;
        writeln!(f, "  Operations:")?;
        writeln!(f, "    Total: {}", self.total_operations)?;
        writeln!(f, "    Store: {}", self.store_count)?;
        writeln!(f, "    Get: {}", self.get_count)?;
        writeln!(f, "    Delete: {}", self.delete_count)?;
        writeln!(f, "    Chord: {}", self.chord_count)?;
        writeln!(f, "    Progress: {}", self.progress_count)?;
        writeln!(f, "  Batch Operations:")?;
        writeln!(f, "    Store: {}", self.batch_store_count)?;
        writeln!(f, "    Get: {}", self.batch_get_count)?;
        writeln!(f, "    Delete: {}", self.batch_delete_count)?;
        writeln!(f, "  Latency:")?;
        writeln!(f, "    Avg Store: {:?}", self.avg_store_latency)?;
        writeln!(f, "    Avg Get: {:?}", self.avg_get_latency)?;
        writeln!(f, "    Avg Delete: {:?}", self.avg_delete_latency)?;
        writeln!(f, "  Compression:")?;
        writeln!(f, "    Ratio: {:.2}", self.avg_compression_ratio)?;
        writeln!(
            f,
            "    Savings: {} bytes ({:.1}%)",
            self.compression_savings_bytes, self.compression_savings_percent
        )?;
        writeln!(f, "    Compressed: {}", self.compressed_count)?;
        writeln!(f, "    Uncompressed: {}", self.uncompressed_count)?;
        writeln!(f, "  Cache:")?;
        writeln!(f, "    Hits: {}", self.cache_hits)?;
        writeln!(f, "    Misses: {}", self.cache_misses)?;
        writeln!(f, "    Hit Rate: {:.1}%", self.cache_hit_rate * 100.0)?;
        writeln!(f, "  Errors:")?;
        writeln!(f, "    Total: {}", self.error_count)?;
        writeln!(f, "    Compression: {}", self.compression_errors)?;
        writeln!(f, "    Connection: {}", self.connection_errors)?;
        writeln!(f, "    Serialization: {}", self.serialization_errors)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metrics_creation() {
        let metrics = BackendMetrics::new();
        assert!(metrics.is_enabled());
        assert_eq!(metrics.total_operations(), 0);
    }

    #[test]
    fn test_metrics_disabled() {
        let metrics = BackendMetrics::disabled();
        assert!(!metrics.is_enabled());

        // Recording operations should be no-op
        metrics.record_operation(OperationType::StoreResult, Duration::from_millis(10));
        assert_eq!(metrics.store_count(), 0);
    }

    #[test]
    fn test_record_operations() {
        let metrics = BackendMetrics::new();

        metrics.record_operation(OperationType::StoreResult, Duration::from_millis(5));
        metrics.record_operation(OperationType::StoreResult, Duration::from_millis(15));
        metrics.record_operation(OperationType::GetResult, Duration::from_millis(10));

        assert_eq!(metrics.store_count(), 2);
        assert_eq!(metrics.get_count(), 1);
        assert_eq!(metrics.total_operations(), 3);
    }

    #[test]
    fn test_average_latency() {
        let metrics = BackendMetrics::new();

        metrics.record_operation(OperationType::StoreResult, Duration::from_micros(1000));
        metrics.record_operation(OperationType::StoreResult, Duration::from_micros(2000));

        let avg = metrics.avg_store_latency();
        assert_eq!(avg.as_micros(), 1500);
    }

    #[test]
    fn test_data_size_tracking() {
        let metrics = BackendMetrics::new();

        metrics.record_data_size(1000, 500); // Compressed
        metrics.record_data_size(1000, 1000); // Not compressed

        assert_eq!(metrics.total_original_bytes(), 2000);
        assert_eq!(metrics.total_stored_bytes(), 1500);
        assert_eq!(metrics.compressed_count(), 1);
        assert_eq!(metrics.uncompressed_count(), 1);
        assert_eq!(metrics.compression_savings_bytes(), 500);
        assert_eq!(metrics.avg_compression_ratio(), 0.75);
    }

    #[test]
    fn test_error_tracking() {
        let metrics = BackendMetrics::new();

        metrics.record_error("compression");
        metrics.record_error("connection");
        metrics.record_error("serialization");

        assert_eq!(metrics.error_count(), 3);
        assert_eq!(metrics.compression_errors(), 1);
        assert_eq!(metrics.connection_errors(), 1);
        assert_eq!(metrics.serialization_errors(), 1);
    }

    #[test]
    fn test_cache_metrics() {
        let metrics = BackendMetrics::new();

        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_hit();
        metrics.record_cache_miss();

        assert_eq!(metrics.cache_hits(), 3);
        assert_eq!(metrics.cache_misses(), 1);
        assert_eq!(metrics.cache_hit_rate(), 0.75);
    }

    #[test]
    fn test_metrics_snapshot() {
        let metrics = BackendMetrics::new();

        metrics.record_operation(OperationType::StoreResult, Duration::from_millis(10));
        metrics.record_data_size(1000, 500);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.store_count, 1);
        assert_eq!(snapshot.total_original_bytes, 1000);
        assert_eq!(snapshot.total_stored_bytes, 500);
    }

    #[test]
    fn test_operation_type_as_str() {
        assert_eq!(OperationType::StoreResult.as_str(), "store_result");
        assert_eq!(OperationType::GetResult.as_str(), "get_result");
        assert_eq!(OperationType::ChordOperation.as_str(), "chord_operation");
    }
}
