//! Garbage collection tuning and memory optimization
//!
//! This module provides tools for optimizing memory usage and providing GC hints:
//! - **Allocation Monitoring**: Track allocation rates and patterns
//! - **GC Pressure Detection**: Detect high memory pressure situations
//! - **Memory Optimization**: Provide hints for when to trigger GC
//! - **Statistics**: Track GC-related metrics over time
//!
//! # Example
//!
//! ```ignore
//! use celers_worker::gc_tuning::{GcTuner, GcConfig};
//!
//! let config = GcConfig::default();
//! let tuner = GcTuner::new(config);
//!
//! // Record allocation
//! tuner.record_allocation(1024);
//!
//! // Check if GC is recommended
//! if tuner.should_gc() {
//!     println!("High memory pressure detected, consider triggering GC");
//! }
//!
//! // Get statistics
//! let stats = tuner.get_stats();
//! println!("Total allocated: {} bytes", stats.total_allocated);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

/// Configuration for GC tuning
#[derive(Debug, Clone)]
pub struct GcConfig {
    /// Memory threshold (bytes) above which GC is recommended
    pub memory_threshold_bytes: usize,

    /// Allocation rate threshold (bytes/sec) above which GC is recommended
    pub allocation_rate_threshold: usize,

    /// Time window for calculating allocation rate
    pub rate_window_secs: u64,

    /// Minimum time between GC recommendations (to avoid thrashing)
    pub min_gc_interval_secs: u64,

    /// Enable automatic memory compaction hints
    pub enable_compaction_hints: bool,

    /// Number of allocation samples to keep for statistics
    pub sample_window_size: usize,
}

impl Default for GcConfig {
    fn default() -> Self {
        Self {
            memory_threshold_bytes: 512 * 1024 * 1024,   // 512 MB
            allocation_rate_threshold: 10 * 1024 * 1024, // 10 MB/s
            rate_window_secs: 10,
            min_gc_interval_secs: 30,
            enable_compaction_hints: true,
            sample_window_size: 100,
        }
    }
}

impl GcConfig {
    /// Create a conservative configuration (less aggressive GC)
    pub fn conservative() -> Self {
        Self {
            memory_threshold_bytes: 1024 * 1024 * 1024,  // 1 GB
            allocation_rate_threshold: 50 * 1024 * 1024, // 50 MB/s
            min_gc_interval_secs: 60,
            ..Default::default()
        }
    }

    /// Create an aggressive configuration (more frequent GC)
    pub fn aggressive() -> Self {
        Self {
            memory_threshold_bytes: 256 * 1024 * 1024,  // 256 MB
            allocation_rate_threshold: 5 * 1024 * 1024, // 5 MB/s
            min_gc_interval_secs: 15,
            ..Default::default()
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.memory_threshold_bytes == 0 {
            return Err("Memory threshold must be greater than 0".to_string());
        }
        if self.rate_window_secs == 0 {
            return Err("Rate window must be greater than 0".to_string());
        }
        if self.sample_window_size == 0 {
            return Err("Sample window size must be greater than 0".to_string());
        }
        Ok(())
    }
}

/// GC statistics
#[derive(Debug, Clone)]
pub struct GcStats {
    /// Total bytes allocated since start
    pub total_allocated: u64,

    /// Total bytes deallocated since start
    pub total_deallocated: u64,

    /// Current memory usage (allocated - deallocated)
    pub current_usage: u64,

    /// Peak memory usage observed
    pub peak_usage: u64,

    /// Current allocation rate (bytes/sec)
    pub allocation_rate: f64,

    /// Number of times GC was recommended
    pub gc_recommendations: u64,

    /// Number of compaction hints issued
    pub compaction_hints: u64,

    /// Average allocation size
    pub avg_allocation_size: f64,

    /// Time since last GC recommendation
    pub time_since_last_gc: Option<Duration>,
}

impl GcStats {
    /// Check if memory usage is high
    pub fn is_high_usage(&self, threshold: usize) -> bool {
        self.current_usage as usize > threshold
    }

    /// Check if allocation rate is high
    pub fn is_high_allocation_rate(&self, threshold: usize) -> bool {
        self.allocation_rate > threshold as f64
    }

    /// Get memory efficiency (allocated vs deallocated ratio)
    pub fn memory_efficiency(&self) -> f64 {
        if self.total_allocated == 0 {
            return 1.0;
        }
        self.total_deallocated as f64 / self.total_allocated as f64
    }
}

struct AllocationSample {
    timestamp: Instant,
    size: usize,
}

/// GC tuner for monitoring and optimizing memory usage
pub struct GcTuner {
    config: GcConfig,
    total_allocated: AtomicU64,
    total_deallocated: AtomicU64,
    peak_usage: AtomicU64,
    gc_recommendations: AtomicU64,
    compaction_hints: AtomicU64,
    allocation_count: AtomicU64,
    last_gc_recommendation: Arc<RwLock<Option<Instant>>>,
    allocation_samples: Arc<RwLock<Vec<AllocationSample>>>,
}

impl GcTuner {
    /// Create a new GC tuner
    pub fn new(config: GcConfig) -> Self {
        Self {
            config,
            total_allocated: AtomicU64::new(0),
            total_deallocated: AtomicU64::new(0),
            peak_usage: AtomicU64::new(0),
            gc_recommendations: AtomicU64::new(0),
            compaction_hints: AtomicU64::new(0),
            allocation_count: AtomicU64::new(0),
            last_gc_recommendation: Arc::new(RwLock::new(None)),
            allocation_samples: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Record an allocation
    pub fn record_allocation(&self, size: usize) {
        let allocated = self
            .total_allocated
            .fetch_add(size as u64, Ordering::Relaxed)
            + size as u64;
        let deallocated = self.total_deallocated.load(Ordering::Relaxed);
        let current = allocated.saturating_sub(deallocated);

        // Update peak usage
        let mut peak = self.peak_usage.load(Ordering::Relaxed);
        while current > peak {
            match self.peak_usage.compare_exchange_weak(
                peak,
                current,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(new_peak) => peak = new_peak,
            }
        }

        self.allocation_count.fetch_add(1, Ordering::Relaxed);

        // Record sample (async)
        let samples = self.allocation_samples.clone();
        let max_samples = self.config.sample_window_size;
        tokio::spawn(async move {
            let mut samples = samples.write().await;
            samples.push(AllocationSample {
                timestamp: Instant::now(),
                size,
            });

            // Keep only recent samples
            if samples.len() > max_samples {
                let excess = samples.len() - max_samples;
                samples.drain(0..excess);
            }
        });

        #[cfg(feature = "metrics")]
        {
            use celers_metrics::WORKER_MEMORY_USAGE_BYTES;
            WORKER_MEMORY_USAGE_BYTES.set(current as f64);
        }
    }

    /// Record a deallocation
    pub fn record_deallocation(&self, size: usize) {
        self.total_deallocated
            .fetch_add(size as u64, Ordering::Relaxed);
    }

    /// Check if GC should be triggered
    pub async fn should_gc(&self) -> bool {
        let stats = self.get_stats().await;

        // Check memory threshold
        if stats.is_high_usage(self.config.memory_threshold_bytes) {
            return self.record_gc_recommendation().await;
        }

        // Check allocation rate
        if stats.is_high_allocation_rate(self.config.allocation_rate_threshold) {
            return self.record_gc_recommendation().await;
        }

        false
    }

    /// Record a GC recommendation
    async fn record_gc_recommendation(&self) -> bool {
        let mut last_gc = self.last_gc_recommendation.write().await;

        // Check minimum interval
        if let Some(last_time) = *last_gc {
            let elapsed = last_time.elapsed();
            if elapsed.as_secs() < self.config.min_gc_interval_secs {
                return false;
            }
        }

        self.gc_recommendations.fetch_add(1, Ordering::Relaxed);
        *last_gc = Some(Instant::now());

        #[cfg(feature = "metrics")]
        {
            use celers_metrics::GC_RECOMMENDATIONS_TOTAL;
            GC_RECOMMENDATIONS_TOTAL.inc();
        }

        true
    }

    /// Check if memory compaction is recommended
    pub fn should_compact(&self) -> bool {
        if !self.config.enable_compaction_hints {
            return false;
        }

        let efficiency = self.memory_efficiency();

        // Recommend compaction if efficiency is low (< 50%)
        if efficiency < 0.5 {
            self.compaction_hints.fetch_add(1, Ordering::Relaxed);
            return true;
        }

        false
    }

    /// Calculate current allocation rate (bytes/sec)
    async fn calculate_allocation_rate(&self) -> f64 {
        let samples = self.allocation_samples.read().await;

        if samples.len() < 2 {
            return 0.0;
        }

        let window = Duration::from_secs(self.config.rate_window_secs);
        let cutoff = Instant::now() - window;

        let recent_samples: Vec<_> = samples.iter().filter(|s| s.timestamp > cutoff).collect();

        if recent_samples.is_empty() {
            return 0.0;
        }

        let total_bytes: usize = recent_samples.iter().map(|s| s.size).sum();
        let duration = recent_samples
            .last()
            .unwrap()
            .timestamp
            .duration_since(recent_samples.first().unwrap().timestamp);

        if duration.as_secs_f64() == 0.0 {
            return 0.0;
        }

        total_bytes as f64 / duration.as_secs_f64()
    }

    /// Get current memory efficiency
    fn memory_efficiency(&self) -> f64 {
        let allocated = self.total_allocated.load(Ordering::Relaxed);
        if allocated == 0 {
            return 1.0;
        }
        let deallocated = self.total_deallocated.load(Ordering::Relaxed);
        deallocated as f64 / allocated as f64
    }

    /// Get GC statistics
    pub async fn get_stats(&self) -> GcStats {
        let total_allocated = self.total_allocated.load(Ordering::Relaxed);
        let total_deallocated = self.total_deallocated.load(Ordering::Relaxed);
        let peak_usage = self.peak_usage.load(Ordering::Relaxed);
        let gc_recommendations = self.gc_recommendations.load(Ordering::Relaxed);
        let compaction_hints = self.compaction_hints.load(Ordering::Relaxed);
        let allocation_count = self.allocation_count.load(Ordering::Relaxed);

        let current_usage = total_allocated.saturating_sub(total_deallocated);
        let allocation_rate = self.calculate_allocation_rate().await;

        let avg_allocation_size = if allocation_count > 0 {
            total_allocated as f64 / allocation_count as f64
        } else {
            0.0
        };

        let time_since_last_gc = {
            let last_gc = self.last_gc_recommendation.read().await;
            last_gc.map(|t| t.elapsed())
        };

        GcStats {
            total_allocated,
            total_deallocated,
            current_usage,
            peak_usage,
            allocation_rate,
            gc_recommendations,
            compaction_hints,
            avg_allocation_size,
            time_since_last_gc,
        }
    }

    /// Reset all statistics
    pub async fn reset_stats(&self) {
        self.total_allocated.store(0, Ordering::Relaxed);
        self.total_deallocated.store(0, Ordering::Relaxed);
        self.peak_usage.store(0, Ordering::Relaxed);
        self.gc_recommendations.store(0, Ordering::Relaxed);
        self.compaction_hints.store(0, Ordering::Relaxed);
        self.allocation_count.store(0, Ordering::Relaxed);

        let mut last_gc = self.last_gc_recommendation.write().await;
        *last_gc = None;

        let mut samples = self.allocation_samples.write().await;
        samples.clear();
    }

    /// Force a GC recommendation (ignoring minimum interval)
    pub async fn force_gc_recommendation(&self) {
        self.gc_recommendations.fetch_add(1, Ordering::Relaxed);
        let mut last_gc = self.last_gc_recommendation.write().await;
        *last_gc = Some(Instant::now());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_gc_config_validation() {
        let config = GcConfig::default();
        assert!(config.validate().is_ok());

        let config = GcConfig {
            memory_threshold_bytes: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn test_gc_config_presets() {
        let conservative = GcConfig::conservative();
        assert!(conservative.memory_threshold_bytes > GcConfig::default().memory_threshold_bytes);

        let aggressive = GcConfig::aggressive();
        assert!(aggressive.memory_threshold_bytes < GcConfig::default().memory_threshold_bytes);
    }

    #[tokio::test]
    async fn test_allocation_tracking() {
        let config = GcConfig::default();
        let tuner = GcTuner::new(config);

        tuner.record_allocation(1024);
        tuner.record_allocation(2048);

        let stats = tuner.get_stats().await;
        assert_eq!(stats.total_allocated, 3072);
        assert_eq!(stats.current_usage, 3072);
    }

    #[tokio::test]
    async fn test_deallocation_tracking() {
        let config = GcConfig::default();
        let tuner = GcTuner::new(config);

        tuner.record_allocation(2048);
        tuner.record_deallocation(1024);

        let stats = tuner.get_stats().await;
        assert_eq!(stats.total_allocated, 2048);
        assert_eq!(stats.total_deallocated, 1024);
        assert_eq!(stats.current_usage, 1024);
    }

    #[tokio::test]
    async fn test_peak_usage_tracking() {
        let config = GcConfig::default();
        let tuner = GcTuner::new(config);

        tuner.record_allocation(1024);
        tuner.record_allocation(2048);
        tuner.record_deallocation(1024);

        let stats = tuner.get_stats().await;
        assert_eq!(stats.peak_usage, 3072); // Peak was before deallocation
    }

    #[tokio::test]
    async fn test_memory_threshold_gc() {
        let config = GcConfig {
            memory_threshold_bytes: 1000,
            ..Default::default()
        };
        let tuner = GcTuner::new(config);

        tuner.record_allocation(2000);

        // Should recommend GC due to exceeding threshold
        assert!(tuner.should_gc().await);
    }

    #[tokio::test]
    async fn test_gc_minimum_interval() {
        let config = GcConfig {
            memory_threshold_bytes: 1000,
            min_gc_interval_secs: 3600, // 1 hour
            ..Default::default()
        };
        let tuner = GcTuner::new(config);

        tuner.record_allocation(2000);

        // First recommendation should succeed
        assert!(tuner.should_gc().await);

        // Second recommendation should fail (within interval)
        assert!(!tuner.should_gc().await);
    }

    #[tokio::test]
    async fn test_memory_efficiency() {
        let config = GcConfig::default();
        let tuner = GcTuner::new(config);

        tuner.record_allocation(1000);
        tuner.record_deallocation(500);

        let stats = tuner.get_stats().await;
        assert_eq!(stats.memory_efficiency(), 0.5);
    }

    #[tokio::test]
    async fn test_compaction_hints() {
        let config = GcConfig {
            enable_compaction_hints: true,
            ..Default::default()
        };
        let tuner = GcTuner::new(config);

        // High fragmentation (lots of allocation, little deallocation)
        tuner.record_allocation(1000);
        tuner.record_deallocation(100);

        // Should recommend compaction due to low efficiency
        assert!(tuner.should_compact());
    }

    #[tokio::test]
    async fn test_stats_reset() {
        let config = GcConfig::default();
        let tuner = GcTuner::new(config);

        tuner.record_allocation(1024);
        tuner.reset_stats().await;

        let stats = tuner.get_stats().await;
        assert_eq!(stats.total_allocated, 0);
        assert_eq!(stats.current_usage, 0);
    }

    #[tokio::test]
    async fn test_average_allocation_size() {
        let config = GcConfig::default();
        let tuner = GcTuner::new(config);

        tuner.record_allocation(100);
        tuner.record_allocation(200);
        tuner.record_allocation(300);

        let stats = tuner.get_stats().await;
        assert_eq!(stats.avg_allocation_size, 200.0);
    }

    #[tokio::test]
    async fn test_force_gc_recommendation() {
        let config = GcConfig {
            memory_threshold_bytes: 1000000, // High threshold
            min_gc_interval_secs: 3600,      // Long interval
            ..Default::default()
        };
        let tuner = GcTuner::new(config);

        // Force recommendation regardless of thresholds
        tuner.force_gc_recommendation().await;

        let stats = tuner.get_stats().await;
        assert_eq!(stats.gc_recommendations, 1);
    }

    #[tokio::test]
    async fn test_allocation_rate_calculation() {
        let config = GcConfig {
            rate_window_secs: 1,
            ..Default::default()
        };
        let tuner = GcTuner::new(config);

        // Record some allocations
        for _ in 0..5 {
            tuner.record_allocation(1000);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        let stats = tuner.get_stats().await;
        // Rate should be positive (actual value depends on timing)
        assert!(stats.allocation_rate > 0.0);
    }
}
