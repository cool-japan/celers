//! Task prefetching to reduce latency
//!
//! This module provides task prefetching capabilities to reduce latency by fetching
//! tasks from the broker before they're needed. This minimizes the time workers spend
//! waiting for tasks to be dequeued.
//!
//! # Features
//!
//! - **Configurable prefetch count**: Control how many tasks to keep ready
//! - **Adaptive prefetching**: Automatically adjust prefetch behavior based on processing speed
//! - **Graceful shutdown**: Cancel prefetch operations during shutdown
//! - **Batch integration**: Works seamlessly with batch dequeue
//!
//! # Example
//!
//! ```
//! use celers_worker::prefetch::{PrefetchConfig, PrefetchBuffer};
//! use std::time::Duration;
//!
//! # async fn example() {
//! let config = PrefetchConfig {
//!     prefetch_count: 10,
//!     adaptive: true,
//!     min_buffer_size: 2,
//!     max_buffer_size: 50,
//! };
//!
//! let buffer = PrefetchBuffer::new(config);
//!
//! // Check if we should prefetch more tasks
//! if buffer.should_prefetch().await {
//!     // Fetch tasks from broker
//! }
//! # }
//! ```

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{RwLock, Semaphore};
use tracing::{debug, trace};

/// Configuration for task prefetching
#[derive(Debug, Clone)]
pub struct PrefetchConfig {
    /// Number of tasks to prefetch (keep ready in buffer)
    pub prefetch_count: usize,

    /// Enable adaptive prefetching based on processing speed
    pub adaptive: bool,

    /// Minimum buffer size (adaptive mode)
    pub min_buffer_size: usize,

    /// Maximum buffer size (adaptive mode)
    pub max_buffer_size: usize,
}

impl Default for PrefetchConfig {
    fn default() -> Self {
        Self {
            prefetch_count: 5,
            adaptive: true,
            min_buffer_size: 2,
            max_buffer_size: 20,
        }
    }
}

impl PrefetchConfig {
    /// Validate configuration
    pub fn validate(&self) -> Result<(), String> {
        if self.prefetch_count == 0 {
            return Err("prefetch_count must be greater than 0".to_string());
        }

        if self.adaptive {
            if self.min_buffer_size == 0 {
                return Err("min_buffer_size must be greater than 0".to_string());
            }
            if self.max_buffer_size < self.min_buffer_size {
                return Err("max_buffer_size must be >= min_buffer_size".to_string());
            }
            if self.prefetch_count < self.min_buffer_size {
                return Err("prefetch_count must be >= min_buffer_size".to_string());
            }
            if self.prefetch_count > self.max_buffer_size {
                return Err("prefetch_count must be <= max_buffer_size".to_string());
            }
        }

        Ok(())
    }

    /// Check if prefetching is enabled
    pub fn is_enabled(&self) -> bool {
        self.prefetch_count > 0
    }

    /// Check if adaptive mode is enabled
    pub fn is_adaptive(&self) -> bool {
        self.adaptive && self.is_enabled()
    }
}

impl std::fmt::Display for PrefetchConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PrefetchConfig(count={}, adaptive={}, min={}, max={})",
            self.prefetch_count, self.adaptive, self.min_buffer_size, self.max_buffer_size
        )
    }
}

/// Statistics for prefetch buffer
#[derive(Debug, Clone)]
pub struct PrefetchStats {
    /// Current buffer size
    pub buffer_size: usize,
    /// Total tasks prefetched
    pub total_prefetched: usize,
    /// Total tasks consumed from buffer
    pub total_consumed: usize,
    /// Current prefetch count (adaptive mode)
    pub current_prefetch_count: usize,
    /// Average processing time (microseconds)
    pub avg_processing_time_us: u64,
    /// Prefetch hit rate (0.0-1.0)
    pub hit_rate: f64,
}

impl Default for PrefetchStats {
    fn default() -> Self {
        Self {
            buffer_size: 0,
            total_prefetched: 0,
            total_consumed: 0,
            current_prefetch_count: 0,
            avg_processing_time_us: 0,
            hit_rate: 0.0,
        }
    }
}

impl std::fmt::Display for PrefetchStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PrefetchStats(buffer={}, prefetched={}, consumed={}, hit_rate={:.2}%)",
            self.buffer_size,
            self.total_prefetched,
            self.total_consumed,
            self.hit_rate * 100.0
        )
    }
}

/// Internal state for prefetch buffer
#[derive(Debug)]
struct PrefetchState {
    /// Configuration
    config: PrefetchConfig,

    /// Current buffer size (approximation)
    buffer_size: AtomicUsize,

    /// Current prefetch count (adaptive)
    current_prefetch_count: AtomicUsize,

    /// Total tasks prefetched
    total_prefetched: AtomicUsize,

    /// Total tasks consumed from buffer
    total_consumed: AtomicUsize,

    /// Total tasks that had to wait (buffer was empty)
    total_misses: AtomicUsize,

    /// Recent processing times for adaptive adjustment
    processing_times: RwLock<Vec<Duration>>,

    /// Last adjustment time
    last_adjustment: RwLock<Instant>,

    /// Shutdown flag
    shutdown: AtomicBool,
}

/// Task prefetch buffer
///
/// Manages a buffer of prefetched tasks to reduce latency when workers
/// request tasks from the broker.
#[derive(Debug, Clone)]
pub struct PrefetchBuffer {
    state: Arc<PrefetchState>,
    /// Semaphore to limit concurrent prefetch operations
    prefetch_semaphore: Arc<Semaphore>,
}

impl PrefetchBuffer {
    /// Create a new prefetch buffer
    pub fn new(config: PrefetchConfig) -> Self {
        let prefetch_count = config.prefetch_count;

        Self {
            state: Arc::new(PrefetchState {
                current_prefetch_count: AtomicUsize::new(prefetch_count),
                buffer_size: AtomicUsize::new(0),
                total_prefetched: AtomicUsize::new(0),
                total_consumed: AtomicUsize::new(0),
                total_misses: AtomicUsize::new(0),
                processing_times: RwLock::new(Vec::new()),
                last_adjustment: RwLock::new(Instant::now()),
                shutdown: AtomicBool::new(false),
                config,
            }),
            prefetch_semaphore: Arc::new(Semaphore::new(1)),
        }
    }

    /// Check if we should prefetch more tasks
    pub async fn should_prefetch(&self) -> bool {
        if self.state.shutdown.load(Ordering::Relaxed) {
            return false;
        }

        let buffer_size = self.state.buffer_size.load(Ordering::Relaxed);
        let prefetch_count = self.state.current_prefetch_count.load(Ordering::Relaxed);

        buffer_size < prefetch_count
    }

    /// Record that tasks were prefetched
    pub async fn record_prefetch(&self, count: usize) {
        if count == 0 {
            return;
        }

        self.state.buffer_size.fetch_add(count, Ordering::Relaxed);
        self.state
            .total_prefetched
            .fetch_add(count, Ordering::Relaxed);

        trace!(
            "Prefetched {} tasks, buffer size now: {}",
            count,
            self.state.buffer_size.load(Ordering::Relaxed)
        );
    }

    /// Record that a task was consumed from the buffer
    pub async fn record_consume(&self, had_to_wait: bool, processing_time: Option<Duration>) {
        let current_size = self.state.buffer_size.load(Ordering::Relaxed);
        if current_size > 0 {
            self.state.buffer_size.fetch_sub(1, Ordering::Relaxed);
        }
        self.state.total_consumed.fetch_add(1, Ordering::Relaxed);

        if had_to_wait {
            self.state.total_misses.fetch_add(1, Ordering::Relaxed);
        }

        // Record processing time for adaptive adjustment
        if let Some(duration) = processing_time {
            if self.state.config.adaptive {
                let mut times = self.state.processing_times.write().await;
                times.push(duration);

                // Keep only recent samples (last 100)
                if times.len() > 100 {
                    times.remove(0);
                }
            }
        }

        // Adjust prefetch count if adaptive mode is enabled
        if self.state.config.adaptive {
            self.adjust_prefetch_count().await;
        }
    }

    /// Adjust prefetch count based on processing patterns (adaptive mode)
    async fn adjust_prefetch_count(&self) {
        let mut last_adjustment = self.state.last_adjustment.write().await;
        let now = Instant::now();

        // Only adjust every 5 seconds
        if now.duration_since(*last_adjustment) < Duration::from_secs(5) {
            return;
        }

        *last_adjustment = now;

        let times = self.state.processing_times.read().await;
        if times.len() < 10 {
            return; // Not enough samples
        }

        // Calculate average processing time
        let avg_time = times.iter().sum::<Duration>() / times.len() as u32;

        let current_count = self.state.current_prefetch_count.load(Ordering::Relaxed);
        let total_consumed = self.state.total_consumed.load(Ordering::Relaxed);
        let total_misses = self.state.total_misses.load(Ordering::Relaxed);

        // Calculate hit rate
        let hit_rate = if total_consumed > 0 {
            1.0 - (total_misses as f64 / total_consumed as f64)
        } else {
            1.0
        };

        let new_count = if hit_rate < 0.9 {
            // Too many misses, increase prefetch count
            (current_count + 2).min(self.state.config.max_buffer_size)
        } else if hit_rate > 0.98 && avg_time < Duration::from_millis(100) {
            // Very high hit rate and fast processing, can reduce buffer
            (current_count.saturating_sub(1)).max(self.state.config.min_buffer_size)
        } else {
            current_count
        };

        if new_count != current_count {
            debug!(
                "Adjusting prefetch count: {} -> {} (hit_rate={:.2}%, avg_time={:?})",
                current_count,
                new_count,
                hit_rate * 100.0,
                avg_time
            );
            self.state
                .current_prefetch_count
                .store(new_count, Ordering::Relaxed);
        }
    }

    /// Get current statistics
    pub async fn get_stats(&self) -> PrefetchStats {
        let buffer_size = self.state.buffer_size.load(Ordering::Relaxed);
        let total_prefetched = self.state.total_prefetched.load(Ordering::Relaxed);
        let total_consumed = self.state.total_consumed.load(Ordering::Relaxed);
        let current_prefetch_count = self.state.current_prefetch_count.load(Ordering::Relaxed);
        let total_misses = self.state.total_misses.load(Ordering::Relaxed);

        let times = self.state.processing_times.read().await;
        let avg_processing_time_us = if !times.is_empty() {
            let avg = times.iter().sum::<Duration>() / times.len() as u32;
            avg.as_micros() as u64
        } else {
            0
        };

        let hit_rate = if total_consumed > 0 {
            1.0 - (total_misses as f64 / total_consumed as f64)
        } else {
            0.0
        };

        PrefetchStats {
            buffer_size,
            total_prefetched,
            total_consumed,
            current_prefetch_count,
            avg_processing_time_us,
            hit_rate,
        }
    }

    /// Get current buffer size
    pub async fn buffer_size(&self) -> usize {
        self.state.buffer_size.load(Ordering::Relaxed)
    }

    /// Get current prefetch count
    pub async fn prefetch_count(&self) -> usize {
        self.state.current_prefetch_count.load(Ordering::Relaxed)
    }

    /// Signal shutdown (stop prefetching)
    pub async fn shutdown(&self) {
        self.state.shutdown.store(true, Ordering::Relaxed);
        debug!("Prefetch buffer shutdown");
    }

    /// Check if shutdown was signaled
    pub async fn is_shutdown(&self) -> bool {
        self.state.shutdown.load(Ordering::Relaxed)
    }

    /// Reset statistics
    pub async fn reset_stats(&self) {
        self.state.total_prefetched.store(0, Ordering::Relaxed);
        self.state.total_consumed.store(0, Ordering::Relaxed);
        self.state.total_misses.store(0, Ordering::Relaxed);

        let mut times = self.state.processing_times.write().await;
        times.clear();

        let mut last_adjustment = self.state.last_adjustment.write().await;
        *last_adjustment = Instant::now();
    }

    /// Try to acquire the prefetch permit (non-blocking)
    pub fn try_acquire_prefetch_permit(&self) -> Option<tokio::sync::SemaphorePermit<'_>> {
        self.prefetch_semaphore.try_acquire().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prefetch_config_default() {
        let config = PrefetchConfig::default();
        assert_eq!(config.prefetch_count, 5);
        assert!(config.adaptive);
        assert_eq!(config.min_buffer_size, 2);
        assert_eq!(config.max_buffer_size, 20);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_prefetch_config_validation() {
        // Invalid: prefetch_count = 0
        let config = PrefetchConfig {
            prefetch_count: 0,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        // Invalid: min_buffer_size = 0
        let config = PrefetchConfig {
            prefetch_count: 5,
            adaptive: true,
            min_buffer_size: 0,
            max_buffer_size: 10,
        };
        assert!(config.validate().is_err());

        // Invalid: max < min
        let config = PrefetchConfig {
            prefetch_count: 5,
            adaptive: true,
            min_buffer_size: 10,
            max_buffer_size: 5,
        };
        assert!(config.validate().is_err());

        // Invalid: prefetch_count < min
        let config = PrefetchConfig {
            prefetch_count: 1,
            adaptive: true,
            min_buffer_size: 2,
            max_buffer_size: 20,
        };
        assert!(config.validate().is_err());

        // Invalid: prefetch_count > max
        let config = PrefetchConfig {
            prefetch_count: 25,
            adaptive: true,
            min_buffer_size: 2,
            max_buffer_size: 20,
        };
        assert!(config.validate().is_err());

        // Valid
        let config = PrefetchConfig {
            prefetch_count: 10,
            adaptive: true,
            min_buffer_size: 2,
            max_buffer_size: 20,
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_prefetch_config_predicates() {
        let config = PrefetchConfig {
            prefetch_count: 5,
            adaptive: true,
            min_buffer_size: 2,
            max_buffer_size: 20,
        };

        assert!(config.is_enabled());
        assert!(config.is_adaptive());

        let config = PrefetchConfig {
            prefetch_count: 0,
            ..config
        };
        assert!(!config.is_enabled());
        assert!(!config.is_adaptive());
    }

    #[tokio::test]
    async fn test_prefetch_buffer_new() {
        let config = PrefetchConfig::default();
        let buffer = PrefetchBuffer::new(config.clone());

        assert_eq!(buffer.buffer_size().await, 0);
        assert_eq!(buffer.prefetch_count().await, config.prefetch_count);
        assert!(!buffer.is_shutdown().await);
    }

    #[tokio::test]
    async fn test_prefetch_buffer_should_prefetch() {
        let config = PrefetchConfig {
            prefetch_count: 5,
            ..Default::default()
        };
        let buffer = PrefetchBuffer::new(config);

        // Empty buffer should prefetch
        assert!(buffer.should_prefetch().await);

        // Add some tasks
        buffer.record_prefetch(3).await;
        assert!(buffer.should_prefetch().await);

        // Fill buffer
        buffer.record_prefetch(2).await;
        assert!(!buffer.should_prefetch().await);

        // Consume one task
        buffer.record_consume(false, None).await;
        assert!(buffer.should_prefetch().await);
    }

    #[tokio::test]
    async fn test_prefetch_buffer_record_operations() {
        let config = PrefetchConfig::default();
        let buffer = PrefetchBuffer::new(config);

        // Record prefetch
        buffer.record_prefetch(5).await;
        let stats = buffer.get_stats().await;
        assert_eq!(stats.buffer_size, 5);
        assert_eq!(stats.total_prefetched, 5);
        assert_eq!(stats.total_consumed, 0);

        // Record consume
        buffer
            .record_consume(false, Some(Duration::from_millis(100)))
            .await;
        let stats = buffer.get_stats().await;
        assert_eq!(stats.buffer_size, 4);
        assert_eq!(stats.total_consumed, 1);
    }

    #[tokio::test]
    async fn test_prefetch_buffer_shutdown() {
        let config = PrefetchConfig::default();
        let buffer = PrefetchBuffer::new(config);

        assert!(!buffer.is_shutdown().await);
        assert!(buffer.should_prefetch().await);

        buffer.shutdown().await;

        assert!(buffer.is_shutdown().await);
        assert!(!buffer.should_prefetch().await);
    }

    #[tokio::test]
    async fn test_prefetch_buffer_stats() {
        let config = PrefetchConfig::default();
        let buffer = PrefetchBuffer::new(config.clone());

        buffer.record_prefetch(10).await;
        buffer
            .record_consume(false, Some(Duration::from_millis(50)))
            .await;
        buffer
            .record_consume(false, Some(Duration::from_millis(100)))
            .await;
        buffer
            .record_consume(false, Some(Duration::from_millis(75)))
            .await;

        let stats = buffer.get_stats().await;
        assert_eq!(stats.buffer_size, 7);
        assert_eq!(stats.total_prefetched, 10);
        assert_eq!(stats.total_consumed, 3);
        assert_eq!(stats.current_prefetch_count, config.prefetch_count);
        assert!(stats.avg_processing_time_us > 0);
    }

    #[tokio::test]
    async fn test_prefetch_buffer_hit_rate() {
        let config = PrefetchConfig::default();
        let buffer = PrefetchBuffer::new(config);

        buffer.record_prefetch(10).await;

        // 8 hits (had tasks in buffer)
        for _ in 0..8 {
            buffer.record_consume(false, None).await;
        }

        // 2 misses (had to wait)
        for _ in 0..2 {
            buffer.record_consume(true, None).await;
        }

        let stats = buffer.get_stats().await;
        assert_eq!(stats.total_consumed, 10);
        assert!((stats.hit_rate - 0.8).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_prefetch_buffer_reset_stats() {
        let config = PrefetchConfig::default();
        let buffer = PrefetchBuffer::new(config);

        buffer.record_prefetch(5).await;
        buffer
            .record_consume(false, Some(Duration::from_millis(100)))
            .await;

        let stats = buffer.get_stats().await;
        assert!(stats.total_prefetched > 0);
        assert!(stats.total_consumed > 0);

        buffer.reset_stats().await;

        let stats = buffer.get_stats().await;
        assert_eq!(stats.total_prefetched, 0);
        assert_eq!(stats.total_consumed, 0);
        // buffer_size should remain unchanged
        assert_eq!(stats.buffer_size, 4);
    }
}
