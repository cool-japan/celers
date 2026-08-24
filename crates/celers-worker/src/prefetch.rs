//! Task prefetching to reduce latency
//!
//! This module provides task prefetching capabilities to reduce latency by fetching
//! tasks from the broker before they're needed. This minimizes the time workers spend
//! waiting for tasks to be dequeued.
//!
//! # Features
//!
//! - **Real buffering**: [`PrefetchBuffer`] holds the actual prefetched
//!   [`BrokerMessage`]s (a FIFO queue), not just a count
//! - **Configurable prefetch count**: Control how many tasks to keep ready
//! - **Adaptive prefetching**: Automatically adjust prefetch behavior based on
//!   *recent* hit rate and processing speed (a sliding window, not a
//!   lifetime average that would go stale on a long-running worker)
//! - **Graceful shutdown**: [`PrefetchBuffer::drain`] hands back any
//!   still-buffered messages instead of silently discarding them
//! - **Batch integration**: Works seamlessly with batch dequeue
//!
//! # Example
//!
//! ```
//! use celers_worker::prefetch::{PrefetchConfig, PrefetchBuffer};
//! use celers_core::BrokerMessage;
//!
//! # async fn example(fetch_from_broker: impl Fn() -> Vec<BrokerMessage>) {
//! let config = PrefetchConfig {
//!     prefetch_count: 10,
//!     adaptive: true,
//!     min_buffer_size: 2,
//!     max_buffer_size: 50,
//! };
//!
//! let buffer = PrefetchBuffer::new(config);
//!
//! // Check if we should prefetch more tasks, and if so, make sure only
//! // one prefetch round-trip is in flight at a time.
//! if buffer.should_prefetch().await {
//!     if let Some(_permit) = buffer.try_acquire_prefetch_permit() {
//!         let messages = fetch_from_broker();
//!         buffer.push_batch(messages).await;
//!     }
//! }
//!
//! // Workers pop real messages from the buffer instead of hitting the
//! // broker directly.
//! if let Some(message) = buffer.pop().await {
//!     // process `message`...
//!     let _ = message;
//! }
//! # }
//! ```

use celers_core::BrokerMessage;
use std::collections::VecDeque;
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
    /// Lifetime prefetch hit rate (0.0-1.0). Note this is a *cumulative*
    /// figure for reporting purposes; the adaptive tuner itself decides
    /// on a short recent window instead (see module docs), so this value
    /// intentionally does not reflect what just drove a tuning decision.
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

    /// The actual buffered messages, oldest-first. This is the real
    /// backing store: `buffer_size` is always `queue.len()`, computed on
    /// demand rather than tracked by a separate counter that could drift
    /// out of sync with reality.
    queue: RwLock<VecDeque<BrokerMessage>>,

    /// Current prefetch count (adaptive)
    current_prefetch_count: AtomicUsize,

    /// Total tasks prefetched (lifetime)
    total_prefetched: AtomicUsize,

    /// Total tasks consumed from buffer (lifetime)
    total_consumed: AtomicUsize,

    /// Total tasks that had to wait (buffer was empty) (lifetime)
    total_misses: AtomicUsize,

    /// Tasks consumed *since the last adaptive adjustment*. Reset (via
    /// `swap(0, ..)`) every time `adjust_prefetch_count` actually runs, so
    /// the hit-rate decision always reflects recent behavior instead of
    /// being diluted by hours of prior history.
    window_consumed: AtomicUsize,

    /// Misses *since the last adaptive adjustment*; see `window_consumed`.
    window_misses: AtomicUsize,

    /// Recent processing times for adaptive adjustment
    processing_times: RwLock<Vec<Duration>>,

    /// Last adjustment time
    last_adjustment: RwLock<Instant>,

    /// Shutdown flag
    shutdown: AtomicBool,
}

/// Task prefetch buffer
///
/// Holds prefetched [`BrokerMessage`]s in a FIFO queue so workers can
/// [`pop`](Self::pop) a task immediately instead of waiting on the broker,
/// while [`push_batch`](Self::push_batch) is how prefetched messages enter
/// the buffer in the first place.
#[derive(Debug, Clone)]
pub struct PrefetchBuffer {
    state: Arc<PrefetchState>,
    /// Semaphore limiting concurrent *prefetch operations* (broker
    /// round-trips), acquired by the caller via
    /// [`try_acquire_prefetch_permit`](Self::try_acquire_prefetch_permit)
    /// around its own fetch-then-`push_batch` sequence -- see that
    /// method's docs.
    prefetch_semaphore: Arc<Semaphore>,
}

impl PrefetchBuffer {
    /// Create a new prefetch buffer
    pub fn new(config: PrefetchConfig) -> Self {
        let prefetch_count = config.prefetch_count;

        Self {
            state: Arc::new(PrefetchState {
                current_prefetch_count: AtomicUsize::new(prefetch_count),
                queue: RwLock::new(VecDeque::new()),
                total_prefetched: AtomicUsize::new(0),
                total_consumed: AtomicUsize::new(0),
                total_misses: AtomicUsize::new(0),
                window_consumed: AtomicUsize::new(0),
                window_misses: AtomicUsize::new(0),
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

        let buffer_size = self.state.queue.read().await.len();
        let prefetch_count = self.state.current_prefetch_count.load(Ordering::Relaxed);

        buffer_size < prefetch_count
    }

    /// Add prefetched messages to the buffer.
    ///
    /// This is how messages actually enter the buffer: [`pop`](Self::pop)
    /// hands back exactly what was pushed here, in FIFO order. A no-op
    /// for an empty batch.
    pub async fn push_batch(&self, messages: Vec<BrokerMessage>) {
        if messages.is_empty() {
            return;
        }

        let count = messages.len();
        let buffer_size = {
            let mut queue = self.state.queue.write().await;
            queue.extend(messages);
            queue.len()
        };

        self.state
            .total_prefetched
            .fetch_add(count, Ordering::Relaxed);

        trace!(
            "Prefetched {} tasks, buffer size now: {}",
            count,
            buffer_size
        );
    }

    /// Remove and return the next buffered message, if any.
    ///
    /// A `None` result is a genuine miss: the buffer was empty and the
    /// caller should fetch directly from the broker instead of blocking
    /// here. Hit/miss bookkeeping (including the adaptive-tuning window)
    /// happens automatically on every call; report how long a popped
    /// task took to actually process separately via
    /// [`record_processing_time`](Self::record_processing_time).
    pub async fn pop(&self) -> Option<BrokerMessage> {
        let message = {
            let mut queue = self.state.queue.write().await;
            queue.pop_front()
        };

        self.state.total_consumed.fetch_add(1, Ordering::Relaxed);
        self.state.window_consumed.fetch_add(1, Ordering::Relaxed);
        if message.is_none() {
            self.state.total_misses.fetch_add(1, Ordering::Relaxed);
            self.state.window_misses.fetch_add(1, Ordering::Relaxed);
        }

        if self.state.config.adaptive {
            self.adjust_prefetch_count().await;
        }

        message
    }

    /// Drain and return every currently-buffered message.
    ///
    /// Intended for graceful shutdown: a caller can hand these back to
    /// the broker (nack/requeue) instead of silently discarding
    /// already-prefetched work when the worker stops.
    pub async fn drain(&self) -> Vec<BrokerMessage> {
        let mut queue = self.state.queue.write().await;
        queue.drain(..).collect()
    }

    /// Record how long a popped task took to process, feeding the
    /// adaptive prefetch-count tuner. A no-op if `adaptive` is disabled.
    pub async fn record_processing_time(&self, processing_time: Duration) {
        if !self.state.config.adaptive {
            return;
        }

        let mut times = self.state.processing_times.write().await;
        times.push(processing_time);

        // Keep only recent samples (last 100)
        if times.len() > 100 {
            times.remove(0);
        }
    }

    /// Adjust prefetch count based on *recent* processing patterns
    /// (adaptive mode).
    ///
    /// Deliberately uses `window_consumed`/`window_misses` -- reset every
    /// time this runs -- rather than the lifetime `total_consumed`/
    /// `total_misses` counters. A lifetime ratio becomes essentially
    /// immovable after a long-running worker has accumulated a large
    /// history: a fresh burst of misses on a workload change would be
    /// diluted into insignificance and the buffer size would never
    /// respond. The windowed ratio always reflects only what happened
    /// since the previous adjustment.
    async fn adjust_prefetch_count(&self) {
        let mut last_adjustment = self.state.last_adjustment.write().await;
        let now = Instant::now();

        // Only adjust every 5 seconds
        if now.duration_since(*last_adjustment) < Duration::from_secs(5) {
            return;
        }

        *last_adjustment = now;
        drop(last_adjustment);

        let times = self.state.processing_times.read().await;
        if times.len() < 10 {
            return; // Not enough samples
        }

        // Calculate average processing time
        let avg_time = times.iter().sum::<Duration>() / times.len() as u32;
        drop(times);

        let current_count = self.state.current_prefetch_count.load(Ordering::Relaxed);

        // Consume (and reset) this adjustment's window in one atomic
        // step, so the very next pop starts accumulating a fresh window
        // rather than continuing to add to whatever was just evaluated.
        let window_consumed = self.state.window_consumed.swap(0, Ordering::Relaxed);
        let window_misses = self.state.window_misses.swap(0, Ordering::Relaxed);

        let hit_rate = if window_consumed > 0 {
            1.0 - (window_misses as f64 / window_consumed as f64)
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
                "Adjusting prefetch count: {} -> {} (recent_hit_rate={:.2}%, avg_time={:?})",
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
        let buffer_size = self.state.queue.read().await.len();
        let total_prefetched = self.state.total_prefetched.load(Ordering::Relaxed);
        let total_consumed = self.state.total_consumed.load(Ordering::Relaxed);
        let current_prefetch_count = self.state.current_prefetch_count.load(Ordering::Relaxed);
        let total_misses = self.state.total_misses.load(Ordering::Relaxed);

        let avg_processing_time_us = {
            let times = self.state.processing_times.read().await;
            if !times.is_empty() {
                let avg = times.iter().sum::<Duration>() / times.len() as u32;
                avg.as_micros() as u64
            } else {
                0
            }
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
        self.state.queue.read().await.len()
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

    /// Reset statistics (does not affect the buffered messages themselves)
    pub async fn reset_stats(&self) {
        self.state.total_prefetched.store(0, Ordering::Relaxed);
        self.state.total_consumed.store(0, Ordering::Relaxed);
        self.state.total_misses.store(0, Ordering::Relaxed);
        self.state.window_consumed.store(0, Ordering::Relaxed);
        self.state.window_misses.store(0, Ordering::Relaxed);

        let mut times = self.state.processing_times.write().await;
        times.clear();
        drop(times);

        let mut last_adjustment = self.state.last_adjustment.write().await;
        *last_adjustment = Instant::now();
    }

    /// Try to acquire the prefetch permit (non-blocking).
    ///
    /// Intended to serialize the *broker fetch* step across concurrent
    /// callers that might decide to prefetch at the same moment (e.g.
    /// several worker tasks each observing
    /// [`should_prefetch`](Self::should_prefetch) == `true` before any of
    /// them has actually fetched, which would otherwise over-fetch):
    /// acquire a permit, perform the broker round-trip, call
    /// [`push_batch`](Self::push_batch), then drop the permit. A `None`
    /// result means another prefetch is already in flight -- skip
    /// fetching this round rather than doubling up.
    pub fn try_acquire_prefetch_permit(&self) -> Option<tokio::sync::SemaphorePermit<'_>> {
        self.prefetch_semaphore.try_acquire().ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::SerializedTask;

    fn make_message(name: &str) -> BrokerMessage {
        BrokerMessage::new(SerializedTask::new(name.to_string(), Vec::new()))
    }

    fn make_messages(n: usize) -> Vec<BrokerMessage> {
        (0..n).map(|i| make_message(&format!("task-{i}"))).collect()
    }

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
        buffer.push_batch(make_messages(3)).await;
        assert!(buffer.should_prefetch().await);

        // Fill buffer
        buffer.push_batch(make_messages(2)).await;
        assert!(!buffer.should_prefetch().await);

        // Consume one task
        buffer.pop().await;
        assert!(buffer.should_prefetch().await);
    }

    #[tokio::test]
    async fn test_prefetch_buffer_record_operations() {
        let config = PrefetchConfig::default();
        let buffer = PrefetchBuffer::new(config);

        // Push a batch
        buffer.push_batch(make_messages(5)).await;
        let stats = buffer.get_stats().await;
        assert_eq!(stats.buffer_size, 5);
        assert_eq!(stats.total_prefetched, 5);
        assert_eq!(stats.total_consumed, 0);

        // Pop one
        let popped = buffer.pop().await;
        assert!(popped.is_some());
        buffer
            .record_processing_time(Duration::from_millis(100))
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

        buffer.push_batch(make_messages(10)).await;
        for ms in [50, 100, 75] {
            buffer.pop().await;
            buffer
                .record_processing_time(Duration::from_millis(ms))
                .await;
        }

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

        // Only 8 messages available...
        buffer.push_batch(make_messages(8)).await;

        // ...but 10 pops: the first 8 are real hits, the last 2 are real
        // misses (the buffer is genuinely empty by then), rather than the
        // caller asserting an arbitrary hit/miss flag independent of what
        // the buffer actually held.
        for _ in 0..10 {
            buffer.pop().await;
        }

        let stats = buffer.get_stats().await;
        assert_eq!(stats.total_consumed, 10);
        assert!((stats.hit_rate - 0.8).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_prefetch_buffer_reset_stats() {
        let config = PrefetchConfig::default();
        let buffer = PrefetchBuffer::new(config);

        buffer.push_batch(make_messages(5)).await;
        buffer.pop().await;
        buffer
            .record_processing_time(Duration::from_millis(100))
            .await;

        let stats = buffer.get_stats().await;
        assert!(stats.total_prefetched > 0);
        assert!(stats.total_consumed > 0);

        buffer.reset_stats().await;

        let stats = buffer.get_stats().await;
        assert_eq!(stats.total_prefetched, 0);
        assert_eq!(stats.total_consumed, 0);
        // buffer_size should remain unchanged (reset only clears
        // statistics, not the actual buffered messages)
        assert_eq!(stats.buffer_size, 4);
    }

    // --- Regression tests (idx 184) ----------------------------------------

    /// Regression test: `PrefetchBuffer` used to be a counter with a
    /// "Buffer" name -- no `BrokerMessage`s were ever stored, so `pop`
    /// (which didn't exist) could not have handed back real data. This
    /// proves messages actually round-trip through the buffer in FIFO
    /// order.
    #[tokio::test]
    async fn test_pop_returns_the_exact_messages_that_were_pushed_in_fifo_order() {
        let buffer = PrefetchBuffer::new(PrefetchConfig::default());

        let messages = vec![
            make_message("first"),
            make_message("second"),
            make_message("third"),
        ];
        buffer.push_batch(messages).await;

        let first = buffer.pop().await.expect("first message");
        let second = buffer.pop().await.expect("second message");
        let third = buffer.pop().await.expect("third message");

        assert_eq!(first.task.metadata.name, "first");
        assert_eq!(second.task.metadata.name, "second");
        assert_eq!(third.task.metadata.name, "third");
        assert!(buffer.pop().await.is_none());
    }

    #[tokio::test]
    async fn test_pop_on_empty_buffer_is_none_and_counts_as_a_miss() {
        let buffer = PrefetchBuffer::new(PrefetchConfig::default());

        assert!(buffer.pop().await.is_none());

        let stats = buffer.get_stats().await;
        assert_eq!(stats.total_consumed, 1);
        assert_eq!(stats.hit_rate, 0.0);
    }

    #[tokio::test]
    async fn test_drain_returns_and_empties_the_buffer() {
        let buffer = PrefetchBuffer::new(PrefetchConfig::default());
        buffer.push_batch(make_messages(3)).await;

        let drained = buffer.drain().await;
        assert_eq!(drained.len(), 3);
        assert_eq!(buffer.buffer_size().await, 0);
    }

    #[tokio::test]
    async fn test_prefetch_permit_is_exclusive_while_held() {
        let buffer = PrefetchBuffer::new(PrefetchConfig::default());

        let permit1 = buffer.try_acquire_prefetch_permit();
        assert!(permit1.is_some());

        // A second concurrent prefetch attempt must be told "one is
        // already in flight" rather than being allowed to double up.
        assert!(buffer.try_acquire_prefetch_permit().is_none());

        drop(permit1);

        // Released, so a subsequent attempt succeeds again.
        assert!(buffer.try_acquire_prefetch_permit().is_some());
    }

    /// The central regression test for the adaptive-tuning half of this
    /// fix: computing the hit rate from lifetime totals means a
    /// long-running worker's history makes the ratio essentially
    /// immovable, so a real change in workload (a burst of misses) would
    /// never be able to move it past the 0.9 threshold and the buffer
    /// size would freeze forever. The windowed computation must respond
    /// to the recent burst regardless of how large the lifetime totals
    /// already are.
    #[tokio::test]
    async fn test_adaptive_tuning_responds_to_recent_burst_not_diluted_by_lifetime_history() {
        let config = PrefetchConfig {
            prefetch_count: 5,
            adaptive: true,
            min_buffer_size: 2,
            max_buffer_size: 20,
        };
        let buffer = PrefetchBuffer::new(config);

        // A long, healthy lifetime history, as if this process had been
        // running for hours with an excellent hit rate. Under the old
        // lifetime-ratio implementation this alone would make the ratio
        // essentially immovable by a handful of new misses:
        // 1.0 - (50 + 8) / (100_000 + 9) is still ~99.9%.
        buffer
            .state
            .total_consumed
            .store(100_000, Ordering::Relaxed);
        buffer.state.total_misses.store(50, Ordering::Relaxed);

        // >=10 processing-time samples so adjust_prefetch_count's
        // sample-size guard passes once it actually runs.
        for _ in 0..10 {
            buffer
                .record_processing_time(Duration::from_millis(50))
                .await;
        }

        // A recent burst: 1 hit followed by 8 misses. All nine land
        // within the fresh buffer's initial 5s adjustment cooldown, so
        // they only accumulate into the *window* counters without
        // triggering an evaluation yet.
        buffer.push_batch(vec![make_message("only-one")]).await;
        for _ in 0..9 {
            buffer.pop().await;
        }

        // Force the cooldown to have elapsed; one more (also-a-miss) pop
        // triggers the evaluation against the accumulated window: 9
        // misses out of 10 window-consumes = 0.1 recent hit rate, far
        // below the 0.9 threshold.
        *buffer.state.last_adjustment.write().await = Instant::now() - Duration::from_secs(10);
        buffer.pop().await;

        let stats = buffer.get_stats().await;
        assert!(
            stats.current_prefetch_count > 5,
            "prefetch count should increase in response to a recent burst of misses \
             even after a long healthy lifetime history; got {}",
            stats.current_prefetch_count
        );
        // The *lifetime* ratio reported by get_stats() indeed stays
        // near-perfect, confirming it really would have masked the
        // recent burst had it been used to drive the tuning decision.
        assert!(stats.hit_rate > 0.99);
    }

    /// A stable, low-noise recent window (well above the 0.98 threshold,
    /// with fast processing) should still be able to *shrink* the buffer
    /// -- the windowed computation is not one-directional.
    #[tokio::test]
    async fn test_adaptive_tuning_shrinks_buffer_on_sustained_high_recent_hit_rate() {
        let config = PrefetchConfig {
            prefetch_count: 10,
            adaptive: true,
            min_buffer_size: 2,
            max_buffer_size: 20,
        };
        let buffer = PrefetchBuffer::new(config);

        for _ in 0..10 {
            buffer
                .record_processing_time(Duration::from_millis(10))
                .await;
        }

        // 20 hits, 0 misses this window.
        buffer.push_batch(make_messages(20)).await;
        for _ in 0..20 {
            buffer.pop().await;
        }

        *buffer.state.last_adjustment.write().await = Instant::now() - Duration::from_secs(10);
        buffer.push_batch(vec![make_message("one-more")]).await;
        buffer.pop().await;

        let stats = buffer.get_stats().await;
        assert!(
            stats.current_prefetch_count < 10,
            "prefetch count should shrink on a sustained high recent hit rate, got {}",
            stats.current_prefetch_count
        );
    }
}
