//! Task batching and coalescing for the worker dequeue/poll loop.
//!
//! Two complementary concerns are implemented here as **pure, deterministic
//! logic** (no clock in the coalescing/grouping decisions, no async in the core
//! types) so they can be exhaustively unit-tested, plus thin integration hooks:
//!
//! - **Batching**: accumulate up to `max_batch_size` dequeued items before they
//!   are handed to processing, flushing early when a max wait elapses. The
//!   size-based flush is clock-free; the wait-based flush is exposed via an
//!   explicit deadline check that the loop supplies the elapsed time to, so the
//!   accumulation logic itself stays deterministic.
//!
//! - **Coalescing**: within a batch window, items sharing a *coalescing key*
//!   are deduplicated — one representative is kept and the duplicates are
//!   dropped (or folded into the survivor). This collapses redundant work such
//!   as repeated "refresh user 42" tasks enqueued in a burst.
//!
//! The two are combined in [`BatchAccumulator`], which is generic over the item
//! type `T` and a key extractor, and in the [`coalesce`] free function for
//! one-shot use on an already-collected `Vec`.
//!
//! # Example
//!
//! ```
//! use celers_worker::batching::{BatchAccumulator, BatchConfig, CoalesceStrategy};
//!
//! // Batch of up to 3, coalescing on the integer key itself, keeping the first.
//! let config = BatchConfig {
//!     max_batch_size: 3,
//!     max_wait_ms: 100,
//!     coalesce: true,
//!     strategy: CoalesceStrategy::KeepFirst,
//! };
//! let mut acc = BatchAccumulator::new(config, |n: &u32| *n);
//!
//! assert!(acc.push(1).is_none()); // 1 distinct
//! assert!(acc.push(1).is_none()); // duplicate key -> dropped, still 1 distinct
//! assert!(acc.push(2).is_none()); // 2 distinct
//! // Third *distinct* key fills the batch and triggers a flush.
//! let batch = acc.push(3).expect("batch full");
//! assert_eq!(batch.items(), &[1, 2, 3]);
//! assert_eq!(batch.coalesced_count(), 1); // one duplicate was dropped
//! ```

use std::collections::HashMap;
use std::hash::Hash;
use std::time::Duration;

use celers_core::BrokerMessage;

/// What to keep when two items share a coalescing key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum CoalesceStrategy {
    /// Keep the first item seen for a key; drop later duplicates. Preserves the
    /// earliest-enqueued representative (FIFO-friendly).
    #[default]
    KeepFirst,
    /// Keep the most recent item seen for a key; the survivor takes the latest
    /// duplicate's position is *not* changed — the original slot is retained so
    /// batch ordering stays stable, but the stored value is replaced.
    KeepLast,
}

impl std::fmt::Display for CoalesceStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::KeepFirst => write!(f, "KeepFirst"),
            Self::KeepLast => write!(f, "KeepLast"),
        }
    }
}

/// Configuration for [`BatchAccumulator`] and the [`coalesce`] helper.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchConfig {
    /// Maximum number of *distinct* items in a batch before it flushes. Must be
    /// `>= 1`.
    pub max_batch_size: usize,

    /// Maximum time to wait accumulating a partial batch before flushing it, in
    /// milliseconds. `0` disables the wait-based flush (size only).
    pub max_wait_ms: u64,

    /// Whether to coalesce duplicate keys within the batch window.
    pub coalesce: bool,

    /// Which item to keep when coalescing a duplicate key.
    pub strategy: CoalesceStrategy,
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self {
            max_batch_size: 16,
            max_wait_ms: 50,
            coalesce: true,
            strategy: CoalesceStrategy::KeepFirst,
        }
    }
}

impl BatchConfig {
    /// Validate the configuration.
    ///
    /// # Errors
    ///
    /// Returns `Err` if `max_batch_size` is zero.
    pub fn validate(&self) -> Result<(), String> {
        if self.max_batch_size == 0 {
            return Err("max_batch_size must be greater than 0".to_string());
        }
        Ok(())
    }

    /// Maximum wait as a [`Duration`] (`None` when wait-based flush is disabled).
    #[inline]
    #[must_use]
    pub const fn max_wait(&self) -> Option<Duration> {
        if self.max_wait_ms == 0 {
            None
        } else {
            Some(Duration::from_millis(self.max_wait_ms))
        }
    }
}

/// A finalized batch produced by [`BatchAccumulator`] / [`coalesce`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch<T> {
    items: Vec<T>,
    coalesced_count: usize,
}

impl<T> Batch<T> {
    /// Construct a batch from already-deduplicated items and the count of
    /// duplicates that were coalesced away while building it.
    #[must_use]
    pub fn new(items: Vec<T>, coalesced_count: usize) -> Self {
        Self {
            items,
            coalesced_count,
        }
    }

    /// The surviving items, in insertion order of their (first-seen) key.
    #[inline]
    #[must_use]
    pub fn items(&self) -> &[T] {
        &self.items
    }

    /// Number of distinct items retained.
    #[inline]
    #[must_use]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Whether the batch retained no items.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Number of duplicate items dropped/merged via coalescing.
    #[inline]
    #[must_use]
    pub const fn coalesced_count(&self) -> usize {
        self.coalesced_count
    }

    /// Total items pushed into this batch before coalescing
    /// (`len() + coalesced_count()`).
    #[inline]
    #[must_use]
    pub fn raw_count(&self) -> usize {
        self.items.len() + self.coalesced_count
    }

    /// Consume the batch, yielding the surviving items.
    #[inline]
    #[must_use]
    pub fn into_items(self) -> Vec<T> {
        self.items
    }
}

/// Why a batch flush occurred.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FlushReason {
    /// The batch reached `max_batch_size` distinct items.
    Size,
    /// The accumulation window (`max_wait_ms`) elapsed.
    Deadline,
    /// An explicit/forced flush (e.g. shutdown or drain).
    Forced,
}

impl std::fmt::Display for FlushReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Size => write!(f, "Size"),
            Self::Deadline => write!(f, "Deadline"),
            Self::Forced => write!(f, "Forced"),
        }
    }
}

/// Deterministic batching + coalescing accumulator.
///
/// Generic over the item type `T` and a key extractor `F: Fn(&T) -> K`. Push
/// items one at a time with [`BatchAccumulator::push`]; when the batch reaches
/// `max_batch_size` *distinct* keys it is returned automatically. Otherwise the
/// caller drains it with [`BatchAccumulator::flush`] (e.g. when its
/// [`BatchAccumulator::deadline_reached`] check, fed the elapsed time, returns
/// true, or on shutdown).
///
/// Coalescing is order-preserving: the position of a key in the output is fixed
/// by where it was *first* seen, regardless of strategy.
pub struct BatchAccumulator<T, K, F>
where
    K: Eq + Hash + Clone,
    F: Fn(&T) -> K,
{
    config: BatchConfig,
    key_of: F,
    /// Surviving items in first-seen order.
    items: Vec<T>,
    /// Map from coalescing key to the index in `items` of its survivor.
    index: HashMap<K, usize>,
    /// Distinct keys pushed since the last flush that were dropped/merged.
    coalesced_in_window: usize,
    /// Cumulative duplicates coalesced across the accumulator's lifetime.
    total_coalesced: u64,
    /// Cumulative items pushed across the accumulator's lifetime.
    total_pushed: u64,
    /// Cumulative batches emitted across the accumulator's lifetime.
    total_batches: u64,
}

impl<T, K, F> BatchAccumulator<T, K, F>
where
    K: Eq + Hash + Clone,
    F: Fn(&T) -> K,
{
    /// Create a new accumulator. The supplied `config` is sanitised so a zero
    /// `max_batch_size` is treated as `1` (construction cannot panic).
    pub fn new(mut config: BatchConfig, key_of: F) -> Self {
        if config.max_batch_size == 0 {
            config.max_batch_size = 1;
        }
        Self {
            config,
            key_of,
            items: Vec::new(),
            index: HashMap::new(),
            coalesced_in_window: 0,
            total_coalesced: 0,
            total_pushed: 0,
            total_batches: 0,
        }
    }

    /// Push one item into the current batch window.
    ///
    /// Returns `Some(batch)` when this push fills the batch to `max_batch_size`
    /// distinct keys (a [`FlushReason::Size`] flush); otherwise `None`. When
    /// coalescing is enabled and the item's key is already present, the item is
    /// dropped or merged per [`CoalesceStrategy`] and never counts toward the
    /// size limit.
    pub fn push(&mut self, item: T) -> Option<Batch<T>> {
        self.total_pushed = self.total_pushed.saturating_add(1);

        if self.config.coalesce {
            let key = (self.key_of)(&item);
            if let Some(&existing_idx) = self.index.get(&key) {
                // Duplicate within the window: coalesce.
                self.coalesced_in_window = self.coalesced_in_window.saturating_add(1);
                self.total_coalesced = self.total_coalesced.saturating_add(1);
                if self.config.strategy == CoalesceStrategy::KeepLast {
                    // Replace the survivor's value in place (position unchanged).
                    self.items[existing_idx] = item;
                }
                return None;
            }
            self.index.insert(key, self.items.len());
        }

        self.items.push(item);

        if self.items.len() >= self.config.max_batch_size {
            return Some(self.take_batch());
        }
        None
    }

    /// Whether the accumulation window deadline has been reached, given the time
    /// elapsed since the window's first item.
    ///
    /// Deterministic: the caller owns the clock and passes `elapsed` in. Returns
    /// `false` when the batch is empty or when wait-based flushing is disabled
    /// (`max_wait_ms == 0`).
    #[must_use]
    pub fn deadline_reached(&self, elapsed: Duration) -> bool {
        if self.items.is_empty() {
            return false;
        }
        match self.config.max_wait() {
            Some(max) => elapsed >= max,
            None => false,
        }
    }

    /// Flush the current window, returning the batch if non-empty.
    ///
    /// Use for deadline- or shutdown-driven flushes; size-driven flushes happen
    /// automatically inside [`BatchAccumulator::push`]. Returns `None` if there
    /// is nothing buffered.
    pub fn flush(&mut self) -> Option<Batch<T>> {
        if self.items.is_empty() {
            return None;
        }
        Some(self.take_batch())
    }

    /// Finalize and reset the current window into a [`Batch`].
    fn take_batch(&mut self) -> Batch<T> {
        let items = std::mem::take(&mut self.items);
        self.index.clear();
        let coalesced = self.coalesced_in_window;
        self.coalesced_in_window = 0;
        self.total_batches = self.total_batches.saturating_add(1);
        Batch::new(items, coalesced)
    }

    /// Number of distinct items currently buffered (not yet flushed).
    #[inline]
    #[must_use]
    pub fn pending(&self) -> usize {
        self.items.len()
    }

    /// Whether anything is currently buffered.
    #[inline]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Duplicates coalesced in the *current* (not-yet-flushed) window.
    #[inline]
    #[must_use]
    pub const fn coalesced_in_window(&self) -> usize {
        self.coalesced_in_window
    }

    /// The accumulator's configuration.
    #[inline]
    #[must_use]
    pub const fn config(&self) -> &BatchConfig {
        &self.config
    }

    /// Snapshot of lifetime counters for observability.
    #[must_use]
    pub const fn stats(&self) -> BatchStats {
        BatchStats {
            pending: self.items.len(),
            total_pushed: self.total_pushed,
            total_coalesced: self.total_coalesced,
            total_batches: self.total_batches,
        }
    }
}

/// Lifetime statistics for a [`BatchAccumulator`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BatchStats {
    /// Distinct items currently buffered.
    pub pending: usize,
    /// Total items pushed over the accumulator's lifetime.
    pub total_pushed: u64,
    /// Total duplicates coalesced over the accumulator's lifetime.
    pub total_coalesced: u64,
    /// Total batches emitted over the accumulator's lifetime.
    pub total_batches: u64,
}

impl std::fmt::Display for BatchStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "BatchStats(pending={}, pushed={}, coalesced={}, batches={})",
            self.pending, self.total_pushed, self.total_coalesced, self.total_batches
        )
    }
}

/// One-shot coalescing of an already-collected `Vec`.
///
/// Deduplicates `items` by `key_of`, keeping one representative per key per
/// `strategy`, preserving first-seen order. Returns the survivors paired with
/// the number of duplicates removed. This is the pure building block that the
/// stateful [`BatchAccumulator`] reuses conceptually; it is handy for coalescing
/// a `dequeue_batch` result in one call.
pub fn coalesce<T, K, F>(items: Vec<T>, strategy: CoalesceStrategy, key_of: F) -> (Vec<T>, usize)
where
    K: Eq + Hash + Clone,
    F: Fn(&T) -> K,
{
    let mut survivors: Vec<T> = Vec::with_capacity(items.len());
    let mut index: HashMap<K, usize> = HashMap::with_capacity(items.len());
    let mut coalesced = 0usize;

    for item in items {
        let key = key_of(&item);
        if let Some(&existing_idx) = index.get(&key) {
            coalesced += 1;
            if strategy == CoalesceStrategy::KeepLast {
                survivors[existing_idx] = item;
            }
            // KeepFirst: simply drop the duplicate.
        } else {
            index.insert(key, survivors.len());
            survivors.push(item);
        }
    }

    (survivors, coalesced)
}

/// Derive a stable coalescing key for a [`BrokerMessage`].
///
/// The key is the task *name* combined with a deterministic hash of the
/// serialized payload, so two messages that would execute identically (same task
/// type and arguments) coalesce, while distinct work does not. The task `id`
/// (which is unique per enqueue) is intentionally excluded.
#[must_use]
pub fn broker_message_coalesce_key(msg: &BrokerMessage) -> (String, u64) {
    use std::hash::Hasher;
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    hasher.write(&msg.task.payload);
    (msg.task.metadata.name.clone(), hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg(max: usize) -> BatchConfig {
        BatchConfig {
            max_batch_size: max,
            max_wait_ms: 100,
            coalesce: true,
            strategy: CoalesceStrategy::KeepFirst,
        }
    }

    #[test]
    fn test_config_validate() {
        assert!(BatchConfig::default().validate().is_ok());
        let bad = BatchConfig {
            max_batch_size: 0,
            ..Default::default()
        };
        assert!(bad.validate().is_err());
    }

    #[test]
    fn test_config_max_wait() {
        assert_eq!(
            BatchConfig {
                max_wait_ms: 100,
                ..Default::default()
            }
            .max_wait(),
            Some(Duration::from_millis(100))
        );
        assert_eq!(
            BatchConfig {
                max_wait_ms: 0,
                ..Default::default()
            }
            .max_wait(),
            None
        );
    }

    #[test]
    fn test_batch_groups_exactly_n() {
        // No coalescing: a batch flushes at exactly max_batch_size pushes.
        let mut acc = BatchAccumulator::new(
            BatchConfig {
                max_batch_size: 3,
                coalesce: false,
                ..cfg(3)
            },
            |n: &u32| *n,
        );
        assert!(acc.push(10).is_none());
        assert!(acc.push(11).is_none());
        let batch = acc.push(12).expect("flush at 3");
        assert_eq!(batch.len(), 3);
        assert_eq!(batch.items(), &[10, 11, 12]);
        assert_eq!(batch.coalesced_count(), 0);
        // Accumulator is empty after the flush; the cycle repeats.
        assert!(acc.is_empty());
        assert!(acc.push(13).is_none());
        assert!(acc.push(14).is_none());
        assert!(acc.push(15).is_some());
    }

    #[test]
    fn test_coalesce_drops_duplicate_keys_keeping_one() {
        let mut acc = BatchAccumulator::new(cfg(3), |n: &u32| *n);
        assert!(acc.push(1).is_none()); // distinct: [1]
        assert!(acc.push(1).is_none()); // dup -> dropped, still [1]
        assert!(acc.push(1).is_none()); // dup -> dropped, still [1]
        assert_eq!(acc.pending(), 1);
        assert_eq!(acc.coalesced_in_window(), 2);
        assert!(acc.push(2).is_none()); // distinct: [1, 2]
                                        // Third *distinct* key fills the batch.
        let batch = acc.push(3).expect("flush at 3 distinct");
        assert_eq!(batch.items(), &[1, 2, 3]);
        assert_eq!(batch.coalesced_count(), 2);
        assert_eq!(batch.raw_count(), 5); // 3 kept + 2 dropped
    }

    #[test]
    fn test_coalesce_keep_first_vs_keep_last() {
        // Items carry a payload; key is the first tuple element.
        let items = vec![(1u32, 'a'), (2, 'b'), (1, 'c'), (2, 'd'), (1, 'e')];

        let (first, dropped) = coalesce(
            items.clone(),
            CoalesceStrategy::KeepFirst,
            |t: &(u32, char)| t.0,
        );
        assert_eq!(first, vec![(1, 'a'), (2, 'b')]);
        assert_eq!(dropped, 3);

        let (last, dropped) = coalesce(items, CoalesceStrategy::KeepLast, |t: &(u32, char)| t.0);
        // Order preserved (1 before 2), but values are the latest seen.
        assert_eq!(last, vec![(1, 'e'), (2, 'd')]);
        assert_eq!(dropped, 3);
    }

    #[test]
    fn test_keep_last_preserves_position_in_accumulator() {
        let mut acc = BatchAccumulator::new(
            BatchConfig {
                max_batch_size: 10,
                strategy: CoalesceStrategy::KeepLast,
                ..cfg(10)
            },
            |t: &(u32, char)| t.0,
        );
        acc.push((1, 'a'));
        acc.push((2, 'b'));
        acc.push((1, 'z')); // updates key 1 in place
        let batch = acc.flush().expect("non-empty");
        assert_eq!(batch.items(), &[(1, 'z'), (2, 'b')]);
        assert_eq!(batch.coalesced_count(), 1);
    }

    #[test]
    fn test_deadline_based_flush() {
        let mut acc = BatchAccumulator::new(cfg(100), |n: &u32| *n);
        // Empty: never past deadline.
        assert!(!acc.deadline_reached(Duration::from_secs(10)));
        acc.push(1);
        acc.push(2);
        // Below the 100ms window.
        assert!(!acc.deadline_reached(Duration::from_millis(50)));
        // At/over the window.
        assert!(acc.deadline_reached(Duration::from_millis(100)));
        assert!(acc.deadline_reached(Duration::from_millis(150)));
        let batch = acc.flush().expect("flush partial on deadline");
        assert_eq!(batch.len(), 2);
        assert!(acc.is_empty());
    }

    #[test]
    fn test_max_wait_zero_disables_deadline() {
        let mut acc = BatchAccumulator::new(
            BatchConfig {
                max_wait_ms: 0,
                ..cfg(100)
            },
            |n: &u32| *n,
        );
        acc.push(1);
        assert!(!acc.deadline_reached(Duration::from_secs(3600)));
    }

    #[test]
    fn test_flush_empty_returns_none() {
        let mut acc: BatchAccumulator<u32, u32, _> = BatchAccumulator::new(cfg(5), |n: &u32| *n);
        assert!(acc.flush().is_none());
    }

    #[test]
    fn test_coalesce_disabled_keeps_duplicates() {
        let mut acc = BatchAccumulator::new(
            BatchConfig {
                max_batch_size: 4,
                coalesce: false,
                ..cfg(4)
            },
            |n: &u32| *n,
        );
        acc.push(7);
        acc.push(7);
        acc.push(7);
        let batch = acc.push(7).expect("flush at 4");
        assert_eq!(batch.items(), &[7, 7, 7, 7]);
        assert_eq!(batch.coalesced_count(), 0);
    }

    #[test]
    fn test_stats_and_reset_across_batches() {
        let mut acc = BatchAccumulator::new(cfg(2), |n: &u32| *n);
        acc.push(1);
        acc.push(1); // coalesced
        let b1 = acc.push(2).expect("flush at 2 distinct");
        assert_eq!(b1.coalesced_count(), 1);

        // Second window starts clean.
        assert_eq!(acc.coalesced_in_window(), 0);
        acc.push(3);
        let s = acc.stats();
        assert_eq!(s.pending, 1);
        assert_eq!(s.total_pushed, 4);
        assert_eq!(s.total_coalesced, 1);
        assert_eq!(s.total_batches, 1);
        assert!(!s.to_string().is_empty());
    }

    #[test]
    fn test_one_shot_coalesce_empty() {
        let (out, dropped): (Vec<u32>, usize) =
            coalesce(Vec::new(), CoalesceStrategy::KeepFirst, |n: &u32| *n);
        assert!(out.is_empty());
        assert_eq!(dropped, 0);
    }

    #[test]
    fn test_strategy_display() {
        assert_eq!(CoalesceStrategy::KeepFirst.to_string(), "KeepFirst");
        assert_eq!(CoalesceStrategy::KeepLast.to_string(), "KeepLast");
        assert_eq!(CoalesceStrategy::default(), CoalesceStrategy::KeepFirst);
    }

    #[test]
    fn test_flush_reason_display() {
        assert_eq!(FlushReason::Size.to_string(), "Size");
        assert_eq!(FlushReason::Deadline.to_string(), "Deadline");
        assert_eq!(FlushReason::Forced.to_string(), "Forced");
    }

    #[test]
    fn test_zero_batch_size_sanitized_to_one() {
        let mut acc = BatchAccumulator::new(
            BatchConfig {
                max_batch_size: 0,
                ..cfg(0)
            },
            |n: &u32| *n,
        );
        // Sanitised to 1 -> every distinct push flushes immediately.
        let batch = acc.push(5).expect("flush at 1");
        assert_eq!(batch.items(), &[5]);
    }

    #[test]
    fn test_broker_message_coalesce_key() {
        use celers_core::{BrokerMessage, SerializedTask};

        let a = BrokerMessage::new(SerializedTask::new("send_email".to_string(), vec![1, 2, 3]));
        let b = BrokerMessage::new(SerializedTask::new("send_email".to_string(), vec![1, 2, 3]));
        let c = BrokerMessage::new(SerializedTask::new("send_email".to_string(), vec![9, 9, 9]));
        let d = BrokerMessage::new(SerializedTask::new("other_task".to_string(), vec![1, 2, 3]));

        // Same name + same payload coalesce (ids differ but are excluded).
        assert_eq!(
            broker_message_coalesce_key(&a),
            broker_message_coalesce_key(&b)
        );
        // Different payload does not coalesce.
        assert_ne!(
            broker_message_coalesce_key(&a),
            broker_message_coalesce_key(&c)
        );
        // Different task name does not coalesce.
        assert_ne!(
            broker_message_coalesce_key(&a),
            broker_message_coalesce_key(&d)
        );
    }

    #[test]
    fn test_broker_messages_coalesce_in_accumulator() {
        use celers_core::{BrokerMessage, SerializedTask};

        let mut acc = BatchAccumulator::new(cfg(10), broker_message_coalesce_key);
        acc.push(BrokerMessage::new(SerializedTask::new(
            "refresh".to_string(),
            vec![42],
        )));
        acc.push(BrokerMessage::new(SerializedTask::new(
            "refresh".to_string(),
            vec![42],
        )));
        acc.push(BrokerMessage::new(SerializedTask::new(
            "refresh".to_string(),
            vec![43],
        )));
        let batch = acc.flush().expect("non-empty");
        // Two distinct (payload 42, payload 43); one duplicate coalesced.
        assert_eq!(batch.len(), 2);
        assert_eq!(batch.coalesced_count(), 1);
    }
}
