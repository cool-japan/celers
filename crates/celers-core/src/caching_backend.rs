//! A bounded, in-memory LRU caching layer for any result backend.
//!
//! [`CachingResultBackend`] wraps any inner [`ResultStore`]
//! and places a bounded, native least-recently-used (LRU) cache in front of it.
//! Read operations ([`get_result`](crate::ResultStore::get_result),
//! [`get_state`](crate::ResultStore::get_state),
//! [`has_result`](crate::ResultStore::has_result)) are served from the cache on
//! a hit, avoiding a round-trip to the (potentially remote) inner backend.
//!
//! Key properties:
//!
//! * **Terminal results only** — only immutable, terminal values
//!   ([`Success`](crate::TaskResultValue::Success),
//!   [`Failure`](crate::TaskResultValue::Failure),
//!   [`Revoked`](crate::TaskResultValue::Revoked),
//!   [`Rejected`](crate::TaskResultValue::Rejected)) are cached. Transient
//!   states (`Pending`, `Received`, `Started`, `Retry`) are guaranteed to change
//!   and are therefore always read from the inner backend.
//! * **Bounded capacity** — at most `capacity` entries are cached; inserting
//!   beyond capacity evicts the least-recently-used entry.
//! * **Per-entry TTL** — cached entries are treated as a miss (and refreshed
//!   from the inner backend) once they expire. Because write-through
//!   invalidation is *instance-local*, a deployment whose writer is another
//!   process (the usual worker/client split) relies on the TTL for freshness,
//!   so [`CachingResultBackend::new`] installs
//!   [`DEFAULT_TTL`] rather than caching forever.
//! * **Separate, short negative cache** — "no result yet" is remembered in its
//!   own map with its own short TTL ([`DEFAULT_NEGATIVE_TTL`]), so polling for a
//!   task that has not finished yet cannot pin a stale "absent" answer.
//! * **Write-through + invalidation** — [`store_result`](crate::ResultStore::store_result) writes through to the
//!   inner backend and refreshes the cache; [`forget`](crate::ResultStore::forget) removes the entry from
//!   both the cache and the inner backend.
//!
//! The LRU itself is implemented natively (an intrusive doubly-linked list over
//! a slab of nodes plus a `HashMap` index) so no additional dependency is
//! required, and all operations are O(1).
//!
//! # Example
//!
//! ```
//! use celers_core::{
//!     CachingResultBackend, InMemoryResultBackend, ResultStore, TaskResultValue,
//! };
//! use std::time::Duration;
//! use uuid::Uuid;
//!
//! # async fn example() -> celers_core::Result<()> {
//! let inner = InMemoryResultBackend::new();
//! let cached = CachingResultBackend::with_ttl(inner, 128, Duration::from_secs(60));
//!
//! let id = Uuid::new_v4();
//! cached
//!     .store_result(id, TaskResultValue::Success(serde_json::json!(1)))
//!     .await?;
//!
//! // Served from the cache (no inner-backend read).
//! assert!(cached.has_result(id).await?);
//! assert_eq!(cached.cache_hits(), 1);
//! # Ok(())
//! # }
//! # tokio::runtime::Builder::new_current_thread()
//! #     .enable_all()
//! #     .build()
//! #     .unwrap()
//! #     .block_on(example())
//! #     .unwrap();
//! ```

use crate::result::{ResultStore, TaskResultValue};
use crate::state::TaskState;
use crate::{Result, TaskId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Sentinel index meaning "no node" within the intrusive linked list.
const NIL: usize = usize::MAX;

/// Default time-to-live applied by [`CachingResultBackend::new`].
///
/// Cached terminal results are immutable, but a result can still be *forgotten*
/// (expired, tombstoned) by another process, so entries are not kept forever by
/// default.
pub const DEFAULT_TTL: Duration = Duration::from_secs(60);

/// Default time-to-live for negative ("no result stored") entries.
///
/// Deliberately short: the writer is typically a worker in another process, so a
/// missing result is expected to appear at any moment.
pub const DEFAULT_NEGATIVE_TTL: Duration = Duration::from_secs(1);

/// One slot of the intrusive doubly-linked LRU list.
#[derive(Debug)]
struct LruNode<V> {
    /// The task ID this node caches (only meaningful when occupied).
    key: TaskId,
    /// The cached value for `key`.
    value: V,
    /// When this entry was inserted, used to enforce the TTL.
    inserted_at: Instant,
    /// Previous node towards the most-recently-used end (or [`NIL`]).
    prev: usize,
    /// Next node towards the least-recently-used end (or [`NIL`]).
    next: usize,
}

/// A native, fixed-capacity LRU map keyed by [`TaskId`].
///
/// Implemented as a slab of [`LruNode`]s threaded into a doubly-linked list with
/// a `HashMap` from key to slab index. All operations are amortised O(1). When
/// at capacity, inserting a new key evicts the least-recently-used entry.
#[derive(Debug)]
struct LruCache<V> {
    /// Maximum number of live entries.
    capacity: usize,
    /// Optional per-entry time-to-live.
    ttl: Option<Duration>,
    /// Backing storage for nodes. Indices are stable for a node's lifetime.
    nodes: Vec<LruNode<V>>,
    /// Map from task ID to its index in `nodes`.
    index: HashMap<TaskId, usize>,
    /// Index of the most-recently-used node, or [`NIL`].
    head: usize,
    /// Index of the least-recently-used node, or [`NIL`].
    tail: usize,
    /// Free-list of reusable node indices.
    free: Vec<usize>,
}

impl<V: Clone> LruCache<V> {
    /// Create a new cache with the given capacity (clamped to at least 1) and
    /// optional TTL.
    fn new(capacity: usize, ttl: Option<Duration>) -> Self {
        let capacity = capacity.max(1);
        Self {
            capacity,
            ttl,
            nodes: Vec::with_capacity(capacity),
            index: HashMap::with_capacity(capacity),
            head: NIL,
            tail: NIL,
            free: Vec::new(),
        }
    }

    /// Number of live entries currently held.
    fn len(&self) -> usize {
        self.index.len()
    }

    /// Detach a node from the linked list (without removing it from the index).
    fn unlink(&mut self, idx: usize) {
        let (prev, next) = {
            let node = &self.nodes[idx];
            (node.prev, node.next)
        };
        if prev != NIL {
            self.nodes[prev].next = next;
        } else {
            self.head = next;
        }
        if next != NIL {
            self.nodes[next].prev = prev;
        } else {
            self.tail = prev;
        }
        self.nodes[idx].prev = NIL;
        self.nodes[idx].next = NIL;
    }

    /// Push an already-stored node to the most-recently-used (head) position.
    fn push_front(&mut self, idx: usize) {
        let old_head = self.head;
        self.nodes[idx].prev = NIL;
        self.nodes[idx].next = old_head;
        if old_head != NIL {
            self.nodes[old_head].prev = idx;
        }
        self.head = idx;
        if self.tail == NIL {
            self.tail = idx;
        }
    }

    /// Move an existing node to the front (mark as most-recently-used).
    fn touch(&mut self, idx: usize) {
        if self.head == idx {
            return;
        }
        self.unlink(idx);
        self.push_front(idx);
    }

    /// Evict the least-recently-used entry, if any.
    fn evict_lru(&mut self) {
        let lru = self.tail;
        if lru == NIL {
            return;
        }
        self.unlink(lru);
        let key = self.nodes[lru].key;
        self.index.remove(&key);
        self.free.push(lru);
    }

    /// Return `true` if the entry at `idx` has outlived the configured TTL.
    fn is_expired(&self, idx: usize) -> bool {
        match self.ttl {
            Some(ttl) => self.nodes[idx].inserted_at.elapsed() >= ttl,
            None => false,
        }
    }

    /// Look up a key. On a live (non-expired) hit, the entry is promoted to
    /// most-recently-used and a clone of its value returned. On an expired hit
    /// the entry is removed and `None` returned (treated as a miss).
    fn get(&mut self, key: &TaskId) -> Option<V> {
        let idx = *self.index.get(key)?;
        if self.is_expired(idx) {
            // Expired: drop it and report a miss.
            self.unlink(idx);
            self.index.remove(key);
            self.free.push(idx);
            return None;
        }
        self.touch(idx);
        Some(self.nodes[idx].value.clone())
    }

    /// Insert or overwrite an entry, promoting it to most-recently-used and
    /// evicting the LRU entry if inserting a new key would exceed capacity.
    fn put(&mut self, key: TaskId, value: V) {
        let inserted_at = Instant::now();
        if let Some(&idx) = self.index.get(&key) {
            self.nodes[idx].value = value;
            self.nodes[idx].inserted_at = inserted_at;
            self.touch(idx);
            return;
        }

        if self.len() >= self.capacity {
            self.evict_lru();
        }

        let idx = if let Some(reused) = self.free.pop() {
            self.nodes[reused] = LruNode {
                key,
                value,
                inserted_at,
                prev: NIL,
                next: NIL,
            };
            reused
        } else {
            self.nodes.push(LruNode {
                key,
                value,
                inserted_at,
                prev: NIL,
                next: NIL,
            });
            self.nodes.len() - 1
        };
        self.index.insert(key, idx);
        self.push_front(idx);
    }

    /// Remove an entry by key, if present.
    fn remove(&mut self, key: &TaskId) {
        if let Some(idx) = self.index.remove(key) {
            self.unlink(idx);
            self.free.push(idx);
        }
    }

    /// Drop every cached entry.
    fn clear(&mut self) {
        self.nodes.clear();
        self.index.clear();
        self.free.clear();
        self.head = NIL;
        self.tail = NIL;
    }
}

/// A result backend that fronts an inner [`ResultStore`] with a bounded LRU
/// cache.
///
/// See the [module documentation](self) for behavioural details. The wrapper is
/// generic over the inner backend `B` and works with any implementation,
/// including remote backends (Redis, databases) and the crate's own
/// [`InMemoryResultBackend`](crate::InMemoryResultBackend).
#[derive(Debug)]
pub struct CachingResultBackend<B: ResultStore> {
    /// The wrapped backend that holds the authoritative results.
    inner: B,
    /// The native LRU cache of *terminal* results.
    cache: Mutex<LruCache<TaskResultValue>>,
    /// Separate, short-lived cache of task IDs the inner backend reported as
    /// having no stored result.
    negative: Mutex<LruCache<()>>,
    /// Configured maximum number of entries per cache.
    capacity: usize,
    /// Whether negative caching is enabled at all.
    negative_enabled: bool,
    /// Number of cache hits observed (diagnostics).
    hits: AtomicU64,
    /// Number of cache misses observed (diagnostics).
    misses: AtomicU64,
}

impl<B: ResultStore> CachingResultBackend<B> {
    /// Wrap `inner` with a cache of the given `capacity` and the default TTL
    /// ([`DEFAULT_TTL`]).
    ///
    /// A TTL is applied by design: write-through invalidation only affects
    /// *this* wrapper instance, so when the writer lives in another process
    /// (worker) the TTL is what bounds staleness. Use
    /// [`CachingResultBackend::with_ttl`] to choose a different bound.
    ///
    /// The capacity is clamped to a minimum of 1.
    #[must_use]
    pub fn new(inner: B, capacity: usize) -> Self {
        Self::build(inner, capacity, Some(DEFAULT_TTL))
    }

    /// Wrap `inner` with a cache of the given `capacity` and a per-entry `ttl`.
    ///
    /// Cached entries older than `ttl` are treated as a miss and transparently
    /// refreshed from the inner backend on the next access.
    ///
    /// The capacity is clamped to a minimum of 1.
    #[must_use]
    pub fn with_ttl(inner: B, capacity: usize, ttl: Duration) -> Self {
        Self::build(inner, capacity, Some(ttl))
    }

    /// Override the TTL used for negative ("no result stored") entries.
    ///
    /// Defaults to [`DEFAULT_NEGATIVE_TTL`]. Keep it short: a task that has not
    /// finished yet may produce a result at any moment.
    #[must_use]
    pub fn with_negative_ttl(self, ttl: Duration) -> Self {
        let capacity = self.capacity;
        Self {
            negative: Mutex::new(LruCache::new(capacity, Some(ttl))),
            negative_enabled: true,
            ..self
        }
    }

    /// Disable negative caching entirely: a task with no stored result always
    /// consults the inner backend.
    #[must_use]
    pub fn without_negative_caching(self) -> Self {
        Self {
            negative_enabled: false,
            ..self
        }
    }

    /// Shared constructor.
    fn build(inner: B, capacity: usize, ttl: Option<Duration>) -> Self {
        let capacity = capacity.max(1);
        Self {
            inner,
            cache: Mutex::new(LruCache::new(capacity, ttl)),
            negative: Mutex::new(LruCache::new(capacity, Some(DEFAULT_NEGATIVE_TTL))),
            capacity,
            negative_enabled: true,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        }
    }

    /// Record a freshly-read value in the appropriate cache.
    ///
    /// Only immutable terminal values are cached; transient states are never
    /// cached because they are guaranteed to change. An absent result is
    /// recorded in the separate, short-TTL negative cache.
    async fn populate(&self, task_id: TaskId, value: Option<&TaskResultValue>) {
        match value {
            Some(v) if v.is_terminal() => {
                self.cache.lock().await.put(task_id, v.clone());
                if self.negative_enabled {
                    self.negative.lock().await.remove(&task_id);
                }
            }
            Some(_) => {
                // Transient state: make sure no stale entry survives.
                self.cache.lock().await.remove(&task_id);
                if self.negative_enabled {
                    self.negative.lock().await.remove(&task_id);
                }
            }
            None => {
                self.cache.lock().await.remove(&task_id);
                if self.negative_enabled {
                    self.negative.lock().await.put(task_id, ());
                }
            }
        }
    }

    /// Look up a live negative-cache entry.
    async fn negative_hit(&self, task_id: TaskId) -> bool {
        self.negative_enabled && self.negative.lock().await.get(&task_id).is_some()
    }

    /// Borrow the wrapped inner backend.
    pub const fn inner(&self) -> &B {
        &self.inner
    }

    /// Consume the wrapper and return the inner backend.
    #[allow(clippy::missing_const_for_fn)]
    pub fn into_inner(self) -> B {
        self.inner
    }

    /// Total number of cache hits observed since construction.
    #[must_use]
    pub fn cache_hits(&self) -> u64 {
        self.hits.load(Ordering::Relaxed)
    }

    /// Total number of cache misses observed since construction.
    #[must_use]
    pub fn cache_misses(&self) -> u64 {
        self.misses.load(Ordering::Relaxed)
    }

    /// Number of terminal results currently held in the cache.
    pub async fn cache_len(&self) -> usize {
        self.cache.lock().await.len()
    }

    /// Number of live negative ("no result stored") entries currently held.
    pub async fn negative_cache_len(&self) -> usize {
        self.negative.lock().await.len()
    }

    /// The configured maximum cache capacity.
    pub async fn capacity(&self) -> usize {
        self.cache.lock().await.capacity
    }

    /// The configured time-to-live for cached terminal results, if any.
    pub async fn ttl(&self) -> Option<Duration> {
        self.cache.lock().await.ttl
    }

    /// Remove a single task's entry from both caches (without touching the
    /// inner backend).
    pub async fn invalidate(&self, task_id: TaskId) {
        self.cache.lock().await.remove(&task_id);
        self.negative.lock().await.remove(&task_id);
    }

    /// Drop every cached entry (without touching the inner backend).
    pub async fn clear_cache(&self) {
        self.cache.lock().await.clear();
        self.negative.lock().await.clear();
    }

    /// Record a hit and return the supplied value (helper to keep call sites
    /// terse).
    fn record_hit(&self) {
        self.hits.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a miss.
    fn record_miss(&self) {
        self.misses.fetch_add(1, Ordering::Relaxed);
    }
}

#[async_trait::async_trait]
impl<B: ResultStore> ResultStore for CachingResultBackend<B> {
    async fn store_result(&self, task_id: TaskId, result: TaskResultValue) -> Result<()> {
        // Write through to the authoritative backend first.
        self.inner.store_result(task_id, result.clone()).await?;
        // Refresh the caches with the freshly stored value (a transient state
        // is not cached, but it does invalidate any previous entry).
        self.populate(task_id, Some(&result)).await;
        Ok(())
    }

    async fn get_result(&self, task_id: TaskId) -> Result<Option<TaskResultValue>> {
        // Fast path: serve a terminal result from the cache on a live hit.
        if let Some(cached) = self.cache.lock().await.get(&task_id) {
            self.record_hit();
            return Ok(Some(cached));
        }
        // Short-lived negative entry: the task had no stored result very
        // recently.
        if self.negative_hit(task_id).await {
            self.record_hit();
            return Ok(None);
        }
        self.record_miss();

        // Slow path: consult the inner backend. Only immutable terminal values
        // are cached; `Pending`/`Received`/`Started`/`Retry` are guaranteed to
        // change and must always be read through.
        let value = self.inner.get_result(task_id).await?;
        self.populate(task_id, value.as_ref()).await;
        Ok(value)
    }

    async fn get_state(&self, task_id: TaskId) -> Result<TaskState> {
        // Derive state from a cached terminal result when possible to avoid a
        // backend round-trip.
        if let Some(cached) = self.cache.lock().await.get(&task_id) {
            self.record_hit();
            return Ok(state_for_cached(Some(&cached)));
        }
        if self.negative_hit(task_id).await {
            self.record_hit();
            // No result stored: the conventional "unknown task" state.
            return Ok(TaskState::Pending);
        }
        self.record_miss();
        // Fall through to the inner backend for the authoritative state. We do
        // not populate the result cache here because `get_state` does not yield
        // a `TaskResultValue` in general.
        self.inner.get_state(task_id).await
    }

    async fn forget(&self, task_id: TaskId) -> Result<()> {
        // Invalidate the caches, then forget in the inner backend.
        self.cache.lock().await.remove(&task_id);
        self.negative.lock().await.remove(&task_id);
        self.inner.forget(task_id).await
    }

    async fn has_result(&self, task_id: TaskId) -> Result<bool> {
        if self.cache.lock().await.get(&task_id).is_some() {
            self.record_hit();
            return Ok(true);
        }
        if self.negative_hit(task_id).await {
            self.record_hit();
            return Ok(false);
        }
        self.record_miss();
        // Populate the cache via a full read so a subsequent `get_result`/
        // `has_result` on a finished task is a hit.
        let value = self.inner.get_result(task_id).await?;
        let present = value.is_some();
        self.populate(task_id, value.as_ref()).await;
        Ok(present)
    }

    // Tombstone and TTL hooks are delegated to the inner backend so the wrapper
    // is transparent with respect to those features. Forgetting via
    // `forget_with_tombstone` still invalidates the cache because the default
    // implementation calls `forget`, which this type overrides above.
    async fn store_tombstone(&self, tombstone: crate::ResultTombstone) -> Result<()> {
        self.inner.store_tombstone(tombstone).await
    }

    async fn get_tombstone(&self, task_id: TaskId) -> Result<Option<crate::ResultTombstone>> {
        self.inner.get_tombstone(task_id).await
    }

    async fn apply_result_ttl(
        &self,
        task_id: TaskId,
        config: &crate::result_ttl::ResultTtlConfig,
        task_name: &str,
    ) -> Result<bool> {
        self.inner
            .apply_result_ttl(task_id, config, task_name)
            .await
    }

    async fn store_result_named(
        &self,
        task_id: TaskId,
        task_name: Option<&str>,
        result: TaskResultValue,
    ) -> Result<()> {
        // Write through with the name intact so an inner backend that honours
        // per-task-type TTLs still sees it, then refresh the caches exactly as
        // `store_result` does.
        self.inner
            .store_result_named(task_id, task_name, result.clone())
            .await?;
        self.populate(task_id, Some(&result)).await;
        Ok(())
    }

    async fn await_result_change(
        &self,
        task_id: TaskId,
        max_wait: std::time::Duration,
    ) -> Result<bool> {
        // Delegate so wrapping a push-capable backend in the cache does not
        // silently downgrade waiters back to polling.
        self.inner.await_result_change(task_id, max_wait).await
    }
}

/// Map an optional cached result value to the conventional [`TaskState`].
fn state_for_cached(value: Option<&TaskResultValue>) -> TaskState {
    match value {
        None | Some(TaskResultValue::Pending) => TaskState::Pending,
        Some(TaskResultValue::Received) => TaskState::Received,
        Some(TaskResultValue::Started) => TaskState::Running,
        Some(TaskResultValue::Success(v)) => {
            TaskState::Succeeded(serde_json::to_vec(v).unwrap_or_default())
        }
        Some(TaskResultValue::Failure { error, .. }) => TaskState::Failed(error.clone()),
        Some(TaskResultValue::Revoked) => TaskState::Revoked,
        Some(TaskResultValue::Retry { attempt, .. }) => TaskState::Retrying(*attempt),
        Some(TaskResultValue::Rejected { .. }) => TaskState::Rejected,
        // A suppressed failure is terminal and non-failing by contract, and its
        // value is the JSON `null` the workflow carries forward — so it maps to
        // the same state a task returning nothing would report. Mapping it to
        // `Failed` would resurrect the error the caller asked to ignore;
        // mapping it to a `Custom` state would make it non-terminal and hang
        // every waiter.
        Some(TaskResultValue::Ignored { .. }) => TaskState::Succeeded(b"null".to_vec()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::in_memory_broker::InMemoryResultBackend;
    use async_trait::async_trait;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;
    use uuid::Uuid;

    /// A backend that counts how many times each read method is called, so
    /// tests can prove that cache hits skip the inner backend.
    #[derive(Debug, Default)]
    struct CountingBackend {
        inner: InMemoryResultBackend,
        get_calls: AtomicUsize,
    }

    impl CountingBackend {
        fn get_calls(&self) -> usize {
            self.get_calls.load(AtomicOrdering::Relaxed)
        }
    }

    #[async_trait]
    impl ResultStore for CountingBackend {
        async fn store_result(&self, task_id: TaskId, result: TaskResultValue) -> Result<()> {
            self.inner.store_result(task_id, result).await
        }
        async fn get_result(&self, task_id: TaskId) -> Result<Option<TaskResultValue>> {
            self.get_calls.fetch_add(1, AtomicOrdering::Relaxed);
            self.inner.get_result(task_id).await
        }
        async fn get_state(&self, task_id: TaskId) -> Result<TaskState> {
            self.inner.get_state(task_id).await
        }
        async fn forget(&self, task_id: TaskId) -> Result<()> {
            self.inner.forget(task_id).await
        }
        async fn has_result(&self, task_id: TaskId) -> Result<bool> {
            self.inner.has_result(task_id).await
        }
    }

    #[tokio::test]
    async fn hit_skips_inner_backend() {
        let counting = CountingBackend::default();
        // Use an explicit (long) negative TTL so the assertion below cannot be
        // affected by the default short negative TTL elapsing.
        let cached =
            CachingResultBackend::new(counting, 16).with_negative_ttl(Duration::from_secs(3600));
        let id = Uuid::new_v4();

        // First read is a miss and touches the inner backend once.
        assert!(cached.get_result(id).await.unwrap().is_none());
        assert_eq!(cached.cache_misses(), 1);
        assert_eq!(cached.inner().get_calls(), 1);

        // Second read is a hit (negative cache) and does NOT touch the inner
        // backend.
        assert!(cached.get_result(id).await.unwrap().is_none());
        assert_eq!(cached.cache_hits(), 1);
        assert_eq!(cached.inner().get_calls(), 1);
    }

    #[tokio::test]
    async fn store_then_get_is_hit() {
        let cached = CachingResultBackend::new(InMemoryResultBackend::new(), 16);
        let id = Uuid::new_v4();
        cached
            .store_result(id, TaskResultValue::Success(json!(123)))
            .await
            .unwrap();
        // store_result populates the cache, so this is a hit.
        let v = cached.get_result(id).await.unwrap().unwrap();
        assert!(v.is_successful());
        assert_eq!(cached.cache_hits(), 1);
        assert_eq!(cached.cache_misses(), 0);
    }

    #[tokio::test]
    async fn eviction_at_capacity() {
        let cached = CachingResultBackend::new(InMemoryResultBackend::new(), 2);
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        let c = Uuid::new_v4();
        for id in [a, b] {
            cached
                .store_result(id, TaskResultValue::Success(json!(1)))
                .await
                .unwrap();
        }
        assert_eq!(cached.cache_len().await, 2);

        // Touch `a` so `b` becomes the LRU entry.
        let _ = cached.get_result(a).await.unwrap();
        // Insert `c`, evicting `b` (the least-recently-used). Cache is {c, a}.
        cached
            .store_result(c, TaskResultValue::Success(json!(1)))
            .await
            .unwrap();
        assert_eq!(cached.cache_len().await, 2);

        // `a` and `c` survived: reading them are hits (do this BEFORE touching
        // `b`, since reading the evicted `b` would itself evict another entry).
        let hits_before = cached.cache_hits();
        let _ = cached.get_result(a).await.unwrap();
        let _ = cached.get_result(c).await.unwrap();
        assert_eq!(cached.cache_hits(), hits_before + 2);

        // `b` was evicted: reading it is a miss.
        let misses_before = cached.cache_misses();
        let _ = cached.get_result(b).await.unwrap();
        assert_eq!(cached.cache_misses(), misses_before + 1);
    }

    #[tokio::test]
    async fn ttl_expiry_is_a_miss() {
        let counting = CountingBackend::default();
        let cached = CachingResultBackend::with_ttl(counting, 16, Duration::from_millis(30));
        let id = Uuid::new_v4();
        cached
            .store_result(id, TaskResultValue::Success(json!(1)))
            .await
            .unwrap();
        // Immediately: hit, no inner read.
        assert!(cached.get_result(id).await.unwrap().is_some());
        assert_eq!(cached.inner().get_calls(), 0);

        // After TTL: entry expired, becomes a miss that hits the inner backend.
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(cached.get_result(id).await.unwrap().is_some());
        assert_eq!(cached.inner().get_calls(), 1);
        assert!(cached.cache_misses() >= 1);
    }

    #[tokio::test]
    async fn forget_invalidates_cache_and_inner() {
        let cached = CachingResultBackend::new(InMemoryResultBackend::new(), 16);
        let id = Uuid::new_v4();
        cached
            .store_result(id, TaskResultValue::Success(json!(1)))
            .await
            .unwrap();
        assert!(cached.has_result(id).await.unwrap());

        cached.forget(id).await.unwrap();
        // Removed from the inner backend.
        assert!(!cached.inner().has_result(id).await.unwrap());
        // And the cache no longer serves a stale positive: this read is a miss.
        let misses_before = cached.cache_misses();
        assert!(cached.get_result(id).await.unwrap().is_none());
        assert_eq!(cached.cache_misses(), misses_before + 1);
    }

    #[tokio::test]
    async fn overwrite_refreshes_cache() {
        let cached = CachingResultBackend::new(InMemoryResultBackend::new(), 16);
        let id = Uuid::new_v4();
        cached
            .store_result(id, TaskResultValue::Success(json!(1)))
            .await
            .unwrap();
        cached
            .store_result(id, TaskResultValue::Success(json!(2)))
            .await
            .unwrap();
        let v = cached.get_result(id).await.unwrap().unwrap();
        assert_eq!(v.success_value().cloned(), Some(json!(2)));
    }

    #[tokio::test]
    async fn invalidate_removes_single_entry() {
        let cached = CachingResultBackend::new(InMemoryResultBackend::new(), 16);
        let id = Uuid::new_v4();
        cached
            .store_result(id, TaskResultValue::Success(json!(1)))
            .await
            .unwrap();
        assert_eq!(cached.cache_len().await, 1);
        cached.invalidate(id).await;
        assert_eq!(cached.cache_len().await, 0);
        // Inner backend still has it.
        assert!(cached.inner().has_result(id).await.unwrap());
    }

    #[tokio::test]
    async fn get_state_served_from_cache() {
        let cached = CachingResultBackend::new(InMemoryResultBackend::new(), 16);
        let id = Uuid::new_v4();
        cached
            .store_result(
                id,
                TaskResultValue::Failure {
                    error: "x".to_string(),
                    traceback: None,
                },
            )
            .await
            .unwrap();
        let hits_before = cached.cache_hits();
        assert_eq!(
            cached.get_state(id).await.unwrap(),
            TaskState::Failed("x".to_string())
        );
        assert_eq!(cached.cache_hits(), hits_before + 1);
    }

    #[tokio::test]
    async fn capacity_clamped_to_one() {
        let cached = CachingResultBackend::new(InMemoryResultBackend::new(), 0);
        assert_eq!(cached.capacity().await, 1);
        let a = Uuid::new_v4();
        let b = Uuid::new_v4();
        cached
            .store_result(a, TaskResultValue::Success(json!(1)))
            .await
            .unwrap();
        cached
            .store_result(b, TaskResultValue::Success(json!(1)))
            .await
            .unwrap();
        // Only one entry fits.
        assert_eq!(cached.cache_len().await, 1);
    }

    #[tokio::test]
    async fn wraps_in_memory_broker_backend_concurrently() {
        // Smoke test: shareable across tasks via Arc.
        let cached = Arc::new(CachingResultBackend::new(InMemoryResultBackend::new(), 64));
        let id = Uuid::new_v4();
        cached
            .store_result(id, TaskResultValue::Success(json!(1)))
            .await
            .unwrap();

        let mut handles = Vec::new();
        for _ in 0..8 {
            let c = cached.clone();
            handles.push(tokio::spawn(async move {
                c.get_result(id).await.unwrap().is_some()
            }));
        }
        for h in handles {
            assert!(h.await.unwrap());
        }
    }

    #[tokio::test]
    async fn new_installs_a_bounded_ttl() {
        // Regression: `new()` used to install `ttl: None`, so an entry could be
        // served forever even though the authoritative writer is another
        // process whose writes this instance never sees.
        let cached = CachingResultBackend::new(InMemoryResultBackend::new(), 4);
        assert_eq!(cached.ttl().await, Some(DEFAULT_TTL));
    }

    #[tokio::test]
    async fn non_terminal_states_are_never_cached() {
        // Regression: `Pending`/`Started`/`Retry` were cached and then served
        // indefinitely even though they are guaranteed to change.
        let counting = CountingBackend::default();
        let cached = CachingResultBackend::new(counting, 16);
        let id = Uuid::new_v4();

        for value in [
            TaskResultValue::Pending,
            TaskResultValue::Received,
            TaskResultValue::Started,
            TaskResultValue::Retry {
                attempt: 1,
                max_retries: 3,
            },
        ] {
            cached.inner().inner.store_result(id, value).await.unwrap();
            let before = cached.inner().get_calls();
            assert!(cached.get_result(id).await.unwrap().is_some());
            assert!(cached.get_result(id).await.unwrap().is_some());
            // Both reads went to the inner backend: nothing was cached.
            assert_eq!(cached.inner().get_calls(), before + 2);
            assert_eq!(cached.cache_len().await, 0);
        }

        // A terminal value, in contrast, is cached.
        cached
            .inner()
            .inner
            .store_result(id, TaskResultValue::Success(json!(1)))
            .await
            .unwrap();
        let before = cached.inner().get_calls();
        assert!(cached.get_result(id).await.unwrap().is_some());
        assert!(cached.get_result(id).await.unwrap().is_some());
        assert_eq!(cached.inner().get_calls(), before + 1);
        assert_eq!(cached.cache_len().await, 1);
    }

    #[tokio::test]
    async fn polling_observes_completion_written_by_another_process() {
        // Regression: a `None` read used to be cached forever, so a client
        // polling `get_result` before the worker finished never observed the
        // completion. A negative entry must be TTL-bounded and separate.
        let counting = CountingBackend::default();
        let cached =
            CachingResultBackend::new(counting, 16).with_negative_ttl(Duration::from_nanos(0));
        let id = Uuid::new_v4();

        assert!(cached.get_result(id).await.unwrap().is_none());
        assert!(!cached.has_result(id).await.unwrap());
        assert_eq!(cached.get_state(id).await.unwrap(), TaskState::Pending);

        // The "worker" (another process) stores the result straight into the
        // inner backend, bypassing this wrapper's write-through path.
        cached
            .inner()
            .inner
            .store_result(id, TaskResultValue::Success(json!(7)))
            .await
            .unwrap();

        let value = cached.get_result(id).await.unwrap().expect("completion");
        assert_eq!(value.success_value().cloned(), Some(json!(7)));
        assert!(cached.has_result(id).await.unwrap());
        assert!(matches!(
            cached.get_state(id).await.unwrap(),
            TaskState::Succeeded(_)
        ));
    }

    #[tokio::test]
    async fn negative_caching_can_be_disabled() {
        let counting = CountingBackend::default();
        let cached = CachingResultBackend::new(counting, 16).without_negative_caching();
        let id = Uuid::new_v4();

        assert!(cached.get_result(id).await.unwrap().is_none());
        assert!(cached.get_result(id).await.unwrap().is_none());
        assert_eq!(cached.inner().get_calls(), 2);
        assert_eq!(cached.negative_cache_len().await, 0);
    }

    #[tokio::test]
    async fn negative_entry_is_dropped_by_write_through() {
        let cached = CachingResultBackend::new(InMemoryResultBackend::new(), 16);
        let id = Uuid::new_v4();
        assert!(cached.get_result(id).await.unwrap().is_none());
        assert_eq!(cached.negative_cache_len().await, 1);

        cached
            .store_result(id, TaskResultValue::Success(json!(1)))
            .await
            .unwrap();
        assert_eq!(cached.negative_cache_len().await, 0);
        assert!(cached.get_result(id).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn transient_overwrite_invalidates_terminal_entry() {
        // Storing a non-terminal value over a cached terminal one must drop the
        // stale terminal entry rather than keep serving it.
        let cached = CachingResultBackend::new(InMemoryResultBackend::new(), 16);
        let id = Uuid::new_v4();
        cached
            .store_result(id, TaskResultValue::Success(json!(1)))
            .await
            .unwrap();
        assert_eq!(cached.cache_len().await, 1);

        cached
            .store_result(
                id,
                TaskResultValue::Retry {
                    attempt: 1,
                    max_retries: 3,
                },
            )
            .await
            .unwrap();
        assert_eq!(cached.cache_len().await, 0);
        assert_eq!(cached.get_state(id).await.unwrap(), TaskState::Retrying(1));
    }

    #[tokio::test]
    async fn into_inner_returns_backend() {
        let cached = CachingResultBackend::new(InMemoryResultBackend::new(), 4);
        let id = Uuid::new_v4();
        cached
            .store_result(id, TaskResultValue::Success(json!(9)))
            .await
            .unwrap();
        let inner = cached.into_inner();
        assert!(inner.has_result(id).await.unwrap());
    }
}
