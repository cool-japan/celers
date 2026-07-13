//! Generic time-to-live (TTL) cache used to avoid redundant broker round
//! trips for frequently-read, slowly-changing CLI data such as queue and
//! worker statistics.
//!
//! [`TtlCache`] is intentionally broker-agnostic (it knows nothing about
//! Redis, queues, or workers) so it can be unit tested in isolation. Its
//! expiry clock is injectable — [`TtlCache::new`] uses the real monotonic
//! clock (`Instant::now`), while [`TtlCache::with_clock`] accepts any
//! `Fn() -> Instant`, letting tests simulate TTL expiry deterministically by
//! advancing a fake clock instead of sleeping in real time.
//!
//! Production call sites in `crate::commands::queue` and
//! `crate::commands::worker` key entries by broker/queue (or
//! broker/worker) identity and invalidate them explicitly whenever a mutating
//! command (purge, move, pause, resume, stop, scale, drain, ...) changes the
//! underlying state.
//!
//! # Examples
//!
//! ```
//! use celers_cli::cache::TtlCache;
//! use std::time::Duration;
//!
//! let cache: TtlCache<String, u32> = TtlCache::new(Duration::from_secs(30));
//! assert_eq!(cache.get(&"queue:default".to_string()), None);
//!
//! cache.insert("queue:default".to_string(), 42);
//! assert_eq!(cache.get(&"queue:default".to_string()), Some(42));
//!
//! cache.invalidate(&"queue:default".to_string());
//! assert_eq!(cache.get(&"queue:default".to_string()), None);
//! ```

use std::collections::HashMap;
use std::hash::Hash;
use std::sync::{Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

/// A cached value plus the instant at which it should be considered expired.
struct Entry<V> {
    value: V,
    expires_at: Instant,
}

/// A generic, thread-safe time-to-live cache.
///
/// Entries inserted via [`TtlCache::insert`] expire `ttl` after insertion, as
/// measured by the cache's clock (see [`TtlCache::with_clock`]). A [`get`]
/// call on an expired entry evicts it and behaves like a miss.
///
/// [`get`]: TtlCache::get
pub struct TtlCache<K, V> {
    ttl: Duration,
    entries: Mutex<HashMap<K, Entry<V>>>,
    now_fn: Box<dyn Fn() -> Instant + Send + Sync>,
    hits: std::sync::atomic::AtomicU64,
    misses: std::sync::atomic::AtomicU64,
}

impl<K, V> TtlCache<K, V>
where
    K: Eq + Hash + Clone,
    V: Clone,
{
    /// Create a cache with the given time-to-live, backed by the real
    /// monotonic clock (`Instant::now`).
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self::with_clock(ttl, Instant::now)
    }

    /// Create a cache with the given time-to-live and an injectable clock.
    ///
    /// Intended for tests that need to simulate TTL expiry deterministically:
    /// pass a closure backed by e.g. an `Arc<Mutex<Instant>>` that the test
    /// can advance between assertions instead of sleeping in real time.
    #[must_use]
    pub fn with_clock<F>(ttl: Duration, now: F) -> Self
    where
        F: Fn() -> Instant + Send + Sync + 'static,
    {
        Self {
            ttl,
            entries: Mutex::new(HashMap::new()),
            now_fn: Box::new(now),
            hits: std::sync::atomic::AtomicU64::new(0),
            misses: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Look up `key`, returning `None` on a miss or if the cached entry has
    /// expired. An expired entry is evicted as a side effect of the lookup.
    pub fn get(&self, key: &K) -> Option<V> {
        use std::sync::atomic::Ordering;

        let now = (self.now_fn)();
        let mut guard = lock(&self.entries);
        let hit = match guard.get(key) {
            Some(entry) if entry.expires_at > now => Some(entry.value.clone()),
            Some(_) => {
                guard.remove(key);
                None
            }
            None => None,
        };
        drop(guard);

        if hit.is_some() {
            self.hits.fetch_add(1, Ordering::Relaxed);
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
        }
        hit
    }

    /// Insert or overwrite `key` with `value`, resetting its expiry to
    /// `now + ttl`.
    pub fn insert(&self, key: K, value: V) {
        let expires_at = (self.now_fn)() + self.ttl;
        let mut guard = lock(&self.entries);
        guard.insert(key, Entry { value, expires_at });
    }

    /// Remove `key` unconditionally, regardless of whether it has expired.
    /// Used to invalidate a cached read after a mutating command changes the
    /// underlying state.
    pub fn invalidate(&self, key: &K) {
        let mut guard = lock(&self.entries);
        guard.remove(key);
    }

    /// Remove all entries.
    ///
    /// Not reachable from this crate's own `bin` target: production call
    /// sites (`crate::commands::queue`, `crate::commands::worker`)
    /// invalidate individual keys via [`TtlCache::invalidate`] after a
    /// mutating command changes just the affected queue/worker, rather than
    /// dropping the whole cache. It is still reachable via the public
    /// `celers_cli::cache` library API, which is the surface this function
    /// exists for, and is covered by this module's own tests; also kept as
    /// the conventional collection-API rounding-out of `TtlCache`'s own impl
    /// block (alongside `insert`/`get`/`invalidate`/`len`). Kept, not
    /// renamed/removed, per its public API contract.
    #[allow(dead_code)]
    pub fn clear(&self) {
        let mut guard = lock(&self.entries);
        guard.clear();
    }

    /// Number of entries currently stored, including any not-yet-evicted
    /// expired entries (they are only swept lazily, on [`TtlCache::get`]).
    #[must_use]
    pub fn len(&self) -> usize {
        lock(&self.entries).len()
    }

    /// Returns `true` if the cache holds no entries.
    ///
    /// Not reachable from this crate's own `bin` target: no production call
    /// site currently queries a `TtlCache`'s emptiness directly. It is
    /// still reachable via the public `celers_cli::cache` library API,
    /// which is the surface this function exists for, and is covered by
    /// this module's own tests; also kept as the conventional companion to
    /// the already-used [`TtlCache::len`] (`clippy::len_without_is_empty`
    /// expects the two to travel together). Kept, not renamed/removed, per
    /// its public API contract.
    #[allow(dead_code)]
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Cumulative hit/miss counters since construction, for observability.
    #[must_use]
    pub fn stats(&self) -> CacheStats {
        use std::sync::atomic::Ordering;
        CacheStats {
            len: self.len(),
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time snapshot of [`TtlCache`] hit/miss activity.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CacheStats {
    /// Number of entries currently stored.
    pub len: usize,
    /// Cumulative number of [`TtlCache::get`] calls that returned a live
    /// value.
    pub hits: u64,
    /// Cumulative number of [`TtlCache::get`] calls that returned `None`
    /// (missing or expired).
    pub misses: u64,
}

impl CacheStats {
    /// Fraction of lookups that were hits (`0.0` when there have been no
    /// lookups yet).
    #[must_use]
    pub fn hit_ratio(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Lock `mutex`, recovering the guard from a poisoned lock instead of
/// panicking. A panic in one cached read should not permanently wedge every
/// subsequent lookup in the same process.
fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex.lock().unwrap_or_else(PoisonError::into_inner)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    /// A clock that starts at a fixed instant and can be advanced by tests,
    /// without ever calling `std::thread::sleep`.
    #[derive(Clone)]
    struct FakeClock(Arc<Mutex<Instant>>);

    impl FakeClock {
        fn new() -> Self {
            Self(Arc::new(Mutex::new(Instant::now())))
        }

        fn advance(&self, by: Duration) {
            let mut guard = lock(&self.0);
            *guard += by;
        }

        fn as_fn(&self) -> impl Fn() -> Instant + Send + Sync + 'static {
            let inner = Arc::clone(&self.0);
            move || *lock(&inner)
        }
    }

    #[test]
    fn miss_on_empty_cache() {
        let cache: TtlCache<String, i32> = TtlCache::new(Duration::from_secs(30));
        assert_eq!(cache.get(&"missing".to_string()), None);
        assert_eq!(cache.stats().misses, 1);
        assert_eq!(cache.stats().hits, 0);
    }

    #[test]
    fn hit_before_expiry() {
        let clock = FakeClock::new();
        let cache: TtlCache<String, i32> =
            TtlCache::with_clock(Duration::from_secs(10), clock.as_fn());

        cache.insert("a".to_string(), 1);
        clock.advance(Duration::from_secs(5));

        assert_eq!(cache.get(&"a".to_string()), Some(1));
        assert_eq!(cache.stats().hits, 1);
    }

    #[test]
    fn expires_after_ttl_elapses() {
        let clock = FakeClock::new();
        let cache: TtlCache<String, i32> =
            TtlCache::with_clock(Duration::from_secs(10), clock.as_fn());

        cache.insert("a".to_string(), 1);
        clock.advance(Duration::from_secs(10) + Duration::from_millis(1));

        assert_eq!(
            cache.get(&"a".to_string()),
            None,
            "entry must be expired once the clock has advanced past the TTL"
        );
        assert_eq!(cache.stats().misses, 1);
        assert!(
            cache.is_empty(),
            "an expired entry is evicted as a side effect of the lookup"
        );
    }

    #[test]
    fn expiry_boundary_is_exclusive() {
        let clock = FakeClock::new();
        let cache: TtlCache<String, i32> =
            TtlCache::with_clock(Duration::from_secs(10), clock.as_fn());

        cache.insert("a".to_string(), 1);
        clock.advance(Duration::from_secs(10));

        assert_eq!(
            cache.get(&"a".to_string()),
            None,
            "an entry is not live at exactly now == expires_at"
        );
    }

    #[test]
    fn insert_resets_expiry() {
        let clock = FakeClock::new();
        let cache: TtlCache<String, i32> =
            TtlCache::with_clock(Duration::from_secs(10), clock.as_fn());

        cache.insert("a".to_string(), 1);
        clock.advance(Duration::from_secs(9));
        cache.insert("a".to_string(), 2); // refresh before it expires
        clock.advance(Duration::from_secs(9));

        assert_eq!(
            cache.get(&"a".to_string()),
            Some(2),
            "re-inserting must push expiry out another full TTL"
        );
    }

    #[test]
    fn invalidate_removes_regardless_of_expiry() {
        let cache: TtlCache<String, i32> = TtlCache::new(Duration::from_secs(60));
        cache.insert("a".to_string(), 1);
        assert_eq!(cache.get(&"a".to_string()), Some(1));

        cache.invalidate(&"a".to_string());
        assert_eq!(cache.get(&"a".to_string()), None);
    }

    #[test]
    fn invalidate_on_missing_key_is_a_no_op() {
        let cache: TtlCache<String, i32> = TtlCache::new(Duration::from_secs(60));
        cache.invalidate(&"never-inserted".to_string());
        assert!(cache.is_empty());
    }

    #[test]
    fn clear_empties_all_entries() {
        let cache: TtlCache<String, i32> = TtlCache::new(Duration::from_secs(60));
        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);
        assert_eq!(cache.len(), 2);

        cache.clear();
        assert!(cache.is_empty());
    }

    #[test]
    fn distinct_keys_are_independent() {
        let clock = FakeClock::new();
        let cache: TtlCache<String, i32> =
            TtlCache::with_clock(Duration::from_secs(10), clock.as_fn());

        cache.insert("a".to_string(), 1);
        clock.advance(Duration::from_secs(6));
        cache.insert("b".to_string(), 2);
        clock.advance(Duration::from_secs(5));

        // "a" was inserted at t=0 (expires t=10), "b" at t=6 (expires t=16);
        // at t=11, "a" must be expired but "b" must still be live.
        assert_eq!(cache.get(&"a".to_string()), None);
        assert_eq!(cache.get(&"b".to_string()), Some(2));
    }

    #[test]
    fn cache_stats_hit_ratio() {
        let cache: TtlCache<String, i32> = TtlCache::new(Duration::from_secs(60));
        cache.insert("a".to_string(), 1);

        cache.get(&"a".to_string()); // hit
        cache.get(&"a".to_string()); // hit
        cache.get(&"missing".to_string()); // miss

        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
        assert!((stats.hit_ratio() - (2.0 / 3.0)).abs() < f64::EPSILON);
    }

    #[test]
    fn cache_stats_hit_ratio_with_no_lookups_is_zero() {
        let stats = CacheStats {
            len: 0,
            hits: 0,
            misses: 0,
        };
        assert_eq!(stats.hit_ratio(), 0.0);
    }
}
