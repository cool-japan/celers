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

/// Default cap on the number of distinct entries a [`TtlCache`] retains
/// before [`TtlCache::insert`] starts making room for a new key, used by
/// [`TtlCache::new`]/[`TtlCache::with_clock`] (see
/// [`TtlCache::with_capacity`]/[`TtlCache::with_capacity_and_clock`] for a
/// configurable cap).
///
/// Chosen generously for this crate's own production call sites
/// (`crate::commands::queue`/`crate::commands::worker` key entries by
/// broker-url/queue or broker-url/worker identity -- a single process
/// realistically touches at most a few thousand distinct pairs) while still
/// giving a long-running process (`celers interactive`, or library-embedded
/// use of this crate) a hard bound instead of growing the backing `HashMap`
/// without limit for the lifetime of the process (idx 339 part 2).
const DEFAULT_MAX_ENTRIES: usize = 10_000;

/// A generic, thread-safe time-to-live cache.
///
/// Entries inserted via [`TtlCache::insert`] expire `ttl` after insertion, as
/// measured by the cache's clock (see [`TtlCache::with_clock`]). A [`get`]
/// call on an expired entry evicts it and behaves like a miss; so does
/// [`insert`] once the cache holds `max_entries` distinct keys (see
/// [`TtlCache::with_capacity_and_clock`]), which additionally prefers
/// sweeping already-expired entries over evicting live ones.
///
/// [`get`]: TtlCache::get
/// [`insert`]: TtlCache::insert
pub struct TtlCache<K, V> {
    ttl: Duration,
    max_entries: usize,
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
    /// monotonic clock (`Instant::now`) and capped at
    /// [`DEFAULT_MAX_ENTRIES`] distinct keys.
    #[must_use]
    pub fn new(ttl: Duration) -> Self {
        Self::with_clock(ttl, Instant::now)
    }

    /// Create a cache with the given time-to-live and an injectable clock,
    /// capped at [`DEFAULT_MAX_ENTRIES`] distinct keys.
    ///
    /// Intended for tests that need to simulate TTL expiry deterministically:
    /// pass a closure backed by e.g. an `Arc<Mutex<Instant>>` that the test
    /// can advance between assertions instead of sleeping in real time.
    #[must_use]
    pub fn with_clock<F>(ttl: Duration, now: F) -> Self
    where
        F: Fn() -> Instant + Send + Sync + 'static,
    {
        Self::with_capacity_and_clock(ttl, DEFAULT_MAX_ENTRIES, now)
    }

    /// Create a cache with the given time-to-live and entry-count cap,
    /// backed by the real monotonic clock (`Instant::now`). `max_entries` is
    /// floored to `1` (a `0`-capacity cache could never hold anything, which
    /// would silently defeat both caching and the sweep behavior below).
    ///
    /// Not reachable from this crate's own `bin` target: this crate's own
    /// production caches (`crate::commands::queue`/`crate::commands::worker`)
    /// are all sized off [`DEFAULT_MAX_ENTRIES`] via [`TtlCache::new`], not
    /// individually tuned. It is still reachable via the public
    /// `celers_cli::cache` library API, which is the surface this function
    /// exists for, and is covered by this module's own tests; also kept as
    /// the conventional companion to the already-used
    /// [`TtlCache::with_clock`] (a caller needing a non-default clock *and* a
    /// non-default capacity uses [`TtlCache::with_capacity_and_clock`]
    /// directly, but one needing only a non-default capacity should not have
    /// to also supply a clock). Kept, not renamed/removed, per its public API
    /// contract -- see [`TtlCache::clear`]/[`TtlCache::is_empty`] for the
    /// same pattern elsewhere in this `impl` block.
    #[allow(dead_code)]
    #[must_use]
    pub fn with_capacity(ttl: Duration, max_entries: usize) -> Self {
        Self::with_capacity_and_clock(ttl, max_entries, Instant::now)
    }

    /// Create a cache with the given time-to-live, entry-count cap, and
    /// injectable clock -- the fully general constructor every other
    /// constructor on this type delegates to.
    #[must_use]
    pub fn with_capacity_and_clock<F>(ttl: Duration, max_entries: usize, now: F) -> Self
    where
        F: Fn() -> Instant + Send + Sync + 'static,
    {
        Self {
            ttl,
            max_entries: max_entries.max(1),
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
    ///
    /// When inserting a *new* key would push the cache over its
    /// `max_entries` cap (see [`TtlCache::with_capacity_and_clock`]), room is
    /// made first: already-expired entries are swept out preferentially
    /// (a cache whose readers move on to new keys over time -- e.g.
    /// `crate::commands::queue`/`crate::commands::worker` caching one
    /// broker/queue or broker/worker pair after another over a long
    /// `celers interactive` session -- otherwise never triggers the lazy,
    /// [`TtlCache::get`]-only eviction path for keys nobody reads again, so
    /// the backing `HashMap` would grow without limit for the life of the
    /// process). If every entry is still live after that sweep, one
    /// arbitrary entry is evicted to make room, mirroring
    /// [`crate::pool::ClientPool::insert`]'s capacity policy. Overwriting an
    /// already-present key never evicts anything, regardless of capacity.
    pub fn insert(&self, key: K, value: V) {
        let now = (self.now_fn)();
        let expires_at = now + self.ttl;
        let mut guard = lock(&self.entries);

        if !guard.contains_key(&key) && guard.len() >= self.max_entries {
            guard.retain(|_, entry| entry.expires_at > now);
        }
        if !guard.contains_key(&key) && guard.len() >= self.max_entries {
            if let Some(evict_key) = guard.keys().next().cloned() {
                guard.remove(&evict_key);
            }
        }

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
    /// expired entries -- they are swept lazily, on [`TtlCache::get`] (that
    /// key only) or when [`TtlCache::insert`] needs room for a new key
    /// (opportunistically, any expired key), never eagerly.
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
    fn with_capacity_floors_zero_to_one() {
        let cache: TtlCache<String, i32> = TtlCache::with_capacity(Duration::from_secs(60), 0);
        cache.insert("a".to_string(), 1);
        assert_eq!(
            cache.get(&"a".to_string()),
            Some(1),
            "a floored-to-1 capacity must still admit and retain one entry"
        );
    }

    /// Regression test for idx 339 part 2: `insert` used to grow the backing
    /// `HashMap` without any bound at all, so a long-running process (e.g.
    /// `celers interactive`) whose reads move on to a new key every time
    /// (never re-`get`ting an old one, so the lazy per-key eviction in
    /// [`TtlCache::get`] never triggers for it) leaked one entry per distinct
    /// key for the life of the process. `insert` must now cap the cache at
    /// `max_entries`, evicting to make room for a genuinely new key.
    #[test]
    fn insert_evicts_to_stay_at_or_under_capacity_for_a_new_key() {
        let cache: TtlCache<String, i32> = TtlCache::with_capacity(Duration::from_secs(60), 2);
        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);
        assert_eq!(cache.len(), 2);

        cache.insert("c".to_string(), 3);
        assert!(
            cache.len() <= 2,
            "insert must not let the cache grow past max_entries for a new key"
        );
        assert_eq!(
            cache.get(&"c".to_string()),
            Some(3),
            "the new key that triggered eviction must itself be admitted"
        );
    }

    #[test]
    fn insert_overwriting_an_existing_key_never_evicts_even_at_capacity() {
        let cache: TtlCache<String, i32> = TtlCache::with_capacity(Duration::from_secs(60), 2);
        cache.insert("a".to_string(), 1);
        cache.insert("b".to_string(), 2);
        assert_eq!(cache.len(), 2);

        cache.insert("a".to_string(), 99); // overwrite, not a new key
        assert_eq!(
            cache.len(),
            2,
            "overwriting a present key must not evict anything"
        );
        assert_eq!(cache.get(&"a".to_string()), Some(99));
        assert_eq!(
            cache.get(&"b".to_string()),
            Some(2),
            "the other live entry must survive an overwrite of a different key"
        );
    }

    /// `insert`'s eviction must prefer sweeping an already-expired entry
    /// over evicting a still-live one, so a cache at capacity keeps serving
    /// its live entries instead of arbitrarily discarding one of them while
    /// dead weight (a key whose TTL elapsed but was never `get`-swept
    /// because nothing looked it up again) lingers.
    #[test]
    fn insert_prefers_sweeping_an_expired_entry_over_evicting_a_live_one_when_full() {
        let clock = FakeClock::new();
        let cache: TtlCache<String, i32> =
            TtlCache::with_capacity_and_clock(Duration::from_secs(10), 2, clock.as_fn());

        cache.insert("soon_to_expire".to_string(), 1); // expires at t=10
        clock.advance(Duration::from_secs(6));
        cache.insert("still_fresh".to_string(), 2); // expires at t=16
        clock.advance(Duration::from_secs(5)); // now t=11: expired, fresh is not

        assert_eq!(
            cache.len(),
            2,
            "both entries remain in the map until something triggers a sweep"
        );

        cache.insert("new".to_string(), 3); // must sweep "soon_to_expire", not evict "still_fresh"

        assert_eq!(
            cache.len(),
            2,
            "cache stays at capacity after the sweep+insert"
        );
        assert_eq!(
            cache.get(&"new".to_string()),
            Some(3),
            "the new key must be admitted"
        );
        assert_eq!(
            cache.get(&"still_fresh".to_string()),
            Some(2),
            "the still-live entry must survive: the expired one is swept first"
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
