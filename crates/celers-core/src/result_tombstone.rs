//! Result tombstone tracking for forgotten/deleted task results.
//!
//! A *tombstone* is a small marker that records the fact that a task result
//! was explicitly deleted ("forgotten") rather than simply never having
//! existed. This distinction matters for clients that need to tell the
//! difference between:
//!
//! * a result that is still being computed / was never produced (absent), and
//! * a result that *did* exist but has since been purged (tombstoned).
//!
//! The [`crate::result::ResultTombstone`] type (defined in [`crate::result`])
//! carries the marker payload (`task_id`, `deleted_at`, `reason`,
//! `deleted_by`, optional `tombstone_ttl`). This module adds the *semantics*
//! around it: a tri-state existence enum, an in-memory registry, and the
//! [`AsyncResult`](crate::result::AsyncResult) integration that populates it.
//!
//! # What is automatic, and what is opt-in
//!
//! | Behaviour | Automatic? |
//! |---|---|
//! | `AsyncResult::forget()` deletes the result from the backend | **yes**, always — this is what it always did |
//! | `AsyncResult::forget()` records a tombstone | **only** after [`AsyncResult::with_tombstone_registry`](crate::result::AsyncResult::with_tombstone_registry) |
//! | `AsyncResult::existence()` can answer "tombstoned" | only when a registry is attached, or the backend overrides [`ResultStore::get_tombstone`](crate::result::ResultStore::get_tombstone) |
//! | `AsyncResult::get()` fails fast on a forgotten result instead of polling forever | **yes** when the tombstone is visible (registry attached or a tombstone-aware backend), and [`AsyncResultConfig::fail_on_tombstone`](crate::result::AsyncResultConfig::fail_on_tombstone) is on by default |
//! | Some other component populating the registry | **no** — the registry is written by `forget` and by whatever else you call [`TombstoneRegistry::insert`] / [`TombstoneRegistry::mark_forgotten`] from |
//!
//! Without a registry, `forget()` is byte-for-byte the call it always was, and
//! a deleted result is indistinguishable from one that never existed.
//!
//! # Scope
//!
//! [`TombstoneRegistry`] is **in-process**: another worker or another CLI
//! invocation sees nothing it recorded. Cross-process visibility needs a
//! backend that overrides
//! [`ResultStore::store_tombstone`](crate::result::ResultStore::store_tombstone)
//! and [`ResultStore::get_tombstone`](crate::result::ResultStore::get_tombstone)
//! — `forget` offers the tombstone to the backend as well as to the registry,
//! so such a backend gets it for free. Entries are also bounded (see
//! [`DEFAULT_TOMBSTONE_TTL`] and [`DEFAULT_MAX_TOMBSTONES`]), so a tombstone is
//! a *finite-lifetime* record: once it expires the result reads as `Absent`
//! again.
//!
//! # Example
//!
//! ```rust
//! use celers_core::result::ResultTombstone;
//! use celers_core::result_tombstone::{ResultExistence, TombstoneRegistry};
//! use uuid::Uuid;
//!
//! let registry = TombstoneRegistry::new();
//! let task_id = Uuid::new_v4();
//!
//! // Nothing recorded yet -> absent (never existed, as far as we know).
//! assert_eq!(registry.lookup(task_id), ResultExistence::Absent);
//! assert!(!registry.is_tombstoned(task_id));
//!
//! // Mark the result as deleted.
//! registry.insert(ResultTombstone::new(task_id).with_reason("manual forget"));
//!
//! // Now it is distinguishable from "never existed".
//! assert!(registry.is_tombstoned(task_id));
//! match registry.lookup(task_id) {
//!     ResultExistence::Tombstoned(t) => assert_eq!(t.task_id, task_id),
//!     other => panic!("expected tombstone, got {other:?}"),
//! }
//! ```

use crate::result::ResultTombstone;
use crate::TaskId;
use std::collections::HashMap;
use std::sync::RwLock;
use std::time::Duration;

/// Tri-state existence of a task result, distinguishing a *forgotten* result
/// (one that has a tombstone marker) from one that simply never existed.
///
/// This is the central abstraction that makes "deleted" distinguishable from
/// "never existed". Backends that support tombstones can map their stored
/// state onto this enum.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ResultExistence {
    /// A live result value is present in the backend.
    Present,

    /// The result was explicitly deleted/forgotten and a tombstone marker
    /// records that fact. Carries the recorded tombstone.
    Tombstoned(ResultTombstone),

    /// No result and no tombstone — as far as the backend knows the result
    /// never existed (or its tombstone has itself expired).
    Absent,
}

impl ResultExistence {
    /// Returns `true` if a live result value is present.
    #[inline]
    #[must_use]
    pub const fn is_present(&self) -> bool {
        matches!(self, ResultExistence::Present)
    }

    /// Returns `true` if the result has been forgotten (a tombstone exists).
    #[inline]
    #[must_use]
    pub const fn is_tombstoned(&self) -> bool {
        matches!(self, ResultExistence::Tombstoned(_))
    }

    /// Returns `true` if the result is absent (never existed / fully purged).
    #[inline]
    #[must_use]
    pub const fn is_absent(&self) -> bool {
        matches!(self, ResultExistence::Absent)
    }

    /// Returns `true` if the result has been explicitly deleted, i.e. it is
    /// *not* simply absent. This is the key predicate that distinguishes a
    /// deleted result from one that never existed.
    #[inline]
    #[must_use]
    pub const fn was_deleted(&self) -> bool {
        matches!(self, ResultExistence::Tombstoned(_))
    }

    /// Borrow the tombstone marker if this existence is a tombstone.
    #[inline]
    #[must_use]
    pub const fn tombstone(&self) -> Option<&ResultTombstone> {
        match self {
            ResultExistence::Tombstoned(t) => Some(t),
            _ => None,
        }
    }

    /// Consume the existence, returning the tombstone marker if present.
    #[inline]
    #[must_use]
    pub fn into_tombstone(self) -> Option<ResultTombstone> {
        match self {
            ResultExistence::Tombstoned(t) => Some(t),
            _ => None,
        }
    }
}

/// Extension helpers for [`ResultTombstone`] that depend on the wall clock.
///
/// These are provided as a separate extension trait (rather than inherent
/// methods on `ResultTombstone` in `result.rs`) to keep the change additive
/// and self-contained in this module.
pub trait TombstoneExt {
    /// Returns the age of the tombstone (time elapsed since `deleted_at`).
    ///
    /// Returns [`Duration::ZERO`] if `deleted_at` is in the future (clock
    /// skew) so the result is always well defined.
    fn age(&self) -> Duration;

    /// Returns `true` if the tombstone itself has expired according to its
    /// `tombstone_ttl`. A tombstone with no TTL never expires.
    fn is_expired(&self) -> bool;

    /// Returns the remaining lifetime of the tombstone, if a TTL is set and it
    /// has not yet expired.
    fn time_until_expiration(&self) -> Option<Duration>;
}

impl TombstoneExt for ResultTombstone {
    fn age(&self) -> Duration {
        let now = chrono::Utc::now();
        let diff = now - self.deleted_at;
        diff.to_std().unwrap_or(Duration::ZERO)
    }

    fn is_expired(&self) -> bool {
        match self.tombstone_ttl {
            Some(ttl) => self.age() >= ttl,
            None => false,
        }
    }

    fn time_until_expiration(&self) -> Option<Duration> {
        self.tombstone_ttl.and_then(|ttl| {
            let age = self.age();
            if age >= ttl {
                None
            } else {
                Some(ttl - age)
            }
        })
    }
}

/// A thread-safe in-memory registry of result tombstones.
///
/// This provides a backend-agnostic, fully testable place to record and query
/// tombstones. Distributed backends (Redis, SQL, …) can keep their own
/// persistent stores; this registry is useful for single-process setups, for
/// caching, and for tests.
///
/// Expired tombstones (those whose `tombstone_ttl` has elapsed) are treated as
/// [`ResultExistence::Absent`] on lookup and can be physically removed via
/// [`TombstoneRegistry::purge_expired`].
///
/// # Bounded growth
///
/// `ResultTombstone::new` leaves `tombstone_ttl` unset, and a tombstone without
/// a TTL never expires — so a long-lived process that forgets results would
/// accumulate one map entry per forgotten task id forever. The registry
/// therefore applies [`DEFAULT_TOMBSTONE_TTL`] to any inserted tombstone that
/// carries none, and caps itself at [`DEFAULT_MAX_TOMBSTONES`] entries, evicting
/// the oldest first. Both are configurable, and `purge_expired` can be scheduled
/// from a background task for prompt reclamation.
#[derive(Debug)]
pub struct TombstoneRegistry {
    inner: RwLock<HashMap<TaskId, ResultTombstone>>,
    /// TTL applied to inserted tombstones that carry none.
    default_ttl: Option<Duration>,
    /// Maximum number of tombstones retained.
    max_entries: usize,
}

/// Default TTL applied to tombstones inserted without one.
///
/// A tombstone only needs to outlive the window in which a client might still be
/// waiting on the forgotten result; a day is generous.
pub const DEFAULT_TOMBSTONE_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// Default cap on the number of tombstones retained.
pub const DEFAULT_MAX_TOMBSTONES: usize = 100_000;

impl Default for TombstoneRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl TombstoneRegistry {
    /// Create a new, empty tombstone registry with the default TTL and cap.
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(HashMap::new()),
            default_ttl: Some(DEFAULT_TOMBSTONE_TTL),
            max_entries: DEFAULT_MAX_TOMBSTONES,
        }
    }

    /// Set the TTL applied to inserted tombstones that carry none.
    ///
    /// `None` disables the default, restoring "never expires" for such
    /// tombstones — they are then only reclaimed by the capacity bound.
    #[must_use]
    pub fn with_default_ttl(mut self, ttl: Option<Duration>) -> Self {
        self.default_ttl = ttl;
        self
    }

    /// Set the maximum number of tombstones retained. `0` is treated as `1`.
    #[must_use]
    pub fn with_max_entries(mut self, max_entries: usize) -> Self {
        self.max_entries = max_entries.max(1);
        self.enforce_capacity();
        self
    }

    /// The TTL applied to tombstones inserted without one.
    #[inline]
    #[must_use]
    pub const fn default_ttl(&self) -> Option<Duration> {
        self.default_ttl
    }

    /// The maximum number of tombstones retained.
    #[inline]
    #[must_use]
    pub const fn max_entries(&self) -> usize {
        self.max_entries
    }

    /// Drop expired tombstones first, then the oldest, until the cap holds.
    fn enforce_capacity(&self) {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let max = self.max_entries.max(1);
        if guard.len() <= max {
            return;
        }
        guard.retain(|_, tombstone| !tombstone.is_expired());
        while guard.len() > max {
            let Some(victim) = guard
                .iter()
                .min_by_key(|(_, tombstone)| tombstone.deleted_at)
                .map(|(id, _)| *id)
            else {
                break;
            };
            guard.remove(&victim);
        }
    }

    /// Record a tombstone, overwriting any previous marker for the same task.
    ///
    /// A tombstone with no TTL of its own inherits the registry's
    /// [`Self::default_ttl`], so the entry is reclaimable.
    ///
    /// Returns the previously stored tombstone for the task, if any.
    pub fn insert(&self, mut tombstone: ResultTombstone) -> Option<ResultTombstone> {
        if tombstone.tombstone_ttl.is_none() {
            if let Some(ttl) = self.default_ttl {
                tombstone.tombstone_ttl = Some(ttl);
            }
        }
        let task_id = tombstone.task_id;
        let previous = {
            let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
            guard.insert(task_id, tombstone)
        };
        self.enforce_capacity();
        previous
    }

    /// Convenience: record that `task_id` was forgotten, with an optional
    /// reason. Returns the previously stored tombstone, if any.
    pub fn mark_forgotten(
        &self,
        task_id: TaskId,
        reason: Option<String>,
    ) -> Option<ResultTombstone> {
        let mut tombstone = ResultTombstone::new(task_id);
        if let Some(reason) = reason {
            tombstone = tombstone.with_reason(reason);
        }
        self.insert(tombstone)
    }

    /// Look up the existence of a task result.
    ///
    /// * Returns [`ResultExistence::Tombstoned`] if a non-expired tombstone is
    ///   recorded.
    /// * Returns [`ResultExistence::Absent`] if no tombstone is recorded, or if
    ///   the recorded tombstone has itself expired.
    ///
    /// Note: this registry only tracks tombstones, so it never returns
    /// [`ResultExistence::Present`]; combine with a live result lookup to get
    /// the full tri-state.
    #[must_use]
    pub fn lookup(&self, task_id: TaskId) -> ResultExistence {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        match guard.get(&task_id) {
            Some(tombstone) if !tombstone.is_expired() => {
                ResultExistence::Tombstoned(tombstone.clone())
            }
            _ => ResultExistence::Absent,
        }
    }

    /// Returns `true` if a non-expired tombstone is recorded for `task_id`.
    #[must_use]
    pub fn is_tombstoned(&self, task_id: TaskId) -> bool {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard
            .get(&task_id)
            .is_some_and(|tombstone| !tombstone.is_expired())
    }

    /// Fetch a clone of the recorded tombstone for `task_id`, ignoring
    /// expiration (returns the raw marker even if its TTL has elapsed).
    #[must_use]
    pub fn get(&self, task_id: TaskId) -> Option<ResultTombstone> {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard.get(&task_id).cloned()
    }

    /// Remove the tombstone for `task_id`, returning it if present.
    ///
    /// This is used when a previously forgotten task is re-created and its
    /// result genuinely exists again.
    pub fn remove(&self, task_id: TaskId) -> Option<ResultTombstone> {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        guard.remove(&task_id)
    }

    /// Number of tombstones currently recorded (including expired ones not yet
    /// purged).
    #[must_use]
    pub fn len(&self) -> usize {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard.len()
    }

    /// Returns `true` if no tombstones are recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        let guard = self.inner.read().unwrap_or_else(|e| e.into_inner());
        guard.is_empty()
    }

    /// Physically remove all expired tombstones, returning the number removed.
    pub fn purge_expired(&self) -> usize {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let before = guard.len();
        guard.retain(|_, tombstone| !tombstone.is_expired());
        before - guard.len()
    }

    /// Remove every tombstone, returning the number removed.
    pub fn clear(&self) -> usize {
        let mut guard = self.inner.write().unwrap_or_else(|e| e.into_inner());
        let count = guard.len();
        guard.clear();
        count
    }
}

// ===========================================================================
// AsyncResult integration
//
// This is the *runtime wiring* half of the module: without it the registry
// above is a type nobody ever writes to. It lives here rather than in
// `result.rs` so the tombstone semantics stay in one file (and so `result.rs`
// stays under the 2000-line ceiling).
// ===========================================================================

impl<S: crate::result::ResultStore + Clone> crate::result::AsyncResult<S> {
    /// Record forgotten results in a shared [`TombstoneRegistry`].
    ///
    /// **Opt-in.** Without it, forgetting a result makes it indistinguishable
    /// from one that never existed: [`Self::existence`] reports
    /// [`ResultExistence::Absent`] and
    /// [`AsyncResult::get`](crate::result::AsyncResult::get) keeps polling for
    /// a result that will never arrive. With a registry attached,
    /// [`AsyncResult::forget`](crate::result::AsyncResult::forget) writes a
    /// tombstone both into the registry (always, whatever the backend
    /// supports) and through
    /// [`ResultStore::forget_with_tombstone`](crate::result::ResultStore::forget_with_tombstone)
    /// (so a tombstone-aware backend persists it for other processes too).
    ///
    /// The registry is shared state: clone the [`Arc`](std::sync::Arc) into
    /// every `AsyncResult` that should see the same tombstones.
    #[must_use]
    pub fn with_tombstone_registry(mut self, registry: std::sync::Arc<TombstoneRegistry>) -> Self {
        self.tombstones = Some(registry);
        self
    }

    /// The attached tombstone registry, if [`Self::with_tombstone_registry`]
    /// was called.
    #[inline]
    #[must_use]
    pub fn tombstone_registry(&self) -> Option<&std::sync::Arc<TombstoneRegistry>> {
        self.tombstones.as_ref()
    }

    /// The tri-state existence of this task's result: present, tombstoned
    /// (explicitly forgotten) or absent (never existed, as far as anyone knows).
    ///
    /// The attached registry is consulted first, because it is authoritative
    /// for anything *this process* forgot even when the backend cannot store
    /// tombstones; otherwise the question goes to
    /// [`ResultStore::result_existence`](crate::result::ResultStore::result_existence),
    /// whose default only ever distinguishes present from absent.
    ///
    /// # Errors
    ///
    /// Propagates any backend error from `ResultStore::result_existence`.
    pub async fn existence(&self) -> crate::Result<ResultExistence> {
        if let Some(ref registry) = self.tombstones {
            let local = registry.lookup(self.task_id());
            if local.is_tombstoned() {
                return Ok(local);
            }
        }
        self.store.result_existence(self.task_id()).await
    }

    /// Forget the task result, recording `reason` on the tombstone.
    ///
    /// The reason is what
    /// [`AsyncResult::get`](crate::result::AsyncResult::get) reports to a
    /// caller waiting on a result that has since been deleted, so it is worth
    /// making it say who deleted it and why ("GDPR erasure request", "nightly
    /// purge").
    ///
    /// With no registry attached the reason is discarded along with the
    /// tombstone: only
    /// [`ResultStore::forget`](crate::result::ResultStore::forget) runs,
    /// exactly as it did before this method existed.
    ///
    /// # Errors
    ///
    /// Propagates the backend's deletion error. The registry is written only
    /// *after* the backend deletion succeeds, so a failed delete never leaves a
    /// tombstone for a result that is still there.
    pub async fn forget_with_reason(&self, reason: impl Into<String>) -> crate::Result<()> {
        let Some(ref registry) = self.tombstones else {
            return self.store.forget(self.task_id()).await;
        };

        let tombstone = ResultTombstone::new(self.task_id()).with_reason(reason);
        // The backend gets the tombstone too, so a tombstone-aware store can
        // answer other processes; the registry is the in-process record that
        // works even when the backend cannot store one.
        self.store.forget_with_tombstone(tombstone.clone()).await?;
        registry.insert(tombstone);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn absent_when_never_recorded() {
        let registry = TombstoneRegistry::new();
        let task_id = Uuid::new_v4();
        assert_eq!(registry.lookup(task_id), ResultExistence::Absent);
        assert!(registry.lookup(task_id).is_absent());
        assert!(!registry.is_tombstoned(task_id));
        assert!(!registry.lookup(task_id).was_deleted());
    }

    #[test]
    fn distinguishes_deleted_from_absent() {
        let registry = TombstoneRegistry::new();
        let deleted = Uuid::new_v4();
        let never = Uuid::new_v4();

        registry.insert(ResultTombstone::new(deleted).with_reason("forget"));

        // Deleted: tombstoned, was_deleted() true.
        let existence = registry.lookup(deleted);
        assert!(existence.is_tombstoned());
        assert!(existence.was_deleted());
        assert!(registry.is_tombstoned(deleted));
        assert_eq!(
            existence.tombstone().and_then(|t| t.reason.clone()),
            Some("forget".to_string())
        );

        // Never recorded: absent, was_deleted() false.
        let existence = registry.lookup(never);
        assert!(existence.is_absent());
        assert!(!existence.was_deleted());
        assert!(!registry.is_tombstoned(never));
    }

    #[test]
    fn mark_forgotten_helper() {
        let registry = TombstoneRegistry::new();
        let task_id = Uuid::new_v4();

        let prev = registry.mark_forgotten(task_id, Some("expired ttl".to_string()));
        assert!(prev.is_none());
        assert!(registry.is_tombstoned(task_id));

        let prev = registry.mark_forgotten(task_id, None);
        assert!(prev.is_some());
        assert_eq!(prev.and_then(|t| t.reason), Some("expired ttl".to_string()));
    }

    #[test]
    fn remove_restores_absent() {
        let registry = TombstoneRegistry::new();
        let task_id = Uuid::new_v4();

        registry.insert(ResultTombstone::new(task_id));
        assert!(registry.is_tombstoned(task_id));

        let removed = registry.remove(task_id);
        assert!(removed.is_some());
        assert_eq!(registry.lookup(task_id), ResultExistence::Absent);
    }

    #[test]
    fn expired_tombstone_is_absent_on_lookup() {
        let registry = TombstoneRegistry::new();
        let task_id = Uuid::new_v4();

        // Build a tombstone that is already expired by back-dating deleted_at.
        let mut tombstone = ResultTombstone::new(task_id).with_ttl(Duration::from_secs(1));
        tombstone.deleted_at = chrono::Utc::now() - chrono::Duration::seconds(10);

        registry.insert(tombstone);

        // Raw get still returns it, but lookup treats it as absent.
        assert!(registry.get(task_id).is_some());
        assert!(!registry.is_tombstoned(task_id));
        assert_eq!(registry.lookup(task_id), ResultExistence::Absent);
    }

    #[test]
    fn purge_expired_removes_only_expired() {
        let registry = TombstoneRegistry::new();
        let live = Uuid::new_v4();
        let expired = Uuid::new_v4();

        registry.insert(ResultTombstone::new(live).with_ttl(Duration::from_secs(3600)));

        let mut old = ResultTombstone::new(expired).with_ttl(Duration::from_secs(1));
        old.deleted_at = chrono::Utc::now() - chrono::Duration::seconds(10);
        registry.insert(old);

        assert_eq!(registry.len(), 2);
        let purged = registry.purge_expired();
        assert_eq!(purged, 1);
        assert_eq!(registry.len(), 1);
        assert!(registry.is_tombstoned(live));
        assert!(registry.get(expired).is_none());
    }

    #[test]
    fn tombstone_ext_age_and_expiry() {
        let task_id = Uuid::new_v4();

        // No TTL -> never expires.
        let no_ttl = ResultTombstone::new(task_id);
        assert!(!no_ttl.is_expired());
        assert!(no_ttl.time_until_expiration().is_none());

        // With TTL, not yet expired.
        let fresh = ResultTombstone::new(task_id).with_ttl(Duration::from_secs(3600));
        assert!(!fresh.is_expired());
        assert!(fresh.time_until_expiration().is_some());

        // Back-dated -> expired.
        let mut old = ResultTombstone::new(task_id).with_ttl(Duration::from_secs(1));
        old.deleted_at = chrono::Utc::now() - chrono::Duration::seconds(10);
        assert!(old.is_expired());
        assert!(old.time_until_expiration().is_none());
        assert!(old.age() >= Duration::from_secs(9));
    }

    #[test]
    fn into_tombstone_consumes() {
        let task_id = Uuid::new_v4();
        let existence = ResultExistence::Tombstoned(ResultTombstone::new(task_id));
        let tombstone = existence.into_tombstone();
        assert_eq!(tombstone.map(|t| t.task_id), Some(task_id));

        assert!(ResultExistence::Absent.into_tombstone().is_none());
        assert!(ResultExistence::Present.into_tombstone().is_none());
    }

    #[test]
    fn clear_and_counts() {
        let registry = TombstoneRegistry::new();
        assert!(registry.is_empty());
        registry.insert(ResultTombstone::new(Uuid::new_v4()));
        registry.insert(ResultTombstone::new(Uuid::new_v4()));
        assert_eq!(registry.len(), 2);
        assert!(!registry.is_empty());
        assert_eq!(registry.clear(), 2);
        assert!(registry.is_empty());
    }

    // ------------------------------------------------------------------
    // Regression tests
    // ------------------------------------------------------------------

    /// Regression: `ResultTombstone::new` leaves the TTL unset and
    /// `is_expired()` returns `false` for a `None` TTL, so a tombstone created
    /// through `mark_forgotten` or a bare `insert` never expired and nothing
    /// ever reclaimed it.
    #[test]
    fn mark_forgotten_tombstones_expire_by_default() {
        let registry = TombstoneRegistry::new().with_default_ttl(Some(Duration::from_millis(0)));
        let task_id = Uuid::new_v4();
        registry.mark_forgotten(task_id, Some("aged out".to_string()));

        // A zero TTL means the tombstone is immediately expired.
        let stored = registry.get(task_id).expect("tombstone recorded");
        assert_eq!(stored.tombstone_ttl, Some(Duration::from_millis(0)));
        assert!(registry.lookup(task_id).is_absent());
        assert!(!registry.is_tombstoned(task_id));
        assert_eq!(registry.purge_expired(), 1);
        assert!(registry.is_empty());
    }

    #[test]
    fn default_ttl_is_applied_and_configurable() {
        let registry = TombstoneRegistry::new();
        assert_eq!(registry.default_ttl(), Some(DEFAULT_TOMBSTONE_TTL));
        assert_eq!(registry.max_entries(), DEFAULT_MAX_TOMBSTONES);

        let task_id = Uuid::new_v4();
        registry.mark_forgotten(task_id, None);
        let stored = registry.get(task_id).expect("tombstone recorded");
        assert_eq!(stored.tombstone_ttl, Some(DEFAULT_TOMBSTONE_TTL));
        // Not yet expired, so it is still a tombstone.
        assert!(registry.is_tombstoned(task_id));

        // An explicit TTL on the tombstone wins over the registry default.
        let other = Uuid::new_v4();
        registry.insert(ResultTombstone::new(other).with_ttl(Duration::from_secs(7)));
        assert_eq!(
            registry.get(other).and_then(|t| t.tombstone_ttl),
            Some(Duration::from_secs(7))
        );

        // Opting out restores the never-expiring behaviour explicitly.
        let forever = TombstoneRegistry::new().with_default_ttl(None);
        let id = Uuid::new_v4();
        forever.mark_forgotten(id, None);
        assert_eq!(forever.get(id).and_then(|t| t.tombstone_ttl), None);
        assert!(forever.is_tombstoned(id));
    }

    #[test]
    fn registry_is_capacity_bounded() {
        let registry = TombstoneRegistry::new().with_max_entries(10);
        assert_eq!(registry.max_entries(), 10);

        for _ in 0..500 {
            registry.mark_forgotten(Uuid::new_v4(), None);
        }
        assert_eq!(registry.len(), 10, "registry must respect its capacity");

        // Zero is clamped to one.
        let tiny = TombstoneRegistry::new().with_max_entries(0);
        assert_eq!(tiny.max_entries(), 1);
        tiny.mark_forgotten(Uuid::new_v4(), None);
        tiny.mark_forgotten(Uuid::new_v4(), None);
        assert_eq!(tiny.len(), 1);
    }
}
