//! Per-tenant (per-user) rate limiting.
//!
//! This module composes the existing rate-limiting primitives in
//! [`crate::rate_limit`] (the token-bucket / sliding-window [`RateLimiter`]s and
//! [`RateLimitConfig`]) into a [`TenantRateLimiter`] keyed by *tenant id* (which
//! may equally represent a user, an organization, or any other isolation unit).
//!
//! Each tenant gets its own independent rate limiter, so one noisy tenant cannot
//! starve the others. A registry-wide default [`RateLimitConfig`] is applied to
//! tenants without a specific override, and individual tenants may override both
//! the rate configuration and an optional cumulative *quota* — a ceiling on the
//! total number of permits a tenant may consume (useful for billing plans or
//! trial limits).
//!
//! # Quota scope
//!
//! By default the cumulative quota is **per process**: the consumed count lives
//! in this limiter's memory, so it resets on restart and each worker process
//! enforces the quota independently. That is fine for a coarse safety valve and
//! wrong for billing. Supply a [`TenantQuotaCounter`] via
//! [`TenantRateLimiter::with_quota_counter`] to move the authoritative count
//! into a shared store (Redis, a database, ...), mirroring the backend design in
//! [`crate::rate_limit_distributed`].
//!
//! # Bounded memory
//!
//! Tenant ids are attacker-influenced in any multi-tenant deployment, so the
//! registry is bounded: it tracks at most
//! [`DEFAULT_MAX_TRACKED_TENANTS`] live tenants (configurable), evicting the
//! least recently used, and [`TenantRateLimiter::sweep_idle`] reclaims tenants
//! that have not been seen for a while.
//!
//! # Example
//!
//! ```
//! use celers_core::rate_limit::RateLimitConfig;
//! use celers_core::tenant_rate_limit::TenantRateLimiter;
//!
//! // Default: 100/s. Tenant "free" is overridden to a 2-permit burst.
//! let limiter = TenantRateLimiter::with_default(RateLimitConfig::new(100.0));
//! limiter.set_tenant_config("free", RateLimitConfig::new(1000.0).with_burst(2));
//!
//! // The "free" tenant exhausts its small burst...
//! assert!(limiter.try_acquire("free"));
//! assert!(limiter.try_acquire("free"));
//! assert!(!limiter.try_acquire("free"));
//!
//! // ...while another tenant on the generous default is unaffected.
//! assert!(limiter.try_acquire("enterprise"));
//! ```

use std::collections::hash_map::DefaultHasher;
use std::collections::HashMap;
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use crate::rate_limit::{create_rate_limiter, RateLimitConfig, RateLimiter};

/// Default upper bound on the number of tenants with live state.
///
/// Tenant ids come from users or organizations, so an unbounded registry is a
/// memory-exhaustion vector: a stream of distinct ids would allocate a limiter
/// and counters for each, forever.
pub const DEFAULT_MAX_TRACKED_TENANTS: usize = 100_000;

/// Number of independent lock shards the registry is split across.
///
/// `RateLimiter::try_acquire` needs `&mut self`, so every admission decision
/// takes a write lock. With one lock for the whole registry that serialised
/// every tenant against every other — the opposite of the isolation this module
/// exists to provide. Sharding by tenant id keeps contention proportional to the
/// number of tenants actually competing.
const SHARD_COUNT: usize = 16;

/// Authoritative counter for cumulative per-tenant quota consumption.
///
/// The default counter is process-local. Implement this over Redis, a database,
/// or any shared store to make a quota hold across restarts and across every
/// worker in the cluster.
pub trait TenantQuotaCounter: Send + Sync + std::fmt::Debug {
    /// Atomically consume one permit against `quota` for `tenant_id`.
    ///
    /// Returns `true` if the permit was within quota (and was consumed),
    /// `false` if the tenant has already reached its ceiling.
    fn try_consume(&self, tenant_id: &str, quota: u64) -> bool;

    /// Permits consumed by `tenant_id` so far.
    fn consumed(&self, tenant_id: &str) -> u64;

    /// Reset the consumed count for one tenant.
    fn reset(&self, tenant_id: &str);

    /// Reset every tenant's consumed count.
    fn reset_all(&self);
}

/// Process-local [`TenantQuotaCounter`].
///
/// Used when no shared counter is configured. Its count resets on restart and is
/// not shared between worker processes; see the module-level "Quota scope"
/// section.
#[derive(Debug, Default)]
pub struct InProcessQuotaCounter {
    consumed: Mutex<HashMap<String, u64>>,
}

impl InProcessQuotaCounter {
    /// Create an empty counter.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }
}

impl TenantQuotaCounter for InProcessQuotaCounter {
    fn try_consume(&self, tenant_id: &str, quota: u64) -> bool {
        let mut guard = self.consumed.lock().unwrap_or_else(PoisonError::into_inner);
        let entry = guard.entry(tenant_id.to_string()).or_insert(0);
        if *entry >= quota {
            return false;
        }
        *entry = entry.saturating_add(1);
        true
    }

    fn consumed(&self, tenant_id: &str) -> u64 {
        self.consumed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .get(tenant_id)
            .copied()
            .unwrap_or(0)
    }

    fn reset(&self, tenant_id: &str) {
        self.consumed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .remove(tenant_id);
    }

    fn reset_all(&self) {
        self.consumed
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }
}

/// The full policy applied to a single tenant: its rate-limit configuration plus
/// an optional cumulative quota.
///
/// A `quota` of `None` means unlimited total permits (only the per-second rate
/// applies). When set, once a tenant has consumed `quota` permits in total,
/// [`TenantRateLimiter::try_acquire`] denies further requests regardless of the
/// rate limiter's available tokens, until the tenant is reset.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TenantRateLimit {
    /// The rate-limit configuration (token bucket or sliding window).
    pub config: RateLimitConfig,
    /// Optional cumulative permit ceiling for the tenant.
    pub quota: Option<u64>,
}

impl TenantRateLimit {
    /// Create a tenant policy from a rate-limit configuration, with no quota.
    #[must_use]
    pub fn new(config: RateLimitConfig) -> Self {
        Self {
            config,
            quota: None,
        }
    }

    /// Attach a cumulative quota (maximum total permits) to this policy.
    #[must_use]
    pub fn with_quota(mut self, quota: u64) -> Self {
        self.quota = Some(quota);
        self
    }
}

impl From<RateLimitConfig> for TenantRateLimit {
    fn from(config: RateLimitConfig) -> Self {
        Self::new(config)
    }
}

impl Default for TenantRateLimit {
    fn default() -> Self {
        Self::new(RateLimitConfig::default())
    }
}

/// Live state for one tenant: its limiter, resolved policy, and usage counters.
struct TenantState {
    /// The concrete rate limiter (token bucket or sliding window).
    limiter: Box<dyn RateLimiter>,
    /// Optional cumulative quota ceiling.
    quota: Option<u64>,
    /// Total permits granted to this tenant so far (process-local view).
    granted: u64,
    /// Total requests denied for this tenant so far (rate-limited or over quota).
    denied: u64,
    /// Last time this tenant was seen, used by [`TenantRateLimiter::sweep_idle`].
    last_seen: Instant,
    /// Monotonic touch counter, used for capacity eviction.
    ///
    /// Ordering by `Instant` alone is unreliable: two touches can land on the
    /// same clock tick, making the eviction victim arbitrary.
    touch_seq: u64,
}

impl TenantState {
    fn from_policy(policy: &TenantRateLimit, touch_seq: u64) -> Self {
        Self {
            limiter: create_rate_limiter(policy.config.clone()),
            quota: policy.quota,
            granted: 0,
            denied: 0,
            last_seen: Instant::now(),
            touch_seq,
        }
    }

    /// Returns `true` if this tenant has already reached its cumulative quota,
    /// according to `counter` when one is configured.
    fn quota_exhausted(&self, tenant_id: &str, counter: Option<&dyn TenantQuotaCounter>) -> bool {
        let Some(limit) = self.quota else {
            return false;
        };
        match counter {
            Some(counter) => counter.consumed(tenant_id) >= limit,
            None => self.granted >= limit,
        }
    }

    fn try_acquire(
        &mut self,
        tenant_id: &str,
        counter: Option<&dyn TenantQuotaCounter>,
        touch_seq: u64,
    ) -> bool {
        self.last_seen = Instant::now();
        self.touch_seq = touch_seq;

        // The cumulative quota is a hard ceiling: check it before spending a
        // rate-limit token.
        if self.quota_exhausted(tenant_id, counter) {
            self.denied = self.denied.saturating_add(1);
            return false;
        }
        if !self.limiter.try_acquire() {
            self.denied = self.denied.saturating_add(1);
            return false;
        }
        // Commit the consumption to the authoritative counter, if any.
        if let (Some(limit), Some(counter)) = (self.quota, counter) {
            if !counter.try_consume(tenant_id, limit) {
                self.denied = self.denied.saturating_add(1);
                return false;
            }
        }

        self.granted = self.granted.saturating_add(1);
        true
    }
}

/// A point-in-time, serializable snapshot of a tenant's usage.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct TenantUsage {
    /// Total permits granted so far.
    pub granted: u64,
    /// Total requests denied so far.
    pub denied: u64,
    /// The configured cumulative quota, if any.
    pub quota: Option<u64>,
    /// Permits remaining before the quota is hit (`None` if unlimited).
    pub quota_remaining: Option<u64>,
}

/// One shard of the registry: tenant state and configuration for the tenant ids
/// that hash to this shard.
struct TenantRegistry {
    /// Default policy for tenants without an explicit override.
    default_policy: Option<TenantRateLimit>,
    /// Per-tenant policy overrides (used to (re)build a tenant's limiter).
    overrides: HashMap<String, TenantRateLimit>,
    /// Live per-tenant state.
    tenants: HashMap<String, TenantState>,
    /// Maximum number of live tenants retained in this shard.
    max_tracked: usize,
}

impl TenantRegistry {
    fn new(default_policy: Option<TenantRateLimit>, max_tracked: usize) -> Self {
        Self {
            default_policy,
            overrides: HashMap::new(),
            tenants: HashMap::new(),
            max_tracked,
        }
    }

    /// Resolve the effective policy for a tenant (override wins over default).
    fn policy_for(&self, tenant_id: &str) -> Option<TenantRateLimit> {
        self.overrides
            .get(tenant_id)
            .cloned()
            .or_else(|| self.default_policy.clone())
    }

    /// Evict the least recently used tenants until there is room for one more.
    fn enforce_capacity(&mut self, incoming: &str) {
        if self.tenants.contains_key(incoming) {
            return;
        }
        while self.tenants.len() >= self.max_tracked.max(1) {
            let Some(victim) = self
                .tenants
                .iter()
                .min_by_key(|(_, state)| state.touch_seq)
                .map(|(id, _)| id.clone())
            else {
                break;
            };
            self.tenants.remove(&victim);
        }
    }

    /// Drop tenants not seen for at least `idle_for`, returning how many went.
    fn sweep_idle(&mut self, idle_for: Duration) -> usize {
        let now = Instant::now();
        let before = self.tenants.len();
        self.tenants
            .retain(|_, state| now.saturating_duration_since(state.last_seen) < idle_for);
        before - self.tenants.len()
    }

    /// Try to acquire a permit for a tenant, lazily creating its state.
    fn try_acquire(
        &mut self,
        tenant_id: &str,
        counter: Option<&dyn TenantQuotaCounter>,
        touch_seq: u64,
    ) -> bool {
        if let Some(state) = self.tenants.get_mut(tenant_id) {
            return state.try_acquire(tenant_id, counter, touch_seq);
        }
        // Tenant not yet seen: resolve its policy (override or default).
        let Some(policy) = self.policy_for(tenant_id) else {
            // No rate limit configured for this tenant at all -> always allow.
            return true;
        };
        self.enforce_capacity(tenant_id);
        let mut state = TenantState::from_policy(&policy, touch_seq);
        let granted = state.try_acquire(tenant_id, counter, touch_seq);
        self.tenants.insert(tenant_id.to_string(), state);
        granted
    }
}

/// A thread-safe, per-tenant rate limiter.
///
/// Keyed by tenant id, each tenant receives its own independent
/// [`RateLimiter`] built from the resolved [`RateLimitConfig`] (default or
/// per-tenant override), plus an optional cumulative quota. The limiter is cheap
/// to clone (`Arc`-backed) and safe to share across threads.
///
/// State is split across [`SHARD_COUNT`] independently locked shards keyed by a
/// hash of the tenant id, so admission decisions for unrelated tenants do not
/// serialise against each other, and the registry is capacity-bounded with
/// least-recently-used eviction (see [`DEFAULT_MAX_TRACKED_TENANTS`]).
#[derive(Clone)]
pub struct TenantRateLimiter {
    shards: Arc<Vec<RwLock<TenantRegistry>>>,
    /// Authoritative cumulative-quota counter, when one is configured.
    quota_counter: Option<Arc<dyn TenantQuotaCounter>>,
    /// Monotonic counter driving deterministic LRU eviction.
    touch_counter: Arc<AtomicU64>,
}

impl std::fmt::Debug for TenantRateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut tracked = 0usize;
        let mut overrides = 0usize;
        let mut has_default = false;
        for shard in self.shards.iter() {
            if let Ok(guard) = shard.read() {
                tracked += guard.tenants.len();
                overrides += guard.overrides.len();
                has_default |= guard.default_policy.is_some();
            }
        }
        f.debug_struct("TenantRateLimiter")
            .field("tracked_tenants", &tracked)
            .field("overrides", &overrides)
            .field("has_default", &has_default)
            .field("shards", &self.shards.len())
            .field("shared_quota_counter", &self.quota_counter.is_some())
            .finish()
    }
}

impl TenantRateLimiter {
    /// Build a sharded registry from a default policy and a tracking bound.
    fn build(default_policy: Option<TenantRateLimit>, max_tracked_tenants: usize) -> Self {
        // Split the bound evenly across shards, keeping at least one slot each.
        let per_shard = (max_tracked_tenants / SHARD_COUNT).max(1);
        let shards = (0..SHARD_COUNT)
            .map(|_| RwLock::new(TenantRegistry::new(default_policy.clone(), per_shard)))
            .collect();
        Self {
            shards: Arc::new(shards),
            quota_counter: None,
            touch_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a limiter with no default policy. Tenants without an explicit
    /// override are *not* rate limited (every request is allowed).
    #[must_use]
    pub fn new() -> Self {
        Self::build(None, DEFAULT_MAX_TRACKED_TENANTS)
    }

    /// Create a limiter whose default [`RateLimitConfig`] (without a quota) is
    /// applied to any tenant lacking an explicit override.
    #[must_use]
    pub fn with_default(config: RateLimitConfig) -> Self {
        Self::build(
            Some(TenantRateLimit::new(config)),
            DEFAULT_MAX_TRACKED_TENANTS,
        )
    }

    /// Create a limiter whose default policy (rate *and* optional quota) is
    /// applied to any tenant lacking an explicit override.
    #[must_use]
    pub fn with_default_policy(policy: TenantRateLimit) -> Self {
        Self::build(Some(policy), DEFAULT_MAX_TRACKED_TENANTS)
    }

    /// Set the maximum number of tenants with live state.
    ///
    /// The bound is split evenly across the internal shards; when it is reached
    /// the least recently used tenant in that shard is evicted (which resets
    /// that tenant's process-local limiter and usage counters). A value of `0`
    /// is treated as one slot per shard.
    #[must_use]
    pub fn with_max_tracked_tenants(self, max_tracked_tenants: usize) -> Self {
        let per_shard = (max_tracked_tenants / SHARD_COUNT).max(1);
        for shard in self.shards.iter() {
            if let Ok(mut guard) = shard.write() {
                guard.max_tracked = per_shard;
                while guard.tenants.len() > per_shard {
                    let Some(victim) = guard
                        .tenants
                        .iter()
                        .min_by_key(|(_, state)| state.touch_seq)
                        .map(|(id, _)| id.clone())
                    else {
                        break;
                    };
                    guard.tenants.remove(&victim);
                }
            }
        }
        self
    }

    /// Use a shared, authoritative [`TenantQuotaCounter`] for cumulative quotas.
    ///
    /// Without one, quotas are enforced per process (see the module docs).
    #[must_use]
    pub fn with_quota_counter(mut self, counter: Arc<dyn TenantQuotaCounter>) -> Self {
        self.quota_counter = Some(counter);
        self
    }

    /// Whether an authoritative shared quota counter is configured.
    ///
    /// When this is `false`, cumulative quotas are best-effort per process.
    #[inline]
    #[must_use]
    pub fn has_shared_quota_counter(&self) -> bool {
        self.quota_counter.is_some()
    }

    /// The shard owning a tenant id.
    fn shard_for(&self, tenant_id: &str) -> &RwLock<TenantRegistry> {
        let mut hasher = DefaultHasher::new();
        tenant_id.hash(&mut hasher);
        let index = (hasher.finish() as usize) % self.shards.len();
        &self.shards[index]
    }

    fn quota_counter(&self) -> Option<&dyn TenantQuotaCounter> {
        self.quota_counter.as_deref()
    }

    fn next_touch_seq(&self) -> u64 {
        self.touch_counter.fetch_add(1, Ordering::Relaxed)
    }

    /// Register (or replace) the rate-limit configuration override for a tenant.
    ///
    /// Any existing live state for the tenant is rebuilt with the new config.
    pub fn set_tenant_config(&self, tenant_id: impl Into<String>, config: RateLimitConfig) {
        self.set_tenant_policy(tenant_id, TenantRateLimit::new(config));
    }

    /// Register (or replace) the full policy (rate + optional quota) for a tenant.
    ///
    /// Any existing live state for the tenant is rebuilt, resetting its usage
    /// counters and rate-limiter tokens.
    pub fn set_tenant_policy(&self, tenant_id: impl Into<String>, policy: TenantRateLimit) {
        let id = tenant_id.into();
        if let Ok(mut guard) = self.shard_for(&id).write() {
            guard.tenants.remove(&id);
            guard.overrides.insert(id, policy);
        }
    }

    /// Set (or update) just the cumulative quota for a tenant, keeping its
    /// effective rate configuration **and its accrued usage**.
    ///
    /// If the tenant had no override yet, the current effective configuration
    /// (its override or the default) is captured as the basis for the override.
    /// The live state is mutated in place rather than dropped: dropping it reset
    /// `granted` to zero, so every quota update silently handed the tenant a
    /// fresh full quota.
    pub fn set_tenant_quota(&self, tenant_id: impl Into<String>, quota: Option<u64>) {
        let id = tenant_id.into();
        if let Ok(mut guard) = self.shard_for(&id).write() {
            let base = guard
                .policy_for(&id)
                .unwrap_or_else(|| TenantRateLimit::new(RateLimitConfig::default()));
            let policy = TenantRateLimit {
                config: base.config,
                quota,
            };
            if let Some(state) = guard.tenants.get_mut(&id) {
                // Preserve the limiter and the granted/denied counters.
                state.quota = quota;
            }
            guard.overrides.insert(id, policy);
        }
    }

    /// Remove a tenant's override and any live state. The next request for this
    /// tenant is served by the default policy (if any).
    pub fn remove_tenant(&self, tenant_id: &str) {
        if let Ok(mut guard) = self.shard_for(tenant_id).write() {
            guard.overrides.remove(tenant_id);
            guard.tenants.remove(tenant_id);
        }
        if let Some(counter) = self.quota_counter() {
            counter.reset(tenant_id);
        }
    }

    /// Drop live state for tenants not seen for at least `idle_for`, returning
    /// how many were reclaimed.
    ///
    /// Intended to be called periodically from a background task; the registry
    /// is also capacity-bounded, so this is an optimisation rather than the only
    /// line of defence.
    pub fn sweep_idle(&self, idle_for: Duration) -> usize {
        let mut swept = 0usize;
        for shard in self.shards.iter() {
            if let Ok(mut guard) = shard.write() {
                swept += guard.sweep_idle(idle_for);
            }
        }
        swept
    }

    /// Try to acquire a single permit for a tenant.
    ///
    /// Returns `true` if the request is allowed, `false` if the tenant is rate
    /// limited or has exhausted its cumulative quota. Tenants with no override
    /// and no default policy are always allowed.
    #[must_use]
    pub fn try_acquire(&self, tenant_id: &str) -> bool {
        let touch_seq = self.next_touch_seq();
        let counter = self.quota_counter();
        if let Ok(mut guard) = self.shard_for(tenant_id).write() {
            guard.try_acquire(tenant_id, counter, touch_seq)
        } else {
            // Poisoned lock: fail open so a panic elsewhere does not block work.
            true
        }
    }

    /// Returns `true` if a tenant currently has at least one permit available
    /// (and has not exhausted its quota) without consuming anything.
    #[must_use]
    pub fn has_capacity(&self, tenant_id: &str) -> bool {
        let counter = self.quota_counter();
        if let Ok(guard) = self.shard_for(tenant_id).read() {
            if let Some(state) = guard.tenants.get(tenant_id) {
                return !state.quota_exhausted(tenant_id, counter)
                    && state.limiter.available_permits() > 0;
            }
            // Not yet materialized: capacity is governed by the resolved policy.
            return match guard.policy_for(tenant_id) {
                Some(policy) => policy.config.effective_burst() > 0,
                None => true,
            };
        }
        true
    }

    /// The time until a tenant's next permit becomes available. Returns
    /// [`Duration::ZERO`] for tenants that are not materialized or not limited.
    #[must_use]
    pub fn time_until_available(&self, tenant_id: &str) -> Duration {
        let counter = self.quota_counter();
        if let Ok(guard) = self.shard_for(tenant_id).read() {
            if let Some(state) = guard.tenants.get(tenant_id) {
                if state.quota_exhausted(tenant_id, counter) {
                    // Quota is a hard ceiling; no amount of waiting frees it.
                    return Duration::MAX;
                }
                return state.limiter.time_until_available();
            }
        }
        Duration::ZERO
    }

    /// Returns `true` if a rate limit (override or default) applies to a tenant.
    #[must_use]
    pub fn has_rate_limit(&self, tenant_id: &str) -> bool {
        if let Ok(guard) = self.shard_for(tenant_id).read() {
            guard.overrides.contains_key(tenant_id) || guard.default_policy.is_some()
        } else {
            false
        }
    }

    /// Return the effective policy for a tenant (override or default), if any.
    #[must_use]
    pub fn policy_for(&self, tenant_id: &str) -> Option<TenantRateLimit> {
        self.shard_for(tenant_id)
            .read()
            .ok()
            .and_then(|g| g.policy_for(tenant_id))
    }

    /// Return the usage snapshot for a tenant, if it has been materialized.
    #[must_use]
    pub fn usage(&self, tenant_id: &str) -> Option<TenantUsage> {
        let counter = self.quota_counter();
        let guard = self.shard_for(tenant_id).read().ok()?;
        let state = guard.tenants.get(tenant_id)?;
        let granted = match counter {
            Some(counter) => counter.consumed(tenant_id),
            None => state.granted,
        };
        let quota_remaining = state.quota.map(|q| q.saturating_sub(granted));
        Some(TenantUsage {
            granted: state.granted,
            denied: state.denied,
            quota: state.quota,
            quota_remaining,
        })
    }

    /// Reset a single tenant's rate limiter and usage counters. Its configured
    /// policy (rate + quota) is preserved.
    pub fn reset_tenant(&self, tenant_id: &str) {
        if let Ok(mut guard) = self.shard_for(tenant_id).write() {
            if let Some(state) = guard.tenants.get_mut(tenant_id) {
                state.limiter.reset();
                state.granted = 0;
                state.denied = 0;
            }
        }
        if let Some(counter) = self.quota_counter() {
            counter.reset(tenant_id);
        }
    }

    /// Reset every materialized tenant's limiter and usage counters.
    pub fn reset_all(&self) {
        for shard in self.shards.iter() {
            if let Ok(mut guard) = shard.write() {
                for state in guard.tenants.values_mut() {
                    state.limiter.reset();
                    state.granted = 0;
                    state.denied = 0;
                }
            }
        }
        if let Some(counter) = self.quota_counter() {
            counter.reset_all();
        }
    }

    /// The number of tenants that currently have live state.
    #[must_use]
    pub fn tracked_count(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.read().map_or(0, |g| g.tenants.len()))
            .sum()
    }
}

impl Default for TenantRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_default_allows_unconfigured_tenants() {
        let limiter = TenantRateLimiter::new();
        for _ in 0..1000 {
            assert!(limiter.try_acquire("anyone"));
        }
        assert!(!limiter.has_rate_limit("anyone"));
    }

    #[test]
    fn per_tenant_isolation() {
        let limiter = TenantRateLimiter::new();
        // Both tenants share the same shape but have independent buckets.
        limiter.set_tenant_config("alice", RateLimitConfig::new(1000.0).with_burst(2));
        limiter.set_tenant_config("bob", RateLimitConfig::new(1000.0).with_burst(2));

        // Alice burns through her burst.
        assert!(limiter.try_acquire("alice"));
        assert!(limiter.try_acquire("alice"));
        assert!(!limiter.try_acquire("alice"));

        // Bob is completely unaffected by Alice hitting her cap.
        assert!(limiter.try_acquire("bob"));
        assert!(limiter.try_acquire("bob"));
        assert!(!limiter.try_acquire("bob"));
    }

    #[test]
    fn default_vs_override() {
        // Generous default, strict override for one tenant.
        let limiter = TenantRateLimiter::with_default(RateLimitConfig::new(1000.0).with_burst(100));
        limiter.set_tenant_config("trial", RateLimitConfig::new(1000.0).with_burst(1));

        // Trial tenant is limited to a single burst permit.
        assert!(limiter.try_acquire("trial"));
        assert!(!limiter.try_acquire("trial"));

        // A default tenant has the generous burst available.
        for _ in 0..100 {
            assert!(limiter.try_acquire("default_tenant"));
        }
        assert!(!limiter.try_acquire("default_tenant"));
    }

    #[test]
    fn default_applies_to_all_unknown_tenants() {
        let limiter = TenantRateLimiter::with_default(RateLimitConfig::new(1000.0).with_burst(2));
        assert!(limiter.has_rate_limit("whoever"));
        assert!(limiter.try_acquire("t1"));
        assert!(limiter.try_acquire("t1"));
        assert!(!limiter.try_acquire("t1"));
        // Different tenant, fresh default bucket.
        assert!(limiter.try_acquire("t2"));
    }

    #[test]
    fn quota_caps_total_permits() {
        let limiter = TenantRateLimiter::new();
        // High rate so the bucket never limits us; quota is the only ceiling.
        limiter.set_tenant_policy(
            "metered",
            TenantRateLimit::new(RateLimitConfig::new(1_000_000.0).with_burst(1000)).with_quota(3),
        );

        assert!(limiter.try_acquire("metered"));
        assert!(limiter.try_acquire("metered"));
        assert!(limiter.try_acquire("metered"));
        // Quota exhausted even though the rate limiter still has tokens.
        assert!(!limiter.try_acquire("metered"));
        assert!(!limiter.try_acquire("metered"));

        let usage = limiter.usage("metered").expect("tenant materialized");
        assert_eq!(usage.granted, 3);
        assert_eq!(usage.denied, 2);
        assert_eq!(usage.quota, Some(3));
        assert_eq!(usage.quota_remaining, Some(0));
    }

    #[test]
    fn quota_exhaustion_does_not_affect_other_tenant() {
        let limiter = TenantRateLimiter::new();
        limiter.set_tenant_policy(
            "capped",
            TenantRateLimit::new(RateLimitConfig::new(1_000_000.0).with_burst(1000)).with_quota(1),
        );
        limiter.set_tenant_config("uncapped", RateLimitConfig::new(1000.0).with_burst(5));

        assert!(limiter.try_acquire("capped"));
        assert!(!limiter.try_acquire("capped")); // over quota

        // Uncapped tenant proceeds normally.
        for _ in 0..5 {
            assert!(limiter.try_acquire("uncapped"));
        }
    }

    #[test]
    fn set_tenant_quota_preserves_rate() {
        let limiter =
            TenantRateLimiter::with_default(RateLimitConfig::new(1_000_000.0).with_burst(10));
        limiter.set_tenant_quota("vip", Some(2));
        let policy = limiter.policy_for("vip").expect("override created");
        assert_eq!(policy.quota, Some(2));
        // Rate config came from the default.
        assert_eq!(policy.config.effective_burst(), 10);

        assert!(limiter.try_acquire("vip"));
        assert!(limiter.try_acquire("vip"));
        assert!(!limiter.try_acquire("vip"));

        // Clearing the quota lets the rate config (burst 10) govern again.
        limiter.set_tenant_quota("vip", None);
        assert!(limiter.policy_for("vip").expect("override").quota.is_none());
        for _ in 0..10 {
            assert!(limiter.try_acquire("vip"));
        }
    }

    #[test]
    fn reset_tenant_restores_capacity() {
        let limiter = TenantRateLimiter::new();
        limiter.set_tenant_config("svc", RateLimitConfig::new(0.0001).with_burst(2));
        assert!(limiter.try_acquire("svc"));
        assert!(limiter.try_acquire("svc"));
        assert!(!limiter.try_acquire("svc"));

        limiter.reset_tenant("svc");
        assert!(limiter.try_acquire("svc"));

        let usage = limiter.usage("svc").expect("materialized");
        // After reset the granted count restarts; one acquire since reset.
        assert_eq!(usage.granted, 1);
    }

    #[test]
    fn reset_all_restores_all_tenants() {
        let limiter = TenantRateLimiter::new();
        limiter.set_tenant_config("a", RateLimitConfig::new(0.0001).with_burst(1));
        limiter.set_tenant_config("b", RateLimitConfig::new(0.0001).with_burst(1));
        assert!(limiter.try_acquire("a"));
        assert!(limiter.try_acquire("b"));
        assert!(!limiter.try_acquire("a"));
        assert!(!limiter.try_acquire("b"));

        limiter.reset_all();
        assert!(limiter.try_acquire("a"));
        assert!(limiter.try_acquire("b"));
    }

    #[test]
    fn remove_tenant_falls_back_to_default() {
        let limiter = TenantRateLimiter::with_default(RateLimitConfig::new(1000.0).with_burst(5));
        // The override's rate has to be slow enough that the bucket cannot
        // refill between the two acquires below. At 1000/s — which this used to
        // use — a single millisecond of scheduling delay hands back a whole
        // token, so "strict override active" failed intermittently under a
        // loaded parallel test run. The sibling `reset_all_restores_all_tenants`
        // uses the same near-zero rate for the same reason.
        limiter.set_tenant_config("temp", RateLimitConfig::new(0.0001).with_burst(1));
        assert!(limiter.try_acquire("temp"));
        assert!(!limiter.try_acquire("temp")); // strict override active

        limiter.remove_tenant("temp");
        // Now governed by the generous default again.
        let policy = limiter.policy_for("temp").expect("default applies");
        assert_eq!(policy.config.effective_burst(), 5);
        for _ in 0..5 {
            assert!(limiter.try_acquire("temp"));
        }
    }

    #[test]
    fn sliding_window_tenant_is_supported() {
        let limiter = TenantRateLimiter::new();
        // Sliding-window cap is `rate * window_size`; 3/s over a 1s window == 3.
        limiter.set_tenant_config("sw", RateLimitConfig::new(3.0).with_sliding_window(1));
        assert!(limiter.try_acquire("sw"));
        assert!(limiter.try_acquire("sw"));
        assert!(limiter.try_acquire("sw"));
        assert!(!limiter.try_acquire("sw"));
    }

    #[test]
    fn usage_absent_for_unmaterialized_tenant() {
        let limiter = TenantRateLimiter::with_default(RateLimitConfig::new(10.0));
        assert!(limiter.usage("never_seen").is_none());
        assert_eq!(limiter.tracked_count(), 0);
        let _ = limiter.try_acquire("now_seen");
        assert_eq!(limiter.tracked_count(), 1);
        assert!(limiter.usage("now_seen").is_some());
    }

    #[test]
    fn time_until_available_for_exhausted_quota_is_max() {
        let limiter = TenantRateLimiter::new();
        limiter.set_tenant_policy(
            "q",
            TenantRateLimit::new(RateLimitConfig::new(1_000_000.0).with_burst(100)).with_quota(1),
        );
        assert!(limiter.try_acquire("q"));
        assert!(!limiter.try_acquire("q"));
        assert_eq!(limiter.time_until_available("q"), Duration::MAX);
    }

    #[test]
    fn has_capacity_reflects_state() {
        let limiter = TenantRateLimiter::new();
        limiter.set_tenant_config("c", RateLimitConfig::new(0.0001).with_burst(1));
        // Unmaterialized but configured with burst -> reports capacity.
        assert!(limiter.has_capacity("c"));
        assert!(limiter.try_acquire("c"));
        // Bucket now empty (rate is effectively zero over the test window).
        assert!(!limiter.has_capacity("c"));
    }

    #[test]
    fn policy_from_config_conversion() {
        let policy: TenantRateLimit = RateLimitConfig::new(50.0).into();
        assert!(policy.quota.is_none());
        assert!((policy.config.rate - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn tenant_rate_limit_serde_round_trip() {
        let policy =
            TenantRateLimit::new(RateLimitConfig::new(25.0).with_burst(50)).with_quota(500);
        let json = serde_json::to_string(&policy).expect("serialize");
        let parsed: TenantRateLimit = serde_json::from_str(&json).expect("deserialize");
        assert_eq!(parsed.quota, Some(500));
        assert_eq!(parsed.config.burst, Some(50));
    }

    #[test]
    fn concurrent_tenants_respect_caps() {
        use std::thread;

        let limiter = TenantRateLimiter::new();
        // Very low rate so tokens do not regenerate mid-test.
        limiter.set_tenant_config("shared", RateLimitConfig::new(0.001).with_burst(10));

        let handles: Vec<_> = (0..4)
            .map(|_| {
                let l = limiter.clone();
                thread::spawn(move || {
                    let mut count = 0;
                    for _ in 0..10 {
                        if l.try_acquire("shared") {
                            count += 1;
                        }
                    }
                    count
                })
            })
            .collect();

        let total: usize = handles
            .into_iter()
            .map(|h| h.join().expect("thread joins"))
            .sum();
        // No more than the burst capacity may ever be granted.
        assert!(total <= 10, "granted {total} permits, expected <= 10");
    }

    // ------------------------------------------------------------------
    // Regression tests
    // ------------------------------------------------------------------

    /// Regression: the registry inserted a `TenantState` for every previously
    /// unseen tenant id whenever a default policy existed, with no bound and no
    /// eviction — a memory-exhaustion vector, since tenant ids are
    /// attacker-influenced.
    #[test]
    fn tenant_registry_is_capacity_bounded() {
        let limiter = TenantRateLimiter::with_default(RateLimitConfig::new(1000.0).with_burst(5))
            .with_max_tracked_tenants(SHARD_COUNT * 2);

        for i in 0..5_000 {
            assert!(limiter.try_acquire(&format!("tenant-{i}")));
        }

        let tracked = limiter.tracked_count();
        assert!(
            tracked <= SHARD_COUNT * 2,
            "registry grew to {tracked} tenants despite the bound"
        );
        assert!(tracked > 0);
    }

    #[test]
    fn sweep_idle_reclaims_tenants() {
        let limiter = TenantRateLimiter::with_default(RateLimitConfig::new(1000.0).with_burst(5));
        for i in 0..25 {
            assert!(limiter.try_acquire(&format!("tenant-{i}")));
        }
        assert_eq!(limiter.tracked_count(), 25);

        // A generous window keeps everything.
        assert_eq!(limiter.sweep_idle(Duration::from_secs(3600)), 0);
        assert_eq!(limiter.tracked_count(), 25);

        // A zero window reclaims everything.
        assert_eq!(limiter.sweep_idle(Duration::ZERO), 25);
        assert_eq!(limiter.tracked_count(), 0);

        // ...and the tenants simply start over.
        assert!(limiter.try_acquire("tenant-0"));
        assert_eq!(limiter.tracked_count(), 1);
    }

    /// Regression: `set_tenant_quota` removed the live state, wiping `granted`,
    /// so every quota update handed the tenant a free full-quota reset.
    #[test]
    fn set_tenant_quota_preserves_accrued_usage() {
        let limiter = TenantRateLimiter::new();
        limiter.set_tenant_policy(
            "acme",
            TenantRateLimit::new(RateLimitConfig::new(1000.0).with_burst(100)).with_quota(5),
        );

        for _ in 0..3 {
            assert!(limiter.try_acquire("acme"));
        }
        let usage = limiter.usage("acme").expect("tenant materialized");
        assert_eq!(usage.granted, 3);
        assert_eq!(usage.quota_remaining, Some(2));

        // Raising the quota must not reset what has already been consumed.
        limiter.set_tenant_quota("acme", Some(4));
        let usage = limiter.usage("acme").expect("tenant still materialized");
        assert_eq!(
            usage.granted, 3,
            "accrued usage must survive a quota update"
        );
        assert_eq!(usage.quota, Some(4));
        assert_eq!(usage.quota_remaining, Some(1));

        // Only one permit remains before the new ceiling.
        assert!(limiter.try_acquire("acme"));
        assert!(!limiter.try_acquire("acme"));

        // Clearing the quota lifts the ceiling without resetting usage.
        limiter.set_tenant_quota("acme", None);
        let usage = limiter.usage("acme").expect("tenant still materialized");
        assert_eq!(usage.granted, 4);
        assert_eq!(usage.quota, None);
        assert!(limiter.try_acquire("acme"));
    }

    /// Regression: the cumulative quota was documented as a hard ceiling but was
    /// process-local with no way to make it authoritative.
    #[test]
    fn shared_quota_counter_is_authoritative() {
        let counter = Arc::new(InProcessQuotaCounter::new());

        // Two "worker processes" sharing one counter.
        let worker_a = TenantRateLimiter::new().with_quota_counter(counter.clone());
        let worker_b = TenantRateLimiter::new().with_quota_counter(counter.clone());
        assert!(worker_a.has_shared_quota_counter());

        let policy =
            TenantRateLimit::new(RateLimitConfig::new(1000.0).with_burst(100)).with_quota(3);
        worker_a.set_tenant_policy("acme", policy.clone());
        worker_b.set_tenant_policy("acme", policy);

        assert!(worker_a.try_acquire("acme"));
        assert!(worker_b.try_acquire("acme"));
        assert!(worker_a.try_acquire("acme"));
        // The quota of 3 is now spent cluster-wide, not per process.
        assert!(!worker_b.try_acquire("acme"));
        assert!(!worker_a.try_acquire("acme"));
        assert_eq!(counter.consumed("acme"), 3);

        // Without a shared counter each limiter would grant the full quota.
        let solo_a = TenantRateLimiter::new();
        let solo_b = TenantRateLimiter::new();
        let policy =
            TenantRateLimit::new(RateLimitConfig::new(1000.0).with_burst(100)).with_quota(3);
        solo_a.set_tenant_policy("acme", policy.clone());
        solo_b.set_tenant_policy("acme", policy);
        for _ in 0..3 {
            assert!(solo_a.try_acquire("acme"));
            assert!(solo_b.try_acquire("acme"));
        }
        assert!(!solo_a.try_acquire("acme"));
        assert!(!solo_b.try_acquire("acme"));
        assert!(!solo_a.has_shared_quota_counter());
    }

    #[test]
    fn shared_quota_counter_is_reset_with_the_tenant() {
        let counter = Arc::new(InProcessQuotaCounter::new());
        let limiter = TenantRateLimiter::new().with_quota_counter(counter.clone());
        limiter.set_tenant_policy(
            "acme",
            TenantRateLimit::new(RateLimitConfig::new(1000.0).with_burst(100)).with_quota(2),
        );

        assert!(limiter.try_acquire("acme"));
        assert!(limiter.try_acquire("acme"));
        assert!(!limiter.try_acquire("acme"));

        limiter.reset_tenant("acme");
        assert_eq!(counter.consumed("acme"), 0);
        assert!(limiter.try_acquire("acme"));

        limiter.reset_all();
        assert_eq!(counter.consumed("acme"), 0);

        limiter.remove_tenant("acme");
        assert_eq!(counter.consumed("acme"), 0);
    }

    /// Regression: every admission decision took one global write lock, so all
    /// tenants were serialised against each other. Distinct tenants must be able
    /// to make progress concurrently.
    #[test]
    fn distinct_tenants_use_independent_locks() {
        use std::thread;

        let limiter = TenantRateLimiter::with_default(RateLimitConfig::new(1e9).with_burst(10_000));

        let handles: Vec<_> = (0..8)
            .map(|worker| {
                let l = limiter.clone();
                thread::spawn(move || {
                    let mut granted = 0usize;
                    for i in 0..200 {
                        if l.try_acquire(&format!("tenant-{worker}-{i}")) {
                            granted += 1;
                        }
                    }
                    granted
                })
            })
            .collect();

        let total: usize = handles
            .into_iter()
            .map(|h| h.join().expect("thread joins"))
            .sum();
        assert_eq!(total, 8 * 200);
    }

    #[test]
    fn tenant_shard_assignment_is_stable() {
        let limiter = TenantRateLimiter::with_default(RateLimitConfig::new(1000.0).with_burst(2));
        // Repeated operations for the same id must reach the same shard, or the
        // state would appear to reset between calls.
        assert!(limiter.try_acquire("stable"));
        assert!(limiter.try_acquire("stable"));
        assert!(!limiter.try_acquire("stable"));
        assert_eq!(
            limiter.usage("stable").map(|u| u.granted),
            Some(2),
            "state must live in a single, stable shard"
        );
    }
}
