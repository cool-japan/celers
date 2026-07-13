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
//! the rate configuration and an optional cumulative *quota* — a hard ceiling on
//! the total number of permits a tenant may ever consume (useful for billing
//! plans or trial limits).
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

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::rate_limit::{create_rate_limiter, RateLimitConfig, RateLimiter};

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
    /// Total permits granted to this tenant so far.
    granted: u64,
    /// Total requests denied for this tenant so far (rate-limited or over quota).
    denied: u64,
}

impl TenantState {
    fn from_policy(policy: &TenantRateLimit) -> Self {
        Self {
            limiter: create_rate_limiter(policy.config.clone()),
            quota: policy.quota,
            granted: 0,
            denied: 0,
        }
    }

    /// Returns `true` if this tenant has already reached its cumulative quota.
    fn quota_exhausted(&self) -> bool {
        matches!(self.quota, Some(limit) if self.granted >= limit)
    }

    fn try_acquire(&mut self) -> bool {
        if self.quota_exhausted() {
            self.denied += 1;
            return false;
        }
        if self.limiter.try_acquire() {
            self.granted += 1;
            true
        } else {
            self.denied += 1;
            false
        }
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

/// The inner, lock-free registry holding all tenant state and configuration.
struct TenantRegistry {
    /// Default policy for tenants without an explicit override.
    default_policy: Option<TenantRateLimit>,
    /// Per-tenant policy overrides (used to (re)build a tenant's limiter).
    overrides: HashMap<String, TenantRateLimit>,
    /// Live per-tenant state.
    tenants: HashMap<String, TenantState>,
}

impl TenantRegistry {
    fn new(default_policy: Option<TenantRateLimit>) -> Self {
        Self {
            default_policy,
            overrides: HashMap::new(),
            tenants: HashMap::new(),
        }
    }

    /// Resolve the effective policy for a tenant (override wins over default).
    fn policy_for(&self, tenant_id: &str) -> Option<TenantRateLimit> {
        self.overrides
            .get(tenant_id)
            .cloned()
            .or_else(|| self.default_policy.clone())
    }

    /// Try to acquire a permit for a tenant, lazily creating its state.
    fn try_acquire(&mut self, tenant_id: &str) -> bool {
        if let Some(state) = self.tenants.get_mut(tenant_id) {
            return state.try_acquire();
        }
        // Tenant not yet seen: resolve its policy (override or default).
        let Some(policy) = self.policy_for(tenant_id) else {
            // No rate limit configured for this tenant at all -> always allow.
            return true;
        };
        let mut state = TenantState::from_policy(&policy);
        let granted = state.try_acquire();
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
#[derive(Clone)]
pub struct TenantRateLimiter {
    inner: Arc<RwLock<TenantRegistry>>,
}

impl std::fmt::Debug for TenantRateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut dbg = f.debug_struct("TenantRateLimiter");
        if let Ok(guard) = self.inner.read() {
            dbg.field("tracked_tenants", &guard.tenants.len())
                .field("overrides", &guard.overrides.len())
                .field("has_default", &guard.default_policy.is_some());
        }
        dbg.finish()
    }
}

impl TenantRateLimiter {
    /// Create a limiter with no default policy. Tenants without an explicit
    /// override are *not* rate limited (every request is allowed).
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(TenantRegistry::new(None))),
        }
    }

    /// Create a limiter whose default [`RateLimitConfig`] (without a quota) is
    /// applied to any tenant lacking an explicit override.
    #[must_use]
    pub fn with_default(config: RateLimitConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TenantRegistry::new(Some(
                TenantRateLimit::new(config),
            )))),
        }
    }

    /// Create a limiter whose default policy (rate *and* optional quota) is
    /// applied to any tenant lacking an explicit override.
    #[must_use]
    pub fn with_default_policy(policy: TenantRateLimit) -> Self {
        Self {
            inner: Arc::new(RwLock::new(TenantRegistry::new(Some(policy)))),
        }
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
        if let Ok(mut guard) = self.inner.write() {
            guard.tenants.remove(&id);
            guard.overrides.insert(id, policy);
        }
    }

    /// Set (or update) just the cumulative quota for a tenant, keeping its
    /// effective rate configuration. Pass `None` to clear the quota.
    ///
    /// If the tenant had no override yet, the current effective configuration
    /// (its override or the default) is captured as the basis for the override.
    pub fn set_tenant_quota(&self, tenant_id: impl Into<String>, quota: Option<u64>) {
        let id = tenant_id.into();
        if let Ok(mut guard) = self.inner.write() {
            let base = guard
                .policy_for(&id)
                .unwrap_or_else(|| TenantRateLimit::new(RateLimitConfig::default()));
            let policy = TenantRateLimit {
                config: base.config,
                quota,
            };
            guard.tenants.remove(&id);
            guard.overrides.insert(id, policy);
        }
    }

    /// Remove a tenant's override and any live state. The next request for this
    /// tenant is served by the default policy (if any).
    pub fn remove_tenant(&self, tenant_id: &str) {
        if let Ok(mut guard) = self.inner.write() {
            guard.overrides.remove(tenant_id);
            guard.tenants.remove(tenant_id);
        }
    }

    /// Try to acquire a single permit for a tenant.
    ///
    /// Returns `true` if the request is allowed, `false` if the tenant is rate
    /// limited or has exhausted its cumulative quota. Tenants with no override
    /// and no default policy are always allowed.
    #[must_use]
    pub fn try_acquire(&self, tenant_id: &str) -> bool {
        if let Ok(mut guard) = self.inner.write() {
            guard.try_acquire(tenant_id)
        } else {
            // Poisoned lock: fail open so a panic elsewhere does not block work.
            true
        }
    }

    /// Returns `true` if a tenant currently has at least one permit available
    /// (and has not exhausted its quota) without consuming anything.
    #[must_use]
    pub fn has_capacity(&self, tenant_id: &str) -> bool {
        if let Ok(guard) = self.inner.read() {
            if let Some(state) = guard.tenants.get(tenant_id) {
                return !state.quota_exhausted() && state.limiter.available_permits() > 0;
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
        if let Ok(guard) = self.inner.read() {
            if let Some(state) = guard.tenants.get(tenant_id) {
                if state.quota_exhausted() {
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
        if let Ok(guard) = self.inner.read() {
            guard.overrides.contains_key(tenant_id) || guard.default_policy.is_some()
        } else {
            false
        }
    }

    /// Return the effective policy for a tenant (override or default), if any.
    #[must_use]
    pub fn policy_for(&self, tenant_id: &str) -> Option<TenantRateLimit> {
        self.inner.read().ok().and_then(|g| g.policy_for(tenant_id))
    }

    /// Return the usage snapshot for a tenant, if it has been materialized.
    #[must_use]
    pub fn usage(&self, tenant_id: &str) -> Option<TenantUsage> {
        let guard = self.inner.read().ok()?;
        let state = guard.tenants.get(tenant_id)?;
        let quota_remaining = state.quota.map(|q| q.saturating_sub(state.granted));
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
        if let Ok(mut guard) = self.inner.write() {
            if let Some(state) = guard.tenants.get_mut(tenant_id) {
                state.limiter.reset();
                state.granted = 0;
                state.denied = 0;
            }
        }
    }

    /// Reset every materialized tenant's limiter and usage counters.
    pub fn reset_all(&self) {
        if let Ok(mut guard) = self.inner.write() {
            for state in guard.tenants.values_mut() {
                state.limiter.reset();
                state.granted = 0;
                state.denied = 0;
            }
        }
    }

    /// The number of tenants that currently have live state.
    #[must_use]
    pub fn tracked_count(&self) -> usize {
        self.inner.read().map(|g| g.tenants.len()).unwrap_or(0)
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
        limiter.set_tenant_config("temp", RateLimitConfig::new(1000.0).with_burst(1));
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
}
