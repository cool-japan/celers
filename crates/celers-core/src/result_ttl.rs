//! Per-task-type result TTL configuration.
//!
//! Celery lets the result backend expire stored results after a global
//! `result_expires` window. In practice different task types want different
//! retention: a cheap idempotent task might keep results for minutes while an
//! expensive report task keeps them for days.
//!
//! [`ResultTtlConfig`] maps a task **name/type** to a [`std::time::Duration`]
//! TTL, with a configurable default fallback used when a task has no explicit
//! override. It is a pure, additive configuration type — no I/O, no changes to
//! existing types.
//!
//! # Example
//!
//! ```rust
//! use celers_core::result_ttl::ResultTtlConfig;
//! use std::time::Duration;
//!
//! let ttl = ResultTtlConfig::with_default(Duration::from_secs(3600))
//!     .with_task_ttl("reports.generate", Duration::from_secs(86_400))
//!     .with_task_ttl("ping", Duration::from_secs(60));
//!
//! // Explicit overrides win.
//! assert_eq!(ttl.ttl_for("reports.generate"), Some(Duration::from_secs(86_400)));
//! assert_eq!(ttl.ttl_for("ping"), Some(Duration::from_secs(60)));
//!
//! // Unknown task -> default fallback.
//! assert_eq!(ttl.ttl_for("anything.else"), Some(Duration::from_secs(3600)));
//! ```

use crate::TaskId;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Configuration mapping task name/type to a result TTL, with a default
/// fallback.
///
/// Lookups (`ttl_for`) return the per-task override when present, otherwise the
/// configured default. A value of `None` for the default means "no expiry"
/// (results are kept indefinitely unless a per-task override applies).
///
/// TTLs are stored as [`Duration`] but (de)serialized as whole seconds via
/// `u64`, matching the convention used by the broader `CeleryConfig`
/// (`result_expires` is `u64` seconds).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ResultTtlConfig {
    /// Default TTL in seconds applied when no per-task override matches.
    ///
    /// `None` means results never expire by default.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    default_ttl_secs: Option<u64>,

    /// Per-task-name TTL overrides, in seconds.
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    per_task_ttl_secs: HashMap<String, u64>,
}

impl ResultTtlConfig {
    /// Create an empty configuration with no default and no overrides.
    ///
    /// With this configuration every lookup returns `None` ("never expires").
    #[must_use]
    pub fn new() -> Self {
        Self {
            default_ttl_secs: None,
            per_task_ttl_secs: HashMap::new(),
        }
    }

    /// Create a configuration with the given default fallback TTL.
    #[must_use]
    pub fn with_default(default_ttl: Duration) -> Self {
        Self {
            default_ttl_secs: Some(default_ttl.as_secs()),
            per_task_ttl_secs: HashMap::new(),
        }
    }

    /// Set the default fallback TTL (builder style).
    #[must_use]
    pub fn set_default(mut self, default_ttl: Duration) -> Self {
        self.default_ttl_secs = Some(default_ttl.as_secs());
        self
    }

    /// Remove the default fallback TTL so unmatched tasks never expire
    /// (builder style).
    #[must_use]
    pub fn without_default(mut self) -> Self {
        self.default_ttl_secs = None;
        self
    }

    /// Add or replace a per-task TTL override (builder style).
    #[must_use]
    pub fn with_task_ttl(mut self, task_name: impl Into<String>, ttl: Duration) -> Self {
        self.per_task_ttl_secs
            .insert(task_name.into(), ttl.as_secs());
        self
    }

    /// Add or replace a per-task TTL override (mutating).
    pub fn insert_task_ttl(&mut self, task_name: impl Into<String>, ttl: Duration) {
        self.per_task_ttl_secs
            .insert(task_name.into(), ttl.as_secs());
    }

    /// Remove a per-task TTL override, returning the previous value if present.
    pub fn remove_task_ttl(&mut self, task_name: &str) -> Option<Duration> {
        self.per_task_ttl_secs
            .remove(task_name)
            .map(Duration::from_secs)
    }

    /// The default fallback TTL, if configured.
    #[must_use]
    pub fn default_ttl(&self) -> Option<Duration> {
        self.default_ttl_secs.map(Duration::from_secs)
    }

    /// The explicit per-task override for `task_name`, ignoring the default.
    #[must_use]
    pub fn task_override(&self, task_name: &str) -> Option<Duration> {
        self.per_task_ttl_secs
            .get(task_name)
            .copied()
            .map(Duration::from_secs)
    }

    /// Returns `true` if an explicit override exists for `task_name`.
    #[must_use]
    pub fn has_override(&self, task_name: &str) -> bool {
        self.per_task_ttl_secs.contains_key(task_name)
    }

    /// Number of per-task overrides configured.
    #[must_use]
    pub fn override_count(&self) -> usize {
        self.per_task_ttl_secs.len()
    }

    /// Returns `true` if there are no overrides and no default configured.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.default_ttl_secs.is_none() && self.per_task_ttl_secs.is_empty()
    }

    /// Resolve the effective TTL for `task_name`.
    ///
    /// Returns the per-task override if present, otherwise the configured
    /// default. Returns `None` when neither is set, meaning "never expires".
    #[must_use]
    pub fn ttl_for(&self, task_name: &str) -> Option<Duration> {
        self.task_override(task_name).or_else(|| self.default_ttl())
    }

    /// Resolve the effective TTL for `task_name`, falling back to `fallback`
    /// when neither a per-task override nor a configured default applies.
    #[must_use]
    pub fn ttl_for_or(&self, task_name: &str, fallback: Duration) -> Duration {
        self.ttl_for(task_name).unwrap_or(fallback)
    }

    /// Compute the absolute expiration timestamp for a result of `task_name`
    /// created at `created_at`.
    ///
    /// Returns `None` when the effective TTL is "never expires".
    #[must_use]
    pub fn expires_at(&self, task_name: &str, created_at: DateTime<Utc>) -> Option<DateTime<Utc>> {
        let ttl = self.ttl_for(task_name)?;
        chrono::Duration::from_std(ttl)
            .ok()
            .map(|delta| created_at + delta)
    }

    /// Compute the expiration timestamp relative to "now".
    ///
    /// Returns `None` when the effective TTL is "never expires".
    #[must_use]
    pub fn expires_from_now(&self, task_name: &str) -> Option<DateTime<Utc>> {
        self.expires_at(task_name, Utc::now())
    }
}

/// A per-task-type TTL keyed by task **type** rather than dynamic instance.
///
/// This is a thin convenience wrapper that pairs a resolved TTL with the
/// concrete [`TaskId`] it applies to, computed at result-store time. It exists
/// so backends can carry the resolved expiry alongside the result without
/// re-resolving the config.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedResultTtl {
    /// The task instance the TTL applies to.
    pub task_id: TaskId,

    /// The task name/type used to resolve the TTL.
    pub task_name: String,

    /// The resolved TTL, or `None` for "never expires".
    pub ttl: Option<Duration>,
}

impl ResolvedResultTtl {
    /// Resolve a TTL for a concrete task instance using `config`.
    #[must_use]
    pub fn resolve(
        config: &ResultTtlConfig,
        task_id: TaskId,
        task_name: impl Into<String>,
    ) -> Self {
        let task_name = task_name.into();
        let ttl = config.ttl_for(&task_name);
        Self {
            task_id,
            task_name,
            ttl,
        }
    }

    /// Returns `true` if this resolved TTL means "never expires".
    #[inline]
    #[must_use]
    pub const fn never_expires(&self) -> bool {
        self.ttl.is_none()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn empty_config_returns_none() {
        let config = ResultTtlConfig::new();
        assert!(config.is_empty());
        assert_eq!(config.ttl_for("anything"), None);
        assert_eq!(config.default_ttl(), None);
        assert_eq!(config.override_count(), 0);
    }

    #[test]
    fn default_fallback_used_for_unknown_task() {
        let config = ResultTtlConfig::with_default(Duration::from_secs(3600));
        assert_eq!(
            config.ttl_for("unknown.task"),
            Some(Duration::from_secs(3600))
        );
        assert!(!config.has_override("unknown.task"));
    }

    #[test]
    fn per_task_override_wins_over_default() {
        let config = ResultTtlConfig::with_default(Duration::from_secs(60))
            .with_task_ttl("reports.generate", Duration::from_secs(86_400));
        assert!(config.has_override("reports.generate"));
        assert_eq!(
            config.ttl_for("reports.generate"),
            Some(Duration::from_secs(86_400))
        );
        // Different task still uses default.
        assert_eq!(config.ttl_for("other"), Some(Duration::from_secs(60)));
    }

    #[test]
    fn no_default_means_never_expires() {
        let config = ResultTtlConfig::new().with_task_ttl("a", Duration::from_secs(10));
        assert_eq!(config.ttl_for("a"), Some(Duration::from_secs(10)));
        // No default => unknown task never expires.
        assert_eq!(config.ttl_for("b"), None);
    }

    #[test]
    fn ttl_for_or_fallback() {
        let config = ResultTtlConfig::new();
        assert_eq!(
            config.ttl_for_or("x", Duration::from_secs(5)),
            Duration::from_secs(5)
        );
        let config = config.with_task_ttl("x", Duration::from_secs(99));
        assert_eq!(
            config.ttl_for_or("x", Duration::from_secs(5)),
            Duration::from_secs(99)
        );
    }

    #[test]
    fn insert_and_remove_override() {
        let mut config = ResultTtlConfig::with_default(Duration::from_secs(30));
        config.insert_task_ttl("task.a", Duration::from_secs(120));
        assert_eq!(
            config.task_override("task.a"),
            Some(Duration::from_secs(120))
        );

        let removed = config.remove_task_ttl("task.a");
        assert_eq!(removed, Some(Duration::from_secs(120)));
        // Falls back to default after removal.
        assert_eq!(config.ttl_for("task.a"), Some(Duration::from_secs(30)));
        assert!(config.remove_task_ttl("missing").is_none());
    }

    #[test]
    fn set_and_without_default() {
        let config = ResultTtlConfig::new().set_default(Duration::from_secs(7));
        assert_eq!(config.default_ttl(), Some(Duration::from_secs(7)));
        let config = config.without_default();
        assert_eq!(config.default_ttl(), None);
    }

    #[test]
    fn expires_at_computation() {
        let config = ResultTtlConfig::with_default(Duration::from_secs(3600));
        let created = Utc::now();
        let expires = config.expires_at("task", created).unwrap();
        let delta = (expires - created).num_seconds();
        assert_eq!(delta, 3600);

        // No-expiry config returns None.
        let none_config = ResultTtlConfig::new();
        assert!(none_config.expires_at("task", created).is_none());
        assert!(config.expires_from_now("task").is_some());
    }

    #[test]
    fn serde_round_trip() {
        let config = ResultTtlConfig::with_default(Duration::from_secs(100))
            .with_task_ttl("a", Duration::from_secs(200))
            .with_task_ttl("b", Duration::from_secs(300));
        let json = serde_json::to_string(&config).unwrap();
        let back: ResultTtlConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(back.ttl_for("a"), Some(Duration::from_secs(200)));
        assert_eq!(back.ttl_for("b"), Some(Duration::from_secs(300)));
        assert_eq!(back.ttl_for("c"), Some(Duration::from_secs(100)));
    }

    #[test]
    fn resolved_ttl() {
        let config = ResultTtlConfig::with_default(Duration::from_secs(60))
            .with_task_ttl("big", Duration::from_secs(1000));
        let id = Uuid::new_v4();

        let resolved = ResolvedResultTtl::resolve(&config, id, "big");
        assert_eq!(resolved.task_id, id);
        assert_eq!(resolved.ttl, Some(Duration::from_secs(1000)));
        assert!(!resolved.never_expires());

        let resolved = ResolvedResultTtl::resolve(&config, id, "small");
        assert_eq!(resolved.ttl, Some(Duration::from_secs(60)));

        let empty = ResultTtlConfig::new();
        let resolved = ResolvedResultTtl::resolve(&empty, id, "x");
        assert!(resolved.never_expires());
    }
}
