//! Distributed rate-limit coordination for the worker.
//!
//! This module wires `celers-core`'s cluster-wide rate limiter
//! ([`celers_core::rate_limit_distributed::DistributedRateLimiter`] backed by a
//! [`celers_core::rate_limit_distributed::DistributedRateLimitBackend`]) into the
//! worker so that, **before** executing a task, the worker acquires a permit from
//! a *shared* limiter keyed by task name (or queue). When the shared limiter
//! denies the request the worker can either *defer* the task (requeue so another
//! attempt happens later, respecting the suggested retry delay) or *skip* it.
//!
//! Unlike the per-worker [`crate::rate_limit`] limiters, the limiter here is
//! backed by a store shared across every worker, so the configured rate is
//! enforced cluster-wide. The in-memory backend
//! ([`celers_core::rate_limit_distributed::InMemoryDistributedBackend`]) makes
//! this fully testable in-process; a Redis backend implementing the same
//! [`DistributedRateLimitBackend`] trait slots in later without any change here.
//!
//! # Keying
//!
//! Limiters are created lazily per key and cached. The default key is the task
//! name, so all workers throttle a given task type together. A custom
//! [`RateLimitKeyStrategy`] can key by queue instead (or a constant global key)
//! to throttle whole queues.
//!
//! # Example
//!
//! ```rust
//! use celers_worker::coordinated_rate_limit::{
//!     RateLimitDecision, WorkerRateLimitCoordinator,
//! };
//! use celers_core::rate_limit::RateLimitConfig;
//! use celers_core::rate_limit_distributed::InMemoryDistributedBackend;
//! use std::sync::Arc;
//!
//! # async fn example() -> celers_core::Result<()> {
//! let backend = Arc::new(InMemoryDistributedBackend::new());
//! // Burst of 2, refilling 1/sec, shared cluster-wide and applied per task name.
//! let config = RateLimitConfig::new(1.0).with_burst(2);
//! let coordinator = WorkerRateLimitCoordinator::new(backend, config);
//!
//! assert!(matches!(coordinator.acquire("send_email", "celery").await?, RateLimitDecision::Allowed));
//! assert!(matches!(coordinator.acquire("send_email", "celery").await?, RateLimitDecision::Allowed));
//! // Third within the burst is denied with a retry hint.
//! assert!(matches!(coordinator.acquire("send_email", "celery").await?, RateLimitDecision::Denied { .. }));
//! # Ok(())
//! # }
//! ```

use celers_core::rate_limit::RateLimitConfig;
use celers_core::rate_limit_distributed::{DistributedRateLimitBackend, DistributedRateLimiter};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, trace};

/// How a task's limiter key is derived from its name and queue.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum RateLimitKeyStrategy {
    /// Key by task name — every worker throttles a given task type together.
    #[default]
    TaskName,
    /// Key by queue name — throttle a whole queue cluster-wide.
    Queue,
    /// Use a single global key for all tasks — one shared budget for the cluster.
    Global,
}

impl RateLimitKeyStrategy {
    /// Compute the limiter key for a task with `task_name` arriving on `queue`.
    #[must_use]
    pub fn key_for(self, task_name: &str, queue: &str) -> String {
        match self {
            RateLimitKeyStrategy::TaskName => task_name.to_string(),
            RateLimitKeyStrategy::Queue => queue.to_string(),
            RateLimitKeyStrategy::Global => "__global__".to_string(),
        }
    }
}

/// The outcome of asking the coordinator whether a task may run now.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RateLimitDecision {
    /// A permit was acquired; the task may execute.
    Allowed,
    /// The shared limiter denied the request. `retry_after` is the limiter's
    /// estimate of when a permit will be available, so the caller can defer the
    /// task (requeue) instead of busy-looping.
    Denied {
        /// Suggested delay before the next attempt.
        retry_after: Duration,
        /// Approximate permits currently available for this key.
        remaining: f64,
    },
}

impl RateLimitDecision {
    /// Whether the task is allowed to execute.
    #[inline]
    #[must_use]
    pub fn is_allowed(&self) -> bool {
        matches!(self, RateLimitDecision::Allowed)
    }

    /// Whether the task was denied (rate limited).
    #[inline]
    #[must_use]
    pub fn is_denied(&self) -> bool {
        matches!(self, RateLimitDecision::Denied { .. })
    }

    /// The suggested retry delay when denied (`Duration::ZERO` when allowed).
    #[inline]
    #[must_use]
    pub fn retry_after(&self) -> Duration {
        match self {
            RateLimitDecision::Allowed => Duration::ZERO,
            RateLimitDecision::Denied { retry_after, .. } => *retry_after,
        }
    }
}

/// Coordinates cluster-wide rate limiting for a worker.
///
/// Holds a shared [`DistributedRateLimitBackend`] and a base
/// [`RateLimitConfig`], plus optional per-key configuration overrides. Limiter
/// handles are created lazily per key and cached so repeated acquisitions for the
/// same key reuse one handle (and therefore one consistent algorithm + config).
///
/// Cloning is cheap: the backend, cache and config are all shared behind `Arc`s.
#[derive(Clone)]
pub struct WorkerRateLimitCoordinator {
    backend: Arc<dyn DistributedRateLimitBackend>,
    default_config: Arc<RateLimitConfig>,
    overrides: Arc<HashMap<String, RateLimitConfig>>,
    limiters: Arc<Mutex<HashMap<String, DistributedRateLimiter>>>,
    key_strategy: RateLimitKeyStrategy,
    cost: f64,
}

impl WorkerRateLimitCoordinator {
    /// Create a coordinator over `backend` using `default_config` for every key,
    /// keyed by task name.
    #[must_use]
    pub fn new(
        backend: Arc<dyn DistributedRateLimitBackend>,
        default_config: RateLimitConfig,
    ) -> Self {
        Self {
            backend,
            default_config: Arc::new(default_config),
            overrides: Arc::new(HashMap::new()),
            limiters: Arc::new(Mutex::new(HashMap::new())),
            key_strategy: RateLimitKeyStrategy::TaskName,
            cost: 1.0,
        }
    }

    /// Set the key derivation strategy (defaults to [`RateLimitKeyStrategy::TaskName`]).
    #[must_use]
    pub fn with_key_strategy(mut self, strategy: RateLimitKeyStrategy) -> Self {
        self.key_strategy = strategy;
        self
    }

    /// Set the permit cost charged per task (defaults to `1.0`).
    ///
    /// Costs `<= 0` are clamped to a tiny positive value so a single task always
    /// consumes *some* budget.
    #[must_use]
    pub fn with_cost(mut self, cost: f64) -> Self {
        self.cost = if cost > 0.0 { cost } else { f64::EPSILON };
        self
    }

    /// Provide a per-key configuration override.
    ///
    /// Tasks whose derived key equals `key` use `config` instead of the default.
    /// Builder-style; call multiple times to register several overrides.
    #[must_use]
    pub fn with_key_config(mut self, key: impl Into<String>, config: RateLimitConfig) -> Self {
        Arc::make_mut(&mut self.overrides).insert(key.into(), config);
        self
    }

    /// The key strategy in use.
    #[must_use]
    pub fn key_strategy(&self) -> RateLimitKeyStrategy {
        self.key_strategy
    }

    /// The permit cost charged per task.
    #[must_use]
    pub fn cost(&self) -> f64 {
        self.cost
    }

    /// The configuration that applies to `key` (override if present, else default).
    fn config_for(&self, key: &str) -> RateLimitConfig {
        self.overrides
            .get(key)
            .cloned()
            .unwrap_or_else(|| (*self.default_config).clone())
    }

    /// Get (or lazily create and cache) the limiter handle for `key`.
    async fn limiter_for(&self, key: &str) -> DistributedRateLimiter {
        let mut guard = self.limiters.lock().await;
        if let Some(limiter) = guard.get(key) {
            return limiter.clone();
        }
        let config = self.config_for(key);
        let limiter = DistributedRateLimiter::new(Arc::clone(&self.backend), key, config);
        guard.insert(key.to_string(), limiter.clone());
        limiter
    }

    /// Attempt to acquire a permit for a task, returning the gating decision.
    ///
    /// The limiter key is derived from `task_name`/`queue` per the configured
    /// [`RateLimitKeyStrategy`]. On success returns [`RateLimitDecision::Allowed`];
    /// on rate-limit returns [`RateLimitDecision::Denied`] carrying the suggested
    /// retry delay so the caller can defer (requeue) the task.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying backend store is unavailable.
    pub async fn acquire(
        &self,
        task_name: &str,
        queue: &str,
    ) -> celers_core::Result<RateLimitDecision> {
        let key = self.key_strategy.key_for(task_name, queue);
        let limiter = self.limiter_for(&key).await;
        let outcome = limiter.try_acquire_n(self.cost).await?;
        if outcome.allowed {
            trace!(
                "Rate limit granted for key '{}' (remaining ~{:.1})",
                key,
                outcome.remaining
            );
            Ok(RateLimitDecision::Allowed)
        } else {
            debug!(
                "Rate limit denied for key '{}' (remaining ~{:.1}, retry after {:?})",
                key, outcome.remaining, outcome.retry_after
            );
            Ok(RateLimitDecision::Denied {
                retry_after: outcome.retry_after,
                remaining: outcome.remaining,
            })
        }
    }

    /// Currently available permits for the given task/queue key (after refill).
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying backend store is unavailable.
    pub async fn available(&self, task_name: &str, queue: &str) -> celers_core::Result<f64> {
        let key = self.key_strategy.key_for(task_name, queue);
        let limiter = self.limiter_for(&key).await;
        limiter.available().await
    }

    /// Estimated time until a permit is available for the given task/queue key.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying backend store is unavailable.
    pub async fn time_until_available(
        &self,
        task_name: &str,
        queue: &str,
    ) -> celers_core::Result<Duration> {
        let key = self.key_strategy.key_for(task_name, queue);
        let limiter = self.limiter_for(&key).await;
        limiter.time_until_available().await
    }

    /// Reset the limiter state for the given task/queue key.
    ///
    /// # Errors
    ///
    /// Returns an error if the underlying backend store is unavailable.
    pub async fn reset(&self, task_name: &str, queue: &str) -> celers_core::Result<()> {
        let key = self.key_strategy.key_for(task_name, queue);
        let limiter = self.limiter_for(&key).await;
        limiter.reset().await
    }

    /// Number of distinct limiter keys currently cached.
    #[must_use]
    pub async fn tracked_key_count(&self) -> usize {
        self.limiters.lock().await.len()
    }
}

impl std::fmt::Debug for WorkerRateLimitCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkerRateLimitCoordinator")
            .field("backend", &self.backend.backend_name())
            .field("key_strategy", &self.key_strategy)
            .field("cost", &self.cost)
            .field("override_keys", &self.overrides.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::rate_limit_distributed::InMemoryDistributedBackend;

    fn backend() -> Arc<dyn DistributedRateLimitBackend> {
        Arc::new(InMemoryDistributedBackend::new())
    }

    #[test]
    fn test_key_strategy() {
        assert_eq!(
            RateLimitKeyStrategy::TaskName.key_for("send_email", "celery"),
            "send_email"
        );
        assert_eq!(
            RateLimitKeyStrategy::Queue.key_for("send_email", "celery"),
            "celery"
        );
        assert_eq!(
            RateLimitKeyStrategy::Global.key_for("send_email", "celery"),
            "__global__"
        );
    }

    #[test]
    fn test_decision_helpers() {
        let allowed = RateLimitDecision::Allowed;
        assert!(allowed.is_allowed());
        assert!(!allowed.is_denied());
        assert_eq!(allowed.retry_after(), Duration::ZERO);

        let denied = RateLimitDecision::Denied {
            retry_after: Duration::from_millis(500),
            remaining: 0.0,
        };
        assert!(denied.is_denied());
        assert!(!denied.is_allowed());
        assert_eq!(denied.retry_after(), Duration::from_millis(500));
    }

    #[tokio::test]
    async fn test_allows_up_to_burst_then_denies() {
        // Burst of 3, no refill (rate 0) so the cap is deterministic.
        let config = RateLimitConfig::new(0.0).with_burst(3);
        let coordinator = WorkerRateLimitCoordinator::new(backend(), config);

        for _ in 0..3 {
            let d = coordinator.acquire("task_a", "celery").await.unwrap();
            assert!(d.is_allowed());
        }
        let denied = coordinator.acquire("task_a", "celery").await.unwrap();
        assert!(denied.is_denied(), "past the cap must be denied");
    }

    #[tokio::test]
    async fn test_denies_past_cap_then_allows_after_refill() {
        // 50 tokens/sec, burst 2: exhaust, then a permit refills after ~20ms.
        let config = RateLimitConfig::new(50.0).with_burst(2);
        let coordinator = WorkerRateLimitCoordinator::new(backend(), config);

        assert!(coordinator
            .acquire("task_b", "q")
            .await
            .unwrap()
            .is_allowed());
        assert!(coordinator
            .acquire("task_b", "q")
            .await
            .unwrap()
            .is_allowed());

        let denied = coordinator.acquire("task_b", "q").await.unwrap();
        assert!(denied.is_denied());
        assert!(denied.retry_after() > Duration::ZERO);

        // Wait long enough for at least one token to refill.
        tokio::time::sleep(Duration::from_millis(60)).await;
        let allowed = coordinator.acquire("task_b", "q").await.unwrap();
        assert!(allowed.is_allowed(), "should allow again after refill");
    }

    #[tokio::test]
    async fn test_distinct_keys_independent() {
        let config = RateLimitConfig::new(0.0).with_burst(1);
        let coordinator = WorkerRateLimitCoordinator::new(backend(), config);

        assert!(coordinator
            .acquire("task_x", "q")
            .await
            .unwrap()
            .is_allowed());
        // task_x is now exhausted...
        assert!(coordinator
            .acquire("task_x", "q")
            .await
            .unwrap()
            .is_denied());
        // ...but task_y has its own independent budget.
        assert!(coordinator
            .acquire("task_y", "q")
            .await
            .unwrap()
            .is_allowed());

        assert_eq!(coordinator.tracked_key_count().await, 2);
    }

    #[tokio::test]
    async fn test_queue_strategy_shares_budget_across_task_names() {
        let config = RateLimitConfig::new(0.0).with_burst(2);
        let coordinator = WorkerRateLimitCoordinator::new(backend(), config)
            .with_key_strategy(RateLimitKeyStrategy::Queue);

        // Different task names, same queue -> share the per-queue budget of 2.
        assert!(coordinator
            .acquire("task_a", "shared")
            .await
            .unwrap()
            .is_allowed());
        assert!(coordinator
            .acquire("task_b", "shared")
            .await
            .unwrap()
            .is_allowed());
        assert!(coordinator
            .acquire("task_c", "shared")
            .await
            .unwrap()
            .is_denied());
        // Only one key tracked because we key by queue.
        assert_eq!(coordinator.tracked_key_count().await, 1);
    }

    #[tokio::test]
    async fn test_global_strategy_one_budget() {
        let config = RateLimitConfig::new(0.0).with_burst(2);
        let coordinator = WorkerRateLimitCoordinator::new(backend(), config)
            .with_key_strategy(RateLimitKeyStrategy::Global);

        assert!(coordinator.acquire("a", "q1").await.unwrap().is_allowed());
        assert!(coordinator.acquire("b", "q2").await.unwrap().is_allowed());
        assert!(coordinator.acquire("c", "q3").await.unwrap().is_denied());
        assert_eq!(coordinator.tracked_key_count().await, 1);
    }

    #[tokio::test]
    async fn test_per_key_override() {
        let default_config = RateLimitConfig::new(0.0).with_burst(1);
        let coordinator = WorkerRateLimitCoordinator::new(backend(), default_config)
            .with_key_config("vip", RateLimitConfig::new(0.0).with_burst(3));

        // 'vip' uses the override (burst 3).
        for _ in 0..3 {
            assert!(coordinator.acquire("vip", "q").await.unwrap().is_allowed());
        }
        assert!(coordinator.acquire("vip", "q").await.unwrap().is_denied());

        // 'plain' uses the default (burst 1).
        assert!(coordinator
            .acquire("plain", "q")
            .await
            .unwrap()
            .is_allowed());
        assert!(coordinator.acquire("plain", "q").await.unwrap().is_denied());
    }

    #[tokio::test]
    async fn test_cost_consumes_multiple_permits() {
        let config = RateLimitConfig::new(0.0).with_burst(10);
        let coordinator = WorkerRateLimitCoordinator::new(backend(), config).with_cost(4.0);

        assert_eq!(coordinator.cost(), 4.0);
        // 10 / 4 = 2 grants, then denied (only 2 left, needs 4).
        assert!(coordinator.acquire("t", "q").await.unwrap().is_allowed());
        assert!(coordinator.acquire("t", "q").await.unwrap().is_allowed());
        assert!(coordinator.acquire("t", "q").await.unwrap().is_denied());
    }

    #[tokio::test]
    async fn test_available_and_reset() {
        let config = RateLimitConfig::new(0.0).with_burst(5);
        let coordinator = WorkerRateLimitCoordinator::new(backend(), config);

        assert!((coordinator.available("t", "q").await.unwrap() - 5.0).abs() < 1e-6);
        assert!(coordinator.acquire("t", "q").await.unwrap().is_allowed());
        let avail = coordinator.available("t", "q").await.unwrap();
        assert!((avail - 4.0).abs() < 1e-6, "expected 4, got {avail}");

        coordinator.reset("t", "q").await.unwrap();
        assert!((coordinator.available("t", "q").await.unwrap() - 5.0).abs() < 1e-6);
    }

    #[tokio::test]
    async fn test_time_until_available() {
        let config = RateLimitConfig::new(10.0).with_burst(1);
        let coordinator = WorkerRateLimitCoordinator::new(backend(), config);
        assert!(coordinator.acquire("t", "q").await.unwrap().is_allowed());
        let wait = coordinator.time_until_available("t", "q").await.unwrap();
        assert!(wait > Duration::ZERO);
    }

    #[tokio::test]
    async fn test_cost_clamped_positive() {
        let config = RateLimitConfig::new(0.0).with_burst(1);
        let coordinator = WorkerRateLimitCoordinator::new(backend(), config).with_cost(-5.0);
        assert!(coordinator.cost() > 0.0);
        // Still acquirable with a clamped tiny cost.
        assert!(coordinator.acquire("t", "q").await.unwrap().is_allowed());
    }
}
