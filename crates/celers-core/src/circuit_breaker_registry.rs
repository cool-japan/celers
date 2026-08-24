//! Per-task-type circuit breakers.
//!
//! This module provides a circuit-breaker primitive together with a registry
//! ([`TaskTypeCircuitBreakers`]) that keeps an *independent* circuit breaker for
//! every task name (task "type"). A single misbehaving task type can trip its
//! own breaker and be shed without affecting healthy task types sharing the same
//! worker.
//!
//! The circuit-breaker follows the classic three-state machine:
//!
//! - **Closed**: calls flow through; consecutive failures inside a rolling
//!   window are counted, and once they reach `failure_threshold` the breaker
//!   trips to *Open*.
//! - **Open**: calls are rejected immediately. After `open_timeout` elapses the
//!   breaker moves to *`HalfOpen`* to probe recovery.
//! - **`HalfOpen`**: a limited number of trial calls are admitted. Enough
//!   consecutive successes (`success_threshold`) close the breaker; a single
//!   failure re-opens it.
//!
//! # Example
//!
//! ```
//! use celers_core::circuit_breaker_registry::{
//!     CircuitBreakerConfig, CircuitState, TaskTypeCircuitBreakers,
//! };
//!
//! # async fn example() {
//! // A strict default: two failures trip the breaker.
//! let default_config = CircuitBreakerConfig::default()
//!     .with_failure_threshold(2);
//! let registry = TaskTypeCircuitBreakers::with_default(default_config);
//!
//! // `task.a` fails repeatedly and trips its own breaker.
//! registry.record_failure("task.a").await;
//! registry.record_failure("task.a").await;
//! assert!(registry.is_open("task.a").await);
//!
//! // `task.b` is completely unaffected.
//! assert!(!registry.is_open("task.b").await);
//! assert_eq!(registry.state("task.b").await, CircuitState::Closed);
//! # }
//! # tokio::runtime::Builder::new_current_thread()
//! #     .enable_all()
//! #     .build()
//! #     .unwrap()
//! #     .block_on(example());
//! ```
//!
//! [`call`](TaskTypeCircuitBreakers::call) wraps an async operation, automatically
//! recording success/failure and short-circuiting when the breaker is open:
//!
//! ```
//! use celers_core::circuit_breaker_registry::{
//!     CircuitBreakerError, TaskTypeCircuitBreakers,
//! };
//!
//! # async fn example() {
//! let registry = TaskTypeCircuitBreakers::new();
//! let out: Result<u32, CircuitBreakerError<&str>> = registry
//!     .call("compute", || async { Ok::<u32, &str>(21 * 2) })
//!     .await;
//! assert_eq!(out.unwrap(), 42);
//! # }
//! # tokio::runtime::Builder::new_current_thread()
//! #     .enable_all()
//! #     .build()
//! #     .unwrap()
//! #     .block_on(example());
//! ```

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::future::Future;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

/// The lifecycle state of a single circuit breaker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitState {
    /// Calls flow through normally; failures are being counted.
    Closed,
    /// Calls are rejected immediately until the open timeout elapses.
    Open,
    /// Trial calls are admitted to test whether the dependency recovered.
    HalfOpen,
}

impl CircuitState {
    /// Returns `true` if the breaker is closed.
    #[inline]
    #[must_use]
    pub const fn is_closed(self) -> bool {
        matches!(self, CircuitState::Closed)
    }

    /// Returns `true` if the breaker is open (calls are rejected).
    #[inline]
    #[must_use]
    pub const fn is_open(self) -> bool {
        matches!(self, CircuitState::Open)
    }

    /// Returns `true` if the breaker is half-open (probing recovery).
    #[inline]
    #[must_use]
    pub const fn is_half_open(self) -> bool {
        matches!(self, CircuitState::HalfOpen)
    }

    /// Returns `true` if a call is permitted in this state (closed or half-open).
    #[inline]
    #[must_use]
    pub const fn can_execute(self) -> bool {
        !self.is_open()
    }
}

impl fmt::Display for CircuitState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CircuitState::Closed => f.write_str("Closed"),
            CircuitState::Open => f.write_str("Open"),
            CircuitState::HalfOpen => f.write_str("HalfOpen"),
        }
    }
}

/// Configuration for a circuit breaker.
///
/// The same configuration shape is used both as the registry-wide default and
/// as a per-task override. All fields have sensible defaults via
/// [`CircuitBreakerConfig::default`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CircuitBreakerConfig {
    /// Number of failures within the rolling window required to trip the breaker.
    pub failure_threshold: u32,
    /// Consecutive successes in `HalfOpen` required to close the breaker.
    pub success_threshold: u32,
    /// How long the breaker stays `Open` before moving to `HalfOpen` (seconds).
    pub open_timeout_secs: u64,
    /// Rolling window (seconds) over which closed-state failures are counted.
    /// Failures older than this window are forgotten.
    pub failure_window_secs: u64,
    /// Maximum number of trial calls admitted while `HalfOpen`. Additional calls
    /// are rejected until the trials resolve.
    pub half_open_max_calls: u32,
    /// How long a reserved half-open probe slot may stay unresolved before it is
    /// reclaimed (seconds). `None` reuses `open_timeout_secs`.
    ///
    /// Without this bound a probe whose outcome is never recorded (a dropped
    /// future, a caller that used [`CircuitBreaker::try_acquire`] and forgot to
    /// report) would hold its slot forever, wedging the breaker in `HalfOpen`
    /// and rejecting every subsequent call.
    #[serde(default)]
    pub half_open_probe_timeout_secs: Option<u64>,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            success_threshold: 2,
            open_timeout_secs: 60,
            failure_window_secs: 60,
            half_open_max_calls: 1,
            half_open_probe_timeout_secs: None,
        }
    }
}

impl CircuitBreakerConfig {
    /// Create a configuration from a failure threshold, keeping other defaults.
    #[must_use]
    pub fn new(failure_threshold: u32) -> Self {
        Self {
            failure_threshold,
            ..Self::default()
        }
    }

    /// Set the failure threshold (failures within the window that trip the breaker).
    #[must_use]
    pub const fn with_failure_threshold(mut self, threshold: u32) -> Self {
        self.failure_threshold = threshold;
        self
    }

    /// Set the success threshold (successes in half-open that close the breaker).
    #[must_use]
    pub const fn with_success_threshold(mut self, threshold: u32) -> Self {
        self.success_threshold = threshold;
        self
    }

    /// Set how long the breaker stays open before probing (seconds).
    #[must_use]
    pub const fn with_open_timeout_secs(mut self, secs: u64) -> Self {
        self.open_timeout_secs = secs;
        self
    }

    /// Set the rolling failure-counting window (seconds).
    #[must_use]
    pub const fn with_failure_window_secs(mut self, secs: u64) -> Self {
        self.failure_window_secs = secs;
        self
    }

    /// Set the maximum number of concurrent trial calls in half-open.
    #[must_use]
    pub const fn with_half_open_max_calls(mut self, max_calls: u32) -> Self {
        self.half_open_max_calls = max_calls;
        self
    }

    /// Set how long an unresolved half-open probe may hold its slot (seconds).
    #[must_use]
    pub const fn with_half_open_probe_timeout_secs(mut self, secs: u64) -> Self {
        self.half_open_probe_timeout_secs = Some(secs);
        self
    }

    /// Returns `true` if the configuration is internally consistent.
    #[must_use]
    pub const fn is_valid(&self) -> bool {
        self.failure_threshold > 0
            && self.success_threshold > 0
            && self.open_timeout_secs > 0
            && self.failure_window_secs > 0
            && self.half_open_max_calls > 0
            && match self.half_open_probe_timeout_secs {
                Some(secs) => secs > 0,
                None => true,
            }
    }

    /// The open timeout as a [`Duration`].
    #[inline]
    #[must_use]
    pub const fn open_timeout(&self) -> Duration {
        Duration::from_secs(self.open_timeout_secs)
    }

    /// The failure window as a [`Duration`].
    #[inline]
    #[must_use]
    pub const fn failure_window(&self) -> Duration {
        Duration::from_secs(self.failure_window_secs)
    }

    /// How long an unresolved half-open probe may hold its slot, as a
    /// [`Duration`] (falls back to [`Self::open_timeout`]).
    #[inline]
    #[must_use]
    pub const fn half_open_probe_timeout(&self) -> Duration {
        match self.half_open_probe_timeout_secs {
            Some(secs) => Duration::from_secs(secs),
            None => self.open_timeout(),
        }
    }
}

/// A point-in-time, serializable snapshot of a single breaker's state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CircuitSnapshot {
    /// Current lifecycle state.
    pub state: CircuitState,
    /// Failures counted in the current rolling window (closed state).
    pub failure_count: u32,
    /// Consecutive successes recorded while half-open.
    pub success_count: u32,
    /// Trial calls currently admitted while half-open.
    pub half_open_in_flight: u32,
}

/// Mutable inner state for a single circuit breaker.
#[derive(Debug)]
struct CircuitInner {
    config: CircuitBreakerConfig,
    state: CircuitState,
    /// Timestamps of the failures currently inside the rolling window.
    ///
    /// A true rolling window (rather than a counter reset by a large gap
    /// between consecutive failures) is what `failure_window_secs` documents:
    /// failures older than the window are forgotten.
    failures: VecDeque<Instant>,
    success_count: u32,
    /// Reservation timestamps of the half-open probes currently in flight. A
    /// probe whose outcome is never recorded is reclaimed once it is older than
    /// `half_open_probe_timeout`.
    half_open_probes: VecDeque<Instant>,
    last_failure_at: Option<Instant>,
    opened_at: Option<Instant>,
    /// Last time this breaker saw any activity (admission or outcome), used by
    /// the registry to prune idle breakers.
    last_activity_at: Instant,
}

impl CircuitInner {
    fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            config,
            state: CircuitState::Closed,
            failures: VecDeque::new(),
            success_count: 0,
            half_open_probes: VecDeque::new(),
            last_failure_at: None,
            opened_at: None,
            last_activity_at: Instant::now(),
        }
    }

    /// Number of failures currently counted in the rolling window.
    fn failure_count(&self) -> u32 {
        u32::try_from(self.failures.len()).unwrap_or(u32::MAX)
    }

    /// Number of half-open probes currently holding a slot.
    fn half_open_in_flight(&self) -> u32 {
        u32::try_from(self.half_open_probes.len()).unwrap_or(u32::MAX)
    }

    fn snapshot(&self) -> CircuitSnapshot {
        CircuitSnapshot {
            state: self.state,
            failure_count: self.failure_count(),
            success_count: self.success_count,
            half_open_in_flight: self.half_open_in_flight(),
        }
    }

    /// Drop failures that have fallen out of the rolling window.
    fn prune_failures(&mut self, now: Instant) {
        let window = self.config.failure_window();
        while let Some(&front) = self.failures.front() {
            if now.duration_since(front) >= window {
                self.failures.pop_front();
            } else {
                break;
            }
        }
    }

    /// Reclaim half-open probe slots whose owner never reported an outcome.
    fn reclaim_stale_probes(&mut self, now: Instant) {
        let timeout = self.config.half_open_probe_timeout();
        while let Some(&front) = self.half_open_probes.front() {
            if now.duration_since(front) >= timeout {
                self.half_open_probes.pop_front();
            } else {
                break;
            }
        }
    }

    /// Advance any time-based transition (Open -> `HalfOpen`, expiry of stale
    /// half-open reservations and of out-of-window failures) given `now`.
    fn refresh(&mut self, now: Instant) {
        if self.state == CircuitState::Open {
            if let Some(opened_at) = self.opened_at {
                if now.duration_since(opened_at) >= self.config.open_timeout() {
                    self.state = CircuitState::HalfOpen;
                    self.failures.clear();
                    self.success_count = 0;
                    self.half_open_probes.clear();
                }
            }
        }
        if self.state == CircuitState::HalfOpen {
            self.reclaim_stale_probes(now);
        }
        if self.state == CircuitState::Closed {
            self.prune_failures(now);
        }
    }

    /// Decide whether a call may proceed, reserving a half-open slot if needed.
    fn try_admit(&mut self, now: Instant) -> bool {
        self.refresh(now);
        self.last_activity_at = now;
        match self.state {
            CircuitState::Closed => true,
            CircuitState::Open => false,
            CircuitState::HalfOpen => {
                if self.half_open_in_flight() < self.config.half_open_max_calls {
                    self.half_open_probes.push_back(now);
                    true
                } else {
                    false
                }
            }
        }
    }

    /// Release one half-open probe slot (the oldest reservation).
    fn release_probe(&mut self) {
        self.half_open_probes.pop_front();
    }

    fn record_success(&mut self, now: Instant) {
        self.last_activity_at = now;
        match self.state {
            CircuitState::Closed => {
                self.failures.clear();
                self.last_failure_at = None;
            }
            CircuitState::HalfOpen => {
                self.release_probe();
                self.success_count += 1;
                if self.success_count >= self.config.success_threshold {
                    self.state = CircuitState::Closed;
                    self.failures.clear();
                    self.success_count = 0;
                    self.half_open_probes.clear();
                    self.opened_at = None;
                    self.last_failure_at = None;
                }
            }
            CircuitState::Open => {
                // A late success arriving after the breaker opened is ignored.
            }
        }
    }

    fn record_failure(&mut self, now: Instant) {
        self.last_activity_at = now;
        match self.state {
            CircuitState::Closed => {
                self.prune_failures(now);
                self.failures.push_back(now);
                // The deque never needs to hold more than `failure_threshold`
                // entries: the oldest surplus ones can no longer influence the
                // decision, so dropping them bounds memory.
                let cap = self.config.failure_threshold.max(1) as usize;
                while self.failures.len() > cap {
                    self.failures.pop_front();
                }
                self.last_failure_at = Some(now);
                if self.failure_count() >= self.config.failure_threshold {
                    self.state = CircuitState::Open;
                    self.opened_at = Some(now);
                }
            }
            CircuitState::HalfOpen => {
                // Any failure during probing immediately re-opens the breaker.
                self.release_probe();
                self.state = CircuitState::Open;
                self.opened_at = Some(now);
                self.failures.clear();
                self.success_count = 0;
                self.last_failure_at = Some(now);
            }
            CircuitState::Open => {
                self.last_failure_at = Some(now);
            }
        }
    }

    fn reset(&mut self) {
        self.state = CircuitState::Closed;
        self.failures.clear();
        self.success_count = 0;
        self.half_open_probes.clear();
        self.last_failure_at = None;
        self.opened_at = None;
        self.last_activity_at = Instant::now();
    }
}

/// A standalone, thread-safe circuit breaker for a single dependency.
///
/// [`TaskTypeCircuitBreakers`] composes one of these per task name, but it can
/// also be used directly. Cloning shares the underlying state (it is `Arc`-backed).
#[derive(Debug, Clone)]
pub struct CircuitBreaker {
    inner: Arc<Mutex<CircuitInner>>,
}

/// RAII guard for one admitted call.
///
/// If the guarded call's future is dropped before an outcome is recorded
/// (worker shutdown, a task timeout, a losing `tokio::select!` branch, a panic),
/// the guard records a *failure* on drop. That both releases any reserved
/// half-open probe slot — which would otherwise wedge the breaker in `HalfOpen`
/// forever — and treats an unknown outcome conservatively.
#[derive(Debug)]
struct CallGuard {
    inner: Arc<Mutex<CircuitInner>>,
    resolved: bool,
}

impl CallGuard {
    /// Record the call's outcome and disarm the drop handler.
    fn resolve(mut self, success: bool) {
        self.resolved = true;
        let now = Instant::now();
        let mut guard = lock_inner(&self.inner);
        if success {
            guard.record_success(now);
        } else {
            guard.record_failure(now);
        }
    }
}

impl Drop for CallGuard {
    fn drop(&mut self) {
        if self.resolved {
            return;
        }
        let now = Instant::now();
        lock_inner(&self.inner).record_failure(now);
    }
}

/// Lock the inner state, recovering the value if a previous holder panicked.
fn lock_inner(inner: &Arc<Mutex<CircuitInner>>) -> MutexGuard<'_, CircuitInner> {
    inner.lock().unwrap_or_else(PoisonError::into_inner)
}

impl CircuitBreaker {
    /// Create a circuit breaker with the default configuration.
    #[must_use]
    pub fn new() -> Self {
        Self::with_config(CircuitBreakerConfig::default())
    }

    /// Create a circuit breaker with a custom configuration.
    #[must_use]
    pub fn with_config(config: CircuitBreakerConfig) -> Self {
        Self {
            inner: Arc::new(Mutex::new(CircuitInner::new(config))),
        }
    }

    /// Returns `true` if a call may proceed right now, reserving a half-open
    /// trial slot when applicable. Prefer [`CircuitBreaker::call`] which pairs
    /// this with automatic success/failure recording.
    ///
    /// A caller that acquires a half-open slot and never records an outcome
    /// holds that slot only until `half_open_probe_timeout` elapses, after which
    /// it is reclaimed automatically.
    pub async fn try_acquire(&self) -> bool {
        lock_inner(&self.inner).try_admit(Instant::now())
    }

    /// Returns `true` if the breaker is currently open (rejecting calls).
    ///
    /// This also performs any pending time-based transition to half-open, so a
    /// breaker whose open timeout has elapsed reports `false` here.
    pub async fn is_open(&self) -> bool {
        let mut guard = lock_inner(&self.inner);
        guard.refresh(Instant::now());
        guard.state.is_open()
    }

    /// Return the current breaker state (after applying time-based transitions).
    pub async fn state(&self) -> CircuitState {
        let mut guard = lock_inner(&self.inner);
        guard.refresh(Instant::now());
        guard.state
    }

    /// Return a serializable snapshot of the breaker's internal counters.
    pub async fn snapshot(&self) -> CircuitSnapshot {
        let mut guard = lock_inner(&self.inner);
        guard.refresh(Instant::now());
        guard.snapshot()
    }

    /// Record a successful call, advancing recovery in the half-open state.
    pub async fn record_success(&self) {
        lock_inner(&self.inner).record_success(Instant::now());
    }

    /// Record a failed call, possibly tripping or re-opening the breaker.
    pub async fn record_failure(&self) {
        lock_inner(&self.inner).record_failure(Instant::now());
    }

    /// Force the breaker back to the closed state, clearing all counters.
    pub async fn reset(&self) {
        lock_inner(&self.inner).reset();
    }

    /// The last time this breaker admitted a call or recorded an outcome.
    pub(crate) fn last_activity_at(&self) -> Instant {
        lock_inner(&self.inner).last_activity_at
    }

    /// Returns `true` if the breaker is closed and holds no pending state, i.e.
    /// it is indistinguishable from a freshly created breaker.
    pub(crate) fn is_quiescent(&self) -> bool {
        let mut guard = lock_inner(&self.inner);
        guard.refresh(Instant::now());
        guard.state.is_closed() && guard.failures.is_empty() && guard.half_open_probes.is_empty()
    }

    /// Run `operation` through the breaker.
    ///
    /// If the breaker is open the operation is *not* invoked and
    /// [`CircuitBreakerError::Open`] is returned. Otherwise the operation runs and
    /// its outcome is recorded (success closes/keeps-closed; failure counts toward
    /// tripping). An `Err` from the operation is surfaced as
    /// [`CircuitBreakerError::Operation`].
    ///
    /// This method is cancel-safe with respect to breaker bookkeeping: if the
    /// returned future is dropped while `operation` is in flight, the call is
    /// recorded as a failure and any reserved half-open slot is released.
    pub async fn call<F, Fut, T, E>(&self, operation: F) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        let admitted = lock_inner(&self.inner).try_admit(Instant::now());
        if !admitted {
            return Err(CircuitBreakerError::Open);
        }
        let guard = CallGuard {
            inner: Arc::clone(&self.inner),
            resolved: false,
        };
        match operation().await {
            Ok(value) => {
                guard.resolve(true);
                Ok(value)
            }
            Err(err) => {
                guard.resolve(false);
                Err(CircuitBreakerError::Operation(err))
            }
        }
    }
}

impl Default for CircuitBreaker {
    fn default() -> Self {
        Self::new()
    }
}

/// Error returned by [`CircuitBreaker::call`] / [`TaskTypeCircuitBreakers::call`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CircuitBreakerError<E> {
    /// The breaker was open, so the operation was rejected without running.
    Open,
    /// The operation itself returned an error (also recorded as a failure).
    Operation(E),
}

impl<E> CircuitBreakerError<E> {
    /// Returns `true` if the error is [`CircuitBreakerError::Open`].
    #[inline]
    #[must_use]
    pub const fn is_open(&self) -> bool {
        matches!(self, CircuitBreakerError::Open)
    }

    /// Consume the error, returning the inner operation error if present.
    #[inline]
    #[must_use]
    pub fn into_operation(self) -> Option<E> {
        match self {
            CircuitBreakerError::Open => None,
            CircuitBreakerError::Operation(err) => Some(err),
        }
    }
}

impl<E: fmt::Display> fmt::Display for CircuitBreakerError<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CircuitBreakerError::Open => f.write_str("circuit breaker is open"),
            CircuitBreakerError::Operation(err) => write!(f, "operation failed: {err}"),
        }
    }
}

impl<E: fmt::Debug + fmt::Display> std::error::Error for CircuitBreakerError<E> {}

/// A registry of per-task-type circuit breakers.
///
/// Each task name maps to its own independent [`CircuitBreaker`]. A configurable
/// default is applied when a task type is first seen, and per-type overrides can
/// be registered up front via [`TaskTypeCircuitBreakers::set_task_config`].
///
/// All operations are async and internally synchronized; the registry is cheap
/// to clone (`Arc`-backed) and safe to share across worker tasks.
#[derive(Debug, Clone)]
pub struct TaskTypeCircuitBreakers {
    default_config: CircuitBreakerConfig,
    overrides: Arc<RwLock<HashMap<String, CircuitBreakerConfig>>>,
    breakers: Arc<RwLock<HashMap<String, CircuitBreaker>>>,
    max_tracked: usize,
}

/// Default upper bound on the number of task types that may hold a live
/// breaker at once. Task names can come from remote messages, so the map must
/// not be allowed to grow without limit.
pub const DEFAULT_MAX_TRACKED_TASK_TYPES: usize = 4096;

impl TaskTypeCircuitBreakers {
    /// Create a registry using the default [`CircuitBreakerConfig`].
    #[must_use]
    pub fn new() -> Self {
        Self::with_default(CircuitBreakerConfig::default())
    }

    /// Create a registry with an explicit default configuration applied to any
    /// task type that has no specific override.
    #[must_use]
    pub fn with_default(default_config: CircuitBreakerConfig) -> Self {
        Self {
            default_config,
            overrides: Arc::new(RwLock::new(HashMap::new())),
            breakers: Arc::new(RwLock::new(HashMap::new())),
            max_tracked: DEFAULT_MAX_TRACKED_TASK_TYPES,
        }
    }

    /// Set the maximum number of task types that may hold a live breaker.
    ///
    /// When the bound is reached, quiescent (closed, no recorded failures)
    /// breakers are evicted first, then the least recently active one.
    #[must_use]
    pub fn with_max_tracked(mut self, max_tracked: usize) -> Self {
        self.max_tracked = max_tracked.max(1);
        self
    }

    /// The configured maximum number of tracked task types.
    #[inline]
    #[must_use]
    pub const fn max_tracked(&self) -> usize {
        self.max_tracked
    }

    /// The default configuration applied to task types without an override.
    #[inline]
    #[must_use]
    pub const fn default_config(&self) -> CircuitBreakerConfig {
        self.default_config
    }

    /// Register (or replace) the configuration override for a task type.
    ///
    /// If a breaker for this task already exists it is rebuilt with the new
    /// configuration (its runtime counters are reset).
    pub async fn set_task_config(
        &self,
        task_name: impl Into<String>,
        config: CircuitBreakerConfig,
    ) {
        let name = task_name.into();
        {
            let mut overrides = self.overrides.write().await;
            overrides.insert(name.clone(), config);
        }
        let mut breakers = self.breakers.write().await;
        breakers.insert(name, CircuitBreaker::with_config(config));
    }

    /// Remove a task type's configuration override.
    ///
    /// Any existing breaker for the task is dropped; the next access recreates it
    /// from the default configuration.
    pub async fn remove_task_config(&self, task_name: &str) {
        let mut overrides = self.overrides.write().await;
        overrides.remove(task_name);
        let mut breakers = self.breakers.write().await;
        breakers.remove(task_name);
    }

    /// Return the effective configuration for a task type (override or default).
    pub async fn config_for(&self, task_name: &str) -> CircuitBreakerConfig {
        let overrides = self.overrides.read().await;
        overrides
            .get(task_name)
            .copied()
            .unwrap_or(self.default_config)
    }

    /// Get (creating on demand) the [`CircuitBreaker`] for a task type.
    ///
    /// The returned handle shares state with the registry, so recording outcomes
    /// on it is observed by the registry and vice versa.
    pub async fn breaker_for(&self, task_name: &str) -> CircuitBreaker {
        {
            let breakers = self.breakers.read().await;
            if let Some(breaker) = breakers.get(task_name) {
                return breaker.clone();
            }
        }
        // Resolve the configuration (override wins) before taking the write lock.
        let config = self.config_for(task_name).await;
        let mut breakers = self.breakers.write().await;
        if let Some(breaker) = breakers.get(task_name) {
            // Another task created it between the locks.
            return breaker.clone();
        }
        Self::make_room(&mut breakers, self.max_tracked);
        breakers
            .entry(task_name.to_string())
            .or_insert_with(|| CircuitBreaker::with_config(config))
            .clone()
    }

    /// Return the breaker for `task_name` **without** creating one.
    ///
    /// Read-only queries use this so that merely asking about an unknown (or
    /// attacker-chosen) task name cannot leak a permanent map entry.
    pub async fn existing_breaker(&self, task_name: &str) -> Option<CircuitBreaker> {
        self.breakers.read().await.get(task_name).cloned()
    }

    /// Evict entries until there is room for one more breaker.
    ///
    /// Quiescent breakers go first. That is deliberately aggressive — quiescent
    /// is also the *healthy* state — but a dropped breaker carries no
    /// information by definition and is recreated on demand, so churning them is
    /// strictly preferable to discarding a breaker that is currently counting
    /// failures or probing.
    fn make_room(breakers: &mut HashMap<String, CircuitBreaker>, max_tracked: usize) {
        if breakers.len() < max_tracked {
            return;
        }
        // Prefer dropping breakers that carry no state at all.
        breakers.retain(|_, breaker| !breaker.is_quiescent());
        while breakers.len() >= max_tracked {
            let Some(victim) = breakers
                .iter()
                .min_by_key(|(_, breaker)| breaker.last_activity_at())
                .map(|(name, _)| name.clone())
            else {
                return;
            };
            breakers.remove(&victim);
        }
    }

    /// Drop breakers that are quiescent (closed, nothing recorded) and have seen
    /// no activity for at least `idle_for`, returning how many were removed.
    ///
    /// Intended to be called from a worker's periodic maintenance so a stream of
    /// unknown task names cannot pin memory for the process lifetime.
    pub async fn prune_idle(&self, idle_for: Duration) -> usize {
        let now = Instant::now();
        let mut breakers = self.breakers.write().await;
        let before = breakers.len();
        breakers.retain(|_, breaker| {
            let idle = now.saturating_duration_since(breaker.last_activity_at());
            !(idle >= idle_for && breaker.is_quiescent())
        });
        before - breakers.len()
    }

    /// Returns `true` if the breaker for `task_name` is currently open.
    ///
    /// An unknown task type reports `false` (closed) without creating a breaker.
    pub async fn is_open(&self, task_name: &str) -> bool {
        match self.existing_breaker(task_name).await {
            Some(breaker) => breaker.is_open().await,
            None => false,
        }
    }

    /// Returns `true` if a call for `task_name` may proceed right now, reserving
    /// a half-open trial slot when applicable.
    pub async fn try_acquire(&self, task_name: &str) -> bool {
        self.breaker_for(task_name).await.try_acquire().await
    }

    /// Return the current [`CircuitState`] for a task type.
    ///
    /// An unknown task type reports [`CircuitState::Closed`] without creating a
    /// breaker.
    pub async fn state(&self, task_name: &str) -> CircuitState {
        match self.existing_breaker(task_name).await {
            Some(breaker) => breaker.state().await,
            None => CircuitState::Closed,
        }
    }

    /// Return a snapshot of a task type's breaker counters.
    ///
    /// An unknown task type reports a pristine closed snapshot without creating
    /// a breaker.
    pub async fn snapshot(&self, task_name: &str) -> CircuitSnapshot {
        match self.existing_breaker(task_name).await {
            Some(breaker) => breaker.snapshot().await,
            None => CircuitSnapshot {
                state: CircuitState::Closed,
                failure_count: 0,
                success_count: 0,
                half_open_in_flight: 0,
            },
        }
    }

    /// Record a successful execution for a task type.
    pub async fn record_success(&self, task_name: &str) {
        self.breaker_for(task_name).await.record_success().await;
    }

    /// Record a failed execution for a task type.
    pub async fn record_failure(&self, task_name: &str) {
        self.breaker_for(task_name).await.record_failure().await;
    }

    /// Run `operation` through the breaker for `task_name`.
    ///
    /// See [`CircuitBreaker::call`] for the success/failure semantics.
    pub async fn call<F, Fut, T, E>(
        &self,
        task_name: &str,
        operation: F,
    ) -> Result<T, CircuitBreakerError<E>>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<T, E>>,
    {
        self.breaker_for(task_name).await.call(operation).await
    }

    /// Reset a single task type's breaker to closed.
    pub async fn reset(&self, task_name: &str) {
        let breaker = {
            let breakers = self.breakers.read().await;
            breakers.get(task_name).cloned()
        };
        if let Some(breaker) = breaker {
            breaker.reset().await;
        }
    }

    /// Reset every known breaker to closed.
    pub async fn reset_all(&self) {
        let breakers = self.breakers.read().await;
        for breaker in breakers.values() {
            breaker.reset().await;
        }
    }

    /// Snapshot the state of every known breaker, keyed by task name.
    pub async fn all_states(&self) -> HashMap<String, CircuitState> {
        let breakers = self.breakers.read().await;
        let mut out = HashMap::with_capacity(breakers.len());
        for (name, breaker) in breakers.iter() {
            out.insert(name.clone(), breaker.state().await);
        }
        out
    }

    /// The number of task types that currently have a live breaker.
    pub async fn tracked_count(&self) -> usize {
        self.breakers.read().await.len()
    }
}

impl Default for TaskTypeCircuitBreakers {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn config_defaults_are_valid() {
        assert!(CircuitBreakerConfig::default().is_valid());
        assert!(CircuitBreakerConfig::new(3).is_valid());
        let zero = CircuitBreakerConfig::default().with_failure_threshold(0);
        assert!(!zero.is_valid());
    }

    #[test]
    fn config_builders_apply() {
        let config = CircuitBreakerConfig::default()
            .with_failure_threshold(7)
            .with_success_threshold(3)
            .with_open_timeout_secs(30)
            .with_failure_window_secs(15)
            .with_half_open_max_calls(4);
        assert_eq!(config.failure_threshold, 7);
        assert_eq!(config.success_threshold, 3);
        assert_eq!(config.open_timeout_secs, 30);
        assert_eq!(config.failure_window_secs, 15);
        assert_eq!(config.half_open_max_calls, 4);
        assert_eq!(config.open_timeout(), Duration::from_secs(30));
        assert_eq!(config.failure_window(), Duration::from_secs(15));
    }

    #[test]
    fn circuit_state_predicates() {
        assert!(CircuitState::Closed.is_closed());
        assert!(CircuitState::Closed.can_execute());
        assert!(CircuitState::Open.is_open());
        assert!(!CircuitState::Open.can_execute());
        assert!(CircuitState::HalfOpen.is_half_open());
        assert!(CircuitState::HalfOpen.can_execute());
    }

    #[tokio::test]
    async fn breaker_trips_after_threshold() {
        let cb = CircuitBreaker::with_config(CircuitBreakerConfig::new(3));
        assert_eq!(cb.state().await, CircuitState::Closed);

        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Closed);

        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);
        assert!(cb.is_open().await);
        assert!(!cb.try_acquire().await);
    }

    #[tokio::test]
    async fn success_resets_closed_failure_count() {
        let cb = CircuitBreaker::with_config(CircuitBreakerConfig::new(3));
        cb.record_failure().await;
        cb.record_failure().await;
        cb.record_success().await; // resets the running count
        cb.record_failure().await;
        cb.record_failure().await;
        // Only two failures since the success, so still closed.
        assert_eq!(cb.state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn open_recovers_through_half_open() {
        let config = CircuitBreakerConfig::new(2)
            .with_success_threshold(2)
            .with_open_timeout_secs(1);
        let cb = CircuitBreaker::with_config(config);

        cb.record_failure().await;
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);

        tokio::time::sleep(Duration::from_millis(1100)).await;

        // Timeout elapsed -> transitions to half-open and admits the trial call.
        assert!(cb.try_acquire().await);
        assert_eq!(cb.state().await, CircuitState::HalfOpen);

        cb.record_success().await;
        assert_eq!(cb.state().await, CircuitState::HalfOpen);
        cb.record_success().await;
        assert_eq!(cb.state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn half_open_failure_reopens() {
        let config = CircuitBreakerConfig::new(1).with_open_timeout_secs(1);
        let cb = CircuitBreaker::with_config(config);
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);

        tokio::time::sleep(Duration::from_millis(1100)).await;
        assert!(cb.try_acquire().await);
        assert_eq!(cb.state().await, CircuitState::HalfOpen);

        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Open);
    }

    #[tokio::test]
    async fn half_open_limits_trial_calls() {
        let config = CircuitBreakerConfig::new(1)
            .with_open_timeout_secs(1)
            .with_half_open_max_calls(1);
        let cb = CircuitBreaker::with_config(config);
        cb.record_failure().await;
        tokio::time::sleep(Duration::from_millis(1100)).await;

        // First probe admitted, second rejected until the first resolves.
        assert!(cb.try_acquire().await);
        assert!(!cb.try_acquire().await);
    }

    #[tokio::test]
    async fn failure_window_expiry_resets_count() {
        let config = CircuitBreakerConfig::new(3).with_failure_window_secs(1);
        let cb = CircuitBreaker::with_config(config);
        cb.record_failure().await;
        cb.record_failure().await;
        tokio::time::sleep(Duration::from_millis(1100)).await;
        // The window expired; this is treated as the first failure of a new one.
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn call_records_success_and_failure() {
        let cb = CircuitBreaker::with_config(CircuitBreakerConfig::new(2));
        let ok: Result<u32, CircuitBreakerError<&str>> =
            cb.call(|| async { Ok::<u32, &str>(7) }).await;
        assert_eq!(ok.unwrap(), 7);

        let err1: Result<u32, CircuitBreakerError<&str>> =
            cb.call(|| async { Err::<u32, &str>("boom") }).await;
        assert!(matches!(err1, Err(CircuitBreakerError::Operation("boom"))));
        let err2: Result<u32, CircuitBreakerError<&str>> =
            cb.call(|| async { Err::<u32, &str>("boom") }).await;
        assert!(matches!(err2, Err(CircuitBreakerError::Operation("boom"))));

        // Breaker is now open; the next call short-circuits without running.
        let rejected: Result<u32, CircuitBreakerError<&str>> = cb
            .call(|| async {
                panic!("operation must not run while open");
            })
            .await;
        assert!(rejected.unwrap_err().is_open());
    }

    #[tokio::test]
    async fn breaker_reset_clears_open() {
        let cb = CircuitBreaker::with_config(CircuitBreakerConfig::new(1));
        cb.record_failure().await;
        assert!(cb.is_open().await);
        cb.reset().await;
        assert_eq!(cb.state().await, CircuitState::Closed);
        assert!(cb.try_acquire().await);
    }

    #[tokio::test]
    async fn registry_isolates_task_types() {
        let registry = TaskTypeCircuitBreakers::with_default(CircuitBreakerConfig::new(2));

        registry.record_failure("task.a").await;
        registry.record_failure("task.a").await;
        assert!(registry.is_open("task.a").await);
        assert_eq!(registry.state("task.a").await, CircuitState::Open);

        // A completely different task type is untouched.
        assert!(!registry.is_open("task.b").await);
        assert_eq!(registry.state("task.b").await, CircuitState::Closed);
        assert!(registry.try_acquire("task.b").await);
    }

    #[tokio::test]
    async fn registry_default_vs_override() {
        let registry = TaskTypeCircuitBreakers::with_default(CircuitBreakerConfig::new(5));
        // Strict override for one task type: a single failure trips it.
        registry
            .set_task_config("strict.task", CircuitBreakerConfig::new(1))
            .await;

        assert_eq!(
            registry.config_for("strict.task").await.failure_threshold,
            1
        );
        assert_eq!(registry.config_for("other.task").await.failure_threshold, 5);

        registry.record_failure("strict.task").await;
        assert!(registry.is_open("strict.task").await);

        // The default (threshold 5) task is unaffected by a single failure.
        registry.record_failure("other.task").await;
        assert!(!registry.is_open("other.task").await);
    }

    #[tokio::test]
    async fn registry_call_helper() {
        let registry = TaskTypeCircuitBreakers::with_default(CircuitBreakerConfig::new(1));
        let value: Result<&str, CircuitBreakerError<&str>> = registry
            .call("compute", || async { Ok::<&str, &str>("done") })
            .await;
        assert_eq!(value.unwrap(), "done");

        let failure: Result<&str, CircuitBreakerError<&str>> = registry
            .call("compute", || async { Err::<&str, &str>("nope") })
            .await;
        assert!(matches!(
            failure,
            Err(CircuitBreakerError::Operation("nope"))
        ));
        assert!(registry.is_open("compute").await);
    }

    #[tokio::test]
    async fn registry_reset_and_inspection() {
        let registry = TaskTypeCircuitBreakers::with_default(CircuitBreakerConfig::new(1));
        registry.record_failure("a").await;
        registry.record_failure("b").await;
        assert_eq!(registry.tracked_count().await, 2);

        let states = registry.all_states().await;
        assert_eq!(states.get("a"), Some(&CircuitState::Open));
        assert_eq!(states.get("b"), Some(&CircuitState::Open));

        registry.reset("a").await;
        assert_eq!(registry.state("a").await, CircuitState::Closed);
        assert_eq!(registry.state("b").await, CircuitState::Open);

        registry.reset_all().await;
        assert_eq!(registry.state("b").await, CircuitState::Closed);
    }

    #[tokio::test]
    async fn registry_remove_config_falls_back_to_default() {
        let registry = TaskTypeCircuitBreakers::with_default(CircuitBreakerConfig::new(4));
        registry
            .set_task_config("temp", CircuitBreakerConfig::new(1))
            .await;
        assert_eq!(registry.config_for("temp").await.failure_threshold, 1);
        registry.remove_task_config("temp").await;
        assert_eq!(registry.config_for("temp").await.failure_threshold, 4);
    }

    #[tokio::test]
    async fn snapshot_reports_counts() {
        let cb = CircuitBreaker::with_config(CircuitBreakerConfig::new(5));
        cb.record_failure().await;
        cb.record_failure().await;
        let snap = cb.snapshot().await;
        assert_eq!(snap.state, CircuitState::Closed);
        assert_eq!(snap.failure_count, 2);
        assert_eq!(snap.success_count, 0);
    }

    #[test]
    fn rolling_window_forgets_failures_older_than_the_window() {
        // Regression: the window used to be a *gap* timer, so a slow drip of
        // failures spaced just under the window apart accumulated forever and
        // tripped the breaker even though only one failure was ever inside the
        // window.
        let config = CircuitBreakerConfig::new(5).with_failure_window_secs(60);
        let mut inner = CircuitInner::new(config);
        let t0 = Instant::now();

        for secs in [0u64, 59, 118, 177, 236] {
            inner.record_failure(t0 + Duration::from_secs(secs));
            assert_eq!(
                inner.state,
                CircuitState::Closed,
                "breaker tripped at t={secs}s despite only 1-2 failures in the window"
            );
        }
        // Only the failures still inside the 60s window are counted: t=177 and
        // t=236 (the earlier three have been forgotten), nowhere near the
        // threshold of 5.
        assert_eq!(inner.failure_count(), 2);

        // A genuine burst inside one window still trips it.
        let base = t0 + Duration::from_secs(300);
        for i in 0..5 {
            inner.record_failure(base + Duration::from_secs(i));
        }
        assert_eq!(inner.state, CircuitState::Open);
    }

    #[test]
    fn failure_deque_is_bounded_by_the_threshold() {
        let config = CircuitBreakerConfig::new(3).with_failure_window_secs(3600);
        let mut inner = CircuitInner::new(config);
        let t0 = Instant::now();
        for i in 0..100 {
            inner.record_failure(t0 + Duration::from_millis(i));
        }
        assert!(inner.failures.len() <= 3);
    }

    #[test]
    fn stale_half_open_probe_slot_is_reclaimed() {
        // Regression: a probe that never recorded an outcome held its slot
        // forever, so `try_admit` rejected every later call while the breaker
        // reported HalfOpen (not Open) to monitoring.
        let config = CircuitBreakerConfig::new(1)
            .with_open_timeout_secs(10)
            .with_half_open_probe_timeout_secs(5)
            .with_half_open_max_calls(1);
        let mut inner = CircuitInner::new(config);
        let t0 = Instant::now();

        inner.record_failure(t0);
        assert_eq!(inner.state, CircuitState::Open);

        // Open timeout elapses: the first probe is admitted...
        let probe_at = t0 + Duration::from_secs(10);
        assert!(inner.try_admit(probe_at));
        assert_eq!(inner.state, CircuitState::HalfOpen);
        // ...and while it is in flight no other call gets in.
        assert!(!inner.try_admit(probe_at + Duration::from_secs(1)));

        // The probe never reports an outcome. Once the probe timeout passes the
        // slot is reclaimed and probing can continue.
        assert!(inner.try_admit(probe_at + Duration::from_secs(6)));
        assert_eq!(inner.half_open_in_flight(), 1);
    }

    #[tokio::test]
    async fn dropped_call_future_releases_half_open_slot() {
        use std::pin::Pin;
        use std::task::{Context, Waker};

        // open_timeout of 0 puts the breaker into HalfOpen as soon as it trips,
        // which keeps this test free of any sleeping.
        // A long probe timeout ensures the slot is released by the guard on
        // drop, not merely reclaimed by the timeout reaper.
        let config = CircuitBreakerConfig::new(1)
            .with_open_timeout_secs(0)
            .with_half_open_probe_timeout_secs(3600)
            .with_half_open_max_calls(1);
        let cb = CircuitBreaker::with_config(config);
        cb.record_failure().await;
        assert_eq!(cb.state().await, CircuitState::HalfOpen);

        {
            let mut fut =
                Box::pin(cb.call(|| async { std::future::pending::<Result<u32, &str>>().await }));
            let mut cx = Context::from_waker(Waker::noop());
            // The probe starts (slot reserved) but never completes.
            assert!(Pin::new(&mut fut).poll(&mut cx).is_pending());
            assert_eq!(cb.snapshot().await.half_open_in_flight, 1);
            // Dropping the future (worker shutdown, task timeout, select!) must
            // release the slot instead of wedging the breaker.
        }

        let snapshot = cb.snapshot().await;
        assert_eq!(snapshot.half_open_in_flight, 0);
        // The unknown outcome is conservatively recorded as a failure, so the
        // breaker re-opens (and, with a zero open timeout, immediately re-probes).
        assert!(cb.try_acquire().await);
    }

    #[tokio::test]
    async fn read_only_queries_do_not_create_breakers() {
        // Regression: `is_open`/`state`/`snapshot` funnelled through
        // `breaker_for`, so querying an attacker-chosen task name leaked a
        // permanent map entry.
        let registry = TaskTypeCircuitBreakers::new();
        for i in 0..1_000 {
            let name = format!("unknown.task.{i}");
            assert!(!registry.is_open(&name).await);
            assert_eq!(registry.state(&name).await, CircuitState::Closed);
            assert_eq!(registry.snapshot(&name).await.failure_count, 0);
        }
        assert_eq!(registry.tracked_count().await, 0);
        assert!(registry.existing_breaker("unknown.task.0").await.is_none());

        // Recording an outcome does create one.
        registry.record_failure("real.task").await;
        assert_eq!(registry.tracked_count().await, 1);
    }

    #[tokio::test]
    async fn tracked_breakers_are_bounded_and_prunable() {
        let registry =
            TaskTypeCircuitBreakers::with_default(CircuitBreakerConfig::new(1)).with_max_tracked(8);
        for i in 0..100 {
            registry.record_failure(&format!("task.{i}")).await;
        }
        assert!(registry.tracked_count().await <= 8);

        // Quiescent breakers are pruned by the maintenance hook.
        let registry = TaskTypeCircuitBreakers::new();
        for i in 0..10 {
            // `try_acquire` on a closed breaker leaves it quiescent.
            assert!(registry.try_acquire(&format!("task.{i}")).await);
        }
        assert_eq!(registry.tracked_count().await, 10);
        assert_eq!(registry.prune_idle(Duration::ZERO).await, 10);
        assert_eq!(registry.tracked_count().await, 0);
    }

    #[tokio::test]
    async fn probe_timeout_defaults_to_open_timeout() {
        let config = CircuitBreakerConfig::default();
        assert_eq!(config.half_open_probe_timeout(), config.open_timeout());
        let config = config.with_half_open_probe_timeout_secs(5);
        assert_eq!(config.half_open_probe_timeout(), Duration::from_secs(5));
        assert!(config.is_valid());
        assert!(!CircuitBreakerConfig::default()
            .with_half_open_probe_timeout_secs(0)
            .is_valid());
    }

    #[tokio::test]
    async fn shared_breaker_handle_is_consistent() {
        let registry = TaskTypeCircuitBreakers::with_default(CircuitBreakerConfig::new(2));
        let handle = registry.breaker_for("shared").await;
        handle.record_failure().await;
        // Recording through the handle is visible via the registry.
        registry.record_failure("shared").await;
        assert!(registry.is_open("shared").await);
        assert!(handle.is_open().await);
    }
}
