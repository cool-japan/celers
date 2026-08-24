//! Worker struct and core implementation for task execution.

mod execution;
mod support;

#[cfg(test)]
mod tests;

use crate::adaptive_poll::{AdaptivePoll, PollOutcome};
use crate::affinity::{AffinityDecision, AffinityRegistry};
use crate::batching::{self, CoalesceStrategy};
use crate::cancellation::CancellationToken;
use crate::checkpoint::CheckpointManager;
use crate::circuit_breaker::CircuitBreaker;
use crate::coordinated_rate_limit::{RateLimitDecision, WorkerRateLimitCoordinator};
use crate::dlq::DlqHandler;
use crate::execution_context::{RevocationWatcher, SoftTimeout, TaskExecutionContext};
use crate::health::HealthChecker;
use crate::memory::MemoryTracker;
use crate::middleware;
use crate::poison_pill::PoisonPillDetector;
use crate::routing::RoutingStrategy;
use crate::types::{DynamicConfig, WorkerConfig, WorkerHandle, WorkerMode, WorkerStats};

use execution::{DeadLetterRequest, ExecutionLimits, TaskDispatch};
use support::{
    clamp_defer_delay, effective_max_retries, ActiveTaskGuard, EventSink, InFlightRegistry,
};

use celers_core::time_limit::{TimeLimitConfig, WorkerTimeLimits};
use celers_core::{
    Broker, Event, EventEmitter, NoOpEventEmitter, Result, TaskEvent, TaskEventBuilder, TaskId,
    TaskRegistry, WorkerEventBuilder,
};

use std::sync::atomic::{AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration as StdDuration;
use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::time::{sleep, timeout, Duration};
use tracing::{debug, error, info, warn};

/// How long the dequeue loop waits for a free concurrency permit before
/// looping back to re-check the worker mode and the shutdown channel.
const PERMIT_WAIT: Duration = Duration::from_millis(100);

/// Maximum number of buffered lifecycle events flushed in one `emit_batch`.
const EVENT_FLUSH_BATCH: usize = 64;

/// How long to wait for buffered lifecycle events to flush at shutdown.
const EVENT_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// Why the dequeue loop stopped.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopReason {
    /// A shutdown signal was received on the shutdown channel.
    Shutdown,
    /// The shutdown channel was closed.
    Disconnected,
    /// The worker was switched into draining mode.
    Draining,
}

impl StopReason {
    /// Human-readable reason for logging.
    fn as_str(self) -> &'static str {
        match self {
            StopReason::Shutdown => "shutdown signal",
            StopReason::Disconnected => "shutdown channel closed",
            StopReason::Draining => "draining mode",
        }
    }
}

/// Worker runtime for consuming and executing tasks
pub struct Worker<B: Broker, E: EventEmitter = NoOpEventEmitter> {
    pub(crate) broker: Arc<B>,
    pub(crate) registry: Arc<TaskRegistry>,
    pub(crate) config: WorkerConfig,
    pub(crate) circuit_breaker: Option<Arc<CircuitBreaker>>,
    pub(crate) dlq_handler: Option<Arc<DlqHandler>>,
    pub(crate) shutdown_tx: Option<mpsc::Sender<()>>,
    pub(crate) event_emitter: Arc<E>,
    pub(crate) stats: Arc<WorkerStats>,
    pub(crate) mode: Arc<AtomicU8>, // Stores WorkerMode as u8
    pub(crate) dynamic_config: Arc<RwLock<DynamicConfig>>,
    pub(crate) middleware_stack: Option<Arc<middleware::MiddlewareStack>>,
    /// Cooperative cancellation: watches the broker's revocation Pub/Sub and
    /// trips the matching in-flight task's token (enabled via
    /// [`Worker::with_revocation_watcher`]).
    pub(crate) revocation_watcher: Option<RevocationWatcher>,
    /// Distributed (cluster-wide) rate-limit gate applied before execution
    /// (enabled via [`Worker::with_rate_limit_coordinator`]).
    pub(crate) rate_limit_coordinator: Option<WorkerRateLimitCoordinator>,
    /// Task-affinity admission registry mapping task names to their label
    /// requirements (enabled via [`Worker::with_affinity`]). When set, a task
    /// is matched against the worker's labels before execution and deferred if
    /// the worker cannot serve it. `None` (the default) is a no-op.
    pub(crate) affinity_registry: Option<Arc<AffinityRegistry>>,
    /// Celery-style soft/hard time limits, resolved per task name (enabled via
    /// [`Worker::with_time_limits`]). `None` (the default) leaves the plain
    /// execution timeout as the only bound on a running task.
    pub(crate) time_limits: Option<WorkerTimeLimits>,
    /// Poison-pill quarantine (enabled via [`Worker::with_poison_pill`]).
    /// `None` (the default) is a no-op.
    pub(crate) poison_pill: Option<Arc<PoisonPillDetector>>,
    /// Task-checkpoint store made available to running tasks (enabled via
    /// [`Worker::with_checkpoints`]). `None` (the default) makes
    /// [`save_checkpoint`](crate::execution_context::save_checkpoint) a no-op.
    pub(crate) checkpoints: Option<Arc<CheckpointManager>>,
    /// Liveness/readiness accounting, always on (a handful of atomics).
    pub(crate) health: HealthChecker,
}

impl<B: Broker + 'static> Worker<B, NoOpEventEmitter> {
    /// Create a new worker with default (no-op) event emitter
    pub fn new(broker: B, registry: TaskRegistry, config: WorkerConfig) -> Self {
        Worker::with_event_emitter(broker, registry, config, NoOpEventEmitter::new())
    }

    /// Create a new worker from a shared broker handle (no-op event emitter).
    ///
    /// Useful when the caller needs to retain its own [`Arc`] to the broker (for
    /// example to inspect broker state after the worker has been moved into its
    /// run loop).
    pub fn new_from_arc(broker: Arc<B>, registry: TaskRegistry, config: WorkerConfig) -> Self {
        Worker::with_event_emitter_from_arc(broker, registry, config, NoOpEventEmitter::new())
    }

    /// Create a new worker, opening the configured DLQ storage backend.
    ///
    /// [`Worker::new`] cannot honour a Redis/PostgreSQL
    /// [`DlqConfig::storage`](crate::DlqConfig::storage) because opening one is
    /// async: it falls back to in-process memory (with a warning), so a
    /// deployment that configured a *persistent* dead-letter queue silently got
    /// a volatile one that empties on every restart. Use this constructor
    /// whenever `dlq_config.storage` names a persistent backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the DLQ backend cannot be reached, or if the
    /// configuration names a backend whose cargo feature is not compiled in.
    pub async fn connect(broker: B, registry: TaskRegistry, config: WorkerConfig) -> Result<Self> {
        Worker::connect_with_event_emitter(broker, registry, config, NoOpEventEmitter::new()).await
    }
}

impl<B: Broker + 'static, E: EventEmitter + 'static> Worker<B, E> {
    /// Create a new worker with a custom event emitter
    pub fn with_event_emitter(
        broker: B,
        registry: TaskRegistry,
        config: WorkerConfig,
        event_emitter: E,
    ) -> Self {
        Self::with_event_emitter_from_arc(Arc::new(broker), registry, config, event_emitter)
    }

    /// Create a new worker from a shared broker handle with a custom event emitter.
    pub fn with_event_emitter_from_arc(
        broker: Arc<B>,
        registry: TaskRegistry,
        config: WorkerConfig,
        event_emitter: E,
    ) -> Self {
        // `enable_dlq` is authoritative: `DlqConfig::enabled` defaults to false,
        // so building the handler straight from the default config produced a
        // handler that silently discarded every entry.
        let dlq_handler = Self::effective_dlq_config(&config)
            .map(|dlq_config| Arc::new(DlqHandler::new(dlq_config)));

        Self::assemble(broker, registry, config, event_emitter, dlq_handler)
    }

    /// Build the worker around an already-decided DLQ handler.
    ///
    /// Split out so [`Worker::connect`] does not have to build a throwaway
    /// in-memory handler first: `DlqHandler::new` warns that a configured
    /// persistent backend is being downgraded, and emitting that warning on the
    /// path that *does* honour the backend told operators the exact opposite of
    /// the truth.
    fn assemble(
        broker: Arc<B>,
        registry: TaskRegistry,
        config: WorkerConfig,
        event_emitter: E,
        dlq_handler: Option<Arc<DlqHandler>>,
    ) -> Self {
        let circuit_breaker = if config.enable_circuit_breaker {
            Some(Arc::new(CircuitBreaker::with_config(
                config.circuit_breaker_config.clone(),
            )))
        } else {
            None
        };

        // Initialize dynamic config from static config
        let dynamic_config = DynamicConfig {
            poll_interval_ms: config.poll_interval_ms,
            default_timeout_secs: config.default_timeout_secs,
            max_retries: config.max_retries,
        };

        Self {
            broker,
            registry: Arc::new(registry),
            config,
            circuit_breaker,
            dlq_handler,
            shutdown_tx: None,
            event_emitter: Arc::new(event_emitter),
            stats: Arc::new(WorkerStats::new()),
            mode: Arc::new(AtomicU8::new(WorkerMode::Normal as u8)),
            dynamic_config: Arc::new(RwLock::new(dynamic_config)),
            middleware_stack: None,
            revocation_watcher: None,
            rate_limit_coordinator: None,
            affinity_registry: None,
            time_limits: None,
            poison_pill: None,
            checkpoints: None,
            health: HealthChecker::new(),
        }
    }

    /// The DLQ configuration this worker should actually run with, or `None`
    /// when the dead-letter queue is disabled.
    ///
    /// `enable_dlq` is authoritative over `DlqConfig::enabled`, whose `false`
    /// default otherwise produced a handler that silently discarded every entry.
    fn effective_dlq_config(config: &WorkerConfig) -> Option<crate::dlq::DlqConfig> {
        if !config.enable_dlq {
            return None;
        }
        Some(crate::dlq::DlqConfig {
            enabled: true,
            ..config.dlq_config.clone()
        })
    }

    /// Create a new worker with a custom event emitter, opening the configured
    /// DLQ storage backend.
    ///
    /// See [`Worker::connect`] for why this exists.
    ///
    /// # Errors
    ///
    /// Returns an error if the DLQ backend cannot be reached, or if the
    /// configuration names a backend whose cargo feature is not compiled in.
    pub async fn connect_with_event_emitter(
        broker: B,
        registry: TaskRegistry,
        config: WorkerConfig,
        event_emitter: E,
    ) -> Result<Self> {
        Self::connect_with_event_emitter_from_arc(Arc::new(broker), registry, config, event_emitter)
            .await
    }

    /// Create a new worker from a shared broker handle with a custom event
    /// emitter, opening the configured DLQ storage backend.
    ///
    /// # Errors
    ///
    /// Returns an error if the DLQ backend cannot be reached, or if the
    /// configuration names a backend whose cargo feature is not compiled in.
    pub async fn connect_with_event_emitter_from_arc(
        broker: Arc<B>,
        registry: TaskRegistry,
        config: WorkerConfig,
        event_emitter: E,
    ) -> Result<Self> {
        let dlq_handler = match Self::effective_dlq_config(&config) {
            Some(dlq_config) => Some(Arc::new(DlqHandler::connect(dlq_config).await?)),
            None => None,
        };

        Ok(Self::assemble(
            broker,
            registry,
            config,
            event_emitter,
            dlq_handler,
        ))
    }

    /// Enable cooperative cancellation-during-execution.
    ///
    /// The supplied [`RevocationWatcher`] subscribes to the broker's revocation
    /// Pub/Sub. While the worker runs, every in-flight task is registered with the
    /// watcher's registry and executed inside a [`TaskExecutionContext`] carrying
    /// a [`CancellationToken`](crate::cancellation::CancellationToken). When a
    /// revocation signal for an in-flight task arrives, its token is tripped: the
    /// task's future is raced against the token via [`tokio::select!`], so a
    /// cooperative task stops at its next `is_cancelled()` check (and any task is
    /// aborted at its next `.await`), after which the worker transitions it to
    /// `Revoked`.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_worker::{Worker, WorkerConfig, RevocationWatcher};
    /// use celers_core::TaskRegistry;
    /// # use celers_core::Broker;
    /// # async fn example<B: Broker + 'static>(broker: B) {
    /// let watcher = RevocationWatcher::new();
    /// let publisher = watcher.publisher(); // feed revocations here (or from a broker)
    /// let worker = Worker::new(broker, TaskRegistry::new(), WorkerConfig::default())
    ///     .with_revocation_watcher(watcher);
    /// # let _ = publisher;
    /// # }
    /// ```
    #[must_use]
    pub fn with_revocation_watcher(mut self, watcher: RevocationWatcher) -> Self {
        self.revocation_watcher = Some(watcher);
        self
    }

    /// Enable distributed (cluster-wide) rate-limit coordination.
    ///
    /// Before executing each task the worker acquires a permit from the shared
    /// [`WorkerRateLimitCoordinator`] (keyed by task name or by
    /// [`WorkerConfig::queue_name`](crate::WorkerConfig::queue_name)). If the
    /// shared limiter denies the request the task is deferred — requeued for a
    /// later attempt after the limiter's suggested delay — rather than executed,
    /// so the configured rate is enforced across every worker.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_worker::{Worker, WorkerConfig, WorkerRateLimitCoordinator};
    /// use celers_core::TaskRegistry;
    /// use celers_core::rate_limit::RateLimitConfig;
    /// use celers_core::rate_limit_distributed::InMemoryDistributedBackend;
    /// use std::sync::Arc;
    /// # use celers_core::Broker;
    /// # async fn example<B: Broker + 'static>(broker: B) {
    /// let backend = Arc::new(InMemoryDistributedBackend::new());
    /// let coordinator =
    ///     WorkerRateLimitCoordinator::new(backend, RateLimitConfig::new(10.0).with_burst(20));
    /// let worker = Worker::new(broker, TaskRegistry::new(), WorkerConfig::default())
    ///     .with_rate_limit_coordinator(coordinator);
    /// # }
    /// ```
    #[must_use]
    pub fn with_rate_limit_coordinator(mut self, coordinator: WorkerRateLimitCoordinator) -> Self {
        self.rate_limit_coordinator = Some(coordinator);
        self
    }

    /// Enable label-based task-affinity admission checks.
    ///
    /// The worker advertises the labels in
    /// [`WorkerConfig::worker_labels`](crate::WorkerConfig::worker_labels) and,
    /// before executing each task, looks the task name up in the supplied
    /// [`AffinityRegistry`]. If the task declares affinity requirements that the
    /// worker's labels do not satisfy (a missing *required* label or a present
    /// *anti-affinity* label), the task is deferred — requeued for another
    /// worker — instead of executed. Tasks with no registered affinity are
    /// admitted unconditionally, so this is a no-op until the registry is
    /// populated.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_worker::{Worker, WorkerConfig, WorkerLabels};
    /// use celers_worker::affinity::{AffinityRegistry, TaskAffinity};
    /// use celers_core::TaskRegistry;
    /// # use celers_core::Broker;
    /// # async fn example<B: Broker + 'static>(broker: B) {
    /// let config = WorkerConfig {
    ///     worker_labels: WorkerLabels::from_iter(["gpu", "region:eu"]),
    ///     ..Default::default()
    /// };
    /// let registry = AffinityRegistry::new()
    ///     .with_task("train_model", TaskAffinity::new().require("gpu").anti("spot"));
    /// let worker = Worker::new(broker, TaskRegistry::new(), config)
    ///     .with_affinity(registry);
    /// # }
    /// ```
    #[must_use]
    pub fn with_affinity(mut self, registry: AffinityRegistry) -> Self {
        self.affinity_registry = Some(Arc::new(registry));
        self
    }

    /// Enable Celery-style soft/hard time limits.
    ///
    /// For every task the worker resolves the effective
    /// [`TimeLimitConfig`] for that task *name* (the per-task override merged
    /// onto the manager's default) and applies it to the execution:
    ///
    /// * The **hard** limit becomes part of the execution deadline — the task
    ///   future is raced against the earlier of the hard limit and the task's
    ///   own `timeout_secs` — and expiry aborts the task with a timeout
    ///   failure (retried while the retry budget allows, dead-lettered after,
    ///   with `failure_type = "hard_time_limit"`).
    /// * The **soft** limit is *not* terminal. When it expires the worker trips
    ///   the task's [`SoftTimeout`] signal, counts it in
    ///   [`WorkerStats::soft_timeouts`](crate::WorkerStats::soft_timeouts) and
    ///   logs a warning; the task keeps running and can observe the signal via
    ///   [`check_soft_time_limit`](crate::execution_context::check_soft_time_limit)
    ///   to clean up and return partial work before the hard limit lands.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_worker::{Worker, WorkerConfig};
    /// use celers_core::TaskRegistry;
    /// use celers_core::time_limit::{TimeLimitConfig, WorkerTimeLimits};
    /// use std::time::Duration;
    /// # use celers_core::Broker;
    /// # async fn example<B: Broker + 'static>(broker: B) {
    /// let limits = WorkerTimeLimits::with_default(
    ///     TimeLimitConfig::new()
    ///         .with_soft_limit(Duration::from_secs(30))
    ///         .with_hard_limit(Duration::from_secs(60)),
    /// );
    /// // A single task may run longer; the 30s soft warning still applies.
    /// limits.set_task_limit(
    ///     "generate_report",
    ///     TimeLimitConfig::new().with_hard_limit(Duration::from_secs(600)),
    /// );
    ///
    /// let worker = Worker::new(broker, TaskRegistry::new(), WorkerConfig::default())
    ///     .with_time_limits(limits);
    /// # }
    /// ```
    #[must_use]
    pub fn with_time_limits(mut self, limits: WorkerTimeLimits) -> Self {
        self.time_limits = Some(limits);
        self
    }

    /// Get the configured time limits (if soft/hard time limits are enabled).
    pub fn time_limits(&self) -> Option<&WorkerTimeLimits> {
        self.time_limits.as_ref()
    }

    /// Enable poison-pill detection and quarantine.
    ///
    /// A *poison pill* is a task that keeps failing (or keeps coming back
    /// undelivered) and would otherwise be redelivered forever, burning CPU and
    /// wedging the queue behind it. With a detector installed the worker:
    ///
    /// * records a strike for every failed execution,
    /// * records a *redelivery* strike when a re-attempt arrives whose previous
    ///   failure this worker never saw (another worker's, or one that died
    ///   mid-task — the classic poison-pill signature),
    /// * clears a task's strikes when it eventually succeeds, and
    /// * refuses to execute a quarantined task at all, dead-lettering it
    ///   instead so it stops cycling.
    ///
    /// When [`PoisonPillConfig::decay_window`](crate::PoisonPillConfig::decay_window)
    /// is set, the worker also runs the detector's background pruner for the
    /// duration of its run loop.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_worker::{PoisonPillConfig, PoisonPillDetector, Worker, WorkerConfig};
    /// use celers_core::TaskRegistry;
    /// use std::sync::Arc;
    /// use std::time::Duration;
    /// # use celers_core::Broker;
    /// # async fn example<B: Broker + 'static>(broker: B) {
    /// let detector = Arc::new(PoisonPillDetector::new(
    ///     PoisonPillConfig::new()
    ///         .with_threshold(3)
    ///         .with_decay_window(Duration::from_secs(600)),
    /// ));
    /// let worker = Worker::new(broker, TaskRegistry::new(), WorkerConfig::default())
    ///     .with_poison_pill(Arc::clone(&detector));
    /// # }
    /// ```
    #[must_use]
    pub fn with_poison_pill(mut self, detector: Arc<PoisonPillDetector>) -> Self {
        self.poison_pill = Some(detector);
        self
    }

    /// Get the poison-pill detector (if quarantine is enabled).
    pub fn poison_pill(&self) -> Option<&Arc<PoisonPillDetector>> {
        self.poison_pill.as_ref()
    }

    /// Make a [`CheckpointManager`] available to running tasks.
    ///
    /// The manager is installed into every task's ambient
    /// [`TaskExecutionContext`], so a long-running task body can call
    /// [`save_checkpoint`](crate::execution_context::save_checkpoint) at its own
    /// progress boundaries and
    /// [`load_checkpoint`](crate::execution_context::load_checkpoint) on a later
    /// attempt to resume instead of restarting from scratch — no argument has to
    /// be threaded through the task's call stack. The worker deletes a task's
    /// checkpoints once it completes successfully.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_worker::{CheckpointConfig, CheckpointManager, Worker, WorkerConfig};
    /// use celers_core::TaskRegistry;
    /// use std::sync::Arc;
    /// # use celers_core::Broker;
    /// # async fn example<B: Broker + 'static>(broker: B) {
    /// let checkpoints = Arc::new(CheckpointManager::new(CheckpointConfig::new()));
    /// let worker = Worker::new(broker, TaskRegistry::new(), WorkerConfig::default())
    ///     .with_checkpoints(checkpoints);
    /// # }
    /// ```
    #[must_use]
    pub fn with_checkpoints(mut self, checkpoints: Arc<CheckpointManager>) -> Self {
        self.checkpoints = Some(checkpoints);
        self
    }

    /// Get the checkpoint manager (if task checkpointing is enabled).
    pub fn checkpoints(&self) -> Option<&Arc<CheckpointManager>> {
        self.checkpoints.as_ref()
    }

    /// A shared handle to this worker's liveness/readiness accounting.
    ///
    /// Cloning is cheap and the clone keeps reporting after the worker has been
    /// moved into its run loop, so an embedder can serve `/healthz` and
    /// `/readyz` from it:
    ///
    /// ```no_run
    /// # use celers_worker::{Worker, WorkerConfig};
    /// # use celers_core::{Broker, TaskRegistry};
    /// # async fn example<B: Broker + 'static>(broker: B) {
    /// let worker = Worker::new(broker, TaskRegistry::new(), WorkerConfig::default());
    /// let health = worker.health();
    /// let _handle = worker.run_with_shutdown().await;
    /// // ... elsewhere, in an HTTP handler:
    /// let live = health.is_healthy();
    /// let ready = health.is_ready();
    /// # let _ = (live, ready);
    /// # }
    /// ```
    pub fn health(&self) -> HealthChecker {
        self.health.clone()
    }

    /// Whether `task_id` is quarantined, recording a redelivery strike first
    /// when this delivery is a re-attempt whose failure this worker never saw.
    ///
    /// Striking only in that case is what keeps the two signals from
    /// double-counting: an attempt this worker failed itself already produced a
    /// failure strike, and its redelivery must not produce a second one.
    async fn is_quarantined(&self, task_id: TaskId, spent_retries: u32) -> bool {
        let Some(ref detector) = self.poison_pill else {
            return false;
        };
        if detector.is_poison(&task_id).await {
            return true;
        }
        if spent_retries > 0 && detector.strike_count(&task_id).await == 0 {
            return detector.record_redelivery(task_id).await.is_quarantined();
        }
        false
    }

    /// The effective (merged) time-limit configuration for `task_name`.
    ///
    /// Returns `None` when time limits are disabled, when the task name has no
    /// applicable configuration, or when the resolved configuration is empty.
    /// The merge (per-task override *onto* the manager default) is
    /// [`TaskTimeLimits::get_limit`](celers_core::TaskTimeLimits::get_limit)'s
    /// job; it is reached here through
    /// [`WorkerTimeLimits::create_tracker`], the manager's only public
    /// resolution entry point.
    fn resolve_time_limits(&self, task_id: TaskId, task_name: &str) -> Option<TimeLimitConfig> {
        let limits = self.time_limits.as_ref()?;
        let tracker = limits.create_tracker(&task_id.to_string(), task_name)?;
        Some(tracker.config().clone())
    }

    /// Get the task-affinity registry (if affinity admission is enabled).
    pub fn affinity_registry(&self) -> Option<&Arc<AffinityRegistry>> {
        self.affinity_registry.as_ref()
    }

    /// Decide whether this worker should admit a task of the given name based on
    /// its configured labels and the affinity registry.
    ///
    /// Returns [`AffinityDecision::NoAffinity`] when affinity admission is
    /// disabled or the task declares no (non-empty) affinity, so callers can
    /// treat the absence of a registry as "admit everything".
    fn affinity_decision(&self, task_name: &str) -> AffinityDecision {
        match self.affinity_registry {
            Some(ref registry) => registry.decide(task_name, &self.config.worker_labels),
            None => AffinityDecision::NoAffinity,
        }
    }

    /// Set a custom middleware stack
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_worker::{Worker, WorkerConfig, MiddlewareStack, TracingMiddleware};
    /// use celers_core::TaskRegistry;
    /// # use celers_core::Broker;
    /// # async fn example<B: Broker + 'static>(broker: B) {
    /// let registry = TaskRegistry::new();
    /// let config = WorkerConfig::default();
    ///
    /// let middleware = MiddlewareStack::new()
    ///     .add(TracingMiddleware::new(true));
    ///
    /// let worker = Worker::new(broker, registry, config)
    ///     .with_middleware(middleware);
    /// # }
    /// ```
    pub fn with_middleware(mut self, stack: middleware::MiddlewareStack) -> Self {
        self.middleware_stack = Some(Arc::new(stack));
        self
    }

    /// Get the worker statistics
    pub fn stats(&self) -> &WorkerStats {
        &self.stats
    }

    /// Get a shared handle to the worker statistics.
    ///
    /// Unlike [`stats`](Self::stats), the returned [`Arc`] outlives the worker
    /// once it has been moved into its run loop, so callers can keep observing
    /// counters (active/processed/revoked/rate-limited) after starting it.
    pub fn stats_arc(&self) -> Arc<WorkerStats> {
        Arc::clone(&self.stats)
    }

    /// Get the DLQ handler (if enabled)
    pub fn dlq_handler(&self) -> Option<&Arc<DlqHandler>> {
        self.dlq_handler.as_ref()
    }

    /// Get the revocation watcher (if cooperative cancellation is enabled)
    pub fn revocation_watcher(&self) -> Option<&RevocationWatcher> {
        self.revocation_watcher.as_ref()
    }

    /// Get the distributed rate-limit coordinator (if enabled)
    pub fn rate_limit_coordinator(&self) -> Option<&WorkerRateLimitCoordinator> {
        self.rate_limit_coordinator.as_ref()
    }

    /// Check if this worker can handle a specific task type based on routing configuration
    ///
    /// The configured [`RoutingStrategy`] decides how the worker's tags are
    /// applied:
    ///
    /// - [`RoutingStrategy::Lenient`] (default) and
    ///   [`RoutingStrategy::TaskTypeOnly`] admit anything that is not on the
    ///   worker's exclusion list, honouring the allow-list when one is set.
    /// - [`RoutingStrategy::Strict`] additionally requires the task type to be
    ///   named explicitly in the worker's allow-list, so a worker never picks up
    ///   work it was not told about.
    fn can_handle_task(&self, task_name: &str) -> bool {
        if !self.config.enable_routing {
            return true; // Routing disabled, accept all tasks
        }

        if !self.config.worker_tags.can_handle_task(task_name) {
            return false;
        }

        match self.config.routing_strategy {
            RoutingStrategy::Lenient | RoutingStrategy::TaskTypeOnly => true,
            RoutingStrategy::Strict => self.config.worker_tags.task_types().contains(task_name),
        }
    }

    /// Check whether this worker's feature flags satisfy the task's declared
    /// feature requirements.
    ///
    /// Requirements are looked up in
    /// [`WorkerConfig::task_feature_requirements`](crate::WorkerConfig::task_feature_requirements);
    /// tasks with no registered requirements are admitted unconditionally, so
    /// this is a no-op until the map is populated.
    fn features_satisfied(&self, task_name: &str) -> bool {
        match self.config.task_feature_requirements.get(task_name) {
            Some(requirements) if requirements.has_requirements() => {
                self.config.feature_flags.satisfies(requirements)
            }
            _ => true,
        }
    }

    /// Start the worker loop with graceful shutdown support
    /// Returns a WorkerHandle that can be used to signal shutdown
    pub async fn run_with_shutdown(mut self) -> Result<WorkerHandle> {
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel(1);
        let handle = WorkerHandle {
            shutdown_tx: shutdown_tx.clone(),
            mode: Arc::clone(&self.mode),
            stats: Arc::clone(&self.stats),
            dynamic_config: Arc::clone(&self.dynamic_config),
            health: self.health.clone(),
        };

        self.shutdown_tx = Some(shutdown_tx);

        tokio::spawn(async move {
            if let Err(e) = self.run_loop(Some(&mut shutdown_rx)).await {
                error!("Worker error: {}", e);
            }
        });

        Ok(handle)
    }

    /// Start the worker loop (blocks until shutdown or error)
    pub async fn run(&self) -> Result<()> {
        self.run_loop(None).await
    }

    /// Internal worker loop implementation
    async fn run_loop(&self, mut shutdown_rx: Option<&mut mpsc::Receiver<()>>) -> Result<()> {
        let hostname = self.config.hostname.clone();
        let pid = std::process::id();

        info!(
            "Starting worker with concurrency {} and max retries {}",
            self.config.concurrency, self.config.max_retries
        );

        // Emit worker online event
        if self.config.enable_events {
            let event = WorkerEventBuilder::new(&hostname).online();
            if let Err(e) = self.event_emitter.emit(event).await {
                warn!("Failed to emit worker-online event: {}", e);
            }
        }

        // Lifecycle events are telemetry: buffer them so a task never waits on
        // an event round trip, while preserving emission order.
        let (events, event_drainer) =
            if self.config.enable_events && self.event_emitter.is_enabled() {
                let (sink, drainer) = EventSink::buffered(
                    Arc::clone(&self.event_emitter),
                    self.config.event_buffer_capacity,
                    EVENT_FLUSH_BATCH,
                );
                (sink, Some(drainer))
            } else {
                (EventSink::disabled(), None)
            };

        // Start heartbeat task if configured
        let heartbeat_handle =
            if self.config.enable_events && self.config.heartbeat_interval_secs > 0 {
                let heartbeat_hostname = hostname.clone();
                let heartbeat_interval = Duration::from_secs(self.config.heartbeat_interval_secs);
                let heartbeat_emitter = Arc::clone(&self.event_emitter);
                let heartbeat_stats = Arc::clone(&self.stats);
                let heartbeat_freq = self.config.heartbeat_interval_secs as f64;

                Some(tokio::spawn(async move {
                    Self::heartbeat_loop(
                        heartbeat_hostname,
                        heartbeat_interval,
                        heartbeat_emitter,
                        heartbeat_stats,
                        heartbeat_freq,
                    )
                    .await;
                }))
            } else {
                None
            };

        // Start the revocation watcher if cooperative cancellation is enabled.
        // It subscribes to the broker's revocation Pub/Sub and trips the matching
        // in-flight task's cancellation token.
        let revocation_handle = self.revocation_watcher.as_ref().map(|w| {
            info!("Starting revocation watcher for cooperative cancellation");
            w.spawn()
        });

        // A DLQ TTL does nothing on its own: something has to run the sweep, or
        // expired entries accumulate forever and `ttl_seconds` is decoration.
        let dlq_cleanup_handle = self
            .dlq_handler
            .as_ref()
            .zip(self.config.dlq_config.ttl_seconds)
            .map(|(handler, ttl_seconds)| {
                let interval = support::dlq_cleanup_interval(ttl_seconds);
                info!(
                    "Starting DLQ TTL sweep every {:?} (entry TTL {}s)",
                    interval, ttl_seconds
                );
                Arc::clone(handler).spawn_cleanup_task(interval)
            });

        // Poison-pill tracking records for task ids that are simply never seen
        // again only expire if something prunes them. `spawn_pruner` is a no-op
        // (returns `None`) unless a decay window is configured.
        let poison_pruner_handle = self
            .poison_pill
            .as_ref()
            .and_then(|detector| detector.spawn_pruner());

        let result = self
            .run_loop_inner(&mut shutdown_rx, &hostname, pid, &events)
            .await;

        // Stop heartbeat task
        if let Some(handle) = heartbeat_handle {
            handle.abort();
        }

        // Stop the revocation watcher
        if let Some(handle) = revocation_handle {
            handle.abort();
        }

        // Stop the DLQ TTL sweep
        if let Some(handle) = dlq_cleanup_handle {
            handle.abort();
        }

        // Stop the poison-pill pruner
        if let Some(handle) = poison_pruner_handle {
            handle.abort();
        }

        // Flush buffered lifecycle events before the offline event, so consumers
        // never see "offline" ahead of a task's terminal event.
        let dropped_events = events.dropped();
        drop(events);
        if let Some(drainer) = event_drainer {
            if timeout(EVENT_FLUSH_TIMEOUT, drainer).await.is_err() {
                warn!("Timed out flushing buffered lifecycle events");
            }
        }
        if dropped_events > 0 {
            warn!(
                "Dropped {} lifecycle event(s) while the event buffer was full",
                dropped_events
            );
        }

        // Emit worker offline event
        if self.config.enable_events {
            let event = WorkerEventBuilder::new(&hostname).offline();
            if let Err(e) = self.event_emitter.emit(event).await {
                warn!("Failed to emit worker-offline event: {}", e);
            }
        }

        result
    }

    /// Heartbeat loop that periodically emits worker-heartbeat events
    async fn heartbeat_loop<EE: EventEmitter>(
        hostname: String,
        interval: Duration,
        event_emitter: Arc<EE>,
        stats: Arc<WorkerStats>,
        freq: f64,
    ) {
        loop {
            sleep(interval).await;

            let active = stats.active() as u32;
            let processed = stats.processed();

            // Get system load average (on Unix systems)
            let loadavg = Self::get_load_average();

            let event =
                WorkerEventBuilder::new(&hostname).heartbeat(active, processed, loadavg, freq);

            if let Err(e) = event_emitter.emit(event).await {
                debug!("Failed to emit worker-heartbeat event: {}", e);
            }
        }
    }

    /// Get system load average (returns [0.0, 0.0, 0.0] where unsupported)
    ///
    /// Delegates to [`crate::sysinfo::read_load_average`], which reads
    /// `/proc/loadavg` on Linux and `getloadavg(3)` on the BSDs/macOS — the
    /// latter has no `/proc`, where the previous inline implementation silently
    /// reported a flat zero load in every heartbeat.
    fn get_load_average() -> [f64; 3] {
        crate::sysinfo::read_load_average().unwrap_or([0.0, 0.0, 0.0])
    }

    /// Current poll interval from the (runtime updatable) dynamic config.
    fn poll_interval(&self) -> Duration {
        let ms = self
            .dynamic_config
            .read()
            .map(|c| c.poll_interval_ms)
            .unwrap_or(1000);
        Duration::from_millis(ms)
    }

    /// Sleep for `duration`, returning early with `true` if a shutdown signal
    /// arrives first (so shutdown latency never inherits a poll or backoff
    /// interval).
    async fn sleep_or_shutdown(
        shutdown_rx: &mut Option<&mut mpsc::Receiver<()>>,
        duration: Duration,
    ) -> bool {
        match shutdown_rx.as_mut() {
            Some(rx) => {
                tokio::select! {
                    biased;
                    _ = rx.recv() => true,
                    () = sleep(duration) => false,
                }
            }
            None => {
                sleep(duration).await;
                false
            }
        }
    }

    /// Acquire up to `wanted` concurrency permits, waiting at most
    /// [`PERMIT_WAIT`] for the first one.
    ///
    /// Returning `None` means the worker is saturated: the caller loops back to
    /// re-check the worker mode and shutdown channel instead of dequeuing more
    /// work it cannot run.
    async fn acquire_permits(
        permits: &Arc<Semaphore>,
        wanted: usize,
    ) -> Option<Vec<OwnedSemaphorePermit>> {
        let first = match timeout(PERMIT_WAIT, Arc::clone(permits).acquire_owned()).await {
            Ok(Ok(permit)) => permit,
            Ok(Err(_closed)) => return None,
            Err(_elapsed) => return None,
        };

        let mut held = Vec::with_capacity(wanted.max(1));
        held.push(first);
        while held.len() < wanted {
            match Arc::clone(permits).try_acquire_owned() {
                Ok(permit) => held.push(permit),
                Err(_) => break,
            }
        }
        Some(held)
    }

    /// Defer a message: return it to the queue for a later attempt without
    /// treating it as a failed execution.
    async fn defer_message(&self, task_id: &TaskId, receipt_handle: Option<&str>, reason: &str) {
        self.stats.task_deferred();
        if let Err(e) = self.broker.reject(task_id, receipt_handle, true).await {
            error!("Failed to defer task {} ({}): {}", task_id, reason, e);
        }
    }

    /// Inner worker loop (separated to ensure offline event is always emitted)
    async fn run_loop_inner(
        &self,
        shutdown_rx: &mut Option<&mut mpsc::Receiver<()>>,
        hostname: &str,
        pid: u32,
        events: &EventSink,
    ) -> Result<()> {
        // Adaptive poll-interval controller (clock-free decision math). When
        // adaptive polling is disabled the controller is left as `None` and the
        // legacy fixed `poll_interval_ms` path is used unchanged.
        let mut adaptive_poll = if self.config.enable_adaptive_poll {
            Some(AdaptivePoll::new(self.config.adaptive_poll_config))
        } else {
            None
        };

        // Concurrency control: `WorkerConfig::concurrency` permits, acquired
        // *before* dequeuing so a saturated worker stops pulling messages out of
        // the broker instead of buffering unbounded work in RAM.
        let concurrency = self
            .config
            .concurrency
            .max(1)
            .min(u32::MAX as usize)
            .min(Semaphore::MAX_PERMITS);
        let permits = Arc::new(Semaphore::new(concurrency));

        // Messages dispatched but not yet disposed of, so a shutdown deadline
        // can hand them back to the broker instead of stranding them.
        let in_flight = InFlightRegistry::new();

        let memory_tracker = if self.config.track_memory_usage {
            Some(Arc::new(MemoryTracker::new()))
        } else {
            None
        };

        let retry_config = self.config.get_retry_config();

        let stop_reason = loop {
            // Check current worker mode
            let current_mode = WorkerMode::from(self.mode.load(Ordering::SeqCst));

            // If draining, stop dequeuing and let in-flight work finish
            if current_mode.is_draining() {
                break StopReason::Draining;
            }

            // Check for shutdown signal if receiver is provided
            if let Some(ref mut rx) = shutdown_rx {
                match rx.try_recv() {
                    Ok(_) => break StopReason::Shutdown,
                    Err(mpsc::error::TryRecvError::Disconnected) => break StopReason::Disconnected,
                    Err(mpsc::error::TryRecvError::Empty) => {
                        // No shutdown signal, continue
                    }
                }
            }

            // If in maintenance mode, skip dequeuing and sleep
            if current_mode.is_maintenance() {
                let poll_interval = self.poll_interval();
                debug!(
                    "Worker in maintenance mode, sleeping for {:?}",
                    poll_interval
                );
                if Self::sleep_or_shutdown(shutdown_rx, poll_interval).await {
                    break StopReason::Shutdown;
                }
                continue;
            }

            // Backpressure: never dequeue more than we have capacity to run.
            let wanted = if self.config.enable_batch_dequeue {
                self.config.batch_size.max(1).min(concurrency)
            } else {
                1
            };
            let Some(mut held_permits) = Self::acquire_permits(&permits, wanted).await else {
                debug!("Worker at concurrency limit ({}), waiting", concurrency);
                continue;
            };
            let capacity = held_permits.len();

            // Dequeue tasks (single or batch depending on configuration)
            let messages_result = if self.config.enable_batch_dequeue {
                debug!("Batch dequeue enabled, fetching up to {} tasks", capacity);
                self.broker.dequeue_batch(capacity).await
            } else {
                // Single task dequeue (convert to Vec for uniform handling)
                match self.broker.dequeue().await {
                    Ok(Some(msg)) => Ok(vec![msg]),
                    Ok(None) => Ok(vec![]),
                    Err(e) => Err(e),
                }
            };

            // Coalesce duplicate tasks within the dequeued batch (drop redundant
            // work sharing a coalescing key) before processing. The dropped
            // duplicates are acknowledged so an at-least-once broker removes them
            // rather than redelivering them.
            let messages_result = match messages_result {
                Ok(messages) if self.config.enable_coalescing && messages.len() > 1 => {
                    let strategy = self.config.coalescing_config.strategy;
                    let raw = messages.len();
                    let (kept, dropped) =
                        self.coalesce_and_ack_duplicates(messages, strategy).await;
                    if !dropped.is_empty() {
                        info!(
                            "Coalesced {} duplicate task(s) ({} -> {} distinct)",
                            dropped.len(),
                            raw,
                            kept.len()
                        );
                    }
                    Ok(kept)
                }
                other => other,
            };

            match messages_result {
                Ok(messages) if !messages.is_empty() => {
                    if let Some(ref mut ap) = adaptive_poll {
                        ap.record(PollOutcome::found(messages.len()));
                    }
                    if self.config.enable_batch_dequeue {
                        info!("Dequeued {} tasks in batch", messages.len());
                    }

                    let mut dispatched = 0usize;
                    let mut defer_delay: Option<Duration> = None;

                    // Process each message
                    for msg in messages {
                        // Every dispatched message consumes one permit; a
                        // deferred/rejected one releases its permit immediately.
                        let permit = held_permits.pop();

                        let task_id = msg.task.metadata.id;
                        let task_name = msg.task.metadata.name.clone();
                        info!("Processing task {} ({})", task_id, task_name);

                        // Emit task-received event
                        events.emit(
                            TaskEventBuilder::new(task_id, &task_name)
                                .hostname(hostname)
                                .pid(pid)
                                .received(),
                        );

                        // Poison-pill quarantine comes before every other
                        // admission check: a quarantined task must not run no
                        // matter which worker or queue it lands on, and letting
                        // it be deferred instead would put it straight back in
                        // the queue it is wedging.
                        if self
                            .is_quarantined(task_id, execution::spent_retries(&msg.task))
                            .await
                        {
                            warn!(
                                "Task {} ('{}') is quarantined as a poison pill; dead-lettering \
                                 instead of executing it",
                                task_id, task_name
                            );

                            events.emit(Event::Task(TaskEvent::Rejected {
                                task_id,
                                task_name: Some(task_name.clone()),
                                hostname: hostname.to_string(),
                                timestamp: chrono::Utc::now(),
                                reason: "Quarantined as a poison pill".to_string(),
                            }));

                            execution::dead_letter(
                                &self.broker,
                                self.dlq_handler.as_ref(),
                                events,
                                hostname,
                                pid,
                                DeadLetterRequest {
                                    task: &msg.task,
                                    task_id,
                                    receipt_handle: msg.receipt_handle.as_deref(),
                                    retry_count: execution::spent_retries(&msg.task),
                                    error_msg: "Task quarantined as a poison pill",
                                    failure_type: "poison_pill",
                                    extra_metadata: vec![("task_name", task_name.clone())],
                                    dispose: true,
                                },
                            )
                            .await;
                            continue;
                        }

                        // Check routing - can this worker handle this task type?
                        if !self.can_handle_task(&task_name) {
                            warn!(
                                "Worker routing: cannot handle task type '{}', deferring task {}",
                                task_name, task_id
                            );

                            events.emit(Event::Task(TaskEvent::Rejected {
                                task_id,
                                task_name: Some(task_name.clone()),
                                hostname: hostname.to_string(),
                                timestamp: chrono::Utc::now(),
                                reason: "Worker routing mismatch".to_string(),
                            }));

                            // Defer with requeue (another worker might handle it)
                            self.defer_message(&task_id, msg.receipt_handle.as_deref(), "routing")
                                .await;
                            defer_delay = Some(
                                self.admission_defer_delay()
                                    .max(defer_delay.unwrap_or(Duration::ZERO)),
                            );
                            continue;
                        }

                        // Task-affinity admission: match this worker's labels
                        // against the task's affinity requirements. A worker that
                        // lacks a required label, or carries an anti-affinity
                        // label, defers the task (requeues it) so a better-suited
                        // worker can serve it. Tasks with no registered affinity
                        // are admitted unconditionally (no-op).
                        if let AffinityDecision::Defer = self.affinity_decision(&task_name) {
                            warn!(
                                "Worker affinity: labels do not satisfy task type '{}', \
                                 deferring task {}",
                                task_name, task_id
                            );

                            events.emit(Event::Task(TaskEvent::Rejected {
                                task_id,
                                task_name: Some(task_name.clone()),
                                hostname: hostname.to_string(),
                                timestamp: chrono::Utc::now(),
                                reason: "Worker affinity mismatch".to_string(),
                            }));

                            // Defer (requeue) so a worker with matching labels can
                            // pick the task up.
                            self.defer_message(&task_id, msg.receipt_handle.as_deref(), "affinity")
                                .await;
                            defer_delay = Some(
                                self.admission_defer_delay()
                                    .max(defer_delay.unwrap_or(Duration::ZERO)),
                            );
                            continue;
                        }

                        // Feature-flag admission: a task may declare features the
                        // worker must have enabled to run it.
                        if !self.features_satisfied(&task_name) {
                            warn!(
                                "Worker features do not satisfy task type '{}', deferring task {}",
                                task_name, task_id
                            );

                            events.emit(Event::Task(TaskEvent::Rejected {
                                task_id,
                                task_name: Some(task_name.clone()),
                                hostname: hostname.to_string(),
                                timestamp: chrono::Utc::now(),
                                reason: "Worker feature-flag mismatch".to_string(),
                            }));

                            self.defer_message(
                                &task_id,
                                msg.receipt_handle.as_deref(),
                                "feature flags",
                            )
                            .await;
                            defer_delay = Some(
                                self.admission_defer_delay()
                                    .max(defer_delay.unwrap_or(Duration::ZERO)),
                            );
                            continue;
                        }

                        // Check circuit breaker
                        if let Some(ref cb) = self.circuit_breaker {
                            if !cb.should_allow(&task_name).await {
                                // Two very different rejections share this
                                // branch. A *half-open* circuit rejects because
                                // its trial-probe budget is already spoken for:
                                // nothing is known to be wrong with this task,
                                // so it is deferred (requeued) like any other
                                // admission miss. Only a genuinely *open*
                                // circuit is terminal.
                                if cb.get_state(&task_name).await.is_half_open() {
                                    debug!(
                                        "Circuit breaker HALF-OPEN for task type '{}' with its \
                                         probe budget in use, deferring task {}",
                                        task_name, task_id
                                    );

                                    events.emit(Event::Task(TaskEvent::Rejected {
                                        task_id,
                                        task_name: Some(task_name.clone()),
                                        hostname: hostname.to_string(),
                                        timestamp: chrono::Utc::now(),
                                        reason: "Circuit breaker HALF-OPEN (probe budget in use)"
                                            .to_string(),
                                    }));

                                    self.defer_message(
                                        &task_id,
                                        msg.receipt_handle.as_deref(),
                                        "circuit breaker half-open",
                                    )
                                    .await;
                                    defer_delay = Some(
                                        self.admission_defer_delay()
                                            .max(defer_delay.unwrap_or(Duration::ZERO)),
                                    );
                                    continue;
                                }

                                warn!(
                                    "Circuit breaker OPEN for task type '{}', failing task {}",
                                    task_name, task_id
                                );

                                events.emit(Event::Task(TaskEvent::Rejected {
                                    task_id,
                                    task_name: Some(task_name.clone()),
                                    hostname: hostname.to_string(),
                                    timestamp: chrono::Utc::now(),
                                    reason: "Circuit breaker OPEN".to_string(),
                                }));

                                // Terminal for the caller: emit task-failed and
                                // record a DLQ entry so an open circuit is an
                                // observable failure rather than a task that
                                // silently disappears.
                                execution::dead_letter(
                                    &self.broker,
                                    self.dlq_handler.as_ref(),
                                    events,
                                    hostname,
                                    pid,
                                    DeadLetterRequest {
                                        task: &msg.task,
                                        task_id,
                                        receipt_handle: msg.receipt_handle.as_deref(),
                                        retry_count: execution::spent_retries(&msg.task),
                                        error_msg: "Circuit breaker OPEN for task type",
                                        failure_type: "circuit_breaker",
                                        extra_metadata: vec![("task_name", task_name.clone())],
                                        dispose: true,
                                    },
                                )
                                .await;
                                continue;
                            }
                        }

                        // Distributed rate-limit gate: acquire a permit from the
                        // cluster-wide limiter before executing. If denied, defer
                        // the task by requeueing so another attempt happens later
                        // (respecting the limiter's suggested retry delay), rather
                        // than running it and exceeding the shared rate.
                        if let Some(ref coordinator) = self.rate_limit_coordinator {
                            match coordinator
                                .acquire(&task_name, &self.config.queue_name)
                                .await
                            {
                                Ok(RateLimitDecision::Allowed) => {
                                    // Permit acquired; proceed to execution.
                                }
                                Ok(RateLimitDecision::Denied {
                                    retry_after,
                                    remaining,
                                }) => {
                                    debug!(
                                        "Distributed rate limit denied task {} ('{}'): \
                                         remaining ~{:.1}, retry after {:?}; deferring",
                                        task_id, task_name, remaining, retry_after
                                    );
                                    self.stats.task_rate_limited();

                                    // The circuit breaker already admitted this
                                    // task (and, while half-open, handed it a
                                    // trial slot); give the slot back since the
                                    // task is not going to run.
                                    if let Some(ref cb) = self.circuit_breaker {
                                        cb.release_probe(&task_name).await;
                                    }

                                    // Defer (requeue) so the task is retried later.
                                    self.defer_message(
                                        &task_id,
                                        msg.receipt_handle.as_deref(),
                                        "rate limit",
                                    )
                                    .await;
                                    // Honour the limiter's own retry hint (clamped
                                    // into the configured band) instead of
                                    // discarding it and spinning.
                                    defer_delay = Some(
                                        self.clamped_defer_delay(retry_after)
                                            .max(defer_delay.unwrap_or(Duration::ZERO)),
                                    );
                                    continue;
                                }
                                Err(e) => {
                                    // Backend unavailable: fail open (allow) so a
                                    // limiter outage does not stall the worker, but
                                    // record it for visibility.
                                    warn!(
                                        "Distributed rate-limit backend error for task {} ('{}'): \
                                         {}; allowing task (fail-open)",
                                        task_id, task_name, e
                                    );
                                }
                            }
                        }

                        // Execute task with timeout (use dynamic config if task doesn't specify)
                        let (default_timeout, dynamic_max_retries) = self
                            .dynamic_config
                            .read()
                            .map(|c| (c.default_timeout_secs, c.max_retries))
                            .unwrap_or((300, self.config.max_retries));
                        let timeout_secs =
                            msg.task.metadata.timeout_secs.unwrap_or(default_timeout);

                        // Celery-style time limits for this task name: the hard
                        // limit joins the execution deadline, the soft one is
                        // armed as a cooperative warning.
                        let limits = match self.resolve_time_limits(task_id, &task_name) {
                            Some(ref config) => {
                                ExecutionLimits::from_timeout(timeout_secs).with_time_limits(config)
                            }
                            None => ExecutionLimits::from_timeout(timeout_secs),
                        };

                        // Cooperative cancellation: register this task as in-flight
                        // and obtain its cancellation token + execution context. The
                        // token is tripped by the revocation watcher if a matching
                        // revocation signal arrives while the task runs.
                        //
                        // A soft time limit needs the same ambient context (that is
                        // where the task reads its `SoftTimeout` from), so the
                        // context is also installed when revocation is disabled but
                        // a soft limit applies. The token is then a private one that
                        // nothing can trip.
                        let exec_context = match self.revocation_watcher {
                            Some(ref watcher) => {
                                let token = watcher.register(task_id).await;
                                Some(TaskExecutionContext::with_soft_timeout(
                                    token,
                                    SoftTimeout::new(task_id, limits.soft_limit),
                                ))
                            }
                            // A soft limit or a checkpoint store also has to be
                            // reachable from inside the task, so the context is
                            // installed for those too.
                            None if limits.soft_limit.is_some() || self.checkpoints.is_some() => {
                                Some(TaskExecutionContext::with_soft_timeout(
                                    CancellationToken::new(task_id),
                                    SoftTimeout::new(task_id, limits.soft_limit),
                                ))
                            }
                            None => None,
                        };
                        let exec_context = match self.checkpoints {
                            Some(ref manager) => {
                                exec_context.map(|ctx| ctx.with_checkpoints(Arc::clone(manager)))
                            }
                            None => exec_context,
                        };

                        // The task's own retry request, capped by the worker's
                        // (runtime updatable) retry budget.
                        let task_max_retries = msg.task.metadata.max_retries;
                        let receipt_handle = msg.receipt_handle;

                        let dispatch = TaskDispatch {
                            broker: Arc::clone(&self.broker),
                            registry: Arc::clone(&self.registry),
                            task: Arc::new(msg.task),
                            task_id,
                            receipt_handle: receipt_handle.clone(),
                            events: events.clone(),
                            hostname: hostname.to_string(),
                            pid,
                            stats: Arc::clone(&self.stats),
                            middleware: self.middleware_stack.clone(),
                            dlq_handler: self.dlq_handler.clone(),
                            circuit_breaker: self.circuit_breaker.clone(),
                            revocation_watcher: self.revocation_watcher.clone(),
                            exec_context,
                            in_flight: in_flight.clone(),
                            memory_tracker: memory_tracker.clone(),
                            poison_pill: self.poison_pill.clone(),
                            checkpoints: self.checkpoints.clone(),
                            health: self.health.clone(),
                            limits,
                            max_retries: effective_max_retries(
                                task_max_retries,
                                dynamic_max_retries,
                            ),
                            retry_config: retry_config.clone(),
                            max_result_size_bytes: self.config.max_result_size_bytes,
                        };

                        in_flight.register(task_id, receipt_handle);

                        // Count the task as active *before* spawning: both drain
                        // paths gate on this counter, and a task that is queued
                        // but not yet polled must not look like idle capacity.
                        self.stats.task_started();
                        let guard = ActiveTaskGuard::new(Arc::clone(&self.stats), permit);

                        tokio::spawn(execution::run_dispatched_task(dispatch, guard));
                        dispatched += 1;
                    }

                    // Nothing ran: back off before polling again so a queue full
                    // of undeliverable work cannot spin the loop at 100% CPU.
                    if let Some(delay) = defer_delay.filter(|d| dispatched == 0 && !d.is_zero()) {
                        debug!("All dequeued messages deferred, backing off {:?}", delay);
                        if Self::sleep_or_shutdown(shutdown_rx, delay).await {
                            break StopReason::Shutdown;
                        }
                    }
                }
                Ok(_) => {
                    // Queue is empty (messages vec is empty), sleep before next
                    // poll. The adaptive controller backs the interval off on
                    // repeated empties; otherwise the fixed dynamic interval is
                    // used.
                    let sleep_for = match adaptive_poll {
                        Some(ref mut ap) => ap.record(PollOutcome::Empty),
                        None => self.poll_interval(),
                    };
                    debug!("Queue empty, sleeping for {:?}", sleep_for);
                    if Self::sleep_or_shutdown(shutdown_rx, sleep_for).await {
                        break StopReason::Shutdown;
                    }
                }
                Err(e) => {
                    // Dequeue failed: back off like an empty poll under the
                    // adaptive controller so a flapping broker is not hammered.
                    let sleep_for = match adaptive_poll {
                        Some(ref mut ap) => ap.record(PollOutcome::Error),
                        None => self.poll_interval(),
                    };
                    error!("Error dequeueing tasks: {}", e);
                    if Self::sleep_or_shutdown(shutdown_rx, sleep_for).await {
                        break StopReason::Shutdown;
                    }
                }
            }
        };

        info!(
            "Worker stopping ({}), draining in-flight tasks",
            stop_reason.as_str()
        );
        self.drain_in_flight(&permits, &in_flight, concurrency)
            .await;
        info!("Worker stopped ({})", stop_reason.as_str());

        Ok(())
    }

    /// Wait for every dispatched task to finish, then hand anything still
    /// undisposed back to the broker.
    ///
    /// The concurrency semaphore doubles as the drain barrier: holding all
    /// `concurrency` permits means no task is running. On deadline (or when
    /// [`WorkerConfig::graceful_shutdown`](crate::WorkerConfig::graceful_shutdown)
    /// is off) the messages that were dequeued but never disposed of are
    /// requeued, so they are redelivered instead of being stranded in the
    /// broker's processing list with no reaper to recover them.
    async fn drain_in_flight(
        &self,
        permits: &Arc<Semaphore>,
        in_flight: &InFlightRegistry,
        concurrency: usize,
    ) {
        let deadline = Duration::from_secs(self.config.shutdown_timeout_secs);
        let drain_permits = u32::try_from(concurrency).unwrap_or(u32::MAX);

        if self.config.graceful_shutdown && !deadline.is_zero() {
            let outstanding = in_flight.len();
            if outstanding > 0 {
                info!(
                    "Waiting up to {:?} for {} in-flight task(s) to finish",
                    deadline, outstanding
                );
            }
            match timeout(deadline, permits.acquire_many(drain_permits)).await {
                Ok(Ok(_all_permits)) => {
                    info!("All in-flight tasks completed");
                }
                Ok(Err(e)) => {
                    warn!("Concurrency semaphore closed while draining: {}", e);
                }
                Err(_elapsed) => {
                    warn!(
                        "Graceful shutdown deadline of {:?} exceeded with {} task(s) still \
                         running; requeueing their messages",
                        deadline,
                        in_flight.len()
                    );
                }
            }
        } else if !in_flight.is_empty() {
            warn!(
                "Graceful shutdown disabled; requeueing {} in-flight message(s)",
                in_flight.len()
            );
        }

        // Whatever is left was never disposed of by its task: give it back to
        // the broker rather than losing it.
        for (task_id, receipt_handle) in in_flight.take_all() {
            warn!("Requeueing undisposed task {} at shutdown", task_id);
            if let Err(e) = self
                .broker
                .reject(&task_id, receipt_handle.as_deref(), true)
                .await
            {
                error!(
                    "Failed to requeue in-flight task {} at shutdown: {}",
                    task_id, e
                );
            }
        }
    }

    /// Deferral delay for admission decisions (routing / affinity / features).
    fn admission_defer_delay(&self) -> Duration {
        clamp_defer_delay(
            Duration::from_millis(self.config.defer_delay_ms),
            self.config.defer_delay_ms,
            self.config.defer_max_delay_ms,
        )
    }

    /// Clamp an externally supplied delay (e.g. a rate limiter's `retry_after`)
    /// into the configured deferral band.
    fn clamped_defer_delay(&self, requested: Duration) -> Duration {
        clamp_defer_delay(
            requested,
            self.config.defer_delay_ms,
            self.config.defer_max_delay_ms,
        )
    }

    /// Coalesce duplicate messages within a dequeued batch, acknowledging the
    /// dropped duplicates so an at-least-once broker removes them.
    ///
    /// Returns `(survivors, dropped)`. Survivors are deduplicated by
    /// [`batching::broker_message_coalesce_key`] preserving first-seen order;
    /// the chosen representative follows `strategy`. Duplicates are best-effort
    /// acked (a failed ack is logged but does not abort processing).
    ///
    /// # Result loss
    ///
    /// The default coalescing key is `(task name, payload hash)`, which does
    /// **not** include the task id: two independent submissions with identical
    /// arguments coalesce into one, and the dropped one never runs and never
    /// produces a result. Set
    /// [`WorkerConfig::coalesce_require_same_task_id`](crate::WorkerConfig::coalesce_require_same_task_id)
    /// to restrict coalescing to true redelivery duplicates (same task id),
    /// which is lossless.
    async fn coalesce_and_ack_duplicates(
        &self,
        messages: Vec<celers_core::BrokerMessage>,
        strategy: CoalesceStrategy,
    ) -> (
        Vec<celers_core::BrokerMessage>,
        Vec<celers_core::BrokerMessage>,
    ) {
        use std::collections::HashMap;

        let require_same_id = self.config.coalesce_require_same_task_id;

        let mut survivors: Vec<celers_core::BrokerMessage> = Vec::with_capacity(messages.len());
        let mut dropped: Vec<celers_core::BrokerMessage> = Vec::new();
        let mut index: HashMap<(Option<TaskId>, String, u64), usize> =
            HashMap::with_capacity(messages.len());

        for msg in messages {
            let (name, payload_hash) = batching::broker_message_coalesce_key(&msg);
            let key = if require_same_id {
                (Some(msg.task.metadata.id), name, payload_hash)
            } else {
                (None, name, payload_hash)
            };

            if let Some(&existing_idx) = index.get(&key) {
                match strategy {
                    CoalesceStrategy::KeepFirst => {
                        // Drop the newcomer.
                        dropped.push(msg);
                    }
                    CoalesceStrategy::KeepLast => {
                        // The newcomer wins its slot; the prior survivor is dropped.
                        let prev = std::mem::replace(&mut survivors[existing_idx], msg);
                        dropped.push(prev);
                    }
                }
            } else {
                index.insert(key, survivors.len());
                survivors.push(msg);
            }
        }

        // Acknowledge dropped duplicates so they are not redelivered.
        for msg in &dropped {
            let task_id = msg.task.metadata.id;
            if let Err(e) = self
                .broker
                .ack(&task_id, msg.receipt_handle.as_deref())
                .await
            {
                warn!(
                    "Failed to acknowledge coalesced duplicate task {}: {}",
                    task_id, e
                );
            }
        }

        (survivors, dropped)
    }

    /// Calculate the backoff delay applied before retry attempt `retry_count`.
    ///
    /// Delegates to the effective [`RetryConfig`](crate::RetryConfig) (the
    /// explicit `retry_config` when set, otherwise the legacy
    /// `retry_base_delay_ms` / `retry_max_delay_ms` pair) — the same value the
    /// execution loop schedules a retry with. The computation is done in
    /// floating point and capped, where the previous
    /// `base * 2u64.pow(retry_count)` overflowed and panicked for large retry
    /// counts.
    pub fn calculate_backoff_delay(&self, retry_count: u32) -> StdDuration {
        support::backoff_delay(&self.config.get_retry_config(), retry_count)
    }
}
