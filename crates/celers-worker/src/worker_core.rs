//! Worker struct and core implementation for task execution.

mod broker_revocation;
mod control_wiring;
mod execution;
mod runtime;
pub(crate) mod support;

#[cfg(test)]
mod loop_tests;
#[cfg(test)]
mod tests;

use crate::adaptive_poll::{AdaptivePoll, PollOutcome};
use crate::affinity::{AffinityDecision, AffinityRegistry};
use crate::cancellation::CancellationToken;
use crate::checkpoint::CheckpointManager;
use crate::circuit_breaker::CircuitBreaker;
use crate::control::{ControlService, ControlSurface, RuntimeRateLimitDecision, RuntimeRateLimits};
use crate::coordinated_rate_limit::{RateLimitDecision, WorkerRateLimitCoordinator};
use crate::dlq::DlqHandler;
use crate::execution_context::{RevocationWatcher, SoftTimeout, TaskExecutionContext};
use crate::health::HealthChecker;
use crate::memory::MemoryTracker;
use crate::middleware;
use crate::poison_pill::PoisonPillDetector;
use crate::routing::RoutingStrategy;
use crate::types::{DynamicConfig, WorkerConfig, WorkerHandle, WorkerMode, WorkerStats};

use execution::{DeadLetterRequest, ExecutionLimits, TaskDispatch, UnverifiedMessage};
use support::{effective_max_retries, ActiveTaskGuard, EventSink, InFlightRegistry};

use celers_core::control_transport::ControlTransport;
use celers_core::revocation::WorkerRevocationManager;
use celers_core::time_limit::{TimeLimitConfig, WorkerTimeLimits};
use celers_core::{
    Broker, Event, EventEmitter, NoOpEventEmitter, Result, TaskEvent, TaskEventBuilder, TaskId,
    TaskRegistry, WorkerEventBuilder,
};

use std::sync::atomic::{AtomicU64, AtomicU8, Ordering};
use std::sync::{Arc, RwLock};
use tokio::sync::{mpsc, Semaphore};
use tokio::time::{sleep, timeout, Duration};
use tracing::{debug, error, info, warn};

/// How long the dequeue loop waits for a free concurrency permit before
/// looping back to re-check the worker mode and the shutdown channel.
const PERMIT_WAIT: Duration = Duration::from_millis(100);

/// Maximum number of buffered lifecycle events flushed in one `emit_batch`.
const EVENT_FLUSH_BATCH: usize = 64;

/// How long to wait for buffered lifecycle events to flush at shutdown.
const EVENT_FLUSH_TIMEOUT: Duration = Duration::from_secs(5);

/// How long to let an in-flight control command publish its reply before the
/// control listener is torn down at shutdown.
const CONTROL_REPLY_GRACE: Duration = Duration::from_secs(2);

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

/// Whether `broker`'s blocking [`Broker::dequeue`] is cancel-safe, so the
/// dequeue loop may race it against the shutdown signal.
///
/// This used to be a `TypeId` allowlist naming the one implementation whose
/// `dequeue` had been read and verified. [`Broker`] now carries the answer
/// itself as [`Broker::dequeue_is_cancel_safe`], so this is the one-line
/// delegation that comment predicted — with two things the allowlist could not
/// do: a broker outside this workspace can opt in, and a *decorator* (a wrapper
/// that records delays, injects faults, ...) can forward its inner broker's
/// answer instead of being silently excluded by its own type.
///
/// The trait's default is `false`, which keeps the conservative behaviour for
/// every broker that has not made the promise: a parked `dequeue` observes
/// shutdown when it next returns. [`celers_core::InMemoryBroker`] overrides it
/// to `true` — it has no block timeout at all, so an idle in-process worker
/// would otherwise learn about `control shutdown` only when the next message
/// happened to arrive.
///
/// Whoever builds the worker can still override the answer in either direction
/// with [`Worker::with_cancel_safe_dequeue`].
fn broker_dequeue_is_cancel_safe<B: Broker + ?Sized>(broker: &B) -> bool {
    broker.dequeue_is_cancel_safe()
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
    /// Cooperative cancellation: trips the matching in-flight task's token when
    /// a revocation is published into its channel (enabled via
    /// [`Worker::with_revocation_watcher`]). The channel is fed by the control
    /// protocol, by the broker bridge below, or by the host application.
    pub(crate) revocation_watcher: Option<RevocationWatcher>,
    /// Whether the broker feeds revocations to this worker: a subscription to
    /// [`Broker::subscribe_revocations`] plus a [`Broker::is_revoked`] check
    /// before every dispatch (enabled via
    /// [`Worker::with_broker_revocation`]). `false` (the default) leaves
    /// revocation in-process only.
    pub(crate) broker_revocation: bool,
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
    /// Revocations recorded by the remote control protocol, checked before
    /// every dispatch. Empty (and therefore free) until something revokes.
    pub(crate) revocations: WorkerRevocationManager,
    /// Worker-local per-task rate limits installed by
    /// [`ControlCommand::RateLimit`](celers_core::ControlCommand). Empty (and
    /// therefore a single atomic load) until an operator sets one.
    pub(crate) rate_limits: RuntimeRateLimits,
    /// Drain deadline in seconds, seeded from
    /// [`WorkerConfig::shutdown_timeout_secs`](crate::WorkerConfig) and
    /// replaceable at runtime by `ControlCommand::Shutdown { timeout }`.
    pub(crate) shutdown_timeout_secs: Arc<AtomicU64>,
    /// Remote control channel (enabled via
    /// [`Worker::with_control_transport`]). `None` (the default) leaves the
    /// worker unreachable by remote control.
    pub(crate) control_transport: Option<Arc<dyn ControlTransport>>,
    /// Broker URL reported by `inspect conf` / `inspect stats`, credentials
    /// stripped. The broker itself never exposes its URL, so this is supplied
    /// by whoever built the worker or left unreported.
    pub(crate) broker_url: Option<String>,
    /// Result-backend URL reported by `inspect conf`, credentials stripped.
    pub(crate) result_backend_url: Option<String>,
    /// Result store terminal task dispositions are written to (enabled via
    /// [`Worker::with_result_store`]).
    ///
    /// `None` (the default) means the worker records nothing: a caller holding
    /// an [`AsyncResult`](celers_core::AsyncResult) then polls for a result no
    /// one will ever write. With a store, successes, terminal failures and
    /// deliberately ignored failures all become observable.
    pub(crate) result_store: Option<Arc<dyn celers_core::ResultStore>>,
    /// Whether this worker may race a blocking [`Broker::dequeue`] against its
    /// shutdown signal (see [`Worker::with_cancel_safe_dequeue`]).
    ///
    /// Seeded by [`broker_dequeue_is_cancel_safe`] and overridable by whoever
    /// built the worker. `false` keeps the conservative behaviour: shutdown is
    /// observed at the *next* trip round the loop.
    pub(crate) cancel_safe_dequeue: bool,
    /// Chord barrier store (enabled via [`Worker::with_chord_backend`]).
    ///
    /// Deliberately a second handle rather than a reuse of `result_store`: the
    /// barrier primitives (`chord_complete_task`, `chord_get_state`,
    /// `chord_get_partial_results`) live on
    /// [`celers_backend_redis::ResultBackend`] and take `&mut self`, which
    /// [`celers_core::ResultStore`] neither declares nor could. `None` (the
    /// default) leaves chord callbacks un-triggered — a chord's header runs,
    /// but nothing counts it.
    #[cfg(feature = "workflows")]
    pub(crate) chord_backend:
        Option<Arc<tokio::sync::Mutex<dyn celers_backend_redis::ResultBackend>>>,
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

        let shutdown_timeout_secs = Arc::new(AtomicU64::new(config.shutdown_timeout_secs));

        // Read before `broker` is moved into the struct below.
        let cancel_safe_dequeue = broker_dequeue_is_cancel_safe(broker.as_ref());

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
            broker_revocation: false,
            rate_limit_coordinator: None,
            affinity_registry: None,
            time_limits: None,
            poison_pill: None,
            checkpoints: None,
            health: HealthChecker::new(),
            revocations: WorkerRevocationManager::new(),
            rate_limits: RuntimeRateLimits::new(),
            shutdown_timeout_secs,
            control_transport: None,
            broker_url: None,
            result_backend_url: None,
            result_store: None,
            cancel_safe_dequeue,
            #[cfg(feature = "workflows")]
            chord_backend: None,
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
    /// The supplied [`RevocationWatcher`] subscribes to the worker's in-process
    /// revocation channel — use
    /// [`with_broker_revocation`](Self::with_broker_revocation) to have the
    /// *broker* feed that channel, which is what lets another process revoke a
    /// task running here.
    /// While the worker runs, every in-flight task is registered with the
    /// watcher's registry and executed inside a [`TaskExecutionContext`] carrying
    /// a [`CancellationToken`]. When a
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

    /// Declare whether this worker's broker has a **cancel-safe**
    /// [`Broker::dequeue`], letting the dequeue loop race it against the
    /// shutdown signal.
    ///
    /// A worker parked in a blocking `dequeue` otherwise observes a shutdown
    /// only when that call returns. Brokers with a block timeout (Redis,
    /// PostgreSQL/MySQL, SQS) return within one block period, so the delay is
    /// bounded and small. [`celers_core::InMemoryBroker`] has no timeout at
    /// all: it waits indefinitely on an empty queue, so an idle in-process
    /// worker would act on `control shutdown` only when the next message
    /// happened to arrive. It declares itself cancel-safe through
    /// [`Broker::dequeue_is_cancel_safe`] and needs no call to this method.
    ///
    /// # Safety of the race
    ///
    /// Racing means the `dequeue` future is **dropped** when shutdown wins.
    /// That is only sound if the implementation never holds a message across an
    /// `.await` — otherwise the dropped future takes an already-dequeued
    /// message with it, and the message is lost rather than redelivered.
    /// [`Broker::dequeue_is_cancel_safe`] defaults to `false` for every
    /// broker but the verified in-memory one. Pass `true` only for a broker
    /// whose `dequeue` you have checked.
    ///
    /// Passing `false` is always safe: it restores the "observe shutdown at the
    /// next dequeue return" behaviour, including for the in-memory broker.
    ///
    /// Batch dequeue is unaffected either way: the loop never races
    /// [`Broker::dequeue_batch`], because dropping it would abandon a whole
    /// batch rather than one message. A worker configured for batch dequeue
    /// therefore still observes shutdown when the batch call returns.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_worker::{Worker, WorkerConfig};
    /// use celers_core::TaskRegistry;
    /// # use celers_core::Broker;
    /// # async fn example<B: Broker + 'static>(broker: B) {
    /// let worker = Worker::new(broker, TaskRegistry::new(), WorkerConfig::default())
    ///     // Only for a broker whose `dequeue` holds no message across an await.
    ///     .with_cancel_safe_dequeue(true);
    /// # let _ = worker;
    /// # }
    /// ```
    #[must_use]
    pub fn with_cancel_safe_dequeue(mut self, cancel_safe: bool) -> Self {
        self.cancel_safe_dequeue = cancel_safe;
        self
    }

    /// Whether the dequeue loop will race a blocking dequeue against the
    /// shutdown signal for this worker's broker.
    ///
    /// See [`Worker::with_cancel_safe_dequeue`].
    pub fn cancel_safe_dequeue(&self) -> bool {
        self.cancel_safe_dequeue
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

    /// Give the worker a result store, so task outcomes are actually recorded.
    ///
    /// Without one the worker executes tasks and writes nothing anywhere: a
    /// caller holding an [`AsyncResult`](celers_core::AsyncResult) polls a
    /// backend no one is writing to, and every terminal failure (retries
    /// exhausted, hard time limit, oversized result, open circuit, poison-pill
    /// quarantine) is visible only as a log line and a dead-letter entry.
    ///
    /// With one, the worker records:
    ///
    /// * [`Success`](celers_core::TaskResultValue::Success) with the task's
    ///   return value,
    /// * [`Failure`](celers_core::TaskResultValue::Failure) for every terminal
    ///   failure, and
    /// * [`Ignored`](celers_core::TaskResultValue::Ignored) for a failure the
    ///   task asked to have suppressed.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use std::sync::Arc;
    /// # use celers_core::{InMemoryBroker, InMemoryResultBackend, TaskRegistry};
    /// # use celers_worker::{Worker, WorkerConfig};
    /// let store = Arc::new(InMemoryResultBackend::new());
    /// let worker = Worker::new(
    ///     InMemoryBroker::new(),
    ///     TaskRegistry::new(),
    ///     WorkerConfig::default(),
    /// )
    /// .with_result_store(store);
    /// # let _ = worker;
    /// ```
    #[must_use]
    pub fn with_result_store(mut self, store: Arc<dyn celers_core::ResultStore>) -> Self {
        self.result_store = Some(store);
        self
    }

    /// The result store this worker records outcomes in, if any.
    pub fn result_store(&self) -> Option<&Arc<dyn celers_core::ResultStore>> {
        self.result_store.as_ref()
    }

    /// Give the worker a chord barrier store, so chord callbacks actually fire.
    ///
    /// A chord is defined by its barrier: the callback runs once, after every
    /// header task has completed, with their results. Counting those
    /// completions needs shared state, which is what
    /// [`celers_backend_redis::ResultBackend`] provides. Without this handle a
    /// chord's header runs and its callback never does.
    ///
    /// It is a separate handle from [`with_result_store`](Self::with_result_store)
    /// because the barrier primitives take `&mut self` and are not part of the
    /// backend-agnostic [`celers_core::ResultStore`] trait; the `Mutex` is what
    /// makes the `&mut` available from the worker's concurrent task futures.
    #[cfg(feature = "workflows")]
    #[must_use]
    pub fn with_chord_backend(
        mut self,
        backend: Arc<tokio::sync::Mutex<dyn celers_backend_redis::ResultBackend>>,
    ) -> Self {
        self.chord_backend = Some(backend);
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
        // It reads the worker's revocation channel and trips the matching
        // in-flight task's cancellation token.
        let revocation_handle = self.revocation_watcher.as_ref().map(|w| {
            info!("Starting revocation watcher for cooperative cancellation");
            w.spawn()
        });

        // Start the broker → worker revocation bridge, which is what puts
        // anything on that channel from outside this process.
        let revocation_bridge_handle = self.spawn_broker_revocation_bridge();

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

        // Messages dispatched but not yet disposed of, so a shutdown deadline
        // can hand them back to the broker instead of stranding them — and the
        // one place that knows *which* tasks are running, for `inspect active`.
        //
        // Created per run, not once per worker: `drain_in_flight` empties it at
        // shutdown, and a second `run()` inheriting entries from the first would
        // requeue messages that were already disposed of.
        let in_flight = if self.control_transport.is_some() {
            InFlightRegistry::with_args_capture(self.config.payload_hygiene.clone())
        } else {
            InFlightRegistry::new()
        };

        // Start the remote control subscriber if a control channel is wired up.
        let control = self.control_transport.as_ref().map(|transport| {
            info!("Starting remote control listener for worker {}", hostname);
            let service = ControlService::new(Arc::new(self.control_surface(in_flight.clone())));
            let handle = service.spawn(Arc::clone(transport));
            (service, handle)
        });

        let result = self
            .run_loop_inner(&mut shutdown_rx, &hostname, pid, &events, &in_flight)
            .await;

        // Stop the remote control listener, but let a command that is still
        // being answered finish first. `ControlCommand::Shutdown` is exactly
        // this case: it stops the worker from inside the listener, so aborting
        // the moment the run loop exits would cancel the acknowledgement of the
        // very command that caused the exit, and the operator would see a
        // command that appears never to have arrived.
        if let Some((service, handle)) = control {
            let deadline = tokio::time::Instant::now() + CONTROL_REPLY_GRACE;
            while service.pending() > 0 && tokio::time::Instant::now() < deadline {
                sleep(Duration::from_millis(5)).await;
            }
            if service.pending() > 0 {
                warn!(
                    "Aborting the control listener with {} command(s) still unanswered",
                    service.pending()
                );
            }
            handle.abort();
        }

        // Stop heartbeat task
        if let Some(handle) = heartbeat_handle {
            handle.abort();
        }

        // Stop the revocation watcher
        if let Some(handle) = revocation_handle {
            handle.abort();
        }

        // Stop the broker revocation bridge
        if let Some(handle) = revocation_bridge_handle {
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
        let event_emit_failures = events.emit_failures();
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
        if event_emit_failures > 0 {
            warn!(
                "The event transport rejected {} lifecycle event batch(es); monitors saw an \
                 incomplete event stream for this worker",
                event_emit_failures
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

    /// Inner worker loop (separated to ensure offline event is always emitted)
    async fn run_loop_inner(
        &self,
        shutdown_rx: &mut Option<&mut mpsc::Receiver<()>>,
        hostname: &str,
        pid: u32,
        events: &EventSink,
        in_flight: &InFlightRegistry,
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
                // Single task dequeue (convert to Vec for uniform handling).
                //
                // A blocking `dequeue` is the one place the loop can sit for an
                // unbounded time, so a broker whose `dequeue` is cancel-safe
                // races it against the shutdown signal: without that, an idle
                // in-memory worker (no block timeout at all) acts on
                // `control shutdown` only when the next message arrives. The
                // race drops the losing `dequeue` future, which is exactly why
                // it is gated on `cancel_safe_dequeue` — for a broker that
                // holds a message across an await, dropping it would lose the
                // message. See [`Worker::with_cancel_safe_dequeue`].
                let raced: std::result::Result<
                    Result<Option<celers_core::BrokerMessage>>,
                    StopReason,
                > = match shutdown_rx.as_mut().filter(|_| self.cancel_safe_dequeue) {
                    Some(rx) => {
                        tokio::select! {
                            biased;
                            // Shutdown first: a signal already waiting must
                            // not lose to a message arriving in the same
                            // poll.
                            signal = rx.recv() => Err(match signal {
                                Some(()) => StopReason::Shutdown,
                                None => StopReason::Disconnected,
                            }),
                            dequeued = self.broker.dequeue() => Ok(dequeued),
                        }
                    }
                    None => Ok(self.broker.dequeue().await),
                };

                match raced {
                    // `held_permits` drops with the loop body, returning this
                    // iteration's capacity before the drain waits for it.
                    Err(reason) => {
                        info!("Shutdown observed while waiting for a message");
                        break reason;
                    }
                    Ok(Ok(Some(msg))) => Ok(vec![msg]),
                    Ok(Ok(None)) => Ok(vec![]),
                    Ok(Err(e)) => Err(e),
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
                    // The worker mode can change while the loop is parked in a
                    // blocking `dequeue`, and it routinely does: draining,
                    // maintenance and `ControlCommand::CancelConsumer` all
                    // arrive from another task. Without this re-check a worker
                    // that has been told to stop consuming still runs the next
                    // message to arrive, because the only mode check happens
                    // *before* the dequeue it is parked in.
                    let mode_after_dequeue = WorkerMode::from(self.mode.load(Ordering::SeqCst));
                    if !mode_after_dequeue.should_accept_tasks() {
                        info!(
                            "Worker is {} and will not run the {} message(s) just dequeued; \
                             returning them to the queue",
                            mode_after_dequeue,
                            messages.len()
                        );
                        for msg in messages {
                            // No broker-side delay: it is *this* worker that
                            // stopped consuming, not the task that must wait.
                            // Holding the message back would delay the healthy
                            // worker that should take it.
                            self.defer_message(
                                &msg.task.metadata.id,
                                msg.receipt_handle.as_deref(),
                                Duration::ZERO,
                                "worker not accepting tasks",
                            )
                            .await;
                        }
                        // Back to the top: draining breaks the loop, maintenance
                        // sleeps a poll interval, so this cannot spin.
                        continue;
                    }

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

                        // Message authentication comes before *everything*
                        // else, admission checks included. The checks below
                        // write worker-local state keyed on the message's own
                        // task id and name — the revocation registry
                        // (`revocations.revoke`) and the poison-pill strike
                        // table (`record_redelivery`) — so letting an
                        // unauthenticated message reach them would let an
                        // attacker seed both. A message that fails is rejected
                        // here and never dispatched.
                        //
                        // No-op (a single `Option` check) unless
                        // `WorkerConfig::signature_verification` is set.
                        if let Some(ref verification) = self.config.signature_verification {
                            if let Err(e) = verification.verify(&msg.task) {
                                self.stats.task_signature_rejected();
                                execution::reject_unverified(
                                    &self.broker,
                                    self.dlq_handler.as_ref(),
                                    events,
                                    hostname,
                                    pid,
                                    UnverifiedMessage {
                                        task: &msg.task,
                                        receipt_handle: msg.receipt_handle.as_deref(),
                                        error: &e,
                                    },
                                )
                                .await;
                                continue;
                            }
                        }

                        // Revocation comes next: a task an operator has
                        // revoked must not run, must not be deferred back into
                        // the queue, and must not be dead-lettered — it is
                        // simply dropped. This is what makes
                        // `ControlCommand::Revoke` / `RevokeByPattern` real for
                        // a message that was already queued when the revocation
                        // arrived.
                        //
                        // Two sources are consulted. The worker-local record is
                        // free and covers everything this worker was told about
                        // (control commands, and the broker bridge while it was
                        // subscribed). The broker's *persisted* revoked set is
                        // asked only when broker revocation is enabled, and is
                        // what catches a revocation published while this worker
                        // was restarting — Pub/Sub has no redelivery.
                        let revocation = self.revocations.check_revocation(task_id, &task_name);
                        let broker_revoked =
                            !revocation.revoked && self.is_revoked_in_broker(task_id).await;
                        if revocation.revoked || broker_revoked {
                            if broker_revoked {
                                // Remember it, so a redelivery of the same
                                // message costs no further round trip.
                                self.revocations
                                    .revoke(task_id, celers_core::RevocationMode::Ignore);
                            }
                            info!(
                                "Task {} ('{}') is revoked ({}); dropping it",
                                task_id,
                                task_name,
                                if broker_revoked {
                                    "listed in the broker's revoked set".to_string()
                                } else {
                                    format!("recorded by this worker, mode {:?}", revocation.mode)
                                }
                            );

                            events.emit(Event::Task(TaskEvent::Revoked {
                                task_id,
                                task_name: Some(task_name.clone()),
                                hostname: hostname.to_string(),
                                timestamp: chrono::Utc::now(),
                                terminated: false,
                                signum: None,
                                expired: false,
                            }));

                            self.stats.task_revoked();

                            // Acknowledge so the broker removes it: a revoked
                            // task that is requeued would come straight back.
                            if let Err(e) = self
                                .broker
                                .ack(&task_id, msg.receipt_handle.as_deref())
                                .await
                            {
                                error!("Failed to acknowledge revoked task {}: {}", task_id, e);
                            }
                            continue;
                        }

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
                                    // Terminal for the caller, so the task's
                                    // own error handlers are exactly what
                                    // should run.
                                    error_links: true,
                                    signature: self.config.signature_verification.as_ref(),
                                    result_store: self.result_store.as_ref(),
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

                            // Defer (another worker might handle it).
                            //
                            // The message is held back for no time at all: the
                            // mismatch is this worker's, and a worker that
                            // *can* route the task must be able to take it at
                            // once. `defer_delay` below is a different thing —
                            // the poll back-off that keeps *this* worker from
                            // spinning on work it cannot serve, which is what
                            // `WorkerConfig::defer_delay_ms` documents itself
                            // as.
                            self.defer_message(
                                &task_id,
                                msg.receipt_handle.as_deref(),
                                Duration::ZERO,
                                "routing",
                            )
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

                            // Defer so a worker with matching labels can pick
                            // the task up — immediately, hence no broker-side
                            // delay; see the routing branch above.
                            self.defer_message(
                                &task_id,
                                msg.receipt_handle.as_deref(),
                                Duration::ZERO,
                                "affinity",
                            )
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
                                Duration::ZERO,
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
                                        Duration::ZERO,
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
                                        error_links: true,
                                        signature: self.config.signature_verification.as_ref(),
                                        result_store: self.result_store.as_ref(),
                                    },
                                )
                                .await;
                                continue;
                            }
                        }

                        // Worker-local rate-limit gate, set at runtime by
                        // `ControlCommand::RateLimit`. Checked before the
                        // cluster-wide gate because it needs no network round
                        // trip, and it is a single atomic load until an operator
                        // installs a limit.
                        if let RuntimeRateLimitDecision::Denied { retry_after } =
                            self.rate_limits.check(&task_name)
                        {
                            debug!(
                                "Worker rate limit denied task {} ('{}'); deferring for {:?}",
                                task_id, task_name, retry_after
                            );
                            self.stats.task_rate_limited();

                            // The circuit breaker already admitted this task
                            // (and, while half-open, handed it a trial slot);
                            // give the slot back since it is not going to run.
                            if let Some(ref cb) = self.circuit_breaker {
                                cb.release_probe(&task_name).await;
                            }

                            // This limiter is per worker, so another worker
                            // may well have budget: the message goes back with
                            // no broker-side hold, and only *this* worker waits
                            // out `retry_after`.
                            self.defer_message(
                                &task_id,
                                msg.receipt_handle.as_deref(),
                                Duration::ZERO,
                                "worker rate limit",
                            )
                            .await;
                            defer_delay = Some(
                                self.clamped_defer_delay(retry_after)
                                    .max(defer_delay.unwrap_or(Duration::ZERO)),
                            );
                            continue;
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

                                    // The one deferral that hands the broker a
                                    // real delay. This budget is shared by the
                                    // whole cluster, so the limiter's
                                    // `retry_after` is a statement about *every*
                                    // worker: handing the message straight back
                                    // would only move the denial to the next
                                    // consumer. A broker with a delayed queue
                                    // holds it until due; one without falls back
                                    // to an immediate requeue, which is the old
                                    // behaviour. The same clamped value is this
                                    // worker's poll back-off, so the two wait in
                                    // step.
                                    let delay = self.clamped_defer_delay(retry_after);
                                    self.defer_message(
                                        &task_id,
                                        msg.receipt_handle.as_deref(),
                                        delay,
                                        "rate limit",
                                    )
                                    .await;
                                    defer_delay =
                                        Some(delay.max(defer_delay.unwrap_or(Duration::ZERO)));
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

                        // Redacted rendering of the arguments, for whoever reads
                        // the logs. Built from a *copy* of the payload and only
                        // when hygiene is configured; `debug!` evaluates its
                        // arguments only when the level is enabled, so a worker
                        // running at `info` pays nothing for it.
                        if let Some(ref hygiene) = self.config.payload_hygiene {
                            if hygiene.is_enabled() {
                                debug!(
                                    "Task {} ('{}') args (redacted): {}",
                                    task_id,
                                    task_name,
                                    support::args_preview(&msg.task.payload, Some(hygiene))
                                );
                            }
                        }

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
                            signature: self.config.signature_verification.clone(),
                            result_store: self.result_store.clone(),
                            #[cfg(feature = "workflows")]
                            chord_backend: self.chord_backend.clone(),
                        };

                        in_flight.register(task_id, receipt_handle, &dispatch.task);

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
        self.drain_in_flight(&permits, in_flight, concurrency).await;
        info!("Worker stopped ({})", stop_reason.as_str());

        Ok(())
    }
}
