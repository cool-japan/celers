//! Worker-side handler for the remote control protocol.
//!
//! [`celers_core::control`] defines the message vocabulary and
//! [`celers_core::control_transport`] the wire framing; this module is the half
//! that actually *does* something: it subscribes to a broadcast control
//! channel, deserialises each [`ControlCommand`], dispatches it to the running
//! worker's facilities, and publishes a [`ControlResponse`] back to the
//! envelope's reply address.
//!
//! A worker gets one by calling
//! [`Worker::with_control_transport`](crate::Worker::with_control_transport);
//! the run loop then starts the subscriber alongside the revocation watcher and
//! stops it on shutdown. [`ControlService`] can also be driven directly (see
//! [`ControlService::handle`]) when a host application wants to expose the same
//! operations over its own channel.
//!
//! # What each command actually does
//!
//! | Command | Effect on the running worker |
//! |---|---|
//! | `Ping` | `Pong` with hostname and clock |
//! | `Inspect(Active)` | The worker's in-flight registry (id, name, args preview, start time) |
//! | `Inspect(Scheduled)` / `Inspect(Reserved)` | Always empty — see "Always-empty inspections" |
//! | `Inspect(Revoked)` | Ids held by the worker's [`WorkerRevocationManager`] |
//! | `Inspect(Registered)` | [`TaskRegistry::list_tasks`], sorted |
//! | `Inspect(Stats)` / `Inspect(Report)` / `Inspect(Conf)` | Live counters, pool and configuration |
//! | `Inspect(QueueInfo)` | `queue_size()` of the worker's own queue |
//! | `Inspect(CircuitBreakers)` | Per-task-name breaker state |
//! | `Shutdown` | Switches the worker to draining and, with an explicit `timeout`, replaces the drain deadline |
//! | `Revoke` / `BulkRevoke` | Records the revocation, asks the broker to drop queued copies, and (with `terminate`) trips the running task's cancellation token |
//! | `RevokeByPattern` | Records a glob revocation checked before every dispatch (worker-local: a broker cannot match task *names*) |
//! | `RateLimit` | Installs/removes a per-task-name token bucket consulted before every dispatch |
//! | `TimeLimit` | Sets the soft/hard limits [`Worker`](crate::Worker) resolves per task |
//! | `AddConsumer` / `CancelConsumer` | Resumes / suspends consumption of the worker's queue |
//! | `Queue(Length)` | `queue_size()` of the worker's own queue |
//! | `Queue(Purge/Delete/Bind/Unbind/Declare)` | **Unsupported** — see "Unsupported commands" |
//! | `ResetCircuitBreaker` | Closes one or every task-type breaker |
//!
//! # Always-empty inspections
//!
//! `Inspect(Scheduled)` and `Inspect(Reserved)` always answer with an empty
//! list, and that answer is accurate rather than a stub: a `CeleRS` worker
//! keeps **no** worker-local scheduled or reserved set. ETA/countdown tasks are
//! held by the broker's delayed queue until they are due, and a batch dequeue
//! dispatches every message it pulls instead of parking it in a prefetch
//! reserve. There is therefore never a task in either state to report.
//!
//! # Unsupported commands
//!
//! [`Broker`] has no purge, delete, bind, unbind or declare operation, so a
//! worker genuinely cannot perform those on the operator's behalf; those
//! sub-commands answer with [`ControlResponse::Error`] naming the reason rather
//! than a fake success. Use the broker-specific tooling (`celers queue purge`,
//! for example) for those.
//!
//! # When a mode change takes effect
//!
//! `Shutdown` and `CancelConsumer` set the worker's mode from *outside* the run
//! loop, and the run loop reads that mode between dequeues. A worker parked
//! inside a broker's blocking [`Broker::dequeue`] therefore acts on the change
//! when that call returns:
//!
//! - Brokers whose dequeue has a block timeout (Redis, Postgres/MySQL, SQS)
//!   return within it, so the change lands within one block period.
//! - [`InMemoryBroker`](celers_core::InMemoryBroker)'s dequeue waits
//!   indefinitely on an empty queue, so an *idle* in-process worker acts on the
//!   change when the next message arrives — which it then hands straight back
//!   to the queue instead of running.
//!
//! Either way the command is acknowledged immediately and no message is lost:
//! the run loop re-checks the mode after every dequeue and returns anything it
//! must not run.
//!
//! # Not Celery's pidbox
//!
//! The channel, framing and command names are `CeleRS`-native and do not
//! interoperate with `celery -A app control ...`. See
//! [`celers_core::control_transport`] for the wire description.

use crate::circuit_breaker::CircuitBreaker;
use crate::execution_context::{RevocationSignal, RevocationWatcher};
use crate::health::HealthChecker;
use crate::types::{DynamicConfig, WorkerMode, WorkerStats};
use crate::worker_core::support::InFlightRegistry;

use celers_core::control::{
    ActiveTaskInfo, BrokerStats, ControlCommand, ControlResponse, InspectCommand, InspectResponse,
    PoolStats, QueueCommand, QueueResponse, QueueStats, WorkerConf, WorkerReport,
};
use celers_core::control_transport::{ControlReply, ControlTransport};
use celers_core::rate_limit::{RateLimitConfig, WorkerRateLimiter};
use celers_core::revocation::{RevocationMode, RevocationRequest, WorkerRevocationManager};
use celers_core::time_limit::{TimeLimitConfig, WorkerTimeLimits};
use celers_core::{Broker, TaskId, TaskRegistry};

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, AtomicU8, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// How long the subscriber waits after a transport error before re-reading, so
/// a permanently broken subscription cannot spin the runtime at 100% CPU.
const SUBSCRIBER_ERROR_BACKOFF: Duration = Duration::from_millis(200);

/// Software identity reported by `inspect report`.
const SOFTWARE_SYSTEM: &str = "celers";

// ---------------------------------------------------------------------------
// Runtime rate limits
// ---------------------------------------------------------------------------

/// The verdict of a runtime rate-limit check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeRateLimitDecision {
    /// No limit applies, or a permit was available.
    Allowed,
    /// The task is over its limit; retry after this long.
    Denied {
        /// How long until a permit is expected to be available.
        retry_after: Duration,
    },
}

/// Per-task-name rate limits installed at runtime by
/// [`ControlCommand::RateLimit`].
///
/// This is the *worker-local* limiter (Celery's `worker rate_limit`), distinct
/// from [`WorkerRateLimitCoordinator`](crate::WorkerRateLimitCoordinator),
/// which enforces a cluster-wide budget through a shared backend and is
/// configured at construction. Both gates are consulted; this one first,
/// because it needs no network round trip.
///
/// Until an operator sets a limit the check is a single relaxed atomic load, so
/// a worker that never uses remote control pays nothing for carrying one.
#[derive(Debug, Clone, Default)]
pub struct RuntimeRateLimits {
    inner: Arc<RuntimeRateLimitsInner>,
}

/// Shared state behind every clone of a [`RuntimeRateLimits`].
#[derive(Debug, Default)]
struct RuntimeRateLimitsInner {
    limiter: WorkerRateLimiter,
    /// Configured rates by task name, kept for reporting.
    rates: RwLock<BTreeMap<String, f64>>,
    /// `rates.len()`, readable without taking the lock.
    configured: AtomicUsize,
}

impl RuntimeRateLimits {
    /// Create an empty set of limits (every task unlimited).
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Install (or replace) the limit for `task_name`, in tasks per second.
    ///
    /// A rate that is not finite and positive is rejected: a zero or negative
    /// rate would stall the task type forever with no way to tell that apart
    /// from a wedged worker. Use [`clear`](Self::clear) to remove a limit.
    pub fn set(&self, task_name: &str, rate_per_second: f64) -> Result<(), String> {
        if !rate_per_second.is_finite() || rate_per_second <= 0.0 {
            return Err(format!(
                "rate must be a finite positive number of tasks per second, got {rate_per_second}"
            ));
        }
        self.inner
            .limiter
            .set_task_rate(task_name, RateLimitConfig::new(rate_per_second));
        let mut rates = self.write_rates();
        rates.insert(task_name.to_string(), rate_per_second);
        self.inner.configured.store(rates.len(), Ordering::Relaxed);
        Ok(())
    }

    /// Remove the limit for `task_name`. Returns whether one was in force.
    pub fn clear(&self, task_name: &str) -> bool {
        self.inner.limiter.remove_task_rate(task_name);
        let mut rates = self.write_rates();
        let removed = rates.remove(task_name).is_some();
        self.inner.configured.store(rates.len(), Ordering::Relaxed);
        removed
    }

    /// Every configured limit, by task name.
    #[must_use]
    pub fn rates(&self) -> BTreeMap<String, f64> {
        self.read_rates().clone()
    }

    /// Whether any limit is in force.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.configured.load(Ordering::Relaxed) == 0
    }

    /// Consume a permit for `task_name` if one is required and available.
    #[must_use]
    pub fn check(&self, task_name: &str) -> RuntimeRateLimitDecision {
        // Fast path: nothing configured, so no lock and no bucket maths.
        if self.is_empty() {
            return RuntimeRateLimitDecision::Allowed;
        }
        if self.inner.limiter.try_acquire(task_name) {
            RuntimeRateLimitDecision::Allowed
        } else {
            RuntimeRateLimitDecision::Denied {
                retry_after: self.inner.limiter.time_until_available(task_name),
            }
        }
    }

    /// Read the rate table, recovering from a poisoned lock.
    ///
    /// The table is a plain map: an unrelated panic cannot leave it
    /// inconsistent, and refusing to read it would make the control plane
    /// report "no limits" while limits are still being enforced.
    fn read_rates(&self) -> std::sync::RwLockReadGuard<'_, BTreeMap<String, f64>> {
        self.inner
            .rates
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Write to the rate table, recovering from a poisoned lock.
    fn write_rates(&self) -> std::sync::RwLockWriteGuard<'_, BTreeMap<String, f64>> {
        self.inner
            .rates
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

// ---------------------------------------------------------------------------
// Control surface
// ---------------------------------------------------------------------------

/// Handles onto the parts of a running worker the control protocol can reach.
///
/// Built by [`Worker`](crate::Worker) when its run loop starts; every field is
/// a shared handle, so a command dispatched here changes the behaviour of the
/// worker that is running *right now*.
pub(crate) struct ControlSurface {
    pub(crate) hostname: String,
    pub(crate) pid: u32,
    pub(crate) queue_name: String,
    pub(crate) broker: Arc<dyn Broker>,
    pub(crate) registry: Arc<TaskRegistry>,
    pub(crate) in_flight: InFlightRegistry,
    pub(crate) stats: Arc<WorkerStats>,
    pub(crate) health: HealthChecker,
    pub(crate) mode: Arc<AtomicU8>,
    pub(crate) shutdown_tx: Option<mpsc::Sender<()>>,
    pub(crate) shutdown_timeout_secs: Arc<AtomicU64>,
    pub(crate) dynamic_config: Arc<RwLock<DynamicConfig>>,
    pub(crate) revocations: WorkerRevocationManager,
    pub(crate) revocation_watcher: Option<RevocationWatcher>,
    pub(crate) rate_limits: RuntimeRateLimits,
    pub(crate) time_limits: Option<WorkerTimeLimits>,
    pub(crate) circuit_breaker: Option<Arc<CircuitBreaker>>,
    pub(crate) concurrency: u32,
    pub(crate) prefetch_multiplier: u32,
    pub(crate) acks_late: bool,
    pub(crate) requeue_on_worker_lost: bool,
    pub(crate) broker_url: Option<String>,
    pub(crate) result_backend: Option<String>,
}

/// Dispatches control commands into a running worker.
///
/// Cheap to clone: every clone shares one surface, and therefore one worker.
#[derive(Clone)]
pub struct ControlService {
    surface: Arc<ControlSurface>,
    /// Commands currently being dispatched or replied to.
    ///
    /// A command can stop the worker that is executing it (`Shutdown`,
    /// `CancelConsumer`), and the run loop tears the listener down as soon as
    /// it exits. Without this counter that teardown races the reply publish and
    /// the operator's `shutdown` is never acknowledged — it looks exactly like
    /// a command that never arrived.
    pending: Arc<AtomicUsize>,
}

impl ControlService {
    /// Wrap a control surface.
    pub(crate) fn new(surface: Arc<ControlSurface>) -> Self {
        Self {
            surface,
            pending: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// The hostname this service answers as.
    #[must_use]
    pub fn hostname(&self) -> &str {
        &self.surface.hostname
    }

    /// How many commands are still being dispatched or replied to.
    ///
    /// The run loop waits (briefly) for this to reach zero before aborting the
    /// listener, so a command that stops the worker still gets answered.
    #[must_use]
    pub fn pending(&self) -> usize {
        self.pending.load(Ordering::SeqCst)
    }

    /// Subscribe to `transport` and serve control commands until the channel
    /// closes or the returned handle is aborted.
    ///
    /// Commands whose envelope names a different worker are ignored, and a
    /// command with no `reply_to` is executed without answering
    /// (fire-and-forget), matching Celery's `reply=False`.
    pub fn spawn(&self, transport: Arc<dyn ControlTransport>) -> JoinHandle<()> {
        let service = self.clone();
        tokio::spawn(async move { service.serve(transport).await })
    }

    /// Subscribe to `transport` and serve control commands (the body of
    /// [`spawn`](Self::spawn); runs until the channel closes).
    pub async fn serve(&self, transport: Arc<dyn ControlTransport>) {
        let hostname = self.surface.hostname.clone();
        let mut stream = match transport.subscribe_commands().await {
            Ok(stream) => stream,
            Err(e) => {
                error!("Worker {hostname} could not subscribe to the control channel: {e}");
                return;
            }
        };
        info!("Worker {hostname} listening for remote control commands");

        loop {
            match stream.recv().await {
                Ok(Some(envelope)) => {
                    if !envelope.targets(&hostname) {
                        debug!(
                            "Ignoring control command {} addressed to {:?}",
                            envelope.id, envelope.destination
                        );
                        continue;
                    }
                    // Counted across dispatch *and* reply: `Shutdown` takes
                    // effect during `handle`, so the reply must be protected
                    // from the teardown it just triggered.
                    self.pending.fetch_add(1, Ordering::SeqCst);
                    let response = self.handle(&envelope.command).await;
                    if let Some(ref reply_to) = envelope.reply_to {
                        let reply = ControlReply::new(envelope.id, &hostname, response);
                        if let Err(e) = transport.send_reply(reply_to, &reply).await {
                            warn!("Failed to publish control reply to {reply_to}: {e}");
                        }
                    }
                    self.pending.fetch_sub(1, Ordering::SeqCst);
                }
                Ok(None) => {
                    info!("Control channel closed for worker {hostname}");
                    break;
                }
                Err(e) => {
                    // A lagging subscriber or a transient transport fault: log
                    // and keep serving, but back off so a permanently broken
                    // subscription cannot spin.
                    warn!("Control subscription error for worker {hostname}: {e}");
                    tokio::time::sleep(SUBSCRIBER_ERROR_BACKOFF).await;
                }
            }
        }
    }

    /// Execute one command against the running worker.
    ///
    /// Never fails: an operation the worker cannot perform is reported as
    /// [`ControlResponse::Error`] so the operator gets a reason rather than a
    /// dropped request.
    pub async fn handle(&self, command: &ControlCommand) -> ControlResponse {
        let s = &self.surface;
        match *command {
            ControlCommand::Ping { .. } => ControlResponse::pong(&s.hostname),
            ControlCommand::Inspect(ref inspect) => self.handle_inspect(inspect).await,
            ControlCommand::Shutdown { timeout } => self.handle_shutdown(timeout),
            ControlCommand::Revoke {
                task_id,
                terminate,
                ref signal,
            } => self.handle_revoke(task_id, terminate, signal.clone()).await,
            ControlCommand::BulkRevoke {
                ref task_ids,
                terminate,
            } => self.handle_bulk_revoke(task_ids, terminate).await,
            ControlCommand::RevokeByPattern {
                ref pattern,
                terminate,
            } => self.handle_revoke_by_pattern(pattern, terminate),
            ControlCommand::RateLimit {
                ref task_name,
                rate,
            } => self.handle_rate_limit(task_name, rate),
            ControlCommand::TimeLimit {
                ref task_name,
                soft,
                hard,
            } => self.handle_time_limit(task_name, soft, hard),
            ControlCommand::AddConsumer { ref queue } => self.handle_add_consumer(queue),
            ControlCommand::CancelConsumer { ref queue } => self.handle_cancel_consumer(queue),
            ControlCommand::Queue(ref queue_command) => self.handle_queue(queue_command).await,
            ControlCommand::ResetCircuitBreaker { ref task_name } => {
                self.handle_reset_circuit_breaker(task_name.as_deref())
                    .await
            }
        }
    }

    // -- inspection ---------------------------------------------------------

    async fn handle_inspect(&self, command: &InspectCommand) -> ControlResponse {
        match *command {
            InspectCommand::Active => {
                ControlResponse::inspect(InspectResponse::Active(self.active_tasks()))
            }
            // Accurate, not a stub: a CeleRS worker holds no worker-local
            // scheduled or reserved set (see the module docs).
            InspectCommand::Scheduled => {
                ControlResponse::inspect(InspectResponse::Scheduled(Vec::new()))
            }
            InspectCommand::Reserved => {
                ControlResponse::inspect(InspectResponse::Reserved(Vec::new()))
            }
            InspectCommand::Revoked => ControlResponse::inspect(InspectResponse::Revoked(
                self.surface.revocations.revoked_ids(),
            )),
            InspectCommand::Registered => {
                ControlResponse::inspect(InspectResponse::Registered(self.registered_tasks().await))
            }
            InspectCommand::Stats => {
                ControlResponse::inspect(InspectResponse::Stats(self.worker_stats()))
            }
            InspectCommand::QueueInfo => match self.queue_stats().await {
                Ok(stats) => ControlResponse::inspect(InspectResponse::QueueInfo(stats)),
                Err(e) => ControlResponse::error(e),
            },
            InspectCommand::Report => {
                ControlResponse::inspect(InspectResponse::Report(self.worker_report().await))
            }
            InspectCommand::Conf => {
                ControlResponse::inspect(InspectResponse::Conf(self.worker_conf()))
            }
            InspectCommand::CircuitBreakers => ControlResponse::inspect(
                InspectResponse::CircuitBreakers(self.circuit_breaker_states().await),
            ),
        }
    }

    /// Every task this worker has dispatched and not yet disposed of.
    fn active_tasks(&self) -> Vec<ActiveTaskInfo> {
        let s = &self.surface;
        let mut active: Vec<ActiveTaskInfo> = s
            .in_flight
            .snapshot()
            .into_iter()
            .map(|(task_id, entry)| ActiveTaskInfo {
                id: task_id,
                name: entry.name,
                // CeleRS tasks take a single serialized input rather than
                // Celery's (args, kwargs) pair, so the whole payload preview is
                // reported as `args` and `kwargs` is always empty.
                args: entry.args_preview.unwrap_or_default(),
                kwargs: "{}".to_string(),
                started: entry.started,
                hostname: s.hostname.clone(),
                worker_pid: Some(s.pid),
                delivery_info: Some(celers_core::control::DeliveryInfo {
                    queue: s.queue_name.clone(),
                    routing_key: None,
                    exchange: None,
                    delivery_tag: None,
                    redelivered: false,
                }),
            })
            .collect();
        // Stable output so two inspections of an unchanged worker match.
        active.sort_by(|a, b| {
            a.started
                .partial_cmp(&b.started)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.id.cmp(&b.id))
        });
        active
    }

    /// Registered task names, sorted.
    async fn registered_tasks(&self) -> Vec<String> {
        let mut names = self.surface.registry.list_tasks().await;
        names.sort_unstable();
        names
    }

    /// Live counters in the protocol's shape.
    fn worker_stats(&self) -> celers_core::control::WorkerStats {
        let s = &self.surface;
        let health = s.health.get_health();
        let active = u32::try_from(s.stats.active()).unwrap_or(u32::MAX);
        celers_core::control::WorkerStats {
            total_tasks: s.stats.processed(),
            active_tasks: active,
            succeeded: health.tasks_processed,
            failed: health.tasks_failed,
            retried: s.stats.retried(),
            uptime: health.uptime_seconds as f64,
            loadavg: crate::sysinfo::read_load_average(),
            memory_usage: u64::try_from(crate::sysinfo::read_process_memory_bytes())
                .ok()
                .filter(|bytes| *bytes > 0),
            pool: Some(PoolStats {
                // CeleRS runs tasks as tokio tasks in one process; there is no
                // prefork pool, so there are no child pids to report.
                pool_type: "async".to_string(),
                max_concurrency: s.concurrency,
                pool_size: s.concurrency,
                available: s.concurrency.saturating_sub(active),
                processes: vec![s.pid],
            }),
            broker: s.broker_url.as_deref().map(|url| BrokerStats {
                url: sanitize_url(url),
                connected: true,
                heartbeat: None,
                transport: transport_scheme(url),
            }),
            // No clock synchronisation protocol is implemented, so reporting an
            // offset would be inventing one.
            clock: None,
        }
    }

    /// Queue depth of the one queue this worker consumes.
    async fn queue_stats(&self) -> Result<HashMap<String, QueueStats>, String> {
        let s = &self.surface;
        let size = s
            .broker
            .queue_size()
            .await
            .map_err(|e| format!("failed to read queue size for '{}': {e}", s.queue_name))?;
        let mut stats = HashMap::new();
        stats.insert(
            s.queue_name.clone(),
            QueueStats {
                name: s.queue_name.clone(),
                messages: size as u64,
                // A worker knows it is itself consuming; it has no view of its
                // peers, so it reports only itself.
                consumers: 1,
            },
        );
        Ok(stats)
    }

    /// The comprehensive status report.
    async fn worker_report(&self) -> WorkerReport {
        WorkerReport {
            hostname: self.surface.hostname.clone(),
            sw_ver: env!("CARGO_PKG_VERSION").to_string(),
            sw_sys: SOFTWARE_SYSTEM.to_string(),
            stats: self.worker_stats(),
            active: self.active_tasks(),
            scheduled: Vec::new(),
            reserved: Vec::new(),
            registered: self.registered_tasks().await,
        }
    }

    /// The worker's effective configuration.
    fn worker_conf(&self) -> WorkerConf {
        let s = &self.surface;
        let default_timeout = s
            .dynamic_config
            .read()
            .map(|c| c.default_timeout_secs)
            .unwrap_or_default();
        WorkerConf {
            broker_url: s.broker_url.as_deref().map_or_else(
                // The broker URL never reaches `WorkerConfig`; it is known only
                // to whoever constructed the broker. Say so rather than
                // printing an empty string that reads like "no broker".
                || "<not reported by this worker>".to_string(),
                sanitize_url,
            ),
            result_backend: s.result_backend.as_deref().map(sanitize_url),
            default_queue: s.queue_name.clone(),
            prefetch_multiplier: s.prefetch_multiplier,
            concurrency: s.concurrency,
            // Celery's `task_soft_time_limit` / `task_time_limit` are global
            // defaults; CeleRS resolves limits per task name through
            // `WorkerTimeLimits`, so only the execution-timeout default is a
            // worker-wide number.
            task_soft_time_limit: None,
            task_time_limit: Some(default_timeout),
            task_acks_late: s.acks_late,
            task_reject_on_worker_lost: s.requeue_on_worker_lost,
            hostname: s.hostname.clone(),
        }
    }

    /// Circuit-breaker state per task name.
    async fn circuit_breaker_states(&self) -> HashMap<String, String> {
        match self.surface.circuit_breaker {
            Some(ref breaker) => breaker
                .get_all_states()
                .await
                .into_iter()
                .map(|(name, state)| (name, state.to_string()))
                .collect(),
            None => HashMap::new(),
        }
    }

    // -- control ------------------------------------------------------------

    /// Switch the worker to draining, optionally replacing the drain deadline.
    fn handle_shutdown(&self, timeout: Option<u64>) -> ControlResponse {
        let s = &self.surface;
        if let Some(secs) = timeout {
            s.shutdown_timeout_secs.store(secs, Ordering::SeqCst);
        }
        s.mode.store(WorkerMode::Draining as u8, Ordering::SeqCst);
        // The run loop breaks on draining mode by itself; the shutdown channel
        // is nudged as well so a loop parked in a poll sleep wakes immediately
        // instead of after a full poll interval.
        if let Some(ref tx) = s.shutdown_tx {
            if let Err(e) = tx.try_send(()) {
                debug!("Shutdown channel not nudged ({e}); draining mode still set");
            }
        }
        info!(
            "Worker {} entering shutdown by remote control (drain deadline {}s)",
            s.hostname,
            s.shutdown_timeout_secs.load(Ordering::SeqCst)
        );
        ControlResponse::ack(
            true,
            Some(format!(
                "worker {} draining; in-flight tasks have {}s to finish",
                s.hostname,
                s.shutdown_timeout_secs.load(Ordering::SeqCst)
            )),
        )
    }

    /// Record a revocation, drop queued copies, and trip a running task.
    ///
    /// The [`Broker::revoke`] call is what makes a revocation stick for a task
    /// that is still *queued* — the worker-local record only helps once this
    /// worker dequeues it. Every worker that receives the broadcast makes that
    /// call, so a fleet of N workers issues N idempotent revocations; that
    /// redundancy is deliberate, because a control broadcast has no leader and
    /// no worker can know whether another one is listening.
    ///
    /// `terminate` is passed to the broker rather than applied only locally, so
    /// a worker that is *not* on this control channel — one wired up with
    /// `Worker::with_broker_revocation` alone — still learns whether to abort
    /// the task it is running.
    async fn handle_revoke(
        &self,
        task_id: TaskId,
        terminate: bool,
        signal: Option<String>,
    ) -> ControlResponse {
        let s = &self.surface;
        let mode = revocation_mode(terminate);
        let mut request = RevocationRequest::new(task_id, mode);
        if let Some(signal) = signal {
            request = request.with_signal(signal);
        }
        s.revocations.revoke_with_request(request);

        let dropped_from_queue = self.revoke_in_broker(task_id, terminate).await;
        let terminated = self.terminate_running(task_id, terminate).await;

        ControlResponse::ack(
            true,
            Some(format!(
                "revoked {task_id} (mode {mode:?}); broker copy: {dropped_from_queue}; \
                 running task terminated: {terminated}"
            )),
        )
    }

    /// Revoke a batch of task ids in one command.
    async fn handle_bulk_revoke(&self, task_ids: &[TaskId], terminate: bool) -> ControlResponse {
        let s = &self.surface;
        let mode = revocation_mode(terminate);
        s.revocations.bulk_revoke(task_ids, mode);

        let mut terminated = 0usize;
        for task_id in task_ids {
            let _ = self.revoke_in_broker(*task_id, terminate).await;
            if self.terminate_running(*task_id, terminate).await {
                terminated += 1;
            }
        }

        ControlResponse::ack(
            true,
            Some(format!(
                "revoked {} task(s) (mode {mode:?}); {terminated} were running and were terminated",
                task_ids.len()
            )),
        )
    }

    /// Revoke every task whose *name* matches a glob.
    ///
    /// Worker-local by construction: a broker queue is keyed by task id, so
    /// nothing broker-side can match a name pattern. The pattern is checked
    /// before every dispatch, so it also catches tasks enqueued later.
    fn handle_revoke_by_pattern(&self, pattern: &str, terminate: bool) -> ControlResponse {
        let s = &self.surface;
        let mode = revocation_mode(terminate);
        s.revocations.revoke_by_pattern(pattern, mode);
        ControlResponse::ack(
            true,
            Some(format!(
                "task names matching '{pattern}' will be refused (mode {mode:?}); \
                 already-queued messages stay in the queue until this worker sees them"
            )),
        )
    }

    /// Ask the broker to record the revocation and drop queued copies.
    async fn revoke_in_broker(&self, task_id: TaskId, terminate: bool) -> String {
        match self.surface.broker.revoke(&task_id, terminate).await {
            Ok(true) => "recorded".to_string(),
            Ok(false) => "not queued".to_string(),
            Err(e) => {
                warn!("Broker refused to cancel {task_id}: {e}");
                format!("broker error: {e}")
            }
        }
    }

    /// Trip the cancellation token of a running task, if there is one.
    async fn terminate_running(&self, task_id: TaskId, terminate: bool) -> bool {
        if !terminate {
            return false;
        }
        match self.surface.revocation_watcher {
            Some(ref watcher) => watcher.apply(&RevocationSignal::terminate(task_id)).await,
            None => false,
        }
    }

    /// Install or remove a worker-local rate limit.
    fn handle_rate_limit(&self, task_name: &str, rate: Option<f64>) -> ControlResponse {
        let limits = &self.surface.rate_limits;
        match rate {
            Some(rate) => match limits.set(task_name, rate) {
                Ok(()) => ControlResponse::ack(
                    true,
                    Some(format!("rate limit for '{task_name}' set to {rate}/s")),
                ),
                Err(reason) => ControlResponse::error(reason),
            },
            None => {
                let removed = limits.clear(task_name);
                ControlResponse::ack(
                    true,
                    Some(if removed {
                        format!("rate limit for '{task_name}' removed")
                    } else {
                        format!("'{task_name}' had no rate limit")
                    }),
                )
            }
        }
    }

    /// Set or clear the soft/hard time limits for a task name.
    fn handle_time_limit(
        &self,
        task_name: &str,
        soft: Option<u64>,
        hard: Option<u64>,
    ) -> ControlResponse {
        let Some(ref limits) = self.surface.time_limits else {
            return ControlResponse::error(
                "this worker has no time-limit manager; start it with \
                 Worker::with_time_limits to accept TimeLimit commands",
            );
        };
        if soft.is_none() && hard.is_none() {
            limits.remove_task_limit(task_name);
            return ControlResponse::ack(
                true,
                Some(format!("time limits for '{task_name}' removed")),
            );
        }
        let mut config = TimeLimitConfig::new();
        if let Some(soft) = soft {
            config = config.with_soft_limit(Duration::from_secs(soft));
        }
        if let Some(hard) = hard {
            config = config.with_hard_limit(Duration::from_secs(hard));
        }
        limits.set_task_limit(task_name, config);
        ControlResponse::ack(
            true,
            Some(format!(
                "time limits for '{task_name}' set (soft {}, hard {})",
                describe_limit(soft),
                describe_limit(hard)
            )),
        )
    }

    /// Refuse a mode change once the worker has started shutting down.
    ///
    /// `Shutdown` is one-way: the run loop exits and requeues whatever it was
    /// holding. Letting a later `AddConsumer` write `Normal` over `Draining`
    /// would, in the window before the loop notices, cancel a shutdown the
    /// operator believes has happened — and after the loop has exited it would
    /// silently do nothing while reporting success. Both readings are worse
    /// than saying no.
    fn refuse_if_draining(&self, action: &str) -> Option<ControlResponse> {
        let mode = WorkerMode::from(self.surface.mode.load(Ordering::SeqCst));
        if mode.is_draining() {
            return Some(ControlResponse::error(format!(
                "worker {} is shutting down; cannot {action} (a shutdown cannot be cancelled)",
                self.surface.hostname
            )));
        }
        None
    }

    /// Resume consumption of the worker's queue.
    fn handle_add_consumer(&self, queue: &str) -> ControlResponse {
        let s = &self.surface;
        if queue != s.queue_name {
            return ControlResponse::error(format!(
                "this worker consumes only '{}'; a queue cannot be added at runtime \
                 (start another worker for '{queue}')",
                s.queue_name
            ));
        }
        if let Some(refusal) = self.refuse_if_draining("resume consumption") {
            return refusal;
        }
        s.mode.store(WorkerMode::Normal as u8, Ordering::SeqCst);
        ControlResponse::ack(true, Some(format!("consuming '{queue}'")))
    }

    /// Suspend consumption of the worker's queue without stopping the worker.
    fn handle_cancel_consumer(&self, queue: &str) -> ControlResponse {
        let s = &self.surface;
        if queue != s.queue_name {
            return ControlResponse::error(format!(
                "this worker consumes only '{}', not '{queue}'",
                s.queue_name
            ));
        }
        if let Some(refusal) = self.refuse_if_draining("suspend consumption") {
            return refusal;
        }
        s.mode
            .store(WorkerMode::Maintenance as u8, Ordering::SeqCst);
        ControlResponse::ack(
            true,
            Some(format!(
                "stopped consuming '{queue}'; in-flight tasks keep running \
                 (send AddConsumer to resume)"
            )),
        )
    }

    /// Queue sub-commands: only `Length` is something a worker can answer.
    async fn handle_queue(&self, command: &QueueCommand) -> ControlResponse {
        let s = &self.surface;
        match *command {
            QueueCommand::Length { ref queue } => {
                if queue != &s.queue_name {
                    return ControlResponse::error(format!(
                        "this worker only knows the depth of its own queue '{}'",
                        s.queue_name
                    ));
                }
                match s.broker.queue_size().await {
                    Ok(size) => ControlResponse::Queue(QueueResponse::Length {
                        queue: queue.clone(),
                        message_count: size as u64,
                    }),
                    Err(e) => ControlResponse::error(format!("failed to read queue size: {e}")),
                }
            }
            QueueCommand::Purge { .. }
            | QueueCommand::Delete { .. }
            | QueueCommand::Bind { .. }
            | QueueCommand::Unbind { .. }
            | QueueCommand::Declare { .. } => ControlResponse::error(format!(
                "'{}' is not available through the worker control channel: the Broker \
                 trait has no such operation, so a worker cannot perform it. Use the \
                 broker-specific tooling (for example `celers queue purge`) instead.",
                queue_command_name(command)
            )),
        }
    }

    /// Close one circuit breaker, or all of them.
    async fn handle_reset_circuit_breaker(&self, task_name: Option<&str>) -> ControlResponse {
        let Some(ref breaker) = self.surface.circuit_breaker else {
            return ControlResponse::error(
                "this worker runs without a circuit breaker \
                 (WorkerConfig::enable_circuit_breaker is false)",
            );
        };
        match task_name {
            Some(name) => {
                breaker.reset(name).await;
                ControlResponse::ack(true, Some(format!("circuit breaker for '{name}' closed")))
            }
            None => {
                breaker.reset_all().await;
                ControlResponse::ack(true, Some("every circuit breaker closed".to_string()))
            }
        }
    }
}

impl std::fmt::Debug for ControlService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlService")
            .field("hostname", &self.surface.hostname)
            .field("queue", &self.surface.queue_name)
            .finish_non_exhaustive()
    }
}

/// Render an optional seconds limit for an operator-facing message.
fn describe_limit(seconds: Option<u64>) -> String {
    match seconds {
        Some(seconds) => format!("{seconds}s"),
        None => "unset".to_string(),
    }
}

/// The revocation mode a `terminate` flag selects.
fn revocation_mode(terminate: bool) -> RevocationMode {
    if terminate {
        RevocationMode::Terminate
    } else {
        RevocationMode::Ignore
    }
}

/// The sub-command name, for error messages.
fn queue_command_name(command: &QueueCommand) -> &'static str {
    match *command {
        QueueCommand::Purge { .. } => "queue purge",
        QueueCommand::Length { .. } => "queue length",
        QueueCommand::Delete { .. } => "queue delete",
        QueueCommand::Bind { .. } => "queue bind",
        QueueCommand::Unbind { .. } => "queue unbind",
        QueueCommand::Declare { .. } => "queue declare",
    }
}

/// Replace any credentials in a connection URL with `***`.
///
/// Control responses travel over a broadcast channel and land in operator
/// terminals and logs; a broker URL routinely carries a password.
fn sanitize_url(url: &str) -> String {
    let Some(scheme_end) = url.find("://") else {
        return url.to_string();
    };
    let (scheme, rest) = url.split_at(scheme_end + 3);
    // Userinfo, if present, ends at the first '@' before the first '/'.
    let authority_end = rest.find('/').unwrap_or(rest.len());
    match rest[..authority_end].find('@') {
        Some(at) => format!("{scheme}***@{}", &rest[at + 1..]),
        None => url.to_string(),
    }
}

/// The transport name implied by a connection URL's scheme.
fn transport_scheme(url: &str) -> String {
    url.split("://")
        .next()
        .filter(|scheme| !scheme.is_empty() && *scheme != url)
        .unwrap_or("unknown")
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rate_limits_start_empty_and_allow_everything() {
        let limits = RuntimeRateLimits::new();
        assert!(limits.is_empty());
        assert_eq!(
            limits.check("anything"),
            RuntimeRateLimitDecision::Allowed,
            "an unconfigured limiter must not gate anything"
        );
    }

    #[test]
    fn rate_limits_deny_after_the_burst_is_spent() {
        let limits = RuntimeRateLimits::new();
        limits.set("slow", 1.0).expect("valid rate");
        assert!(!limits.is_empty());
        assert_eq!(limits.rates().get("slow").copied(), Some(1.0));

        // A rate of 1/s starts with a single token.
        assert_eq!(limits.check("slow"), RuntimeRateLimitDecision::Allowed);
        assert!(matches!(
            limits.check("slow"),
            RuntimeRateLimitDecision::Denied { .. }
        ));
        // An unrelated task keeps running.
        assert_eq!(limits.check("fast"), RuntimeRateLimitDecision::Allowed);
    }

    #[test]
    fn rate_limits_reject_non_positive_rates() {
        let limits = RuntimeRateLimits::new();
        assert!(limits.set("bad", 0.0).is_err());
        assert!(limits.set("bad", -1.0).is_err());
        assert!(limits.set("bad", f64::NAN).is_err());
        assert!(limits.is_empty(), "a rejected rate must not be installed");
    }

    #[test]
    fn rate_limits_clear_removes_the_gate() {
        let limits = RuntimeRateLimits::new();
        limits.set("slow", 1.0).expect("valid rate");
        assert_eq!(limits.check("slow"), RuntimeRateLimitDecision::Allowed);
        assert!(matches!(
            limits.check("slow"),
            RuntimeRateLimitDecision::Denied { .. }
        ));

        assert!(limits.clear("slow"));
        assert!(limits.is_empty());
        assert_eq!(limits.check("slow"), RuntimeRateLimitDecision::Allowed);
        assert!(!limits.clear("slow"), "clearing twice reports 'absent'");
    }

    #[test]
    fn sanitize_url_hides_credentials() {
        assert_eq!(
            sanitize_url("redis://user:hunter2@127.0.0.1:6379/0"),
            "redis://***@127.0.0.1:6379/0"
        );
        assert_eq!(
            sanitize_url("redis://127.0.0.1:6379"),
            "redis://127.0.0.1:6379"
        );
        assert_eq!(sanitize_url("not-a-url"), "not-a-url");
        // A '@' in the path must not be mistaken for userinfo.
        assert_eq!(
            sanitize_url("redis://127.0.0.1:6379/db@1"),
            "redis://127.0.0.1:6379/db@1"
        );
    }

    #[test]
    fn transport_scheme_reads_the_url_scheme() {
        assert_eq!(transport_scheme("redis://localhost"), "redis");
        assert_eq!(transport_scheme("amqp://localhost"), "amqp");
        assert_eq!(transport_scheme("nonsense"), "unknown");
    }
}
