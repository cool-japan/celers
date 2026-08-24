//! Remote-control wiring for [`Worker`](super::Worker).
//!
//! The builder methods that attach a control channel to a worker, plus the
//! assembly of the `ControlSurface` the control protocol dispatches against.
//! Split out of `worker_core.rs` to keep that file under the 2000-line ceiling;
//! this is the same `impl` block, so nothing about the public API changes.

use super::{support, ControlService, ControlSurface, Worker};

use celers_core::control_transport::ControlTransport;
use celers_core::revocation::WorkerRevocationManager;
use celers_core::{Broker, EventEmitter};

use crate::control::RuntimeRateLimits;

use std::sync::Arc;

impl<B: Broker + 'static, E: EventEmitter + 'static> Worker<B, E> {
    /// Expose this worker on a remote control channel.
    ///
    /// While the worker runs, a subscriber on `transport` receives
    /// [`ControlCommand`](celers_core::ControlCommand)s addressed to this
    /// worker, dispatches them against the live worker (revocations, shutdown,
    /// rate limits, time limits, circuit breakers, task registry, active-task
    /// inspection) and publishes a
    /// [`ControlResponse`](celers_core::ControlResponse) back. The subscriber
    /// stops when the run loop does.
    ///
    /// Enabling this also makes the worker retain a bounded preview of each
    /// running task's payload so `inspect active` can report task arguments;
    /// a worker without a control transport keeps none.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_core::{InMemoryControlTransport, TaskRegistry};
    /// use celers_worker::{Worker, WorkerConfig};
    /// use std::sync::Arc;
    /// # use celers_core::Broker;
    /// # async fn example<B: Broker + 'static>(broker: B) {
    /// let transport = Arc::new(InMemoryControlTransport::new());
    /// let worker = Worker::new(broker, TaskRegistry::new(), WorkerConfig::default())
    ///     .with_control_transport(Arc::clone(&transport) as Arc<dyn celers_core::ControlTransport>);
    /// # }
    /// ```
    #[must_use]
    pub fn with_control_transport(mut self, transport: Arc<dyn ControlTransport>) -> Self {
        self.control_transport = Some(transport);
        self
    }

    /// Record the broker URL for `inspect conf` / `inspect stats`.
    ///
    /// The [`Broker`] trait never exposes its connection string, so a worker
    /// cannot discover it; without this, those inspections honestly report the
    /// URL as unknown. Credentials are stripped before the value is reported.
    #[must_use]
    pub fn with_broker_url(mut self, url: impl Into<String>) -> Self {
        self.broker_url = Some(url.into());
        self
    }

    /// Record the result-backend URL for `inspect conf`.
    ///
    /// Credentials are stripped before the value is reported.
    #[must_use]
    pub fn with_result_backend_url(mut self, url: impl Into<String>) -> Self {
        self.result_backend_url = Some(url.into());
        self
    }

    /// Revocations recorded through the remote control protocol.
    ///
    /// Shared with the running worker: revoking here refuses the task at its
    /// next dispatch, exactly as a `Revoke` control command would.
    pub fn revocations(&self) -> &WorkerRevocationManager {
        &self.revocations
    }

    /// Worker-local per-task rate limits installed through remote control.
    pub fn rate_limits(&self) -> &RuntimeRateLimits {
        &self.rate_limits
    }

    /// A control service bound to this worker, for hosts that want to drive
    /// the control protocol themselves instead of over a transport.
    ///
    /// The returned service shares the worker's state, but the in-flight
    /// registry it inspects is created when the run loop starts, so a service
    /// built before [`run`](Self::run) reports no active tasks. Prefer
    /// [`with_control_transport`](Self::with_control_transport), which binds
    /// the service the run loop actually uses.
    pub fn control_service(&self) -> ControlService {
        ControlService::new(Arc::new(
            self.control_surface(support::InFlightRegistry::with_args_capture()),
        ))
    }

    /// Assemble the handles the control protocol dispatches against.
    pub(super) fn control_surface(&self, in_flight: support::InFlightRegistry) -> ControlSurface {
        ControlSurface {
            hostname: self.config.hostname.clone(),
            pid: std::process::id(),
            queue_name: self.config.queue_name.clone(),
            broker: Arc::clone(&self.broker) as Arc<dyn Broker>,
            registry: Arc::clone(&self.registry),
            in_flight,
            stats: Arc::clone(&self.stats),
            health: self.health.clone(),
            mode: Arc::clone(&self.mode),
            shutdown_tx: self.shutdown_tx.clone(),
            shutdown_timeout_secs: Arc::clone(&self.shutdown_timeout_secs),
            dynamic_config: Arc::clone(&self.dynamic_config),
            revocations: self.revocations.clone(),
            revocation_watcher: self.revocation_watcher.clone(),
            rate_limits: self.rate_limits.clone(),
            time_limits: self.time_limits.clone(),
            circuit_breaker: self.circuit_breaker.clone(),
            concurrency: u32::try_from(self.config.concurrency).unwrap_or(u32::MAX),
            // CeleRS has no prefetch reserve; a batch dequeue is the closest
            // analogue of Celery's prefetch multiplier.
            prefetch_multiplier: if self.config.enable_batch_dequeue {
                u32::try_from(self.config.batch_size).unwrap_or(u32::MAX)
            } else {
                1
            },
            // The worker acknowledges a message only after the task reaches a
            // terminal state, which is Celery's `task_acks_late = True`.
            acks_late: true,
            // Undisposed messages are requeued when the drain deadline passes,
            // so a lost worker's work is redelivered.
            requeue_on_worker_lost: self.config.graceful_shutdown,
            broker_url: self.broker_url.clone(),
            result_backend: self.result_backend_url.clone(),
        }
    }
}
