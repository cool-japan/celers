//! Internal execution-loop helpers for [`Worker`](crate::worker_core::Worker).
//!
//! Everything in this module is private to the crate: it exists so the hot path
//! in [`worker_core`](crate::worker_core) stays readable and so the decision
//! math (backoff, deferral delays, admission) can be unit tested without
//! standing up a broker.

use crate::retry::RetryConfig;
use crate::types::WorkerStats;

use celers_core::task_security::PayloadHygiene;
use celers_core::{Event, EventEmitter, TaskId};

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{mpsc, OwnedSemaphorePermit};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

/// RAII guard for a dispatched task.
///
/// Constructed by the dequeue loop *before* the task is spawned (paired with
/// [`WorkerStats::task_started`]) and moved into the spawned future, so the
/// active-task counter and the concurrency permit are released even when the
/// task future panics or is dropped without ever being polled. Both drain
/// implementations gate on `stats.active()`, so a leaked increment here would
/// hang graceful shutdown for the rest of the process lifetime.
pub(crate) struct ActiveTaskGuard {
    stats: Arc<WorkerStats>,
    /// Concurrency permit; released on drop (that is the whole point).
    _permit: Option<OwnedSemaphorePermit>,
}

impl ActiveTaskGuard {
    /// Create a guard that will decrement the active counter when dropped.
    ///
    /// The caller must have already called [`WorkerStats::task_started`].
    pub(crate) fn new(stats: Arc<WorkerStats>, permit: Option<OwnedSemaphorePermit>) -> Self {
        Self {
            stats,
            _permit: permit,
        }
    }
}

impl Drop for ActiveTaskGuard {
    fn drop(&mut self) {
        // Only synchronous work here: `Drop` can run during a panic unwind and
        // during runtime shutdown, where spawning is not allowed.
        self.stats.task_completed();
    }
}

/// Longest task payload preview retained per in-flight task for
/// `inspect active`.
///
/// A payload can be megabytes; the registry exists to answer "what is this
/// worker doing right now", not to mirror the queue in RAM.
const ARGS_PREVIEW_LIMIT: usize = 512;

/// What the registry remembers about one dispatched message.
#[derive(Clone)]
pub(crate) struct InFlightEntry {
    /// Broker receipt handle needed to ack/reject the delivery.
    pub(crate) receipt_handle: Option<String>,
    /// Task name, for `inspect active`.
    pub(crate) name: String,
    /// When the worker dispatched it (Unix timestamp, fractional seconds).
    pub(crate) started: f64,
    /// Bounded preview of the serialized payload, captured only when the
    /// registry was built with [`InFlightRegistry::with_args_capture`].
    pub(crate) args_preview: Option<String>,
}

/// The messages a worker has dispatched but not yet disposed of (acked,
/// rejected or re-enqueued).
///
/// The entry doubles as a *disposition token*: whoever removes it owns the
/// broker-side disposition of that message. This makes the shutdown-deadline
/// requeue and a task finishing at the same instant mutually exclusive instead
/// of racing into a "requeued and acked" double delivery.
///
/// It is also the worker's source of truth for `inspect active`: the counters
/// in [`WorkerStats`] know *how many* tasks are running, only this map knows
/// *which*.
#[derive(Clone, Default)]
pub(crate) struct InFlightRegistry {
    inner: Arc<Mutex<HashMap<TaskId, InFlightEntry>>>,
    /// Whether to retain a payload preview per task. Off unless the remote
    /// control protocol is wired up, so an ordinary worker pays nothing.
    capture_args: bool,
    /// Redaction applied to the retained preview, when
    /// [`WorkerConfig::payload_hygiene`](crate::WorkerConfig::payload_hygiene)
    /// is configured. The preview is a *copy*: the payload the task executes is
    /// never routed through this.
    hygiene: Option<PayloadHygiene>,
}

impl InFlightRegistry {
    /// Create an empty registry that does not retain payload previews.
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// Create an empty registry that retains a bounded payload preview per
    /// task, so `inspect active` can report task arguments.
    ///
    /// `hygiene` redacts that preview — secret-looking keys and PII — before it
    /// is retained, so what an operator inspects never carries what the payload
    /// carried. `None` retains the payload verbatim, which is what a worker
    /// with no hygiene configured has always done.
    pub(crate) fn with_args_capture(hygiene: Option<PayloadHygiene>) -> Self {
        Self {
            inner: Arc::new(Mutex::new(HashMap::new())),
            capture_args: true,
            hygiene,
        }
    }

    /// Lock the map, recovering from a poisoned lock.
    ///
    /// A poisoned lock only means some other thread panicked while holding it;
    /// the map itself is still consistent, and refusing to serve it would
    /// strand every in-flight message.
    fn entries(&self) -> std::sync::MutexGuard<'_, HashMap<TaskId, InFlightEntry>> {
        self.inner
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Record a dispatched message, its receipt handle and its identity.
    pub(crate) fn register(
        &self,
        task_id: TaskId,
        receipt_handle: Option<String>,
        task: &celers_core::SerializedTask,
    ) {
        let entry = InFlightEntry {
            receipt_handle,
            name: task.metadata.name.clone(),
            started: unix_now(),
            args_preview: if self.capture_args {
                Some(args_preview(&task.payload, self.hygiene.as_ref()))
            } else {
                None
            },
        };
        self.entries().insert(task_id, entry);
    }

    /// Claim the right to dispose of `task_id`.
    ///
    /// Returns `true` exactly once per registered message.
    pub(crate) fn claim(&self, task_id: &TaskId) -> bool {
        self.entries().remove(task_id).is_some()
    }

    /// Take every still-undisposed message, leaving the registry empty.
    pub(crate) fn take_all(&self) -> Vec<(TaskId, Option<String>)> {
        self.entries()
            .drain()
            .map(|(task_id, entry)| (task_id, entry.receipt_handle))
            .collect()
    }

    /// Snapshot of every currently dispatched message.
    pub(crate) fn snapshot(&self) -> Vec<(TaskId, InFlightEntry)> {
        self.entries()
            .iter()
            .map(|(task_id, entry)| (*task_id, entry.clone()))
            .collect()
    }

    /// Number of messages still awaiting disposition.
    pub(crate) fn len(&self) -> usize {
        self.entries().len()
    }

    /// Whether every dispatched message has been disposed of.
    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Current wall-clock time as fractional seconds since the Unix epoch.
fn unix_now() -> f64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// A bounded, always-valid-UTF-8 rendering of a task payload.
///
/// `CeleRS` payloads are JSON produced by `serde_json`, so the common case
/// renders verbatim; a binary payload (msgpack, a compressed body) is reported
/// by size rather than mangled into replacement characters.
///
/// With `hygiene` configured the rendering is the *redacted* one — secret-looking
/// keyword values replaced, PII masked — computed on a copy. `payload` itself is
/// only ever read.
pub(crate) fn args_preview(payload: &[u8], hygiene: Option<&PayloadHygiene>) -> String {
    match hygiene.filter(|h| h.is_enabled()) {
        Some(hygiene) => truncate_preview(&hygiene.redact_payload(payload).text, payload.len()),
        None => match std::str::from_utf8(payload) {
            Ok(text) => truncate_preview(text, payload.len()),
            Err(_) => format!("<binary payload, {} bytes>", payload.len()),
        },
    }
}

/// Clip `text` to [`ARGS_PREVIEW_LIMIT`], never mid-code-point.
///
/// `source_len` is the size of the payload the text was rendered from, reported
/// alongside the ellipsis so an operator can tell a clipped preview from a short
/// payload.
fn truncate_preview(text: &str, source_len: usize) -> String {
    if text.len() <= ARGS_PREVIEW_LIMIT {
        return text.to_string();
    }
    let mut end = ARGS_PREVIEW_LIMIT;
    while end > 0 && !text.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… ({source_len} bytes)", &text[..end])
}

/// A non-blocking sink for task/worker lifecycle events.
///
/// Lifecycle events are telemetry, not data: emitting them inline costs a
/// broker round trip *per event* on the task critical path (and the
/// `task-received` emit sits between the dequeue and the dispatch, extending
/// queue latency for every message). The sink hands events to a bounded
/// channel drained by a single background task that batches them into
/// [`EventEmitter::emit_batch`], so the critical path pays an atomic enqueue
/// and ordering is still preserved (one FIFO channel, one drainer).
///
/// When the buffer is full events are dropped and counted rather than
/// back-pressuring task execution.
#[derive(Clone)]
pub(crate) struct EventSink {
    tx: Option<mpsc::Sender<Event>>,
    dropped: Arc<AtomicU64>,
    emit_failures: Arc<AtomicU64>,
}

impl EventSink {
    /// A sink that discards everything (events disabled).
    pub(crate) fn disabled() -> Self {
        Self {
            tx: None,
            dropped: Arc::new(AtomicU64::new(0)),
            emit_failures: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create a buffered sink plus the background drainer that flushes it.
    ///
    /// The drainer ends once every clone of the sink has been dropped, so
    /// awaiting the returned handle flushes the buffer.
    pub(crate) fn buffered<E: EventEmitter + 'static>(
        emitter: Arc<E>,
        capacity: usize,
        max_batch: usize,
    ) -> (Self, JoinHandle<()>) {
        let (tx, mut rx) = mpsc::channel::<Event>(capacity.max(1));
        let max_batch = max_batch.max(1);
        let emit_failures = Arc::new(AtomicU64::new(0));
        let drainer_failures = Arc::clone(&emit_failures);

        let handle = tokio::spawn(async move {
            while let Some(first) = rx.recv().await {
                let mut batch = Vec::with_capacity(max_batch);
                batch.push(first);
                while batch.len() < max_batch {
                    match rx.try_recv() {
                        Ok(event) => batch.push(event),
                        Err(_) => break,
                    }
                }
                let batched = batch.len();
                if let Err(e) = emitter.emit_batch(batch).await {
                    // A permanently dead event pipeline (unreachable broker,
                    // wrong credentials) must not be invisible: count every
                    // failure and warn — rate-limited so a sustained outage
                    // cannot itself flood the log.
                    let failures = drainer_failures.fetch_add(1, Ordering::Relaxed) + 1;
                    if failures == 1 || failures.is_multiple_of(100) {
                        warn!(
                            "Failed to emit {} buffered event(s) ({} emit failure(s) so far): {}",
                            batched, failures, e
                        );
                    } else {
                        debug!("Failed to emit {} buffered event(s): {}", batched, e);
                    }
                }
            }
        });

        (
            Self {
                tx: Some(tx),
                dropped: Arc::new(AtomicU64::new(0)),
                emit_failures,
            },
            handle,
        )
    }

    /// Queue an event. Never blocks and never awaits.
    pub(crate) fn emit(&self, event: Event) {
        let Some(ref tx) = self.tx else {
            return;
        };
        if tx.try_send(event).is_err() {
            let dropped = self.dropped.fetch_add(1, Ordering::Relaxed) + 1;
            if dropped == 1 || dropped.is_multiple_of(100) {
                warn!(
                    "Event buffer full or closed, dropped {} lifecycle event(s)",
                    dropped
                );
            }
        }
    }

    /// Number of events dropped because the buffer was full or closed.
    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Number of `emit_batch` calls the transport rejected.
    ///
    /// Non-zero means the event pipeline is failing downstream of this sink
    /// (dead broker, bad credentials) even though nothing was dropped locally.
    pub(crate) fn emit_failures(&self) -> u64 {
        self.emit_failures.load(Ordering::Relaxed)
    }
}

/// Render the payload of a task panic as a log/error message.
pub(crate) fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    if let Some(msg) = payload.downcast_ref::<&'static str>() {
        (*msg).to_string()
    } else if let Some(msg) = payload.downcast_ref::<String>() {
        msg.clone()
    } else {
        "task panicked".to_string()
    }
}

/// Clamp a requested deferral delay into the configured band and spread it with
/// a little jitter so a fleet that all defers the same task does not
/// synchronise its retries.
///
/// A limiter with a rate of `0.0` reports an effectively unbounded
/// `retry_after`; clamping *before* sleeping is what keeps a deferral from
/// parking the dequeue loop for hours.
pub(crate) fn clamp_defer_delay(requested: Duration, min_ms: u64, max_ms: u64) -> Duration {
    let max_ms = max_ms.max(min_ms);
    let requested_ms = u64::try_from(requested.as_millis()).unwrap_or(u64::MAX);
    let clamped_ms = requested_ms.clamp(min_ms, max_ms);
    if clamped_ms == 0 {
        return Duration::ZERO;
    }

    // +/-10% jitter, never leaving the configured band.
    let jitter_span = (clamped_ms as f64 * 0.1).max(1.0);
    let offset = {
        use rand::RngExt;
        let mut rng = rand::rng();
        rng.random_range(-jitter_span..=jitter_span)
    };
    let jittered = (clamped_ms as f64 + offset).max(0.0) as u64;
    Duration::from_millis(jittered.clamp(min_ms, max_ms))
}

/// Whole-second delay to hand to a broker's delayed-enqueue API, if the delay
/// is long enough to survive the API's one-second granularity.
///
/// Returns `None` for sub-second delays, for which the caller should sleep
/// instead (rounding those down to `0` would re-enqueue immediately and
/// reintroduce the retry storm the backoff exists to prevent).
pub(crate) fn schedulable_delay_secs(delay: Duration) -> Option<u64> {
    if delay < Duration::from_secs(1) {
        return None;
    }
    // Round up so a 1.9s backoff is not truncated to 1s.
    let secs = delay.as_secs();
    let rounded = if delay.subsec_nanos() > 0 {
        secs.saturating_add(1)
    } else {
        secs
    };
    Some(rounded.max(1))
}

/// Backoff delay before retry attempt `retry_count`, computed from the
/// effective retry configuration.
///
/// [`RetryStrategy::calculate_delay`](crate::RetryStrategy::calculate_delay)
/// casts the attempt number to `i32` to exponentiate: an attempt number above
/// `i32::MAX` wraps negative and *shrinks* the delay to a fraction of the base
/// (u32::MAX becomes `2^-1`), so the count is clamped before delegating.
pub(crate) fn backoff_delay(config: &RetryConfig, retry_count: u32) -> Duration {
    const MAX_EXPONENT: u32 = i32::MAX as u32;
    config.calculate_delay(retry_count.min(MAX_EXPONENT))
}

/// How often the worker sweeps expired dead-letter entries, given their TTL.
///
/// A TTL only describes *when* an entry becomes stale; something has to run the
/// sweep or the queue grows without bound. Sweeping once per TTL keeps a short
/// TTL honest without burning a round trip per second, and the hour cap keeps a
/// multi-day TTL from meaning "never actually reclaimed within one worker's
/// lifetime". A zero TTL is clamped to one second: `interval` panics on a zero
/// period.
pub(crate) fn dlq_cleanup_interval(ttl_seconds: u64) -> Duration {
    /// Never sweep less often than hourly, whatever the TTL.
    const MAX_INTERVAL_SECS: u64 = 3_600;
    Duration::from_secs(ttl_seconds.clamp(1, MAX_INTERVAL_SECS))
}

/// The retry budget actually applied to a task.
///
/// The task's own `max_retries` is the request; the worker's (runtime
/// updatable) `max_retries` is an operator cap, so the effective budget is the
/// smaller of the two. Without this, `WorkerHandle::set_max_retries` logs a
/// confirming line and changes nothing.
pub(crate) fn effective_max_retries(task_max_retries: u32, worker_max_retries: u32) -> u32 {
    task_max_retries.min(worker_max_retries)
}
