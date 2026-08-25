//! Run-loop support for [`Worker`](super::Worker): pacing, backpressure,
//! deferral, batch coalescing and the shutdown drain.
//!
//! A continuation of `worker_core`'s inherent `impl`, split out so neither file
//! passes the workspace's size cap. Everything here is called from
//! `run_loop`/`run_loop_inner` (or, in `calculate_backoff_delay`'s case,
//! reports the same arithmetic that loop schedules retries with).

use super::{Worker, PERMIT_WAIT};

use crate::batching::{self, CoalesceStrategy};
use crate::types::WorkerStats;
use crate::worker_core::support::{self, clamp_defer_delay, InFlightRegistry};

use celers_core::{Broker, EventEmitter, TaskId, WorkerEventBuilder};

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration as StdDuration;

use tokio::sync::{mpsc, OwnedSemaphorePermit, Semaphore};
use tokio::time::{sleep, timeout, Duration};
use tracing::{error, info, warn};

impl<B: Broker + 'static, E: EventEmitter + 'static> Worker<B, E> {
    /// Heartbeat loop that periodically emits worker-heartbeat events
    pub(super) async fn heartbeat_loop<EE: EventEmitter>(
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
                // A heartbeat nobody receives is how a monitor decides this
                // worker is dead, so a failure here is operationally visible.
                warn!("Failed to emit worker-heartbeat event: {}", e);
            }
        }
    }

    /// Get system load average (returns [0.0, 0.0, 0.0] where unsupported)
    ///
    /// Delegates to [`crate::sysinfo::read_load_average`], which reads
    /// `/proc/loadavg` on Linux and `getloadavg(3)` on the BSDs/macOS — the
    /// latter has no `/proc`, where the previous inline implementation silently
    /// reported a flat zero load in every heartbeat.
    pub(super) fn get_load_average() -> [f64; 3] {
        crate::sysinfo::read_load_average().unwrap_or([0.0, 0.0, 0.0])
    }

    /// Current poll interval from the (runtime updatable) dynamic config.
    pub(super) fn poll_interval(&self) -> Duration {
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
    pub(super) async fn sleep_or_shutdown(
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
    pub(super) async fn acquire_permits(
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
    ///
    /// This goes through [`Broker::defer`](celers_core::Broker::defer), not
    /// `reject(requeue = true)`. The distinction is not cosmetic: a broker that
    /// records retry state — the Redis one rewrites the payload to
    /// `Retrying(n + 1)` on requeue — would otherwise charge an admission miss
    /// (wrong worker, unmet affinity, a disabled feature flag, a saturated rate
    /// limiter, a half-open circuit's spent probe budget, a draining worker)
    /// against the task's `max_retries`, and a task that merely visited the
    /// wrong worker often enough would be dead-lettered without ever having
    /// run.
    ///
    /// `delay` is how long the message should stay invisible; a broker with a
    /// delayed queue holds it there. It is the same value the dequeue loop uses
    /// for its own back-off, so the worker and the message wait in step.
    pub(super) async fn defer_message(
        &self,
        task_id: &TaskId,
        receipt_handle: Option<&str>,
        delay: Duration,
        reason: &str,
    ) {
        self.stats.task_deferred();
        if let Err(e) = self.broker.defer(task_id, receipt_handle, delay).await {
            error!("Failed to defer task {} ({}): {}", task_id, reason, e);
        }
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
    pub(super) async fn drain_in_flight(
        &self,
        permits: &Arc<Semaphore>,
        in_flight: &InFlightRegistry,
        concurrency: usize,
    ) {
        // Read the runtime value, not the static one: `ControlCommand::Shutdown
        // { timeout }` replaces the drain deadline, and reading the config here
        // would make that parameter decorative.
        let deadline = Duration::from_secs(self.shutdown_timeout_secs.load(Ordering::SeqCst));
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
                         running; deferring their messages",
                        deadline,
                        in_flight.len()
                    );
                }
            }
        } else if !in_flight.is_empty() {
            warn!(
                "Graceful shutdown disabled; deferring {} in-flight message(s)",
                in_flight.len()
            );
        }

        // Whatever is left was never disposed of by its task: hand it back to
        // the broker without spending a retry. This worker cannot tell
        // whether the task ran and failed or never got the chance to run at
        // all -- it only knows the deadline passed with the message still
        // claimed -- so `defer_message` (retry-neutral `Broker::defer`) is
        // the right call, not `reject(requeue = true)` (which a broker that
        // records retry state, like the Redis one, reads as a failed
        // attempt). See `defer_message`'s doc comment for the full rationale.
        for (task_id, receipt_handle) in in_flight.take_all() {
            warn!("Deferring undisposed task {} at shutdown", task_id);
            self.defer_message(
                &task_id,
                receipt_handle.as_deref(),
                Duration::ZERO,
                "shutdown drain",
            )
            .await;
        }
    }

    /// Deferral delay for admission decisions (routing / affinity / features).
    pub(super) fn admission_defer_delay(&self) -> Duration {
        clamp_defer_delay(
            Duration::from_millis(self.config.defer_delay_ms),
            self.config.defer_delay_ms,
            self.config.defer_max_delay_ms,
        )
    }

    /// Clamp an externally supplied delay (e.g. a rate limiter's `retry_after`)
    /// into the configured deferral band.
    pub(super) fn clamped_defer_delay(&self, requested: Duration) -> Duration {
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
    pub(super) async fn coalesce_and_ack_duplicates(
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
