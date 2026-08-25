//! Retry-neutral deferral for [`RedisBroker`].
//!
//! A worker refuses a message for two different reasons and only one of them
//! is the task's fault. A task that *ran and failed* is retried, and that must
//! count against `max_retries`. A task that *never ran* — the worker cannot
//! route it, its labels do not satisfy the task's affinity, a feature flag is
//! off, a rate limiter is saturated, a circuit breaker's half-open probe budget
//! is spoken for, or the worker is draining — has nothing wrong with it.
//!
//! [`Broker::reject`](celers_core::Broker::reject) with `requeue = true` is the
//! first meaning, and this broker implements it that way: it rewrites the
//! payload's state to `Retrying(n + 1)` on the way back. Routing an admission
//! miss through it means a task that merely visited the wrong worker
//! `max_retries` times is dead-lettered without ever having executed.
//!
//! [`Broker::defer`](celers_core::Broker::defer) is the second meaning, and
//! this module is its Redis implementation: the message goes back **byte for
//! byte as delivered**.
//!
//! # Ready queue or delayed set
//!
//! The delayed sorted set is scored in whole Unix seconds — the same scale
//! [`RedisBroker::promote_delayed_tasks`] compares against — so a delay under
//! one second cannot be expressed there at all: it would land at "now", wait
//! for the next housekeeping sweep, and come back *later* than an immediate
//! requeue would have. Deferrals therefore split:
//!
//! * `delay < 1s` (including the `Duration::ZERO` the worker passes for every
//!   admission miss) — straight back to the ready queue, at the *back* of the
//!   delivery order. Another worker that can serve the task picks it up at
//!   once, which is the whole point of deferring an admission miss.
//! * `delay >= 1s` (a cluster-wide rate limiter's `retry_after`, say) — into
//!   the delayed set, where it is invisible until due.
//!
//! Either way the payload is unchanged, so retry accounting is untouched.

use std::time::Duration;

use celers_core::{CelersError, Result, SerializedTask, TaskId};
use tracing::{debug, warn};

use crate::RedisBroker;

/// Shortest delay the delayed sorted set can express.
///
/// Its scores are whole Unix seconds, so anything below this rounds to "due
/// now" and would merely wait for a housekeeping sweep.
pub(crate) const MIN_DELAYED_DELAY: Duration = Duration::from_secs(1);

impl RedisBroker {
    /// Return an in-flight message to the queue without touching its retry
    /// state. See the module documentation for the ready/delayed split.
    pub(crate) async fn defer_delivery(
        &self,
        task_id: &TaskId,
        receipt_handle: Option<&str>,
        delay: Duration,
    ) -> Result<()> {
        let Some(handle) = receipt_handle else {
            // Nothing is in flight, so there is nothing to give back. This
            // mirrors `reject`, which is likewise a no-op without a handle.
            warn!("Cannot defer task {task_id} without a receipt handle");
            return Ok(());
        };

        // Parsing is the same validity check `reject` performs — an
        // undeserializable handle is not a message this broker produced, and
        // quietly re-adding it would keep it circulating — and it yields the
        // score the message would be enqueued with in Priority mode.
        let task: SerializedTask = serde_json::from_str(handle)
            .map_err(|e| CelersError::Deserialization(e.to_string()))?;
        let score = -(task.metadata.priority as f64);

        let execute_at = if delay < MIN_DELAYED_DELAY {
            0
        } else {
            crate::now_secs().saturating_add(delay.as_secs())
        };

        let mut conn = self.get_connection().await?;
        let deferred = self
            .visibility_manager
            .defer_unacked(&mut conn, &self.keys, handle, execute_at, self.mode, score)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to defer task: {}", e)))?;

        if !deferred {
            // Acknowledged, reaped or revoked in the meantime. Re-adding it
            // would resurrect a message another path already disposed of.
            debug!("Task {task_id} was no longer in flight; deferral dropped");
        } else if execute_at == 0 {
            debug!("Deferred task {task_id} back onto the ready queue");
        } else {
            debug!("Deferred task {task_id} until Unix timestamp {execute_at}");
        }
        Ok(())
    }
}
