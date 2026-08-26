//! Broker-fed revocation for [`Worker`](super::Worker).
//!
//! A [`RevocationWatcher`] on its own is an in-process channel: something has to
//! publish into it. This module is the thing that does, when the broker offers
//! revocations — it subscribes to
//! [`Broker::subscribe_revocations`](celers_core::Broker::subscribe_revocations)
//! and forwards every [`RevocationNotice`] into both halves of the worker's
//! revocation machinery:
//!
//! - the [`WorkerRevocationManager`], checked before every dispatch, so a task
//!   that has not started yet is refused; and
//! - the [`RevocationPublisher`], read by the [`RevocationWatcher`], so a task
//!   that *is* running has its cancellation token tripped (for a notice that
//!   says `terminate`).
//!
//! It also carries the dequeue-time consultation of the broker's persisted
//! revoked-id set ([`Broker::is_revoked`](celers_core::Broker::is_revoked)),
//! which is what catches a revocation published while this worker was not
//! listening — Pub/Sub has no redelivery.
//!
//! Split out of `worker_core.rs` to keep that file under the 2000-line ceiling;
//! the `impl` block here is the same one.

use super::Worker;

use crate::execution_context::{RevocationPublisher, RevocationSignal, RevocationWatcher};

use celers_core::revocation::{RevocationMode, WorkerRevocationManager};
use celers_core::revocation_channel::{RevocationNotice, RevocationStream};
use celers_core::{Broker, EventEmitter, TaskId};

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tracing::{debug, error, info, warn};

/// How long the bridge waits before resubscribing after the broker's revocation
/// stream ends or errors, so a broker that is down cannot spin the runtime.
const RESUBSCRIBE_BACKOFF: Duration = Duration::from_secs(1);

impl<B: Broker + 'static, E: EventEmitter + 'static> Worker<B, E> {
    /// Let the *broker* revoke this worker's tasks.
    ///
    /// [`with_revocation_watcher`](Self::with_revocation_watcher) alone gives
    /// the worker a revocation channel that nothing outside the process can
    /// publish to. This connects it to the broker, which is what makes
    /// `celers control revoke <id>` — or any other process calling
    /// [`Broker::revoke`] — reach a task running
    /// here. Two things are switched on together, because each covers what the
    /// other cannot:
    ///
    /// 1. **The bridge.** A background task subscribes to
    ///    [`Broker::subscribe_revocations`]
    ///    and, for every notice, records the revocation in this worker's
    ///    [`WorkerRevocationManager`] *and* publishes it into the
    ///    [`RevocationWatcher`]'s channel — so a task that is already running is
    ///    aborted (when the notice says `terminate`), and one that has not
    ///    started yet is refused at dispatch. The subscription resubscribes on
    ///    its own if the broker connection drops.
    /// 2. **The dequeue-time check.** Every message this worker dequeues is
    ///    checked against
    ///    [`Broker::is_revoked`] — the
    ///    broker's *persisted* revoked-id set — before it runs. Pub/Sub is
    ///    fire-and-forget, so a worker that was restarting when the revocation
    ///    was published never saw it; the persisted set is what survives that.
    ///    A revoked message is acknowledged, skipped and reported as
    ///    [`TaskEvent::Revoked`](celers_core::TaskEvent) instead of executing.
    ///
    /// A [`RevocationWatcher`] is created here if one was not supplied, since
    /// without it a `terminate` revocation could not trip a running task's
    /// cancellation token.
    ///
    /// # Cost
    ///
    /// The check adds one round trip per dequeued message on a broker that
    /// keeps a revoked set (Redis: a single `ZSCORE`), which is why this is
    /// opt-in rather than always on. It is free on brokers that keep none: the
    /// default `is_revoked` answers `false` with no I/O. A broker with no
    /// revocation channel is reported once, at startup, instead of leaving a
    /// bridge attached to something that will never carry anything.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_worker::{Worker, WorkerConfig};
    /// use celers_core::TaskRegistry;
    /// # use celers_core::Broker;
    /// # async fn example<B: Broker + 'static>(broker: B) {
    /// let worker = Worker::new(broker, TaskRegistry::new(), WorkerConfig::default())
    ///     .with_broker_revocation();
    /// # }
    /// ```
    #[must_use]
    pub fn with_broker_revocation(mut self) -> Self {
        self.broker_revocation = true;
        if self.revocation_watcher.is_none() {
            // Without a watcher a `terminate` notice would have nowhere to go,
            // and the feature would silently do half of what it says.
            self.revocation_watcher = Some(RevocationWatcher::new());
        }
        self
    }

    /// Whether broker-fed revocation is enabled (see
    /// [`with_broker_revocation`](Self::with_broker_revocation)).
    #[must_use]
    pub fn broker_revocation_enabled(&self) -> bool {
        self.broker_revocation
    }

    /// Start the broker → worker revocation bridge, if it is enabled.
    ///
    /// Returns the [`JoinHandle`] so the run loop can stop it on shutdown, or
    /// `None` when broker revocation is off.
    pub(super) fn spawn_broker_revocation_bridge(&self) -> Option<JoinHandle<()>> {
        if !self.broker_revocation {
            return None;
        }
        let broker = Arc::clone(&self.broker);
        let revocations = self.revocations.clone();
        let publisher = self
            .revocation_watcher
            .as_ref()
            .map(RevocationWatcher::publisher);
        let hostname = self.config.hostname.clone();
        Some(tokio::spawn(async move {
            bridge_loop(broker, revocations, publisher, hostname).await;
        }))
    }

    /// Whether the broker's persisted revoked-id set lists `task_id`.
    ///
    /// Always `false` (with no I/O) when broker revocation is off. A broker
    /// error is logged and read as "not known to be revoked": dropping a task
    /// because the revoked set was momentarily unreadable would lose live work.
    pub(super) async fn is_revoked_in_broker(&self, task_id: TaskId) -> bool {
        if !self.broker_revocation {
            return false;
        }
        match self.broker.is_revoked(&task_id).await {
            Ok(revoked) => revoked,
            Err(e) => {
                warn!(
                    "Could not check the broker's revoked set for task {}: {}; \
                     treating it as not revoked",
                    task_id, e
                );
                false
            }
        }
    }
}

/// Subscribe to the broker's revocations and apply each one, resubscribing for
/// as long as the task is alive.
async fn bridge_loop<B: Broker + 'static>(
    broker: Arc<B>,
    revocations: WorkerRevocationManager,
    publisher: Option<RevocationPublisher>,
    hostname: String,
) {
    loop {
        match broker.subscribe_revocations().await {
            Ok(Some(mut stream)) => {
                info!("Worker {hostname} is receiving revocations from the broker");
                drain_stream(&mut *stream, &revocations, publisher.as_ref()).await;
                // The stream ended (the connection dropped, or the broker went
                // away). Resubscribe rather than leaving revocation silently
                // dead for the rest of the worker's life.
                warn!("Worker {hostname} lost the broker revocation stream; resubscribing");
            }
            Ok(None) => {
                // Nothing to bridge, and no amount of retrying will change
                // that: say so once and stop.
                warn!(
                    "Worker {hostname} was configured with broker revocation, but this broker \
                     publishes none; queued tasks are still checked against its revoked set"
                );
                return;
            }
            Err(e) => {
                error!("Worker {hostname} could not subscribe to broker revocations: {e}");
            }
        }
        tokio::time::sleep(RESUBSCRIBE_BACKOFF).await;
    }
}

/// Apply every notice from one subscription until it ends.
async fn drain_stream(
    stream: &mut dyn RevocationStream,
    revocations: &WorkerRevocationManager,
    publisher: Option<&RevocationPublisher>,
) {
    loop {
        match stream.recv().await {
            Ok(Some(notice)) => apply_notice(notice, revocations, publisher),
            Ok(None) => return,
            Err(e) => {
                // A lagging subscriber or a malformed notice: keep the
                // subscription, but back off so a permanently broken stream
                // cannot spin.
                warn!("Broker revocation stream error: {e}");
                tokio::time::sleep(RESUBSCRIBE_BACKOFF).await;
            }
        }
    }
}

/// Record one revocation locally and publish it to the in-flight watcher.
fn apply_notice(
    notice: RevocationNotice,
    revocations: &WorkerRevocationManager,
    publisher: Option<&RevocationPublisher>,
) {
    // Recording is the half that matters for a task which has not started: the
    // dispatch check refuses it whether or not `terminate` is set.
    let mode = if notice.terminate {
        RevocationMode::Terminate
    } else {
        RevocationMode::Ignore
    };
    revocations.revoke(notice.task_id, mode);

    match publisher {
        Some(publisher) => {
            let signal = RevocationSignal {
                task_id: notice.task_id,
                terminate: notice.terminate,
            };
            let reached = publisher.publish(signal);
            debug!(
                "Broker revoked task {} (terminate={}); {} watcher(s) notified",
                notice.task_id, notice.terminate, reached
            );
        }
        None => debug!(
            "Broker revoked task {} (terminate={}); recorded for dispatch",
            notice.task_id, notice.terminate
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::{CelersError, Result};
    use tokio::sync::mpsc;
    use uuid::Uuid;

    /// A revocation stream fed from a channel, so a test can decide exactly
    /// what a broker "publishes" and when the stream ends.
    struct ScriptedStream {
        rx: mpsc::UnboundedReceiver<Result<Option<RevocationNotice>>>,
    }

    #[async_trait::async_trait]
    impl RevocationStream for ScriptedStream {
        async fn recv(&mut self) -> Result<Option<RevocationNotice>> {
            match self.rx.recv().await {
                Some(next) => next,
                // The script ran out: end the subscription.
                None => Ok(None),
            }
        }
    }

    #[test]
    fn a_notice_is_recorded_for_dispatch_and_published_to_the_watcher() {
        let revocations = WorkerRevocationManager::new();
        let watcher = RevocationWatcher::new();
        let publisher = watcher.publisher();
        let mut signals = publisher.subscribe();

        let task_id = Uuid::new_v4();
        apply_notice(
            RevocationNotice::terminate(task_id),
            &revocations,
            Some(&publisher),
        );

        assert!(revocations.is_revoked(task_id), "recorded for dispatch");
        let signal = signals.try_recv().expect("a signal was published");
        assert_eq!(signal, RevocationSignal::terminate(task_id));
    }

    #[test]
    fn a_non_terminating_notice_still_refuses_the_task_at_dispatch() {
        // Celery's `revoke(id)` must not abort a running task, but it must
        // still stop the task from starting — the recording is what does that.
        let revocations = WorkerRevocationManager::new();
        let watcher = RevocationWatcher::new();
        let publisher = watcher.publisher();
        let mut signals = publisher.subscribe();

        let task_id = Uuid::new_v4();
        apply_notice(
            RevocationNotice::ignore(task_id),
            &revocations,
            Some(&publisher),
        );

        let result = revocations.check_revocation(task_id, "any.task");
        assert!(result.revoked);
        assert_eq!(result.mode, RevocationMode::Ignore);

        let signal = signals.try_recv().expect("a signal was published");
        assert!(!signal.terminate, "a plain revoke must not terminate");
    }

    #[test]
    fn a_notice_without_a_watcher_is_still_recorded() {
        let revocations = WorkerRevocationManager::new();
        let task_id = Uuid::new_v4();
        apply_notice(RevocationNotice::terminate(task_id), &revocations, None);
        assert!(revocations.is_revoked(task_id));
    }

    #[tokio::test]
    async fn draining_survives_a_stream_error_and_ends_with_the_stream() {
        let (tx, rx) = mpsc::unbounded_channel();
        let first = Uuid::new_v4();
        let second = Uuid::new_v4();

        tx.send(Ok(Some(RevocationNotice::ignore(first)))).unwrap();
        // A malformed notice or a lagging subscriber: must not end the loop.
        tx.send(Err(CelersError::Other("lagged".to_string())))
            .unwrap();
        tx.send(Ok(Some(RevocationNotice::terminate(second))))
            .unwrap();
        tx.send(Ok(None)).unwrap();
        drop(tx);

        let revocations = WorkerRevocationManager::new();
        let mut stream = ScriptedStream { rx };

        // The error path backs off, so this needs the real clock but is bounded.
        tokio::time::timeout(
            Duration::from_secs(10),
            drain_stream(&mut stream, &revocations, None),
        )
        .await
        .expect("draining ends when the stream does");

        assert!(revocations.is_revoked(first));
        assert!(
            revocations.is_revoked(second),
            "an error must not swallow the notices after it"
        );
    }
}
