//! Task execution context with cooperative cancellation.
//!
//! This module makes the [`CancellationToken`]
//! a first-class part of a running task's environment so that long-running task
//! code can *cooperatively* check whether it has been revoked and abort cleanly,
//! exactly as Celery's `task.is_aborted()` works.
//!
//! There are two complementary mechanisms:
//!
//! 1. **Task-local context** ([`TaskExecutionContext`]). Before the worker drives
//!    a task future, it installs a context (carrying the task's
//!    [`CancellationToken`]) into a [`tokio::task_local!`] slot for the duration
//!    of that future. Task implementations can then call
//!    [`current_token`] / [`is_cancelled`] / [`check_cancelled`] from anywhere in
//!    their async call stack — no need to thread an argument through every
//!    function — to observe cancellation and return early.
//!
//! 2. **Revocation watching** ([`RevocationWatcher`] + [`RevocationPublisher`]).
//!    The broker's revocation Pub/Sub is modelled as a
//!    [`tokio::sync::broadcast`] channel of [`RevocationSignal`]s. The worker runs
//!    a background watcher that subscribes to this channel and, for every signal,
//!    trips the matching *in-flight* task's token via a shared
//!    [`CancellationRegistry`]. A
//!    real broker (e.g. Redis pub/sub) feeds the same channel; an in-process
//!    publisher is provided for tests.
//!
//! The worker combines the two by running the task future inside
//! [`TaskExecutionContext::scope`] *and* racing it against
//! [`CancellationToken::cancelled`](crate::cancellation::CancellationToken::cancelled)
//! with [`tokio::select!`]. Cooperative tasks stop themselves at their next
//! check; even fully opaque tasks are dropped at the next `.await` point when the
//! token trips, after which the worker transitions the task to `Revoked`.
//!
//! # Example
//!
//! ```rust
//! use celers_worker::execution_context::{TaskExecutionContext, current_token};
//! use celers_worker::cancellation::CancellationToken;
//!
//! # async fn example() {
//! let token = CancellationToken::new(uuid::Uuid::new_v4());
//! let ctx = TaskExecutionContext::new(token.clone());
//!
//! let result = ctx
//!     .scope(async {
//!         // Cooperative task body: poll the ambient token.
//!         for _ in 0..1_000 {
//!             if current_token().map(|t| t.is_cancelled()).unwrap_or(false) {
//!                 return "aborted";
//!             }
//!             tokio::task::yield_now().await;
//!         }
//!         "done"
//!     })
//!     .await;
//! # let _ = result;
//! # }
//! ```

use crate::cancellation::{CancellationError, CancellationRegistry, CancellationToken};
use celers_core::TaskId;
use std::future::Future;
use std::sync::Arc;
use tokio::sync::broadcast;
use tokio::task::JoinHandle;
use tracing::{debug, warn};

tokio::task_local! {
    /// The cancellation token of the task currently executing on this task,
    /// installed for the duration of [`TaskExecutionContext::scope`].
    static CURRENT_CONTEXT: TaskExecutionContext;
}

/// Ambient context describing the task currently being executed.
///
/// Cheaply cloneable. Installed into a task-local for the lifetime of a task
/// future so cooperative task code can reach its [`CancellationToken`] without
/// having it threaded explicitly through every call.
#[derive(Clone)]
pub struct TaskExecutionContext {
    token: CancellationToken,
}

impl TaskExecutionContext {
    /// Create a new execution context wrapping a cancellation token.
    #[must_use]
    pub fn new(token: CancellationToken) -> Self {
        Self { token }
    }

    /// The cancellation token for this task.
    #[must_use]
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// The id of the task this context belongs to.
    #[must_use]
    pub fn task_id(&self) -> TaskId {
        self.token.task_id()
    }

    /// Whether cancellation has been requested for this task.
    #[must_use]
    pub fn is_cancelled(&self) -> bool {
        self.token.is_cancelled()
    }

    /// Run `future` with this context installed as the ambient task-local
    /// context, so [`current_context`] / [`current_token`] resolve to it inside.
    ///
    /// The context is automatically removed when `future` completes.
    pub async fn scope<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        CURRENT_CONTEXT.scope(self.clone(), future).await
    }
}

/// Get the ambient [`TaskExecutionContext`] for the currently executing task, if
/// one was installed via [`TaskExecutionContext::scope`].
///
/// Returns `None` when called outside of a task scope (e.g. from worker plumbing
/// rather than from inside a task body).
#[must_use]
pub fn current_context() -> Option<TaskExecutionContext> {
    CURRENT_CONTEXT.try_with(Clone::clone).ok()
}

/// Get the ambient [`CancellationToken`] for the currently executing task, if any.
#[must_use]
pub fn current_token() -> Option<CancellationToken> {
    CURRENT_CONTEXT.try_with(|ctx| ctx.token.clone()).ok()
}

/// Convenience: whether the currently executing task has been cancelled.
///
/// Returns `false` when there is no ambient context (nothing to cancel).
#[must_use]
pub fn is_cancelled() -> bool {
    CURRENT_CONTEXT
        .try_with(|ctx| ctx.token.is_cancelled())
        .unwrap_or(false)
}

/// Convenience: return [`CancellationError::Cancelled`] if the currently
/// executing task has been cancelled, otherwise `Ok(())`.
///
/// Cooperative tasks can `?` this at natural checkpoints to bail out cleanly.
/// Outside of a task scope this is always `Ok(())`.
pub fn check_cancelled() -> Result<(), CancellationError> {
    match CURRENT_CONTEXT.try_with(|ctx| ctx.token.clone()) {
        Ok(token) => token.check_cancelled(),
        Err(_) => Ok(()),
    }
}

/// A revocation signal published on the broker's cancellation Pub/Sub.
///
/// This is the in-process representation of a message a broker broadcasts when a
/// task is revoked. A `terminate` signal asks the worker to abort the task if it
/// is currently running; otherwise it merely records intent (handled elsewhere
/// for not-yet-started tasks).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevocationSignal {
    /// The task to revoke.
    pub task_id: TaskId,
    /// Whether a running task should be terminated (vs. ignored if not running).
    pub terminate: bool,
}

impl RevocationSignal {
    /// Create a terminating revocation signal for `task_id`.
    #[must_use]
    pub fn terminate(task_id: TaskId) -> Self {
        Self {
            task_id,
            terminate: true,
        }
    }

    /// Create a non-terminating revocation signal for `task_id`.
    #[must_use]
    pub fn ignore(task_id: TaskId) -> Self {
        Self {
            task_id,
            terminate: false,
        }
    }
}

/// Publisher side of the revocation Pub/Sub.
///
/// A broker (or a control-command handler, or a test) publishes
/// [`RevocationSignal`]s here; every [`RevocationWatcher`] subscribed to the
/// associated channel observes them. Cloning shares the same channel.
#[derive(Clone)]
pub struct RevocationPublisher {
    sender: broadcast::Sender<RevocationSignal>,
}

impl RevocationPublisher {
    /// Create a new publisher with a bounded broadcast buffer of `capacity`
    /// pending signals per subscriber.
    #[must_use]
    pub fn new(capacity: usize) -> Self {
        let (sender, _rx) = broadcast::channel(capacity.max(1));
        Self { sender }
    }

    /// Publish a revocation signal to all current subscribers.
    ///
    /// Returns the number of subscribers that received it (0 if none are
    /// currently subscribed, which is not an error — the signal is simply
    /// dropped, matching fire-and-forget Pub/Sub semantics).
    pub fn publish(&self, signal: RevocationSignal) -> usize {
        self.sender.send(signal).unwrap_or(0)
    }

    /// Convenience: publish a terminating revocation for `task_id`.
    pub fn revoke(&self, task_id: TaskId) -> usize {
        self.publish(RevocationSignal::terminate(task_id))
    }

    /// Subscribe a new receiver to this publisher's channel.
    #[must_use]
    pub fn subscribe(&self) -> broadcast::Receiver<RevocationSignal> {
        self.sender.subscribe()
    }

    /// Number of active subscribers.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for RevocationPublisher {
    fn default() -> Self {
        Self::new(256)
    }
}

/// Watches the broker's revocation Pub/Sub and trips the matching in-flight
/// task's [`CancellationToken`].
///
/// The watcher holds a shared [`CancellationRegistry`] (the same one the worker
/// registers in-flight tasks into). For each [`RevocationSignal`] received it
/// looks up the registry: if the task is currently in flight, its token is
/// tripped (cooperative cancellation kicks in); if it is not in flight, the
/// signal is ignored here (not-yet-started revocations are enforced at dequeue
/// time by other machinery).
#[derive(Clone)]
pub struct RevocationWatcher {
    registry: Arc<CancellationRegistry>,
    publisher: RevocationPublisher,
}

impl RevocationWatcher {
    /// Create a new watcher over a fresh registry and a fresh publisher.
    #[must_use]
    pub fn new() -> Self {
        Self {
            registry: Arc::new(CancellationRegistry::new()),
            publisher: RevocationPublisher::default(),
        }
    }

    /// Create a watcher bound to an existing publisher (e.g. one a broker already
    /// owns) and a fresh registry.
    #[must_use]
    pub fn with_publisher(publisher: RevocationPublisher) -> Self {
        Self {
            registry: Arc::new(CancellationRegistry::new()),
            publisher,
        }
    }

    /// The shared registry of in-flight cancellation tokens.
    #[must_use]
    pub fn registry(&self) -> Arc<CancellationRegistry> {
        Arc::clone(&self.registry)
    }

    /// The publisher feeding this watcher (clone to publish signals).
    #[must_use]
    pub fn publisher(&self) -> RevocationPublisher {
        self.publisher.clone()
    }

    /// Register a task as in-flight, returning its cancellation token.
    ///
    /// The worker calls this just before executing a task; the returned token is
    /// installed into the task's [`TaskExecutionContext`].
    pub async fn register(&self, task_id: TaskId) -> CancellationToken {
        self.registry.create_token(task_id).await
    }

    /// Remove a task's token after it finishes (cleanup).
    pub async fn unregister(&self, task_id: &TaskId) {
        self.registry.remove_token(task_id).await;
    }

    /// Apply a single revocation signal directly (without going through the
    /// channel). Returns `true` if a matching in-flight task was found and its
    /// token tripped.
    ///
    /// Exposed primarily for deterministic testing and for callers that already
    /// hold the signal; the running [`spawn`](Self::spawn) loop uses this
    /// internally.
    pub async fn apply(&self, signal: &RevocationSignal) -> bool {
        if !signal.terminate {
            debug!(
                "Ignoring non-terminating revocation for task {} (not aborting running work)",
                signal.task_id
            );
            return false;
        }
        let tripped = self.registry.cancel(&signal.task_id).await;
        if tripped {
            debug!(
                "Tripped cancellation token for in-flight task {}",
                signal.task_id
            );
        } else {
            debug!(
                "Revocation for task {} had no in-flight match",
                signal.task_id
            );
        }
        tripped
    }

    /// Spawn the background watcher loop.
    ///
    /// It subscribes to the publisher and, for each signal, calls
    /// [`apply`](Self::apply). The loop ends when all publishers are dropped.
    /// Returns the [`JoinHandle`] so the worker can abort it on shutdown.
    pub fn spawn(&self) -> JoinHandle<()> {
        let watcher = self.clone();
        let mut rx = self.publisher.subscribe();
        tokio::spawn(async move {
            loop {
                match rx.recv().await {
                    Ok(signal) => {
                        watcher.apply(&signal).await;
                    }
                    Err(broadcast::error::RecvError::Lagged(skipped)) => {
                        // Under heavy revocation bursts a slow watcher may miss
                        // some signals; surface it but keep going.
                        warn!("Revocation watcher lagged, skipped {} signal(s)", skipped);
                    }
                    Err(broadcast::error::RecvError::Closed) => {
                        debug!("Revocation channel closed, stopping watcher");
                        break;
                    }
                }
            }
        })
    }
}

impl Default for RevocationWatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[tokio::test]
    async fn test_scope_installs_token() {
        let token = CancellationToken::new(uuid::Uuid::new_v4());
        let ctx = TaskExecutionContext::new(token.clone());

        // Outside scope there is no ambient context.
        assert!(current_token().is_none());
        assert!(!is_cancelled());
        assert!(check_cancelled().is_ok());

        let observed = ctx
            .scope(async {
                let inner = current_token().expect("token visible inside scope");
                assert_eq!(inner.task_id(), token.task_id());
                assert!(!is_cancelled());
                token.cancel();
                (is_cancelled(), check_cancelled().is_err())
            })
            .await;

        assert_eq!(observed, (true, true));
        // Context removed after scope ends.
        assert!(current_token().is_none());
    }

    #[tokio::test]
    async fn test_cooperative_task_observes_cancellation() {
        let token = CancellationToken::new(uuid::Uuid::new_v4());
        let ctx = TaskExecutionContext::new(token.clone());

        let canceller = token.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            canceller.cancel();
        });

        let iterations = ctx
            .scope(async {
                let mut count = 0u64;
                loop {
                    if is_cancelled() {
                        break;
                    }
                    count += 1;
                    tokio::time::sleep(Duration::from_millis(1)).await;
                    if count > 100_000 {
                        break; // safety valve so the test cannot hang
                    }
                }
                count
            })
            .await;

        assert!(token.is_cancelled());
        assert!(iterations < 100_000, "task should have stopped on cancel");
    }

    #[tokio::test]
    async fn test_watcher_apply_trips_in_flight_token() {
        let watcher = RevocationWatcher::new();
        let task_id = uuid::Uuid::new_v4();
        let token = watcher.register(task_id).await;
        assert!(!token.is_cancelled());

        let tripped = watcher.apply(&RevocationSignal::terminate(task_id)).await;
        assert!(tripped);
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn test_watcher_apply_unrelated_id_does_nothing() {
        let watcher = RevocationWatcher::new();
        let task_id = uuid::Uuid::new_v4();
        let other_id = uuid::Uuid::new_v4();
        let token = watcher.register(task_id).await;

        let tripped = watcher.apply(&RevocationSignal::terminate(other_id)).await;
        assert!(!tripped);
        assert!(
            !token.is_cancelled(),
            "unrelated revocation must not cancel"
        );
    }

    #[tokio::test]
    async fn test_watcher_ignore_signal_does_not_trip() {
        let watcher = RevocationWatcher::new();
        let task_id = uuid::Uuid::new_v4();
        let token = watcher.register(task_id).await;

        let tripped = watcher.apply(&RevocationSignal::ignore(task_id)).await;
        assert!(!tripped);
        assert!(!token.is_cancelled());
    }

    #[tokio::test]
    async fn test_spawned_watcher_trips_token_via_pubsub() {
        let watcher = RevocationWatcher::new();
        let publisher = watcher.publisher();
        let _handle = watcher.spawn();

        let task_id = uuid::Uuid::new_v4();
        let token = watcher.register(task_id).await;

        // Wait until the watcher has actually subscribed so the broadcast is not
        // dropped for having zero receivers.
        for _ in 0..100 {
            if publisher.subscriber_count() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(publisher.subscriber_count() > 0);

        publisher.revoke(task_id);

        // Wait for the watcher to process the signal.
        for _ in 0..200 {
            if token.is_cancelled() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn test_publisher_clone_shares_channel() {
        let watcher = RevocationWatcher::new();
        let _handle = watcher.spawn();
        let p1 = watcher.publisher();
        let p2 = p1.clone();

        let task_id = uuid::Uuid::new_v4();
        let token = watcher.register(task_id).await;

        for _ in 0..100 {
            if p1.subscriber_count() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        // Publishing via the clone reaches the same subscriber.
        let delivered = p2.revoke(task_id);
        assert!(delivered >= 1);

        for _ in 0..200 {
            if token.is_cancelled() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert!(token.is_cancelled());
    }

    #[tokio::test]
    async fn test_unregister_removes_token() {
        let watcher = RevocationWatcher::new();
        let task_id = uuid::Uuid::new_v4();
        watcher.register(task_id).await;
        assert!(watcher.registry().has_token(&task_id).await);
        watcher.unregister(&task_id).await;
        assert!(!watcher.registry().has_token(&task_id).await);
    }
}
