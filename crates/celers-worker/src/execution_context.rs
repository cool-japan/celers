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
use crate::checkpoint::{Checkpoint, CheckpointManager, CheckpointStoreError};
use celers_core::time_limit::TimeLimitExceeded;
use celers_core::TaskId;
use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Notify};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

tokio::task_local! {
    /// The cancellation token of the task currently executing on this task,
    /// installed for the duration of [`TaskExecutionContext::scope`].
    static CURRENT_CONTEXT: TaskExecutionContext;
}

/// Cooperative **soft time limit** signal for a running task.
///
/// Celery's soft time limit is deliberately *not* terminal: when it expires the
/// task is told to wrap up (Celery raises `SoftTimeLimitExceeded` inside the
/// task body) while the worker keeps the task running until the *hard* limit
/// kills it. This type is the Rust equivalent of that signal — the worker arms
/// a timer, the timer trips this flag, and cooperative task code observes it
/// through [`soft_time_limit_exceeded`] / [`check_soft_time_limit`] (or by
/// awaiting [`SoftTimeout::expired`]) and returns early with whatever partial
/// work it has.
///
/// Crucially it is a *separate* channel from the [`CancellationToken`]: tripping
/// the token makes the worker treat the task as **revoked** (acked, never
/// retried), which is the wrong disposition for a soft-limit expiry.
///
/// Cloning is cheap and shares the same underlying flag.
///
/// # Example
///
/// ```rust
/// use celers_worker::execution_context::{check_soft_time_limit, TaskExecutionContext, SoftTimeout};
/// use celers_worker::cancellation::CancellationToken;
/// use std::time::Duration;
///
/// # async fn example() {
/// let task_id = uuid::Uuid::new_v4();
/// let soft = SoftTimeout::new(task_id, Some(Duration::from_millis(50)));
/// let ctx = TaskExecutionContext::with_soft_timeout(CancellationToken::new(task_id), soft);
///
/// let outcome = ctx
///     .scope(async {
///         loop {
///             // Cooperative task body: bail out once the soft limit fires.
///             if check_soft_time_limit().is_err() {
///                 return "wrapped up early";
///             }
///             tokio::task::yield_now().await;
///         }
///     })
///     .await;
/// # let _ = outcome;
/// # }
/// ```
#[derive(Clone)]
pub struct SoftTimeout {
    inner: Arc<SoftTimeoutInner>,
}

/// Shared state behind a [`SoftTimeout`].
struct SoftTimeoutInner {
    /// The task the limit belongs to.
    task_id: TaskId,
    /// Configured soft limit in milliseconds; `0` means "no soft limit".
    limit_millis: u64,
    /// Whether the limit has fired.
    expired: AtomicBool,
    /// How long the task had been running when the limit fired.
    elapsed_millis: AtomicU64,
    /// Wakes anything awaiting [`SoftTimeout::expired`].
    notify: Notify,
}

impl SoftTimeout {
    /// Create a soft-timeout signal for `task_id`.
    ///
    /// `limit` of `None` (or `Duration::ZERO`) means no soft limit is
    /// configured: the signal exists but can never fire, so task code observing
    /// it always sees "within limits".
    #[must_use]
    pub fn new(task_id: TaskId, limit: Option<Duration>) -> Self {
        let limit_millis = limit
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0);
        Self {
            inner: Arc::new(SoftTimeoutInner {
                task_id,
                limit_millis,
                expired: AtomicBool::new(false),
                elapsed_millis: AtomicU64::new(0),
                notify: Notify::new(),
            }),
        }
    }

    /// Create a signal for a task with no soft limit configured.
    #[must_use]
    pub fn unlimited(task_id: TaskId) -> Self {
        Self::new(task_id, None)
    }

    /// The task this signal belongs to.
    #[must_use]
    pub fn task_id(&self) -> TaskId {
        self.inner.task_id
    }

    /// The configured soft limit, if any.
    #[must_use]
    pub fn limit(&self) -> Option<Duration> {
        if self.inner.limit_millis == 0 {
            None
        } else {
            Some(Duration::from_millis(self.inner.limit_millis))
        }
    }

    /// Whether a soft limit is configured at all.
    #[must_use]
    pub fn is_configured(&self) -> bool {
        self.inner.limit_millis > 0
    }

    /// Whether the soft limit has already fired.
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.inner.expired.load(Ordering::Acquire)
    }

    /// Trip the signal, recording how long the task had been running.
    ///
    /// Idempotent: returns `true` only for the first call, so the worker emits
    /// exactly one warning per task.
    pub fn expire(&self, elapsed: Duration) -> bool {
        // `notify_waiters` only wakes *currently registered* waiters, so it is
        // paired with the flag check inside `expired()` to avoid lost wakeups.
        if self.is_expired() {
            // Already fired. The reported elapsed time belongs to the moment the
            // limit was *crossed*, so a later redundant call must not overwrite
            // it with a larger, meaningless number.
            self.inner.notify.notify_waiters();
            return false;
        }

        let elapsed_millis = u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX);
        // Publish the elapsed time *before* the flag that makes it readable, so
        // a reader that observes the expiry never sees a zero elapsed.
        self.inner
            .elapsed_millis
            .store(elapsed_millis, Ordering::Release);
        let was_expired = self.inner.expired.swap(true, Ordering::Release);
        self.inner.notify.notify_waiters();
        !was_expired
    }

    /// Wait until the soft limit fires.
    ///
    /// Notification-based (no polling) and safe against lost wakeups: the waiter
    /// is registered before the flag is re-checked, exactly as
    /// [`CancellationToken::cancelled`](crate::cancellation::CancellationToken::cancelled)
    /// does.
    pub async fn expired(&self) {
        loop {
            if self.is_expired() {
                return;
            }
            let notified = self.inner.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if self.is_expired() {
                return;
            }
            notified.await;
        }
    }

    /// The violation, once the soft limit has fired.
    #[must_use]
    pub fn exceeded(&self) -> Option<TimeLimitExceeded> {
        if !self.is_expired() {
            return None;
        }
        Some(TimeLimitExceeded::SoftLimitExceeded {
            task_id: self.inner.task_id.to_string(),
            elapsed_millis: self.inner.elapsed_millis.load(Ordering::Acquire),
            limit_millis: self.inner.limit_millis,
        })
    }

    /// `Err` once the soft limit has fired, so cooperative task code can `?`
    /// this at natural checkpoints.
    pub fn check(&self) -> Result<(), TimeLimitExceeded> {
        match self.exceeded() {
            Some(exceeded) => Err(exceeded),
            None => Ok(()),
        }
    }
}

/// Ambient context describing the task currently being executed.
///
/// Cheaply cloneable. Installed into a task-local for the lifetime of a task
/// future so cooperative task code can reach its [`CancellationToken`] and its
/// [`SoftTimeout`] without having them threaded explicitly through every call.
#[derive(Clone)]
pub struct TaskExecutionContext {
    token: CancellationToken,
    soft_timeout: SoftTimeout,
    checkpoints: Option<Arc<CheckpointManager>>,
}

impl TaskExecutionContext {
    /// Create a new execution context wrapping a cancellation token.
    ///
    /// The context carries an unconfigured [`SoftTimeout`] (no soft time limit);
    /// use [`with_soft_timeout`](Self::with_soft_timeout) to attach one.
    #[must_use]
    pub fn new(token: CancellationToken) -> Self {
        let soft_timeout = SoftTimeout::unlimited(token.task_id());
        Self {
            token,
            soft_timeout,
            checkpoints: None,
        }
    }

    /// Create a context carrying both a cancellation token and a soft
    /// time-limit signal.
    #[must_use]
    pub fn with_soft_timeout(token: CancellationToken, soft_timeout: SoftTimeout) -> Self {
        Self {
            token,
            soft_timeout,
            checkpoints: None,
        }
    }

    /// Attach a checkpoint manager, making it reachable from inside the task
    /// through [`save_checkpoint`] / [`load_checkpoint`].
    #[must_use]
    pub fn with_checkpoints(mut self, checkpoints: Arc<CheckpointManager>) -> Self {
        self.checkpoints = Some(checkpoints);
        self
    }

    /// The checkpoint manager available to this task, if any.
    #[must_use]
    pub fn checkpoints(&self) -> Option<&Arc<CheckpointManager>> {
        self.checkpoints.as_ref()
    }

    /// The cancellation token for this task.
    #[must_use]
    pub fn token(&self) -> &CancellationToken {
        &self.token
    }

    /// The soft time-limit signal for this task.
    #[must_use]
    pub fn soft_timeout(&self) -> &SoftTimeout {
        &self.soft_timeout
    }

    /// Whether this task's soft time limit has fired.
    #[must_use]
    pub fn is_soft_time_limit_exceeded(&self) -> bool {
        self.soft_timeout.is_expired()
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

/// Get the ambient [`SoftTimeout`] for the currently executing task, if any.
#[must_use]
pub fn current_soft_timeout() -> Option<SoftTimeout> {
    CURRENT_CONTEXT
        .try_with(|ctx| ctx.soft_timeout.clone())
        .ok()
}

/// Convenience: whether the currently executing task has passed its **soft**
/// time limit.
///
/// Returns `false` outside a task scope, and for tasks with no soft limit
/// configured.
#[must_use]
pub fn soft_time_limit_exceeded() -> bool {
    CURRENT_CONTEXT
        .try_with(|ctx| ctx.soft_timeout.is_expired())
        .unwrap_or(false)
}

/// Convenience: return the [`TimeLimitExceeded`] violation if the currently
/// executing task has passed its soft time limit, otherwise `Ok(())`.
///
/// This is the Rust equivalent of Celery raising `SoftTimeLimitExceeded` inside
/// the task body: a cooperative task `?`s this at natural checkpoints and gets
/// the chance to clean up before the hard limit kills it. Outside of a task
/// scope this is always `Ok(())`.
pub fn check_soft_time_limit() -> Result<(), TimeLimitExceeded> {
    match CURRENT_CONTEXT.try_with(|ctx| ctx.soft_timeout.clone()) {
        Ok(soft_timeout) => soft_timeout.check(),
        Err(_) => Ok(()),
    }
}

/// Get the ambient [`CheckpointManager`] for the currently executing task, if
/// the worker was built with one.
#[must_use]
pub fn current_checkpoints() -> Option<Arc<CheckpointManager>> {
    CURRENT_CONTEXT
        .try_with(|ctx| ctx.checkpoints.clone())
        .ok()
        .flatten()
}

/// Save a checkpoint for the currently executing task.
///
/// A long-running task calls this at its own natural progress boundaries; the
/// worker deletes the task's checkpoints once it completes successfully, and
/// [`load_checkpoint`] hands them back when a retry (or a restart) picks the
/// task up again — so work already done is not repeated.
///
/// Returns `Ok(false)` when no checkpoint manager is installed or there is no
/// ambient task context, so task code can call it unconditionally.
///
/// # Errors
///
/// Returns the storage backend's error when the checkpoint cannot be written.
///
/// # Example
///
/// ```no_run
/// # use celers_worker::execution_context::{load_checkpoint, save_checkpoint};
/// # async fn process(from: usize) -> Vec<u8> { Vec::new() }
/// # async fn task_body() {
/// // Resume where the previous attempt stopped.
/// let resume_from = load_checkpoint()
///     .await
///     .and_then(|cp| String::from_utf8(cp.data).ok())
///     .and_then(|s| s.parse::<usize>().ok())
///     .unwrap_or(0);
///
/// for step in resume_from..1_000 {
///     let _ = process(step).await;
///     let _ = save_checkpoint(step.to_string().into_bytes()).await;
/// }
/// # }
/// ```
pub async fn save_checkpoint(data: Vec<u8>) -> Result<bool, CheckpointStoreError> {
    let Some(ctx) = current_context() else {
        return Ok(false);
    };
    let Some(manager) = ctx.checkpoints else {
        return Ok(false);
    };
    manager
        .save_checkpoint(Checkpoint::new(ctx.token.task_id().to_string(), data))
        .await?;
    Ok(true)
}

/// Load the most recent checkpoint saved for the currently executing task.
///
/// Returns `None` when no checkpoint manager is installed, when there is no
/// ambient task context, or when the task has never checkpointed.
#[must_use]
pub async fn load_checkpoint() -> Option<Checkpoint> {
    let ctx = current_context()?;
    let manager = ctx.checkpoints?;
    manager
        .load_checkpoint(&ctx.token.task_id().to_string())
        .await
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

    // ----------------------------------------------------------------------
    // Soft time limit (idx 42)
    // ----------------------------------------------------------------------

    #[test]
    fn test_soft_timeout_without_a_limit_never_fires() {
        let signal = SoftTimeout::unlimited(uuid::Uuid::new_v4());
        assert!(!signal.is_configured());
        assert_eq!(signal.limit(), None);
        assert!(!signal.is_expired());
        assert!(signal.exceeded().is_none());
        assert!(signal.check().is_ok());
    }

    #[test]
    fn test_soft_timeout_expiry_is_idempotent_and_reports_the_violation() {
        let task_id = uuid::Uuid::new_v4();
        let signal = SoftTimeout::new(task_id, Some(Duration::from_millis(250)));
        assert!(signal.is_configured());
        assert_eq!(signal.limit(), Some(Duration::from_millis(250)));

        assert!(
            signal.expire(Duration::from_millis(300)),
            "the first expiry is the one that warns"
        );
        assert!(
            !signal.expire(Duration::from_millis(400)),
            "a second expiry must not warn again"
        );

        let exceeded = signal.check().expect_err("the limit has fired");
        assert!(matches!(
            exceeded,
            celers_core::time_limit::TimeLimitExceeded::SoftLimitExceeded { .. }
        ));
        assert!(!exceeded.is_hard(), "a soft limit is never the hard one");
        assert_eq!(exceeded.task_id(), task_id.to_string());
        assert_eq!(exceeded.limit(), Duration::from_millis(250));
        assert_eq!(exceeded.elapsed(), Duration::from_millis(300));
    }

    #[test]
    fn test_soft_timeout_clones_share_one_flag() {
        let signal = SoftTimeout::new(uuid::Uuid::new_v4(), Some(Duration::from_secs(1)));
        let observer = signal.clone();
        assert!(!observer.is_expired());
        signal.expire(Duration::from_secs(2));
        assert!(observer.is_expired());
    }

    #[tokio::test]
    async fn test_awaiting_expiry_wakes_and_never_misses_an_early_fire() {
        let signal = SoftTimeout::new(uuid::Uuid::new_v4(), Some(Duration::from_millis(10)));

        // Already expired before the await: returns immediately.
        signal.expire(Duration::from_millis(11));
        tokio::time::timeout(Duration::from_secs(1), signal.expired())
            .await
            .expect("an already-expired signal must not park");

        // And a waiter registered first is woken by a later expiry.
        let signal = SoftTimeout::new(uuid::Uuid::new_v4(), Some(Duration::from_millis(10)));
        let waiter = signal.clone();
        let firing = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(20)).await;
            signal.expire(Duration::from_millis(20));
        });
        tokio::time::timeout(Duration::from_secs(2), waiter.expired())
            .await
            .expect("the waiter must be woken");
        firing.await.expect("firing task");
    }

    #[tokio::test]
    async fn test_soft_limit_is_visible_inside_the_task_scope_only() {
        let task_id = uuid::Uuid::new_v4();
        let signal = SoftTimeout::new(task_id, Some(Duration::from_millis(5)));
        let ctx = TaskExecutionContext::with_soft_timeout(
            CancellationToken::new(task_id),
            signal.clone(),
        );

        // Outside a scope the helpers are inert.
        assert!(current_soft_timeout().is_none());
        assert!(!soft_time_limit_exceeded());
        assert!(check_soft_time_limit().is_ok());

        let seen = ctx
            .scope(async {
                let before = (soft_time_limit_exceeded(), check_soft_time_limit().is_ok());
                signal.expire(Duration::from_millis(6));
                let after = (soft_time_limit_exceeded(), check_soft_time_limit().is_err());
                assert!(current_soft_timeout().is_some());
                (before, after)
            })
            .await;

        assert_eq!(seen, ((false, true), (true, true)));
        assert!(ctx.is_soft_time_limit_exceeded());
        // The cancellation token is a *separate* channel: a soft limit must
        // never make the worker treat the task as revoked.
        assert!(!ctx.token().is_cancelled());
    }

    #[test]
    fn test_plain_context_carries_an_unconfigured_soft_timeout() {
        let task_id = uuid::Uuid::new_v4();
        let ctx = TaskExecutionContext::new(CancellationToken::new(task_id));
        assert!(!ctx.soft_timeout().is_configured());
        assert_eq!(ctx.soft_timeout().task_id(), task_id);
        assert!(!ctx.is_soft_time_limit_exceeded());
        assert!(ctx.checkpoints().is_none());
    }
}
