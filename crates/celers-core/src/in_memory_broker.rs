//! In-memory broker and result backend for local development and testing.
//!
//! This module provides fully-featured, dependency-free, in-process
//! implementations of the [`Broker`](crate::Broker) and
//! [`ResultStore`] traits. They are intended for local
//! development, unit/integration tests, and example code where standing up an
//! external service such as Redis is undesirable.
//!
//! Both types are backed by [`tokio`]-synchronised data structures so they can
//! be shared freely across tasks via [`Arc`](std::sync::Arc) and exercised from
//! concurrent code exactly like a real broker/backend.
//!
//! # Examples
//!
//! ## In-memory broker round-trip
//!
//! ```
//! use celers_core::{Broker, InMemoryBroker, SerializedTask};
//!
//! # async fn example() -> celers_core::Result<()> {
//! let broker = InMemoryBroker::new();
//!
//! let task = SerializedTask::new("send_email".to_string(), vec![1, 2, 3]);
//! let id = broker.enqueue(task).await?;
//!
//! let msg = broker.dequeue().await?.expect("a message is available");
//! assert_eq!(msg.task_id(), id);
//!
//! // Acknowledge with the receipt handle the broker attached on dequeue.
//! broker.ack(&id, msg.receipt_handle.as_deref()).await?;
//! assert_eq!(broker.queue_size().await?, 0);
//! # Ok(())
//! # }
//! # tokio::runtime::Builder::new_current_thread()
//! #     .enable_all()
//! #     .build()
//! #     .unwrap()
//! #     .block_on(example())
//! #     .unwrap();
//! ```
//!
//! ## In-memory result backend
//!
//! ```
//! use celers_core::{InMemoryResultBackend, ResultStore, TaskResultValue};
//! use uuid::Uuid;
//!
//! # async fn example() -> celers_core::Result<()> {
//! let backend = InMemoryResultBackend::new();
//! let id = Uuid::new_v4();
//!
//! backend
//!     .store_result(id, TaskResultValue::Success(serde_json::json!(42)))
//!     .await?;
//! assert!(backend.has_result(id).await?);
//!
//! backend.forget(id).await?;
//! assert!(!backend.has_result(id).await?);
//! # Ok(())
//! # }
//! # tokio::runtime::Builder::new_current_thread()
//! #     .enable_all()
//! #     .build()
//! #     .unwrap()
//! #     .block_on(example())
//! #     .unwrap();
//! ```

use crate::result::{ResultStore, TaskResultValue};
use crate::revocation_channel::{RevocationNotice, RevocationStream};
use crate::state::TaskState;
use crate::{BrokerMessage, CelersError, Result, SerializedTask, TaskId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::sync::{broadcast, Mutex, Semaphore};
use tokio::time::Instant;
use uuid::Uuid;

/// How long an in-memory revocation is remembered by default.
///
/// A revoked-id set has to forget eventually or it grows without bound; an hour
/// is far longer than the lifetime of a queued message in any test or local
/// development run, which is what this broker is for.
pub const DEFAULT_REVOCATION_TTL: Duration = Duration::from_secs(3600);

/// Buffer depth of the in-memory revocation broadcast channel.
///
/// A subscriber that falls this far behind skips the overflow and reports it
/// (`tokio::sync::broadcast`'s contract) rather than blocking the revoker.
const REVOCATION_CHANNEL_CAPACITY: usize = 256;

/// A single entry in the in-memory ready queue.
///
/// Entries are ordered first by descending priority and then by ascending
/// insertion sequence, so that, within a priority class, tasks are delivered in
/// FIFO order (stable ordering).
#[derive(Debug)]
struct QueueEntry {
    /// The serialized task awaiting delivery.
    task: SerializedTask,
    /// Monotonically increasing sequence number used as a FIFO tie-breaker.
    seq: u64,
}

/// A task held back until its scheduled delivery time.
#[derive(Debug)]
struct ScheduledEntry {
    /// The instant at which the task becomes deliverable.
    due_at: Instant,
    /// The queue entry to move into the ready queue when due.
    entry: QueueEntry,
}

/// Internal queue state guarded by a single [`tokio::sync::Mutex`].
#[derive(Debug, Default)]
struct BrokerState {
    /// Tasks that are ready to be delivered, kept sorted on every push.
    ready: Vec<QueueEntry>,
    /// Tasks scheduled for future delivery, kept sorted by ascending due time.
    scheduled: Vec<ScheduledEntry>,
    /// Tasks that have been delivered but not yet acknowledged, keyed by their
    /// receipt handle. Used to support `ack`/`reject` semantics.
    in_flight: HashMap<String, SerializedTask>,
    /// Task IDs that have been requested to be cancelled. A cancelled task is
    /// dropped at dequeue time (for ready tasks) or refused requeue (for
    /// in-flight tasks).
    cancelled: HashMap<TaskId, ()>,
    /// Persisted revoked-id set: task id to the instant its revocation expires.
    ///
    /// This is the in-memory equivalent of `celers_broker_redis`'s
    /// `<queue>:revoked` sorted set. Unlike `cancelled` it survives the task it
    /// refers to — a task revoked *before* it is enqueued is still refused when
    /// it arrives, which is what
    /// [`Broker::is_revoked`](crate::Broker::is_revoked) reports to a worker at
    /// dequeue time.
    revoked: HashMap<TaskId, Instant>,
}

impl BrokerState {
    /// Insert a task into the ready queue, preserving the priority + FIFO
    /// ordering invariant.
    fn push_ready(&mut self, task: SerializedTask, seq: u64) {
        self.push_ready_entry(QueueEntry { task, seq });
    }

    /// Insert an already-built entry into the ready queue.
    fn push_ready_entry(&mut self, entry: QueueEntry) {
        // Find the insertion point that keeps `ready` sorted by descending
        // priority and ascending sequence. `partition_point` gives the first
        // index for which the predicate is false.
        let idx = self.ready.partition_point(|existing| {
            existing.task.metadata.priority > entry.task.metadata.priority
                || (existing.task.metadata.priority == entry.task.metadata.priority
                    && existing.seq < entry.seq)
        });
        self.ready.insert(idx, entry);
    }

    /// Insert a task into the scheduled set, keeping it sorted by due time.
    fn push_scheduled(&mut self, task: SerializedTask, seq: u64, due_at: Instant) {
        let scheduled = ScheduledEntry {
            due_at,
            entry: QueueEntry { task, seq },
        };
        let idx = self
            .scheduled
            .partition_point(|existing| existing.due_at <= scheduled.due_at);
        self.scheduled.insert(idx, scheduled);
    }

    /// Move every scheduled task whose due time has arrived into the ready
    /// queue, returning how many were promoted (one ready permit must be
    /// released per promoted task).
    fn promote_due(&mut self, now: Instant) -> usize {
        let ready_count = self.scheduled.partition_point(|entry| entry.due_at <= now);
        if ready_count == 0 {
            return 0;
        }
        let due: Vec<ScheduledEntry> = self.scheduled.drain(0..ready_count).collect();
        for scheduled in due {
            self.push_ready_entry(scheduled.entry);
        }
        ready_count
    }

    /// The earliest due time among scheduled tasks, if any.
    fn next_due(&self) -> Option<Instant> {
        self.scheduled.first().map(|entry| entry.due_at)
    }

    /// Drop revocations whose lifetime has passed.
    ///
    /// Pruning on every read keeps the set bounded without a sweeper task, the
    /// same way the Redis `revoke` script prunes by score on every call.
    fn prune_revoked(&mut self, now: Instant) {
        if self.revoked.is_empty() {
            return;
        }
        self.revoked.retain(|_, expires_at| *expires_at > now);
    }

    /// Whether `task_id` is currently revoked (expired entries pruned first).
    fn is_revoked(&mut self, task_id: &TaskId, now: Instant) -> bool {
        self.prune_revoked(now);
        self.revoked.contains_key(task_id)
    }
}

/// A fully in-memory implementation of the [`Broker`](crate::Broker) trait.
///
/// This broker keeps all state in process behind a [`tokio::sync::Mutex`] and
/// supports the full broker contract: priority-ordered enqueue/dequeue,
/// receipt-handle based [`ack`](crate::Broker::ack) /
/// [`reject`](crate::Broker::reject) with optional requeue,
/// [`queue_size`](crate::Broker::queue_size), and
/// [`cancel`](crate::Broker::cancel).
///
/// Higher-priority tasks are delivered first; within the same priority tasks are
/// delivered in FIFO (insertion) order. Delayed delivery
/// ([`enqueue_at`](crate::Broker::enqueue_at) /
/// [`enqueue_after`](crate::Broker::enqueue_after)) is supported natively: a
/// scheduled task is held back until its due time and only then becomes
/// deliverable.
///
/// The broker is *not* `Clone`; share it across tasks via
/// `Arc<InMemoryBroker>`, which is what all of the crate's own tests and
/// examples do.
#[derive(Debug)]
pub struct InMemoryBroker {
    /// Shared mutable state.
    state: Mutex<BrokerState>,
    /// One permit per entry pushed onto the ready queue.
    ///
    /// Using a counting semaphore (rather than a `Notify`) makes wakeups
    /// impossible to lose: a permit released by a producer survives even when
    /// no consumer is registered yet, and a batch enqueue releases one permit
    /// per task so *every* waiting consumer can make progress.
    ready_permits: Semaphore,
    /// Monotonic sequence counter for FIFO tie-breaking.
    seq: AtomicU64,
    /// The revocation channel [`Broker::subscribe_revocations`] hands out.
    ///
    /// The broker owns the sender for its whole life, so a subscription only
    /// ends when the broker is dropped.
    revocations: broadcast::Sender<RevocationNotice>,
    /// How long a recorded revocation is remembered.
    revocation_ttl: Duration,
}

impl Default for InMemoryBroker {
    fn default() -> Self {
        Self::new()
    }
}

impl InMemoryBroker {
    /// Create a new, empty in-memory broker.
    #[must_use]
    pub fn new() -> Self {
        let (revocations, _rx) = broadcast::channel(REVOCATION_CHANNEL_CAPACITY);
        Self {
            state: Mutex::new(BrokerState::default()),
            ready_permits: Semaphore::new(0),
            seq: AtomicU64::new(0),
            revocations,
            revocation_ttl: DEFAULT_REVOCATION_TTL,
        }
    }

    /// Set how long a revocation recorded by
    /// [`revoke`](crate::Broker::revoke) / [`cancel`](crate::Broker::cancel) is
    /// remembered.
    ///
    /// The default is [`DEFAULT_REVOCATION_TTL`]. Shorten it to exercise expiry
    /// in a test; a revocation that has expired stops refusing the task, exactly
    /// as an expired entry in the Redis broker's `<queue>:revoked` set does.
    #[must_use]
    pub fn with_revocation_ttl(mut self, ttl: Duration) -> Self {
        self.revocation_ttl = ttl;
        self
    }

    /// How long a recorded revocation is remembered.
    #[must_use]
    pub fn revocation_ttl(&self) -> Duration {
        self.revocation_ttl
    }

    /// Number of unexpired entries in the persisted revoked-id set.
    ///
    /// Expired entries are pruned by this call, so the count is the number of
    /// revocations still in force.
    pub async fn revoked_len(&self) -> usize {
        let mut guard = self.state.lock().await;
        guard.prune_revoked(Instant::now());
        guard.revoked.len()
    }

    /// Number of live subscriptions to this broker's revocation channel.
    ///
    /// Primarily useful for tests: it is how a test can wait until a worker's
    /// revocation bridge has actually attached before publishing.
    #[must_use]
    pub fn revocation_subscriber_count(&self) -> usize {
        self.revocations.receiver_count()
    }

    /// Number of tasks that have been delivered but not yet acknowledged.
    ///
    /// Primarily useful for tests and diagnostics.
    pub async fn in_flight_len(&self) -> usize {
        self.state.lock().await.in_flight.len()
    }

    /// Number of tasks scheduled for future delivery that are not yet due.
    pub async fn scheduled_len(&self) -> usize {
        let mut guard = self.state.lock().await;
        let promoted = guard.promote_due(Instant::now());
        drop(guard);
        self.ready_permits.add_permits(promoted);
        self.state.lock().await.scheduled.len()
    }

    /// Number of live cancellation markers held for in-flight tasks.
    ///
    /// Markers are dropped when the task is acknowledged, rejected, or
    /// re-delivered, so this should stay bounded by the number of outstanding
    /// cancellations. Primarily useful for tests and diagnostics.
    pub async fn cancelled_len(&self) -> usize {
        self.state.lock().await.cancelled.len()
    }

    /// Returns `true` if there are no ready, scheduled, or in-flight tasks.
    pub async fn is_empty(&self) -> bool {
        let guard = self.state.lock().await;
        guard.ready.is_empty() && guard.in_flight.is_empty() && guard.scheduled.is_empty()
    }

    /// Remove every task from the broker (ready, scheduled, in-flight,
    /// cancellation markers and recorded revocations). Mainly intended for
    /// resetting state between tests.
    pub async fn clear(&self) {
        let mut guard = self.state.lock().await;
        guard.ready.clear();
        guard.scheduled.clear();
        guard.in_flight.clear();
        guard.cancelled.clear();
        guard.revoked.clear();
    }

    /// Release `count` ready permits (one per newly deliverable entry).
    fn release_permits(&self, count: usize) {
        if count > 0 {
            self.ready_permits.add_permits(count);
        }
    }

    /// Consume one ready permit, waiting until one is available.
    async fn acquire_permit(&self) -> Result<()> {
        match self.ready_permits.acquire().await {
            Ok(permit) => {
                // The permit is consumed by the dequeue attempt that follows.
                permit.forget();
                Ok(())
            }
            Err(_) => Err(CelersError::Broker(
                "in-memory broker has been shut down".to_string(),
            )),
        }
    }

    /// Consume one ready permit if one happens to be available.
    ///
    /// Every entry that leaves the ready queue balances the permit its enqueue
    /// released. Failing to acquire is harmless (it only leaves a spare permit,
    /// which costs a waiter one extra, immediately-retried loop iteration).
    fn consume_permit_best_effort(&self) {
        if let Ok(permit) = self.ready_permits.try_acquire() {
            permit.forget();
        }
    }

    /// Promote any due scheduled tasks and attempt an immediate dequeue.
    ///
    /// Returns the message when one was available, along with the earliest
    /// pending due time (used by [`Self::dequeue`] to time its wait).
    async fn poll_once(&self) -> (Option<BrokerMessage>, Option<Instant>) {
        let (promoted, removed, message, next_due) = {
            let mut guard = self.state.lock().await;
            let promoted = guard.promote_due(Instant::now());
            let before = guard.ready.len();
            let message = Self::try_dequeue_locked(&mut guard);
            let removed = before - guard.ready.len();
            let next_due = guard.next_due();
            (promoted, removed, message, next_due)
        };
        self.release_permits(promoted);
        for _ in 0..removed {
            self.consume_permit_best_effort();
        }
        (message, next_due)
    }

    /// Convert a delay in seconds into a deadline on the tokio clock.
    fn deadline_after(delay_secs: u64) -> Instant {
        Instant::now() + Duration::from_secs(delay_secs)
    }

    /// Pop the next deliverable ready entry, skipping (and discarding) any
    /// task that has been cancelled. Returns `None` if the ready queue is
    /// empty after skipping cancelled tasks.
    fn pop_deliverable(state: &mut BrokerState) -> Option<SerializedTask> {
        while !state.ready.is_empty() {
            let entry = state.ready.remove(0);
            if state.cancelled.remove(&entry.task.metadata.id).is_some() {
                // Task was cancelled before delivery; drop it and continue.
                continue;
            }
            return Some(entry.task);
        }
        None
    }

    /// Non-blocking dequeue: if a deliverable task is available, move it
    /// in-flight and return a [`BrokerMessage`] for it; otherwise return
    /// `None` immediately without waiting. Used by both [`Self::dequeue`] (which
    /// then waits on a miss) and the batch dequeue (which must never block).
    fn try_dequeue_locked(state: &mut BrokerState) -> Option<BrokerMessage> {
        let task = Self::pop_deliverable(state)?;
        let receipt = Uuid::new_v4().to_string();
        state.in_flight.insert(receipt.clone(), task.clone());
        Some(BrokerMessage::with_receipt_handle(task, receipt))
    }

    /// Remove every pending copy of `task_id`, returning whether the broker
    /// knew about the task at all.
    ///
    /// A ready or not-yet-due task is dropped outright; a task that is already
    /// in flight gets a cancellation marker instead, so a later requeue is
    /// suppressed rather than putting the task back on the queue.
    async fn drop_pending(&self, task_id: &TaskId) -> bool {
        let mut guard = self.state.lock().await;

        // Try to remove the task directly from the ready queue first.
        if let Some(pos) = guard
            .ready
            .iter()
            .position(|entry| entry.task.metadata.id == *task_id)
        {
            guard.ready.remove(pos);
            // Also drop any stale cancellation marker for this id.
            guard.cancelled.remove(task_id);
            drop(guard);
            self.consume_permit_best_effort();
            return true;
        }

        // A task still waiting for its scheduled delivery time can simply be
        // dropped; it never released a ready permit.
        if let Some(pos) = guard
            .scheduled
            .iter()
            .position(|scheduled| scheduled.entry.task.metadata.id == *task_id)
        {
            guard.scheduled.remove(pos);
            guard.cancelled.remove(task_id);
            return true;
        }

        // If the task is currently in flight, record a cancellation marker so
        // that a subsequent requeue is suppressed. Report success because the
        // task is known to the broker.
        let in_flight = guard
            .in_flight
            .values()
            .any(|task| task.metadata.id == *task_id);
        if in_flight {
            guard.cancelled.insert(*task_id, ());
            return true;
        }

        false
    }
}

/// A subscription to an [`InMemoryBroker`]'s revocation channel.
struct InMemoryRevocationStream {
    rx: broadcast::Receiver<RevocationNotice>,
}

#[async_trait::async_trait]
impl RevocationStream for InMemoryRevocationStream {
    async fn recv(&mut self) -> Result<Option<RevocationNotice>> {
        match self.rx.recv().await {
            Ok(notice) => Ok(Some(notice)),
            // Only reachable once the broker itself is dropped.
            Err(broadcast::error::RecvError::Closed) => Ok(None),
            // Dropping revocations silently is how a revoked task keeps
            // running; report it and let the caller keep reading.
            Err(broadcast::error::RecvError::Lagged(skipped)) => Err(CelersError::Other(format!(
                "revocation subscriber lagged, {skipped} notice(s) skipped"
            ))),
        }
    }
}

#[async_trait::async_trait]
impl crate::Broker for InMemoryBroker {
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        let task_id = task.metadata.id;
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        {
            let mut guard = self.state.lock().await;
            guard.push_ready(task, seq);
        }
        // Release one permit per ready entry so no wakeup can be lost.
        self.release_permits(1);
        Ok(task_id)
    }

    async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
        loop {
            let (message, next_due) = self.poll_once().await;
            if let Some(msg) = message {
                return Ok(Some(msg));
            }
            // Nothing deliverable right now. Wait for a producer to release a
            // permit, bounded by the next scheduled task's due time so a
            // delayed task is delivered on time even without new enqueues.
            match next_due {
                Some(deadline) => {
                    // A timeout here simply retries the loop, which promotes
                    // the now-due scheduled task.
                    let _ = tokio::time::timeout_at(deadline, self.acquire_permit()).await;
                }
                None => self.acquire_permit().await?,
            }
        }
    }

    /// A real non-blocking dequeue: [`Self::poll_once`] promotes what is due,
    /// takes a deliverable entry if there is one, and returns either way. It
    /// never waits on [`Self::acquire_permit`], which is the only thing
    /// [`Broker::dequeue`](crate::Broker::dequeue) parks on.
    async fn try_dequeue(&self) -> Result<Option<BrokerMessage>> {
        let (message, _) = self.poll_once().await;
        Ok(message)
    }

    /// Verified cancel-safe: `dequeue` holds no message across an `.await`.
    ///
    /// The only place a message is taken is [`Self::try_dequeue_locked`], a
    /// synchronous function called under the state lock inside
    /// [`Self::poll_once`]. Once that lock await has resolved, `poll_once` runs
    /// straight through to its return — permit bookkeeping included — and
    /// `dequeue` returns the message immediately. There is no poll boundary
    /// between "the message left the ready queue" and "the caller has it", so
    /// dropping the future can never take a message with it.
    ///
    /// A drop while `dequeue` is *waiting* can abandon an already-acquired
    /// readiness permit. The message that permit stood for stays in the queue;
    /// the only cost is that a concurrent consumer may park until the next
    /// enqueue.
    fn dequeue_is_cancel_safe(&self) -> bool {
        true
    }

    async fn ack(&self, _task_id: &TaskId, receipt_handle: Option<&str>) -> Result<()> {
        let Some(handle) = receipt_handle else {
            return Err(CelersError::Broker(
                "in-memory broker ack requires a receipt handle".to_string(),
            ));
        };
        let mut guard = self.state.lock().await;
        let Some(task) = guard.in_flight.remove(handle) else {
            return Err(CelersError::Broker(format!(
                "unknown receipt handle on ack: {handle}"
            )));
        };
        // The task completed, so any cancellation marker recorded while it was
        // in flight is now moot. Dropping it here keeps `cancelled` bounded by
        // the number of *live* cancellations instead of leaking one entry per
        // cancelled-then-acknowledged task.
        guard.cancelled.remove(&task.metadata.id);
        Ok(())
    }

    async fn reject(
        &self,
        _task_id: &TaskId,
        receipt_handle: Option<&str>,
        requeue: bool,
    ) -> Result<()> {
        let Some(handle) = receipt_handle else {
            return Err(CelersError::Broker(
                "in-memory broker reject requires a receipt handle".to_string(),
            ));
        };
        let task = {
            let mut guard = self.state.lock().await;
            match guard.in_flight.remove(handle) {
                Some(task) => task,
                None => {
                    return Err(CelersError::Broker(format!(
                        "unknown receipt handle on reject: {handle}"
                    )));
                }
            }
        };

        if !requeue {
            return Ok(());
        }

        let task_id = task.metadata.id;
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        {
            let mut guard = self.state.lock().await;
            // Honour a cancellation that arrived while the task was in flight.
            if guard.cancelled.remove(&task_id).is_some() {
                return Ok(());
            }
            guard.push_ready(task, seq);
        }
        self.release_permits(1);
        Ok(())
    }

    /// Return the message to the queue with its retry state untouched.
    ///
    /// This broker's [`reject`](Self::reject) already leaves `Retrying(n)`
    /// alone, so the retry-neutrality half of the contract is free here. What
    /// the override buys is the other half: `delay` is honoured through the
    /// same scheduled set [`enqueue_after`](Self::enqueue_after) uses, so a
    /// deferred message is genuinely invisible until it is due instead of being
    /// re-delivered to the very worker that just refused it.
    async fn defer(
        &self,
        _task_id: &TaskId,
        receipt_handle: Option<&str>,
        delay: Duration,
    ) -> Result<()> {
        let Some(handle) = receipt_handle else {
            return Err(CelersError::Broker(
                "in-memory broker defer requires a receipt handle".to_string(),
            ));
        };
        let task = {
            let mut guard = self.state.lock().await;
            match guard.in_flight.remove(handle) {
                Some(task) => task,
                None => {
                    return Err(CelersError::Broker(format!(
                        "unknown receipt handle on defer: {handle}"
                    )));
                }
            }
        };

        let task_id = task.metadata.id;
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        {
            let mut guard = self.state.lock().await;
            // A revocation that landed while the task was in flight outranks
            // the deferral: there is no point holding a slot for work that has
            // been called off. Same rule `reject` applies.
            if guard.cancelled.remove(&task_id).is_some() {
                return Ok(());
            }
            if delay.is_zero() {
                guard.push_ready(task, seq);
            } else {
                guard.push_scheduled(task, seq, Instant::now() + delay);
            }
        }
        // One permit either way: a ready entry is deliverable now, and a
        // scheduled one wakes a parked consumer so it re-arms its wait on the
        // new due time (see `dequeue`).
        self.release_permits(1);
        Ok(())
    }

    async fn queue_size(&self) -> Result<usize> {
        // Promote anything that has come due so the reported size reflects what
        // a `dequeue` would actually deliver right now.
        let promoted = {
            let mut guard = self.state.lock().await;
            guard.promote_due(Instant::now())
        };
        self.release_permits(promoted);
        Ok(self.state.lock().await.ready.len())
    }

    async fn cancel(&self, task_id: &TaskId) -> Result<bool> {
        // A plain cancel is a non-terminating revocation: it stops the task
        // from starting but never aborts a copy that is already running.
        self.revoke(task_id, false).await
    }

    async fn revoke(&self, task_id: &TaskId, terminate: bool) -> Result<bool> {
        // Record first, so a task enqueued between the purge below and the
        // caller's next action is still refused at dequeue time.
        {
            let mut guard = self.state.lock().await;
            let expires_at = Instant::now() + self.revocation_ttl;
            guard.revoked.insert(*task_id, expires_at);
            guard.prune_revoked(Instant::now());
        }

        let found = self.drop_pending(task_id).await;

        // Fire-and-forget, exactly like a broker's Pub/Sub: an error here means
        // nobody is subscribed, which is not a failure to revoke.
        let _ = self
            .revocations
            .send(RevocationNotice::new(*task_id, terminate));

        Ok(found)
    }

    async fn is_revoked(&self, task_id: &TaskId) -> Result<bool> {
        let mut guard = self.state.lock().await;
        Ok(guard.is_revoked(task_id, Instant::now()))
    }

    async fn subscribe_revocations(&self) -> Result<Option<Box<dyn RevocationStream>>> {
        Ok(Some(Box::new(InMemoryRevocationStream {
            rx: self.revocations.subscribe(),
        })))
    }

    async fn enqueue_batch(&self, tasks: Vec<SerializedTask>) -> Result<Vec<TaskId>> {
        if tasks.is_empty() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::with_capacity(tasks.len());
        let pushed = tasks.len();
        {
            let mut guard = self.state.lock().await;
            for task in tasks {
                ids.push(task.metadata.id);
                let seq = self.seq.fetch_add(1, Ordering::Relaxed);
                guard.push_ready(task, seq);
            }
        }
        // One permit per pushed task, so *every* waiting consumer can proceed
        // (a `notify_waiters()`-style wakeup would both miss unregistered
        // waiters and wake at most the currently-parked ones).
        self.release_permits(pushed);
        Ok(ids)
    }

    async fn dequeue_batch(&self, count: usize) -> Result<Vec<BrokerMessage>> {
        // Unlike the trait default (which calls the *blocking* `dequeue` for the
        // first message), this override drains up to `count` immediately-
        // available messages and returns straight away when the queue runs dry,
        // so it never blocks waiting for more tasks to arrive.
        let (promoted, removed, messages) = {
            let mut guard = self.state.lock().await;
            let promoted = guard.promote_due(Instant::now());
            let before = guard.ready.len();
            let mut messages = Vec::with_capacity(count.min(64));
            for _ in 0..count {
                match Self::try_dequeue_locked(&mut guard) {
                    Some(msg) => messages.push(msg),
                    None => break,
                }
            }
            (promoted, before - guard.ready.len(), messages)
        };
        self.release_permits(promoted);
        for _ in 0..removed {
            self.consume_permit_best_effort();
        }
        Ok(messages)
    }

    async fn enqueue_at(&self, task: SerializedTask, execute_at: i64) -> Result<TaskId> {
        let now_unix = chrono::Utc::now().timestamp();
        let delay_secs = execute_at.saturating_sub(now_unix).max(0).unsigned_abs();
        self.enqueue_after(task, delay_secs).await
    }

    async fn enqueue_after(&self, task: SerializedTask, delay_secs: u64) -> Result<TaskId> {
        if delay_secs == 0 {
            return self.enqueue(task).await;
        }
        let task_id = task.metadata.id;
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let due_at = Self::deadline_after(delay_secs);
        {
            let mut guard = self.state.lock().await;
            guard.push_scheduled(task, seq, due_at);
        }
        // Nudge one consumer so a `dequeue` that is parked without a deadline
        // re-evaluates and starts waiting until this task's due time. The extra
        // permit is harmless: a consumer that finds the ready queue empty simply
        // loops and waits again.
        self.release_permits(1);
        Ok(task_id)
    }
}

/// Stored state for a single task result in [`InMemoryResultBackend`].
#[derive(Debug, Clone)]
struct StoredResult {
    /// The most recently stored result value.
    value: TaskResultValue,
}

/// Internal state for [`InMemoryResultBackend`].
#[derive(Debug, Default)]
struct BackendState {
    /// Live results keyed by task ID.
    results: HashMap<TaskId, StoredResult>,
    /// Tombstones recorded for forgotten results.
    tombstones: HashMap<TaskId, crate::ResultTombstone>,
}

/// A fully in-memory implementation of the [`ResultStore`]
/// trait.
///
/// All results are stored in process behind a [`tokio::sync::Mutex`]. The
/// backend implements the complete `ResultStore` contract — `store_result`,
/// `get_result`, `get_state`, `forget`, and `has_result` — and additionally
/// provides native support for the optional tombstone hooks so that a forgotten
/// result can be distinguished from one that never existed.
///
/// The backend is *not* `Clone`; share it across tasks via
/// `Arc<InMemoryResultBackend>`.
#[derive(Debug, Default)]
pub struct InMemoryResultBackend {
    /// Shared mutable state.
    state: Mutex<BackendState>,
}

impl InMemoryResultBackend {
    /// Create a new, empty in-memory result backend.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(BackendState::default()),
        }
    }

    /// Number of live (non-forgotten) results currently stored.
    pub async fn len(&self) -> usize {
        self.state.lock().await.results.len()
    }

    /// Returns `true` if no live results are stored.
    pub async fn is_empty(&self) -> bool {
        self.state.lock().await.results.is_empty()
    }

    /// Remove all results and tombstones. Mainly intended for tests.
    pub async fn clear(&self) {
        let mut guard = self.state.lock().await;
        guard.results.clear();
        guard.tombstones.clear();
    }

    /// Derive the [`TaskState`] that corresponds to a stored result value.
    fn state_for(value: &TaskResultValue) -> TaskState {
        match value {
            TaskResultValue::Pending => TaskState::Pending,
            TaskResultValue::Received => TaskState::Received,
            TaskResultValue::Started => TaskState::Running,
            TaskResultValue::Success(v) => {
                // Best-effort serialization of the success payload into the
                // state's byte buffer; an unserializable value degrades to an
                // empty buffer rather than failing the state query.
                let bytes = serde_json::to_vec(v).unwrap_or_default();
                TaskState::Succeeded(bytes)
            }
            TaskResultValue::Failure { error, .. } => TaskState::Failed(error.clone()),
            TaskResultValue::Revoked => TaskState::Revoked,
            TaskResultValue::Retry { attempt, .. } => TaskState::Retrying(*attempt),
            TaskResultValue::Rejected { .. } => TaskState::Rejected,
            // Terminal and non-failing by contract (see
            // `TaskResultValue::Ignored`): the workflow carries a JSON `null`
            // forward, so the state matches a task that returned nothing.
            TaskResultValue::Ignored { .. } => TaskState::Succeeded(b"null".to_vec()),
        }
    }
}

#[async_trait::async_trait]
impl ResultStore for InMemoryResultBackend {
    async fn store_result(&self, task_id: TaskId, result: TaskResultValue) -> Result<()> {
        let mut guard = self.state.lock().await;
        // Storing a fresh result clears any prior tombstone for that task.
        guard.tombstones.remove(&task_id);
        guard
            .results
            .insert(task_id, StoredResult { value: result });
        Ok(())
    }

    async fn get_result(&self, task_id: TaskId) -> Result<Option<TaskResultValue>> {
        Ok(self
            .state
            .lock()
            .await
            .results
            .get(&task_id)
            .map(|stored| stored.value.clone()))
    }

    async fn get_state(&self, task_id: TaskId) -> Result<TaskState> {
        let guard = self.state.lock().await;
        match guard.results.get(&task_id) {
            Some(stored) => Ok(Self::state_for(&stored.value)),
            None => Ok(TaskState::Pending),
        }
    }

    async fn forget(&self, task_id: TaskId) -> Result<()> {
        self.state.lock().await.results.remove(&task_id);
        Ok(())
    }

    async fn has_result(&self, task_id: TaskId) -> Result<bool> {
        Ok(self.state.lock().await.results.contains_key(&task_id))
    }

    async fn store_tombstone(&self, tombstone: crate::ResultTombstone) -> Result<()> {
        self.state
            .lock()
            .await
            .tombstones
            .insert(tombstone.task_id, tombstone);
        Ok(())
    }

    async fn get_tombstone(&self, task_id: TaskId) -> Result<Option<crate::ResultTombstone>> {
        Ok(self.state.lock().await.tombstones.get(&task_id).cloned())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Broker;
    use serde_json::json;

    fn task(name: &str) -> SerializedTask {
        SerializedTask::new(name.to_string(), vec![1, 2, 3])
    }

    #[tokio::test]
    async fn enqueue_dequeue_ack_round_trip() {
        let broker = InMemoryBroker::new();
        let t = task("a");
        let id = broker.enqueue(t).await.unwrap();
        assert_eq!(broker.queue_size().await.unwrap(), 1);

        let msg = broker.dequeue().await.unwrap().expect("message");
        assert_eq!(msg.task_id(), id);
        assert!(msg.has_receipt_handle());
        // Dequeued task leaves the ready queue but is tracked in flight.
        assert_eq!(broker.queue_size().await.unwrap(), 0);
        assert_eq!(broker.in_flight_len().await, 1);

        broker
            .ack(&id, msg.receipt_handle.as_deref())
            .await
            .unwrap();
        assert_eq!(broker.in_flight_len().await, 0);
        assert!(broker.is_empty().await);
    }

    #[tokio::test]
    async fn ack_without_handle_errors() {
        let broker = InMemoryBroker::new();
        let id = broker.enqueue(task("a")).await.unwrap();
        let _ = broker.dequeue().await.unwrap().unwrap();
        let err = broker.ack(&id, None).await.unwrap_err();
        assert!(err.is_broker());
    }

    #[tokio::test]
    async fn ack_unknown_handle_errors() {
        let broker = InMemoryBroker::new();
        let id = broker.enqueue(task("a")).await.unwrap();
        let err = broker.ack(&id, Some("does-not-exist")).await.unwrap_err();
        assert!(err.is_broker());
    }

    #[tokio::test]
    async fn fifo_order_within_same_priority() {
        let broker = InMemoryBroker::new();
        let id1 = broker.enqueue(task("first")).await.unwrap();
        let id2 = broker.enqueue(task("second")).await.unwrap();
        let id3 = broker.enqueue(task("third")).await.unwrap();

        let m1 = broker.dequeue().await.unwrap().unwrap();
        let m2 = broker.dequeue().await.unwrap().unwrap();
        let m3 = broker.dequeue().await.unwrap().unwrap();
        assert_eq!(m1.task_id(), id1);
        assert_eq!(m2.task_id(), id2);
        assert_eq!(m3.task_id(), id3);
    }

    #[tokio::test]
    async fn higher_priority_delivered_first() {
        let broker = InMemoryBroker::new();
        let low = broker.enqueue(task("low").with_priority(1)).await.unwrap();
        let high = broker
            .enqueue(task("high").with_priority(10))
            .await
            .unwrap();
        let mid = broker.enqueue(task("mid").with_priority(5)).await.unwrap();

        assert_eq!(broker.dequeue().await.unwrap().unwrap().task_id(), high);
        assert_eq!(broker.dequeue().await.unwrap().unwrap().task_id(), mid);
        assert_eq!(broker.dequeue().await.unwrap().unwrap().task_id(), low);
    }

    #[tokio::test]
    async fn priority_then_fifo_combined() {
        let broker = InMemoryBroker::new();
        let a = broker.enqueue(task("a").with_priority(5)).await.unwrap();
        let b = broker.enqueue(task("b").with_priority(5)).await.unwrap();
        let c = broker.enqueue(task("c").with_priority(9)).await.unwrap();

        // c (priority 9) first, then a and b in FIFO order (priority 5).
        assert_eq!(broker.dequeue().await.unwrap().unwrap().task_id(), c);
        assert_eq!(broker.dequeue().await.unwrap().unwrap().task_id(), a);
        assert_eq!(broker.dequeue().await.unwrap().unwrap().task_id(), b);
    }

    #[tokio::test]
    async fn reject_with_requeue_returns_task() {
        let broker = InMemoryBroker::new();
        let id = broker.enqueue(task("a")).await.unwrap();
        let msg = broker.dequeue().await.unwrap().unwrap();

        broker
            .reject(&id, msg.receipt_handle.as_deref(), true)
            .await
            .unwrap();
        // Task should be back on the ready queue.
        assert_eq!(broker.queue_size().await.unwrap(), 1);
        assert_eq!(broker.in_flight_len().await, 0);

        let msg2 = broker.dequeue().await.unwrap().unwrap();
        assert_eq!(msg2.task_id(), id);
    }

    #[tokio::test]
    async fn defer_without_a_delay_returns_the_task_immediately() {
        let broker = InMemoryBroker::new();
        let id = broker.enqueue(task("a")).await.unwrap();
        let msg = broker.dequeue().await.unwrap().unwrap();

        broker
            .defer(&id, msg.receipt_handle.as_deref(), Duration::ZERO)
            .await
            .unwrap();

        // Straight back to the ready queue: nothing is held back.
        assert_eq!(broker.queue_size().await.unwrap(), 1);
        assert_eq!(broker.scheduled_len().await, 0);
        assert_eq!(broker.in_flight_len().await, 0);
        assert_eq!(broker.dequeue().await.unwrap().unwrap().task_id(), id);
    }

    /// A deferral must leave the task's retry accounting exactly as delivered.
    ///
    /// This broker does not rewrite retry state on `reject` either, so what
    /// this pins is the invariant rather than a difference: whichever path a
    /// deferral takes here, the payload that comes back must be the payload
    /// that went in.
    #[tokio::test]
    async fn defer_does_not_advance_retry_state() {
        let broker = InMemoryBroker::new();
        let mut queued = task("a");
        queued.metadata.state = TaskState::Retrying(2);
        let id = broker.enqueue(queued).await.unwrap();

        let msg = broker.dequeue().await.unwrap().unwrap();
        assert_eq!(msg.task.metadata.state, TaskState::Retrying(2));

        broker
            .defer(&id, msg.receipt_handle.as_deref(), Duration::ZERO)
            .await
            .unwrap();

        let again = broker.dequeue().await.unwrap().unwrap();
        assert_eq!(
            again.task.metadata.state,
            TaskState::Retrying(2),
            "a deferral is not an attempt and must not spend retry budget"
        );
    }

    /// The delay is real: a deferred message is invisible until it is due, and
    /// a parked consumer wakes exactly when it comes due.
    #[tokio::test(start_paused = true)]
    async fn defer_with_a_delay_holds_the_task_until_it_is_due() {
        let broker = std::sync::Arc::new(InMemoryBroker::new());
        let id = broker.enqueue(task("deferred")).await.unwrap();
        let msg = broker.dequeue().await.unwrap().unwrap();

        broker
            .defer(&id, msg.receipt_handle.as_deref(), Duration::from_secs(30))
            .await
            .unwrap();

        assert_eq!(broker.in_flight_len().await, 0);
        assert_eq!(broker.scheduled_len().await, 1);
        assert_eq!(
            broker.queue_size().await.unwrap(),
            0,
            "a deferred message must not be deliverable before it is due"
        );
        assert!(broker.try_dequeue().await.unwrap().is_none());

        let consumer = broker.clone();
        let handle = tokio::spawn(async move { consumer.dequeue().await });
        tokio::time::advance(Duration::from_secs(31)).await;

        let redelivered = handle.await.unwrap().unwrap().expect("deferred message");
        assert_eq!(redelivered.task_id(), id);
        assert_eq!(broker.scheduled_len().await, 0);
    }

    /// A revocation that lands while the task is in flight outranks a deferral:
    /// holding a slot for work that has been called off would deliver it again.
    #[tokio::test]
    async fn defer_drops_a_task_cancelled_while_in_flight() {
        let broker = InMemoryBroker::new();
        let id = broker.enqueue(task("a")).await.unwrap();
        let msg = broker.dequeue().await.unwrap().unwrap();
        assert!(broker.cancel(&id).await.unwrap());

        broker
            .defer(&id, msg.receipt_handle.as_deref(), Duration::ZERO)
            .await
            .unwrap();

        assert!(broker.is_empty().await);
    }

    #[tokio::test]
    async fn defer_without_a_receipt_handle_is_an_error() {
        let broker = InMemoryBroker::new();
        let id = broker.enqueue(task("a")).await.unwrap();
        let _msg = broker.dequeue().await.unwrap().unwrap();

        assert!(broker.defer(&id, None, Duration::ZERO).await.is_err());
        assert!(
            broker
                .defer(&id, Some("not-a-handle"), Duration::ZERO)
                .await
                .is_err(),
            "an unknown handle must be reported, not silently ignored"
        );
    }

    #[tokio::test]
    async fn reject_without_requeue_drops_task() {
        let broker = InMemoryBroker::new();
        let id = broker.enqueue(task("a")).await.unwrap();
        let msg = broker.dequeue().await.unwrap().unwrap();

        broker
            .reject(&id, msg.receipt_handle.as_deref(), false)
            .await
            .unwrap();
        assert!(broker.is_empty().await);
    }

    #[tokio::test]
    async fn cancel_ready_task() {
        let broker = InMemoryBroker::new();
        let id = broker.enqueue(task("a")).await.unwrap();
        assert!(broker.cancel(&id).await.unwrap());
        assert_eq!(broker.queue_size().await.unwrap(), 0);
        // Cancelling an unknown task returns false.
        assert!(!broker.cancel(&id).await.unwrap());
    }

    #[tokio::test]
    async fn cancel_records_a_durable_revocation() {
        // The revoked-id set outlives the message: a task revoked before it is
        // enqueued must still be refused when it arrives, which is what the
        // worker's dequeue-time `is_revoked` check reports on.
        let broker = InMemoryBroker::new();
        let id = Uuid::new_v4();
        assert!(!broker.is_revoked(&id).await.unwrap());

        // Nothing queued yet, so no pending copy was found...
        assert!(!broker.cancel(&id).await.unwrap());
        // ...but the revocation is recorded all the same.
        assert!(broker.is_revoked(&id).await.unwrap());
        assert_eq!(broker.revoked_len().await, 1);

        let mut queued = task("a");
        queued.metadata.id = id;
        broker.enqueue(queued).await.unwrap();
        let msg = broker.dequeue().await.unwrap().unwrap();
        assert_eq!(msg.task_id(), id);
        assert!(
            broker.is_revoked(&msg.task_id()).await.unwrap(),
            "a message enqueued after the revocation is still revoked"
        );
    }

    #[tokio::test]
    async fn a_revocation_expires_with_its_ttl() {
        let broker = InMemoryBroker::new().with_revocation_ttl(Duration::from_millis(30));
        assert_eq!(broker.revocation_ttl(), Duration::from_millis(30));

        let id = Uuid::new_v4();
        broker.cancel(&id).await.unwrap();
        assert!(broker.is_revoked(&id).await.unwrap());

        tokio::time::sleep(Duration::from_millis(60)).await;
        assert!(
            !broker.is_revoked(&id).await.unwrap(),
            "an expired revocation must stop refusing the task"
        );
        assert_eq!(broker.revoked_len().await, 0, "expired entries are pruned");
    }

    #[tokio::test]
    async fn revoke_publishes_a_notice_carrying_the_terminate_flag() {
        let broker = InMemoryBroker::new();
        let mut stream = broker
            .subscribe_revocations()
            .await
            .unwrap()
            .expect("the in-memory broker has a revocation channel");
        assert_eq!(broker.revocation_subscriber_count(), 1);

        let terminated = Uuid::new_v4();
        broker.revoke(&terminated, true).await.unwrap();
        let notice = stream.recv().await.unwrap().expect("a notice arrives");
        assert_eq!(notice, RevocationNotice::terminate(terminated));

        // A plain `cancel` is the non-terminating form.
        let ignored = Uuid::new_v4();
        broker.cancel(&ignored).await.unwrap();
        let notice = stream.recv().await.unwrap().expect("a notice arrives");
        assert_eq!(notice, RevocationNotice::ignore(ignored));
    }

    #[tokio::test]
    async fn revoking_with_no_subscriber_is_not_an_error() {
        // Fire-and-forget Pub/Sub semantics: revoking into an empty cluster
        // still records the revocation.
        let broker = InMemoryBroker::new();
        let id = Uuid::new_v4();
        assert_eq!(broker.revocation_subscriber_count(), 0);
        broker.revoke(&id, true).await.unwrap();
        assert!(broker.is_revoked(&id).await.unwrap());
    }

    #[tokio::test]
    async fn clear_forgets_recorded_revocations() {
        let broker = InMemoryBroker::new();
        let id = Uuid::new_v4();
        broker.cancel(&id).await.unwrap();
        broker.clear().await;
        assert!(!broker.is_revoked(&id).await.unwrap());
    }

    #[tokio::test]
    async fn cancel_in_flight_suppresses_requeue() {
        let broker = InMemoryBroker::new();
        let id = broker.enqueue(task("a")).await.unwrap();
        let msg = broker.dequeue().await.unwrap().unwrap();

        // Cancel while in flight.
        assert!(broker.cancel(&id).await.unwrap());
        // Requeue attempt should be suppressed by the cancellation marker.
        broker
            .reject(&id, msg.receipt_handle.as_deref(), true)
            .await
            .unwrap();
        assert!(broker.is_empty().await);
    }

    #[tokio::test]
    async fn dequeue_empty_returns_when_enqueued() {
        let broker = std::sync::Arc::new(InMemoryBroker::new());
        let b2 = broker.clone();
        let handle = tokio::spawn(async move { b2.dequeue().await });

        // Give the waiter a moment to block, then enqueue.
        tokio::task::yield_now().await;
        let id = broker.enqueue(task("late")).await.unwrap();

        let msg = handle.await.unwrap().unwrap().unwrap();
        assert_eq!(msg.task_id(), id);
    }

    #[tokio::test]
    async fn enqueue_batch_and_dequeue_batch() {
        let broker = InMemoryBroker::new();
        let tasks = vec![task("a"), task("b"), task("c")];
        let ids = broker.enqueue_batch(tasks).await.unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(broker.queue_size().await.unwrap(), 3);

        let msgs = broker.dequeue_batch(10).await.unwrap();
        assert_eq!(msgs.len(), 3);
        assert_eq!(broker.queue_size().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn batch_enqueue_wakes_every_waiting_consumer() {
        // Regression: `enqueue_batch` used to call `notify_waiters()`, which
        // stores no permit when no waiter is registered yet, so consumers that
        // had already checked the (empty) queue parked forever with tasks
        // sitting in `ready`. Two consumers + a batch of two must both return.
        let broker = std::sync::Arc::new(InMemoryBroker::new());

        let c1 = broker.clone();
        let c2 = broker.clone();
        let h1 = tokio::spawn(async move { c1.dequeue().await });
        let h2 = tokio::spawn(async move { c2.dequeue().await });

        // Let both consumers reach their (empty) queue check.
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        let ids = broker
            .enqueue_batch(vec![task("a"), task("b")])
            .await
            .unwrap();
        assert_eq!(ids.len(), 2);

        let m1 = h1.await.unwrap().unwrap().expect("first consumer message");
        let m2 = h2.await.unwrap().unwrap().expect("second consumer message");
        let mut got = vec![m1.task_id(), m2.task_id()];
        got.sort();
        let mut expected = ids;
        expected.sort();
        assert_eq!(got, expected);
    }

    #[tokio::test]
    async fn concurrent_single_enqueues_wake_all_consumers() {
        // Two consumers, two separate `enqueue` calls: each enqueue must make a
        // permit available so neither consumer is stranded.
        let broker = std::sync::Arc::new(InMemoryBroker::new());
        let c1 = broker.clone();
        let c2 = broker.clone();
        let h1 = tokio::spawn(async move { c1.dequeue().await });
        let h2 = tokio::spawn(async move { c2.dequeue().await });

        for _ in 0..16 {
            tokio::task::yield_now().await;
        }

        broker.enqueue(task("a")).await.unwrap();
        broker.enqueue(task("b")).await.unwrap();

        assert!(h1.await.unwrap().unwrap().is_some());
        assert!(h2.await.unwrap().unwrap().is_some());
    }

    /// The cancel-safety claim, exercised rather than asserted.
    ///
    /// [`crate::Broker::dequeue_is_cancel_safe`] says this broker's `dequeue`
    /// future may be dropped mid-flight, which is what lets a worker race it
    /// against a shutdown signal. Dropping it must therefore either hand the
    /// message over or leave it in the queue — never neither.
    #[tokio::test(start_paused = true)]
    async fn dropping_a_dequeue_future_never_loses_a_message() {
        let broker = InMemoryBroker::new();
        assert!(broker.dequeue_is_cancel_safe());

        // Dropped while parked on an empty queue: nothing is lost and nothing
        // is invented.
        assert!(tokio::time::timeout(Duration::ZERO, broker.dequeue())
            .await
            .is_err());
        assert!(broker.is_empty().await);

        // With a message queued, the same race must account for it either way.
        let id = broker.enqueue(task("kept")).await.unwrap();
        let taken = match tokio::time::timeout(Duration::ZERO, broker.dequeue()).await {
            Ok(result) => {
                let msg = result.unwrap().expect("a queued message");
                assert_eq!(msg.task_id(), id);
                true
            }
            Err(_elapsed) => false,
        };
        assert_eq!(
            broker.queue_size().await.unwrap() + broker.in_flight_len().await,
            1,
            "the dropped future stranded the message"
        );

        if !taken {
            // Still deliverable to the next consumer, unchanged.
            let msg = broker.dequeue().await.unwrap().expect("still queued");
            assert_eq!(msg.task_id(), id);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn enqueue_after_delays_delivery() {
        let broker = std::sync::Arc::new(InMemoryBroker::new());
        let id = broker.enqueue_after(task("later"), 30).await.unwrap();

        // Not deliverable yet.
        assert_eq!(broker.queue_size().await.unwrap(), 0);
        assert_eq!(broker.scheduled_len().await, 1);
        assert!(broker.try_dequeue().await.unwrap().is_none());
        assert!(!broker.is_empty().await);

        // A blocked consumer wakes up exactly when the task comes due.
        let consumer = broker.clone();
        let handle = tokio::spawn(async move { consumer.dequeue().await });
        tokio::time::advance(Duration::from_secs(31)).await;

        let msg = handle.await.unwrap().unwrap().expect("delayed message");
        assert_eq!(msg.task_id(), id);
        assert_eq!(broker.scheduled_len().await, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn enqueue_at_in_the_past_is_immediate() {
        let broker = InMemoryBroker::new();
        let past = chrono::Utc::now().timestamp() - 60;
        let id = broker.enqueue_at(task("now"), past).await.unwrap();
        assert_eq!(broker.queue_size().await.unwrap(), 1);
        let msg = broker.dequeue().await.unwrap().unwrap();
        assert_eq!(msg.task_id(), id);
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_scheduled_task() {
        let broker = InMemoryBroker::new();
        let id = broker.enqueue_after(task("later"), 60).await.unwrap();
        assert!(broker.cancel(&id).await.unwrap());
        assert_eq!(broker.scheduled_len().await, 0);
        assert!(broker.is_empty().await);
    }

    #[tokio::test]
    async fn ack_clears_cancellation_marker() {
        // Regression: cancelling an in-flight task recorded a marker that `ack`
        // never removed, leaking one entry per cancelled-then-completed task.
        let broker = InMemoryBroker::new();
        let id = broker.enqueue(task("a")).await.unwrap();
        let msg = broker.dequeue().await.unwrap().unwrap();
        assert!(broker.cancel(&id).await.unwrap());

        broker
            .ack(&id, msg.receipt_handle.as_deref())
            .await
            .unwrap();
        assert_eq!(broker.cancelled_len().await, 0);

        // And a fresh enqueue of the same id is delivered (not suppressed by a
        // stale marker).
        let mut again = task("a");
        again.metadata.id = id;
        broker.enqueue(again).await.unwrap();
        let msg2 = broker.dequeue().await.unwrap().expect("redelivered");
        assert_eq!(msg2.task_id(), id);
    }

    #[tokio::test]
    async fn clear_empties_broker() {
        let broker = InMemoryBroker::new();
        broker.enqueue(task("a")).await.unwrap();
        let _ = broker.dequeue().await.unwrap();
        broker.enqueue(task("b")).await.unwrap();
        broker.clear().await;
        assert!(broker.is_empty().await);
    }

    // -------- result backend tests --------

    #[tokio::test]
    async fn backend_store_get_forget() {
        let backend = InMemoryResultBackend::new();
        let id = Uuid::new_v4();
        assert!(!backend.has_result(id).await.unwrap());
        assert!(backend.get_result(id).await.unwrap().is_none());

        backend
            .store_result(id, TaskResultValue::Success(json!({"x": 1})))
            .await
            .unwrap();
        assert!(backend.has_result(id).await.unwrap());
        assert_eq!(backend.len().await, 1);

        let got = backend.get_result(id).await.unwrap().unwrap();
        assert!(got.is_successful());

        backend.forget(id).await.unwrap();
        assert!(!backend.has_result(id).await.unwrap());
        assert!(backend.is_empty().await);
    }

    #[tokio::test]
    async fn backend_get_state_maps_values() {
        let backend = InMemoryResultBackend::new();
        let id = Uuid::new_v4();
        // Unknown task is Pending.
        assert_eq!(backend.get_state(id).await.unwrap(), TaskState::Pending);

        backend
            .store_result(
                id,
                TaskResultValue::Failure {
                    error: "boom".to_string(),
                    traceback: None,
                },
            )
            .await
            .unwrap();
        assert_eq!(
            backend.get_state(id).await.unwrap(),
            TaskState::Failed("boom".to_string())
        );

        backend
            .store_result(
                id,
                TaskResultValue::Retry {
                    attempt: 2,
                    max_retries: 5,
                },
            )
            .await
            .unwrap();
        assert_eq!(backend.get_state(id).await.unwrap(), TaskState::Retrying(2));

        backend
            .store_result(id, TaskResultValue::Success(json!(7)))
            .await
            .unwrap();
        match backend.get_state(id).await.unwrap() {
            TaskState::Succeeded(bytes) => {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
                    json!(7)
                );
            }
            other => panic!("expected Succeeded, got {other:?}"),
        }
    }

    /// A deliberately suppressed failure has to map to a **terminal** state:
    /// anything else (a `Custom` state, or no mapping at all) leaves every
    /// `AsyncResult` waiter polling forever, which is the failure this variant
    /// exists to remove. `null` is the value the workflow carries forward in
    /// its place.
    #[tokio::test]
    async fn backend_get_state_maps_an_ignored_failure_to_a_terminal_state() {
        let backend = InMemoryResultBackend::new();
        let id = Uuid::new_v4();

        backend
            .store_result(
                id,
                TaskResultValue::Ignored {
                    error: "sink unreachable".to_string(),
                },
            )
            .await
            .unwrap();

        let state = backend.get_state(id).await.unwrap();
        assert!(state.is_terminal(), "a waiter must resolve, got {state:?}");
        match state {
            TaskState::Succeeded(bytes) => {
                assert_eq!(
                    serde_json::from_slice::<serde_json::Value>(&bytes).unwrap(),
                    serde_json::Value::Null
                );
            }
            other => panic!("expected Succeeded(null), got {other:?}"),
        }

        // The suppressed error itself is preserved in the stored result, so the
        // suppression stays observable rather than silent.
        let stored = backend.get_result(id).await.unwrap().unwrap();
        assert!(stored.is_ignored());
        assert_eq!(stored.error_message(), Some("sink unreachable"));
    }

    #[tokio::test]
    async fn backend_overwrite_clears_tombstone() {
        let backend = InMemoryResultBackend::new();
        let id = Uuid::new_v4();
        backend
            .store_result(id, TaskResultValue::Success(json!(1)))
            .await
            .unwrap();
        // Forget with tombstone.
        backend
            .forget_with_tombstone(crate::ResultTombstone::new(id))
            .await
            .unwrap();
        assert!(backend.has_tombstone(id).await.unwrap());
        assert!(!backend.has_result(id).await.unwrap());

        // Re-storing clears the tombstone.
        backend
            .store_result(id, TaskResultValue::Success(json!(2)))
            .await
            .unwrap();
        assert!(!backend.has_tombstone(id).await.unwrap());
        assert!(backend.has_result(id).await.unwrap());
    }

    #[tokio::test]
    async fn backend_result_existence_tri_state() {
        use crate::result_tombstone::ResultExistence;
        let backend = InMemoryResultBackend::new();
        let id = Uuid::new_v4();
        // Absent.
        assert!(matches!(
            backend.result_existence(id).await.unwrap(),
            ResultExistence::Absent
        ));
        // Present.
        backend
            .store_result(id, TaskResultValue::Success(json!(1)))
            .await
            .unwrap();
        assert!(matches!(
            backend.result_existence(id).await.unwrap(),
            ResultExistence::Present
        ));
        // Tombstoned.
        backend
            .forget_with_tombstone(crate::ResultTombstone::new(id))
            .await
            .unwrap();
        assert!(matches!(
            backend.result_existence(id).await.unwrap(),
            ResultExistence::Tombstoned(_)
        ));
    }
}
