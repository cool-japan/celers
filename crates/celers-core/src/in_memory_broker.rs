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
use crate::state::TaskState;
use crate::{BrokerMessage, CelersError, Result, SerializedTask, TaskId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::{Mutex, Notify};
use uuid::Uuid;

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

/// Internal queue state guarded by a single [`tokio::sync::Mutex`].
#[derive(Debug, Default)]
struct BrokerState {
    /// Tasks that are ready to be delivered, kept sorted on every push.
    ready: Vec<QueueEntry>,
    /// Tasks that have been delivered but not yet acknowledged, keyed by their
    /// receipt handle. Used to support `ack`/`reject` semantics.
    in_flight: HashMap<String, SerializedTask>,
    /// Task IDs that have been requested to be cancelled. A cancelled task is
    /// dropped at dequeue time (for ready tasks) or refused requeue (for
    /// in-flight tasks).
    cancelled: HashMap<TaskId, ()>,
}

impl BrokerState {
    /// Insert a task into the ready queue, preserving the priority + FIFO
    /// ordering invariant.
    fn push_ready(&mut self, task: SerializedTask, seq: u64) {
        let entry = QueueEntry { task, seq };
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
/// delivered in FIFO (insertion) order.
///
/// The broker is cheap to [`Clone`]; clones share the same underlying queue.
#[derive(Debug, Default)]
pub struct InMemoryBroker {
    /// Shared mutable state.
    state: Mutex<BrokerState>,
    /// Notifier used to wake waiters blocked in [`InMemoryBroker::dequeue`].
    notify: Notify,
    /// Monotonic sequence counter for FIFO tie-breaking.
    seq: AtomicU64,
}

impl InMemoryBroker {
    /// Create a new, empty in-memory broker.
    #[must_use]
    pub fn new() -> Self {
        Self {
            state: Mutex::new(BrokerState::default()),
            notify: Notify::new(),
            seq: AtomicU64::new(0),
        }
    }

    /// Number of tasks that have been delivered but not yet acknowledged.
    ///
    /// Primarily useful for tests and diagnostics.
    pub async fn in_flight_len(&self) -> usize {
        self.state.lock().await.in_flight.len()
    }

    /// Returns `true` if there are no ready and no in-flight tasks.
    pub async fn is_empty(&self) -> bool {
        let guard = self.state.lock().await;
        guard.ready.is_empty() && guard.in_flight.is_empty()
    }

    /// Remove every task from the broker (ready, in-flight, and cancellation
    /// markers). Mainly intended for resetting state between tests.
    pub async fn clear(&self) {
        let mut guard = self.state.lock().await;
        guard.ready.clear();
        guard.in_flight.clear();
        guard.cancelled.clear();
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
        // Wake a single waiter (if any) that may be blocked in `dequeue`.
        self.notify.notify_one();
        Ok(task_id)
    }

    async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
        loop {
            // Register interest *before* checking the queue to avoid a lost
            // wakeup between the check and the await.
            let notified = self.notify.notified();
            {
                let mut guard = self.state.lock().await;
                if let Some(msg) = Self::try_dequeue_locked(&mut guard) {
                    return Ok(Some(msg));
                }
            }
            // Queue empty: wait until something is enqueued, then retry.
            notified.await;
        }
    }

    async fn ack(&self, _task_id: &TaskId, receipt_handle: Option<&str>) -> Result<()> {
        let Some(handle) = receipt_handle else {
            return Err(CelersError::Broker(
                "in-memory broker ack requires a receipt handle".to_string(),
            ));
        };
        let mut guard = self.state.lock().await;
        if guard.in_flight.remove(handle).is_none() {
            return Err(CelersError::Broker(format!(
                "unknown receipt handle on ack: {handle}"
            )));
        }
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
        self.notify.notify_one();
        Ok(())
    }

    async fn queue_size(&self) -> Result<usize> {
        Ok(self.state.lock().await.ready.len())
    }

    async fn cancel(&self, task_id: &TaskId) -> Result<bool> {
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
            return Ok(true);
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
            return Ok(true);
        }

        Ok(false)
    }

    async fn enqueue_batch(&self, tasks: Vec<SerializedTask>) -> Result<Vec<TaskId>> {
        if tasks.is_empty() {
            return Ok(Vec::new());
        }
        let mut ids = Vec::with_capacity(tasks.len());
        {
            let mut guard = self.state.lock().await;
            for task in tasks {
                ids.push(task.metadata.id);
                let seq = self.seq.fetch_add(1, Ordering::Relaxed);
                guard.push_ready(task, seq);
            }
        }
        // Wake potentially several waiters.
        self.notify.notify_waiters();
        Ok(ids)
    }

    async fn dequeue_batch(&self, count: usize) -> Result<Vec<BrokerMessage>> {
        // Unlike the trait default (which calls the *blocking* `dequeue` in a
        // loop), this override drains up to `count` immediately-available
        // messages and returns straight away when the queue runs dry, so it
        // never blocks waiting for more tasks to arrive.
        let mut messages = Vec::with_capacity(count.min(64));
        let mut guard = self.state.lock().await;
        for _ in 0..count {
            match Self::try_dequeue_locked(&mut guard) {
                Some(msg) => messages.push(msg),
                None => break,
            }
        }
        Ok(messages)
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
/// The backend is cheap to [`Clone`]; clones share the same underlying storage.
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
