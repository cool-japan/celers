use crate::revocation_channel::RevocationStream;
use crate::{CelersError, Result, SerializedTask, TaskId};
use std::time::Duration;

/// Message envelope for broker operations
#[derive(Debug, Clone)]
pub struct BrokerMessage {
    /// The serialized task
    pub task: SerializedTask,

    /// Receipt handle for acknowledging/rejecting the message
    pub receipt_handle: Option<String>,
}

impl BrokerMessage {
    /// Create a new broker message
    #[must_use]
    pub const fn new(task: SerializedTask) -> Self {
        Self {
            task,
            receipt_handle: None,
        }
    }

    /// Create a new broker message with a receipt handle
    #[must_use]
    pub const fn with_receipt_handle(task: SerializedTask, receipt_handle: String) -> Self {
        Self {
            task,
            receipt_handle: Some(receipt_handle),
        }
    }

    /// Check if message has a receipt handle
    #[inline]
    #[must_use]
    pub const fn has_receipt_handle(&self) -> bool {
        self.receipt_handle.is_some()
    }

    /// Get task ID
    #[inline]
    #[must_use]
    pub const fn task_id(&self) -> crate::TaskId {
        self.task.metadata.id
    }

    /// Get task name
    #[inline]
    #[must_use]
    pub fn task_name(&self) -> &str {
        &self.task.metadata.name
    }

    /// Get task priority
    #[inline]
    #[must_use]
    pub const fn priority(&self) -> i32 {
        self.task.metadata.priority
    }

    /// Whether the message has passed its `expires_at` deadline.
    ///
    /// See [`crate::TaskMetadata::is_expired`]: this is Celery's `expires`, not
    /// the execution time limit, so a message that has merely been queued for a
    /// long time is **not** expired.
    #[inline]
    #[must_use]
    pub fn is_expired(&self) -> bool {
        self.task.is_expired()
    }

    /// Get task age
    #[inline]
    #[must_use]
    pub fn age(&self) -> chrono::Duration {
        self.task.age()
    }
}

impl std::fmt::Display for BrokerMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "BrokerMessage[task={}]", self.task)?;
        if let Some(ref handle) = self.receipt_handle {
            // Take characters, not bytes: byte-slicing a non-ASCII receipt
            // handle would panic inside a `Display` impl (typically reached
            // from logging).
            let prefix: String = handle.chars().take(8).collect();
            write!(f, " receipt={prefix}")?;
        }
        Ok(())
    }
}

/// Core trait for task queue brokers
#[async_trait::async_trait]
pub trait Broker: Send + Sync {
    /// Enqueue a task to the broker
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId>;

    /// Dequeue a task from the broker (blocking/waiting)
    async fn dequeue(&self) -> Result<Option<BrokerMessage>>;

    /// Acknowledge successful processing of a task
    async fn ack(&self, task_id: &TaskId, receipt_handle: Option<&str>) -> Result<()>;

    /// Reject a task and potentially requeue it
    async fn reject(
        &self,
        task_id: &TaskId,
        receipt_handle: Option<&str>,
        requeue: bool,
    ) -> Result<()>;

    /// Get the current queue size
    async fn queue_size(&self) -> Result<usize>;

    /// Cancel a pending task
    async fn cancel(&self, task_id: &TaskId) -> Result<bool>;

    // Revocation (optional, with default implementations)

    /// Revoke a task, saying whether a worker already running it should abort.
    ///
    /// This is [`cancel`](Self::cancel) plus Celery's `terminate` flag. A
    /// broker that keeps a durable revoked-id set records the revocation there
    /// and publishes a
    /// [`RevocationNotice`](crate::revocation_channel::RevocationNotice)
    /// carrying `terminate`, so a worker
    /// subscribed through [`subscribe_revocations`](Self::subscribe_revocations)
    /// can trip the running task's cancellation token.
    ///
    /// The default implementation forwards to [`cancel`](Self::cancel), which
    /// drops `terminate`: a broker with no revocation channel cannot reach a
    /// worker that is already executing the task, and pretending otherwise
    /// would be worse than saying nothing.
    ///
    /// Returns whether the broker found a pending copy of the task. `false`
    /// does **not** mean the revocation was lost — a broker with a persisted
    /// revoked set still records it, which is what
    /// [`is_revoked`](Self::is_revoked) reports afterwards.
    ///
    /// # Errors
    ///
    /// Returns a broker error if the revocation cannot be recorded.
    async fn revoke(&self, task_id: &TaskId, terminate: bool) -> Result<bool> {
        // Deliberately ignored: see the doc comment above.
        let _ = terminate;
        self.cancel(task_id).await
    }

    /// Whether `task_id` is listed in the broker's persisted revoked-id set.
    ///
    /// A worker consults this before executing a message it has just dequeued,
    /// which is what makes revoking a *queued* task work even for a worker that
    /// was not running when the revocation was published.
    ///
    /// The default implementation returns `false` with no I/O: a broker that
    /// keeps no revoked set has nothing to report, and answering `true`
    /// speculatively would drop live tasks.
    ///
    /// # Errors
    ///
    /// Returns a broker error if the set cannot be read. Callers should treat
    /// an error as "not known to be revoked" and log it rather than dropping
    /// the task.
    async fn is_revoked(&self, _task_id: &TaskId) -> Result<bool> {
        Ok(false)
    }

    /// Subscribe to this broker's revocation channel, if it has one.
    ///
    /// `Ok(None)` — the default — means the broker publishes no revocations, so
    /// there is nothing to subscribe to; a worker reports that once rather than
    /// polling a channel that will never carry anything.
    ///
    /// # Errors
    ///
    /// Returns a broker error if the subscription cannot be established.
    async fn subscribe_revocations(&self) -> Result<Option<Box<dyn RevocationStream>>> {
        Ok(None)
    }

    // Batch Operations (optional, with default implementations)

    /// Enqueue multiple tasks in a single operation (batch)
    ///
    /// Default implementation calls `enqueue()` for each task.
    /// Brokers should override this for better performance.
    async fn enqueue_batch(&self, tasks: Vec<SerializedTask>) -> Result<Vec<TaskId>> {
        let mut task_ids = Vec::with_capacity(tasks.len());
        for task in tasks {
            task_ids.push(self.enqueue(task).await?);
        }
        Ok(task_ids)
    }

    /// Whether this broker's [`dequeue`](Broker::dequeue) future may be
    /// **dropped** mid-flight without losing the message it was fetching.
    ///
    /// A cancel-safe `dequeue` never holds a message across an `.await`: every
    /// poll either leaves the message where it was or runs synchronously
    /// through to the return. Only such an implementation may be raced against
    /// another future (a shutdown signal, a timer), because the loser's future
    /// is dropped.
    ///
    /// The default is `false`, which is the only answer that is safe without
    /// having read the implementation. A broker whose `dequeue` removes the
    /// message from the queue at one `.await` point and returns it at a later
    /// one — the shape of nearly every network broker, where the pop is already
    /// committed server-side before the reply is read — would silently lose
    /// messages if it answered `true`.
    ///
    /// [`celers_core::InMemoryBroker`](crate::InMemoryBroker) overrides this to
    /// `true`; see its implementation for what "verified" means here.
    ///
    /// A *decorator* over a cancel-safe broker (a wrapper that records delays,
    /// injects faults, ...) inherits `false` unless it forwards this method to
    /// the broker it wraps — its own `dequeue` may well hold the inner
    /// message across an await.
    fn dequeue_is_cancel_safe(&self) -> bool {
        false
    }

    /// Try to dequeue a task without waiting for one to arrive.
    ///
    /// Returns `Ok(None)` when the broker has no message available *right now*.
    /// This is the non-blocking counterpart of [`Broker::dequeue`]: an
    /// implementation must be a genuine "take it if it is there" primitive
    /// (Redis `LPOP`, AMQP `basic_get`, SQS `ReceiveMessage` with
    /// `WaitTimeSeconds=0`, a `try_lock` over an in-process queue).
    ///
    /// # The default refuses
    ///
    /// There is no way to synthesise a non-blocking dequeue out of a blocking
    /// one. Polling `dequeue()` once and calling a pending poll "empty" — which
    /// this default used to do — is wrong twice over: for any broker that does
    /// I/O the first poll is *always* pending, so it reports an empty queue no
    /// matter how much work is waiting; and dropping that half-polled future
    /// abandons whatever it had already committed to, which for a broker that
    /// pops server-side before reading the reply strands the message until its
    /// visibility timeout expires.
    ///
    /// So the default returns [`CelersError::Broker`] — the same shape of named
    /// refusal [`enqueue_at`](Self::enqueue_at) uses — rather than a plausible
    /// lie. Nothing in this crate's defaults calls it:
    /// [`dequeue_batch`](Self::dequeue_batch) drains with `queue_size` +
    /// `dequeue` instead, so a broker that does not override this keeps working.
    ///
    /// # Errors
    ///
    /// The default implementation always returns [`CelersError::Broker`].
    /// Overrides return whatever their transport reports.
    async fn try_dequeue(&self) -> Result<Option<BrokerMessage>> {
        Err(CelersError::Broker(
            "this broker does not support non-blocking dequeue (try_dequeue)".to_string(),
        ))
    }

    /// Dequeue multiple tasks in a single operation (batch)
    ///
    /// Returns up to `count` messages from the queue.
    ///
    /// # Default implementation
    ///
    /// The first message is waited for with [`Broker::dequeue`]; the rest are
    /// drained by asking [`Broker::queue_size`] whether anything is left and
    /// calling `dequeue` again only when it says yes. That ordering is the
    /// whole design:
    ///
    /// * **No `dequeue` future is ever dropped.** Every one this method creates
    ///   is awaited to completion, so a broker that commits the pop before its
    ///   first `.await` cannot have a message stranded by the batch path.
    /// * **It does not park on an empty queue.** `dequeue` is only called
    ///   speculatively once — for the first message, where waiting is the
    ///   documented behaviour. The drain stops as soon as `queue_size` reports
    ///   nothing, so it returns the messages already in hand instead of
    ///   blocking inside the loop.
    ///
    /// The cost is one `queue_size` round trip per extra message, and a drain
    /// that can only be as accurate as `queue_size` is. **Brokers with a real
    /// batch primitive (`LRANGE`, a multi-row `DELETE ... RETURNING`,
    /// `ReceiveMessage(MaxNumberOfMessages)`) should override this**, and every
    /// broker in this workspace that talks to a network does.
    ///
    /// Two contract notes for anyone relying on the default:
    ///
    /// * `queue_size` must count messages `dequeue` can actually return. A
    ///   broker whose `queue_size` also counts in-flight or not-yet-due
    ///   messages, *and* whose `dequeue` has no block timeout, can park here —
    ///   such a broker must override `dequeue_batch`.
    /// * With several consumers on one queue another may take the message
    ///   between the probe and the `dequeue`. Nothing is lost: that `dequeue`
    ///   waits as long as the broker's own block timeout and then ends the
    ///   drain.
    ///
    /// # Errors
    ///
    /// Propagates a failure of the *first* `dequeue`. Once at least one message
    /// is in hand a later probe or drain failure ends the drain instead of
    /// failing the call: those messages have already left the queue, and
    /// discarding them to report an error on a message that was never taken
    /// would turn a transient fault into lost work.
    async fn dequeue_batch(&self, count: usize) -> Result<Vec<BrokerMessage>> {
        let mut messages = Vec::with_capacity(count.min(64));
        if count == 0 {
            return Ok(messages);
        }
        // The first message may be waited for; the remainder must not block.
        match self.dequeue().await? {
            Some(msg) => messages.push(msg),
            None => return Ok(messages),
        }
        while messages.len() < count {
            // Ask before taking: `dequeue` is allowed to park on an empty
            // queue, so it must never be called speculatively here.
            match self.queue_size().await {
                Ok(0) => break,
                Ok(_) => {}
                Err(e) => {
                    tracing::debug!(
                        error = %e,
                        collected = messages.len(),
                        "queue_size probe failed; returning the batch drained so far"
                    );
                    break;
                }
            }
            match self.dequeue().await {
                Ok(Some(msg)) => messages.push(msg),
                Ok(None) => break,
                Err(e) => {
                    tracing::debug!(
                        error = %e,
                        collected = messages.len(),
                        "dequeue failed mid-drain; returning the batch drained so far"
                    );
                    break;
                }
            }
        }
        Ok(messages)
    }

    /// Return a delivered message to the queue for a later attempt, **without**
    /// spending any of the task's retry budget.
    ///
    /// # Why this is not `reject(requeue = true)`
    ///
    /// A worker refuses a message for two very different reasons, and only one
    /// of them is the task's fault:
    ///
    /// * The task **ran and failed** — that is a retry, and it must count
    ///   against `max_retries` or a permanently failing task loops forever.
    /// * The task **never ran**: this worker cannot route it, its labels do not
    ///   satisfy the task's affinity, a feature flag is off, a rate limiter is
    ///   saturated, a circuit breaker's half-open probe budget is spoken for,
    ///   or the worker is draining. Nothing is known to be wrong with the task.
    ///   Spending a retry here means a task that merely visited the wrong
    ///   worker `max_retries` times is dead-lettered without ever executing.
    ///
    /// `reject(requeue = true)` is the first meaning, and a broker that records
    /// retry state (the Redis broker rewrites the payload to `Retrying(n + 1)`)
    /// implements it that way. `defer` is the second: the message goes back
    /// exactly as delivered.
    ///
    /// # `delay`
    ///
    /// How long the message should stay invisible. A broker with a delayed
    /// queue holds it there and releases it when due; one without releases it
    /// immediately, which is a latency difference rather than a correctness
    /// one. `Duration::ZERO` asks for the ready queue directly.
    ///
    /// `Duration` rather than whole seconds is deliberate: the worker's default
    /// admission deferral is 250 ms, which truncates to zero as an integer
    /// number of seconds.
    ///
    /// # Default implementation
    ///
    /// Forwards to `reject(requeue = true)` and drops the delay. That is the
    /// only universally available way to return a message, and refusing (as
    /// [`enqueue_at`](Self::enqueue_at) does for scheduling) would strand
    /// deferred work on every broker that has not overridden this. The cost is
    /// documented rather than hidden: **on a broker that rewrites retry state
    /// on requeue, the default still spends retry budget**, so such a broker
    /// must override this method. In this workspace `RedisBroker` and
    /// `InMemoryBroker` do.
    ///
    /// # Errors
    ///
    /// Whatever the underlying return-to-queue operation reports.
    async fn defer(
        &self,
        task_id: &TaskId,
        receipt_handle: Option<&str>,
        delay: Duration,
    ) -> Result<()> {
        let _ = delay;
        self.reject(task_id, receipt_handle, true).await
    }

    /// Acknowledge multiple tasks in a single operation (batch)
    ///
    /// Default implementation calls `ack()` for each task.
    async fn ack_batch(&self, tasks: &[(TaskId, Option<String>)]) -> Result<()> {
        for (task_id, receipt_handle) in tasks {
            self.ack(task_id, receipt_handle.as_deref()).await?;
        }
        Ok(())
    }

    // Delayed Task Execution (optional, with default implementations)

    /// Schedule a task for execution at a specific Unix timestamp (seconds)
    ///
    /// # Errors
    ///
    /// The default implementation returns [`CelersError::Broker`] because a
    /// broker that cannot hold the task back must not silently run it *now*:
    /// dropping the schedule would turn "next week" into "immediately" with no
    /// diagnostic. Brokers with scheduling support override this.
    async fn enqueue_at(&self, _task: SerializedTask, _execute_at: i64) -> Result<TaskId> {
        Err(CelersError::Broker(
            "this broker does not support delayed execution (enqueue_at)".to_string(),
        ))
    }

    /// Schedule a task for execution after a delay (seconds)
    ///
    /// # Errors
    ///
    /// The default implementation returns [`CelersError::Broker`]; see
    /// [`Broker::enqueue_at`] for why scheduling is never silently downgraded to
    /// immediate execution. Brokers with scheduling support override this.
    async fn enqueue_after(&self, _task: SerializedTask, _delay_secs: u64) -> Result<TaskId> {
        Err(CelersError::Broker(
            "this broker does not support delayed execution (enqueue_after)".to_string(),
        ))
    }
}

/// Batch utilities for `BrokerMessage` collections
pub mod broker_batch {
    use super::{BrokerMessage, TaskId};
    use std::collections::HashMap;

    /// Sort messages by priority (highest first)
    ///
    /// # Example
    /// ```
    /// use celers_core::{BrokerMessage, SerializedTask, broker::broker_batch};
    ///
    /// let mut messages = vec![
    ///     BrokerMessage::new(SerializedTask::new("task1".to_string(), vec![1]).with_priority(1)),
    ///     BrokerMessage::new(SerializedTask::new("task2".to_string(), vec![2]).with_priority(10)),
    ///     BrokerMessage::new(SerializedTask::new("task3".to_string(), vec![3]).with_priority(5)),
    /// ];
    ///
    /// broker_batch::sort_by_priority(&mut messages);
    /// assert_eq!(messages[0].priority(), 10);
    /// assert_eq!(messages[2].priority(), 1);
    /// ```
    #[inline]
    pub fn sort_by_priority(messages: &mut [BrokerMessage]) {
        messages.sort_by_key(|b| std::cmp::Reverse(b.priority()));
    }

    /// Group messages by task name
    ///
    /// # Example
    /// ```
    /// use celers_core::{BrokerMessage, SerializedTask, broker::broker_batch};
    ///
    /// let messages = vec![
    ///     BrokerMessage::new(SerializedTask::new("task1".to_string(), vec![1])),
    ///     BrokerMessage::new(SerializedTask::new("task2".to_string(), vec![2])),
    ///     BrokerMessage::new(SerializedTask::new("task1".to_string(), vec![3])),
    /// ];
    ///
    /// let grouped = broker_batch::group_by_task_name(&messages);
    /// assert_eq!(grouped.get("task1").unwrap().len(), 2);
    /// assert_eq!(grouped.get("task2").unwrap().len(), 1);
    /// ```
    #[inline]
    #[must_use]
    pub fn group_by_task_name(messages: &[BrokerMessage]) -> HashMap<String, Vec<&BrokerMessage>> {
        let mut map: HashMap<String, Vec<&BrokerMessage>> = HashMap::new();
        for msg in messages {
            map.entry(msg.task_name().to_string())
                .or_default()
                .push(msg);
        }
        map
    }

    /// Filter messages by task name pattern
    ///
    /// # Example
    /// ```
    /// use celers_core::{BrokerMessage, SerializedTask, broker::broker_batch};
    ///
    /// let messages = vec![
    ///     BrokerMessage::new(SerializedTask::new("process_data".to_string(), vec![1])),
    ///     BrokerMessage::new(SerializedTask::new("send_email".to_string(), vec![2])),
    ///     BrokerMessage::new(SerializedTask::new("process_image".to_string(), vec![3])),
    /// ];
    ///
    /// let process_messages = broker_batch::filter_by_name_prefix(&messages, "process");
    /// assert_eq!(process_messages.len(), 2);
    /// ```
    #[inline]
    #[must_use]
    pub fn filter_by_name_prefix<'a>(
        messages: &'a [BrokerMessage],
        prefix: &str,
    ) -> Vec<&'a BrokerMessage> {
        messages
            .iter()
            .filter(|msg| msg.task_name().starts_with(prefix))
            .collect()
    }

    /// Get total payload size of all messages
    ///
    /// # Example
    /// ```
    /// use celers_core::{BrokerMessage, SerializedTask, broker::broker_batch};
    ///
    /// let messages = vec![
    ///     BrokerMessage::new(SerializedTask::new("task1".to_string(), vec![1, 2, 3])),
    ///     BrokerMessage::new(SerializedTask::new("task2".to_string(), vec![4, 5])),
    /// ];
    ///
    /// let total_size = broker_batch::total_payload_size(&messages);
    /// assert_eq!(total_size, 5);
    /// ```
    #[inline]
    #[must_use]
    pub fn total_payload_size(messages: &[BrokerMessage]) -> usize {
        messages.iter().map(|msg| msg.task.payload.len()).sum()
    }

    /// Filter expired messages
    ///
    /// # Example
    /// ```
    /// use celers_core::{BrokerMessage, SerializedTask, broker::broker_batch};
    ///
    /// let messages = vec![
    ///     BrokerMessage::new(SerializedTask::new("task1".to_string(), vec![1])),
    ///     BrokerMessage::new(SerializedTask::new("task2".to_string(), vec![2])),
    /// ];
    ///
    /// let expired = broker_batch::filter_expired(&messages);
    /// // Newly created tasks should not be expired
    /// assert_eq!(expired.len(), 0);
    /// ```
    #[inline]
    #[must_use]
    pub fn filter_expired(messages: &[BrokerMessage]) -> Vec<&BrokerMessage> {
        messages.iter().filter(|msg| msg.is_expired()).collect()
    }

    /// Extract task IDs and receipt handles for batch acknowledgement
    ///
    /// # Example
    /// ```
    /// use celers_core::{BrokerMessage, SerializedTask, broker::broker_batch};
    ///
    /// let messages = vec![
    ///     BrokerMessage::with_receipt_handle(
    ///         SerializedTask::new("task1".to_string(), vec![1]),
    ///         "receipt1".to_string()
    ///     ),
    ///     BrokerMessage::with_receipt_handle(
    ///         SerializedTask::new("task2".to_string(), vec![2]),
    ///         "receipt2".to_string()
    ///     ),
    /// ];
    ///
    /// let ack_data = broker_batch::prepare_ack_batch(&messages);
    /// assert_eq!(ack_data.len(), 2);
    /// ```
    #[inline]
    #[must_use]
    pub fn prepare_ack_batch(messages: &[BrokerMessage]) -> Vec<(TaskId, Option<String>)> {
        messages
            .iter()
            .map(|msg| (msg.task_id(), msg.receipt_handle.clone()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::task::SerializedTask;

    fn create_test_task() -> SerializedTask {
        SerializedTask::new("test_task".to_string(), vec![1, 2, 3])
    }

    #[test]
    fn test_broker_message_new() {
        let task = create_test_task();
        let task_id = task.metadata.id;
        let msg = BrokerMessage::new(task);

        assert_eq!(msg.task_id(), task_id);
        assert_eq!(msg.task_name(), "test_task");
        assert!(!msg.has_receipt_handle());
        assert!(msg.receipt_handle.is_none());
    }

    #[test]
    fn test_broker_message_with_receipt_handle() {
        let task = create_test_task();
        let receipt = "receipt-123456".to_string();
        let msg = BrokerMessage::with_receipt_handle(task, receipt.clone());

        assert!(msg.has_receipt_handle());
        assert_eq!(msg.receipt_handle, Some(receipt));
    }

    #[test]
    fn test_broker_message_task_accessors() {
        let task = create_test_task().with_priority(5);
        let task_id = task.metadata.id;
        let msg = BrokerMessage::new(task);

        assert_eq!(msg.task_id(), task_id);
        assert_eq!(msg.task_name(), "test_task");
        assert_eq!(msg.priority(), 5);
    }

    #[test]
    fn test_broker_message_is_expired() {
        let task = create_test_task();
        let msg = BrokerMessage::new(task);

        // Newly created task should not be expired
        assert!(!msg.is_expired());

        // A message-expiry deadline in the past does expire it.
        let mut stale = create_test_task();
        stale.metadata.expires_at = Some(chrono::Utc::now() - chrono::Duration::seconds(1));
        assert!(BrokerMessage::new(stale).is_expired());

        // An elapsed *execution* time limit does not: the task never started.
        let mut queued = create_test_task();
        queued.metadata.timeout_secs = Some(1);
        queued.metadata.created_at = chrono::Utc::now() - chrono::Duration::hours(1);
        assert!(!BrokerMessage::new(queued).is_expired());
    }

    #[test]
    fn test_broker_message_age() {
        let task = create_test_task();
        let msg = BrokerMessage::new(task);

        // Age should be very small for newly created task
        let age = msg.age();
        assert!(age.num_seconds() < 1);
    }

    #[test]
    fn test_broker_message_display() {
        let task = create_test_task();
        let msg = BrokerMessage::new(task);

        let display = format!("{msg}");
        assert!(display.contains("BrokerMessage"));
        assert!(display.contains("task="));
    }

    #[test]
    fn test_broker_message_display_non_ascii_receipt() {
        // Regression: the receipt prefix used to be byte-sliced, which panics
        // when the 8-byte boundary falls inside a multi-byte character.
        let task = create_test_task();
        let msg = BrokerMessage::with_receipt_handle(task, "日本語のレシート".to_string());
        let display = format!("{msg}");
        assert!(display.contains("receipt=日本語のレシート"));
    }

    /// A minimal broker whose `dequeue` never yields a message, used to prove
    /// the trait defaults neither block nor silently drop a schedule.
    struct BlockingBroker {
        queue: tokio::sync::Mutex<Vec<SerializedTask>>,
    }

    #[async_trait::async_trait]
    impl Broker for BlockingBroker {
        async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
            let id = task.metadata.id;
            self.queue.lock().await.push(task);
            Ok(id)
        }

        async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
            {
                let mut queue = self.queue.lock().await;
                if !queue.is_empty() {
                    return Ok(Some(BrokerMessage::new(queue.remove(0))));
                }
            }
            // Empty: a real broker would wait here. Never resolves.
            std::future::pending::<()>().await;
            unreachable!("pending future never completes")
        }

        async fn ack(&self, _task_id: &TaskId, _receipt_handle: Option<&str>) -> Result<()> {
            Ok(())
        }

        async fn reject(
            &self,
            _task_id: &TaskId,
            _receipt_handle: Option<&str>,
            _requeue: bool,
        ) -> Result<()> {
            Ok(())
        }

        async fn queue_size(&self) -> Result<usize> {
            Ok(self.queue.lock().await.len())
        }

        async fn cancel(&self, _task_id: &TaskId) -> Result<bool> {
            Ok(false)
        }
    }

    /// A broker that takes the message out of its queue *before* its first
    /// `.await`, exactly like a server-side pop whose reply has not been read
    /// yet (a Redis `EVAL`, an SQS receive). A future dropped after that point
    /// takes the staged message with it.
    struct StagingBroker {
        ready: tokio::sync::Mutex<Vec<SerializedTask>>,
        /// Messages popped from `ready` but not yet returned to a caller. A
        /// non-empty `staged` after a completed call means a message was
        /// stranded.
        staged: std::sync::Mutex<Vec<SerializedTask>>,
    }

    impl StagingBroker {
        fn new() -> Self {
            Self {
                ready: tokio::sync::Mutex::new(Vec::new()),
                staged: std::sync::Mutex::new(Vec::new()),
            }
        }

        fn stranded(&self) -> usize {
            self.staged.lock().expect("staged lock").len()
        }
    }

    #[async_trait::async_trait]
    impl Broker for StagingBroker {
        async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
            let id = task.metadata.id;
            self.ready.lock().await.push(task);
            Ok(id)
        }

        async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
            // Commit the pop first...
            let popped = self.ready.lock().await.pop();
            match popped {
                Some(task) => self.staged.lock().expect("staged lock").push(task),
                // Empty: a real broker waits here. Never resolves, so a
                // speculative `dequeue` in a default path shows up as a hung
                // test rather than a silent pass.
                None => {
                    std::future::pending::<()>().await;
                    unreachable!("pending future never completes")
                }
            }
            // ...then await, the way a client awaits the server's reply. A
            // future dropped here loses the staged message.
            tokio::task::yield_now().await;
            let task = self.staged.lock().expect("staged lock").pop();
            Ok(task.map(BrokerMessage::new))
        }

        async fn ack(&self, _task_id: &TaskId, _receipt_handle: Option<&str>) -> Result<()> {
            Ok(())
        }

        async fn reject(
            &self,
            _task_id: &TaskId,
            _receipt_handle: Option<&str>,
            _requeue: bool,
        ) -> Result<()> {
            Ok(())
        }

        async fn queue_size(&self) -> Result<usize> {
            Ok(self.ready.lock().await.len())
        }

        async fn cancel(&self, _task_id: &TaskId) -> Result<bool> {
            Ok(false)
        }
    }

    #[tokio::test]
    async fn default_dequeue_batch_returns_when_queue_drains() {
        // Regression: the default `dequeue_batch` looped on the *blocking*
        // `dequeue`, so it parked forever instead of returning the messages it
        // had already collected.
        let broker = BlockingBroker {
            queue: tokio::sync::Mutex::new(Vec::new()),
        };
        broker.enqueue(create_test_task()).await.unwrap();
        broker.enqueue(create_test_task()).await.unwrap();

        let messages = broker.dequeue_batch(10).await.unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(broker.queue_size().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn default_dequeue_batch_never_strands_a_staged_message() {
        // Regression, and the reason the poll-once default had to go: the
        // drain used to poll a `dequeue()` future exactly once and drop it on
        // `Poll::Pending`. For a broker that commits the pop before its first
        // await — every network broker — that dropped future carried a real
        // message away with it, invisible until the visibility timeout.
        let broker = StagingBroker::new();
        for _ in 0..3 {
            broker.enqueue(create_test_task()).await.unwrap();
        }

        let messages = tokio::time::timeout(Duration::from_secs(5), broker.dequeue_batch(10))
            .await
            .expect("the drain must not park on a queue it has emptied")
            .unwrap();

        assert_eq!(messages.len(), 3, "every queued message must be delivered");
        assert_eq!(broker.stranded(), 0, "a message was left staged in flight");
        assert_eq!(broker.queue_size().await.unwrap(), 0);
    }

    #[tokio::test]
    async fn default_try_dequeue_refuses_instead_of_reporting_a_false_empty() {
        // A blocking `dequeue` cannot be turned into a non-blocking one. The
        // default says so by name rather than answering "nothing available",
        // which for an I/O broker was true exactly never.
        let broker = StagingBroker::new();
        broker.enqueue(create_test_task()).await.unwrap();

        let err = tokio::time::timeout(Duration::from_secs(5), broker.try_dequeue())
            .await
            .expect("the refusal must be immediate")
            .expect_err("the default must not claim the queue is empty");
        assert!(err.is_broker(), "unexpected error kind: {err}");
        assert!(err.to_string().contains("try_dequeue"), "unexpected: {err}");

        // Refusing touched nothing: the message is still queued, not staged.
        assert_eq!(broker.stranded(), 0);
        assert_eq!(broker.queue_size().await.unwrap(), 1);
    }

    #[test]
    fn dequeue_cancel_safety_is_opt_in() {
        // The conservative default is what makes racing a `dequeue` against a
        // shutdown signal safe to enable per broker rather than everywhere.
        let broker = BlockingBroker {
            queue: tokio::sync::Mutex::new(Vec::new()),
        };
        assert!(!broker.dequeue_is_cancel_safe());
        assert!(!StagingBroker::new().dequeue_is_cancel_safe());
        // The one implementation in this crate that has been read and verified.
        assert!(crate::InMemoryBroker::new().dequeue_is_cancel_safe());
    }

    #[tokio::test]
    async fn default_dequeue_batch_stops_at_the_requested_count() {
        let broker = BlockingBroker {
            queue: tokio::sync::Mutex::new(Vec::new()),
        };
        for _ in 0..5 {
            broker.enqueue(create_test_task()).await.unwrap();
        }

        let messages = tokio::time::timeout(Duration::from_secs(5), broker.dequeue_batch(2))
            .await
            .expect("the drain must respect `count` rather than probing forever")
            .unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(broker.queue_size().await.unwrap(), 3);

        // `count == 0` asks for nothing and must not touch the queue at all
        // (in particular it must not park in the first `dequeue`).
        let none = broker.dequeue_batch(0).await.unwrap();
        assert!(none.is_empty());
        assert_eq!(broker.queue_size().await.unwrap(), 3);
    }

    #[tokio::test]
    async fn default_schedule_is_an_error_not_immediate_execution() {
        let broker = BlockingBroker {
            queue: tokio::sync::Mutex::new(Vec::new()),
        };
        let err = broker
            .enqueue_at(create_test_task(), 4_102_444_800)
            .await
            .unwrap_err();
        assert!(err.is_broker());
        let err = broker
            .enqueue_after(create_test_task(), 3600)
            .await
            .unwrap_err();
        assert!(err.is_broker());
        // Crucially, nothing was executed immediately.
        assert_eq!(broker.queue_size().await.unwrap(), 0);
    }

    /// The default `defer` must actually return the message.
    ///
    /// It is the one trait default that deliberately degrades instead of
    /// erroring (unlike `enqueue_at`), because a broker with no delayed queue
    /// can still give a message back — and a `defer` that quietly did nothing
    /// would lose every message a worker refused on admission grounds.
    #[tokio::test]
    async fn default_defer_returns_the_message_through_reject() {
        /// Records the exact `reject` the default `defer` is documented to make.
        struct RecordingBroker {
            rejected: tokio::sync::Mutex<Vec<(TaskId, Option<String>, bool)>>,
        }

        #[async_trait::async_trait]
        impl Broker for RecordingBroker {
            async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
                Ok(task.metadata.id)
            }

            async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
                Ok(None)
            }

            async fn ack(&self, _task_id: &TaskId, _receipt_handle: Option<&str>) -> Result<()> {
                Ok(())
            }

            async fn reject(
                &self,
                task_id: &TaskId,
                receipt_handle: Option<&str>,
                requeue: bool,
            ) -> Result<()> {
                self.rejected.lock().await.push((
                    *task_id,
                    receipt_handle.map(str::to_string),
                    requeue,
                ));
                Ok(())
            }

            async fn queue_size(&self) -> Result<usize> {
                Ok(0)
            }

            async fn cancel(&self, _task_id: &TaskId) -> Result<bool> {
                Ok(false)
            }
        }

        let broker = RecordingBroker {
            rejected: tokio::sync::Mutex::new(Vec::new()),
        };
        let task_id = create_test_task().metadata.id;

        broker
            .defer(&task_id, Some("handle"), Duration::from_secs(5))
            .await
            .unwrap();

        let calls = broker.rejected.lock().await.clone();
        assert_eq!(
            calls,
            vec![(task_id, Some("handle".to_string()), true)],
            "the default must requeue the message, not swallow it"
        );
    }

    #[test]
    fn test_broker_message_display_with_receipt() {
        let task = create_test_task();
        let receipt = "very-long-receipt-handle-12345678901234567890".to_string();
        let msg = BrokerMessage::with_receipt_handle(task, receipt);

        let display = format!("{msg}");
        assert!(display.contains("BrokerMessage"));
        assert!(display.contains("receipt="));
        // Should truncate to 8 characters
        assert!(display.contains("very-lon"));
    }
}
