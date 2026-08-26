// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: Apache-2.0

//! The [`celers_core::Broker`] implementation over a Kombu-style transport.

use async_trait::async_trait;
use celers_core::{BrokerMessage, CelersError, SerializedTask, TaskId};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, error, warn};

use super::convert::{message_to_task, task_to_message};
use super::transport::CoreBrokerTransport;
use crate::{BrokerError, Envelope};

/// How long [`KombuBrokerAdapter::dequeue`] waits for a message by default.
///
/// One second is the smallest value that is not actively wrong on either
/// transport this adapter was built for:
///
/// * SQS resolves `WaitTimeSeconds` in **whole seconds**, so anything below one
///   second becomes a short poll — correct, but one billed `ReceiveMessage` per
///   loop iteration on an idle queue.
/// * Everything above roughly a second is time an `ack` from an already-running
///   task may spend waiting for the transport lock (see the type's docs), and
///   time a `control shutdown` sits unobserved.
///
/// [`KombuBrokerAdapter::with_poll_timeout`] moves it in either direction.
pub const DEFAULT_POLL_TIMEOUT: Duration = Duration::from_secs(1);

/// Translate a transport error into the task-queue error model.
fn to_core_error(error: BrokerError) -> CelersError {
    match error {
        BrokerError::Serialization(message) => CelersError::Serialization(message),
        other => CelersError::Broker(other.to_string()),
    }
}

/// A [`celers_core::Broker`] backed by any Kombu-style transport.
///
/// # What this is for
///
/// `celers_worker::Worker` consumes from [`celers_core::Broker`]
/// (`enqueue`/`dequeue`/`ack`/`reject`/...). The transports in this workspace —
/// `celers_broker_amqp::AmqpBroker`, `celers_broker_sqs::SqsBroker` — implement
/// this crate's [`Broker`](crate::Broker)/[`Producer`](crate::Producer)/
/// [`Consumer`](crate::Consumer) traits instead, which speak
/// `publish`/`consume` over [`celers_protocol::Message`]. Without an adapter
/// those transports cannot feed a worker at all. This is that adapter.
///
/// ```no_run
/// # use celers_kombu::{MockBroker, core_adapter::KombuBrokerAdapter};
/// # async fn example() {
/// // Any transport that opts into `CoreBrokerTransport`:
/// let broker = KombuBrokerAdapter::new(MockBroker::new(), "celery");
/// // ... now usable anywhere a `celers_core::Broker` is expected.
/// # let _ = broker;
/// # }
/// ```
///
/// # One transport, one lock
///
/// [`celers_core::Broker`] takes `&self`; the transport traits take
/// `&mut self`. The adapter therefore owns its transport behind a
/// [`tokio::sync::Mutex`], and **every operation is serialised through it**.
///
/// That is not an implementation detail a caller can ignore. The worker's main
/// loop parks in [`dequeue`](celers_core::Broker::dequeue) holding the lock,
/// while tasks it has already dispatched call
/// [`ack`](celers_core::Broker::ack) from other tasks; those acks wait for the
/// poll to return. If the poll timeout approaches the transport's redelivery
/// window (SQS's visibility timeout, AMQP's consumer cancellation), an
/// in-flight message can be redelivered before its acknowledgement is ever
/// sent, and the task runs twice. Hence the deliberately short
/// [`DEFAULT_POLL_TIMEOUT`], and hence the guidance on
/// [`with_poll_timeout`](Self::with_poll_timeout).
///
/// Sharing one adapter between several workers compounds this: they take turns
/// on one connection rather than consuming in parallel. Give each worker its
/// own transport.
///
/// # Connecting
///
/// Neither transport connects when it is constructed, so the adapter connects
/// **lazily**: every operation checks [`Transport::is_connected`](crate::Transport::is_connected)
/// and calls [`connect`](crate::Transport::connect) first if needed. That also
/// makes it self-healing — a transport that reports itself disconnected after a
/// failure is reconnected on the next call. Call [`connect`](Self::connect)
/// explicitly if you would rather find out about a bad URL at start-up than on
/// the first message.
///
/// # At-least-once delivery
///
/// A message is never removed from the queue by dequeuing it — only by
/// [`ack`](celers_core::Broker::ack) (or by a
/// [`reject`](celers_core::Broker::reject) that does not requeue). A worker
/// that dies mid-task therefore gets the message again once the transport's
/// redelivery window expires, which is the guarantee that makes the queue
/// durable, and the reason a task must tolerate running twice.
///
/// **The adapter does not count deliveries**, and neither transport rewrites a
/// message in place, so a redelivery carries exactly the metadata the producer
/// enqueued: the same id, the same retry state. Retry *accounting* happens in
/// the worker, which acknowledges the message it ran and enqueues a fresh one
/// for the next attempt — so a task that keeps killing its worker is redelivered
/// with an unchanged budget. The backstop for that is the transport's own, and
/// it has to be configured there:
///
/// * **SQS** — a redrive policy with `maxReceiveCount`
///   ([`with_dlq`](https://docs.rs/celers-broker-sqs)), which moves a message to
///   the dead-letter queue after that many receives.
/// * **AMQP** — a dead-letter exchange, plus `x-delivery-limit` on a quorum
///   queue, which does the same for redeliveries.
///
/// # What it does not do
///
/// [`cancel`](celers_core::Broker::cancel) always reports `false`, and the
/// revocation hooks keep the trait's inert defaults: neither AMQP nor SQS can
/// address an individual message that is already queued, so there is no pending
/// copy for this adapter to find. See the method's own documentation.
pub struct KombuBrokerAdapter<T: CoreBrokerTransport> {
    transport: Mutex<T>,
    queue: String,
    poll_timeout: Duration,
}

impl<T: CoreBrokerTransport> KombuBrokerAdapter<T> {
    /// Wrap `transport`, consuming from and publishing to `queue`.
    ///
    /// The transport does not need to be connected yet; see the type's docs.
    pub fn new(transport: T, queue: impl Into<String>) -> Self {
        Self {
            transport: Mutex::new(transport),
            queue: queue.into(),
            poll_timeout: DEFAULT_POLL_TIMEOUT,
        }
    }

    /// Set how long a dequeue waits for a message to arrive.
    ///
    /// Raising it makes an idle worker cheaper (on SQS, a longer
    /// `WaitTimeSeconds` is fewer billed `ReceiveMessage` calls) at the cost of
    /// acknowledgement latency and shutdown responsiveness, because every other
    /// operation waits for the in-flight poll — see the type's docs. Keep it
    /// well below the transport's redelivery window.
    ///
    /// `Duration::ZERO` turns dequeuing into a pure poll, which is only sensible
    /// if something else in the loop provides the pacing.
    #[must_use]
    pub fn with_poll_timeout(mut self, poll_timeout: Duration) -> Self {
        self.poll_timeout = poll_timeout;
        self
    }

    /// The queue this adapter publishes to and consumes from.
    #[must_use]
    pub fn queue(&self) -> &str {
        &self.queue
    }

    /// The current dequeue wait; see [`with_poll_timeout`](Self::with_poll_timeout).
    #[must_use]
    pub fn poll_timeout(&self) -> Duration {
        self.poll_timeout
    }

    /// Connect the underlying transport now instead of on first use.
    ///
    /// # Errors
    ///
    /// Whatever the transport's [`connect`](crate::Transport::connect) reports.
    pub async fn connect(&self) -> celers_core::Result<()> {
        let mut transport = self.transport.lock().await;
        transport.connect().await.map_err(to_core_error)
    }

    /// Whether the underlying transport currently considers itself connected.
    pub async fn is_connected(&self) -> bool {
        self.transport.lock().await.is_connected()
    }

    /// Take the transport back.
    #[must_use]
    pub fn into_transport(self) -> T {
        self.transport.into_inner()
    }

    /// Connect the transport if it is not connected already.
    async fn ensure_connected(transport: &mut T) -> celers_core::Result<()> {
        if transport.is_connected() {
            return Ok(());
        }
        debug!("Transport is not connected; connecting before the next operation");
        transport.connect().await.map_err(to_core_error)
    }

    /// Turn a delivered envelope into a task-queue message.
    ///
    /// A message whose metadata cannot be decoded is **dead-lettered** before
    /// the error is returned: leaving it in flight would have AMQP redeliver it
    /// forever and SQS re-serve it every visibility timeout, so a single
    /// unreadable message would wedge the queue. `reject(requeue = false)` is
    /// what routes it to the dead-letter exchange (AMQP) or lets the redrive
    /// policy take it (SQS).
    async fn into_broker_message(
        transport: &mut T,
        envelope: Envelope,
    ) -> celers_core::Result<BrokerMessage> {
        let delivery_tag = envelope.delivery_tag;
        match message_to_task(envelope.message) {
            Ok(task) => Ok(BrokerMessage::with_receipt_handle(task, delivery_tag)),
            Err(error) => {
                error!(
                    "Dead-lettering undecodable message (tag {}): {}",
                    delivery_tag, error
                );
                if let Err(reject_error) = transport.reject(&delivery_tag, false).await {
                    // Still report the decode failure: the message is the
                    // problem, the failed reject is a second one.
                    warn!(
                        "Could not dead-letter undecodable message (tag {}): {}",
                        delivery_tag, reject_error
                    );
                }
                Err(CelersError::Deserialization(error.to_string()))
            }
        }
    }

    /// The receipt handle an operation needs, or a diagnostic explaining why it
    /// cannot proceed without one.
    ///
    /// Both transports address a delivered message by its delivery tag and
    /// nothing else. Quietly succeeding without one would leave the message in
    /// flight until its redelivery window expired and the task would run again,
    /// so this refuses instead.
    fn require_receipt<'a>(
        &self,
        task_id: &TaskId,
        receipt_handle: Option<&'a str>,
        operation: &str,
    ) -> celers_core::Result<&'a str> {
        receipt_handle.ok_or_else(|| {
            CelersError::Broker(format!(
                "cannot {} task {} on transport queue '{}': no receipt handle. \
                 Messages dequeued through this adapter always carry one; a message \
                 constructed by hand does not",
                operation, task_id, self.queue
            ))
        })
    }
}

impl<T: CoreBrokerTransport> std::fmt::Debug for KombuBrokerAdapter<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KombuBrokerAdapter")
            .field("queue", &self.queue)
            .field("poll_timeout", &self.poll_timeout)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl<T: CoreBrokerTransport> celers_core::Broker for KombuBrokerAdapter<T> {
    async fn enqueue(&self, task: SerializedTask) -> celers_core::Result<TaskId> {
        let task_id = task.metadata.id;
        let message = task_to_message(task).map_err(to_core_error)?;

        let mut transport = self.transport.lock().await;
        Self::ensure_connected(&mut transport).await?;
        transport
            .publish(&self.queue, message)
            .await
            .map_err(to_core_error)?;

        debug!(
            "Enqueued task {} to transport queue {}",
            task_id, self.queue
        );
        Ok(task_id)
    }

    async fn dequeue(&self) -> celers_core::Result<Option<BrokerMessage>> {
        let mut transport = self.transport.lock().await;
        Self::ensure_connected(&mut transport).await?;

        let envelope = transport
            .consume(&self.queue, self.poll_timeout)
            .await
            .map_err(to_core_error)?;

        match envelope {
            None => Ok(None),
            Some(envelope) => Self::into_broker_message(&mut transport, envelope)
                .await
                .map(Some),
        }
    }

    /// Poll once, without ever waiting for a message to arrive.
    ///
    /// Overriding this is mandatory rather than an optimisation. The trait's
    /// default polls the [`dequeue`](celers_core::Broker::dequeue) future once
    /// and **drops it** when it is not ready — which, for a transport that has
    /// already issued an SQS `ReceiveMessage`, throws away messages that are now
    /// in flight and invisible until their visibility timeout expires.
    async fn try_dequeue(&self) -> celers_core::Result<Option<BrokerMessage>> {
        let mut transport = self.transport.lock().await;
        Self::ensure_connected(&mut transport).await?;

        let mut envelopes = transport
            .receive_batch(&self.queue, 1, Duration::ZERO)
            .await
            .map_err(to_core_error)?;

        if envelopes.is_empty() {
            return Ok(None);
        }
        Self::into_broker_message(&mut transport, envelopes.remove(0))
            .await
            .map(Some)
    }

    async fn ack(&self, task_id: &TaskId, receipt_handle: Option<&str>) -> celers_core::Result<()> {
        let tag = self.require_receipt(task_id, receipt_handle, "acknowledge")?;

        let mut transport = self.transport.lock().await;
        Self::ensure_connected(&mut transport).await?;
        transport.ack(tag).await.map_err(to_core_error)?;

        debug!("Acknowledged task {}", task_id);
        Ok(())
    }

    /// Refuse the message: put it back (`requeue`) or let it be dead-lettered.
    ///
    /// `requeue = false` hands the message to whatever the transport does with
    /// a refused delivery — AMQP's dead-letter exchange, SQS's redrive policy —
    /// or drops it if neither is configured.
    ///
    /// Note that neither transport rewrites a message in place, so
    /// `requeue = true` returns it **exactly as delivered**, retry state
    /// included. A broker that records retry state on requeue (the Redis one
    /// rewrites the payload to `Retrying(n + 1)`) makes this the "spend a
    /// retry" operation; here it is not, and retry accounting is entirely the
    /// worker's — which enqueues a fresh attempt and acknowledges the one it
    /// ran. See the type's *At-least-once delivery* section for the backstop
    /// that bounds redeliveries.
    async fn reject(
        &self,
        task_id: &TaskId,
        receipt_handle: Option<&str>,
        requeue: bool,
    ) -> celers_core::Result<()> {
        let tag = self.require_receipt(task_id, receipt_handle, "reject")?;

        let mut transport = self.transport.lock().await;
        Self::ensure_connected(&mut transport).await?;
        transport
            .reject(tag, requeue)
            .await
            .map_err(to_core_error)?;

        debug!("Rejected task {} (requeue: {})", task_id, requeue);
        Ok(())
    }

    /// Return the message without spending any of the task's retry budget.
    ///
    /// Delegates to [`CoreBrokerTransport::return_message`], so a transport
    /// that can hold the message back for `delay` (SQS, via
    /// `ChangeMessageVisibility`) does exactly that, and one that cannot makes
    /// it available again immediately.
    async fn defer(
        &self,
        task_id: &TaskId,
        receipt_handle: Option<&str>,
        delay: Duration,
    ) -> celers_core::Result<()> {
        let tag = self.require_receipt(task_id, receipt_handle, "defer")?;

        let mut transport = self.transport.lock().await;
        Self::ensure_connected(&mut transport).await?;
        transport
            .return_message(tag, delay)
            .await
            .map_err(to_core_error)?;

        debug!("Deferred task {} for {:?}", task_id, delay);
        Ok(())
    }

    async fn queue_size(&self) -> celers_core::Result<usize> {
        let mut transport = self.transport.lock().await;
        Self::ensure_connected(&mut transport).await?;
        transport
            .queue_size(&self.queue)
            .await
            .map_err(to_core_error)
    }

    /// Always `Ok(false)`: neither transport can withdraw a queued message.
    ///
    /// AMQP and SQS both hand a consumer whatever is next in the queue; there is
    /// no operation that addresses one message by id while it is still waiting.
    /// `false` is the trait's own word for "no pending copy was found", and
    /// [`revoke`](celers_core::Broker::revoke) already documents that `false`
    /// does not mean the revocation was lost. Reporting an error instead would
    /// make the defaulted `revoke` fail loudly for a condition the contract
    /// models perfectly well.
    ///
    /// A worker still refuses a revoked task it has *dequeued*, because that
    /// check runs against its own revocation registry rather than the broker.
    async fn cancel(&self, _task_id: &TaskId) -> celers_core::Result<bool> {
        Ok(false)
    }

    /// Publish the whole batch through the transport's native batch API.
    ///
    /// The trait default publishes one message per call; SQS's
    /// `SendMessageBatch` moves ten for the price of one request.
    async fn enqueue_batch(&self, tasks: Vec<SerializedTask>) -> celers_core::Result<Vec<TaskId>> {
        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        let mut task_ids = Vec::with_capacity(tasks.len());
        let mut messages = Vec::with_capacity(tasks.len());
        for task in tasks {
            task_ids.push(task.metadata.id);
            messages.push(task_to_message(task).map_err(to_core_error)?);
        }

        let mut transport = self.transport.lock().await;
        Self::ensure_connected(&mut transport).await?;
        transport
            .send_batch(&self.queue, messages)
            .await
            .map_err(to_core_error)?;

        debug!(
            "Enqueued {} tasks to transport queue {}",
            task_ids.len(),
            self.queue
        );
        Ok(task_ids)
    }

    /// Take a whole batch in as few round trips as the transport allows.
    ///
    /// The trait default calls [`dequeue`](celers_core::Broker::dequeue) once
    /// and then [`try_dequeue`](celers_core::Broker::try_dequeue) `count - 1`
    /// times — one network round trip per message, and on SQS one billed
    /// request per message. This goes through
    /// [`CoreBrokerTransport::receive_batch`] instead, which is a single
    /// `ReceiveMessage` (up to ten messages) on SQS and a `basic.get` drain on
    /// one channel for AMQP.
    ///
    /// An individual undecodable message is dead-lettered and skipped rather
    /// than failing the whole batch; the messages around it are still returned.
    async fn dequeue_batch(&self, count: usize) -> celers_core::Result<Vec<BrokerMessage>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let mut transport = self.transport.lock().await;
        Self::ensure_connected(&mut transport).await?;

        let envelopes = transport
            .receive_batch(&self.queue, count, self.poll_timeout)
            .await
            .map_err(to_core_error)?;

        let mut messages = Vec::with_capacity(envelopes.len());
        for envelope in envelopes {
            match Self::into_broker_message(&mut transport, envelope).await {
                Ok(message) => messages.push(message),
                // Already dead-lettered and logged; the rest of the batch is
                // perfectly good work and must not be dropped with it.
                Err(error) => warn!("Skipping undecodable message in batch: {}", error),
            }
        }

        debug!(
            "Dequeued {} message(s) in one batch from transport queue {}",
            messages.len(),
            self.queue
        );
        Ok(messages)
    }

    /// Acknowledge the whole batch through the transport's native batch API.
    ///
    /// The trait default acks one message per call and stops at the first
    /// failure; SQS's `DeleteMessageBatch` removes ten per request.
    async fn ack_batch(&self, tasks: &[(TaskId, Option<String>)]) -> celers_core::Result<()> {
        if tasks.is_empty() {
            return Ok(());
        }

        let mut tags = Vec::with_capacity(tasks.len());
        for (task_id, receipt_handle) in tasks {
            tags.push(
                self.require_receipt(task_id, receipt_handle.as_deref(), "acknowledge")?
                    .to_string(),
            );
        }

        let mut transport = self.transport.lock().await;
        Self::ensure_connected(&mut transport).await?;
        transport.ack_receipts(&tags).await.map_err(to_core_error)
    }

    /// Schedule a task for an absolute time, if the transport can hold it back.
    ///
    /// # Errors
    ///
    /// Whatever [`CoreBrokerTransport::send_delayed`] reports — by default, that
    /// the transport has no delayed-delivery primitive. A deadline in the past
    /// publishes immediately, which is what it means.
    async fn enqueue_at(
        &self,
        task: SerializedTask,
        execute_at: i64,
    ) -> celers_core::Result<TaskId> {
        // `SystemTime` rather than a `chrono` dependency: this is the only
        // place in the crate that needs a wall clock, and seconds since the
        // epoch is all it needs.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since_epoch| {
                i64::try_from(since_epoch.as_secs()).unwrap_or(i64::MAX)
            });
        let delay_secs = execute_at.saturating_sub(now).max(0);
        self.enqueue_after(task, delay_secs.unsigned_abs()).await
    }

    /// Schedule a task after a delay, if the transport can hold it back.
    ///
    /// # Errors
    ///
    /// Whatever [`CoreBrokerTransport::send_delayed`] reports — by default, that
    /// the transport has no delayed-delivery primitive.
    async fn enqueue_after(
        &self,
        task: SerializedTask,
        delay_secs: u64,
    ) -> celers_core::Result<TaskId> {
        let task_id = task.metadata.id;
        let message = task_to_message(task).map_err(to_core_error)?;

        let mut transport = self.transport.lock().await;
        Self::ensure_connected(&mut transport).await?;
        transport
            .send_delayed(&self.queue, message, Duration::from_secs(delay_secs))
            .await
            .map_err(to_core_error)?;

        debug!(
            "Scheduled task {} on transport queue {} after {}s",
            task_id, self.queue, delay_secs
        );
        Ok(task_id)
    }
}
