// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: Apache-2.0

//! Running a `celers_worker::Worker` against Amazon SQS.
//!
//! [`SqsBroker`] speaks the `celers-kombu` transport traits
//! (`publish`/`consume`/`purge`/...). A worker consumes
//! [`celers_core::Broker`] (`enqueue`/`dequeue`/`ack`/`reject`/...). This
//! module is the join: it teaches `SqsBroker` the
//! [`CoreBrokerTransport`](celers_kombu::core_adapter::CoreBrokerTransport) seam
//! and re-exports the resulting task-queue broker as [`SqsCoreBroker`].
//!
//! ```no_run
//! use celers_broker_sqs::SqsBroker;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let broker = SqsBroker::new("celers-tasks")
//!     .await?
//!     .with_max_messages(10)
//!     .into_core_broker("celers-tasks");
//! // `broker` is now a `celers_core::Broker`; hand it to `Worker::new`.
//! # Ok(())
//! # }
//! ```
//!
//! # What SQS lends the adapter
//!
//! Three of the task-queue operations map onto an SQS API exactly, and the
//! adapter uses each of them rather than the generic fallback:
//!
//! | Task-queue operation | SQS call | Why it matters |
//! |---|---|---|
//! | `dequeue_batch` | `ReceiveMessage` with `MaxNumberOfMessages` up to 10 | one billed request instead of ten |
//! | `enqueue_batch` / `ack_batch` | `SendMessageBatch` / `DeleteMessageBatch` | same, in the other direction |
//! | `defer(delay)` | `ChangeMessageVisibility` | gives the message back **without** spending retry budget, and holds it for exactly the delay asked for |
//! | `enqueue_after(delay)` | `SendMessage` with `DelaySeconds` | real broker-side scheduling, up to SQS's 15 minute cap |
//!
//! Available with the `core-broker` feature, which is on by default.

use async_trait::async_trait;
use celers_kombu::core_adapter::{CoreBrokerTransport, KombuBrokerAdapter};
use celers_kombu::{BrokerError, Consumer, Envelope, Result};
use celers_protocol::Message;
use std::time::Duration;

use crate::broker_core::SqsBroker;
use crate::delivery::{resolve_wait_time, MAX_RECEIVE_MESSAGES};

/// Longest `DelaySeconds` the SQS `SendMessage` API accepts (15 minutes).
pub const MAX_DELAY_SECONDS: u64 = 900;

/// Longest visibility timeout SQS accepts (12 hours), and therefore the longest
/// a [`defer`](celers_core::Broker::defer) can hold a message back.
pub const MAX_VISIBILITY_DELAY_SECONDS: u64 = 43_200;

/// A [`celers_core::Broker`] backed by Amazon SQS.
///
/// Build one with [`SqsBroker::into_core_broker`]. See [`KombuBrokerAdapter`]
/// for the lock, poll-timeout and lazy-connect behaviour every transport
/// adapter shares — on SQS the poll timeout is also what decides
/// `WaitTimeSeconds`, so it is the knob that trades long-polling savings
/// against acknowledgement latency.
pub type SqsCoreBroker = KombuBrokerAdapter<SqsBroker>;

impl SqsBroker {
    /// Turn this transport into a worker-usable [`celers_core::Broker`].
    ///
    /// `queue` becomes the queue the resulting broker publishes to and consumes
    /// from — **and** this transport's own queue name, which is what
    /// [`connect`](celers_kombu::Transport::connect) resolves a queue URL for
    /// and what `ack`/`reject` fall back to for a delivery tag that carries no
    /// source queue. Keeping the two in step is deliberate: a transport
    /// pointed at one queue while the adapter consumed another would fail every
    /// operation at connect time, on a queue the caller never mentioned.
    ///
    /// Set [`with_max_messages`](SqsBroker::with_max_messages) to 10 first if
    /// the worker will use batch dequeue: it is the ceiling on how many
    /// messages one `ReceiveMessage` may return.
    ///
    /// No AWS client is built here, so this call performs no I/O.
    #[must_use]
    pub fn into_core_broker(mut self, queue: impl Into<String>) -> SqsCoreBroker {
        let queue = queue.into();
        self.queue_name.clone_from(&queue);
        KombuBrokerAdapter::new(self, queue)
    }

    /// Hold a delivered message back for `delay` without deleting it.
    ///
    /// Shared by [`CoreBrokerTransport::return_message`]; kept here so the
    /// heartbeat and metadata bookkeeping stays next to the other
    /// message-releasing paths (`ack_on`, `reject_on`) rather than drifting
    /// from them.
    async fn hide_message_for(&mut self, delivery_tag: &str, delay: Duration) -> Result<()> {
        // This worker is giving the message up, so nothing here may keep it
        // invisible any longer, and the receive metadata belongs to a delivery
        // that is over (a redelivery gets a fresh receipt handle).
        self.stop_visibility_heartbeat(delivery_tag);

        let seconds = delay.as_secs().min(MAX_VISIBILITY_DELAY_SECONDS);
        let seconds = i32::try_from(seconds).unwrap_or(i32::MAX);
        self.extend_visibility(delivery_tag, seconds).await?;

        self.forget_receipt_metadata(delivery_tag);
        Ok(())
    }
}

#[async_trait]
impl CoreBrokerTransport for SqsBroker {
    /// One `ReceiveMessage` per queue instead of one per message.
    ///
    /// SQS returns up to ten messages for the price of a single request, which
    /// is the whole reason batch dequeue exists on this transport: the generic
    /// fallback would issue `max_messages` separate requests and be billed for
    /// every one of them.
    ///
    /// With Celery priority queues enabled the sibling queues are drained
    /// highest-priority first — the same order [`Consumer::consume`] scans them
    /// in — so enabling batch dequeue cannot quietly starve the high-priority
    /// queue. Only the lowest-priority queue may long-poll, and only while the
    /// batch is still empty: once there is work in hand, waiting for more would
    /// delay it.
    ///
    /// A zero timeout produces a short poll (`WaitTimeSeconds = 0`), which is
    /// what makes [`celers_core::Broker::try_dequeue`] genuinely non-blocking.
    async fn receive_batch(
        &mut self,
        queue: &str,
        max_messages: usize,
        timeout: Duration,
    ) -> Result<Vec<Envelope>> {
        if max_messages == 0 {
            return Ok(Vec::new());
        }

        let wanted = i32::try_from(max_messages).unwrap_or(MAX_RECEIVE_MESSAGES);
        let wait_time = resolve_wait_time(timeout, self.wait_time_seconds);

        let mut candidates = self.priority_queues(queue);
        if candidates.is_empty() {
            candidates.push(self.resolve_queue_name(queue));
        }
        let last_index = candidates.len() - 1;

        let mut envelopes: Vec<Envelope> = Vec::new();
        let mut missing_queues = 0usize;
        let mut last_missing: Option<BrokerError> = None;

        for (index, candidate) in candidates.iter().enumerate() {
            let collected = i32::try_from(envelopes.len()).unwrap_or(wanted);
            let remaining = wanted.saturating_sub(collected);
            if remaining <= 0 {
                break;
            }

            let candidate_wait = if index == last_index && envelopes.is_empty() {
                wait_time
            } else {
                0
            };

            match self
                .consume_batch_physical(candidate, remaining, candidate_wait)
                .await
            {
                Ok(fetched) => envelopes.extend(fetched),
                // A priority sibling that was never created is not an error as
                // long as some other candidate exists: skip it and keep going.
                Err(BrokerError::QueueNotFound(name)) if candidates.len() > 1 => {
                    missing_queues += 1;
                    last_missing = Some(BrokerError::QueueNotFound(name));
                }
                Err(error) => return Err(error),
            }
        }

        // Every candidate missing is a configuration problem, not an empty
        // queue, and must not be reported as "no work".
        if missing_queues == candidates.len() {
            if let Some(error) = last_missing {
                return Err(error);
            }
        }

        Ok(envelopes)
    }

    /// `SendMessageBatch`: ten messages per request instead of one.
    ///
    /// Chunked automatically, and a partial failure is reported rather than
    /// collapsed into success — see
    /// [`publish_batch_detailed`](SqsBroker::publish_batch_detailed) for the
    /// per-entry outcomes.
    async fn send_batch(&mut self, queue: &str, messages: Vec<Message>) -> Result<()> {
        self.publish_batch(queue, messages).await.map(|_| ())
    }

    /// `DeleteMessageBatch`: ten deletions per request instead of one.
    ///
    /// Each delivery tag carries the queue it came from, so a batch spanning
    /// several queues (a DLQ drain, a priority scan) is grouped and deleted
    /// correctly.
    async fn ack_receipts(&mut self, delivery_tags: &[String]) -> Result<()> {
        if delivery_tags.is_empty() {
            return Ok(());
        }
        let fallback = self.resolve_queue_name(&self.queue_name.clone());
        self.ack_batch(&fallback, delivery_tags.to_vec())
            .await
            .map(|_| ())
    }

    /// `ChangeMessageVisibility`: give the message back, optionally later.
    ///
    /// This is the operation [`celers_core::Broker::defer`] was written for.
    /// The worker refused the message without running it (wrong labels, a rate
    /// limiter, draining), so the task's retry budget must not move — and SQS
    /// can express exactly that: resetting the visibility timeout re-queues the
    /// message without touching its `ApproximateReceiveCount` semantics the way
    /// a delete-and-republish would, and the delay is honoured to the second.
    ///
    /// `Duration::ZERO` makes it available immediately. Anything above SQS's
    /// 12 hour visibility ceiling is clamped to it.
    async fn return_message(&mut self, delivery_tag: &str, delay: Duration) -> Result<()> {
        if delay.is_zero() {
            // `reject(requeue = true)` is already "visibility timeout to zero",
            // and it carries the same heartbeat/metadata cleanup.
            return self.reject(delivery_tag, true).await;
        }
        self.hide_message_for(delivery_tag, delay).await
    }

    /// `SendMessage` with `DelaySeconds`: real broker-side scheduling.
    ///
    /// # Errors
    ///
    /// [`BrokerError::OperationFailed`] when `delay` exceeds SQS's 15 minute
    /// `DelaySeconds` ceiling. Truncating it silently would turn "in an hour"
    /// into "in fifteen minutes", which is the failure mode
    /// [`celers_core::Broker::enqueue_at`] exists to prevent; a caller that
    /// needs longer horizons wants `celers-beat` or a delayed queue, not a
    /// clamped delay. FIFO queues reject per-message delays outright, which
    /// [`publish_with_delay`](SqsBroker::publish_with_delay) reports.
    async fn send_delayed(&mut self, queue: &str, message: Message, delay: Duration) -> Result<()> {
        let seconds = delay.as_secs();
        if seconds > MAX_DELAY_SECONDS {
            return Err(BrokerError::OperationFailed(format!(
                "SQS caps DelaySeconds at {MAX_DELAY_SECONDS} seconds (15 minutes); \
                 {seconds} seconds was requested. Schedule longer horizons with celers-beat \
                 rather than letting the broker silently run the task early"
            )));
        }
        let seconds = i32::try_from(seconds).unwrap_or(MAX_DELAY_SECONDS as i32);
        self.publish_with_delay(queue, message, seconds).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use celers_core::Broker as CoreBroker;

    async fn broker(queue: &str) -> SqsBroker {
        SqsBroker::new(queue)
            .await
            .expect("constructing an SQS broker performs no I/O")
    }

    #[tokio::test]
    async fn into_core_broker_is_offline_and_retargets_the_transport() {
        let core = broker("declared-queue").await.into_core_broker("consumed");
        assert_eq!(core.queue(), "consumed");
        assert!(
            !core.is_connected().await,
            "no AWS client is built until the first operation"
        );
        assert_eq!(
            core.into_transport().queue_name,
            "consumed",
            "the transport must resolve a URL for the queue the adapter uses, \
             not the one it happened to be constructed with"
        );
    }

    /// The adapter satisfies exactly the bound `celers_worker::Worker::new`
    /// asks for (`celers_core::Broker + Send + Sync + 'static`).
    ///
    /// Asserting it as a trait object is a compile-time proof that does not
    /// need `celers-worker` in this crate's graph — and it is the thing that
    /// was missing before this module existed, so it is worth pinning rather
    /// than assuming.
    #[tokio::test]
    async fn the_adapter_is_usable_wherever_a_worker_expects_a_broker() {
        let boxed: Box<dyn celers_core::Broker> = Box::new(broker("q").await.into_core_broker("q"));
        assert!(
            !boxed.cancel(&uuid::Uuid::new_v4()).await.expect("cancel"),
            "reached through the trait object, cancel still answers honestly"
        );
    }

    #[tokio::test]
    async fn a_zero_sized_batch_never_reaches_aws() {
        // The offline connector in `test_support` would refuse a request, so
        // reaching AWS here would fail the test rather than hang it.
        let mut transport = broker("q").await;
        let envelopes = transport
            .receive_batch("q", 0, Duration::from_secs(1))
            .await
            .expect("a zero-sized batch needs no request");
        assert!(envelopes.is_empty());
    }

    #[tokio::test]
    async fn an_empty_ack_batch_never_reaches_aws() {
        let mut transport = broker("q").await;
        transport
            .ack_receipts(&[])
            .await
            .expect("acknowledging nothing needs no request");
    }

    #[tokio::test]
    async fn a_delay_beyond_the_sqs_ceiling_is_refused_before_any_request() {
        let mut transport = broker("q").await;
        let message = Message::new("tasks.later".to_string(), uuid::Uuid::new_v4(), Vec::new());

        let error = transport
            .send_delayed("q", message, Duration::from_secs(MAX_DELAY_SECONDS + 1))
            .await
            .expect_err("SQS cannot hold a message for longer than 15 minutes");
        assert!(
            error.to_string().contains("DelaySeconds"),
            "the error must name the limit it hit: {error}"
        );
    }

    // NOTE: there is deliberately no "scheduling beyond the ceiling through the
    // adapter" test. Every adapter operation connects first, so such a test
    // would reach the AWS credential chain before the limit check and then pass
    // on a *connection* error — proving nothing while making the unit suite
    // depend on the network. The limit is pinned at the transport level above,
    // and the adapter's job of forwarding the transport's error is pinned in
    // `celers-kombu`'s own tests.

    #[tokio::test]
    async fn acknowledging_without_a_receipt_handle_is_refused_not_ignored() {
        let core = broker("q").await.into_core_broker("q");
        let error = core
            .ack(&uuid::Uuid::new_v4(), None)
            .await
            .expect_err("an SQS message can only be deleted by its receipt handle");
        assert!(error.is_broker(), "expected a broker error, got {error:?}");
    }

    #[tokio::test]
    async fn cancelling_a_queued_message_reports_no_pending_copy() {
        let core = broker("q").await.into_core_broker("q");
        assert!(
            !core.cancel(&uuid::Uuid::new_v4()).await.expect("cancel"),
            "SQS has no way to withdraw a message that is already queued"
        );
    }
}
