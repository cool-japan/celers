// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! Running a `celers_worker::Worker` against RabbitMQ.
//!
//! [`AmqpBroker`] speaks the `celers-kombu` transport traits
//! (`publish`/`consume`/`purge`/...). A worker consumes
//! [`celers_core::Broker`] (`enqueue`/`dequeue`/`ack`/`reject`/...). This
//! module is the join: it teaches `AmqpBroker` the
//! [`CoreBrokerTransport`] seam and re-exports the resulting task-queue broker
//! as [`AmqpCoreBroker`].
//!
//! ```no_run
//! use celers_broker_amqp::AmqpBroker;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let broker = AmqpBroker::new("amqp://localhost:5672", "celery")
//!     .await?
//!     .into_core_broker("celery");
//! // `broker` is now a `celers_core::Broker`; hand it to `Worker::new`.
//! # Ok(())
//! # }
//! ```
//!
//! Available with the `core-broker` feature, which is on by default.

use async_trait::async_trait;
use celers_kombu::core_adapter::{CoreBrokerTransport, KombuBrokerAdapter};
use celers_kombu::{Consumer, Envelope, Result};
use celers_protocol::Message;
use std::time::Duration;
use tracing::warn;

use crate::AmqpBroker;

/// A [`celers_core::Broker`] backed by RabbitMQ.
///
/// Build one with [`AmqpBroker::into_core_broker`]. See
/// [`KombuBrokerAdapter`] for the lock, poll-timeout and lazy-connect
/// behaviour every transport adapter shares.
pub type AmqpCoreBroker = KombuBrokerAdapter<AmqpBroker>;

impl AmqpBroker {
    /// Turn this transport into a worker-usable [`celers_core::Broker`].
    ///
    /// `queue` becomes the queue the resulting broker publishes to and consumes
    /// from — **and** this transport's own queue name, which is the queue
    /// [`connect`](celers_kombu::Transport::connect) declares and binds during
    /// topology setup. Keeping the two in step is deliberate: a transport that
    /// declared one queue while the adapter consumed another would consume from
    /// a queue nothing had declared.
    ///
    /// The connection is opened lazily, so this call performs no I/O.
    #[must_use]
    pub fn into_core_broker(mut self, queue: impl Into<String>) -> AmqpCoreBroker {
        let queue = queue.into();
        self.queue_name.clone_from(&queue);
        KombuBrokerAdapter::new(self, queue)
    }
}

#[async_trait]
impl CoreBrokerTransport for AmqpBroker {
    /// Take a batch in whichever shape actually batches for the configured
    /// consume mode.
    ///
    /// AMQP has two ways to move several messages at once and they are not
    /// interchangeable:
    ///
    /// * **Subscribed** (the default): a `basic.consume` subscription pushes up
    ///   to [`AmqpConfig::effective_prefetch_count`](crate::AmqpConfig::effective_prefetch_count)
    ///   unacknowledged messages into the client. Draining that window costs
    ///   **no** further round trips, so the batch is built by taking the first
    ///   message with the caller's timeout and then emptying the buffer with
    ///   zero-timeout reads. The batch size is therefore bounded by the
    ///   prefetch count — raise it with
    ///   [`with_prefetch`](crate::AmqpConfig::with_prefetch) if you want larger
    ///   batches.
    /// * **Poll mode** ([`with_poll_mode`](crate::AmqpConfig::with_poll_mode)):
    ///   there is no subscription to drain, so the batch is
    ///   [`consume_batch`](AmqpBroker::consume_batch)'s `basic.get` loop. That
    ///   is one round trip per message, which is what `basic.get` costs; it is
    ///   still a single call from the caller's point of view and it requeues
    ///   the whole batch if a message turns out to be undecodable.
    ///
    /// A zero timeout is honoured strictly in both modes — it is what
    /// [`celers_core::Broker::try_dequeue`] uses, and a poll that waited would
    /// defeat the point.
    async fn receive_batch(
        &mut self,
        queue: &str,
        max_messages: usize,
        timeout: Duration,
    ) -> Result<Vec<Envelope>> {
        if max_messages == 0 {
            return Ok(Vec::new());
        }

        // `consume_batch` checks its deadline *before* each `basic.get`, so a
        // zero timeout would return without ever asking the broker anything.
        // A non-blocking caller still needs one round trip per attempt.
        if self.config().poll_mode && !timeout.is_zero() {
            return self.consume_batch(queue, max_messages, timeout).await;
        }

        let mut envelopes = Vec::with_capacity(max_messages.min(64));

        // The first read may wait; a failure here has stranded nothing.
        match self.consume(queue, timeout).await? {
            Some(envelope) => envelopes.push(envelope),
            None => return Ok(envelopes),
        }

        for _ in 1..max_messages {
            match self.consume(queue, Duration::ZERO).await {
                Ok(Some(envelope)) => envelopes.push(envelope),
                Ok(None) => break,
                Err(error) => {
                    // `consume` has already dead-lettered the delivery it could
                    // not decode. Propagating the error would drop the
                    // envelopes collected so far while they are unacknowledged
                    // on the channel, so they would sit in flight until the
                    // connection went away. Hand back the good ones instead.
                    warn!("Stopping batch drain after a bad delivery: {}", error);
                    break;
                }
            }
        }

        Ok(envelopes)
    }

    /// Publish the whole batch, then wait for the confirms once.
    ///
    /// [`publish_batch`](AmqpBroker::publish_batch) pipelines the
    /// `basic.publish` frames and collects the publisher confirms afterwards,
    /// so a batch of `n` costs one confirm round trip instead of `n`.
    async fn send_batch(&mut self, queue: &str, messages: Vec<Message>) -> Result<()> {
        self.publish_batch(queue, messages).await.map(|_| ())
    }

    /// Acknowledge one delivery at a time — deliberately.
    ///
    /// AMQP's batch acknowledgement is `basic.ack` with `multiple = true`,
    /// which acknowledges **every** unacknowledged delivery up to that tag on
    /// the channel. This broker shares one channel between the adapter and
    /// anything else holding it, so `multiple` would silently acknowledge
    /// deliveries this batch never saw. One `basic.ack` per message is the only
    /// correct form; the frames are pipelined on one channel anyway.
    async fn ack_receipts(&mut self, delivery_tags: &[String]) -> Result<()> {
        for tag in delivery_tags {
            self.ack(tag).await?;
        }
        Ok(())
    }

    // `return_message` and `send_delayed` keep the trait defaults:
    //
    // * AMQP has no per-message visibility deadline, so returning a message can
    //   only make it available again immediately — which is what the default
    //   `reject(requeue = true)` does.
    // * Broker-side delayed delivery needs either RabbitMQ's
    //   `rabbitmq_delayed_message_exchange` plugin or a per-message-TTL holding
    //   queue with a dead-letter exchange, neither of which this broker
    //   declares. The `scheduler` module's `MessageScheduler` is an in-process
    //   heap that dies with the process, so backing `send_delayed` with it
    //   would promise durability it does not have. The default therefore
    //   reports that scheduling is unsupported rather than publishing the
    //   message immediately.
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::AmqpConfig;

    /// `into_core_broker` must perform no I/O: it is called before `connect`.
    #[tokio::test]
    async fn into_core_broker_is_offline_and_retargets_the_transport() {
        let broker = AmqpBroker::with_config(
            "amqp://127.0.0.1:1", // nothing listens here on purpose
            "declared-queue",
            AmqpConfig::default(),
        )
        .await
        .expect("constructing a broker opens no connection");

        let core = broker.into_core_broker("consumed-queue");
        assert_eq!(core.queue(), "consumed-queue");
        assert!(
            !core.is_connected().await,
            "the adapter must connect lazily, not in the constructor"
        );
        assert_eq!(
            core.into_transport().queue_name,
            "consumed-queue",
            "topology setup must declare the queue the adapter consumes from"
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
        let broker = AmqpBroker::with_config("amqp://127.0.0.1:1", "q", AmqpConfig::default())
            .await
            .expect("construct");
        let boxed: Box<dyn celers_core::Broker> = Box::new(broker.into_core_broker("q"));
        assert!(
            !boxed.cancel(&Uuid::new_v4()).await.expect("cancel"),
            "reached through the trait object, cancel still answers honestly"
        );
    }

    /// A batch with nothing to fetch must not reach the broker at all.
    #[tokio::test]
    async fn an_empty_batch_request_is_answered_without_touching_the_connection() {
        let mut broker = AmqpBroker::with_config("amqp://127.0.0.1:1", "q", AmqpConfig::default())
            .await
            .expect("construct");

        let envelopes = broker
            .receive_batch("q", 0, Duration::from_secs(1))
            .await
            .expect("a zero-sized batch needs no connection");
        assert!(envelopes.is_empty());
    }

    // ==================== Live RabbitMQ ====================
    //
    // Gated on `CELERS_TEST_AMQP_URL`. Without it each test prints a visible,
    // greppable SKIPPED line naming its call site and returns, so a skipped run
    // and a real run are told apart in the log rather than both reporting `ok`.

    use celers_core::{Broker as CoreBroker, SerializedTask};
    use celers_kombu::{Broker as KombuBroker, Transport};
    use uuid::Uuid;

    #[track_caller]
    fn integration_url() -> Option<String> {
        match std::env::var("CELERS_TEST_AMQP_URL") {
            Ok(url) if !url.is_empty() => Some(url),
            _ => {
                let location = std::panic::Location::caller();
                eprintln!("SKIPPED: {location} (set CELERS_TEST_AMQP_URL to run)");
                None
            }
        }
    }

    /// A queue name no other run can collide with.
    fn unique_queue(prefix: &str) -> String {
        format!("{}-{}", prefix, Uuid::new_v4().simple())
    }

    /// Build an adapter over a freshly declared queue, and hand back the queue
    /// name so the test can delete it afterwards.
    async fn live_core_broker(url: &str, queue: &str) -> AmqpCoreBroker {
        let broker = AmqpBroker::new(url, queue)
            .await
            .expect("constructing the transport");
        let core = broker.into_core_broker(queue);
        core.connect().await.expect("connecting declares the queue");
        core
    }

    async fn drop_queue(url: &str, queue: &str) {
        let mut broker = AmqpBroker::new(url, queue).await.expect("construct");
        if broker.connect().await.is_ok() {
            let _ = broker.delete_queue(queue).await;
            let _ = broker.disconnect().await;
        }
    }

    #[tokio::test]
    async fn a_worker_can_enqueue_dequeue_and_ack_through_the_adapter() {
        let Some(url) = integration_url() else {
            return;
        };
        let queue = unique_queue("celers-core-roundtrip");
        let broker = live_core_broker(&url, &queue).await;

        let task =
            SerializedTask::new("tasks.roundtrip".to_string(), vec![1, 2, 3]).with_max_retries(4);
        let task_id = broker.enqueue(task).await.expect("enqueue");

        let message = broker
            .dequeue()
            .await
            .expect("dequeue")
            .expect("the message just enqueued is available");
        assert_eq!(message.task.metadata.id, task_id);
        assert_eq!(
            message.task.metadata.max_retries, 4,
            "metadata must survive the AMQP round trip"
        );
        assert_eq!(message.task.payload, vec![1, 2, 3]);

        broker
            .ack(&task_id, message.receipt_handle.as_deref())
            .await
            .expect("ack");

        drop_queue(&url, &queue).await;
    }

    #[tokio::test]
    async fn dequeue_batch_returns_more_than_one_message_per_call() {
        let Some(url) = integration_url() else {
            return;
        };
        let queue = unique_queue("celers-core-batch");
        let broker = live_core_broker(&url, &queue).await;

        let tasks: Vec<SerializedTask> = (0..5u8)
            .map(|i| SerializedTask::new(format!("tasks.batch_{i}"), vec![i]))
            .collect();
        let ids = broker.enqueue_batch(tasks).await.expect("enqueue_batch");
        assert_eq!(ids.len(), 5);

        // The prefetch window is 100 by default, so one call drains all five.
        let messages = broker.dequeue_batch(5).await.expect("dequeue_batch");
        assert!(
            messages.len() > 1,
            "batch dequeue must return a batch, got {} message(s)",
            messages.len()
        );

        let acks: Vec<(Uuid, Option<String>)> = messages
            .iter()
            .map(|m| (m.task.metadata.id, m.receipt_handle.clone()))
            .collect();
        broker.ack_batch(&acks).await.expect("ack_batch");

        drop_queue(&url, &queue).await;
    }

    #[tokio::test]
    async fn a_rejected_message_comes_back_when_requeued() {
        let Some(url) = integration_url() else {
            return;
        };
        let queue = unique_queue("celers-core-requeue");
        let broker = live_core_broker(&url, &queue).await;

        let task_id = broker
            .enqueue(SerializedTask::new("tasks.requeue".to_string(), vec![7]))
            .await
            .expect("enqueue");

        let message = broker.dequeue().await.expect("dequeue").expect("a message");
        broker
            .reject(&task_id, message.receipt_handle.as_deref(), true)
            .await
            .expect("reject");

        let again = broker
            .dequeue()
            .await
            .expect("dequeue")
            .expect("a requeued message is delivered again");
        assert_eq!(again.task.metadata.id, task_id);

        broker
            .ack(&task_id, again.receipt_handle.as_deref())
            .await
            .expect("ack");

        drop_queue(&url, &queue).await;
    }
}
