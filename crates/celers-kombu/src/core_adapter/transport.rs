// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: Apache-2.0

//! The seam between a transport and the task-queue adapter.

use async_trait::async_trait;
use celers_protocol::Message;
use std::time::Duration;

use crate::{Broker, BrokerError, Envelope, Result};

/// What [`KombuBrokerAdapter`](super::KombuBrokerAdapter) needs from a
/// transport beyond the one-message-at-a-time [`Broker`] trait.
///
/// # Why this exists
///
/// [`Broker`] can publish and consume a single message, which is enough to
/// implement [`celers_core::Broker`] — but only badly. Three of the task-queue
/// trait's operations have a native form on real transports that a loop over
/// `publish`/`consume`/`reject` cannot reach:
///
/// * **batching**: SQS `ReceiveMessage` returns up to ten messages for the
///   price of one request, and AMQP can drain a prefetch window with repeated
///   `basic.get` on one channel. Draining a batch by calling `consume` `n`
///   times means `n` round trips, and on SQS `n` times the bill.
/// * **returning a message without spending retry budget**
///   ([`celers_core::Broker::defer`]): SQS expresses it exactly, as
///   `ChangeMessageVisibility` with the delay the caller asked for.
/// * **delayed delivery** ([`celers_core::Broker::enqueue_after`]): SQS has
///   `DelaySeconds`; most transports have nothing.
///
/// Every method has a default that is correct but unoptimised, so a transport
/// opts in with a bare `impl CoreBrokerTransport for MyTransport {}` and
/// overrides only what it can genuinely do better.
///
/// # Implementing it
///
/// The defaults are deliberately conservative:
///
/// * [`receive_batch`](Self::receive_batch) waits for the first message and
///   then takes only what is already available, so it never blocks once it has
///   something to return.
/// * [`return_message`](Self::return_message) drops the delay and requeues,
///   matching [`celers_core::Broker::defer`]'s own documented fallback.
/// * [`send_delayed`](Self::send_delayed) **fails** rather than publishing
///   immediately: turning "in an hour" into "now" silently is worse than
///   saying the transport cannot do it.
///
/// [`KombuBrokerAdapter`]: super::KombuBrokerAdapter
#[async_trait]
pub trait CoreBrokerTransport: Broker {
    /// Receive up to `max_messages` messages in as few round trips as the
    /// transport allows.
    ///
    /// `timeout` bounds the wait for the *first* message only; once anything is
    /// available the call must return promptly rather than waiting out the full
    /// timeout for a full batch. A caller passing `Duration::ZERO` is asking
    /// for a strictly non-blocking poll, and an implementation must honour that
    /// — [`celers_core::Broker::try_dequeue`] depends on it.
    ///
    /// # Errors
    ///
    /// Whatever the transport reports. Returning fewer messages than asked for
    /// (including none) is not an error.
    async fn receive_batch(
        &mut self,
        queue: &str,
        max_messages: usize,
        timeout: Duration,
    ) -> Result<Vec<Envelope>> {
        let mut envelopes = Vec::new();
        if max_messages == 0 {
            return Ok(envelopes);
        }

        match self.consume(queue, timeout).await? {
            Some(envelope) => envelopes.push(envelope),
            // Nothing there: do not spend the remaining polls on an empty queue.
            None => return Ok(envelopes),
        }

        for _ in 1..max_messages {
            match self.consume(queue, Duration::ZERO).await? {
                Some(envelope) => envelopes.push(envelope),
                None => break,
            }
        }

        Ok(envelopes)
    }

    /// Publish several messages to `queue`.
    ///
    /// # Errors
    ///
    /// Whatever the transport reports. The default stops at the first failure,
    /// so an error leaves an unspecified prefix of the batch published — which
    /// is also true of every batch API this can delegate to, since none of them
    /// is transactional.
    async fn send_batch(&mut self, queue: &str, messages: Vec<Message>) -> Result<()> {
        for message in messages {
            self.publish(queue, message).await?;
        }
        Ok(())
    }

    /// Acknowledge several delivered messages.
    ///
    /// # Errors
    ///
    /// Whatever the transport reports.
    async fn ack_receipts(&mut self, delivery_tags: &[String]) -> Result<()> {
        for tag in delivery_tags {
            self.ack(tag).await?;
        }
        Ok(())
    }

    /// Return a delivered message to the queue **without** counting an attempt
    /// against it, optionally holding it back for `delay`.
    ///
    /// This is [`celers_core::Broker::defer`]'s transport-level form: the
    /// worker refused the message (wrong labels, rate limit, draining), so
    /// nothing is known to be wrong with the task and its retry budget must not
    /// move.
    ///
    /// # Errors
    ///
    /// Whatever the transport reports.
    async fn return_message(&mut self, delivery_tag: &str, delay: Duration) -> Result<()> {
        // Deliberately dropped: a transport with no delayed-visibility
        // primitive can only make the message available again immediately.
        let _ = delay;
        self.reject(delivery_tag, true).await
    }

    /// Publish a message that must not become visible for `delay`.
    ///
    /// # Errors
    ///
    /// [`BrokerError::OperationFailed`] from the default implementation: a
    /// transport with no delayed-delivery primitive must say so rather than
    /// publish the message immediately. Implementations that *do* support it
    /// should also report the limits they have (SQS caps `DelaySeconds` at 15
    /// minutes) rather than silently truncating the delay.
    async fn send_delayed(&mut self, queue: &str, message: Message, delay: Duration) -> Result<()> {
        let _ = (queue, message);
        Err(BrokerError::OperationFailed(format!(
            "transport '{}' cannot hold a message back for {:?} (no delayed-delivery support)",
            self.name(),
            delay
        )))
    }
}
