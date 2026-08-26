// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: Apache-2.0

//! Hermetic tests for the task-queue adapter.
//!
//! Everything here runs against in-process transports, so the whole file is
//! green on a machine with no RabbitMQ, no SQS and no network. The transports
//! are deliberately instrumented rather than realistic: what needs proving is
//! *which transport call the adapter makes*, not what a real broker does with
//! it. Live coverage lives in the broker crates.

use std::collections::VecDeque;
use std::time::Duration;

use async_trait::async_trait;
use celers_core::{Broker as CoreBroker, SerializedTask, TaskState};
use celers_protocol::Message;
use uuid::Uuid;

use super::{
    message_to_task, task_to_message, CoreBrokerTransport, KombuBrokerAdapter, TASK_METADATA_HEADER,
};
use crate::{
    Broker as KombuBroker, BrokerError, Consumer, Envelope, MockBroker, Producer, QueueMode,
    Result as KombuResult, Transport,
};

// =============================================================================
// Conversion
// =============================================================================

#[test]
fn round_trip_preserves_every_metadata_field() {
    let mut task = SerializedTask::new("tasks.report".to_string(), vec![9, 8, 7])
        .with_priority(4)
        .with_max_retries(11)
        .with_timeout(42);
    task.metadata.state = TaskState::Retrying(3);
    task.metadata.group_id = Some(Uuid::new_v4());
    task.metadata.chord_id = Some(Uuid::new_v4());
    task.metadata.on_success_link = Some("tasks.notify".to_string());
    task.metadata.expires_at = Some(deadline());

    let expected = task.clone();
    let message = task_to_message(task).expect("task converts to a message");

    // The header a transport (and a human reading the queue) can act on.
    assert_eq!(message.headers.task, "tasks.report");
    assert_eq!(message.headers.id, expected.metadata.id);
    assert_eq!(message.headers.group, expected.metadata.group_id);
    assert_eq!(message.headers.expires, expected.metadata.expires_at);
    assert_eq!(message.headers.retries, Some(3));
    assert_eq!(message.properties.priority, Some(4));

    let restored = message_to_task(message).expect("message converts back");
    assert_eq!(restored.metadata.id, expected.metadata.id);
    assert_eq!(restored.metadata.name, expected.metadata.name);
    assert_eq!(restored.metadata.max_retries, 11);
    assert_eq!(restored.metadata.timeout_secs, Some(42));
    assert_eq!(restored.metadata.priority, 4);
    assert_eq!(restored.metadata.state, TaskState::Retrying(3));
    assert_eq!(restored.metadata.group_id, expected.metadata.group_id);
    assert_eq!(restored.metadata.chord_id, expected.metadata.chord_id);
    assert_eq!(
        restored.metadata.on_success_link.as_deref(),
        Some("tasks.notify")
    );
    assert_eq!(restored.metadata.expires_at, expected.metadata.expires_at);
    assert_eq!(restored.payload, vec![9, 8, 7]);
}

/// A message-expiry deadline far enough out that it cannot go stale mid-test.
///
/// Truncated to whole seconds because that is the resolution the RFC 3339 form
/// a JSON round trip produces would otherwise argue about.
fn deadline() -> chrono::DateTime<chrono::Utc> {
    chrono::Utc::now() + chrono::Duration::hours(1)
}

#[test]
fn round_trip_survives_a_json_hop_through_a_transport() {
    // Every transport in the workspace serialises the message as JSON; the
    // embedded metadata has to survive that, not just an in-process move.
    let task = SerializedTask::new("tasks.hop".to_string(), vec![1, 2, 3]).with_max_retries(7);
    let task_id = task.metadata.id;

    let message = task_to_message(task).expect("convert");
    let wire = serde_json::to_vec(&message).expect("serialize");
    let decoded: Message = serde_json::from_slice(&wire).expect("deserialize");

    let restored = message_to_task(decoded).expect("convert back");
    assert_eq!(restored.metadata.id, task_id);
    assert_eq!(restored.metadata.max_retries, 7);
    assert_eq!(restored.payload, vec![1, 2, 3]);
}

#[test]
fn a_foreign_message_is_reconstructed_rather_than_refused() {
    let id = Uuid::new_v4();
    let mut message = Message::new("tasks.from_elsewhere".to_string(), id, b"body".to_vec());
    message.properties.priority = Some(6);
    message.headers.retries = Some(2);

    let task = message_to_task(message).expect("a message with no embedded metadata is usable");
    assert_eq!(task.metadata.name, "tasks.from_elsewhere");
    assert_eq!(task.metadata.id, id);
    assert_eq!(task.metadata.priority, 6);
    assert_eq!(
        task.metadata.state,
        TaskState::Retrying(2),
        "a foreign producer's spent attempts must not be forgotten"
    );
    assert_eq!(task.payload, b"body".to_vec());
}

#[test]
fn unreadable_embedded_metadata_is_an_error_not_a_silent_downgrade() {
    let mut message = Message::new("tasks.tampered".to_string(), Uuid::new_v4(), Vec::new());
    message.headers.extra.insert(
        TASK_METADATA_HEADER.to_string(),
        serde_json::json!({"not": "a TaskMetadata"}),
    );

    let error = message_to_task(message).expect_err("claimed metadata must decode");
    assert!(
        error.is_serialization(),
        "expected a serialization error, got {error:?}"
    );
}

#[test]
fn priority_is_clamped_onto_the_wire_but_kept_exactly_in_the_metadata() {
    let task = SerializedTask::new("tasks.urgent".to_string(), Vec::new()).with_priority(9_000);
    let message = task_to_message(task).expect("convert");
    assert_eq!(
        message.properties.priority,
        Some(9),
        "the wire model rejects anything above 9"
    );

    let restored = message_to_task(message).expect("convert back");
    assert_eq!(
        restored.metadata.priority, 9_000,
        "the embedded metadata keeps the value the producer chose"
    );
}

// =============================================================================
// Instrumented transports
// =============================================================================

/// Everything the adapter did to a transport, in order.
#[derive(Debug, Default)]
struct TransportLog {
    connect_calls: usize,
    consume_timeouts: Vec<Duration>,
    receive_batch_calls: Vec<(usize, Duration)>,
    send_batch_sizes: Vec<usize>,
    ack_receipt_sizes: Vec<usize>,
    published: Vec<Message>,
    acked: Vec<String>,
    rejected: Vec<(String, bool)>,
    returned: Vec<(String, Duration)>,
    delayed: Vec<Duration>,
}

/// A transport that takes every [`CoreBrokerTransport`] default.
///
/// This is what proves the defaults themselves behave: that the default
/// `receive_batch` waits only for the first message, that `send_delayed`
/// refuses rather than publishing immediately, and so on.
#[derive(Debug, Default)]
struct DefaultsTransport {
    connected: bool,
    ready: VecDeque<Envelope>,
    log: TransportLog,
}

impl DefaultsTransport {
    /// Queue `count` ready messages, tagged `tag-0`, `tag-1`, ...
    fn seed(&mut self, count: usize) {
        for index in 0..count {
            let task = SerializedTask::new(format!("tasks.seeded_{index}"), vec![index as u8]);
            let message = task_to_message(task).expect("seed converts");
            self.ready
                .push_back(Envelope::new(message, format!("tag-{index}")));
        }
    }

    fn take(&mut self, max: usize) -> Vec<Envelope> {
        let mut taken = Vec::new();
        while taken.len() < max {
            match self.ready.pop_front() {
                Some(envelope) => taken.push(envelope),
                None => break,
            }
        }
        taken
    }
}

#[async_trait]
impl Transport for DefaultsTransport {
    async fn connect(&mut self) -> KombuResult<()> {
        self.log.connect_calls += 1;
        self.connected = true;
        Ok(())
    }

    async fn disconnect(&mut self) -> KombuResult<()> {
        self.connected = false;
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.connected
    }

    fn name(&self) -> &str {
        "test-defaults"
    }
}

#[async_trait]
impl Producer for DefaultsTransport {
    async fn publish(&mut self, _queue: &str, message: Message) -> KombuResult<()> {
        self.log.published.push(message.clone());
        let tag = format!("tag-published-{}", self.log.published.len());
        self.ready.push_back(Envelope::new(message, tag));
        Ok(())
    }

    async fn publish_with_routing(
        &mut self,
        _exchange: &str,
        routing_key: &str,
        message: Message,
    ) -> KombuResult<()> {
        self.publish(routing_key, message).await
    }
}

#[async_trait]
impl Consumer for DefaultsTransport {
    async fn consume(&mut self, _queue: &str, timeout: Duration) -> KombuResult<Option<Envelope>> {
        self.log.consume_timeouts.push(timeout);
        Ok(self.ready.pop_front())
    }

    async fn ack(&mut self, delivery_tag: &str) -> KombuResult<()> {
        self.log.acked.push(delivery_tag.to_string());
        Ok(())
    }

    async fn reject(&mut self, delivery_tag: &str, requeue: bool) -> KombuResult<()> {
        self.log.rejected.push((delivery_tag.to_string(), requeue));
        Ok(())
    }

    async fn queue_size(&mut self, _queue: &str) -> KombuResult<usize> {
        Ok(self.ready.len())
    }
}

#[async_trait]
impl KombuBroker for DefaultsTransport {
    async fn purge(&mut self, _queue: &str) -> KombuResult<usize> {
        let count = self.ready.len();
        self.ready.clear();
        Ok(count)
    }

    async fn create_queue(&mut self, _queue: &str, _mode: QueueMode) -> KombuResult<()> {
        Ok(())
    }

    async fn delete_queue(&mut self, _queue: &str) -> KombuResult<()> {
        Ok(())
    }

    async fn list_queues(&mut self) -> KombuResult<Vec<String>> {
        Ok(vec!["celery".to_string()])
    }
}

impl CoreBrokerTransport for DefaultsTransport {}

/// A transport that lends the adapter native batch, defer and delay
/// operations — the shape `SqsBroker` has.
#[derive(Debug, Default)]
struct NativeTransport {
    inner: DefaultsTransport,
}

#[async_trait]
impl Transport for NativeTransport {
    async fn connect(&mut self) -> KombuResult<()> {
        self.inner.connect().await
    }

    async fn disconnect(&mut self) -> KombuResult<()> {
        self.inner.disconnect().await
    }

    fn is_connected(&self) -> bool {
        self.inner.is_connected()
    }

    fn name(&self) -> &str {
        "test-native"
    }
}

#[async_trait]
impl Producer for NativeTransport {
    async fn publish(&mut self, queue: &str, message: Message) -> KombuResult<()> {
        self.inner.publish(queue, message).await
    }

    async fn publish_with_routing(
        &mut self,
        exchange: &str,
        routing_key: &str,
        message: Message,
    ) -> KombuResult<()> {
        self.inner
            .publish_with_routing(exchange, routing_key, message)
            .await
    }
}

#[async_trait]
impl Consumer for NativeTransport {
    async fn consume(&mut self, queue: &str, timeout: Duration) -> KombuResult<Option<Envelope>> {
        self.inner.consume(queue, timeout).await
    }

    async fn ack(&mut self, delivery_tag: &str) -> KombuResult<()> {
        self.inner.ack(delivery_tag).await
    }

    async fn reject(&mut self, delivery_tag: &str, requeue: bool) -> KombuResult<()> {
        self.inner.reject(delivery_tag, requeue).await
    }

    async fn queue_size(&mut self, queue: &str) -> KombuResult<usize> {
        self.inner.queue_size(queue).await
    }
}

#[async_trait]
impl KombuBroker for NativeTransport {
    async fn purge(&mut self, queue: &str) -> KombuResult<usize> {
        self.inner.purge(queue).await
    }

    async fn create_queue(&mut self, queue: &str, mode: QueueMode) -> KombuResult<()> {
        self.inner.create_queue(queue, mode).await
    }

    async fn delete_queue(&mut self, queue: &str) -> KombuResult<()> {
        self.inner.delete_queue(queue).await
    }

    async fn list_queues(&mut self) -> KombuResult<Vec<String>> {
        self.inner.list_queues().await
    }
}

#[async_trait]
impl CoreBrokerTransport for NativeTransport {
    async fn receive_batch(
        &mut self,
        _queue: &str,
        max_messages: usize,
        timeout: Duration,
    ) -> KombuResult<Vec<Envelope>> {
        self.inner
            .log
            .receive_batch_calls
            .push((max_messages, timeout));
        Ok(self.inner.take(max_messages))
    }

    async fn send_batch(&mut self, queue: &str, messages: Vec<Message>) -> KombuResult<()> {
        self.inner.log.send_batch_sizes.push(messages.len());
        for message in messages {
            self.inner.publish(queue, message).await?;
        }
        Ok(())
    }

    async fn ack_receipts(&mut self, delivery_tags: &[String]) -> KombuResult<()> {
        self.inner.log.ack_receipt_sizes.push(delivery_tags.len());
        for tag in delivery_tags {
            self.inner.ack(tag).await?;
        }
        Ok(())
    }

    async fn return_message(&mut self, delivery_tag: &str, delay: Duration) -> KombuResult<()> {
        self.inner
            .log
            .returned
            .push((delivery_tag.to_string(), delay));
        Ok(())
    }

    async fn send_delayed(
        &mut self,
        queue: &str,
        message: Message,
        delay: Duration,
    ) -> KombuResult<()> {
        self.inner.log.delayed.push(delay);
        self.inner.publish(queue, message).await
    }
}

// =============================================================================
// The adapter over the defaults
// =============================================================================

#[tokio::test]
async fn the_transport_is_connected_lazily_and_only_once() {
    let adapter = KombuBrokerAdapter::new(DefaultsTransport::default(), "celery");

    adapter
        .enqueue(SerializedTask::new("tasks.a".to_string(), Vec::new()))
        .await
        .expect("enqueue connects on its own");
    adapter
        .enqueue(SerializedTask::new("tasks.b".to_string(), Vec::new()))
        .await
        .expect("enqueue");

    let transport = adapter.into_transport();
    assert_eq!(
        transport.log.connect_calls, 1,
        "an already-connected transport must not be reconnected per call"
    );
    assert_eq!(transport.log.published.len(), 2);
}

#[tokio::test]
async fn enqueue_dequeue_ack_round_trip() {
    let adapter = KombuBrokerAdapter::new(DefaultsTransport::default(), "celery");

    let task = SerializedTask::new("tasks.round_trip".to_string(), vec![4, 2]).with_max_retries(9);
    let task_id = adapter.enqueue(task).await.expect("enqueue");

    assert_eq!(adapter.queue_size().await.expect("queue size"), 1);

    let message = adapter
        .dequeue()
        .await
        .expect("dequeue")
        .expect("a message is waiting");
    assert_eq!(message.task.metadata.id, task_id);
    assert_eq!(message.task.metadata.max_retries, 9);
    assert_eq!(message.task.payload, vec![4, 2]);
    let handle = message
        .receipt_handle
        .clone()
        .expect("a dequeued message always carries its delivery tag");

    adapter
        .ack(&task_id, message.receipt_handle.as_deref())
        .await
        .expect("ack");

    let transport = adapter.into_transport();
    assert_eq!(transport.log.acked, vec![handle]);
}

#[tokio::test]
async fn an_empty_queue_dequeues_to_none_rather_than_erroring() {
    let adapter = KombuBrokerAdapter::new(DefaultsTransport::default(), "celery");
    assert!(adapter.dequeue().await.expect("dequeue").is_none());
    assert!(adapter.try_dequeue().await.expect("try_dequeue").is_none());
}

#[tokio::test]
async fn reject_forwards_the_requeue_flag() {
    let adapter = KombuBrokerAdapter::new(DefaultsTransport::default(), "celery");
    let task_id = adapter
        .enqueue(SerializedTask::new("tasks.reject".to_string(), Vec::new()))
        .await
        .expect("enqueue");
    let message = adapter
        .dequeue()
        .await
        .expect("dequeue")
        .expect("a message");

    adapter
        .reject(&task_id, message.receipt_handle.as_deref(), true)
        .await
        .expect("reject");

    let transport = adapter.into_transport();
    assert_eq!(transport.log.rejected.len(), 1);
    assert!(
        transport.log.rejected[0].1,
        "requeue = true must reach the transport"
    );
}

#[tokio::test]
async fn ack_without_a_receipt_handle_refuses_instead_of_pretending() {
    let adapter = KombuBrokerAdapter::new(DefaultsTransport::default(), "celery");
    let task_id = Uuid::new_v4();

    let error = adapter
        .ack(&task_id, None)
        .await
        .expect_err("there is no way to address the message");
    assert!(error.is_broker(), "expected a broker error, got {error:?}");

    let transport = adapter.into_transport();
    assert!(
        transport.log.acked.is_empty(),
        "nothing must be acknowledged when the handle is missing"
    );
}

#[tokio::test]
async fn cancel_reports_no_pending_copy() {
    let adapter = KombuBrokerAdapter::new(DefaultsTransport::default(), "celery");
    assert!(
        !adapter.cancel(&Uuid::new_v4()).await.expect("cancel"),
        "neither transport can withdraw a queued message"
    );
}

#[tokio::test]
async fn the_default_batch_waits_for_the_first_message_only() {
    let mut transport = DefaultsTransport::default();
    transport.seed(3);
    let adapter =
        KombuBrokerAdapter::new(transport, "celery").with_poll_timeout(Duration::from_millis(750));

    let messages = adapter.dequeue_batch(5).await.expect("dequeue_batch");
    assert_eq!(messages.len(), 3);

    let transport = adapter.into_transport();
    assert_eq!(
        transport.log.consume_timeouts,
        vec![
            Duration::from_millis(750),
            Duration::ZERO,
            Duration::ZERO,
            Duration::ZERO,
        ],
        "only the first poll may wait; the drain must never block, and it stops \
         as soon as the queue runs dry"
    );
}

#[tokio::test]
async fn try_dequeue_never_asks_the_transport_to_wait() {
    let mut transport = DefaultsTransport::default();
    transport.seed(1);
    let adapter =
        KombuBrokerAdapter::new(transport, "celery").with_poll_timeout(Duration::from_secs(20));

    assert!(adapter.try_dequeue().await.expect("try_dequeue").is_some());

    let transport = adapter.into_transport();
    assert_eq!(
        transport.log.consume_timeouts,
        vec![Duration::ZERO],
        "a 20 second poll timeout must not leak into a non-blocking poll"
    );
}

#[tokio::test]
async fn scheduling_is_refused_rather_than_run_immediately() {
    let adapter = KombuBrokerAdapter::new(DefaultsTransport::default(), "celery");

    let error = adapter
        .enqueue_after(
            SerializedTask::new("tasks.later".to_string(), Vec::new()),
            3_600,
        )
        .await
        .expect_err("a transport with no delayed delivery must say so");
    assert!(error.is_broker(), "expected a broker error, got {error:?}");

    let error = adapter
        .enqueue_at(
            SerializedTask::new("tasks.later".to_string(), Vec::new()),
            4_102_444_800,
        )
        .await
        .expect_err("same for an absolute deadline");
    assert!(error.is_broker(), "expected a broker error, got {error:?}");

    let transport = adapter.into_transport();
    assert!(
        transport.log.published.is_empty(),
        "'in an hour' must never be silently turned into 'now'"
    );
}

#[tokio::test]
async fn defer_falls_back_to_requeue_when_the_transport_cannot_delay() {
    let adapter = KombuBrokerAdapter::new(DefaultsTransport::default(), "celery");
    let task_id = adapter
        .enqueue(SerializedTask::new("tasks.defer".to_string(), Vec::new()))
        .await
        .expect("enqueue");
    let message = adapter
        .dequeue()
        .await
        .expect("dequeue")
        .expect("a message");
    let handle = message.receipt_handle.clone().expect("handle");

    adapter
        .defer(
            &task_id,
            message.receipt_handle.as_deref(),
            Duration::from_secs(30),
        )
        .await
        .expect("defer");

    let transport = adapter.into_transport();
    assert_eq!(
        transport.log.rejected,
        vec![(handle, true)],
        "the default returns the message immediately rather than losing it"
    );
}

// =============================================================================
// The adapter over a transport with native batch support
// =============================================================================

#[tokio::test]
async fn dequeue_batch_makes_one_batch_call_and_no_single_consumes() {
    // The discriminating assertion: a `dequeue_batch` that returned five
    // messages by calling `consume` five times would look identical from the
    // outside, and would cost five SQS requests instead of one.
    let mut transport = NativeTransport::default();
    transport.inner.seed(5);
    let adapter =
        KombuBrokerAdapter::new(transport, "celery").with_poll_timeout(Duration::from_millis(500));

    let messages = adapter.dequeue_batch(5).await.expect("dequeue_batch");
    assert_eq!(messages.len(), 5, "batch dequeue must actually batch");
    assert!(
        messages.iter().all(|m| m.receipt_handle.is_some()),
        "every message must be acknowledgeable"
    );

    let transport = adapter.into_transport();
    assert_eq!(
        transport.inner.log.receive_batch_calls,
        vec![(5, Duration::from_millis(500))],
        "exactly one round trip, asking for the whole batch"
    );
    assert!(
        transport.inner.log.consume_timeouts.is_empty(),
        "the single-message path must not be used for a batch"
    );
}

#[tokio::test]
async fn try_dequeue_uses_the_batch_path_with_a_zero_wait() {
    let mut transport = NativeTransport::default();
    transport.inner.seed(2);
    let adapter =
        KombuBrokerAdapter::new(transport, "celery").with_poll_timeout(Duration::from_secs(20));

    assert!(adapter.try_dequeue().await.expect("try_dequeue").is_some());

    let transport = adapter.into_transport();
    assert_eq!(
        transport.inner.log.receive_batch_calls,
        vec![(1, Duration::ZERO)],
        "a non-blocking poll asks for one message and no wait at all"
    );
}

#[tokio::test]
async fn enqueue_batch_and_ack_batch_use_the_native_batch_apis() {
    let adapter = KombuBrokerAdapter::new(NativeTransport::default(), "celery");

    let tasks: Vec<SerializedTask> = (0..4)
        .map(|i| SerializedTask::new(format!("tasks.batch_{i}"), vec![i as u8]))
        .collect();
    let ids = adapter.enqueue_batch(tasks).await.expect("enqueue_batch");
    assert_eq!(ids.len(), 4);

    let messages = adapter.dequeue_batch(4).await.expect("dequeue_batch");
    assert_eq!(messages.len(), 4);

    let acks: Vec<(Uuid, Option<String>)> = messages
        .iter()
        .map(|m| (m.task.metadata.id, m.receipt_handle.clone()))
        .collect();
    adapter.ack_batch(&acks).await.expect("ack_batch");

    let transport = adapter.into_transport();
    assert_eq!(
        transport.inner.log.send_batch_sizes,
        vec![4],
        "one publish round trip, not four"
    );
    assert_eq!(
        transport.inner.log.ack_receipt_sizes,
        vec![4],
        "one delete round trip, not four"
    );
    assert_eq!(transport.inner.log.acked.len(), 4);
}

#[tokio::test]
async fn defer_reaches_a_transport_that_can_hold_the_message_back() {
    let mut transport = NativeTransport::default();
    transport.inner.seed(1);
    let adapter = KombuBrokerAdapter::new(transport, "celery");

    let message = adapter
        .dequeue()
        .await
        .expect("dequeue")
        .expect("a message");
    let handle = message.receipt_handle.clone().expect("handle");

    adapter
        .defer(
            &message.task.metadata.id,
            message.receipt_handle.as_deref(),
            Duration::from_secs(45),
        )
        .await
        .expect("defer");

    let transport = adapter.into_transport();
    assert_eq!(
        transport.inner.log.returned,
        vec![(handle, Duration::from_secs(45))],
        "the delay must reach the transport, not be dropped"
    );
    assert!(
        transport.inner.log.rejected.is_empty(),
        "defer must not spend the task's retry budget"
    );
}

#[tokio::test]
async fn scheduling_reaches_a_transport_that_supports_it() {
    let adapter = KombuBrokerAdapter::new(NativeTransport::default(), "celery");

    adapter
        .enqueue_after(
            SerializedTask::new("tasks.soon".to_string(), Vec::new()),
            120,
        )
        .await
        .expect("enqueue_after");

    let transport = adapter.into_transport();
    assert_eq!(transport.inner.log.delayed, vec![Duration::from_secs(120)]);
}

#[tokio::test]
async fn an_absolute_deadline_in_the_past_publishes_now() {
    let adapter = KombuBrokerAdapter::new(NativeTransport::default(), "celery");

    adapter
        .enqueue_at(
            SerializedTask::new("tasks.overdue".to_string(), Vec::new()),
            0,
        )
        .await
        .expect("enqueue_at");

    let transport = adapter.into_transport();
    assert_eq!(
        transport.inner.log.delayed,
        vec![Duration::ZERO],
        "a deadline already past means no delay, not a negative one"
    );
}

// =============================================================================
// Poison messages
// =============================================================================

#[tokio::test]
async fn an_undecodable_message_is_dead_lettered_rather_than_redelivered_forever() {
    let mut transport = DefaultsTransport::default();
    let mut poison = Message::new("tasks.poison".to_string(), Uuid::new_v4(), Vec::new());
    poison.headers.extra.insert(
        TASK_METADATA_HEADER.to_string(),
        serde_json::json!("this is not a TaskMetadata"),
    );
    transport
        .ready
        .push_back(Envelope::new(poison, "tag-poison".to_string()));

    let adapter = KombuBrokerAdapter::new(transport, "celery");

    let error = adapter
        .dequeue()
        .await
        .expect_err("an undecodable message is reported");
    assert!(
        matches!(error, celers_core::CelersError::Deserialization(_)),
        "expected a deserialization error, got {error:?}"
    );

    let transport = adapter.into_transport();
    assert_eq!(
        transport.log.rejected,
        vec![("tag-poison".to_string(), false)],
        "requeue = false is what routes it to the DLX / redrive policy instead \
         of back onto the queue it would wedge"
    );
}

#[tokio::test]
async fn one_poison_message_does_not_take_its_batch_down_with_it() {
    let mut transport = NativeTransport::default();
    transport.inner.seed(2);
    let mut poison = Message::new("tasks.poison".to_string(), Uuid::new_v4(), Vec::new());
    poison.headers.extra.insert(
        TASK_METADATA_HEADER.to_string(),
        serde_json::json!(["not", "metadata"]),
    );
    transport
        .inner
        .ready
        .push_back(Envelope::new(poison, "tag-poison".to_string()));

    let adapter = KombuBrokerAdapter::new(transport, "celery");
    let messages = adapter.dequeue_batch(3).await.expect("dequeue_batch");

    assert_eq!(
        messages.len(),
        2,
        "the readable messages in the batch are still delivered"
    );

    let transport = adapter.into_transport();
    assert_eq!(
        transport.inner.log.rejected,
        vec![("tag-poison".to_string(), false)],
        "only the poison message is dead-lettered"
    );
}

// =============================================================================
// The shipped mock transport
// =============================================================================

#[tokio::test]
async fn the_mock_transport_is_adaptable_end_to_end() {
    // `MockBroker` requeues onto the hard-coded "celery" queue, so that is the
    // queue this adapter has to use for the requeue leg to be observable.
    let adapter = KombuBrokerAdapter::new(MockBroker::new(), "celery");

    let task_id = adapter
        .enqueue(SerializedTask::new("tasks.mock".to_string(), vec![1]))
        .await
        .expect("enqueue");

    let message = adapter
        .dequeue()
        .await
        .expect("dequeue")
        .expect("a message");
    assert_eq!(message.task.metadata.id, task_id);

    // Rejected with requeue: it comes back.
    adapter
        .reject(&task_id, message.receipt_handle.as_deref(), true)
        .await
        .expect("reject");

    let again = adapter
        .dequeue()
        .await
        .expect("dequeue")
        .expect("a requeued message comes back");
    assert_eq!(again.task.metadata.id, task_id);

    adapter
        .ack(&task_id, again.receipt_handle.as_deref())
        .await
        .expect("ack");
    assert_eq!(adapter.queue_size().await.expect("queue size"), 0);
}

#[tokio::test]
async fn an_explicit_connect_reports_transport_state() {
    let adapter = KombuBrokerAdapter::new(MockBroker::new(), "celery");
    assert!(!adapter.is_connected().await);
    adapter.connect().await.expect("connect");
    assert!(adapter.is_connected().await);
}

#[test]
fn the_adapter_debug_impl_names_its_queue() {
    let adapter = KombuBrokerAdapter::new(MockBroker::new(), "orders");
    let rendered = format!("{adapter:?}");
    assert!(rendered.contains("orders"), "got {rendered}");
    assert_eq!(adapter.queue(), "orders");
    assert_eq!(adapter.poll_timeout(), super::DEFAULT_POLL_TIMEOUT);
}

/// A transport error that is not a serialization problem stays a broker error.
#[tokio::test]
async fn transport_errors_are_reported_as_broker_errors() {
    struct FailingTransport;

    #[async_trait]
    impl Transport for FailingTransport {
        async fn connect(&mut self) -> KombuResult<()> {
            Err(BrokerError::Connection("no route to host".to_string()))
        }

        async fn disconnect(&mut self) -> KombuResult<()> {
            Ok(())
        }

        fn is_connected(&self) -> bool {
            false
        }

        fn name(&self) -> &str {
            "failing"
        }
    }

    #[async_trait]
    impl Producer for FailingTransport {
        async fn publish(&mut self, _queue: &str, _message: Message) -> KombuResult<()> {
            Ok(())
        }

        async fn publish_with_routing(
            &mut self,
            _exchange: &str,
            _routing_key: &str,
            _message: Message,
        ) -> KombuResult<()> {
            Ok(())
        }
    }

    #[async_trait]
    impl Consumer for FailingTransport {
        async fn consume(
            &mut self,
            _queue: &str,
            _timeout: Duration,
        ) -> KombuResult<Option<Envelope>> {
            Ok(None)
        }

        async fn ack(&mut self, _delivery_tag: &str) -> KombuResult<()> {
            Ok(())
        }

        async fn reject(&mut self, _delivery_tag: &str, _requeue: bool) -> KombuResult<()> {
            Ok(())
        }

        async fn queue_size(&mut self, _queue: &str) -> KombuResult<usize> {
            Ok(0)
        }
    }

    #[async_trait]
    impl KombuBroker for FailingTransport {
        async fn purge(&mut self, _queue: &str) -> KombuResult<usize> {
            Ok(0)
        }

        async fn create_queue(&mut self, _queue: &str, _mode: QueueMode) -> KombuResult<()> {
            Ok(())
        }

        async fn delete_queue(&mut self, _queue: &str) -> KombuResult<()> {
            Ok(())
        }

        async fn list_queues(&mut self) -> KombuResult<Vec<String>> {
            Ok(Vec::new())
        }
    }

    impl CoreBrokerTransport for FailingTransport {}

    let adapter = KombuBrokerAdapter::new(FailingTransport, "celery");
    let error = adapter.dequeue().await.expect_err("connection failure");
    assert!(error.is_broker(), "expected a broker error, got {error:?}");
    assert!(
        error.to_string().contains("no route to host"),
        "the transport's own diagnosis must survive: {error}"
    );
}
