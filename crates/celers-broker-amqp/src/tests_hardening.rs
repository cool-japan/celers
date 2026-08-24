//! Regression tests for the AMQP broker hardening pass.
//!
//! The tests that need a live RabbitMQ are gated on the `CELERS_TEST_AMQP_URL`
//! environment variable and skip themselves (rather than fail) when it is
//! unset, so the suite stays green on a machine without a broker.

#![cfg(test)]

use crate::confirm::classify_confirmation;
use crate::connect::build_uri;
use crate::types::AmqpConfig;
use crate::AmqpBroker;
use celers_kombu::{Broker, Consumer, Envelope, Producer, Transport};
use celers_protocol::builder::MessageBuilder;
use lapin::Confirmation;
use std::time::Duration;

/// AMQP URL of a live broker to run integration tests against, if any.
fn integration_url() -> Option<String> {
    std::env::var("CELERS_TEST_AMQP_URL")
        .ok()
        .filter(|url| !url.is_empty())
}

fn test_message(task: &str) -> celers_protocol::Message {
    MessageBuilder::new(task)
        .build()
        .expect("building a test message must not fail")
}

// ==================== RPC (idx 193 / 220) ====================

#[test]
fn reply_queue_name_is_derived_from_the_correlation_id() {
    let correlation_id = "6f1e6a9c-0000-4000-8000-000000000001";
    assert_eq!(
        AmqpBroker::reply_queue_for(correlation_id),
        format!("reply.{}", correlation_id)
    );
}

#[test]
fn rpc_reply_target_reads_reply_to_and_never_reconstructs_it() {
    let mut message = test_message("tasks.calculate");
    message.properties.correlation_id = Some("corr-1".to_string());
    // A reply address that is deliberately NOT `reply.{correlation_id}`:
    // the requester's `reply_to` is authoritative.
    message.properties.reply_to = Some("amq.rabbitmq.reply-to.abc".to_string());

    let envelope = Envelope {
        delivery_tag: "1".to_string(),
        message,
        redelivered: false,
    };

    let (correlation_id, reply_to) =
        AmqpBroker::rpc_reply_target(&envelope).expect("reply target must resolve");
    assert_eq!(correlation_id, "corr-1");
    assert_eq!(reply_to, "amq.rabbitmq.reply-to.abc");
}

#[test]
fn rpc_reply_target_requires_reply_to() {
    let mut message = test_message("tasks.calculate");
    message.properties.correlation_id = Some("corr-1".to_string());
    // No reply_to: the destination must not be fabricated from the id.
    let envelope = Envelope {
        delivery_tag: "1".to_string(),
        message,
        redelivered: false,
    };

    let err = AmqpBroker::rpc_reply_target(&envelope)
        .expect_err("a request without reply_to cannot be answered");
    assert!(err.to_string().contains("reply_to"));
}

#[test]
fn rpc_reply_target_requires_correlation_id() {
    let mut message = test_message("tasks.calculate");
    // The builder fills in a correlation id by default; clear it explicitly.
    message.properties.correlation_id = None;
    message.properties.reply_to = Some("reply.q".to_string());
    let envelope = Envelope {
        delivery_tag: "1".to_string(),
        message,
        redelivered: false,
    };

    let err = AmqpBroker::rpc_reply_target(&envelope)
        .expect_err("a request without correlation_id cannot be answered");
    assert!(err.to_string().contains("correlation_id"));
}

/// A `reply_to` that only lives in the AMQP frame is invisible to a server
/// that deserializes the JSON body, so it has to survive serialization.
#[test]
fn reply_to_survives_message_serialization() {
    let mut message = test_message("tasks.calculate");
    message.properties.correlation_id = Some("corr-2".to_string());
    message.properties.reply_to = Some(AmqpBroker::reply_queue_for("corr-2"));

    let payload = serde_json::to_vec(&message).expect("serialize");
    let decoded: celers_protocol::Message = serde_json::from_slice(&payload).expect("deserialize");

    assert_eq!(
        decoded.properties.reply_to,
        Some("reply.corr-2".to_string())
    );
    assert_eq!(
        decoded.properties.correlation_id,
        Some("corr-2".to_string())
    );

    let envelope = Envelope {
        delivery_tag: "7".to_string(),
        message: decoded,
        redelivered: false,
    };
    let (correlation_id, reply_to) =
        AmqpBroker::rpc_reply_target(&envelope).expect("reply target must resolve");
    assert_eq!(correlation_id, "corr-2");
    assert_eq!(reply_to, AmqpBroker::reply_queue_for(&correlation_id));
}

// ==================== Publisher confirms (idx 214 / 298) ====================

#[test]
fn a_nack_is_never_counted_as_a_successful_confirm() {
    assert!(classify_confirmation(Confirmation::Nack(None), true).is_err());
}

#[test]
fn an_unconfirmed_publish_is_not_a_successful_confirm() {
    assert!(classify_confirmation(Confirmation::NotRequested, true).is_err());
}

#[test]
fn publisher_confirms_are_enabled_by_default_and_configurable() {
    let config = AmqpConfig::default();
    assert!(config.publisher_confirms);
    assert!(!config.mandatory_publish);

    let config = AmqpConfig::default()
        .with_publisher_confirms(false)
        .with_mandatory_publish(true);
    assert!(!config.publisher_confirms);
    assert!(config.mandatory_publish);
}

#[tokio::test]
async fn confirms_are_suppressed_while_a_transaction_is_active() {
    let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_queue")
        .await
        .expect("broker construction is offline");

    assert!(broker.confirms_enabled());

    broker.transaction_state = crate::types::TransactionState::Started;
    assert!(
        !broker.confirms_enabled(),
        "confirm.select and tx.select are mutually exclusive"
    );

    broker.transaction_state = crate::types::TransactionState::Committed;
    assert!(broker.confirms_enabled());
}

// ==================== Channel health (idx 215) ====================

#[tokio::test]
async fn a_missing_channel_is_not_reported_as_alive() {
    let broker = AmqpBroker::new("amqp://localhost:5672", "test_queue")
        .await
        .expect("broker construction is offline");
    assert!(!broker.channel_alive());
    assert!(!broker.is_healthy());
}

#[tokio::test]
async fn discarding_a_channel_drops_its_subscriptions() {
    let mut broker = AmqpBroker::new("amqp://localhost:5672", "test_queue")
        .await
        .expect("broker construction is offline");

    broker.channel_confirm_mode = true;
    assert!(broker.consumers.is_empty());

    broker.discard_channel();
    assert!(broker.channel.is_none());
    assert!(broker.consumers.is_empty());
    assert!(!broker.channel_confirm_mode);
}

// ==================== Heartbeat / connection timeout (idx 235) ====================

#[test]
fn configured_heartbeat_and_timeout_reach_the_connection_uri() {
    let config = AmqpConfig::default()
        .with_heartbeat(11)
        .with_connection_timeout(Duration::from_millis(1234));

    let uri = build_uri(
        "amqp://localhost:5672",
        config.heartbeat,
        config.connection_timeout,
    )
    .expect("valid uri");

    assert_eq!(uri.query.heartbeat, Some(11));
    assert_eq!(uri.query.connection_timeout, Some(1234));
}

#[tokio::test]
async fn connecting_to_a_black_hole_gives_up_after_the_configured_timeout() {
    // 203.0.113.0/24 is TEST-NET-3: guaranteed not routable, so the connect
    // attempt hangs until *something* times it out. Without the configured
    // timeout that would be the OS TCP timeout (minutes).
    let config = AmqpConfig::default().with_connection_timeout(Duration::from_millis(150));
    let mut broker = AmqpBroker::with_config("amqp://203.0.113.1:5672", "test_queue", config)
        .await
        .expect("broker construction is offline");

    let started = std::time::Instant::now();
    let result = broker.connect().await;
    let elapsed = started.elapsed();

    assert!(result.is_err(), "connecting to TEST-NET-3 must fail");
    assert!(
        elapsed < Duration::from_secs(10),
        "connect took {:?}; the configured timeout was not applied",
        elapsed
    );
}

// ==================== Prefetch (idx 299) ====================

#[test]
fn unlimited_prefetch_is_bounded_for_pushing_consumers() {
    // `0` means "broker decides", which for basic.consume is unbounded.
    assert_eq!(
        AmqpConfig::default().effective_prefetch_count(),
        AmqpConfig::DEFAULT_PREFETCH_COUNT
    );
    assert_eq!(
        AmqpConfig::default()
            .with_prefetch(7)
            .effective_prefetch_count(),
        7
    );
}

#[test]
fn consume_defaults_to_a_subscription_not_polling() {
    let config = AmqpConfig::default();
    assert!(!config.poll_mode);
    assert!(config.with_poll_mode(true).poll_mode);
}

// ==================== Live-broker integration tests ====================

/// End-to-end RPC round trip: `rpc_call` must receive the reply that
/// `rpc_reply` publishes. Before the fix the reply queue name and the
/// correlation id were unrelated UUIDs and every call timed out.
#[tokio::test]
async fn rpc_round_trip_against_a_live_broker() {
    let Some(url) = integration_url() else {
        return; // No broker configured: nothing to verify here.
    };

    let queue = format!("celers_test_rpc_{}", uuid::Uuid::new_v4());

    let mut server = AmqpBroker::new(&url, &queue)
        .await
        .expect("server broker construction");
    server.connect().await.expect("server connect");

    let mut client = AmqpBroker::new(&url, &queue)
        .await
        .expect("client broker construction");
    client.connect().await.expect("client connect");

    let request = test_message("tasks.echo");
    let queue_for_client = queue.clone();
    let client_task = tokio::spawn(async move {
        client
            .rpc_call(&queue_for_client, request, Duration::from_secs(10))
            .await
    });

    // Serve exactly one request.
    let mut served = false;
    for _ in 0..50 {
        if let Some(envelope) = server
            .consume(&queue, Duration::from_millis(200))
            .await
            .expect("server consume")
        {
            let reply = test_message("tasks.echo.reply");
            server
                .rpc_reply(&envelope, reply)
                .await
                .expect("server rpc_reply");
            server
                .ack(&envelope.delivery_tag)
                .await
                .expect("server ack");
            served = true;
            break;
        }
    }
    assert!(served, "the RPC server never saw the request");

    let reply = client_task
        .await
        .expect("client task")
        .expect("rpc_call must receive the reply");
    assert_eq!(reply.headers.task, "tasks.echo.reply");

    let _ = server.delete_queue(&queue).await;
}

/// `peek_queue` must return distinct messages, not the same one repeatedly.
#[tokio::test]
async fn peek_queue_returns_distinct_messages_against_a_live_broker() {
    let Some(url) = integration_url() else {
        return;
    };

    let queue = format!("celers_test_peek_{}", uuid::Uuid::new_v4());
    let mut broker = AmqpBroker::new(&url, &queue)
        .await
        .expect("broker construction");
    broker.connect().await.expect("connect");

    for idx in 0..3 {
        broker
            .publish(&queue, test_message(&format!("tasks.peek.{}", idx)))
            .await
            .expect("publish");
    }

    let peeked = broker.peek_queue(&queue, 10).await.expect("peek");
    assert_eq!(peeked.len(), 3, "peek returned {:?}", peeked.len());

    let mut ids: Vec<String> = peeked.iter().map(|m| m.headers.id.to_string()).collect();
    ids.sort();
    ids.dedup();
    assert_eq!(ids.len(), 3, "peek returned the same message repeatedly");

    // The messages must still be in the queue afterwards.
    assert_eq!(broker.queue_size(&queue).await.unwrap_or(0), 3);

    let _ = broker.delete_queue(&queue).await;
}
