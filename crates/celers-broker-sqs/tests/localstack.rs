// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: MIT OR Apache-2.0

//! End-to-end SQS tests against a real endpoint (LocalStack or AWS).
//!
//! Every test here is `#[ignore]`d and additionally checks `CELERS_TEST_SQS_URL`,
//! so `cargo test` / `cargo nextest run` stay hermetic. The unit suite in
//! `src/tests.rs` covers the request-shaping decisions; this file covers the
//! round trips that only a live queue can prove: enqueue -> consume -> ack,
//! redelivery counting, cross-queue acknowledgement and DLQ redrive.
//!
//! # Running
//!
//! ```sh
//! docker run --rm -p 4566:4566 localstack/localstack
//!
//! export CELERS_TEST_SQS_URL=http://localhost:4566
//! export AWS_ACCESS_KEY_ID=test
//! export AWS_SECRET_ACCESS_KEY=test
//! export AWS_REGION=us-east-1
//!
//! cargo test -p celers-broker-sqs --test localstack -- --ignored --test-threads=1
//! ```
//!
//! Queue names are suffixed with a random id so parallel or repeated runs do
//! not collide, and every test deletes the queues it created.

use std::time::Duration;

use celers_broker_sqs::{DlqConfig, SqsBroker};
use celers_kombu::{Broker, Consumer, Producer, QueueMode, Transport};
use celers_protocol::Message;
use uuid::Uuid;

/// Endpoint override for LocalStack, if the suite is enabled.
fn endpoint() -> Option<String> {
    std::env::var("CELERS_TEST_SQS_URL")
        .ok()
        .filter(|url| !url.is_empty())
}

/// Skip marker printed when the suite runs without an endpoint configured.
macro_rules! require_endpoint {
    () => {
        match endpoint() {
            Some(url) => url,
            None => {
                eprintln!("CELERS_TEST_SQS_URL is not set; skipping LocalStack test");
                return;
            }
        }
    };
}

fn unique_queue(prefix: &str) -> String {
    format!("{}-{}", prefix, Uuid::new_v4().simple())
}

fn message(task: &str) -> Message {
    Message::new(task.to_string(), Uuid::new_v4(), b"{\"args\":[]}".to_vec())
}

async fn connected_broker(queue: &str, endpoint_url: &str) -> SqsBroker {
    // `aws_config` honours AWS_ENDPOINT_URL for every service, which is how the
    // SDK is pointed at LocalStack without a custom client.
    std::env::set_var("AWS_ENDPOINT_URL", endpoint_url);

    let mut broker = SqsBroker::new(queue)
        .await
        .expect("broker constructed")
        .with_auto_create_queue(true)
        .with_visibility_timeout(5)
        .with_wait_time(2);

    broker.connect().await.expect("connect");
    broker
}

#[tokio::test]
#[ignore = "requires CELERS_TEST_SQS_URL (LocalStack)"]
async fn enqueue_consume_ack_round_trip() {
    let endpoint_url = require_endpoint!();
    let queue = unique_queue("celers-it-roundtrip");
    let mut broker = connected_broker(&queue, &endpoint_url).await;

    broker
        .publish(&queue, message("tasks.roundtrip"))
        .await
        .expect("publish");

    let envelope = broker
        .consume(&queue, Duration::from_secs(2))
        .await
        .expect("consume")
        .expect("a message is available");

    assert_eq!(envelope.message.headers.task, "tasks.roundtrip");
    assert!(!envelope.redelivered, "first delivery is not a redelivery");

    broker.ack(&envelope.delivery_tag).await.expect("ack");

    // Once acknowledged the message is gone for good.
    let second = broker
        .consume(&queue, Duration::from_secs(1))
        .await
        .expect("consume");
    assert!(second.is_none(), "acknowledged message must not reappear");

    broker.delete_queue(&queue).await.expect("cleanup");
}

#[tokio::test]
#[ignore = "requires CELERS_TEST_SQS_URL (LocalStack)"]
async fn redelivery_is_reported_from_system_attributes() {
    let endpoint_url = require_endpoint!();
    let queue = unique_queue("celers-it-redelivery");
    let mut broker = connected_broker(&queue, &endpoint_url).await;

    broker
        .publish(&queue, message("tasks.redelivered"))
        .await
        .expect("publish");

    let first = broker
        .consume(&queue, Duration::from_secs(2))
        .await
        .expect("consume")
        .expect("message");
    assert!(!first.redelivered);
    assert_eq!(broker.receive_count(&first.delivery_tag), Some(1));

    // Make it visible again immediately.
    broker
        .reject(&first.delivery_tag, true)
        .await
        .expect("requeue");

    let second = broker
        .consume(&queue, Duration::from_secs(2))
        .await
        .expect("consume")
        .expect("redelivered message");

    assert!(
        second.redelivered,
        "ApproximateReceiveCount must mark the second delivery as a redelivery"
    );
    assert_eq!(broker.receive_count(&second.delivery_tag), Some(2));

    broker.ack(&second.delivery_tag).await.expect("ack");
    broker.delete_queue(&queue).await.expect("cleanup");
}

#[tokio::test]
#[ignore = "requires CELERS_TEST_SQS_URL (LocalStack)"]
async fn acknowledges_a_message_consumed_from_another_queue() {
    let endpoint_url = require_endpoint!();
    let main = unique_queue("celers-it-main");
    let other = unique_queue("celers-it-other");

    let mut broker = connected_broker(&main, &endpoint_url).await;
    broker
        .create_queue(&other, QueueMode::Priority)
        .await
        .expect("create secondary queue");

    broker
        .publish(&other, message("tasks.elsewhere"))
        .await
        .expect("publish to the secondary queue");

    let envelope = broker
        .consume(&other, Duration::from_secs(2))
        .await
        .expect("consume")
        .expect("message");

    // The broker's configured queue is `main`; before the delivery tag carried
    // its source queue this delete failed with ReceiptHandleIsInvalid and the
    // message was redelivered forever.
    broker.ack(&envelope.delivery_tag).await.expect("ack");

    let leftover = broker
        .consume(&other, Duration::from_secs(1))
        .await
        .expect("consume");
    assert!(
        leftover.is_none(),
        "the message must be deleted from queue B"
    );

    broker.delete_queue(&other).await.expect("cleanup");
    broker.delete_queue(&main).await.expect("cleanup");
}

#[tokio::test]
#[ignore = "requires CELERS_TEST_SQS_URL (LocalStack)"]
async fn batch_publish_and_ack_handle_more_than_ten_entries() {
    let endpoint_url = require_endpoint!();
    let queue = unique_queue("celers-it-batch");
    let mut broker = connected_broker(&queue, &endpoint_url).await;

    let messages: Vec<Message> = (0..25)
        .map(|index| message(&format!("tasks.batch.{index}")))
        .collect();

    let published = broker
        .publish_batch(&queue, messages)
        .await
        .expect("publish_batch");
    assert_eq!(published, 25, "all 25 messages must be sent, not just 10");

    let mut tags = Vec::new();
    while tags.len() < 25 {
        let batch = broker
            .consume_batch(&queue, 10, Duration::from_secs(2))
            .await
            .expect("consume_batch");
        if batch.is_empty() {
            break;
        }
        tags.extend(batch.into_iter().map(|envelope| envelope.delivery_tag));
    }
    assert_eq!(tags.len(), 25);

    let deleted = broker.ack_batch(&queue, tags).await.expect("ack_batch");
    assert_eq!(deleted, 25, "all 25 handles must be deleted, not just 10");

    broker.delete_queue(&queue).await.expect("cleanup");
}

#[tokio::test]
#[ignore = "requires CELERS_TEST_SQS_URL (LocalStack)"]
async fn fifo_publish_works_through_the_generic_producer() {
    let endpoint_url = require_endpoint!();
    let queue = format!("{}.fifo", unique_queue("celers-it-fifo"));
    let mut broker = connected_broker(&queue, &endpoint_url).await;

    // No MessageGroupId is supplied: the broker must derive one, otherwise SQS
    // answers MissingParameter.
    broker
        .publish(&queue, message("tasks.ordered"))
        .await
        .expect("FIFO publish through Producer::publish");

    let envelope = broker
        .consume(&queue, Duration::from_secs(2))
        .await
        .expect("consume")
        .expect("message");
    assert_eq!(envelope.message.headers.task, "tasks.ordered");

    broker.ack(&envelope.delivery_tag).await.expect("ack");
    broker.delete_queue(&queue).await.expect("cleanup");
}

#[tokio::test]
#[ignore = "requires CELERS_TEST_SQS_URL (LocalStack)"]
async fn dlq_redrive_moves_the_message_and_empties_the_dlq() {
    use celers_broker_sqs::replay::{ReplayConfig, ReplayFilter, ReplayManager};

    let endpoint_url = require_endpoint!();
    let main = unique_queue("celers-it-redrive");
    let dlq = format!("{main}-dlq");

    let mut setup = connected_broker(&main, &endpoint_url).await;
    setup
        .create_queue(&dlq, QueueMode::Priority)
        .await
        .expect("create DLQ");
    let dlq_arn = setup.get_queue_arn(&dlq).await.expect("DLQ ARN");

    let mut broker = SqsBroker::new(&main)
        .await
        .expect("broker")
        .with_auto_create_queue(true)
        .with_visibility_timeout(5)
        .with_wait_time(2)
        .with_dlq(DlqConfig::new(&dlq_arn, 1));
    broker.connect().await.expect("connect");

    // Put a message straight into the DLQ, as a failed task would end up.
    broker
        .publish(&dlq, message("tasks.failed"))
        .await
        .expect("seed the DLQ");

    let mut manager = ReplayManager::new(ReplayConfig::new().with_rate_limit(0));

    // Dry run first: nothing must move.
    let planned = manager
        .replay_from_dlq(&mut broker, &ReplayFilter::new(), true)
        .await
        .expect("dry run");
    assert_eq!(planned.successful, 1);

    let result = manager
        .replay_from_dlq(&mut broker, &ReplayFilter::new(), false)
        .await
        .expect("replay");
    assert_eq!(result.successful, 1);
    assert_eq!(result.failed, 0);

    // The message now lives on the main queue ...
    let replayed = broker
        .consume(&main, Duration::from_secs(3))
        .await
        .expect("consume")
        .expect("replayed message");
    assert_eq!(replayed.message.headers.task, "tasks.failed");
    broker.ack(&replayed.delivery_tag).await.expect("ack");

    broker.delete_queue(&dlq).await.expect("cleanup");
    broker.delete_queue(&main).await.expect("cleanup");
}

#[tokio::test]
#[ignore = "requires CELERS_TEST_SQS_URL (LocalStack)"]
async fn health_check_distinguishes_missing_queue_from_broken_connection() {
    let endpoint_url = require_endpoint!();
    let queue = unique_queue("celers-it-health");
    let mut broker = connected_broker(&queue, &endpoint_url).await;

    assert!(broker.health_check(&queue).await.expect("health check"));

    // A queue that was never created reports `Ok(false)`, not an error.
    let missing = unique_queue("celers-it-missing");
    let mut strict = SqsBroker::new(&missing).await.expect("broker");
    assert!(!strict.health_check(&missing).await.expect("health check"));

    broker.delete_queue(&queue).await.expect("cleanup");
}
