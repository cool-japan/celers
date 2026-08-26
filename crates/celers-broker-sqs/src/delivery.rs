// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: Apache-2.0

//! Delivery-tag encoding and receive-request shaping helpers.
//!
//! # Why delivery tags carry their source queue
//!
//! An SQS receipt handle is only valid against the queue the message was
//! received from. The [`Consumer`](celers_kombu::Consumer) trait however hands
//! `ack`/`reject` nothing but the delivery tag, so a broker that resolves the
//! queue URL from a single configured queue name can never acknowledge a
//! message consumed from a *different* queue — the delete fails with
//! `ReceiptHandleIsInvalid`, the message becomes visible again after the
//! visibility timeout, and the task is executed forever.
//!
//! CeleRS therefore encodes the source queue into the delivery tag:
//!
//! ```text
//! {queue-name}\u{1}{receipt-handle}
//! ```
//!
//! `\u{1}` cannot occur in an SQS queue name (`[A-Za-z0-9_-]{1,80}`, optionally
//! suffixed with `.fifo`), so [`decode_delivery_tag`] can split on the *first*
//! separator without ambiguity. Tags produced by older versions (or handed in
//! by a caller directly) simply decode to "no queue recorded" and the caller
//! falls back to its configured queue.

use std::time::Duration;

/// Separator between the source queue name and the receipt handle inside a
/// delivery tag.
///
/// `U+0001` is not a legal character in an SQS queue name, and receipt handles
/// are base64-ish text, so this byte never collides with real data.
pub const QUEUE_TAG_SEPARATOR: char = '\u{1}';

/// Maximum `WaitTimeSeconds` accepted by the SQS `ReceiveMessage` API.
pub const MAX_WAIT_TIME_SECONDS: i32 = 20;

/// Maximum `MaxNumberOfMessages` accepted by the SQS `ReceiveMessage` API.
pub const MAX_RECEIVE_MESSAGES: i32 = 10;

/// Encode a receipt handle together with the queue it was received from.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::delivery::{decode_delivery_tag, encode_delivery_tag};
///
/// let tag = encode_delivery_tag("orders", "AQEB1234");
/// assert_eq!(decode_delivery_tag(&tag), (Some("orders"), "AQEB1234"));
/// ```
pub fn encode_delivery_tag(queue: &str, receipt_handle: &str) -> String {
    let mut tag = String::with_capacity(queue.len() + 1 + receipt_handle.len());
    tag.push_str(queue);
    tag.push(QUEUE_TAG_SEPARATOR);
    tag.push_str(receipt_handle);
    tag
}

/// Split a delivery tag back into `(source queue, receipt handle)`.
///
/// Returns `(None, tag)` when the tag carries no queue information, which lets
/// callers fall back to their configured queue for tags produced elsewhere.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::delivery::decode_delivery_tag;
///
/// // Plain receipt handles round-trip untouched.
/// assert_eq!(decode_delivery_tag("AQEB1234"), (None, "AQEB1234"));
/// ```
pub fn decode_delivery_tag(tag: &str) -> (Option<&str>, &str) {
    match tag.split_once(QUEUE_TAG_SEPARATOR) {
        Some((queue, handle)) if !queue.is_empty() && !handle.is_empty() => (Some(queue), handle),
        _ => (None, tag),
    }
}

/// Resolve the `WaitTimeSeconds` for a `ReceiveMessage` request.
///
/// The effective wait time is the smallest of
///
/// * the caller supplied poll timeout,
/// * the broker's configured [`with_wait_time`](crate::SqsBroker::with_wait_time)
///   value, and
/// * the SQS maximum of 20 seconds.
///
/// Before this existed, `wait_time_seconds` was only ever written into the
/// queue's `ReceiveMessageWaitTimeSeconds` *attribute* at creation time — which
/// AWS ignores whenever the request itself carries an explicit
/// `WaitTimeSeconds`, as both receive paths always did. The configured value
/// was therefore dead.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::delivery::resolve_wait_time;
/// use std::time::Duration;
///
/// // The configured value caps the poll ...
/// assert_eq!(resolve_wait_time(Duration::from_secs(20), 5), 5);
/// // ... and so does the caller's timeout.
/// assert_eq!(resolve_wait_time(Duration::from_secs(2), 20), 2);
/// // SQS never accepts more than 20 seconds.
/// assert_eq!(resolve_wait_time(Duration::from_secs(600), 60), 20);
/// ```
pub fn resolve_wait_time(timeout: Duration, configured_wait_time: i32) -> i32 {
    let requested = i32::try_from(timeout.as_secs()).unwrap_or(MAX_WAIT_TIME_SECONDS);
    let configured = configured_wait_time.clamp(0, MAX_WAIT_TIME_SECONDS);
    requested.clamp(0, MAX_WAIT_TIME_SECONDS).min(configured)
}

/// Clamp a requested receive count into the range SQS accepts.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::delivery::resolve_max_messages;
///
/// assert_eq!(resolve_max_messages(0), 1);
/// assert_eq!(resolve_max_messages(4), 4);
/// assert_eq!(resolve_max_messages(50), 10);
/// ```
pub fn resolve_max_messages(requested: i32) -> i32 {
    requested.clamp(1, MAX_RECEIVE_MESSAGES)
}

/// Metadata recovered from the SQS message *system* attributes.
///
/// These attributes are only present in a `ReceiveMessage` response when they
/// were explicitly requested via `MessageSystemAttributeNames`. Because the
/// broker never asked for them, [`Envelope::redelivered`](celers_kombu::Envelope)
/// used to be permanently `false` and every downstream redelivery heuristic
/// (poison detection, DLQ analytics, replay filtering) was silently inert.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReceiptMetadata {
    /// SQS `MessageId` of the received message, when reported.
    pub message_id: Option<String>,
    /// `ApproximateReceiveCount` — 1 on the first delivery.
    pub receive_count: u32,
    /// `SentTimestamp` in milliseconds since the Unix epoch, when reported.
    pub sent_timestamp_ms: Option<u64>,
}

impl ReceiptMetadata {
    /// Whether this delivery is a redelivery (receive count greater than one).
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_broker_sqs::delivery::ReceiptMetadata;
    ///
    /// let first = ReceiptMetadata { receive_count: 1, ..Default::default() };
    /// assert!(!first.is_redelivered());
    ///
    /// let retry = ReceiptMetadata { receive_count: 3, ..Default::default() };
    /// assert!(retry.is_redelivered());
    /// ```
    pub fn is_redelivered(&self) -> bool {
        self.receive_count > 1
    }

    /// `SentTimestamp` converted to whole seconds since the Unix epoch.
    pub fn sent_timestamp_secs(&self) -> Option<u64> {
        self.sent_timestamp_ms.map(|ms| ms / 1000)
    }
}

/// Extract [`ReceiptMetadata`] from a received SQS message.
pub(crate) fn extract_receipt_metadata(message: &aws_sdk_sqs::types::Message) -> ReceiptMetadata {
    use aws_sdk_sqs::types::MessageSystemAttributeName;

    let attributes = message.attributes();

    let receive_count = attributes
        .and_then(|attrs| attrs.get(&MessageSystemAttributeName::ApproximateReceiveCount))
        .and_then(|value| value.parse::<u32>().ok())
        .unwrap_or(1);

    let sent_timestamp_ms = attributes
        .and_then(|attrs| attrs.get(&MessageSystemAttributeName::SentTimestamp))
        .and_then(|value| value.parse::<u64>().ok());

    ReceiptMetadata {
        message_id: message.message_id().map(str::to_string),
        receive_count,
        sent_timestamp_ms,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_round_trips() {
        let tag = encode_delivery_tag("celers-dlq", "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a");
        assert_eq!(
            decode_delivery_tag(&tag),
            (Some("celers-dlq"), "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a")
        );
    }

    #[test]
    fn tag_round_trips_for_fifo_queue_names() {
        let tag = encode_delivery_tag("orders.fifo", "AQEB-handle");
        assert_eq!(
            decode_delivery_tag(&tag),
            (Some("orders.fifo"), "AQEB-handle")
        );
    }

    #[test]
    fn legacy_plain_handles_decode_without_queue() {
        assert_eq!(decode_delivery_tag("AQEBplain"), (None, "AQEBplain"));
        assert_eq!(decode_delivery_tag(""), (None, ""));
    }

    #[test]
    fn separator_inside_handle_does_not_confuse_decoding() {
        // Only the first separator delimits the queue name.
        let tag = encode_delivery_tag("queue-a", "handle\u{1}with-separator");
        assert_eq!(
            decode_delivery_tag(&tag),
            (Some("queue-a"), "handle\u{1}with-separator")
        );
    }

    #[test]
    fn empty_halves_fall_back_to_the_raw_tag() {
        assert_eq!(decode_delivery_tag("\u{1}handle"), (None, "\u{1}handle"));
        assert_eq!(decode_delivery_tag("queue\u{1}"), (None, "queue\u{1}"));
    }

    #[test]
    fn wait_time_honours_configuration_and_sqs_limit() {
        assert_eq!(resolve_wait_time(Duration::from_secs(20), 20), 20);
        assert_eq!(resolve_wait_time(Duration::from_secs(20), 5), 5);
        assert_eq!(resolve_wait_time(Duration::from_secs(1), 20), 1);
        assert_eq!(resolve_wait_time(Duration::from_secs(3_600), 20), 20);
        assert_eq!(resolve_wait_time(Duration::from_secs(0), 20), 0);
    }

    #[test]
    fn wait_time_survives_absurd_timeouts() {
        assert_eq!(resolve_wait_time(Duration::from_secs(u64::MAX), 20), 20);
        assert_eq!(resolve_wait_time(Duration::from_secs(30), -5), 0);
        assert_eq!(resolve_wait_time(Duration::from_secs(30), 1_000), 20);
    }

    #[test]
    fn max_messages_is_clamped() {
        assert_eq!(resolve_max_messages(-3), 1);
        assert_eq!(resolve_max_messages(1), 1);
        assert_eq!(resolve_max_messages(10), 10);
        assert_eq!(resolve_max_messages(11), 10);
    }

    #[test]
    fn receipt_metadata_redelivery_flag() {
        let metadata = ReceiptMetadata {
            message_id: Some("m-1".to_string()),
            receive_count: 1,
            sent_timestamp_ms: Some(1_700_000_000_500),
        };
        assert!(!metadata.is_redelivered());
        assert_eq!(metadata.sent_timestamp_secs(), Some(1_700_000_000));

        let redelivered = ReceiptMetadata {
            receive_count: 2,
            ..metadata
        };
        assert!(redelivered.is_redelivered());
    }

    #[test]
    fn extract_metadata_from_sqs_message() {
        use aws_sdk_sqs::types::MessageSystemAttributeName;

        let message = aws_sdk_sqs::types::Message::builder()
            .message_id("m-42")
            .receipt_handle("AQEB")
            .body("{}")
            .attributes(
                MessageSystemAttributeName::ApproximateReceiveCount,
                "4".to_string(),
            )
            .attributes(
                MessageSystemAttributeName::SentTimestamp,
                "1700000000500".to_string(),
            )
            .build();

        let metadata = extract_receipt_metadata(&message);
        assert_eq!(metadata.message_id.as_deref(), Some("m-42"));
        assert_eq!(metadata.receive_count, 4);
        assert!(metadata.is_redelivered());
        assert_eq!(metadata.sent_timestamp_ms, Some(1_700_000_000_500));
    }

    #[test]
    fn extract_metadata_defaults_when_attributes_absent() {
        let message = aws_sdk_sqs::types::Message::builder()
            .receipt_handle("AQEB")
            .body("{}")
            .build();

        let metadata = extract_receipt_metadata(&message);
        assert_eq!(metadata.receive_count, 1);
        assert!(!metadata.is_redelivered());
        assert_eq!(metadata.sent_timestamp_ms, None);
    }
}
