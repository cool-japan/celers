// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: Apache-2.0

//! FIFO identifier derivation.
//!
//! SQS rejects every `SendMessage` to a FIFO queue that does not carry a
//! `MessageGroupId`. The generic [`Producer`](celers_kombu::Producer) interface
//! — the one the worker and kombu layers actually call — has nowhere to put
//! one, so the broker has to *derive* it from the message and its own
//! configuration. [`FifoGroupIdSource`]
//! selects the derivation; the functions here implement it and are pure so
//! they can be unit tested without AWS.
//!
//! The deduplication id is derived from the task id, which makes a retried
//! `SendMessage` genuinely idempotent on a FIFO queue (within the 5-minute
//! deduplication interval). Standard queues have no equivalent and therefore
//! remain at-least-once.

use celers_protocol::Message;

use crate::types::FifoGroupIdSource;

/// Maximum length of a `MessageGroupId` / `MessageDeduplicationId` (SQS limit).
pub const MAX_FIFO_ID_LEN: usize = 128;

/// Fallback identifier used when derivation would produce an empty string.
pub const FALLBACK_FIFO_ID: &str = "celers-default";

/// Normalise a string into a value SQS accepts as a FIFO identifier.
///
/// SQS allows alphanumeric characters and the punctuation set
/// ``!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~``, up to 128 characters. Anything else
/// (whitespace, control characters, non-ASCII) is replaced with `-`.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::fifo::sanitize_fifo_id;
///
/// assert_eq!(sanitize_fifo_id("tasks.add"), "tasks.add");
/// assert_eq!(sanitize_fifo_id("group one"), "group-one");
/// assert_eq!(sanitize_fifo_id(""), "celers-default");
/// ```
pub fn sanitize_fifo_id(raw: &str) -> String {
    let sanitized: String = raw
        .chars()
        .take(MAX_FIFO_ID_LEN)
        .map(|c| {
            if c.is_ascii_alphanumeric() || is_allowed_fifo_punctuation(c) {
                c
            } else {
                '-'
            }
        })
        .collect();

    if sanitized.is_empty() {
        FALLBACK_FIFO_ID.to_string()
    } else {
        sanitized
    }
}

fn is_allowed_fifo_punctuation(c: char) -> bool {
    matches!(
        c,
        '!' | '"'
            | '#'
            | '$'
            | '%'
            | '&'
            | '\''
            | '('
            | ')'
            | '*'
            | '+'
            | ','
            | '-'
            | '.'
            | '/'
            | ':'
            | ';'
            | '<'
            | '='
            | '>'
            | '?'
            | '@'
            | '['
            | '\\'
            | ']'
            | '^'
            | '_'
            | '`'
            | '{'
            | '|'
            | '}'
            | '~'
    )
}

/// Derive the `MessageGroupId` for a message published to a FIFO queue.
///
/// `default_group_id` is the broker's
/// [`FifoConfig::default_message_group_id`](crate::types::FifoConfig) and acts
/// as the final fallback for every source.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::fifo::derive_message_group_id;
/// use celers_broker_sqs::types::FifoGroupIdSource;
/// use celers_protocol::Message;
/// use uuid::Uuid;
///
/// let message = Message::new("tasks.add".to_string(), Uuid::new_v4(), Vec::new());
///
/// assert_eq!(
///     derive_message_group_id(&FifoGroupIdSource::PerTaskName, "orders.fifo", &message, None),
///     "tasks.add"
/// );
/// assert_eq!(
///     derive_message_group_id(&FifoGroupIdSource::PerQueue, "orders.fifo", &message, None),
///     "orders.fifo"
/// );
/// assert_eq!(
///     derive_message_group_id(
///         &FifoGroupIdSource::Fixed("global".to_string()),
///         "orders.fifo",
///         &message,
///         None
///     ),
///     "global"
/// );
/// ```
pub fn derive_message_group_id(
    source: &FifoGroupIdSource,
    queue: &str,
    message: &Message,
    default_group_id: Option<&str>,
) -> String {
    let raw = match source {
        FifoGroupIdSource::Fixed(value) => value.clone(),
        FifoGroupIdSource::PerQueue => queue.to_string(),
        FifoGroupIdSource::PerTaskName => message.headers.task.clone(),
        FifoGroupIdSource::MessageGroup => match message.headers.group {
            Some(group) => group.to_string(),
            None => message.headers.task.clone(),
        },
    };

    let raw = if raw.trim().is_empty() {
        default_group_id.unwrap_or("").to_string()
    } else {
        raw
    };

    sanitize_fifo_id(raw.trim())
}

/// Derive the `MessageDeduplicationId` for a FIFO publish.
///
/// * An explicitly supplied id always wins.
/// * When the queue has content-based deduplication enabled, `None` is
///   returned and SQS hashes the body itself.
/// * Otherwise the task id is used. Unlike the previous `Uuid::new_v4()`, this
///   is *stable* across retries of the same logical publish, so a retried
///   `SendMessage` is deduplicated by SQS instead of enqueuing the task twice.
///
/// # Examples
///
/// ```
/// use celers_broker_sqs::fifo::derive_deduplication_id;
/// use celers_protocol::Message;
/// use uuid::Uuid;
///
/// let message = Message::new("tasks.add".to_string(), Uuid::new_v4(), Vec::new());
///
/// // Content-based deduplication: let SQS do it.
/// assert_eq!(derive_deduplication_id(true, None, &message), None);
///
/// // Otherwise the (stable) task id is used.
/// assert_eq!(
///     derive_deduplication_id(false, None, &message),
///     Some(message.headers.id.to_string())
/// );
///
/// // An explicit id always wins.
/// assert_eq!(
///     derive_deduplication_id(true, Some("explicit"), &message),
///     Some("explicit".to_string())
/// );
/// ```
pub fn derive_deduplication_id(
    content_based_deduplication: bool,
    explicit: Option<&str>,
    message: &Message,
) -> Option<String> {
    if let Some(value) = explicit {
        return Some(sanitize_fifo_id(value));
    }

    if content_based_deduplication {
        return None;
    }

    Some(sanitize_fifo_id(&message.headers.id.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    fn message_with_group(task: &str, group: Option<Uuid>) -> Message {
        let mut message = Message::new(task.to_string(), Uuid::new_v4(), Vec::new());
        message.headers.group = group;
        message
    }

    #[test]
    fn sanitize_keeps_allowed_characters() {
        assert_eq!(
            sanitize_fifo_id("tasks.payment.process"),
            "tasks.payment.process"
        );
        assert_eq!(sanitize_fifo_id("a-b_c:d"), "a-b_c:d");
    }

    #[test]
    fn sanitize_replaces_disallowed_characters() {
        assert_eq!(sanitize_fifo_id("group one"), "group-one");
        assert_eq!(sanitize_fifo_id("日本語"), "---");
        assert_eq!(sanitize_fifo_id("line\nbreak"), "line-break");
    }

    #[test]
    fn sanitize_truncates_and_falls_back() {
        let long = "a".repeat(300);
        assert_eq!(sanitize_fifo_id(&long).len(), MAX_FIFO_ID_LEN);
        assert_eq!(sanitize_fifo_id(""), FALLBACK_FIFO_ID);
    }

    #[test]
    fn group_id_from_message_group_header() {
        let group = Uuid::new_v4();
        let message = message_with_group("tasks.add", Some(group));

        assert_eq!(
            derive_message_group_id(&FifoGroupIdSource::MessageGroup, "q.fifo", &message, None),
            group.to_string()
        );
    }

    #[test]
    fn group_id_falls_back_to_task_name() {
        let message = message_with_group("tasks.add", None);
        assert_eq!(
            derive_message_group_id(&FifoGroupIdSource::MessageGroup, "q.fifo", &message, None),
            "tasks.add"
        );
    }

    #[test]
    fn group_id_uses_configured_default_when_source_is_empty() {
        let message = message_with_group("", None);
        assert_eq!(
            derive_message_group_id(
                &FifoGroupIdSource::PerTaskName,
                "q.fifo",
                &message,
                Some("fallback-group")
            ),
            "fallback-group"
        );
    }

    #[test]
    fn group_id_never_empty() {
        let message = message_with_group("", None);
        assert_eq!(
            derive_message_group_id(&FifoGroupIdSource::PerTaskName, "q.fifo", &message, None),
            FALLBACK_FIFO_ID
        );
    }

    #[test]
    fn dedup_id_is_stable_across_calls() {
        let message = message_with_group("tasks.add", None);
        let first = derive_deduplication_id(false, None, &message);
        let second = derive_deduplication_id(false, None, &message);

        assert_eq!(first, second);
        assert_eq!(first, Some(message.headers.id.to_string()));
    }

    #[test]
    fn dedup_id_absent_for_content_based_queues() {
        let message = message_with_group("tasks.add", None);
        assert_eq!(derive_deduplication_id(true, None, &message), None);
    }
}
