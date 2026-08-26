// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: Apache-2.0

//! Conversion between the task-queue model and the wire model.
//!
//! [`celers_core::SerializedTask`] is what a worker executes;
//! [`celers_protocol::Message`] is what a transport carries. The two overlap
//! (name, id, priority, expiry) but neither contains the other, so a round trip
//! through the overlap alone would silently drop `max_retries`, the retry
//! state, `timeout_secs`, chord/dependency wiring and the producer's signature.
//!
//! [`task_to_message`] therefore does both things at once:
//!
//! * it fills the [`celers_protocol::MessageHeaders`] a transport (and a human reading the
//!   queue) can act on, and
//! * it stores the complete [`TaskMetadata`] as JSON under the
//!   [`TASK_METADATA_HEADER`] header, so [`message_to_task`] can restore the
//!   task exactly as it was enqueued.
//!
//! # This is not Celery interop
//!
//! The extra header makes a CeleRS → CeleRS round trip lossless. It does **not**
//! make the envelope a Celery task message: the body is the task's own payload
//! rather than Celery's `[args, kwargs, embed]` triple, and a Python worker
//! reading this queue would not understand it. `docs/CELERY_COMPATIBILITY.md`
//! marks both transports "not interoperable", and this module does not change
//! that — it changes whether a *`celers_worker::Worker`* can consume from them.
//!
//! # Foreign messages
//!
//! A message that carries no [`TASK_METADATA_HEADER`] is still accepted:
//! [`message_to_task`] reconstructs what the headers do say (name, id,
//! priority, expiry, group, creation time) and hands the body over as the
//! payload. Refusing it would turn any producer that is not this adapter into
//! an unreadable queue.

use celers_core::{SerializedTask, TaskMetadata, TaskState};
use celers_protocol::Message;

use crate::{BrokerError, Result};

/// Header under which the full [`TaskMetadata`] travels, as JSON.
///
/// Namespaced so it cannot collide with a Celery header. It is written into
/// [`celers_protocol::MessageHeaders::extra`], which is `#[serde(flatten)]`ed, so on the wire it
/// is a plain top-level field of the headers object.
pub const TASK_METADATA_HEADER: &str = "celers_task_metadata";

/// Highest priority AMQP (and this crate's [`celers_protocol::MessageProperties`]) can express.
const MAX_WIRE_PRIORITY: i32 = 9;

/// Map a task-queue priority onto the 0-9 range the wire model allows.
///
/// Task priorities are `i32` and unbounded; `MessageProperties::priority` is a
/// `u8` that [`celers_protocol::MessageProperties::validate`] rejects above 9.
/// Clamping is the only lossless-where-it-matters option: the embedded metadata
/// still carries the exact original value, so the clamp affects broker-side
/// ordering only.
///
/// # Examples
///
/// ```
/// use celers_kombu::core_adapter::wire_priority;
///
/// assert_eq!(wire_priority(0), 0);
/// assert_eq!(wire_priority(5), 5);
/// // Out-of-range values are clamped, never wrapped.
/// assert_eq!(wire_priority(1_000), 9);
/// assert_eq!(wire_priority(-7), 0);
/// ```
#[must_use]
pub fn wire_priority(priority: i32) -> u8 {
    // `as` is safe here only because of the clamp above it.
    priority.clamp(0, MAX_WIRE_PRIORITY) as u8
}

/// How many retries a task has already spent, if any.
///
/// Celery's `retries` header counts attempts already made, which is what
/// [`TaskState::Retrying`] records. Any other state means "not retried yet",
/// which is `None` rather than `Some(0)`: the header is
/// `skip_serializing_if = "Option::is_none"`, so `None` keeps a first-attempt
/// envelope free of a field that says nothing.
fn spent_retries(state: &TaskState) -> Option<u32> {
    match state {
        TaskState::Retrying(count) => Some(*count),
        _ => None,
    }
}

/// Build the wire message for a task, preserving every metadata field.
///
/// # Errors
///
/// [`BrokerError::Serialization`] if the metadata cannot be encoded as JSON.
/// In practice [`TaskMetadata`] always can; the error exists rather than a
/// panic because the task's `signature` field holds producer-supplied data.
///
/// # Examples
///
/// ```
/// use celers_core::SerializedTask;
/// use celers_kombu::core_adapter::{message_to_task, task_to_message};
///
/// let task = SerializedTask::new("tasks.add".to_string(), vec![1, 2, 3])
///     .with_priority(7)
///     .with_max_retries(5);
/// let task_id = task.metadata.id;
///
/// let message = task_to_message(task).expect("conversion");
/// assert_eq!(message.headers.task, "tasks.add");
/// assert_eq!(message.headers.id, task_id);
/// assert_eq!(message.properties.priority, Some(7));
///
/// // ... and back, with nothing lost.
/// let restored = message_to_task(message).expect("conversion");
/// assert_eq!(restored.metadata.id, task_id);
/// assert_eq!(restored.metadata.max_retries, 5);
/// assert_eq!(restored.payload, vec![1, 2, 3]);
/// ```
pub fn task_to_message(task: SerializedTask) -> Result<Message> {
    let SerializedTask { metadata, payload } = task;

    let encoded_metadata = serde_json::to_value(&metadata).map_err(|e| {
        BrokerError::Serialization(format!("cannot encode task metadata as JSON: {}", e))
    })?;

    // Built through `Message::new` so the content type and encoding stay
    // whatever the protocol crate considers canonical, rather than a second
    // copy of those strings that can drift.
    let mut message = Message::new(metadata.name.clone(), metadata.id, payload);
    message.headers.created_at = Some(metadata.created_at);
    message.headers.expires = metadata.expires_at;
    message.headers.group = metadata.group_id;
    message.headers.retries = spent_retries(&metadata.state);
    message
        .headers
        .extra
        .insert(TASK_METADATA_HEADER.to_string(), encoded_metadata);
    message.properties.priority = Some(wire_priority(metadata.priority));

    Ok(message)
}

/// Recover the task a wire message carries.
///
/// Uses the embedded [`TASK_METADATA_HEADER`] when present (an exact
/// restoration of what [`task_to_message`] enqueued) and otherwise
/// reconstructs what the headers do say.
///
/// # Errors
///
/// [`BrokerError::Serialization`] if the message *claims* to carry embedded
/// metadata but that metadata cannot be decoded. This is deliberately an error
/// rather than a silent fall back to header reconstruction: a truncated or
/// tampered metadata header would otherwise resurrect the task with default
/// retry limits and a fresh state, which is a worse outcome than refusing it.
///
/// # Examples
///
/// ```
/// use celers_kombu::core_adapter::message_to_task;
/// use celers_protocol::Message;
/// use uuid::Uuid;
///
/// // A message from some other producer: no embedded metadata.
/// let id = Uuid::new_v4();
/// let message = Message::new("tasks.foreign".to_string(), id, b"payload".to_vec())
///     .with_priority(4);
///
/// let task = message_to_task(message).expect("foreign messages are accepted");
/// assert_eq!(task.metadata.name, "tasks.foreign");
/// assert_eq!(task.metadata.id, id);
/// assert_eq!(task.metadata.priority, 4);
/// assert_eq!(task.payload, b"payload".to_vec());
/// ```
pub fn message_to_task(message: Message) -> Result<SerializedTask> {
    let Message {
        mut headers,
        properties,
        body,
        ..
    } = message;

    if let Some(encoded) = headers.extra.remove(TASK_METADATA_HEADER) {
        let metadata: TaskMetadata = serde_json::from_value(encoded).map_err(|e| {
            BrokerError::Serialization(format!(
                "message {} carries unreadable `{}`: {}",
                headers.id, TASK_METADATA_HEADER, e
            ))
        })?;
        return Ok(SerializedTask {
            metadata,
            payload: body,
        });
    }

    let mut metadata = TaskMetadata::new(headers.task);
    metadata.id = headers.id;
    if let Some(created_at) = headers.created_at {
        metadata.created_at = created_at;
        metadata.updated_at = created_at;
    }
    metadata.expires_at = headers.expires;
    metadata.group_id = headers.group;
    metadata.priority = properties.priority.map_or(0, i32::from);
    if let Some(retries) = headers.retries {
        // A non-zero `retries` header means attempts have already been spent;
        // recording that keeps a foreign producer's retry budget honest.
        if retries > 0 {
            metadata.state = TaskState::Retrying(retries);
        }
    }

    Ok(SerializedTask {
        metadata,
        payload: body,
    })
}
