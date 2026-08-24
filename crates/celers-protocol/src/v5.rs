//! Celery Protocol v5 wire format builder.
//!
//! This module provides a native builder for the Celery **protocol v5** wire
//! layout. Where [`crate::migration`] *re-stamps* an existing [`Message`] onto a
//! target protocol version, this module constructs the concrete v5 envelope
//! (headers + AMQP-style properties + `[args, kwargs, embed]` body) from scratch
//! and renders it to the on-the-wire JSON object a v5 broker/consumer expects.
//!
//! # Protocol v5 vs v2
//!
//! The v2 and v5 envelopes share the same outer structure
//! (`{headers, properties, content-type, content-encoding, body}`) and the same
//! `[args, kwargs, embed]` body tuple. The v5-specific differences mirrored here
//! are exactly the ones documented and implemented in [`crate::migration`]:
//!
//! * **Protocol stamp** — the numeric protocol version (`5`) is recorded in the
//!   headers under [`crate::migration::PROTOCOL_VERSION_HEADER`] so the version
//!   is observable on the wire (v2 leaves this implicit).
//! * **Header-surfaced priority** — v5 surfaces the delivery priority in the
//!   headers (under [`DELIVERY_PRIORITY_HEADER`]) *in addition to* the AMQP
//!   `priority` property, so header-only consumers can route on it. v2 carries
//!   priority only as an AMQP property.
//! * **Inline workflow stamping** — `group` / `parent_id` / `root_id` are
//!   carried as native, inline headers (v5 understands them directly), whereas
//!   migrating *to v2* mirrors them into `_legacy_*` string fallbacks.
//!
//! # Example
//!
//! ```
//! use celers_protocol::v5::{build_v5_message, V5MessageSpec};
//! use serde_json::json;
//! use uuid::Uuid;
//!
//! let spec = V5MessageSpec::new("tasks.add", Uuid::new_v4())
//!     .with_args(vec![json!(2), json!(3)])
//!     .with_priority(7);
//!
//! let wire = build_v5_message(&spec).expect("v5 build");
//! let value = wire.to_wire_value().expect("v5 render");
//!
//! // The protocol version is stamped, and priority is mirrored into headers.
//! assert_eq!(value["headers"]["protocol_version"], json!("5"));
//! assert_eq!(value["headers"]["delivery_priority"], json!(7));
//! assert_eq!(value["properties"]["priority"], json!(7));
//! ```

use std::collections::HashMap;

use chrono::{DateTime, Utc};
use serde_json::Value;
use uuid::Uuid;

use crate::embed::{EmbedOptions, EmbeddedBody};
use crate::migration::PROTOCOL_VERSION_HEADER;
use crate::{
    ContentType, Message, MessageHeaders, MessageProperties, ProtocolVersion, CONTENT_TYPE_JSON,
    DEFAULT_LANG, ENCODING_UTF8,
};

/// Header key under which protocol v5 surfaces the delivery priority (in
/// addition to the AMQP `priority` property), enabling header-only routing.
///
/// This matches the key used by [`crate::migration`] when migrating a message
/// to v5, keeping the two code paths byte-for-byte consistent.
pub const DELIVERY_PRIORITY_HEADER: &str = "delivery_priority";

/// Errors that can occur while building a protocol v5 wire message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum V5BuildError {
    /// The task name was empty.
    EmptyTaskName,
    /// The `[args, kwargs, embed]` body could not be encoded.
    BodyEncoding(String),
}

impl std::fmt::Display for V5BuildError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            V5BuildError::EmptyTaskName => write!(f, "Task name cannot be empty"),
            V5BuildError::BodyEncoding(msg) => {
                write!(f, "Failed to encode v5 message body: {}", msg)
            }
        }
    }
}

impl std::error::Error for V5BuildError {}

/// Specification of a Celery protocol v5 message to build.
///
/// This is a declarative description of the desired message; the actual v5 wire
/// envelope is produced by [`build_v5_message`] (or [`V5MessageSpec::build`]).
#[derive(Debug, Clone)]
pub struct V5MessageSpec {
    task: String,
    id: Uuid,
    lang: String,
    args: Vec<Value>,
    kwargs: HashMap<String, Value>,
    embed: EmbedOptions,
    root_id: Option<Uuid>,
    parent_id: Option<Uuid>,
    group: Option<Uuid>,
    retries: Option<u32>,
    eta: Option<DateTime<Utc>>,
    expires: Option<DateTime<Utc>>,
    priority: Option<u8>,
    correlation_id: Option<String>,
    reply_to: Option<String>,
    delivery_mode: u8,
    content_type: ContentType,
    extra_headers: HashMap<String, Value>,
}

impl V5MessageSpec {
    /// Create a new v5 message specification for the given task and id.
    pub fn new(task: impl Into<String>, id: Uuid) -> Self {
        Self {
            task: task.into(),
            id,
            lang: DEFAULT_LANG.to_string(),
            args: Vec::new(),
            kwargs: HashMap::new(),
            embed: EmbedOptions::new(),
            root_id: None,
            parent_id: None,
            group: None,
            retries: None,
            eta: None,
            expires: None,
            priority: None,
            correlation_id: None,
            reply_to: None,
            delivery_mode: 2,
            content_type: ContentType::Json,
            extra_headers: HashMap::new(),
        }
    }

    /// Set the originating language (defaults to `"rust"`).
    #[must_use]
    pub fn with_lang(mut self, lang: impl Into<String>) -> Self {
        self.lang = lang.into();
        self
    }

    /// Set the positional arguments.
    #[must_use]
    pub fn with_args(mut self, args: Vec<Value>) -> Self {
        self.args = args;
        self
    }

    /// Append a single positional argument.
    #[must_use]
    pub fn with_arg(mut self, arg: Value) -> Self {
        self.args.push(arg);
        self
    }

    /// Set the keyword arguments.
    #[must_use]
    pub fn with_kwargs(mut self, kwargs: HashMap<String, Value>) -> Self {
        self.kwargs = kwargs;
        self
    }

    /// Insert a single keyword argument.
    #[must_use]
    pub fn with_kwarg(mut self, key: impl Into<String>, value: Value) -> Self {
        self.kwargs.insert(key.into(), value);
        self
    }

    /// Set the embedded options (callbacks, errbacks, chain, chord).
    #[must_use]
    pub fn with_embed(mut self, embed: EmbedOptions) -> Self {
        self.embed = embed;
        self
    }

    /// Set the root task id (inline workflow stamping, native in v5).
    #[must_use]
    pub fn with_root(mut self, root_id: Uuid) -> Self {
        self.root_id = Some(root_id);
        self
    }

    /// Set the parent task id (inline workflow stamping, native in v5).
    #[must_use]
    pub fn with_parent(mut self, parent_id: Uuid) -> Self {
        self.parent_id = Some(parent_id);
        self
    }

    /// Set the group id (inline workflow stamping, native in v5).
    #[must_use]
    pub fn with_group(mut self, group: Uuid) -> Self {
        self.group = Some(group);
        self
    }

    /// Set the retry count.
    #[must_use]
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = Some(retries);
        self
    }

    /// Set the ETA (delayed execution).
    #[must_use]
    pub fn with_eta(mut self, eta: DateTime<Utc>) -> Self {
        self.eta = Some(eta);
        self
    }

    /// Set the expiration timestamp.
    #[must_use]
    pub fn with_expires(mut self, expires: DateTime<Utc>) -> Self {
        self.expires = Some(expires);
        self
    }

    /// Set the delivery priority (0-9). v5 surfaces this in both headers and
    /// AMQP properties.
    #[must_use]
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Set the AMQP correlation id (RPC-style calls).
    #[must_use]
    pub fn with_correlation_id(mut self, correlation_id: impl Into<String>) -> Self {
        self.correlation_id = Some(correlation_id.into());
        self
    }

    /// Set the AMQP reply-to queue.
    #[must_use]
    pub fn with_reply_to(mut self, reply_to: impl Into<String>) -> Self {
        self.reply_to = Some(reply_to.into());
        self
    }

    /// Set the AMQP delivery mode (1 = transient, 2 = persistent).
    #[must_use]
    pub fn with_delivery_mode(mut self, delivery_mode: u8) -> Self {
        self.delivery_mode = delivery_mode;
        self
    }

    /// Set the content type used for the serialized body.
    #[must_use]
    pub fn with_content_type(mut self, content_type: ContentType) -> Self {
        self.content_type = content_type;
        self
    }

    /// Insert an additional custom header.
    #[must_use]
    pub fn with_header(mut self, key: impl Into<String>, value: Value) -> Self {
        self.extra_headers.insert(key.into(), value);
        self
    }

    /// Build the v5 wire message from this specification.
    ///
    /// Equivalent to calling [`build_v5_message`] with `&self`.
    pub fn build(&self) -> Result<V5Message, V5BuildError> {
        build_v5_message(self)
    }
}

/// A fully-formed Celery protocol v5 wire message.
///
/// Holds the v5 headers, AMQP-style properties and the encoded
/// `[args, kwargs, embed]` body. Render it with [`V5Message::to_wire_value`] or
/// [`V5Message::to_wire_bytes`], or lower it to the shared [`Message`] envelope
/// with [`V5Message::into_message`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct V5Message {
    /// Message headers, including the v5 protocol stamp and inline workflow ids.
    pub headers: MessageHeaders,
    /// AMQP-style message properties.
    pub properties: MessageProperties,
    /// Encoded `[args, kwargs, embed]` body.
    pub body: Vec<u8>,
    /// Content type of the serialized body.
    pub content_type: String,
    /// Content encoding of the serialized body.
    pub content_encoding: String,
}

impl V5Message {
    /// The protocol version of this message: always [`ProtocolVersion::V5`].
    #[inline]
    pub const fn protocol_version(&self) -> ProtocolVersion {
        ProtocolVersion::V5
    }

    /// Render the message to the on-the-wire JSON value.
    ///
    /// The shape is the canonical Celery envelope
    /// `{headers, properties, content-type, content-encoding, body}` where
    /// `body` is the base64-encoded `[args, kwargs, embed]` tuple — identical in
    /// structure to v2, but carrying the v5-specific header stamps.
    ///
    /// Mirrors [`V5Message::to_wire_bytes`] in returning the serialization
    /// error rather than a stand-in value: the documented usage pattern indexes
    /// straight into the result (`value["headers"]["protocol_version"]`), and
    /// indexing a `Value::Null` yields `Null` instead of failing, so a failed
    /// render would masquerade as a valid-shaped but empty message.
    pub fn to_wire_value(&self) -> Result<Value, V5BuildError> {
        // Lowering to `Message` and serializing guarantees the wire layout stays
        // byte-for-byte identical to the canonical envelope (same field renames,
        // same base64 body encoding) without duplicating that logic here.
        let message: Message = self.clone().into_message();
        serde_json::to_value(&message).map_err(|e| V5BuildError::BodyEncoding(e.to_string()))
    }

    /// Render the message to on-the-wire JSON bytes.
    pub fn to_wire_bytes(&self) -> Result<Vec<u8>, V5BuildError> {
        let message: Message = self.clone().into_message();
        serde_json::to_vec(&message).map_err(|e| V5BuildError::BodyEncoding(e.to_string()))
    }

    /// Lower this v5 message into the shared [`Message`] envelope.
    ///
    /// The v5 header stamps (protocol version, mirrored priority, inline
    /// workflow ids) are preserved, so a round-trip through `Message` keeps the
    /// message recognisable as v5 (see [`is_v5_wire`]).
    pub fn into_message(self) -> Message {
        Message {
            headers: self.headers,
            properties: self.properties,
            body: self.body,
            content_type: self.content_type,
            content_encoding: self.content_encoding,
        }
    }

    /// Borrow the message as a shared [`Message`] envelope (clones internally).
    pub fn as_message(&self) -> Message {
        self.clone().into_message()
    }
}

/// Build a Celery protocol v5 wire message from a [`V5MessageSpec`].
///
/// This produces the concrete v5 envelope: it encodes the `[args, kwargs,
/// embed]` body, populates the AMQP-style properties, and stamps the v5-specific
/// headers (protocol version + header-surfaced priority) on top of the inline,
/// natively-supported workflow identifiers.
pub fn build_v5_message(spec: &V5MessageSpec) -> Result<V5Message, V5BuildError> {
    if spec.task.is_empty() {
        return Err(V5BuildError::EmptyTaskName);
    }

    // Compose the body as the canonical [args, kwargs, embed] tuple. Workflow
    // identifiers are *also* surfaced inline in the embed block, mirroring how
    // Python Celery records them in the body for v5 workflow tracking.
    let mut embed = spec.embed.clone();
    if embed.group.is_none() {
        embed.group = spec.group;
    }
    if embed.parent_id.is_none() {
        embed.parent_id = spec.parent_id;
    }
    if embed.root_id.is_none() {
        embed.root_id = spec.root_id;
    }

    let embedded = EmbeddedBody::new()
        .with_args(spec.args.clone())
        .with_kwargs(spec.kwargs.clone())
        .with_embed(embed);

    let body = embedded
        .encode()
        .map_err(|e| V5BuildError::BodyEncoding(e.to_string()))?;

    // Build the headers, carrying the inline (native) workflow stamping.
    let mut headers = MessageHeaders::new(spec.task.clone(), spec.id).with_lang(spec.lang.clone());
    headers.root_id = spec.root_id;
    headers.parent_id = spec.parent_id;
    headers.group = spec.group;
    headers.retries = spec.retries;
    headers.eta = spec.eta;
    headers.expires = spec.expires;

    // v5-specific header stamps (kept consistent with `migration::migrate`).
    headers.extra.insert(
        PROTOCOL_VERSION_HEADER.to_string(),
        Value::String(ProtocolVersion::V5.as_number_str().to_string()),
    );
    if let Some(priority) = spec.priority {
        headers.extra.insert(
            DELIVERY_PRIORITY_HEADER.to_string(),
            Value::Number(priority.into()),
        );
    }
    for (key, value) in &spec.extra_headers {
        headers.extra.insert(key.clone(), value.clone());
    }

    let mut properties = MessageProperties::new().with_delivery_mode(spec.delivery_mode);
    properties.correlation_id = spec.correlation_id.clone();
    properties.reply_to = spec.reply_to.clone();
    properties.priority = spec.priority;

    Ok(V5Message {
        headers,
        properties,
        body,
        content_type: spec.content_type.as_str().to_string(),
        content_encoding: if matches!(spec.content_type, ContentType::Json) {
            ENCODING_UTF8.to_string()
        } else {
            // Non-JSON serializations (msgpack/binary/custom) are binary on the
            // wire; default to binary encoding for those.
            crate::ContentEncoding::Binary.as_str().to_string()
        },
    })
}

/// Convert an existing shared [`Message`] into a protocol v5 wire message.
///
/// Unlike [`crate::migration::ProtocolMigrator::migrate`], this does not apply
/// any compatibility gating — it deterministically re-stamps the message with
/// the v5 header layout (protocol version + header-surfaced priority) and
/// returns the v5 envelope. The body and existing headers/properties are
/// preserved.
pub fn to_v5_wire(message: &Message) -> V5Message {
    let mut headers = message.headers.clone();
    headers.extra.insert(
        PROTOCOL_VERSION_HEADER.to_string(),
        Value::String(ProtocolVersion::V5.as_number_str().to_string()),
    );
    if let Some(priority) = message.properties.priority {
        headers.extra.insert(
            DELIVERY_PRIORITY_HEADER.to_string(),
            Value::Number(priority.into()),
        );
    }

    V5Message {
        headers,
        properties: message.properties.clone(),
        body: message.body.clone(),
        content_type: if message.content_type.is_empty() {
            CONTENT_TYPE_JSON.to_string()
        } else {
            message.content_type.clone()
        },
        content_encoding: if message.content_encoding.is_empty() {
            ENCODING_UTF8.to_string()
        } else {
            message.content_encoding.clone()
        },
    }
}

/// Return whether a shared [`Message`] carries the protocol v5 header stamp.
///
/// A message is recognised as v5 when its headers record
/// [`crate::migration::PROTOCOL_VERSION_HEADER`] equal to the v5 numeric string
/// (`"5"`). This is the same stamp written by both [`build_v5_message`] /
/// [`to_v5_wire`] and [`crate::migration::ProtocolMigrator::migrate`].
pub fn is_v5_wire(message: &Message) -> bool {
    message
        .headers
        .extra
        .get(PROTOCOL_VERSION_HEADER)
        .and_then(Value::as_str)
        == Some(ProtocolVersion::V5.as_number_str())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_build_v5_minimal() {
        let id = Uuid::new_v4();
        let msg = build_v5_message(&V5MessageSpec::new("tasks.add", id))
            .expect("minimal v5 build must succeed");

        assert_eq!(msg.protocol_version(), ProtocolVersion::V5);
        assert_eq!(msg.headers.task, "tasks.add");
        assert_eq!(msg.headers.id, id);
        // Protocol stamp is present.
        assert_eq!(
            msg.headers.extra.get(PROTOCOL_VERSION_HEADER),
            Some(&Value::String("5".to_string()))
        );
        // No priority => no header mirror.
        assert!(!msg.headers.extra.contains_key(DELIVERY_PRIORITY_HEADER));
    }

    #[test]
    fn test_build_v5_empty_task_rejected() {
        let err = build_v5_message(&V5MessageSpec::new("", Uuid::new_v4()))
            .expect_err("empty task must be rejected");
        assert_eq!(err, V5BuildError::EmptyTaskName);
    }

    #[test]
    fn test_build_v5_priority_mirrored_to_headers() {
        let spec = V5MessageSpec::new("tasks.add", Uuid::new_v4()).with_priority(7);
        let msg = spec.build().expect("v5 build must succeed");

        // v5 surfaces priority in BOTH headers and AMQP properties.
        assert_eq!(
            msg.headers.extra.get(DELIVERY_PRIORITY_HEADER),
            Some(&Value::Number(7u8.into()))
        );
        assert_eq!(msg.properties.priority, Some(7));
    }

    #[test]
    fn test_build_v5_workflow_inline() {
        let root = Uuid::new_v4();
        let parent = Uuid::new_v4();
        let group = Uuid::new_v4();
        let msg = V5MessageSpec::new("tasks.chord", Uuid::new_v4())
            .with_root(root)
            .with_parent(parent)
            .with_group(group)
            .build()
            .expect("v5 build must succeed");

        // Inline (native) workflow stamping in headers.
        assert_eq!(msg.headers.root_id, Some(root));
        assert_eq!(msg.headers.parent_id, Some(parent));
        assert_eq!(msg.headers.group, Some(group));

        // ... and mirrored into the embed block of the body.
        let decoded = EmbeddedBody::decode(&msg.body).expect("body must decode");
        assert_eq!(decoded.embed.root_id, Some(root));
        assert_eq!(decoded.embed.parent_id, Some(parent));
        assert_eq!(decoded.embed.group, Some(group));
    }

    #[test]
    fn test_v5_body_is_args_kwargs_embed_tuple() {
        let msg = V5MessageSpec::new("tasks.add", Uuid::new_v4())
            .with_args(vec![json!(2), json!(3)])
            .with_kwarg("debug", json!(true))
            .build()
            .expect("v5 build must succeed");

        // Body decodes back to the supplied args/kwargs.
        let decoded = EmbeddedBody::decode(&msg.body).expect("body must decode");
        assert_eq!(decoded.args, vec![json!(2), json!(3)]);
        assert_eq!(decoded.kwargs.get("debug"), Some(&json!(true)));

        // And the raw body is a 3-tuple [args, kwargs, embed].
        let raw: Value = serde_json::from_slice(&msg.body).expect("body is json");
        let arr = raw.as_array().expect("body is array");
        assert_eq!(arr.len(), 3);
        assert!(arr[0].is_array());
        assert!(arr[1].is_object());
        assert!(arr[2].is_object());
    }

    #[test]
    fn test_v5_to_wire_value_shape() {
        let msg = V5MessageSpec::new("tasks.add", Uuid::new_v4())
            .with_args(vec![json!(1)])
            .with_priority(3)
            .with_correlation_id("corr-1")
            .build()
            .expect("v5 build must succeed");

        // Regression: this used to be an infallible `Value` that collapsed a
        // serialization failure into `Value::Null`, which then answered every
        // `wire["headers"][...]` index with `Null` instead of erroring.
        let wire = msg
            .to_wire_value()
            .expect("rendering a well-formed v5 message must succeed");

        // Canonical Celery envelope fields.
        assert!(wire.get("headers").is_some());
        assert!(wire.get("properties").is_some());
        assert!(wire.get("content-type").is_some());
        assert!(wire.get("content-encoding").is_some());
        assert!(wire.get("body").is_some());

        // v5 stamps visible on the wire.
        assert_eq!(wire["headers"]["protocol_version"], json!("5"));
        assert_eq!(wire["headers"]["delivery_priority"], json!(3));
        assert_eq!(wire["properties"]["priority"], json!(3));
        assert_eq!(wire["properties"]["correlation_id"], json!("corr-1"));
        // Body is a base64 string, and the envelope says so, which is what
        // makes a kombu consumer decode it.
        assert!(wire["body"].is_string());
        assert_eq!(wire["properties"]["body_encoding"], json!("base64"));

        // `to_wire_value` and `to_wire_bytes` must agree.
        let bytes = msg.to_wire_bytes().expect("wire bytes");
        let parsed: Value = serde_json::from_slice(&bytes).expect("parse wire bytes");
        assert_eq!(parsed, wire);
    }

    #[test]
    fn test_v5_round_trip_through_message() {
        let id = Uuid::new_v4();
        let v5 = V5MessageSpec::new("tasks.add", id)
            .with_args(vec![json!(10), json!(20)])
            .with_priority(5)
            .build()
            .expect("v5 build must succeed");

        // Lower to the shared envelope, serialize, and read back.
        let message = v5.clone().into_message();
        let bytes = serde_json::to_vec(&message).expect("serialize");
        let restored: Message = serde_json::from_slice(&bytes).expect("deserialize");

        assert_eq!(restored.headers.task, "tasks.add");
        assert_eq!(restored.headers.id, id);
        assert_eq!(restored.properties.priority, Some(5));
        // The restored message is still recognisable as v5.
        assert!(is_v5_wire(&restored));

        // The body survives the round-trip.
        let decoded = EmbeddedBody::decode(&restored.body).expect("body must decode");
        assert_eq!(decoded.args, vec![json!(10), json!(20)]);
    }

    #[test]
    fn test_to_v5_wire_from_message() {
        let id = Uuid::new_v4();
        let body = serde_json::to_vec(&json!([[1, 2], {}, {}])).expect("body");
        let message = Message::new("tasks.add".to_string(), id, body).with_priority(4);

        // A v2-style message lacks the v5 stamp.
        assert!(!is_v5_wire(&message));

        let v5 = to_v5_wire(&message);
        assert_eq!(v5.protocol_version(), ProtocolVersion::V5);
        assert!(is_v5_wire(&v5.as_message()));
        // Priority mirrored into headers.
        assert_eq!(
            v5.headers.extra.get(DELIVERY_PRIORITY_HEADER),
            Some(&Value::Number(4u8.into()))
        );
        // Body preserved unchanged.
        assert_eq!(v5.body, message.body);
    }

    #[test]
    fn test_v5_to_wire_bytes_parses() {
        let v5 = V5MessageSpec::new("tasks.ping", Uuid::new_v4())
            .build()
            .expect("v5 build must succeed");
        let bytes = v5.to_wire_bytes().expect("wire bytes");
        let parsed: Message = serde_json::from_slice(&bytes).expect("parse wire bytes");
        assert_eq!(parsed.headers.task, "tasks.ping");
        assert!(is_v5_wire(&parsed));
    }

    #[test]
    fn test_v5_build_error_display() {
        assert_eq!(
            V5BuildError::EmptyTaskName.to_string(),
            "Task name cannot be empty"
        );
        assert!(V5BuildError::BodyEncoding("x".to_string())
            .to_string()
            .contains("x"));
    }

    #[test]
    fn test_v5_extra_header_preserved() {
        let v5 = V5MessageSpec::new("tasks.add", Uuid::new_v4())
            .with_header("origin", json!("worker@host"))
            .build()
            .expect("v5 build must succeed");
        assert_eq!(v5.headers.extra.get("origin"), Some(&json!("worker@host")));
    }
}
