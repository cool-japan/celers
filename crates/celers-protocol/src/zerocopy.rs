//! Zero-copy deserialization for performance optimization
//!
//! This module provides zero-copy deserialization capabilities using `Cow` (Copy-on-Write)
//! to avoid unnecessary data copying during deserialization. This is particularly useful
//! for large message bodies where avoiding copies can significantly improve performance.
//!
//! # Examples
//!
//! ```
//! use celers_protocol::zerocopy::{MessageRef, TaskArgsRef};
//! use uuid::Uuid;
//!
//! // Create a zero-copy message reference
//! let task_id = Uuid::new_v4();
//! let body = b"{\"args\":[1,2],\"kwargs\":{}}";
//! let msg = MessageRef::new("tasks.add", task_id, body);
//!
//! assert_eq!(msg.task_name(), "tasks.add");
//! assert_eq!(msg.task_id(), task_id);
//! ```

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::borrow::Cow;
use std::collections::HashMap;
use uuid::Uuid;

/// Zero-copy message headers
///
/// Uses `Cow<'a, str>` to avoid unnecessary string allocations when deserializing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageHeadersRef<'a> {
    /// Task name (zero-copy reference)
    #[serde(borrow)]
    pub task: Cow<'a, str>,

    /// Task ID
    pub id: Uuid,

    /// Programming language
    #[serde(borrow, default = "default_lang_cow")]
    pub lang: Cow<'a, str>,

    /// Root task ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_id: Option<Uuid>,

    /// Parent task ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<Uuid>,

    /// Group ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<Uuid>,

    /// Maximum retries
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,

    /// ETA for delayed tasks
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eta: Option<DateTime<Utc>>,

    /// Task expiration timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires: Option<DateTime<Utc>>,

    /// Message creation timestamp (UTC)
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,

    /// Additional custom headers
    #[serde(flatten)]
    pub extra: HashMap<Cow<'a, str>, serde_json::Value>,
}

fn default_lang_cow() -> Cow<'static, str> {
    Cow::Borrowed("rust")
}

/// Zero-copy message properties
///
/// # Wire format
///
/// Like [`crate::MessageProperties`], this always carries kombu's
/// `body_encoding` property (see [`crate::BODY_ENCODING_BASE64`]), because
/// [`MessageRef::body`] -- like [`crate::Message::body`] -- always
/// base64-encodes its body. Without it, a kombu consumer hands the base64
/// *text* to the content-type deserializer instead of decoding it first.
///
/// The same applies to `delivery_tag` and `delivery_info`, which
/// `kombu.transport.virtual.base.Message.__init__` indexes without a default:
/// a message published through [`MessageRef`] that omitted them would raise
/// `KeyError` inside the consumer callback and take the worker's event loop
/// down with it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessagePropertiesRef<'a> {
    /// Correlation ID
    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<Cow<'a, str>>,

    /// Reply-to queue
    #[serde(borrow, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<Cow<'a, str>>,

    /// Delivery mode (1 = non-persistent, 2 = persistent)
    #[serde(default = "default_delivery_mode")]
    pub delivery_mode: u8,

    /// Priority (0-9)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,

    /// kombu's body codec selector; see [`crate::BODY_ENCODING_BASE64`].
    ///
    /// Regression guard: this field used to be entirely absent from
    /// [`MessagePropertiesRef`], so a message published through
    /// [`MessageRef`] serialized a `properties` object with no
    /// `body_encoding` at all -- exactly the payload shape a kombu
    /// consumer never base64-decodes.
    #[serde(borrow, default = "default_body_encoding")]
    pub body_encoding: Cow<'a, str>,

    /// kombu's per-delivery handle; see [`crate::MessageProperties::delivery_tag`].
    ///
    /// Defaults to the nil UUID when a payload carries none, so parsing stays
    /// deterministic; a freshly constructed [`MessageRef`] gets a unique tag
    /// from [`MessagePropertiesRef::default`].
    #[serde(borrow, default = "absent_delivery_tag")]
    pub delivery_tag: Cow<'a, str>,

    /// Where the message was published; see [`crate::DeliveryInfo`].
    #[serde(default)]
    pub delivery_info: crate::DeliveryInfo,
}

fn default_delivery_mode() -> u8 {
    2
}

fn default_body_encoding() -> Cow<'static, str> {
    Cow::Borrowed(crate::BODY_ENCODING_BASE64)
}

fn absent_delivery_tag() -> Cow<'static, str> {
    Cow::Owned(Uuid::nil().to_string())
}

impl Default for MessagePropertiesRef<'_> {
    fn default() -> Self {
        Self {
            correlation_id: None,
            reply_to: None,
            delivery_mode: default_delivery_mode(),
            priority: None,
            body_encoding: default_body_encoding(),
            // A fresh tag per constructed message, exactly as
            // `MessageProperties::default` mints one.
            delivery_tag: Cow::Owned(Uuid::new_v4().to_string()),
            delivery_info: crate::DeliveryInfo::default(),
        }
    }
}

/// Base64 wire encoding for [`MessageRef::body`], matching
/// [`crate::Message::body`]'s `serde_bytes_opt` module in `types.rs` byte
/// for byte: a base64 JSON string on the wire, decoded bytes in memory.
///
/// The encoded *text* is borrowed from the input when possible (a cheap,
/// genuinely zero-copy step - no allocation, no decoding), and decoding it
/// into real bytes happens exactly once, eagerly, right here, rather than
/// being deferred to every caller of [`MessageRef::body_slice`] /
/// [`MessageRef::into_owned`]. This keeps those two accessors infallible
/// and interoperable with the rest of the crate. A malformed body surfaces
/// as an ordinary deserialization error at the `serde_json::from_...`
/// call site - exactly where `Message`'s own (non-lazy) body decoding
/// already surfaces the same failure mode.
mod base64_body {
    use base64::Engine;
    use serde::{de::Error as _, Deserialize, Deserializer, Serializer};
    use std::borrow::Cow;

    pub fn serialize<S>(bytes: &[u8], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        serializer.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Cow<'de, [u8]>, D::Error>
    where
        D: Deserializer<'de>,
    {
        // Borrow the encoded text from the input when possible (zero-copy);
        // only the final decode step allocates.
        let encoded = <Cow<'de, str>>::deserialize(deserializer)?;
        let decoded = base64::engine::general_purpose::STANDARD
            .decode(encoded.as_bytes())
            .map_err(D::Error::custom)?;
        Ok(Cow::Owned(decoded))
    }
}

/// Zero-copy Celery message
///
/// Uses borrowed data where possible to avoid unnecessary allocations.
/// This is particularly useful when deserializing messages from a buffer
/// that will remain valid for the lifetime of the message.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MessageRef<'a> {
    /// Message headers (zero-copy)
    #[serde(borrow)]
    pub headers: MessageHeadersRef<'a>,

    /// Message properties (zero-copy)
    #[serde(borrow)]
    pub properties: MessagePropertiesRef<'a>,

    /// Message body (decoded bytes).
    ///
    /// On the wire this is a base64 JSON string, identical to
    /// [`crate::Message::body`]'s representation - `"body": "dGVzdA=="`,
    /// never `"body": [116,101,115,116]` - via the `base64_body` serde
    /// adapter. `MessageRef` and `Message` therefore serialize to, and
    /// deserialize from, the same wire shape, and `into_owned()` never
    /// re-encodes or corrupts an already-decoded body.
    #[serde(borrow, with = "base64_body")]
    pub body: Cow<'a, [u8]>,

    /// Content type (zero-copy)
    #[serde(borrow, rename = "content-type")]
    pub content_type: Cow<'a, str>,

    /// Content encoding (zero-copy)
    #[serde(borrow, rename = "content-encoding")]
    pub content_encoding: Cow<'a, str>,
}

impl<'a> MessageRef<'a> {
    /// Create a new zero-copy message reference
    pub fn new(task: &'a str, id: Uuid, body: &'a [u8]) -> Self {
        Self {
            headers: MessageHeadersRef {
                task: Cow::Borrowed(task),
                id,
                lang: default_lang_cow(),
                root_id: None,
                parent_id: None,
                group: None,
                retries: None,
                eta: None,
                expires: None,
                created_at: Some(Utc::now()),
                extra: HashMap::new(),
            },
            properties: MessagePropertiesRef::default(),
            body: Cow::Borrowed(body),
            content_type: Cow::Borrowed("application/json"),
            content_encoding: Cow::Borrowed("utf-8"),
        }
    }

    /// Get the task ID
    pub fn task_id(&self) -> Uuid {
        self.headers.id
    }

    /// Get the task name
    pub fn task_name(&self) -> &str {
        &self.headers.task
    }

    /// Get body as slice
    pub fn body_slice(&self) -> &[u8] {
        &self.body
    }

    /// Check if message has ETA
    pub fn has_eta(&self) -> bool {
        self.headers.eta.is_some()
    }

    /// Check if message has expiration
    pub fn has_expires(&self) -> bool {
        self.headers.expires.is_some()
    }

    /// Check if message has parent
    pub fn has_parent(&self) -> bool {
        self.headers.parent_id.is_some()
    }

    /// Check if message has root
    pub fn has_root(&self) -> bool {
        self.headers.root_id.is_some()
    }

    /// Check if message has group
    pub fn has_group(&self) -> bool {
        self.headers.group.is_some()
    }

    /// Convert to owned message
    pub fn into_owned(self) -> crate::Message {
        crate::Message {
            headers: crate::MessageHeaders {
                task: self.headers.task.into_owned(),
                id: self.headers.id,
                lang: self.headers.lang.into_owned(),
                root_id: self.headers.root_id,
                parent_id: self.headers.parent_id,
                group: self.headers.group,
                retries: self.headers.retries,
                eta: self.headers.eta,
                expires: self.headers.expires,
                created_at: self.headers.created_at,
                extra: self
                    .headers
                    .extra
                    .into_iter()
                    .map(|(k, v)| (k.into_owned(), v))
                    .collect(),
            },
            properties: crate::MessageProperties {
                correlation_id: self.properties.correlation_id.map(|s| s.into_owned()),
                reply_to: self.properties.reply_to.map(|s| s.into_owned()),
                delivery_mode: self.properties.delivery_mode,
                priority: self.properties.priority,
                delivery_tag: self.properties.delivery_tag.into_owned(),
                delivery_info: self.properties.delivery_info,
            },
            body: self.body.into_owned(),
            content_type: self.content_type.into_owned(),
            content_encoding: self.content_encoding.into_owned(),
        }
    }
}

/// Zero-copy task arguments
///
/// Provides zero-copy access to task arguments when deserializing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskArgsRef<'a> {
    /// Positional arguments
    #[serde(default)]
    pub args: Vec<serde_json::Value>,

    /// Keyword arguments (zero-copy keys)
    #[serde(borrow, default)]
    pub kwargs: HashMap<Cow<'a, str>, serde_json::Value>,
}

impl<'a> TaskArgsRef<'a> {
    /// Create new task arguments reference
    pub fn new() -> Self {
        Self {
            args: Vec::new(),
            kwargs: HashMap::new(),
        }
    }

    /// Convert to owned task arguments
    pub fn into_owned(self) -> crate::TaskArgs {
        crate::TaskArgs {
            args: self.args,
            kwargs: self
                .kwargs
                .into_iter()
                .map(|(k, v)| (k.into_owned(), v))
                .collect(),
        }
    }
}

impl<'a> Default for TaskArgsRef<'a> {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_message_ref_new() {
        let task_id = Uuid::new_v4();
        let body = b"test body";
        let msg = MessageRef::new("tasks.test", task_id, body);

        assert_eq!(msg.task_name(), "tasks.test");
        assert_eq!(msg.task_id(), task_id);
        assert_eq!(msg.body_slice(), body);
    }

    #[test]
    fn test_message_ref_predicates() {
        let task_id = Uuid::new_v4();
        let body = b"{}";
        let mut msg = MessageRef::new("tasks.test", task_id, body);

        assert!(!msg.has_eta());
        assert!(!msg.has_expires());
        assert!(!msg.has_parent());
        assert!(!msg.has_root());
        assert!(!msg.has_group());

        msg.headers.eta = Some(Utc::now());
        msg.headers.parent_id = Some(Uuid::new_v4());

        assert!(msg.has_eta());
        assert!(msg.has_parent());
    }

    #[test]
    fn test_message_ref_to_owned() {
        let task_id = Uuid::new_v4();
        let body = b"test";
        let msg_ref = MessageRef::new("tasks.test", task_id, body);

        let msg = msg_ref.into_owned();

        assert_eq!(msg.headers.task, "tasks.test");
        assert_eq!(msg.headers.id, task_id);
        assert_eq!(msg.body, body);
    }

    #[test]
    fn test_message_ref_deserialize_rejects_malformed_base64() {
        // Regression guard for the new eager-decode adapter: a malformed
        // body must surface as an ordinary deserialization error (exactly
        // like `Message`'s own body decoding already does), not silently
        // produce garbage bytes.
        let task_id = Uuid::new_v4();
        let json = format!(
            r#"{{"headers":{{"task":"tasks.test","id":"{}","lang":"rust"}},"properties":{{"delivery_mode":2}},"body":"not valid base64!!!","content-type":"application/json","content-encoding":"utf-8"}}"#,
            task_id
        );
        assert!(serde_json::from_str::<MessageRef>(&json).is_err());
    }

    #[test]
    fn test_task_args_ref() {
        let args = TaskArgsRef::new();
        assert_eq!(args.args.len(), 0);
        assert_eq!(args.kwargs.len(), 0);
    }

    #[test]
    fn test_task_args_ref_to_owned() {
        let mut args_ref = TaskArgsRef::new();
        args_ref
            .kwargs
            .insert(Cow::Borrowed("key"), serde_json::json!("value"));

        let args = args_ref.into_owned();
        assert_eq!(args.kwargs.get("key").unwrap(), "value");
    }

    #[test]
    fn test_zero_copy_deserialization() {
        let json = r#"{"headers":{"task":"tasks.add","id":"550e8400-e29b-41d4-a716-446655440000","lang":"rust"},"properties":{"delivery_mode":2},"body":"dGVzdA==","content-type":"application/json","content-encoding":"utf-8"}"#;

        let msg: MessageRef = serde_json::from_str(json).unwrap();
        assert_eq!(msg.task_name(), "tasks.add");
        assert_eq!(msg.content_type, "application/json");

        // Regression: previously the "body" JSON string was treated as raw
        // bytes (its ASCII text, undecoded) rather than base64-decoded, so
        // body_slice() returned the literal text "dGVzdA==" instead of the
        // real, decoded body "test".
        assert_eq!(msg.body_slice(), b"test");
    }

    #[test]
    fn test_message_ref_wire_format_matches_message() {
        // Regression: `MessageRef` and `Message` must serialize the body
        // field identically (a base64 JSON string), so a `Message`'s wire
        // bytes parse cleanly as a `MessageRef` and round-trip back through
        // `into_owned()` without corrupting or re-encoding the body.
        let original = crate::builder::MessageBuilder::new("tasks.roundtrip")
            .args(vec![serde_json::json!(1), serde_json::json!(2)])
            .build()
            .unwrap();

        let wire = serde_json::to_vec(&original).unwrap();

        let msg_ref: MessageRef = serde_json::from_slice(&wire).unwrap();
        assert_eq!(msg_ref.body_slice(), original.body.as_slice());

        let round_tripped = msg_ref.into_owned();
        assert_eq!(round_tripped.body, original.body);
        assert_eq!(round_tripped.headers.task, original.headers.task);

        // Re-serializing the round-tripped message must match `Message`'s
        // own wire format exactly, including the "body" shape (a base64
        // string, not a JSON byte array, and not double-encoded).
        let re_serialized = serde_json::to_value(&round_tripped).unwrap();
        let original_value = serde_json::to_value(&original).unwrap();
        assert_eq!(re_serialized["body"], original_value["body"]);
        assert!(original_value["body"].is_string());
    }

    #[test]
    fn test_message_ref_serializes_body_as_base64_string_not_byte_array() {
        // Direct check that MessageRef's own Serialize impl emits the same
        // shape as Message (a base64 string), not a JSON array of numbers.
        let task_id = Uuid::new_v4();
        let msg_ref = MessageRef::new("tasks.test", task_id, b"test");

        let value = serde_json::to_value(&msg_ref).unwrap();
        assert_eq!(value["body"], serde_json::json!("dGVzdA=="));
    }

    /// Regression: `MessagePropertiesRef` used to have no `body_encoding`
    /// field at all, so a message published through `MessageRef` serialized
    /// a `properties` object missing kombu's body codec selector -- a
    /// payload a kombu consumer never base64-decodes, even though the body
    /// really is base64 (see `base64_body`). This is the zero-copy-path
    /// counterpart of the same bug already fixed for `Message` /
    /// `MessageProperties` in `types.rs`.
    #[test]
    fn test_message_ref_serializes_body_encoding() {
        let task_id = Uuid::new_v4();
        let msg_ref = MessageRef::new("tasks.test", task_id, b"test");

        let value = serde_json::to_value(&msg_ref).unwrap();
        assert_eq!(
            value["properties"]["body_encoding"],
            serde_json::json!(crate::BODY_ENCODING_BASE64)
        );

        // And a message serialized through the zero-copy path still
        // round-trips through `MessageRef` itself, keeping the property.
        let wire = serde_json::to_vec(&msg_ref).unwrap();
        let round_tripped: MessageRef = serde_json::from_slice(&wire).unwrap();
        assert_eq!(
            round_tripped.properties.body_encoding,
            crate::BODY_ENCODING_BASE64
        );

        // ... and survives the owned round-trip, where `Message`'s own
        // `Serialize` impl independently re-emits the property.
        let owned = round_tripped.into_owned();
        let owned_value = serde_json::to_value(&owned).unwrap();
        assert_eq!(
            owned_value["properties"]["body_encoding"],
            serde_json::json!(crate::BODY_ENCODING_BASE64)
        );
    }
}
