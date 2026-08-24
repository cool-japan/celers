//! Protocol types, structs, and enums for Celery messages.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use uuid::Uuid;

/// Common content type constants
pub(crate) const CONTENT_TYPE_JSON: &str = "application/json";
#[cfg(feature = "msgpack")]
pub(crate) const CONTENT_TYPE_MSGPACK: &str = "application/x-msgpack";
#[cfg(feature = "binary")]
pub(crate) const CONTENT_TYPE_BINARY: &str = "application/octet-stream";

/// Common encoding constants
pub(crate) const ENCODING_UTF8: &str = "utf-8";
pub(crate) const ENCODING_BINARY: &str = "binary";

/// Default language
pub(crate) const DEFAULT_LANG: &str = "rust";

/// Value of the kombu `body_encoding` property emitted by this crate.
///
/// [`Message`] always serializes its body as a base64 string (see the
/// `serde_bytes_opt` module below), which is the kombu virtual-transport
/// convention. kombu's consumer side only base64-*decodes* when
/// `properties['body_encoding'] == 'base64'`
/// (`kombu.transport.virtual.base.Message.__init__` ->
/// `channel.decode_body(body, properties.get('body_encoding'))`); with the key
/// absent it passes the base64 *text* through unchanged and the JSON
/// content-type deserializer then fails on it. Emitting the property is
/// therefore required for a Python consumer to read a CeleRS message at all.
pub const BODY_ENCODING_BASE64: &str = "base64";

/// Celery header carrying the task time limits as `[soft, hard]` (seconds).
///
/// See [`MessageHeaders::with_timelimit`].
pub const TIMELIMIT_HEADER: &str = "timelimit";

/// Celery header carrying the `repr()` of the positional arguments.
///
/// Purely observational: it is what Flower and `celery events` display, and it
/// exists so a worker never has to deserialize the body just to log a task.
pub const ARGSREPR_HEADER: &str = "argsrepr";

/// Celery header carrying the `repr()` of the keyword arguments.
pub const KWARGSREPR_HEADER: &str = "kwargsrepr";

/// Celery header naming the process that published the task
/// (`"{pid}@{hostname}"` in Python).
pub const ORIGIN_HEADER: &str = "origin";

/// Celery header carrying an alternate task name to display in monitoring.
pub const SHADOW_HEADER: &str = "shadow";

/// Celery header telling the worker not to store a result for this task.
pub const IGNORE_RESULT_HEADER: &str = "ignore_result";

/// Validation errors for Celery protocol messages
///
/// # Examples
///
/// ```
/// use celers_protocol::{Message, ValidationError};
/// use uuid::Uuid;
///
/// // Create a message with an empty task name
/// let msg = Message::new("".to_string(), Uuid::new_v4(), vec![1, 2, 3]);
///
/// // Validation will fail with a structured error
/// match msg.validate() {
///     Ok(_) => panic!("Should have failed"),
///     Err(ValidationError::EmptyTaskName) => {
///         println!("Task name cannot be empty");
///     }
///     Err(e) => panic!("Unexpected error: {}", e),
/// }
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ValidationError {
    /// Task name is empty
    EmptyTaskName,
    /// Retry count exceeds maximum
    RetryLimitExceeded { retries: u32, max: u32 },
    /// ETA is after expiration time
    EtaAfterExpiration,
    /// Invalid delivery mode (must be 1 or 2)
    InvalidDeliveryMode { mode: u8 },
    /// Invalid priority (must be 0-9)
    InvalidPriority { priority: u8 },
    /// Content type is empty
    EmptyContentType,
    /// Message body is empty
    EmptyBody,
    /// Message body exceeds size limit
    BodyTooLarge { size: usize, max: usize },
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ValidationError::EmptyTaskName => write!(f, "Task name cannot be empty"),
            ValidationError::RetryLimitExceeded { retries, max } => {
                write!(f, "Retries ({}) cannot exceed {}", retries, max)
            }
            ValidationError::EtaAfterExpiration => {
                write!(f, "ETA cannot be after expiration time")
            }
            ValidationError::InvalidDeliveryMode { mode } => {
                write!(
                    f,
                    "Invalid delivery mode ({}): must be 1 (non-persistent) or 2 (persistent)",
                    mode
                )
            }
            ValidationError::InvalidPriority { priority } => {
                write!(
                    f,
                    "Invalid priority ({}): must be between 0 and 9",
                    priority
                )
            }
            ValidationError::EmptyContentType => write!(f, "Content type cannot be empty"),
            ValidationError::EmptyBody => write!(f, "Message body cannot be empty"),
            ValidationError::BodyTooLarge { size, max } => {
                write!(
                    f,
                    "Message body too large: {} bytes (max {} bytes)",
                    size, max
                )
            }
        }
    }
}

impl std::error::Error for ValidationError {}

/// Protocol version
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub enum ProtocolVersion {
    /// Protocol version 2 (Celery 4.x+)
    #[default]
    V2,
    /// Protocol version 5 (Celery 5.x+)
    V5,
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolVersion::V2 => write!(f, "v2"),
            ProtocolVersion::V5 => write!(f, "v5"),
        }
    }
}

impl std::str::FromStr for ProtocolVersion {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "v2" | "2" => Ok(ProtocolVersion::V2),
            "v5" | "5" => Ok(ProtocolVersion::V5),
            _ => Err(format!("Invalid protocol version: {}", s)),
        }
    }
}

impl ProtocolVersion {
    /// Check if this is protocol version 2
    #[inline]
    pub const fn is_v2(self) -> bool {
        matches!(self, ProtocolVersion::V2)
    }

    /// Check if this is protocol version 5
    #[inline]
    pub const fn is_v5(self) -> bool {
        matches!(self, ProtocolVersion::V5)
    }

    /// Get the version number as u8
    #[inline]
    pub const fn as_u8(self) -> u8 {
        match self {
            ProtocolVersion::V2 => 2,
            ProtocolVersion::V5 => 5,
        }
    }

    /// Get the version number as a static string
    #[inline]
    pub const fn as_number_str(self) -> &'static str {
        match self {
            ProtocolVersion::V2 => "2",
            ProtocolVersion::V5 => "5",
        }
    }
}

/// Content type for serialization
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ContentType {
    /// JSON serialization
    #[default]
    Json,
    /// MessagePack serialization
    #[cfg(feature = "msgpack")]
    MessagePack,
    /// Binary serialization
    #[cfg(feature = "binary")]
    Binary,
    /// Custom content type
    Custom(String),
}

impl ContentType {
    #[inline]
    pub fn as_str(&self) -> &str {
        match self {
            ContentType::Json => CONTENT_TYPE_JSON,
            #[cfg(feature = "msgpack")]
            ContentType::MessagePack => CONTENT_TYPE_MSGPACK,
            #[cfg(feature = "binary")]
            ContentType::Binary => CONTENT_TYPE_BINARY,
            ContentType::Custom(s) => s,
        }
    }

    /// The [`ContentEncoding`] a message with this content type carries
    /// when no encoding is explicitly chosen.
    ///
    /// JSON is text (`utf-8`); every other format (msgpack, binary, custom)
    /// is treated as opaque binary. Declaring `content-encoding: utf-8` on
    /// a non-JSON body would be a lie the receiving end could act on.
    ///
    /// Shared by [`crate::builder::MessageBuilder::build`] and
    /// [`crate::v5::build_v5_message`] so the two message-construction
    /// paths cannot independently drift on this mapping.
    #[inline]
    pub fn default_content_encoding(&self) -> ContentEncoding {
        match self {
            ContentType::Json => ContentEncoding::Utf8,
            _ => ContentEncoding::Binary,
        }
    }
}

impl std::fmt::Display for ContentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for ContentType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            CONTENT_TYPE_JSON => Ok(ContentType::Json),
            #[cfg(feature = "msgpack")]
            CONTENT_TYPE_MSGPACK => Ok(ContentType::MessagePack),
            #[cfg(feature = "binary")]
            CONTENT_TYPE_BINARY => Ok(ContentType::Binary),
            other => Ok(ContentType::Custom(other.to_string())),
        }
    }
}

impl From<&str> for ContentType {
    fn from(s: &str) -> Self {
        match s {
            CONTENT_TYPE_JSON => ContentType::Json,
            #[cfg(feature = "msgpack")]
            CONTENT_TYPE_MSGPACK => ContentType::MessagePack,
            #[cfg(feature = "binary")]
            CONTENT_TYPE_BINARY => ContentType::Binary,
            other => ContentType::Custom(other.to_string()),
        }
    }
}

impl AsRef<str> for ContentType {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Content encoding
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ContentEncoding {
    /// UTF-8 encoding
    #[default]
    Utf8,
    /// Binary encoding
    Binary,
    /// Custom encoding
    Custom(String),
}

impl ContentEncoding {
    #[inline]
    pub fn as_str(&self) -> &str {
        match self {
            ContentEncoding::Utf8 => ENCODING_UTF8,
            ContentEncoding::Binary => ENCODING_BINARY,
            ContentEncoding::Custom(s) => s,
        }
    }
}

impl std::fmt::Display for ContentEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

impl std::str::FromStr for ContentEncoding {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            ENCODING_UTF8 => Ok(ContentEncoding::Utf8),
            ENCODING_BINARY => Ok(ContentEncoding::Binary),
            other => Ok(ContentEncoding::Custom(other.to_string())),
        }
    }
}

impl From<&str> for ContentEncoding {
    fn from(s: &str) -> Self {
        match s {
            ENCODING_UTF8 => ContentEncoding::Utf8,
            ENCODING_BINARY => ContentEncoding::Binary,
            other => ContentEncoding::Custom(other.to_string()),
        }
    }
}

impl AsRef<str> for ContentEncoding {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

/// Message headers (Celery protocol)
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageHeaders {
    /// Task name (e.g., "tasks.add")
    pub task: String,

    /// Task ID (UUID)
    pub id: Uuid,

    /// Programming language ("rust", "py")
    #[serde(default = "default_lang")]
    pub lang: String,

    /// Root task ID (for workflow tracking)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_id: Option<Uuid>,

    /// Parent task ID (for nested tasks)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<Uuid>,

    /// Group ID (for grouped tasks)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<Uuid>,

    /// Maximum retries
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retries: Option<u32>,

    /// ETA (Estimated Time of Arrival) for delayed tasks
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eta: Option<DateTime<Utc>>,

    /// Task expiration timestamp
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires: Option<DateTime<Utc>>,

    /// Message creation timestamp (UTC).
    ///
    /// Set automatically when a message is created via
    /// [`MessageHeaders::new`]. Used to compute the message age. This field is
    /// optional and `#[serde(default)]` so that messages produced by other
    /// (older) protocol implementations that omit it deserialize cleanly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub created_at: Option<DateTime<Utc>>,

    /// Additional custom headers
    #[serde(flatten)]
    pub extra: HashMap<String, serde_json::Value>,
}

fn default_lang() -> String {
    DEFAULT_LANG.to_string()
}

impl MessageHeaders {
    pub fn new(task: String, id: Uuid) -> Self {
        Self {
            task,
            id,
            lang: default_lang(),
            root_id: None,
            parent_id: None,
            group: None,
            retries: None,
            eta: None,
            expires: None,
            created_at: Some(Utc::now()),
            extra: HashMap::new(),
        }
    }

    /// Set the language field (builder pattern)
    #[must_use]
    pub fn with_lang(mut self, lang: String) -> Self {
        self.lang = lang;
        self
    }

    /// Set the root ID field (builder pattern)
    #[must_use]
    pub fn with_root_id(mut self, root_id: Uuid) -> Self {
        self.root_id = Some(root_id);
        self
    }

    /// Set the parent ID field (builder pattern)
    #[must_use]
    pub fn with_parent_id(mut self, parent_id: Uuid) -> Self {
        self.parent_id = Some(parent_id);
        self
    }

    /// Set the group field (builder pattern)
    #[must_use]
    pub fn with_group(mut self, group: Uuid) -> Self {
        self.group = Some(group);
        self
    }

    /// Set the retries field (builder pattern)
    #[must_use]
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.retries = Some(retries);
        self
    }

    /// Set the ETA field (builder pattern)
    #[must_use]
    pub fn with_eta(mut self, eta: DateTime<Utc>) -> Self {
        self.eta = Some(eta);
        self
    }

    /// Set the expires field (builder pattern)
    #[must_use]
    pub fn with_expires(mut self, expires: DateTime<Utc>) -> Self {
        self.expires = Some(expires);
        self
    }

    /// Set the creation timestamp field (builder pattern)
    #[must_use]
    pub fn with_created_at(mut self, created_at: DateTime<Utc>) -> Self {
        self.created_at = Some(created_at);
        self
    }

    /// Set the Celery task time limits (builder pattern).
    ///
    /// Celery carries time limits as `headers['timelimit'] = [soft, hard]`
    /// (seconds), where either slot may be `null`. The soft limit raises
    /// `SoftTimeLimitExceeded` inside the task; the hard limit kills the worker
    /// child process.
    ///
    /// The pair is stored in the flattened [`MessageHeaders::extra`] map, so it
    /// lands at the top level of the `headers` object on the wire exactly where
    /// a Python worker looks for it.
    ///
    /// # Example
    ///
    /// ```
    /// use celers_protocol::MessageHeaders;
    /// use uuid::Uuid;
    ///
    /// let headers = MessageHeaders::new("tasks.add".to_string(), Uuid::new_v4())
    ///     .with_timelimit(Some(30), Some(60));
    ///
    /// assert_eq!(headers.timelimit(), Some((Some(30), Some(60))));
    ///
    /// let value = serde_json::to_value(&headers).expect("serialize");
    /// assert_eq!(value["timelimit"], serde_json::json!([30, 60]));
    /// ```
    #[must_use]
    pub fn with_timelimit(mut self, soft: Option<u64>, hard: Option<u64>) -> Self {
        self.set_timelimit(soft, hard);
        self
    }

    /// Set the Celery task time limits in place.
    ///
    /// See [`MessageHeaders::with_timelimit`].
    pub fn set_timelimit(&mut self, soft: Option<u64>, hard: Option<u64>) {
        let encode = |limit: Option<u64>| match limit {
            Some(seconds) => serde_json::Value::from(seconds),
            None => serde_json::Value::Null,
        };
        self.extra.insert(
            TIMELIMIT_HEADER.to_string(),
            serde_json::Value::Array(vec![encode(soft), encode(hard)]),
        );
    }

    /// Read the Celery task time limits as `(soft, hard)`.
    ///
    /// Returns [`None`] when the header is absent or malformed. Either slot may
    /// independently be [`None`], matching Celery's `[null, 30]` form.
    pub fn timelimit(&self) -> Option<(Option<u64>, Option<u64>)> {
        let entries = self.extra.get(TIMELIMIT_HEADER)?.as_array()?;
        if entries.len() != 2 {
            return None;
        }
        let parse = |value: &serde_json::Value| -> Option<Option<u64>> {
            match value {
                serde_json::Value::Null => Some(None),
                other => other.as_u64().map(Some),
            }
        };
        Some((parse(&entries[0])?, parse(&entries[1])?))
    }

    /// Read the soft time limit in seconds, if set.
    #[inline]
    pub fn soft_time_limit(&self) -> Option<u64> {
        self.timelimit().and_then(|(soft, _)| soft)
    }

    /// Read the hard time limit in seconds, if set.
    #[inline]
    pub fn hard_time_limit(&self) -> Option<u64> {
        self.timelimit().and_then(|(_, hard)| hard)
    }

    /// Set the `argsrepr` observability header (builder pattern).
    ///
    /// See [`ARGSREPR_HEADER`].
    #[must_use]
    pub fn with_argsrepr(mut self, argsrepr: impl Into<String>) -> Self {
        self.set_string_header(ARGSREPR_HEADER, argsrepr);
        self
    }

    /// Set the `kwargsrepr` observability header (builder pattern).
    #[must_use]
    pub fn with_kwargsrepr(mut self, kwargsrepr: impl Into<String>) -> Self {
        self.set_string_header(KWARGSREPR_HEADER, kwargsrepr);
        self
    }

    /// Set the `origin` header (publisher identity) (builder pattern).
    #[must_use]
    pub fn with_origin(mut self, origin: impl Into<String>) -> Self {
        self.set_string_header(ORIGIN_HEADER, origin);
        self
    }

    /// Set the `shadow` header (display name override) (builder pattern).
    #[must_use]
    pub fn with_shadow(mut self, shadow: impl Into<String>) -> Self {
        self.set_string_header(SHADOW_HEADER, shadow);
        self
    }

    /// Set the `ignore_result` header (builder pattern).
    #[must_use]
    pub fn with_ignore_result(mut self, ignore_result: bool) -> Self {
        self.extra.insert(
            IGNORE_RESULT_HEADER.to_string(),
            serde_json::Value::Bool(ignore_result),
        );
        self
    }

    /// Read the `argsrepr` header, if set.
    #[inline]
    pub fn argsrepr(&self) -> Option<&str> {
        self.string_header(ARGSREPR_HEADER)
    }

    /// Read the `kwargsrepr` header, if set.
    #[inline]
    pub fn kwargsrepr(&self) -> Option<&str> {
        self.string_header(KWARGSREPR_HEADER)
    }

    /// Read the `origin` header, if set.
    #[inline]
    pub fn origin(&self) -> Option<&str> {
        self.string_header(ORIGIN_HEADER)
    }

    /// Read the `shadow` header, if set.
    #[inline]
    pub fn shadow(&self) -> Option<&str> {
        self.string_header(SHADOW_HEADER)
    }

    /// Read the `ignore_result` header, if set.
    #[inline]
    pub fn ignore_result(&self) -> Option<bool> {
        self.extra.get(IGNORE_RESULT_HEADER)?.as_bool()
    }

    /// Read a string-valued custom header.
    #[inline]
    fn string_header(&self, key: &str) -> Option<&str> {
        self.extra.get(key)?.as_str()
    }

    /// Write a string-valued custom header.
    fn set_string_header(&mut self, key: &str, value: impl Into<String>) {
        self.extra
            .insert(key.to_string(), serde_json::Value::String(value.into()));
    }

    /// Validate message headers
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.task.is_empty() {
            return Err(ValidationError::EmptyTaskName);
        }

        if let Some(retries) = self.retries {
            if retries > 1000 {
                return Err(ValidationError::RetryLimitExceeded { retries, max: 1000 });
            }
        }

        // Validate ETA and expiration relationship
        if let (Some(eta), Some(expires)) = (self.eta, self.expires) {
            if eta > expires {
                return Err(ValidationError::EtaAfterExpiration);
            }
        }

        Ok(())
    }
}

/// Message properties (AMQP-like)
///
/// # Wire format
///
/// When these properties are serialized as part of a [`Message`] envelope they
/// additionally carry kombu's `body_encoding` property, always
/// [`BODY_ENCODING_BASE64`], because [`Message`] always base64-encodes its body.
/// Without it a kombu consumer hands the base64 *text* to the JSON
/// deserializer, which then fails. The property is emitted by the
/// [`Serialize`] impl below rather than stored as a field, since the encoding
/// is a property of the envelope and not independently selectable.
///
/// Deserialization accepts and ignores `body_encoding` along with the other
/// virtual-transport keys kombu adds on the consumer side (`delivery_info`,
/// `delivery_tag`), so a message captured off a Redis/SQS queue round-trips.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageProperties {
    /// Correlation ID for RPC-style calls
    pub correlation_id: Option<String>,

    /// Reply-to queue for results
    pub reply_to: Option<String>,

    /// Delivery mode (1 = non-persistent, 2 = persistent)
    pub delivery_mode: u8,

    /// Priority (0-9, higher = more priority)
    pub priority: Option<u8>,
}

const fn default_delivery_mode() -> u8 {
    2 // Persistent by default
}

/// Serialization shape of [`MessageProperties`].
///
/// Borrows from the live properties so no allocation is needed, and adds the
/// `body_encoding` property that the envelope always implies.
#[derive(Serialize)]
struct MessagePropertiesRepr<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    correlation_id: Option<&'a String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reply_to: Option<&'a String>,
    delivery_mode: u8,
    #[serde(skip_serializing_if = "Option::is_none")]
    priority: Option<u8>,
    /// kombu's body codec selector; see [`BODY_ENCODING_BASE64`].
    body_encoding: &'static str,
}

/// Deserialization shape of [`MessageProperties`].
///
/// `body_encoding`, `delivery_info` and `delivery_tag` are accepted (and
/// deliberately discarded) so that a payload produced by this crate, or read
/// back off a kombu virtual transport, deserializes cleanly.
#[derive(Deserialize)]
struct MessagePropertiesDe {
    #[serde(default)]
    correlation_id: Option<String>,
    #[serde(default)]
    reply_to: Option<String>,
    #[serde(default = "default_delivery_mode")]
    delivery_mode: u8,
    #[serde(default)]
    priority: Option<u8>,
    #[serde(default)]
    #[allow(dead_code)]
    body_encoding: Option<String>,
}

impl Serialize for MessageProperties {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        MessagePropertiesRepr {
            correlation_id: self.correlation_id.as_ref(),
            reply_to: self.reply_to.as_ref(),
            delivery_mode: self.delivery_mode,
            priority: self.priority,
            body_encoding: BODY_ENCODING_BASE64,
        }
        .serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for MessageProperties {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let repr = MessagePropertiesDe::deserialize(deserializer)?;
        Ok(Self {
            correlation_id: repr.correlation_id,
            reply_to: repr.reply_to,
            delivery_mode: repr.delivery_mode,
            priority: repr.priority,
        })
    }
}

impl Default for MessageProperties {
    fn default() -> Self {
        Self {
            correlation_id: None,
            reply_to: None,
            delivery_mode: default_delivery_mode(),
            priority: None,
        }
    }
}

impl MessageProperties {
    /// Create new MessageProperties with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Set correlation ID (builder pattern)
    #[must_use]
    pub fn with_correlation_id(mut self, correlation_id: String) -> Self {
        self.correlation_id = Some(correlation_id);
        self
    }

    /// Set reply-to queue (builder pattern)
    #[must_use]
    pub fn with_reply_to(mut self, reply_to: String) -> Self {
        self.reply_to = Some(reply_to);
        self
    }

    /// Set delivery mode (builder pattern)
    #[must_use]
    pub fn with_delivery_mode(mut self, delivery_mode: u8) -> Self {
        self.delivery_mode = delivery_mode;
        self
    }

    /// Set priority (builder pattern)
    #[must_use]
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.priority = Some(priority);
        self
    }

    /// The kombu body codec these properties advertise on the wire.
    ///
    /// Always [`BODY_ENCODING_BASE64`]: [`Message`] has exactly one body
    /// encoding. Exposed so callers building a kombu envelope by hand do not
    /// have to hardcode the string.
    #[inline]
    pub const fn body_encoding(&self) -> &'static str {
        BODY_ENCODING_BASE64
    }

    /// Validate message properties
    pub fn validate(&self) -> Result<(), ValidationError> {
        if self.delivery_mode != 1 && self.delivery_mode != 2 {
            return Err(ValidationError::InvalidDeliveryMode {
                mode: self.delivery_mode,
            });
        }

        if let Some(priority) = self.priority {
            if priority > 9 {
                return Err(ValidationError::InvalidPriority { priority });
            }
        }

        Ok(())
    }
}

/// Complete Celery message
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Message {
    /// Message headers
    pub headers: MessageHeaders,

    /// Message properties
    pub properties: MessageProperties,

    /// Serialized body (task arguments)
    #[serde(with = "serde_bytes_opt")]
    pub body: Vec<u8>,

    /// Content type
    #[serde(rename = "content-type")]
    pub content_type: String,

    /// Content encoding
    #[serde(rename = "content-encoding")]
    pub content_encoding: String,
}

// Custom serde module for optional byte arrays
mod serde_bytes_opt {
    use base64::Engine;
    use serde::de::Error;
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(bytes: &Vec<u8>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Serialize as base64 string for JSON compatibility
        let encoded = base64::engine::general_purpose::STANDARD.encode(bytes);
        serializer.serialize_str(&encoded)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<u8>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        base64::engine::general_purpose::STANDARD
            .decode(&s)
            .map_err(Error::custom)
    }
}

/// Task arguments (args, kwargs)
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
pub struct TaskArgs {
    /// Positional arguments
    #[serde(default)]
    pub args: Vec<serde_json::Value>,

    /// Keyword arguments
    #[serde(default)]
    pub kwargs: HashMap<String, serde_json::Value>,
}

impl TaskArgs {
    /// Create a new empty TaskArgs
    pub fn new() -> Self {
        Self::default()
    }

    /// Set all positional arguments at once (builder pattern)
    #[must_use]
    pub fn with_args(mut self, args: Vec<serde_json::Value>) -> Self {
        self.args = args;
        self
    }

    /// Set all keyword arguments at once (builder pattern)
    #[must_use]
    pub fn with_kwargs(mut self, kwargs: HashMap<String, serde_json::Value>) -> Self {
        self.kwargs = kwargs;
        self
    }

    /// Add a single positional argument
    pub fn add_arg(&mut self, arg: serde_json::Value) {
        self.args.push(arg);
    }

    /// Add a single keyword argument
    pub fn add_kwarg(&mut self, key: String, value: serde_json::Value) {
        self.kwargs.insert(key, value);
    }

    /// Check if both args and kwargs are empty
    #[inline(always)]
    pub fn is_empty(&self) -> bool {
        self.args.is_empty() && self.kwargs.is_empty()
    }

    /// Get the total number of arguments (positional + keyword)
    #[inline(always)]
    pub fn len(&self) -> usize {
        self.args.len() + self.kwargs.len()
    }

    /// Check if there are any positional arguments
    #[inline(always)]
    pub fn has_args(&self) -> bool {
        !self.args.is_empty()
    }

    /// Check if there are any keyword arguments
    #[inline(always)]
    pub fn has_kwargs(&self) -> bool {
        !self.kwargs.is_empty()
    }

    /// Clear all arguments
    pub fn clear(&mut self) {
        self.args.clear();
        self.kwargs.clear();
    }

    /// Get a positional argument by index
    #[inline]
    pub fn get_arg(&self, index: usize) -> Option<&serde_json::Value> {
        self.args.get(index)
    }

    /// Get a keyword argument by key
    #[inline]
    pub fn get_kwarg(&self, key: &str) -> Option<&serde_json::Value> {
        self.kwargs.get(key)
    }

    /// Create TaskArgs from a JSON string
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Convert TaskArgs to a JSON string
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Convert TaskArgs to pretty-printed JSON
    pub fn to_json_pretty(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

// Index trait for accessing positional arguments by index
impl std::ops::Index<usize> for TaskArgs {
    type Output = serde_json::Value;

    #[inline]
    fn index(&self, index: usize) -> &Self::Output {
        &self.args[index]
    }
}

// IndexMut trait for mutating positional arguments by index
impl std::ops::IndexMut<usize> for TaskArgs {
    #[inline]
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.args[index]
    }
}

// Index trait for accessing keyword arguments by string key
impl std::ops::Index<&str> for TaskArgs {
    type Output = serde_json::Value;

    #[inline]
    fn index(&self, key: &str) -> &Self::Output {
        &self.kwargs[key]
    }
}

// IntoIterator for TaskArgs - iterates over positional args
impl IntoIterator for TaskArgs {
    type Item = serde_json::Value;
    type IntoIter = std::vec::IntoIter<serde_json::Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.args.into_iter()
    }
}

// IntoIterator for &TaskArgs - iterates over positional args by reference
impl<'a> IntoIterator for &'a TaskArgs {
    type Item = &'a serde_json::Value;
    type IntoIter = std::slice::Iter<'a, serde_json::Value>;

    fn into_iter(self) -> Self::IntoIter {
        self.args.iter()
    }
}

// Extend trait for TaskArgs - extend with more positional arguments
impl Extend<serde_json::Value> for TaskArgs {
    fn extend<T: IntoIterator<Item = serde_json::Value>>(&mut self, iter: T) {
        self.args.extend(iter);
    }
}

// Extend trait for TaskArgs with key-value pairs for kwargs
impl Extend<(String, serde_json::Value)> for TaskArgs {
    fn extend<T: IntoIterator<Item = (String, serde_json::Value)>>(&mut self, iter: T) {
        self.kwargs.extend(iter);
    }
}

// FromIterator for TaskArgs - build from iterator of positional args
impl FromIterator<serde_json::Value> for TaskArgs {
    fn from_iter<T: IntoIterator<Item = serde_json::Value>>(iter: T) -> Self {
        Self {
            args: iter.into_iter().collect(),
            kwargs: HashMap::new(),
        }
    }
}

#[cfg(test)]
mod wire_format_tests {
    use super::*;
    use serde_json::json;

    /// Golden fixture: the envelope a kombu virtual transport (Redis/SQS)
    /// stores for a Celery task.
    ///
    /// Field semantics:
    /// * `body` -- base64 of the `[args, kwargs, embed]` tuple.
    /// * `properties.body_encoding` -- kombu's codec selector. On the consumer
    ///   side `virtual.Message.__init__` calls
    ///   `channel.decode_body(body, properties.get('body_encoding'))`, which
    ///   base64-decodes **only** for the literal value `"base64"`. Absent, the
    ///   base64 text is passed straight to the JSON deserializer and the
    ///   message is unreadable.
    /// * `properties.delivery_mode` -- 2 = persistent.
    /// * `content-type` / `content-encoding` -- hyphenated, per kombu.
    const KOMBU_ENVELOPE: &str = r#"{
        "body": "W1sxLCAyXSwge30sIHt9XQ==",
        "content-encoding": "utf-8",
        "content-type": "application/json",
        "headers": {
            "task": "tasks.add",
            "id": "6d5b1f1e-6a4f-4a3c-9b6c-1f8f5f1a2b3c",
            "lang": "py",
            "root_id": null,
            "parent_id": null,
            "group": null,
            "retries": 0,
            "eta": null,
            "expires": null,
            "timelimit": [null, null],
            "argsrepr": "(1, 2)",
            "kwargsrepr": "{}",
            "origin": "1234@worker.local",
            "shadow": null,
            "ignore_result": false
        },
        "properties": {
            "correlation_id": "6d5b1f1e-6a4f-4a3c-9b6c-1f8f5f1a2b3c",
            "reply_to": "b7a1e0d0-5c4e-4a1e-9a2b-3c4d5e6f7a8b",
            "delivery_mode": 2,
            "priority": 0,
            "body_encoding": "base64",
            "delivery_tag": "9f8e7d6c-5b4a-3928-1706-abcdefabcdef",
            "delivery_info": {"exchange": "", "routing_key": "celery"}
        }
    }"#;

    /// Regression: a real kombu envelope carries `body_encoding`,
    /// `delivery_tag` and `delivery_info` in `properties`. These must be
    /// accepted (not rejected as unknown), and the Celery observability
    /// headers must survive into `extra`.
    #[test]
    fn test_parses_real_kombu_envelope() {
        let msg: Message =
            serde_json::from_str(KOMBU_ENVELOPE).expect("a real kombu envelope must deserialize");

        assert_eq!(msg.headers.task, "tasks.add");
        assert_eq!(msg.headers.lang, "py");
        assert_eq!(msg.body, b"[[1, 2], {}, {}]");
        assert_eq!(msg.properties.delivery_mode, 2);
        assert_eq!(msg.properties.priority, Some(0));
        assert_eq!(
            msg.properties.reply_to.as_deref(),
            Some("b7a1e0d0-5c4e-4a1e-9a2b-3c4d5e6f7a8b")
        );

        // Celery observability headers land in the flattened `extra` map and
        // are readable through the typed accessors.
        assert_eq!(msg.headers.timelimit(), Some((None, None)));
        assert_eq!(msg.headers.argsrepr(), Some("(1, 2)"));
        assert_eq!(msg.headers.kwargsrepr(), Some("{}"));
        assert_eq!(msg.headers.origin(), Some("1234@worker.local"));
        assert_eq!(msg.headers.ignore_result(), Some(false));
    }

    /// Regression: `body_encoding` used to be absent from the serialized
    /// properties entirely (the string appeared in zero files), so a kombu
    /// consumer never base64-decoded a CeleRS-produced body.
    #[test]
    fn test_serialized_properties_carry_body_encoding() {
        let msg = Message::new(
            "tasks.add".to_string(),
            uuid::Uuid::new_v4(),
            b"[[1, 2], {}, {}]".to_vec(),
        );

        let value = serde_json::to_value(&msg).expect("serialize");

        assert_eq!(value["properties"]["body_encoding"], json!("base64"));
        assert_eq!(value["properties"]["delivery_mode"], json!(2));
        // The body really is base64, i.e. the advertised encoding is truthful.
        assert_eq!(value["body"], json!("W1sxLCAyXSwge30sIHt9XQ=="));
        assert_eq!(msg.properties.body_encoding(), BODY_ENCODING_BASE64);

        // ... and the envelope still round-trips through our own parser.
        let restored: Message = serde_json::from_value(value).expect("round-trip");
        assert_eq!(restored, msg);
    }

    /// A payload written before `body_encoding` existed must still parse
    /// (the field is serde-defaulted, not required).
    #[test]
    fn test_properties_without_body_encoding_still_parse() {
        let props: MessageProperties =
            serde_json::from_str(r#"{"delivery_mode":2}"#).expect("legacy properties must parse");
        assert_eq!(props, MessageProperties::default());
    }

    /// Regression: a canvas-configured time limit had nowhere to go on the
    /// wire -- the Celery header name `timelimit` appeared in zero files.
    /// Celery carries it as `headers['timelimit'] = [soft, hard]`.
    #[test]
    fn test_timelimit_header_round_trips_in_celery_shape() {
        let headers = MessageHeaders::new("tasks.slow".to_string(), uuid::Uuid::new_v4())
            .with_timelimit(Some(30), Some(60));

        let value = serde_json::to_value(&headers).expect("serialize headers");
        // Celery's shape: a two-element [soft, hard] list at the top level of
        // the headers object.
        assert_eq!(value["timelimit"], json!([30, 60]));

        let restored: MessageHeaders = serde_json::from_value(value).expect("deserialize headers");
        assert_eq!(restored.timelimit(), Some((Some(30), Some(60))));
        assert_eq!(restored.soft_time_limit(), Some(30));
        assert_eq!(restored.hard_time_limit(), Some(60));
    }

    /// Either slot of the pair may be null, which is how Celery expresses
    /// "only a hard limit" / "only a soft limit".
    #[test]
    fn test_timelimit_header_allows_null_slots() {
        let headers = MessageHeaders::new("tasks.slow".to_string(), uuid::Uuid::new_v4())
            .with_timelimit(None, Some(45));

        let value = serde_json::to_value(&headers).expect("serialize headers");
        assert_eq!(value["timelimit"], json!([null, 45]));

        let restored: MessageHeaders = serde_json::from_value(value).expect("deserialize headers");
        assert_eq!(restored.timelimit(), Some((None, Some(45))));
        assert_eq!(restored.soft_time_limit(), None);
        assert_eq!(restored.hard_time_limit(), Some(45));

        // Absent header -> no time limits at all.
        let plain = MessageHeaders::new("tasks.fast".to_string(), uuid::Uuid::new_v4());
        assert_eq!(plain.timelimit(), None);
        assert_eq!(plain.soft_time_limit(), None);

        // Malformed values are reported as absent rather than panicking.
        let mut broken = MessageHeaders::new("tasks.fast".to_string(), uuid::Uuid::new_v4());
        broken
            .extra
            .insert(TIMELIMIT_HEADER.to_string(), json!("30"));
        assert_eq!(broken.timelimit(), None);
        broken
            .extra
            .insert(TIMELIMIT_HEADER.to_string(), json!([30]));
        assert_eq!(broken.timelimit(), None);
    }

    /// The observability headers Flower and `celery events` display.
    #[test]
    fn test_observability_headers_round_trip() {
        let headers = MessageHeaders::new("tasks.add".to_string(), uuid::Uuid::new_v4())
            .with_argsrepr("(1, 2)")
            .with_kwargsrepr("{'debug': True}")
            .with_origin("1234@worker.local")
            .with_shadow("tasks.add[display]")
            .with_ignore_result(true);

        let value = serde_json::to_value(&headers).expect("serialize headers");
        assert_eq!(value["argsrepr"], json!("(1, 2)"));
        assert_eq!(value["kwargsrepr"], json!("{'debug': True}"));
        assert_eq!(value["origin"], json!("1234@worker.local"));
        assert_eq!(value["shadow"], json!("tasks.add[display]"));
        assert_eq!(value["ignore_result"], json!(true));

        let restored: MessageHeaders = serde_json::from_value(value).expect("deserialize headers");
        assert_eq!(restored.argsrepr(), Some("(1, 2)"));
        assert_eq!(restored.kwargsrepr(), Some("{'debug': True}"));
        assert_eq!(restored.origin(), Some("1234@worker.local"));
        assert_eq!(restored.shadow(), Some("tasks.add[display]"));
        assert_eq!(restored.ignore_result(), Some(true));
    }

    /// `default_content_encoding` is the single source of truth for the
    /// content-type -> content-encoding mapping shared by
    /// `MessageBuilder::build` and `build_v5_message`.
    #[test]
    fn test_content_type_default_content_encoding() {
        assert_eq!(
            ContentType::Json.default_content_encoding(),
            ContentEncoding::Utf8
        );
        #[cfg(feature = "msgpack")]
        assert_eq!(
            ContentType::MessagePack.default_content_encoding(),
            ContentEncoding::Binary
        );
        assert_eq!(
            ContentType::Custom("application/x-custom".to_string()).default_content_encoding(),
            ContentEncoding::Binary
        );
    }
}
