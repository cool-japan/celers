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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MessageProperties {
    /// Correlation ID for RPC-style calls
    #[serde(skip_serializing_if = "Option::is_none")]
    pub correlation_id: Option<String>,

    /// Reply-to queue for results
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,

    /// Delivery mode (1 = non-persistent, 2 = persistent)
    #[serde(default = "default_delivery_mode")]
    pub delivery_mode: u8,

    /// Priority (0-9, higher = more priority)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
}

const fn default_delivery_mode() -> u8 {
    2 // Persistent by default
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
