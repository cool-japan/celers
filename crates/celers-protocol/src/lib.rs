//! Celery Protocol v2/v5 implementation
//!
//! This crate provides the core protocol definitions for Celery message format,
//! ensuring compatibility with Python Celery workers.
//!
//! # Protocol Compatibility
//!
//! - Celery Protocol v2 (Celery 4.x+)
//! - Celery Protocol v5 (Celery 5.x+)
//!
//! # Message Format
//!
//! Messages consist of:
//! - **Headers**: Task metadata (task name, ID, parent/root IDs, etc.)
//! - **Properties**: AMQP properties (`correlation_id`, `reply_to`, `delivery_mode`)
//! - **Body**: Serialized task arguments
//! - **Content-Type**: Serialization format ("application/json", "application/x-msgpack")
//! - **Content-Encoding**: Encoding format ("utf-8", "binary")
//!
//! # Modules
//!
//! - [`compat`] - Python Celery compatibility verification
//! - [`serializer`] - Pluggable serialization framework
//! - [`result`] - Task result message format
//! - [`event`] - Celery event message format
//! - [`compression`] - Message body compression
//! - [`embed`] - Embedded body format (args, kwargs, embed)
//! - [`negotiation`] - Protocol version negotiation
//! - [`security`] - Security utilities and content-type whitelist
//! - [`builder`] - Fluent message builder API
//! - [`auth`] - Message authentication and signing (HMAC)
//! - [`crypto`] - Message encryption (AES-256-GCM)
//! - [`extensions`] - Message extensions and utility helpers
//! - [`migration`] - Protocol version migration helpers
//! - [`middleware`] - Message transformation middleware
//! - [`zerocopy`] - Zero-copy deserialization for performance
//! - [`lazy`] - Lazy deserialization for large messages
//! - [`pool`] - Message pooling for memory efficiency
//! - [`extension_api`] - Custom protocol extensions API
//! - [`utils`] - Message utility helpers
//! - [`batch`] - Batch message processing utilities
//! - [`routing`] - Message routing helpers
//! - [`retry`] - Retry strategy utilities
//! - [`dedup`] - Message deduplication utilities
//! - [`priority_queue`] - Priority-based message queues
//! - [`workflow`] - Workflow and task chain utilities

pub mod auth;
pub mod batch;
pub mod builder;
pub mod compat;
pub mod compression;
pub mod crypto;
pub mod dedup;
pub mod embed;
pub mod event;
pub mod extension_api;
pub mod extensions;
pub mod lazy;
pub mod middleware;
pub mod migration;
pub mod negotiation;
pub mod pool;
pub mod priority_queue;
pub mod result;
pub mod retry;
pub mod routing;
pub mod security;
pub mod serializer;
pub mod utils;
pub mod workflow;
pub mod zerocopy;

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

impl Message {
    /// Create a new message with JSON body
    pub fn new(task: String, id: Uuid, body: Vec<u8>) -> Self {
        Self {
            headers: MessageHeaders::new(task, id),
            properties: MessageProperties::default(),
            body,
            content_type: CONTENT_TYPE_JSON.to_string(),
            content_encoding: ENCODING_UTF8.to_string(),
        }
    }

    /// Set priority (0-9)
    #[must_use]
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.properties.priority = Some(priority);
        self
    }

    /// Set parent task ID
    #[must_use]
    pub fn with_parent(mut self, parent_id: Uuid) -> Self {
        self.headers.parent_id = Some(parent_id);
        self
    }

    /// Set root task ID
    #[must_use]
    pub fn with_root(mut self, root_id: Uuid) -> Self {
        self.headers.root_id = Some(root_id);
        self
    }

    /// Set group ID
    #[must_use]
    pub fn with_group(mut self, group: Uuid) -> Self {
        self.headers.group = Some(group);
        self
    }

    /// Set ETA (delayed execution)
    #[must_use]
    pub fn with_eta(mut self, eta: DateTime<Utc>) -> Self {
        self.headers.eta = Some(eta);
        self
    }

    /// Set expiration
    #[must_use]
    pub fn with_expires(mut self, expires: DateTime<Utc>) -> Self {
        self.headers.expires = Some(expires);
        self
    }

    /// Set retry count
    #[must_use]
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.headers.retries = Some(retries);
        self
    }

    /// Set correlation ID (for RPC-style calls)
    #[must_use]
    pub fn with_correlation_id(mut self, correlation_id: String) -> Self {
        self.properties.correlation_id = Some(correlation_id);
        self
    }

    /// Set reply-to queue (for results)
    #[must_use]
    pub fn with_reply_to(mut self, reply_to: String) -> Self {
        self.properties.reply_to = Some(reply_to);
        self
    }

    /// Set delivery mode (1 = non-persistent, 2 = persistent)
    #[must_use]
    pub fn with_delivery_mode(mut self, mode: u8) -> Self {
        self.properties.delivery_mode = mode;
        self
    }

    /// Validate the complete message
    ///
    /// Validates:
    /// - Headers (task name, retries, eta/expires)
    /// - Properties (delivery mode, priority)
    /// - Content type format
    /// - Body size
    pub fn validate(&self) -> Result<(), ValidationError> {
        // Validate headers
        self.headers.validate()?;

        // Validate properties
        self.properties.validate()?;

        // Validate content type
        if self.content_type.is_empty() {
            return Err(ValidationError::EmptyContentType);
        }

        // Validate body
        if self.body.is_empty() {
            return Err(ValidationError::EmptyBody);
        }

        if self.body.len() > 10_485_760 {
            // 10MB limit
            return Err(ValidationError::BodyTooLarge {
                size: self.body.len(),
                max: 10_485_760,
            });
        }

        Ok(())
    }

    /// Validate with custom body size limit
    pub fn validate_with_limit(&self, max_body_bytes: usize) -> Result<(), ValidationError> {
        self.headers.validate()?;
        self.properties.validate()?;

        if self.content_type.is_empty() {
            return Err(ValidationError::EmptyContentType);
        }

        if self.body.is_empty() {
            return Err(ValidationError::EmptyBody);
        }

        if self.body.len() > max_body_bytes {
            return Err(ValidationError::BodyTooLarge {
                size: self.body.len(),
                max: max_body_bytes,
            });
        }

        Ok(())
    }

    /// Check if the message has an ETA (delayed execution)
    #[inline(always)]
    pub fn has_eta(&self) -> bool {
        self.headers.eta.is_some()
    }

    /// Check if the message has an expiration time
    #[inline(always)]
    pub fn has_expires(&self) -> bool {
        self.headers.expires.is_some()
    }

    /// Check if the message is part of a group
    #[inline(always)]
    pub fn has_group(&self) -> bool {
        self.headers.group.is_some()
    }

    /// Check if the message has a parent task
    #[inline(always)]
    pub fn has_parent(&self) -> bool {
        self.headers.parent_id.is_some()
    }

    /// Check if the message has a root task
    #[inline(always)]
    pub fn has_root(&self) -> bool {
        self.headers.root_id.is_some()
    }

    /// Check if the message is persistent
    #[inline(always)]
    pub fn is_persistent(&self) -> bool {
        self.properties.delivery_mode == 2
    }

    /// Get the task ID
    #[inline(always)]
    pub fn task_id(&self) -> uuid::Uuid {
        self.headers.id
    }

    /// Get the task name
    #[inline(always)]
    pub fn task_name(&self) -> &str {
        &self.headers.task
    }

    /// Get the content type as a string slice
    #[inline(always)]
    pub fn content_type_str(&self) -> &str {
        &self.content_type
    }

    /// Get the content encoding as a string slice
    #[inline(always)]
    pub fn content_encoding_str(&self) -> &str {
        &self.content_encoding
    }

    /// Get the message body size in bytes
    #[inline(always)]
    pub fn body_size(&self) -> usize {
        self.body.len()
    }

    /// Check if the message body is empty
    #[inline(always)]
    pub fn has_empty_body(&self) -> bool {
        self.body.is_empty()
    }

    /// Get the retry count (0 if not set)
    #[inline(always)]
    pub fn retry_count(&self) -> u32 {
        self.headers.retries.unwrap_or(0)
    }

    /// Get the priority (None if not set)
    #[inline(always)]
    pub fn priority(&self) -> Option<u8> {
        self.properties.priority
    }

    /// Check if message has a correlation ID
    #[inline(always)]
    pub fn has_correlation_id(&self) -> bool {
        self.properties.correlation_id.is_some()
    }

    /// Get the correlation ID
    #[inline]
    pub fn correlation_id(&self) -> Option<&str> {
        self.properties.correlation_id.as_deref()
    }

    /// Get the reply-to queue
    #[inline]
    pub fn reply_to(&self) -> Option<&str> {
        self.properties.reply_to.as_deref()
    }

    /// Check if this is a workflow message (has parent, root, or group)
    #[inline(always)]
    pub fn is_workflow_message(&self) -> bool {
        self.has_parent() || self.has_root() || self.has_group()
    }

    /// Clone the message with a new task ID
    #[must_use]
    pub fn with_new_id(&self) -> Self {
        let mut cloned = self.clone();
        cloned.headers.id = Uuid::new_v4();
        cloned
    }

    /// Create a builder from this message (for modification)
    ///
    /// Note: This creates a new builder with the message's metadata.
    /// The body (args/kwargs) must be set separately on the builder.
    pub fn to_builder(&self) -> crate::builder::MessageBuilder {
        let mut builder = crate::builder::MessageBuilder::new(&self.headers.task);

        // Set basic properties
        builder = builder.id(self.headers.id);

        // Set optional fields
        if let Some(priority) = self.properties.priority {
            builder = builder.priority(priority);
        }
        if let Some(parent_id) = self.headers.parent_id {
            builder = builder.parent(parent_id);
        }
        if let Some(root_id) = self.headers.root_id {
            builder = builder.root(root_id);
        }
        if let Some(group) = self.headers.group {
            builder = builder.group(group);
        }
        if let Some(eta) = self.headers.eta {
            builder = builder.eta(eta);
        }
        if let Some(expires) = self.headers.expires {
            builder = builder.expires(expires);
        }

        builder
    }

    /// Check if the message is ready for immediate execution (not delayed)
    #[inline]
    pub fn is_ready_for_execution(&self) -> bool {
        match self.headers.eta {
            None => true,
            Some(eta) => chrono::Utc::now() >= eta,
        }
    }

    /// Check if the message has not expired yet
    #[inline]
    pub fn is_not_expired(&self) -> bool {
        match self.headers.expires {
            None => true,
            Some(expires) => chrono::Utc::now() < expires,
        }
    }

    /// Check if the message should be processed (not expired and ready for execution)
    #[inline]
    pub fn should_process(&self) -> bool {
        self.is_ready_for_execution() && self.is_not_expired()
    }

    /// Set ETA to now + duration (builder pattern)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_protocol::Message;
    /// use uuid::Uuid;
    /// use chrono::Duration;
    ///
    /// let msg = Message::new("task".to_string(), Uuid::new_v4(), vec![])
    ///     .with_eta_delay(Duration::minutes(5));
    /// assert!(msg.has_eta());
    /// ```
    #[must_use]
    pub fn with_eta_delay(mut self, delay: chrono::Duration) -> Self {
        self.headers.eta = Some(chrono::Utc::now() + delay);
        self
    }

    /// Set expiration to now + duration (builder pattern)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_protocol::Message;
    /// use uuid::Uuid;
    /// use chrono::Duration;
    ///
    /// let msg = Message::new("task".to_string(), Uuid::new_v4(), vec![])
    ///     .with_expires_in(Duration::hours(1));
    /// assert!(msg.has_expires());
    /// ```
    #[must_use]
    pub fn with_expires_in(mut self, duration: chrono::Duration) -> Self {
        self.headers.expires = Some(chrono::Utc::now() + duration);
        self
    }

    /// Get the time remaining until ETA (None if no ETA or already past)
    #[inline]
    pub fn time_until_eta(&self) -> Option<chrono::Duration> {
        self.headers.eta.and_then(|eta| {
            let now = chrono::Utc::now();
            if eta > now {
                Some(eta - now)
            } else {
                None
            }
        })
    }

    /// Get the time remaining until expiration (None if no expiration or already expired)
    #[inline]
    pub fn time_until_expiration(&self) -> Option<chrono::Duration> {
        self.headers.expires.and_then(|expires| {
            let now = chrono::Utc::now();
            if expires > now {
                Some(expires - now)
            } else {
                None
            }
        })
    }

    /// Increment the retry count (returns new count)
    pub fn increment_retry(&mut self) -> u32 {
        let new_count = self.headers.retries.unwrap_or(0) + 1;
        self.headers.retries = Some(new_count);
        new_count
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
mod tests {
    use super::*;

    #[test]
    fn test_message_creation() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body);

        assert_eq!(msg.headers.task, "tasks.add");
        assert_eq!(msg.headers.id, task_id);
        assert_eq!(msg.headers.lang, "rust");
        assert_eq!(msg.content_type, "application/json");
    }

    #[test]
    fn test_message_with_priority() {
        let task_id = Uuid::new_v4();
        let body = vec![];
        let msg = Message::new("tasks.test".to_string(), task_id, body).with_priority(9);

        assert_eq!(msg.properties.priority, Some(9));
    }

    #[test]
    fn test_task_args() {
        let args = TaskArgs::new().with_args(vec![serde_json::json!(1), serde_json::json!(2)]);

        assert_eq!(args.args.len(), 2);
        assert_eq!(args.kwargs.len(), 0);
    }

    #[test]
    fn test_protocol_version_default() {
        let version = ProtocolVersion::default();
        assert_eq!(version, ProtocolVersion::V2);
    }

    #[test]
    fn test_protocol_version_display() {
        assert_eq!(ProtocolVersion::V2.to_string(), "v2");
        assert_eq!(ProtocolVersion::V5.to_string(), "v5");
    }

    #[test]
    fn test_content_type_as_str() {
        assert_eq!(ContentType::Json.as_str(), "application/json");
        assert_eq!(
            ContentType::Custom("text/plain".to_string()).as_str(),
            "text/plain"
        );
    }

    #[test]
    fn test_content_type_default() {
        let ct = ContentType::default();
        assert_eq!(ct, ContentType::Json);
    }

    #[test]
    fn test_content_type_display() {
        assert_eq!(ContentType::Json.to_string(), "application/json");
        assert_eq!(
            ContentType::Custom("text/xml".to_string()).to_string(),
            "text/xml"
        );
    }

    #[test]
    fn test_content_encoding_as_str() {
        assert_eq!(ContentEncoding::Utf8.as_str(), "utf-8");
        assert_eq!(ContentEncoding::Binary.as_str(), "binary");
        assert_eq!(ContentEncoding::Custom("gzip".to_string()).as_str(), "gzip");
    }

    #[test]
    fn test_content_encoding_default() {
        let ce = ContentEncoding::default();
        assert_eq!(ce, ContentEncoding::Utf8);
    }

    #[test]
    fn test_content_encoding_display() {
        assert_eq!(ContentEncoding::Utf8.to_string(), "utf-8");
        assert_eq!(ContentEncoding::Binary.to_string(), "binary");
    }

    #[test]
    fn test_message_headers_validate_empty_task() {
        let headers = MessageHeaders::new("".to_string(), Uuid::new_v4());
        let result = headers.validate();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), ValidationError::EmptyTaskName);
    }

    #[test]
    fn test_message_headers_validate_retries_limit() {
        let mut headers = MessageHeaders::new("test".to_string(), Uuid::new_v4());
        headers.retries = Some(1001);
        let result = headers.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            ValidationError::RetryLimitExceeded {
                retries: 1001,
                max: 1000
            }
        );
    }

    #[test]
    fn test_message_headers_validate_eta_expires() {
        let mut headers = MessageHeaders::new("test".to_string(), Uuid::new_v4());
        headers.eta = Some(Utc::now() + chrono::Duration::hours(2));
        headers.expires = Some(Utc::now() + chrono::Duration::hours(1));
        let result = headers.validate();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), ValidationError::EtaAfterExpiration);
    }

    #[test]
    fn test_message_properties_validate_delivery_mode() {
        let props = MessageProperties {
            delivery_mode: 3,
            ..MessageProperties::default()
        };
        let result = props.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            ValidationError::InvalidDeliveryMode { mode: 3 }
        );
    }

    #[test]
    fn test_message_properties_validate_priority() {
        let props = MessageProperties {
            delivery_mode: 2, // Set valid delivery mode
            priority: Some(10),
            ..MessageProperties::default()
        };
        let result = props.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            ValidationError::InvalidPriority { priority: 10 }
        );
    }

    #[test]
    fn test_message_predicates() {
        let task_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();
        let root_id = Uuid::new_v4();
        let group_id = Uuid::new_v4();

        let mut msg = Message::new("test".to_string(), task_id, vec![1, 2, 3])
            .with_parent(parent_id)
            .with_root(root_id)
            .with_group(group_id)
            .with_eta(Utc::now() + chrono::Duration::hours(1))
            .with_expires(Utc::now() + chrono::Duration::days(1));

        // Set delivery_mode to 2 for persistence check
        msg.properties.delivery_mode = 2;

        assert!(msg.has_parent());
        assert!(msg.has_root());
        assert!(msg.has_group());
        assert!(msg.has_eta());
        assert!(msg.has_expires());
        assert!(msg.is_persistent());
    }

    #[test]
    fn test_message_accessors() {
        let task_id = Uuid::new_v4();
        let msg = Message::new("my_task".to_string(), task_id, vec![1, 2, 3]);

        assert_eq!(msg.task_id(), task_id);
        assert_eq!(msg.task_name(), "my_task");
    }

    #[test]
    fn test_task_args_add_methods() {
        let mut args = TaskArgs::new();
        assert!(args.is_empty());

        args.add_arg(serde_json::json!(1));
        args.add_arg(serde_json::json!(2));
        assert_eq!(args.len(), 2);
        assert!(args.has_args());
        assert!(!args.has_kwargs());

        args.add_kwarg("key1".to_string(), serde_json::json!("value1"));
        assert_eq!(args.len(), 3);
        assert!(args.has_kwargs());
    }

    #[test]
    fn test_task_args_get_methods() {
        let mut args = TaskArgs::new();
        args.add_arg(serde_json::json!(42));
        args.add_kwarg("name".to_string(), serde_json::json!("test"));

        assert_eq!(args.get_arg(0), Some(&serde_json::json!(42)));
        assert_eq!(args.get_arg(1), None);
        assert_eq!(args.get_kwarg("name"), Some(&serde_json::json!("test")));
        assert_eq!(args.get_kwarg("missing"), None);
    }

    #[test]
    fn test_task_args_clear() {
        let mut args = TaskArgs::new()
            .with_args(vec![serde_json::json!(1)])
            .with_kwargs({
                let mut map = HashMap::new();
                map.insert("key".to_string(), serde_json::json!("value"));
                map
            });

        assert!(!args.is_empty());
        args.clear();
        assert!(args.is_empty());
        assert_eq!(args.len(), 0);
    }

    #[test]
    fn test_task_args_partial_eq() {
        let args1 = TaskArgs::new().with_args(vec![serde_json::json!(1), serde_json::json!(2)]);
        let args2 = TaskArgs::new().with_args(vec![serde_json::json!(1), serde_json::json!(2)]);
        let args3 = TaskArgs::new().with_args(vec![serde_json::json!(1)]);

        assert_eq!(args1, args2);
        assert_ne!(args1, args3);
    }

    #[test]
    fn test_message_body_methods() {
        let msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3, 4, 5]);
        assert_eq!(msg.body_size(), 5);
        assert!(!msg.has_empty_body());

        let empty_msg = Message::new("test".to_string(), Uuid::new_v4(), vec![]);
        assert_eq!(empty_msg.body_size(), 0);
        assert!(empty_msg.has_empty_body());
    }

    #[test]
    fn test_message_content_accessors() {
        let msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1]);
        assert_eq!(msg.content_type_str(), "application/json");
        assert_eq!(msg.content_encoding_str(), "utf-8");
    }

    #[test]
    fn test_message_retry_and_priority() {
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1]);
        assert_eq!(msg.retry_count(), 0);
        assert_eq!(msg.priority(), None);

        msg.headers.retries = Some(3);
        msg.properties.priority = Some(5);
        assert_eq!(msg.retry_count(), 3);
        assert_eq!(msg.priority(), Some(5));
    }

    #[test]
    fn test_message_correlation_and_reply() {
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1]);
        assert!(!msg.has_correlation_id());
        assert_eq!(msg.correlation_id(), None);
        assert_eq!(msg.reply_to(), None);

        msg.properties.correlation_id = Some("corr-123".to_string());
        msg.properties.reply_to = Some("reply-queue".to_string());
        assert!(msg.has_correlation_id());
        assert_eq!(msg.correlation_id(), Some("corr-123"));
        assert_eq!(msg.reply_to(), Some("reply-queue"));
    }

    #[test]
    fn test_message_workflow_check() {
        let msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1]);
        assert!(!msg.is_workflow_message());

        let workflow_msg = msg.with_parent(Uuid::new_v4());
        assert!(workflow_msg.is_workflow_message());
    }

    #[test]
    fn test_message_with_new_id() {
        let task_id = Uuid::new_v4();
        let msg = Message::new("test".to_string(), task_id, vec![1, 2, 3]);
        let new_msg = msg.with_new_id();

        assert_ne!(msg.task_id(), new_msg.task_id());
        assert_eq!(msg.task_name(), new_msg.task_name());
        assert_eq!(msg.body, new_msg.body);
    }

    #[test]
    fn test_message_to_builder() {
        let task_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();
        let msg = Message::new("test.task".to_string(), task_id, vec![1, 2, 3])
            .with_priority(5)
            .with_parent(parent_id);

        let builder = msg.to_builder();
        // Need to add args since builder doesn't copy body automatically
        let rebuilt = builder.args(vec![serde_json::json!(1)]).build().unwrap();

        assert_eq!(rebuilt.task_id(), msg.task_id());
        assert_eq!(rebuilt.task_name(), msg.task_name());
        assert_eq!(rebuilt.priority(), msg.priority());
        assert_eq!(rebuilt.headers.parent_id, msg.headers.parent_id);
    }

    #[test]
    fn test_protocol_version_from_str() {
        use std::str::FromStr;
        assert_eq!(
            ProtocolVersion::from_str("v2").unwrap(),
            ProtocolVersion::V2
        );
        assert_eq!(
            ProtocolVersion::from_str("V2").unwrap(),
            ProtocolVersion::V2
        );
        assert_eq!(ProtocolVersion::from_str("2").unwrap(), ProtocolVersion::V2);
        assert_eq!(
            ProtocolVersion::from_str("v5").unwrap(),
            ProtocolVersion::V5
        );
        assert_eq!(
            ProtocolVersion::from_str("V5").unwrap(),
            ProtocolVersion::V5
        );
        assert_eq!(ProtocolVersion::from_str("5").unwrap(), ProtocolVersion::V5);
        assert!(ProtocolVersion::from_str("v3").is_err());
        assert!(ProtocolVersion::from_str("invalid").is_err());
    }

    #[test]
    fn test_protocol_version_ordering() {
        assert!(ProtocolVersion::V2 < ProtocolVersion::V5);
        assert!(ProtocolVersion::V5 > ProtocolVersion::V2);
        assert_eq!(ProtocolVersion::V2, ProtocolVersion::V2);
    }

    #[test]
    fn test_content_type_from_str() {
        use std::str::FromStr;
        assert_eq!(
            ContentType::from_str("application/json").unwrap(),
            ContentType::Json
        );
        assert_eq!(
            ContentType::from_str("text/plain").unwrap(),
            ContentType::Custom("text/plain".to_string())
        );
    }

    #[test]
    fn test_content_encoding_from_str() {
        use std::str::FromStr;
        assert_eq!(
            ContentEncoding::from_str("utf-8").unwrap(),
            ContentEncoding::Utf8
        );
        assert_eq!(
            ContentEncoding::from_str("binary").unwrap(),
            ContentEncoding::Binary
        );
        assert_eq!(
            ContentEncoding::from_str("gzip").unwrap(),
            ContentEncoding::Custom("gzip".to_string())
        );
    }

    #[test]
    fn test_message_headers_equality() {
        let id = Uuid::new_v4();
        let headers1 = MessageHeaders::new("tasks.add".to_string(), id);
        let headers2 = MessageHeaders::new("tasks.add".to_string(), id);
        let headers3 = MessageHeaders::new("tasks.sub".to_string(), id);

        assert_eq!(headers1, headers2);
        assert_ne!(headers1, headers3);
    }

    #[test]
    fn test_message_properties_equality() {
        let props1 = MessageProperties::default();
        let props2 = MessageProperties::default();
        let props3 = MessageProperties {
            priority: Some(5),
            ..Default::default()
        };

        assert_eq!(props1, props2);
        assert_ne!(props1, props3);
    }

    #[test]
    fn test_message_equality() {
        let id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let msg1 = Message::new("tasks.add".to_string(), id, body.clone());
        let msg2 = Message::new("tasks.add".to_string(), id, body.clone());
        let msg3 = Message::new("tasks.add".to_string(), id, vec![4, 5, 6]);

        assert_eq!(msg1, msg2);
        assert_ne!(msg1, msg3);
    }

    #[test]
    fn test_message_equality_with_options() {
        let id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();
        let body = vec![1, 2, 3];

        let msg1 = Message::new("tasks.add".to_string(), id, body.clone())
            .with_priority(5)
            .with_parent(parent_id);
        let msg2 = Message::new("tasks.add".to_string(), id, body.clone())
            .with_priority(5)
            .with_parent(parent_id);
        let msg3 = Message::new("tasks.add".to_string(), id, body.clone())
            .with_priority(3)
            .with_parent(parent_id);

        assert_eq!(msg1, msg2);
        assert_ne!(msg1, msg3);
    }

    #[test]
    fn test_task_args_equality() {
        let args1 = TaskArgs::new().with_args(vec![serde_json::json!(1), serde_json::json!(2)]);
        let args2 = TaskArgs::new().with_args(vec![serde_json::json!(1), serde_json::json!(2)]);
        let args3 = TaskArgs::new().with_args(vec![serde_json::json!(3), serde_json::json!(4)]);

        assert_eq!(args1, args2);
        assert_ne!(args1, args3);
    }

    #[test]
    fn test_task_args_equality_with_kwargs() {
        let mut kwargs1 = std::collections::HashMap::new();
        kwargs1.insert("key".to_string(), serde_json::json!("value"));

        let mut kwargs2 = std::collections::HashMap::new();
        kwargs2.insert("key".to_string(), serde_json::json!("value"));

        let args1 = TaskArgs::new().with_kwargs(kwargs1);
        let args2 = TaskArgs::new().with_kwargs(kwargs2);
        let args3 = TaskArgs::new();

        assert_eq!(args1, args2);
        assert_ne!(args1, args3);
    }

    #[test]
    fn test_content_type_from_str_trait() {
        let ct1: ContentType = "application/json".into();
        let ct2: ContentType = "text/plain".into();

        assert_eq!(ct1, ContentType::Json);
        assert_eq!(ct2, ContentType::Custom("text/plain".to_string()));
    }

    #[test]
    fn test_content_encoding_from_str_trait() {
        let ce1: ContentEncoding = "utf-8".into();
        let ce2: ContentEncoding = "binary".into();
        let ce3: ContentEncoding = "gzip".into();

        assert_eq!(ce1, ContentEncoding::Utf8);
        assert_eq!(ce2, ContentEncoding::Binary);
        assert_eq!(ce3, ContentEncoding::Custom("gzip".to_string()));
    }

    #[test]
    fn test_content_type_as_ref() {
        let ct = ContentType::Json;
        let s: &str = ct.as_ref();
        assert_eq!(s, "application/json");

        let ct_custom = ContentType::Custom("text/plain".to_string());
        let s_custom: &str = ct_custom.as_ref();
        assert_eq!(s_custom, "text/plain");
    }

    #[test]
    fn test_content_encoding_as_ref() {
        let ce = ContentEncoding::Utf8;
        let s: &str = ce.as_ref();
        assert_eq!(s, "utf-8");

        let ce_binary = ContentEncoding::Binary;
        let s_binary: &str = ce_binary.as_ref();
        assert_eq!(s_binary, "binary");
    }

    #[test]
    fn test_content_type_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(ContentType::Json);
        set.insert(ContentType::Json); // Duplicate
        #[cfg(feature = "msgpack")]
        set.insert(ContentType::MessagePack);
        set.insert(ContentType::Custom("text/plain".to_string()));

        #[cfg(feature = "msgpack")]
        assert_eq!(set.len(), 3);
        #[cfg(not(feature = "msgpack"))]
        assert_eq!(set.len(), 2);

        assert!(set.contains(&ContentType::Json));
    }

    #[test]
    fn test_content_encoding_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(ContentEncoding::Utf8);
        set.insert(ContentEncoding::Utf8); // Duplicate
        set.insert(ContentEncoding::Binary);
        set.insert(ContentEncoding::Custom("base64".to_string()));

        assert_eq!(set.len(), 3);
        assert!(set.contains(&ContentEncoding::Utf8));
        assert!(set.contains(&ContentEncoding::Binary));
    }

    #[test]
    fn test_message_with_retries() {
        let msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1]).with_retries(5);

        assert_eq!(msg.headers.retries, Some(5));
        assert_eq!(msg.retry_count(), 5);
    }

    #[test]
    fn test_message_with_correlation_id() {
        let msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1])
            .with_correlation_id("corr-123".to_string());

        assert_eq!(msg.properties.correlation_id, Some("corr-123".to_string()));
        assert_eq!(msg.correlation_id(), Some("corr-123"));
    }

    #[test]
    fn test_message_with_reply_to() {
        let msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1])
            .with_reply_to("reply-queue".to_string());

        assert_eq!(msg.properties.reply_to, Some("reply-queue".to_string()));
        assert_eq!(msg.reply_to(), Some("reply-queue"));
    }

    #[test]
    fn test_message_with_delivery_mode() {
        let msg = Message::new("test".to_string(), Uuid::new_v4(), vec![1]).with_delivery_mode(1);

        assert_eq!(msg.properties.delivery_mode, 1);
        assert!(!msg.is_persistent());

        let persistent_msg =
            Message::new("test".to_string(), Uuid::new_v4(), vec![1]).with_delivery_mode(2);

        assert_eq!(persistent_msg.properties.delivery_mode, 2);
        assert!(persistent_msg.is_persistent());
    }

    #[test]
    fn test_message_builder_chaining() {
        let parent_id = Uuid::new_v4();
        let msg = Message::new("test.task".to_string(), Uuid::new_v4(), vec![1, 2, 3])
            .with_priority(7)
            .with_retries(3)
            .with_correlation_id("corr-456".to_string())
            .with_reply_to("result-queue".to_string())
            .with_parent(parent_id)
            .with_delivery_mode(1);

        assert_eq!(msg.priority(), Some(7));
        assert_eq!(msg.retry_count(), 3);
        assert_eq!(msg.correlation_id(), Some("corr-456"));
        assert_eq!(msg.reply_to(), Some("result-queue"));
        assert_eq!(msg.headers.parent_id, Some(parent_id));
        assert_eq!(msg.properties.delivery_mode, 1);
        assert!(!msg.is_persistent());
    }

    #[test]
    fn test_protocol_version_is_v2() {
        assert!(ProtocolVersion::V2.is_v2());
        assert!(!ProtocolVersion::V5.is_v2());
    }

    #[test]
    fn test_protocol_version_is_v5() {
        assert!(ProtocolVersion::V5.is_v5());
        assert!(!ProtocolVersion::V2.is_v5());
    }

    #[test]
    fn test_protocol_version_as_u8() {
        assert_eq!(ProtocolVersion::V2.as_u8(), 2);
        assert_eq!(ProtocolVersion::V5.as_u8(), 5);
    }

    #[test]
    fn test_protocol_version_as_number_str() {
        assert_eq!(ProtocolVersion::V2.as_number_str(), "2");
        assert_eq!(ProtocolVersion::V5.as_number_str(), "5");
    }

    #[test]
    fn test_task_args_from_json() {
        let json = r#"{"args":[1,2,3],"kwargs":{"key":"value"}}"#;
        let args = TaskArgs::from_json(json).unwrap();

        assert_eq!(args.args.len(), 3);
        assert_eq!(args.kwargs.len(), 1);
        assert_eq!(args.get_kwarg("key"), Some(&serde_json::json!("value")));
    }

    #[test]
    fn test_task_args_to_json() {
        let mut args = TaskArgs::new();
        args.add_arg(serde_json::json!(1));
        args.add_arg(serde_json::json!(2));
        args.add_kwarg("key".to_string(), serde_json::json!("value"));

        let json = args.to_json().unwrap();
        assert!(json.contains("\"args\""));
        assert!(json.contains("\"kwargs\""));
        assert!(json.contains("\"key\""));
    }

    #[test]
    fn test_task_args_to_json_pretty() {
        let args = TaskArgs::new()
            .with_args(vec![serde_json::json!(1)])
            .with_kwargs({
                let mut map = std::collections::HashMap::new();
                map.insert("test".to_string(), serde_json::json!("value"));
                map
            });

        let json_pretty = args.to_json_pretty().unwrap();
        assert!(json_pretty.contains('\n')); // Should have newlines
    }

    #[test]
    fn test_message_is_ready_for_execution() {
        let msg1 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3]);
        assert!(msg1.is_ready_for_execution()); // No ETA

        let msg2 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3])
            .with_eta(chrono::Utc::now() - chrono::Duration::hours(1));
        assert!(msg2.is_ready_for_execution()); // Past ETA

        let msg3 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3])
            .with_eta(chrono::Utc::now() + chrono::Duration::hours(1));
        assert!(!msg3.is_ready_for_execution()); // Future ETA
    }

    #[test]
    fn test_message_is_not_expired() {
        let msg1 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3]);
        assert!(msg1.is_not_expired()); // No expiration

        let msg2 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3])
            .with_expires(chrono::Utc::now() + chrono::Duration::hours(1));
        assert!(msg2.is_not_expired()); // Future expiration

        let msg3 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3])
            .with_expires(chrono::Utc::now() - chrono::Duration::hours(1));
        assert!(!msg3.is_not_expired()); // Past expiration
    }

    #[test]
    fn test_message_should_process() {
        // Ready and not expired
        let msg1 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3]);
        assert!(msg1.should_process());

        // Not ready (future ETA)
        let msg2 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3])
            .with_eta(chrono::Utc::now() + chrono::Duration::hours(1));
        assert!(!msg2.should_process());

        // Expired
        let msg3 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3])
            .with_expires(chrono::Utc::now() - chrono::Duration::hours(1));
        assert!(!msg3.should_process());

        // Ready but expired
        let msg4 = Message::new("test".to_string(), Uuid::new_v4(), vec![1, 2, 3])
            .with_eta(chrono::Utc::now() - chrono::Duration::hours(2))
            .with_expires(chrono::Utc::now() - chrono::Duration::hours(1));
        assert!(!msg4.should_process());
    }

    #[test]
    fn test_message_headers_builder() {
        let task_id = Uuid::new_v4();
        let root_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();
        let group_id = Uuid::new_v4();

        let headers = MessageHeaders::new("task".to_string(), task_id)
            .with_lang("python".to_string())
            .with_root_id(root_id)
            .with_parent_id(parent_id)
            .with_group(group_id)
            .with_retries(3)
            .with_eta(chrono::Utc::now() + chrono::Duration::minutes(5))
            .with_expires(chrono::Utc::now() + chrono::Duration::hours(1));

        assert_eq!(headers.lang, "python");
        assert_eq!(headers.root_id, Some(root_id));
        assert_eq!(headers.parent_id, Some(parent_id));
        assert_eq!(headers.group, Some(group_id));
        assert_eq!(headers.retries, Some(3));
        assert!(headers.eta.is_some());
        assert!(headers.expires.is_some());
    }

    #[test]
    fn test_message_properties_builder() {
        let props = MessageProperties::new()
            .with_correlation_id("corr-123".to_string())
            .with_reply_to("reply.queue".to_string())
            .with_delivery_mode(1)
            .with_priority(5);

        assert_eq!(props.correlation_id, Some("corr-123".to_string()));
        assert_eq!(props.reply_to, Some("reply.queue".to_string()));
        assert_eq!(props.delivery_mode, 1);
        assert_eq!(props.priority, Some(5));
    }

    #[test]
    fn test_message_with_eta_delay() {
        let before = chrono::Utc::now();
        let msg = Message::new("task".to_string(), Uuid::new_v4(), vec![])
            .with_eta_delay(chrono::Duration::minutes(10));
        let after = chrono::Utc::now();

        assert!(msg.has_eta());
        let eta = msg.headers.eta.unwrap();
        // ETA should be roughly 10 minutes from now
        assert!(eta > before + chrono::Duration::minutes(9));
        assert!(eta < after + chrono::Duration::minutes(11));
    }

    #[test]
    fn test_message_with_expires_in() {
        let before = chrono::Utc::now();
        let msg = Message::new("task".to_string(), Uuid::new_v4(), vec![])
            .with_expires_in(chrono::Duration::hours(2));
        let after = chrono::Utc::now();

        assert!(msg.has_expires());
        let expires = msg.headers.expires.unwrap();
        // Expiration should be roughly 2 hours from now
        assert!(expires > before + chrono::Duration::hours(2) - chrono::Duration::seconds(1));
        assert!(expires < after + chrono::Duration::hours(2) + chrono::Duration::seconds(1));
    }

    #[test]
    fn test_message_time_until_eta() {
        // No ETA
        let msg1 = Message::new("task".to_string(), Uuid::new_v4(), vec![]);
        assert!(msg1.time_until_eta().is_none());

        // Future ETA
        let msg2 = Message::new("task".to_string(), Uuid::new_v4(), vec![])
            .with_eta(chrono::Utc::now() + chrono::Duration::minutes(30));
        let time_left = msg2.time_until_eta();
        assert!(time_left.is_some());
        assert!(time_left.unwrap() > chrono::Duration::minutes(29));
        assert!(time_left.unwrap() < chrono::Duration::minutes(31));

        // Past ETA
        let msg3 = Message::new("task".to_string(), Uuid::new_v4(), vec![])
            .with_eta(chrono::Utc::now() - chrono::Duration::minutes(30));
        assert!(msg3.time_until_eta().is_none());
    }

    #[test]
    fn test_message_time_until_expiration() {
        // No expiration
        let msg1 = Message::new("task".to_string(), Uuid::new_v4(), vec![]);
        assert!(msg1.time_until_expiration().is_none());

        // Future expiration
        let msg2 = Message::new("task".to_string(), Uuid::new_v4(), vec![])
            .with_expires(chrono::Utc::now() + chrono::Duration::hours(1));
        let time_left = msg2.time_until_expiration();
        assert!(time_left.is_some());
        assert!(time_left.unwrap() > chrono::Duration::minutes(59));
        assert!(time_left.unwrap() < chrono::Duration::minutes(61));

        // Past expiration
        let msg3 = Message::new("task".to_string(), Uuid::new_v4(), vec![])
            .with_expires(chrono::Utc::now() - chrono::Duration::hours(1));
        assert!(msg3.time_until_expiration().is_none());
    }

    #[test]
    fn test_message_increment_retry() {
        let mut msg = Message::new("task".to_string(), Uuid::new_v4(), vec![]);

        // Initial retry count is 0
        assert_eq!(msg.retry_count(), 0);

        // Increment to 1
        let count1 = msg.increment_retry();
        assert_eq!(count1, 1);
        assert_eq!(msg.retry_count(), 1);

        // Increment to 2
        let count2 = msg.increment_retry();
        assert_eq!(count2, 2);
        assert_eq!(msg.retry_count(), 2);
    }

    #[test]
    fn test_task_args_index_usize() {
        let args = TaskArgs::new().with_args(vec![
            serde_json::json!(1),
            serde_json::json!("hello"),
            serde_json::json!(true),
        ]);

        // Test Index trait
        assert_eq!(args[0], serde_json::json!(1));
        assert_eq!(args[1], serde_json::json!("hello"));
        assert_eq!(args[2], serde_json::json!(true));
    }

    #[test]
    fn test_task_args_index_mut_usize() {
        let mut args = TaskArgs::new().with_args(vec![serde_json::json!(1), serde_json::json!(2)]);

        // Test IndexMut trait
        args[0] = serde_json::json!(100);
        args[1] = serde_json::json!(200);

        assert_eq!(args[0], serde_json::json!(100));
        assert_eq!(args[1], serde_json::json!(200));
    }

    #[test]
    fn test_task_args_index_str() {
        let mut kwargs = HashMap::new();
        kwargs.insert("name".to_string(), serde_json::json!("Alice"));
        kwargs.insert("age".to_string(), serde_json::json!(30));

        let args = TaskArgs::new().with_kwargs(kwargs);

        // Test Index trait with string keys
        assert_eq!(args["name"], serde_json::json!("Alice"));
        assert_eq!(args["age"], serde_json::json!(30));
    }

    #[test]
    #[should_panic(expected = "no entry found for key")]
    fn test_task_args_index_str_panic() {
        let args = TaskArgs::new();
        let _ = &args["nonexistent"]; // Should panic
    }

    #[test]
    fn test_task_args_into_iterator() {
        let args = TaskArgs::new().with_args(vec![
            serde_json::json!(1),
            serde_json::json!(2),
            serde_json::json!(3),
        ]);

        // Test IntoIterator
        let values: Vec<_> = args.into_iter().collect();
        assert_eq!(values.len(), 3);
        assert_eq!(values[0], serde_json::json!(1));
        assert_eq!(values[1], serde_json::json!(2));
        assert_eq!(values[2], serde_json::json!(3));
    }

    #[test]
    fn test_task_args_into_iterator_ref() {
        let args = TaskArgs::new().with_args(vec![serde_json::json!(10), serde_json::json!(20)]);

        // Test IntoIterator for &TaskArgs
        let sum: i64 = (&args).into_iter().filter_map(|v| v.as_i64()).sum();

        assert_eq!(sum, 30);
        // args is still usable
        assert_eq!(args.args.len(), 2);
    }

    #[test]
    fn test_task_args_extend() {
        let mut args = TaskArgs::new().with_args(vec![serde_json::json!(1)]);

        // Test Extend trait with positional args
        args.extend(vec![serde_json::json!(2), serde_json::json!(3)]);

        assert_eq!(args.args.len(), 3);
        assert_eq!(args[0], serde_json::json!(1));
        assert_eq!(args[1], serde_json::json!(2));
        assert_eq!(args[2], serde_json::json!(3));
    }

    #[test]
    fn test_task_args_extend_kwargs() {
        let mut args = TaskArgs::new();

        // Test Extend trait with key-value pairs
        args.extend(vec![
            ("key1".to_string(), serde_json::json!("value1")),
            ("key2".to_string(), serde_json::json!(42)),
        ]);

        assert_eq!(args.kwargs.len(), 2);
        assert_eq!(args["key1"], serde_json::json!("value1"));
        assert_eq!(args["key2"], serde_json::json!(42));
    }

    #[test]
    fn test_task_args_from_iterator() {
        // Test FromIterator trait
        let args: TaskArgs = vec![
            serde_json::json!(1),
            serde_json::json!("hello"),
            serde_json::json!(true),
        ]
        .into_iter()
        .collect();

        assert_eq!(args.args.len(), 3);
        assert_eq!(args.kwargs.len(), 0);
        assert_eq!(args[0], serde_json::json!(1));
        assert_eq!(args[1], serde_json::json!("hello"));
        assert_eq!(args[2], serde_json::json!(true));
    }

    #[test]
    fn test_task_args_from_iterator_range() {
        // Build TaskArgs from range using FromIterator
        let args: TaskArgs = (1..=5).map(|i| serde_json::json!(i)).collect();

        assert_eq!(args.args.len(), 5);
        assert_eq!(args[0], serde_json::json!(1));
        assert_eq!(args[4], serde_json::json!(5));
    }

    #[test]
    fn test_task_args_iterator_chain() {
        // Test combining traits: FromIterator, IntoIterator, Extend
        let args1: TaskArgs = vec![serde_json::json!(1), serde_json::json!(2)]
            .into_iter()
            .collect();

        let mut args2 = TaskArgs::new();
        args2.extend(vec![serde_json::json!(3), serde_json::json!(4)]);

        // Extend args2 with args1's values
        args2.extend(args1);

        assert_eq!(args2.args.len(), 4);
        assert_eq!(args2[0], serde_json::json!(3));
        assert_eq!(args2[3], serde_json::json!(2));
    }
}
