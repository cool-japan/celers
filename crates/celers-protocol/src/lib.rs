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
//! - **Properties**: AMQP properties (correlation_id, reply_to, delivery_mode)
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
#[derive(Debug, Clone, PartialEq, Eq)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProtocolVersion {
    /// Protocol version 2 (Celery 4.x+)
    V2,
    /// Protocol version 5 (Celery 5.x+)
    V5,
}

impl Default for ProtocolVersion {
    fn default() -> Self {
        Self::V2
    }
}

impl std::fmt::Display for ProtocolVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProtocolVersion::V2 => write!(f, "v2"),
            ProtocolVersion::V5 => write!(f, "v5"),
        }
    }
}

/// Content type for serialization
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContentType {
    /// JSON serialization
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
            ContentType::Json => "application/json",
            #[cfg(feature = "msgpack")]
            ContentType::MessagePack => "application/x-msgpack",
            #[cfg(feature = "binary")]
            ContentType::Binary => "application/octet-stream",
            ContentType::Custom(s) => s,
        }
    }
}

impl Default for ContentType {
    fn default() -> Self {
        Self::Json
    }
}

impl std::fmt::Display for ContentType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Content encoding
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum ContentEncoding {
    /// UTF-8 encoding
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
            ContentEncoding::Utf8 => "utf-8",
            ContentEncoding::Binary => "binary",
            ContentEncoding::Custom(s) => s,
        }
    }
}

impl Default for ContentEncoding {
    fn default() -> Self {
        Self::Utf8
    }
}

impl std::fmt::Display for ContentEncoding {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Message headers (Celery protocol)
#[derive(Debug, Clone, Serialize, Deserialize)]
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
    "rust".to_string()
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

fn default_delivery_mode() -> u8 {
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
            content_type: ContentType::default().as_str().to_string(),
            content_encoding: ContentEncoding::default().as_str().to_string(),
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
    #[inline]
    pub fn has_eta(&self) -> bool {
        self.headers.eta.is_some()
    }

    /// Check if the message has an expiration time
    #[inline]
    pub fn has_expires(&self) -> bool {
        self.headers.expires.is_some()
    }

    /// Check if the message is part of a group
    #[inline]
    pub fn has_group(&self) -> bool {
        self.headers.group.is_some()
    }

    /// Check if the message has a parent task
    #[inline]
    pub fn has_parent(&self) -> bool {
        self.headers.parent_id.is_some()
    }

    /// Check if the message has a root task
    #[inline]
    pub fn has_root(&self) -> bool {
        self.headers.root_id.is_some()
    }

    /// Check if the message is persistent
    #[inline]
    pub fn is_persistent(&self) -> bool {
        self.properties.delivery_mode == 2
    }

    /// Get the task ID
    #[inline]
    pub fn task_id(&self) -> uuid::Uuid {
        self.headers.id
    }

    /// Get the task name
    #[inline]
    pub fn task_name(&self) -> &str {
        &self.headers.task
    }

    /// Get the content type as a string slice
    #[inline]
    pub fn content_type_str(&self) -> &str {
        &self.content_type
    }

    /// Get the content encoding as a string slice
    #[inline]
    pub fn content_encoding_str(&self) -> &str {
        &self.content_encoding
    }

    /// Get the message body size in bytes
    #[inline]
    pub fn body_size(&self) -> usize {
        self.body.len()
    }

    /// Check if the message body is empty
    #[inline]
    pub fn has_empty_body(&self) -> bool {
        self.body.is_empty()
    }

    /// Get the retry count (0 if not set)
    #[inline]
    pub fn retry_count(&self) -> u32 {
        self.headers.retries.unwrap_or(0)
    }

    /// Get the priority (None if not set)
    #[inline]
    pub fn priority(&self) -> Option<u8> {
        self.properties.priority
    }

    /// Check if message has a correlation ID
    #[inline]
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
    #[inline]
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
}

/// Task arguments (args, kwargs)
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
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
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.args.is_empty() && self.kwargs.is_empty()
    }

    /// Get the total number of arguments (positional + keyword)
    #[inline]
    pub fn len(&self) -> usize {
        self.args.len() + self.kwargs.len()
    }

    /// Check if there are any positional arguments
    #[inline]
    pub fn has_args(&self) -> bool {
        !self.args.is_empty()
    }

    /// Check if there are any keyword arguments
    #[inline]
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
        let mut props = MessageProperties::default();
        props.delivery_mode = 3;
        let result = props.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            ValidationError::InvalidDeliveryMode { mode: 3 }
        );
    }

    #[test]
    fn test_message_properties_validate_priority() {
        let mut props = MessageProperties::default();
        props.delivery_mode = 2; // Set valid delivery mode
        props.priority = Some(10);
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
}
