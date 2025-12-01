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

pub mod compat;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

/// Protocol version
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
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
    pub fn validate(&self) -> Result<(), String> {
        if self.task.is_empty() {
            return Err("Task name cannot be empty".to_string());
        }

        if let Some(retries) = self.retries {
            if retries > 1000 {
                return Err("Retries cannot exceed 1000".to_string());
            }
        }

        // Validate ETA and expiration relationship
        if let (Some(eta), Some(expires)) = (self.eta, self.expires) {
            if eta > expires {
                return Err("ETA cannot be after expiration time".to_string());
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
    pub fn validate(&self) -> Result<(), String> {
        if self.delivery_mode != 1 && self.delivery_mode != 2 {
            return Err("Delivery mode must be 1 (non-persistent) or 2 (persistent)".to_string());
        }

        if let Some(priority) = self.priority {
            if priority > 9 {
                return Err("Priority must be between 0 and 9".to_string());
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
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.properties.priority = Some(priority);
        self
    }

    /// Set parent task ID
    pub fn with_parent(mut self, parent_id: Uuid) -> Self {
        self.headers.parent_id = Some(parent_id);
        self
    }

    /// Set root task ID
    pub fn with_root(mut self, root_id: Uuid) -> Self {
        self.headers.root_id = Some(root_id);
        self
    }

    /// Set group ID
    pub fn with_group(mut self, group: Uuid) -> Self {
        self.headers.group = Some(group);
        self
    }

    /// Set ETA (delayed execution)
    pub fn with_eta(mut self, eta: DateTime<Utc>) -> Self {
        self.headers.eta = Some(eta);
        self
    }

    /// Set expiration
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
    pub fn validate(&self) -> Result<(), String> {
        // Validate headers
        self.headers.validate()?;

        // Validate properties
        self.properties.validate()?;

        // Validate content type
        if self.content_type.is_empty() {
            return Err("Content type cannot be empty".to_string());
        }

        // Validate body
        if self.body.is_empty() {
            return Err("Message body cannot be empty".to_string());
        }

        if self.body.len() > 10_485_760 {
            // 10MB limit
            return Err(format!(
                "Message body too large: {} bytes (max 10MB)",
                self.body.len()
            ));
        }

        Ok(())
    }

    /// Validate with custom body size limit
    pub fn validate_with_limit(&self, max_body_bytes: usize) -> Result<(), String> {
        self.headers.validate()?;
        self.properties.validate()?;

        if self.content_type.is_empty() {
            return Err("Content type cannot be empty".to_string());
        }

        if self.body.is_empty() {
            return Err("Message body cannot be empty".to_string());
        }

        if self.body.len() > max_body_bytes {
            return Err(format!(
                "Message body too large: {} bytes (max {} bytes)",
                self.body.len(),
                max_body_bytes
            ));
        }

        Ok(())
    }

    /// Check if the message has an ETA (delayed execution)
    pub fn has_eta(&self) -> bool {
        self.headers.eta.is_some()
    }

    /// Check if the message has an expiration time
    pub fn has_expires(&self) -> bool {
        self.headers.expires.is_some()
    }

    /// Check if the message is part of a group
    pub fn has_group(&self) -> bool {
        self.headers.group.is_some()
    }

    /// Check if the message has a parent task
    pub fn has_parent(&self) -> bool {
        self.headers.parent_id.is_some()
    }

    /// Check if the message has a root task
    pub fn has_root(&self) -> bool {
        self.headers.root_id.is_some()
    }

    /// Check if the message is persistent
    pub fn is_persistent(&self) -> bool {
        self.properties.delivery_mode == 2
    }

    /// Get the task ID
    pub fn task_id(&self) -> uuid::Uuid {
        self.headers.id
    }

    /// Get the task name
    pub fn task_name(&self) -> &str {
        &self.headers.task
    }
}

/// Task arguments (args, kwargs)
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TaskArgs {
    /// Positional arguments
    #[serde(default)]
    pub args: Vec<serde_json::Value>,

    /// Keyword arguments
    #[serde(default)]
    pub kwargs: HashMap<String, serde_json::Value>,
}

impl TaskArgs {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_args(mut self, args: Vec<serde_json::Value>) -> Self {
        self.args = args;
        self
    }

    pub fn with_kwargs(mut self, kwargs: HashMap<String, serde_json::Value>) -> Self {
        self.kwargs = kwargs;
        self
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
        assert_eq!(result.unwrap_err(), "Task name cannot be empty");
    }

    #[test]
    fn test_message_headers_validate_retries_limit() {
        let mut headers = MessageHeaders::new("test".to_string(), Uuid::new_v4());
        headers.retries = Some(1001);
        let result = headers.validate();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Retries cannot exceed 1000");
    }

    #[test]
    fn test_message_headers_validate_eta_expires() {
        let mut headers = MessageHeaders::new("test".to_string(), Uuid::new_v4());
        headers.eta = Some(Utc::now() + chrono::Duration::hours(2));
        headers.expires = Some(Utc::now() + chrono::Duration::hours(1));
        let result = headers.validate();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "ETA cannot be after expiration time");
    }

    #[test]
    fn test_message_properties_validate_delivery_mode() {
        let mut props = MessageProperties::default();
        props.delivery_mode = 3;
        let result = props.validate();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            "Delivery mode must be 1 (non-persistent) or 2 (persistent)"
        );
    }

    #[test]
    fn test_message_properties_validate_priority() {
        let mut props = MessageProperties::default();
        props.delivery_mode = 2; // Set valid delivery mode
        props.priority = Some(10);
        let result = props.validate();
        assert!(result.is_err());
        assert_eq!(result.unwrap_err(), "Priority must be between 0 and 9");
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
}
