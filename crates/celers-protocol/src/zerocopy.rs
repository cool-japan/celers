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

    /// Additional custom headers
    #[serde(flatten)]
    pub extra: HashMap<Cow<'a, str>, serde_json::Value>,
}

fn default_lang_cow() -> Cow<'static, str> {
    Cow::Borrowed("rust")
}

/// Zero-copy message properties
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
}

fn default_delivery_mode() -> u8 {
    2
}

impl Default for MessagePropertiesRef<'_> {
    fn default() -> Self {
        Self {
            correlation_id: None,
            reply_to: None,
            delivery_mode: default_delivery_mode(),
            priority: None,
        }
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

    /// Message body (zero-copy reference to raw bytes)
    #[serde(borrow)]
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
    }
}
