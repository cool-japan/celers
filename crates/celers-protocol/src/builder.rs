//! Fluent message builder API
//!
//! This module provides a builder pattern for constructing Celery protocol
//! messages with a clean, fluent API.
//!
//! # Example
//!
//! ```
//! use celers_protocol::builder::MessageBuilder;
//! use serde_json::json;
//!
//! let message = MessageBuilder::new("tasks.add")
//!     .args(vec![json!(1), json!(2)])
//!     .priority(5)
//!     .queue("high-priority")
//!     .build()
//!     .unwrap();
//!
//! assert_eq!(message.task_name(), "tasks.add");
//! ```

use crate::embed::{CallbackSignature, EmbedOptions, EmbeddedBody};
use crate::{ContentEncoding, ContentType, Message, MessageHeaders, MessageProperties};
use chrono::{DateTime, Duration, Utc};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

/// Error type for message building
#[derive(Debug, Clone)]
pub enum BuilderError {
    /// Task name is required
    MissingTaskName,
    /// Serialization failed
    SerializationError(String),
    /// Validation failed
    ValidationError(String),
}

impl std::fmt::Display for BuilderError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BuilderError::MissingTaskName => write!(f, "Task name is required"),
            BuilderError::SerializationError(msg) => write!(f, "Serialization error: {}", msg),
            BuilderError::ValidationError(msg) => write!(f, "Validation error: {}", msg),
        }
    }
}

impl std::error::Error for BuilderError {}

/// Result type for message building
pub type BuilderResult<T> = Result<T, BuilderError>;

/// Fluent builder for creating Celery messages
#[derive(Debug, Clone)]
pub struct MessageBuilder {
    /// Task name
    task: String,
    /// Task ID (auto-generated if not set)
    task_id: Option<Uuid>,
    /// Positional arguments
    args: Vec<Value>,
    /// Keyword arguments
    kwargs: HashMap<String, Value>,
    /// Task priority (0-9)
    priority: Option<u8>,
    /// Queue name
    queue: Option<String>,
    /// Routing key
    routing_key: Option<String>,
    /// ETA (scheduled execution time)
    eta: Option<DateTime<Utc>>,
    /// Countdown (delay in seconds)
    countdown: Option<i64>,
    /// Expiration time
    expires: Option<DateTime<Utc>>,
    /// Maximum retries
    max_retries: Option<u32>,
    /// Current retry count
    retries: Option<u32>,
    /// Parent task ID
    parent_id: Option<Uuid>,
    /// Root task ID
    root_id: Option<Uuid>,
    /// Group ID
    group_id: Option<Uuid>,
    /// Callbacks (on success)
    callbacks: Vec<CallbackSignature>,
    /// Errbacks (on failure)
    errbacks: Vec<CallbackSignature>,
    /// Chain of tasks
    chain: Vec<CallbackSignature>,
    /// Chord callback
    chord: Option<CallbackSignature>,
    /// Content type
    content_type: ContentType,
    /// Delivery mode (persistent by default)
    persistent: bool,
    /// Reply-to queue
    reply_to: Option<String>,
    /// Extra headers
    extra_headers: HashMap<String, Value>,
}

impl MessageBuilder {
    /// Create a new message builder with the given task name
    pub fn new(task: impl Into<String>) -> Self {
        Self {
            task: task.into(),
            task_id: None,
            args: Vec::new(),
            kwargs: HashMap::new(),
            priority: None,
            queue: None,
            routing_key: None,
            eta: None,
            countdown: None,
            expires: None,
            max_retries: None,
            retries: None,
            parent_id: None,
            root_id: None,
            group_id: None,
            callbacks: Vec::new(),
            errbacks: Vec::new(),
            chain: Vec::new(),
            chord: None,
            content_type: ContentType::Json,
            persistent: true,
            reply_to: None,
            extra_headers: HashMap::new(),
        }
    }

    /// Set the task ID
    pub fn id(mut self, id: Uuid) -> Self {
        self.task_id = Some(id);
        self
    }

    /// Set positional arguments
    pub fn args(mut self, args: Vec<Value>) -> Self {
        self.args = args;
        self
    }

    /// Add a positional argument
    pub fn arg(mut self, arg: Value) -> Self {
        self.args.push(arg);
        self
    }

    /// Set keyword arguments
    pub fn kwargs(mut self, kwargs: HashMap<String, Value>) -> Self {
        self.kwargs = kwargs;
        self
    }

    /// Add a keyword argument
    pub fn kwarg(mut self, key: impl Into<String>, value: Value) -> Self {
        self.kwargs.insert(key.into(), value);
        self
    }

    /// Set task priority (0-9, higher = more urgent)
    pub fn priority(mut self, priority: u8) -> Self {
        self.priority = Some(priority.min(9));
        self
    }

    /// Set the queue name
    pub fn queue(mut self, queue: impl Into<String>) -> Self {
        self.queue = Some(queue.into());
        self
    }

    /// Set the routing key
    pub fn routing_key(mut self, key: impl Into<String>) -> Self {
        self.routing_key = Some(key.into());
        self
    }

    /// Set ETA (scheduled execution time)
    pub fn eta(mut self, eta: DateTime<Utc>) -> Self {
        self.eta = Some(eta);
        self.countdown = None; // ETA takes precedence
        self
    }

    /// Set countdown (delay in seconds)
    pub fn countdown(mut self, seconds: i64) -> Self {
        self.countdown = Some(seconds);
        self.eta = None; // Countdown takes precedence
        self
    }

    /// Set expiration time
    pub fn expires(mut self, expires: DateTime<Utc>) -> Self {
        self.expires = Some(expires);
        self
    }

    /// Set expiration as duration from now
    pub fn expires_in(mut self, duration: Duration) -> Self {
        self.expires = Some(Utc::now() + duration);
        self
    }

    /// Set maximum retries
    pub fn max_retries(mut self, max: u32) -> Self {
        self.max_retries = Some(max);
        self
    }

    /// Set current retry count
    pub fn retries(mut self, count: u32) -> Self {
        self.retries = Some(count);
        self
    }

    /// Set parent task ID
    pub fn parent(mut self, parent_id: Uuid) -> Self {
        self.parent_id = Some(parent_id);
        self
    }

    /// Set root task ID
    pub fn root(mut self, root_id: Uuid) -> Self {
        self.root_id = Some(root_id);
        self
    }

    /// Set group ID
    pub fn group(mut self, group_id: Uuid) -> Self {
        self.group_id = Some(group_id);
        self
    }

    /// Add a success callback (link)
    pub fn link(mut self, task: impl Into<String>) -> Self {
        self.callbacks.push(CallbackSignature::new(task));
        self
    }

    /// Add a success callback with full signature
    pub fn link_signature(mut self, callback: CallbackSignature) -> Self {
        self.callbacks.push(callback);
        self
    }

    /// Add an error callback (errback)
    pub fn link_error(mut self, task: impl Into<String>) -> Self {
        self.errbacks.push(CallbackSignature::new(task));
        self
    }

    /// Add an error callback with full signature
    pub fn link_error_signature(mut self, errback: CallbackSignature) -> Self {
        self.errbacks.push(errback);
        self
    }

    /// Add a chain task
    pub fn chain_task(mut self, task: impl Into<String>) -> Self {
        self.chain.push(CallbackSignature::new(task));
        self
    }

    /// Set chord callback
    pub fn chord(mut self, callback: impl Into<String>) -> Self {
        self.chord = Some(CallbackSignature::new(callback));
        self
    }

    /// Set content type
    pub fn content_type(mut self, ct: ContentType) -> Self {
        self.content_type = ct;
        self
    }

    /// Set message persistence
    pub fn persistent(mut self, persistent: bool) -> Self {
        self.persistent = persistent;
        self
    }

    /// Set reply-to queue
    pub fn reply_to(mut self, queue: impl Into<String>) -> Self {
        self.reply_to = Some(queue.into());
        self
    }

    /// Add extra header
    pub fn header(mut self, key: impl Into<String>, value: Value) -> Self {
        self.extra_headers.insert(key.into(), value);
        self
    }

    /// Build the message
    pub fn build(self) -> BuilderResult<Message> {
        // Generate task ID if not set
        let task_id = self.task_id.unwrap_or_else(Uuid::new_v4);

        // Calculate ETA from countdown if set
        let eta = match (self.eta, self.countdown) {
            (Some(eta), _) => Some(eta),
            (None, Some(seconds)) => Some(Utc::now() + Duration::seconds(seconds)),
            _ => None,
        };

        // Build embed options
        let mut embed = EmbedOptions::new();
        for cb in self.callbacks {
            embed = embed.with_callback(cb);
        }
        for eb in self.errbacks {
            embed = embed.with_errback(eb);
        }
        for chain_task in self.chain {
            embed = embed.with_chain_task(chain_task);
        }
        if let Some(chord) = self.chord {
            embed = embed.with_chord(chord);
        }
        if let Some(group_id) = self.group_id {
            embed = embed.with_group(group_id);
        }
        if let Some(parent_id) = self.parent_id {
            embed = embed.with_parent(parent_id);
        }
        if let Some(root_id) = self.root_id {
            embed = embed.with_root(root_id);
        }

        // Build embedded body
        let embedded_body = EmbeddedBody::new()
            .with_args(self.args)
            .with_kwargs(self.kwargs)
            .with_embed(embed);

        // Serialize body
        let body = embedded_body
            .encode()
            .map_err(|e| BuilderError::SerializationError(e.to_string()))?;

        // Build headers
        let mut headers = MessageHeaders::new(self.task.clone(), task_id);
        headers.eta = eta;
        headers.expires = self.expires;
        headers.retries = self.retries;
        headers.parent_id = self.parent_id;
        headers.root_id = self.root_id;
        headers.group = self.group_id;

        // Add extra headers
        for (key, value) in self.extra_headers {
            headers.extra.insert(key, value);
        }

        // Build properties
        let properties = MessageProperties {
            priority: self.priority,
            delivery_mode: if self.persistent { 2 } else { 1 },
            correlation_id: Some(task_id.to_string()),
            reply_to: self.reply_to,
        };

        // Build message
        let message = Message {
            headers,
            properties,
            body,
            content_type: self.content_type.as_str().to_string(),
            content_encoding: ContentEncoding::Utf8.as_str().to_string(),
        };

        Ok(message)
    }

    /// Build and validate the message
    pub fn build_validated(self) -> BuilderResult<Message> {
        let message = self.build()?;
        message.validate().map_err(BuilderError::ValidationError)?;
        Ok(message)
    }
}

/// Create a simple task message
pub fn task(name: impl Into<String>) -> MessageBuilder {
    MessageBuilder::new(name)
}

/// Create a task message with args
pub fn task_with_args(name: impl Into<String>, args: Vec<Value>) -> MessageBuilder {
    MessageBuilder::new(name).args(args)
}

/// Create a delayed task message
pub fn delayed_task(name: impl Into<String>, countdown_seconds: i64) -> MessageBuilder {
    MessageBuilder::new(name).countdown(countdown_seconds)
}

/// Create a scheduled task message
pub fn scheduled_task(name: impl Into<String>, eta: DateTime<Utc>) -> MessageBuilder {
    MessageBuilder::new(name).eta(eta)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_basic_message_builder() {
        let message = MessageBuilder::new("tasks.add")
            .args(vec![json!(1), json!(2)])
            .build()
            .unwrap();

        assert_eq!(message.task_name(), "tasks.add");
        assert!(!message.body.is_empty());
    }

    #[test]
    fn test_message_builder_with_id() {
        let id = Uuid::new_v4();
        let message = MessageBuilder::new("tasks.test").id(id).build().unwrap();

        assert_eq!(message.task_id(), id);
    }

    #[test]
    fn test_message_builder_with_priority() {
        let message = MessageBuilder::new("tasks.test")
            .priority(9)
            .build()
            .unwrap();

        assert_eq!(message.properties.priority, Some(9));
    }

    #[test]
    fn test_message_builder_with_priority_capped() {
        let message = MessageBuilder::new("tasks.test")
            .priority(100)
            .build()
            .unwrap();

        assert_eq!(message.properties.priority, Some(9));
    }

    #[test]
    fn test_message_builder_with_countdown() {
        let message = MessageBuilder::new("tasks.test")
            .countdown(60)
            .build()
            .unwrap();

        assert!(message.has_eta());
    }

    #[test]
    fn test_message_builder_with_eta() {
        let eta = Utc::now() + Duration::hours(1);
        let message = MessageBuilder::new("tasks.test").eta(eta).build().unwrap();

        assert!(message.has_eta());
        assert_eq!(message.headers.eta, Some(eta));
    }

    #[test]
    fn test_message_builder_with_expires() {
        let expires = Utc::now() + Duration::days(1);
        let message = MessageBuilder::new("tasks.test")
            .expires(expires)
            .build()
            .unwrap();

        assert!(message.has_expires());
    }

    #[test]
    fn test_message_builder_with_expires_in() {
        let message = MessageBuilder::new("tasks.test")
            .expires_in(Duration::hours(2))
            .build()
            .unwrap();

        assert!(message.has_expires());
    }

    #[test]
    fn test_message_builder_with_kwargs() {
        let mut kwargs = HashMap::new();
        kwargs.insert("x".to_string(), json!(10));

        let message = MessageBuilder::new("tasks.test")
            .kwargs(kwargs)
            .kwarg("y", json!(20))
            .build()
            .unwrap();

        assert!(!message.body.is_empty());
    }

    #[test]
    fn test_message_builder_with_link() {
        let message = MessageBuilder::new("tasks.first")
            .link("tasks.second")
            .link_error("tasks.on_error")
            .build()
            .unwrap();

        assert!(!message.body.is_empty());
    }

    #[test]
    fn test_message_builder_with_chain() {
        let message = MessageBuilder::new("tasks.step1")
            .chain_task("tasks.step2")
            .chain_task("tasks.step3")
            .build()
            .unwrap();

        assert!(!message.body.is_empty());
    }

    #[test]
    fn test_message_builder_with_workflow_ids() {
        let parent_id = Uuid::new_v4();
        let root_id = Uuid::new_v4();
        let group_id = Uuid::new_v4();

        let message = MessageBuilder::new("tasks.test")
            .parent(parent_id)
            .root(root_id)
            .group(group_id)
            .build()
            .unwrap();

        assert_eq!(message.headers.parent_id, Some(parent_id));
        assert_eq!(message.headers.root_id, Some(root_id));
        assert_eq!(message.headers.group, Some(group_id));
    }

    #[test]
    fn test_message_builder_non_persistent() {
        let message = MessageBuilder::new("tasks.test")
            .persistent(false)
            .build()
            .unwrap();

        assert_eq!(message.properties.delivery_mode, 1);
    }

    #[test]
    fn test_message_builder_with_reply_to() {
        let message = MessageBuilder::new("tasks.test")
            .reply_to("results-queue")
            .build()
            .unwrap();

        assert_eq!(
            message.properties.reply_to,
            Some("results-queue".to_string())
        );
    }

    #[test]
    fn test_message_builder_with_extra_header() {
        let message = MessageBuilder::new("tasks.test")
            .header("custom", json!("value"))
            .build()
            .unwrap();

        assert_eq!(message.headers.extra.get("custom"), Some(&json!("value")));
    }

    #[test]
    fn test_task_helper() {
        let message = task("tasks.add")
            .arg(json!(1))
            .arg(json!(2))
            .build()
            .unwrap();
        assert_eq!(message.task_name(), "tasks.add");
    }

    #[test]
    fn test_task_with_args_helper() {
        let message = task_with_args("tasks.add", vec![json!(1), json!(2)])
            .build()
            .unwrap();
        assert_eq!(message.task_name(), "tasks.add");
    }

    #[test]
    fn test_delayed_task_helper() {
        let message = delayed_task("tasks.later", 300).build().unwrap();
        assert!(message.has_eta());
    }

    #[test]
    fn test_scheduled_task_helper() {
        let eta = Utc::now() + Duration::hours(1);
        let message = scheduled_task("tasks.scheduled", eta).build().unwrap();
        assert!(message.has_eta());
    }

    #[test]
    fn test_build_validated() {
        let message = MessageBuilder::new("tasks.test")
            .args(vec![json!(1)])
            .build_validated()
            .unwrap();

        assert_eq!(message.task_name(), "tasks.test");
    }

    #[test]
    fn test_builder_error_display() {
        let err = BuilderError::MissingTaskName;
        assert_eq!(err.to_string(), "Task name is required");

        let err = BuilderError::SerializationError("test".to_string());
        assert!(err.to_string().contains("test"));

        let err = BuilderError::ValidationError("invalid".to_string());
        assert!(err.to_string().contains("invalid"));
    }
}
