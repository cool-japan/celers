//! Celery embedded body format
//!
//! This module provides support for the Celery Protocol v2 embedded body format.
//! In Protocol v2, the message body is a tuple of `[args, kwargs, embed]` where:
//!
//! - `args` - Positional arguments (list)
//! - `kwargs` - Keyword arguments (dict)
//! - `embed` - Embedded metadata (callbacks, errbacks, chain, chord, etc.)
//!
//! # Example
//!
//! ```
//! use celers_protocol::embed::{EmbeddedBody, EmbedOptions};
//! use serde_json::json;
//!
//! let body = EmbeddedBody::new()
//!     .with_args(vec![json!(1), json!(2)])
//!     .with_kwarg("debug", json!(true));
//!
//! let encoded = body.encode().unwrap();
//! let decoded = EmbeddedBody::decode(&encoded).unwrap();
//! assert_eq!(decoded.args, vec![json!(1), json!(2)]);
//! ```

use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

/// Callback signature for link/errback
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CallbackSignature {
    /// Task name
    pub task: String,

    /// Task ID (optional, will be generated if not provided)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_id: Option<Uuid>,

    /// Positional arguments
    #[serde(default)]
    pub args: Vec<Value>,

    /// Keyword arguments
    #[serde(default)]
    pub kwargs: HashMap<String, Value>,

    /// Task options
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub options: HashMap<String, Value>,

    /// Immutable flag (don't append parent result)
    #[serde(default)]
    pub immutable: bool,

    /// Subtask type (for internal use)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subtask_type: Option<String>,
}

impl CallbackSignature {
    /// Create a new callback signature
    pub fn new(task: impl Into<String>) -> Self {
        Self {
            task: task.into(),
            task_id: None,
            args: Vec::new(),
            kwargs: HashMap::new(),
            options: HashMap::new(),
            immutable: false,
            subtask_type: None,
        }
    }

    /// Set task ID
    pub fn with_task_id(mut self, task_id: Uuid) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Set positional arguments
    pub fn with_args(mut self, args: Vec<Value>) -> Self {
        self.args = args;
        self
    }

    /// Add a keyword argument
    pub fn with_kwarg(mut self, key: impl Into<String>, value: Value) -> Self {
        self.kwargs.insert(key.into(), value);
        self
    }

    /// Set as immutable
    pub fn immutable(mut self) -> Self {
        self.immutable = true;
        self
    }

    /// Add an option
    pub fn with_option(mut self, key: impl Into<String>, value: Value) -> Self {
        self.options.insert(key.into(), value);
        self
    }
}

/// Embed options in the message body
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct EmbedOptions {
    /// Callbacks to execute on success (link)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub callbacks: Vec<CallbackSignature>,

    /// Callbacks to execute on error (errback)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub errbacks: Vec<CallbackSignature>,

    /// Chain of tasks to execute after this one
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub chain: Vec<CallbackSignature>,

    /// Chord callback (executed after group completes)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub chord: Option<CallbackSignature>,

    /// Group ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group: Option<Uuid>,

    /// Parent task ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_id: Option<Uuid>,

    /// Root task ID
    #[serde(skip_serializing_if = "Option::is_none")]
    pub root_id: Option<Uuid>,

    /// Additional custom embed fields
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

impl EmbedOptions {
    /// Create new empty embed options
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a success callback (link)
    pub fn with_callback(mut self, callback: CallbackSignature) -> Self {
        self.callbacks.push(callback);
        self
    }

    /// Add an error callback (errback)
    pub fn with_errback(mut self, errback: CallbackSignature) -> Self {
        self.errbacks.push(errback);
        self
    }

    /// Add a chain task
    pub fn with_chain_task(mut self, task: CallbackSignature) -> Self {
        self.chain.push(task);
        self
    }

    /// Set the chord callback
    pub fn with_chord(mut self, chord: CallbackSignature) -> Self {
        self.chord = Some(chord);
        self
    }

    /// Set the group ID
    pub fn with_group(mut self, group: Uuid) -> Self {
        self.group = Some(group);
        self
    }

    /// Set the parent task ID
    pub fn with_parent(mut self, parent_id: Uuid) -> Self {
        self.parent_id = Some(parent_id);
        self
    }

    /// Set the root task ID
    pub fn with_root(mut self, root_id: Uuid) -> Self {
        self.root_id = Some(root_id);
        self
    }

    /// Check if there are any callbacks
    pub fn has_callbacks(&self) -> bool {
        !self.callbacks.is_empty()
    }

    /// Check if there are any errbacks
    pub fn has_errbacks(&self) -> bool {
        !self.errbacks.is_empty()
    }

    /// Check if there is a chain
    pub fn has_chain(&self) -> bool {
        !self.chain.is_empty()
    }

    /// Check if there is a chord
    pub fn has_chord(&self) -> bool {
        self.chord.is_some()
    }

    /// Check if this has any workflow elements
    pub fn has_workflow(&self) -> bool {
        self.has_callbacks() || self.has_errbacks() || self.has_chain() || self.has_chord()
    }
}

/// Complete embedded body format [args, kwargs, embed]
#[derive(Debug, Clone, Default)]
pub struct EmbeddedBody {
    /// Positional arguments
    pub args: Vec<Value>,

    /// Keyword arguments
    pub kwargs: HashMap<String, Value>,

    /// Embed options
    pub embed: EmbedOptions,
}

impl EmbeddedBody {
    /// Create a new embedded body
    pub fn new() -> Self {
        Self::default()
    }

    /// Set positional arguments
    pub fn with_args(mut self, args: Vec<Value>) -> Self {
        self.args = args;
        self
    }

    /// Add a positional argument
    pub fn with_arg(mut self, arg: Value) -> Self {
        self.args.push(arg);
        self
    }

    /// Set keyword arguments
    pub fn with_kwargs(mut self, kwargs: HashMap<String, Value>) -> Self {
        self.kwargs = kwargs;
        self
    }

    /// Add a keyword argument
    pub fn with_kwarg(mut self, key: impl Into<String>, value: Value) -> Self {
        self.kwargs.insert(key.into(), value);
        self
    }

    /// Set embed options
    pub fn with_embed(mut self, embed: EmbedOptions) -> Self {
        self.embed = embed;
        self
    }

    /// Add a success callback
    pub fn with_callback(mut self, callback: CallbackSignature) -> Self {
        self.embed.callbacks.push(callback);
        self
    }

    /// Add an error callback
    pub fn with_errback(mut self, errback: CallbackSignature) -> Self {
        self.embed.errbacks.push(errback);
        self
    }

    /// Encode to JSON bytes (Celery wire format)
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        // Convert embed to Value (empty object if no workflow)
        let embed_value = if self.embed.has_workflow()
            || self.embed.group.is_some()
            || self.embed.parent_id.is_some()
            || self.embed.root_id.is_some()
        {
            serde_json::to_value(&self.embed)?
        } else {
            Value::Object(serde_json::Map::new())
        };

        let tuple = (&self.args, &self.kwargs, embed_value);

        serde_json::to_vec(&tuple)
    }

    /// Decode from JSON bytes
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let tuple: (Vec<Value>, HashMap<String, Value>, Value) = serde_json::from_slice(bytes)?;

        let embed: EmbedOptions = if tuple.2.is_object() && !tuple.2.as_object().unwrap().is_empty()
        {
            serde_json::from_value(tuple.2)?
        } else {
            EmbedOptions::default()
        };

        Ok(Self {
            args: tuple.0,
            kwargs: tuple.1,
            embed,
        })
    }

    /// Encode to JSON string
    pub fn to_json_string(&self) -> Result<String, serde_json::Error> {
        let bytes = self.encode()?;
        Ok(String::from_utf8_lossy(&bytes).to_string())
    }

    /// Decode from JSON string
    pub fn from_json_string(s: &str) -> Result<Self, serde_json::Error> {
        Self::decode(s.as_bytes())
    }
}

/// Serialize arguments for display/logging
pub fn format_args(args: &[Value], kwargs: &HashMap<String, Value>) -> String {
    let args_str: Vec<String> = args.iter().map(|v| v.to_string()).collect();
    let kwargs_str: Vec<String> = kwargs.iter().map(|(k, v)| format!("{}={}", k, v)).collect();

    let mut parts = args_str;
    parts.extend(kwargs_str);
    parts.join(", ")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_callback_signature_creation() {
        let callback = CallbackSignature::new("tasks.process")
            .with_args(vec![json!(1), json!(2)])
            .with_kwarg("debug", json!(true))
            .immutable();

        assert_eq!(callback.task, "tasks.process");
        assert_eq!(callback.args, vec![json!(1), json!(2)]);
        assert_eq!(callback.kwargs.get("debug"), Some(&json!(true)));
        assert!(callback.immutable);
    }

    #[test]
    fn test_callback_signature_with_task_id() {
        let task_id = Uuid::new_v4();
        let callback = CallbackSignature::new("tasks.callback").with_task_id(task_id);

        assert_eq!(callback.task_id, Some(task_id));
    }

    #[test]
    fn test_embed_options_callbacks() {
        let callback = CallbackSignature::new("tasks.success_handler");
        let errback = CallbackSignature::new("tasks.error_handler");

        let embed = EmbedOptions::new()
            .with_callback(callback)
            .with_errback(errback);

        assert!(embed.has_callbacks());
        assert!(embed.has_errbacks());
        assert!(embed.has_workflow());
    }

    #[test]
    fn test_embed_options_chain() {
        let task1 = CallbackSignature::new("tasks.step1");
        let task2 = CallbackSignature::new("tasks.step2");

        let embed = EmbedOptions::new()
            .with_chain_task(task1)
            .with_chain_task(task2);

        assert!(embed.has_chain());
        assert_eq!(embed.chain.len(), 2);
    }

    #[test]
    fn test_embed_options_chord() {
        let chord_callback = CallbackSignature::new("tasks.chord_callback");
        let group_id = Uuid::new_v4();

        let embed = EmbedOptions::new()
            .with_chord(chord_callback)
            .with_group(group_id);

        assert!(embed.has_chord());
        assert_eq!(embed.group, Some(group_id));
    }

    #[test]
    fn test_embedded_body_basic() {
        let body = EmbeddedBody::new()
            .with_args(vec![json!(1), json!(2)])
            .with_kwarg("key", json!("value"));

        assert_eq!(body.args, vec![json!(1), json!(2)]);
        assert_eq!(body.kwargs.get("key"), Some(&json!("value")));
    }

    #[test]
    fn test_embedded_body_encode_decode() {
        let body = EmbeddedBody::new()
            .with_args(vec![json!(10), json!(20)])
            .with_kwarg("multiplier", json!(2));

        let encoded = body.encode().unwrap();
        let decoded = EmbeddedBody::decode(&encoded).unwrap();

        assert_eq!(decoded.args, body.args);
        assert_eq!(decoded.kwargs, body.kwargs);
    }

    #[test]
    fn test_embedded_body_with_callbacks() {
        let callback = CallbackSignature::new("tasks.on_success");
        let body = EmbeddedBody::new()
            .with_args(vec![json!("test")])
            .with_callback(callback);

        let encoded = body.encode().unwrap();
        let decoded = EmbeddedBody::decode(&encoded).unwrap();

        assert!(decoded.embed.has_callbacks());
        assert_eq!(decoded.embed.callbacks[0].task, "tasks.on_success");
    }

    #[test]
    fn test_embedded_body_wire_format() {
        let body = EmbeddedBody::new()
            .with_args(vec![json!(1), json!(2)])
            .with_kwarg("x", json!(3));

        let json_str = body.to_json_string().unwrap();

        // Should be [args, kwargs, embed] format
        let parsed: Value = serde_json::from_str(&json_str).unwrap();
        assert!(parsed.is_array());

        let arr = parsed.as_array().unwrap();
        assert_eq!(arr.len(), 3);
        assert!(arr[0].is_array()); // args
        assert!(arr[1].is_object()); // kwargs
        assert!(arr[2].is_object()); // embed
    }

    #[test]
    fn test_embedded_body_from_json_string() {
        let json_str = r#"[[1, 2], {"key": "value"}, {}]"#;
        let body = EmbeddedBody::from_json_string(json_str).unwrap();

        assert_eq!(body.args, vec![json!(1), json!(2)]);
        assert_eq!(body.kwargs.get("key"), Some(&json!("value")));
    }

    #[test]
    fn test_embedded_body_python_compatibility() {
        // This is the exact format Python Celery uses
        let python_body = r#"[[4, 5], {"debug": true}, {"callbacks": [{"task": "tasks.callback", "args": [], "kwargs": {}, "options": {}, "immutable": false}]}]"#;

        let body = EmbeddedBody::from_json_string(python_body).unwrap();

        assert_eq!(body.args, vec![json!(4), json!(5)]);
        assert_eq!(body.kwargs.get("debug"), Some(&json!(true)));
        assert!(body.embed.has_callbacks());
        assert_eq!(body.embed.callbacks[0].task, "tasks.callback");
    }

    #[test]
    fn test_format_args() {
        let args = vec![json!(1), json!("hello")];
        let mut kwargs = HashMap::new();
        kwargs.insert("x".to_string(), json!(10));
        kwargs.insert("y".to_string(), json!(20));

        let formatted = format_args(&args, &kwargs);

        assert!(formatted.contains("1"));
        assert!(formatted.contains("\"hello\""));
        assert!(formatted.contains("x=10") || formatted.contains("y=20"));
    }

    #[test]
    fn test_embed_options_workflow_ids() {
        let parent_id = Uuid::new_v4();
        let root_id = Uuid::new_v4();

        let embed = EmbedOptions::new()
            .with_parent(parent_id)
            .with_root(root_id);

        assert_eq!(embed.parent_id, Some(parent_id));
        assert_eq!(embed.root_id, Some(root_id));
    }

    #[test]
    fn test_callback_signature_serialization() {
        let callback = CallbackSignature::new("tasks.test")
            .with_args(vec![json!(1)])
            .with_kwarg("key", json!("val"))
            .with_option("queue", json!("high-priority"));

        let json = serde_json::to_string(&callback).unwrap();
        let decoded: CallbackSignature = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.task, "tasks.test");
        assert_eq!(decoded.args, vec![json!(1)]);
        assert_eq!(decoded.options.get("queue"), Some(&json!("high-priority")));
    }
}
