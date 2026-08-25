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

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::Value;
use std::collections::HashMap;
use uuid::Uuid;

/// Deserialize a list field that Python Celery emits as an explicit `null`.
///
/// `app.amqp.as_task_v2` always writes the embed dict with all four keys
/// present: `{'callbacks': None, 'errbacks': None, 'chain': None, 'chord':
/// None}`. `#[serde(default)]` only covers a *missing* key, so an explicit
/// `null` would otherwise be handed to `Vec`'s sequence deserializer and fail
/// with `invalid type: null, expected a sequence` -- i.e. every genuine
/// Celery-produced body would be rejected. Mapping `null` to an empty list is
/// exactly Python's own reading of the field.
fn deserialize_null_as_empty_vec<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: Deserializer<'de>,
    T: Deserialize<'de>,
{
    Ok(Option::<Vec<T>>::deserialize(deserializer)?.unwrap_or_default())
}

/// Serialize an empty list as `null`, mirroring Python Celery's embed dict.
///
/// Celery emits the key with a `None` value rather than omitting it, so a
/// consumer that indexes `embed['callbacks']` (instead of using `.get`) keeps
/// working against a CeleRS-produced body.
fn serialize_empty_vec_as_null<S, T>(values: &[T], serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
    T: Serialize,
{
    if values.is_empty() {
        serializer.serialize_none()
    } else {
        serializer.serialize_some(values)
    }
}

/// Callback signature for link/errback
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
    #[must_use]
    pub fn with_task_id(mut self, task_id: Uuid) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Set positional arguments
    #[must_use]
    pub fn with_args(mut self, args: Vec<Value>) -> Self {
        self.args = args;
        self
    }

    /// Add a keyword argument
    #[must_use]
    pub fn with_kwarg(mut self, key: impl Into<String>, value: Value) -> Self {
        self.kwargs.insert(key.into(), value);
        self
    }

    /// Set as immutable
    #[must_use]
    pub fn immutable(mut self) -> Self {
        self.immutable = true;
        self
    }

    /// Add an option
    #[must_use]
    pub fn with_option(mut self, key: impl Into<String>, value: Value) -> Self {
        self.options.insert(key.into(), value);
        self
    }
}

/// Embed options in the message body
///
/// This is the third element of the Celery protocol v2 body tuple
/// `[args, kwargs, embed]`. The canonical Python shape always carries the four
/// workflow keys, with `null` standing in for "unused":
///
/// ```text
/// {"callbacks": null, "errbacks": null, "chain": null, "chord": null}
/// ```
///
/// Both directions honour that shape: an explicit `null` decodes to an empty
/// list, and an empty list encodes back to `null`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct EmbedOptions {
    /// Callbacks to execute on success (link)
    #[serde(
        default,
        deserialize_with = "deserialize_null_as_empty_vec",
        serialize_with = "serialize_empty_vec_as_null"
    )]
    pub callbacks: Vec<CallbackSignature>,

    /// Callbacks to execute on error (errback)
    #[serde(
        default,
        deserialize_with = "deserialize_null_as_empty_vec",
        serialize_with = "serialize_empty_vec_as_null"
    )]
    pub errbacks: Vec<CallbackSignature>,

    /// Chain of tasks to execute after this one
    #[serde(
        default,
        deserialize_with = "deserialize_null_as_empty_vec",
        serialize_with = "serialize_empty_vec_as_null"
    )]
    pub chain: Vec<CallbackSignature>,

    /// Chord callback (executed after group completes)
    #[serde(default)]
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
    #[must_use]
    pub fn with_callback(mut self, callback: CallbackSignature) -> Self {
        self.callbacks.push(callback);
        self
    }

    /// Add an error callback (errback)
    #[must_use]
    pub fn with_errback(mut self, errback: CallbackSignature) -> Self {
        self.errbacks.push(errback);
        self
    }

    /// Add a chain task
    #[must_use]
    pub fn with_chain_task(mut self, task: CallbackSignature) -> Self {
        self.chain.push(task);
        self
    }

    /// Set the chord callback
    #[must_use]
    pub fn with_chord(mut self, chord: CallbackSignature) -> Self {
        self.chord = Some(chord);
        self
    }

    /// Set the group ID
    #[must_use]
    pub fn with_group(mut self, group: Uuid) -> Self {
        self.group = Some(group);
        self
    }

    /// Set the parent task ID
    #[must_use]
    pub fn with_parent(mut self, parent_id: Uuid) -> Self {
        self.parent_id = Some(parent_id);
        self
    }

    /// Set the root task ID
    #[must_use]
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
#[derive(Debug, Clone, Default, PartialEq)]
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
    #[must_use]
    pub fn with_args(mut self, args: Vec<Value>) -> Self {
        self.args = args;
        self
    }

    /// Add a positional argument
    #[must_use]
    pub fn with_arg(mut self, arg: Value) -> Self {
        self.args.push(arg);
        self
    }

    /// Set keyword arguments
    #[must_use]
    pub fn with_kwargs(mut self, kwargs: HashMap<String, Value>) -> Self {
        self.kwargs = kwargs;
        self
    }

    /// Add a keyword argument
    #[must_use]
    pub fn with_kwarg(mut self, key: impl Into<String>, value: Value) -> Self {
        self.kwargs.insert(key.into(), value);
        self
    }

    /// Set embed options
    #[must_use]
    pub fn with_embed(mut self, embed: EmbedOptions) -> Self {
        self.embed = embed;
        self
    }

    /// Add a success callback
    #[must_use]
    pub fn with_callback(mut self, callback: CallbackSignature) -> Self {
        self.embed.callbacks.push(callback);
        self
    }

    /// Add an error callback
    #[must_use]
    pub fn with_errback(mut self, errback: CallbackSignature) -> Self {
        self.embed.errbacks.push(errback);
        self
    }

    /// Encode to JSON bytes (Celery wire format)
    ///
    /// Produces the canonical protocol v2 body tuple `[args, kwargs, embed]`.
    /// The embed dict always carries Python Celery's four workflow keys, with
    /// `null` for the unused ones -- byte-for-byte what `app.amqp.as_task_v2`
    /// emits:
    ///
    /// ```text
    /// [[1, 2], {}, {"callbacks": null, "errbacks": null, "chain": null, "chord": null}]
    /// ```
    pub fn encode(&self) -> Result<Vec<u8>, serde_json::Error> {
        // Serializing the embed directly (rather than via an intermediate
        // `Value`) keeps the key order stable and the four canonical keys
        // present even when no workflow is attached.
        let tuple = (&self.args, &self.kwargs, &self.embed);

        serde_json::to_vec(&tuple)
    }

    /// Decode from JSON bytes
    ///
    /// Accepts every embed shape seen in the wild: the canonical Python dict
    /// with explicit `null` values, an empty dict `{}` (emitted by some
    /// third-party producers), a bare `null`, and a non-object placeholder such
    /// as the empty list a serialized Python tuple can turn into. Anything that
    /// is not an object carries no workflow, so it decodes to the default
    /// options rather than failing the whole message.
    pub fn decode(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        let tuple: (Vec<Value>, HashMap<String, Value>, Value) = serde_json::from_slice(bytes)?;

        let embed: EmbedOptions = match tuple.2 {
            object @ Value::Object(_) => serde_json::from_value(object)?,
            // `null`, `[]` and friends carry no workflow at all.
            _ => EmbedOptions::default(),
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

    /// Golden fixture: the body Python Celery actually puts on the wire.
    ///
    /// `celery.app.amqp.AMQP.as_task_v2` builds the body as
    /// `(args, kwargs, {'callbacks': callbacks, 'errbacks': errbacks,
    /// 'chain': chain, 'chord': chord})` -- the four keys are *always* present
    /// and hold `None` when the task carries no workflow. Regression: decoding
    /// used to fail with `invalid type: null, expected a sequence`, so every
    /// genuine Celery-produced task body was rejected.
    #[test]
    fn test_decode_python_celery_embed_with_explicit_nulls() {
        // Captured shape of `json.dumps(as_task_v2(...).body)` for
        // `add.delay(1, 2)` with no callbacks/errbacks/chain/chord.
        const PYTHON_BODY: &str =
            r#"[[1,2],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}]"#;

        let body = EmbeddedBody::from_json_string(PYTHON_BODY)
            .expect("a genuine Celery protocol v2 body must decode");

        assert_eq!(body.args, vec![json!(1), json!(2)]);
        assert!(body.kwargs.is_empty());
        assert!(body.embed.callbacks.is_empty());
        assert!(body.embed.errbacks.is_empty());
        assert!(body.embed.chain.is_empty());
        assert!(body.embed.chord.is_none());
        assert!(!body.embed.has_workflow());
    }

    /// The same null-tolerance must hold when only *some* of the keys are null,
    /// which is what a `link=`/`chord` task looks like on the wire.
    #[test]
    fn test_decode_python_celery_embed_with_partial_nulls() {
        // `add.apply_async((1, 2), link=notify.s())` -- `callbacks` is a list,
        // the other three stay `None`.
        const PYTHON_BODY: &str = r#"[[1,2],{"debug":true},{"callbacks":[{"task":"tasks.notify","args":[],"kwargs":{},"options":{},"immutable":false,"subtask_type":null}],"errbacks":null,"chain":null,"chord":null}]"#;

        let body = EmbeddedBody::from_json_string(PYTHON_BODY)
            .expect("a Celery body with a link callback must decode");

        assert_eq!(body.kwargs.get("debug"), Some(&json!(true)));
        assert_eq!(body.embed.callbacks.len(), 1);
        assert_eq!(body.embed.callbacks[0].task, "tasks.notify");
        assert!(body.embed.errbacks.is_empty());
        assert!(body.embed.has_workflow());
    }

    /// Encoding mirrors the canonical Python shape: the four workflow keys are
    /// always emitted, `null` when unused, so a Python consumer that indexes
    /// `embed['callbacks']` (rather than using `.get`) keeps working.
    #[test]
    fn test_encode_emits_canonical_python_embed_dict() {
        let body = EmbeddedBody::new().with_args(vec![json!(1), json!(2)]);

        let encoded = body.to_json_string().expect("encode must succeed");
        assert_eq!(
            encoded,
            r#"[[1,2],{},{"callbacks":null,"errbacks":null,"chain":null,"chord":null}]"#
        );

        // And it round-trips through our own decoder.
        let decoded = EmbeddedBody::from_json_string(&encoded).expect("round-trip must decode");
        assert_eq!(decoded.args, body.args);
        assert!(!decoded.embed.has_workflow());
    }

    /// Legacy/third-party producers that emit an empty embed dict -- or omit it
    /// entirely as `null` -- must keep decoding.
    #[test]
    fn test_decode_tolerates_empty_and_null_embed() {
        let body = EmbeddedBody::from_json_string(r#"[[1],{"k":"v"},{}]"#)
            .expect("empty embed dict must decode");
        assert_eq!(body.args, vec![json!(1)]);
        assert!(!body.embed.has_workflow());

        let body =
            EmbeddedBody::from_json_string(r#"[[1],{},null]"#).expect("null embed must decode");
        assert_eq!(body.args, vec![json!(1)]);
        assert!(!body.embed.has_workflow());

        // A serialized Python tuple can arrive as an empty list; it carries no
        // workflow, so it must not fail the whole message.
        let body = EmbeddedBody::from_json_string(r#"[[1],{},[]]"#)
            .expect("non-object embed placeholder must decode");
        assert_eq!(body.args, vec![json!(1)]);
        assert!(!body.embed.has_workflow());
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
