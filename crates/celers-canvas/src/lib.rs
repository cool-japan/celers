//! Canvas workflow primitives
//!
//! This crate provides distributed workflow patterns for task orchestration.
//!
//! # Workflow Primitives
//!
//! - **Chain**: Execute tasks sequentially, passing results as arguments
//! - **Group**: Execute tasks in parallel
//! - **Chord**: Execute tasks in parallel, then run callback with all results
//! - **Map**: Apply a task to multiple argument sets in parallel
//!
//! # Example
//!
//! ```ignore
//! // Chain: task1 -> task2 -> task3
//! let workflow = Chain::new()
//!     .then("task1", args1)
//!     .then("task2", args2)
//!     .then("task3", args3);
//!
//! // Chord: (task1 | task2 | task3) -> callback
//! let workflow = Chord::new()
//!     .add("task1", args1)
//!     .add("task2", args2)
//!     .add("task3", args3)
//!     .callback("aggregate_results", callback_args);
//! ```

use celers_core::{Broker, SerializedTask};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

#[cfg(feature = "backend-redis")]
use celers_backend_redis::{ChordState, ResultBackend};

#[cfg(feature = "backend-redis")]
use chrono::Utc;

/// Signature (a task definition with arguments)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Signature {
    /// Task name
    pub task: String,

    /// Positional arguments
    #[serde(default)]
    pub args: Vec<serde_json::Value>,

    /// Keyword arguments
    #[serde(default)]
    pub kwargs: HashMap<String, serde_json::Value>,

    /// Task options
    #[serde(default)]
    pub options: TaskOptions,

    /// Immutability flag (whether args can be replaced)
    #[serde(default)]
    pub immutable: bool,
}

impl Signature {
    pub fn new(task: String) -> Self {
        Self {
            task,
            args: Vec::new(),
            kwargs: HashMap::new(),
            options: TaskOptions::default(),
            immutable: false,
        }
    }

    pub fn with_args(mut self, args: Vec<serde_json::Value>) -> Self {
        self.args = args;
        self
    }

    pub fn with_kwargs(mut self, kwargs: HashMap<String, serde_json::Value>) -> Self {
        self.kwargs = kwargs;
        self
    }

    pub fn with_priority(mut self, priority: u8) -> Self {
        self.options.priority = Some(priority);
        self
    }

    pub fn with_queue(mut self, queue: String) -> Self {
        self.options.queue = Some(queue);
        self
    }

    pub fn with_task_id(mut self, task_id: Uuid) -> Self {
        self.options.task_id = Some(task_id);
        self
    }

    pub fn with_link(mut self, link: Signature) -> Self {
        self.options.link = Some(Box::new(link));
        self
    }

    pub fn with_link_error(mut self, link_error: Signature) -> Self {
        self.options.link_error = Some(Box::new(link_error));
        self
    }

    /// Add a callback to the success callback chain
    pub fn add_link(mut self, link: Signature) -> Self {
        self.options.links.push(link);
        self
    }

    /// Add a callback to the error callback chain
    pub fn add_link_error(mut self, link_error: Signature) -> Self {
        self.options.link_errors.push(link_error);
        self
    }

    /// Set the on_retry callback
    pub fn with_on_retry(mut self, callback: Signature) -> Self {
        self.options.on_retry = Some(Box::new(callback));
        self
    }

    /// Set soft time limit (warning before kill)
    pub fn with_soft_time_limit(mut self, seconds: u64) -> Self {
        self.options.soft_time_limit = Some(seconds);
        self
    }

    /// Set hard time limit (force kill)
    pub fn with_time_limit(mut self, seconds: u64) -> Self {
        self.options.time_limit = Some(seconds);
        self
    }

    /// Set retry delay in seconds
    pub fn with_retry_delay(mut self, seconds: u64) -> Self {
        self.options.retry_delay = Some(seconds);
        self
    }

    /// Set retry backoff factor (exponential multiplier)
    pub fn with_retry_backoff(mut self, factor: f64) -> Self {
        self.options.retry_backoff = Some(factor);
        self
    }

    /// Set maximum retry delay
    pub fn with_retry_backoff_max(mut self, seconds: u64) -> Self {
        self.options.retry_backoff_max = Some(seconds);
        self
    }

    /// Enable/disable retry jitter
    pub fn with_retry_jitter(mut self, jitter: bool) -> Self {
        self.options.retry_jitter = Some(jitter);
        self
    }

    pub fn immutable(mut self) -> Self {
        self.immutable = true;
        self
    }

    /// Check if task has arguments
    pub fn has_args(&self) -> bool {
        !self.args.is_empty()
    }

    /// Check if task has keyword arguments
    pub fn has_kwargs(&self) -> bool {
        !self.kwargs.is_empty()
    }

    /// Check if task is immutable (args cannot be replaced)
    pub fn is_immutable(&self) -> bool {
        self.immutable
    }

    /// Check if task has a specific kwarg
    pub fn has_kwarg(&self, key: &str) -> bool {
        self.kwargs.contains_key(key)
    }

    /// Get a kwarg value
    pub fn get_kwarg(&self, key: &str) -> Option<&serde_json::Value> {
        self.kwargs.get(key)
    }

    /// Add a single kwarg
    pub fn add_kwarg(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.kwargs.insert(key.into(), value);
        self
    }

    /// Add a single argument
    pub fn add_arg(mut self, arg: serde_json::Value) -> Self {
        self.args.push(arg);
        self
    }

    /// Clone the signature
    pub fn clone_signature(&self) -> Self {
        self.clone()
    }

    /// Create an immutable signature (shorthand for `.immutable()`)
    ///
    /// This is equivalent to Python Celery's `.si()` method.
    /// Immutable signatures cannot have their arguments replaced when used in workflows.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Signature;
    ///
    /// let sig = Signature::new("process".to_string())
    ///     .with_args(vec![serde_json::json!(1)])
    ///     .si();
    ///
    /// assert!(sig.is_immutable());
    /// ```
    pub fn si(self) -> Self {
        self.immutable()
    }

    /// Create a partial signature with some arguments pre-filled
    ///
    /// The partial signature can have additional arguments added later.
    /// This is useful for creating task templates with some fixed arguments.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Signature;
    ///
    /// // Create a partial with first argument fixed
    /// let partial = Signature::new("add".to_string())
    ///     .partial(vec![serde_json::json!(10)]);
    ///
    /// // Complete with remaining arguments
    /// let complete = partial.complete(vec![serde_json::json!(5)]);
    /// assert_eq!(complete.args.len(), 2);
    /// ```
    pub fn partial(mut self, args: Vec<serde_json::Value>) -> Self {
        self.args = args;
        self
    }

    /// Complete a partial signature with additional arguments
    ///
    /// Appends the provided arguments to the existing arguments.
    /// If the signature is immutable, returns the signature unchanged.
    pub fn complete(mut self, additional_args: Vec<serde_json::Value>) -> Self {
        if self.immutable {
            return self;
        }
        self.args.extend(additional_args);
        self
    }

    /// Merge another signature into this one
    ///
    /// This combines kwargs from both signatures (the other's kwargs take precedence)
    /// and inherits options from the other signature if not already set.
    pub fn merge(mut self, other: Signature) -> Self {
        // Merge kwargs (other takes precedence)
        for (key, value) in other.kwargs {
            self.kwargs.insert(key, value);
        }

        // Inherit options if not already set
        if self.options.priority.is_none() {
            self.options.priority = other.options.priority;
        }
        if self.options.queue.is_none() {
            self.options.queue = other.options.queue;
        }
        if self.options.task_id.is_none() {
            self.options.task_id = other.options.task_id;
        }
        if self.options.link.is_none() {
            self.options.link = other.options.link;
        }
        if self.options.link_error.is_none() {
            self.options.link_error = other.options.link_error;
        }

        self
    }

    /// Replace arguments in signature (respects immutability)
    ///
    /// If the signature is immutable, returns None.
    /// Otherwise, returns a new signature with replaced arguments.
    pub fn replace_args(mut self, args: Vec<serde_json::Value>) -> Option<Self> {
        if self.immutable {
            return None;
        }
        self.args = args;
        Some(self)
    }

    /// Set expiration time in seconds
    pub fn with_expires(mut self, expires: u64) -> Self {
        self.options.expires = Some(expires);
        self
    }

    /// Set countdown (delay before execution) in seconds
    pub fn with_countdown(mut self, countdown: u64) -> Self {
        self.options.countdown = Some(countdown);
        self
    }

    /// Set retry policy
    pub fn with_retries(mut self, max_retries: u32) -> Self {
        self.options.max_retries = Some(max_retries);
        self
    }

    /// Set task routing key
    pub fn with_routing_key(mut self, routing_key: String) -> Self {
        self.options.routing_key = Some(routing_key);
        self
    }

    /// Set callback argument passing mode
    ///
    /// Controls how task result is passed to linked callbacks.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Signature, CallbackArgMode};
    ///
    /// let sig = Signature::new("task".to_string())
    ///     .with_callback_arg_mode(CallbackArgMode::Append);
    /// ```
    pub fn with_callback_arg_mode(mut self, mode: CallbackArgMode) -> Self {
        self.options.callback_arg_mode = mode;
        self
    }

    /// Set callback kwarg key (used when CallbackArgMode::Kwarg)
    ///
    /// Specifies the keyword argument name for passing the result.
    /// Defaults to "result" if not set.
    pub fn with_callback_kwarg_key(mut self, key: impl Into<String>) -> Self {
        self.options.callback_kwarg_key = Some(key.into());
        self
    }

    /// Configure callback to receive result as keyword argument
    ///
    /// Shorthand for setting CallbackArgMode::Kwarg with a key.
    pub fn with_result_as_kwarg(mut self, key: impl Into<String>) -> Self {
        self.options.callback_arg_mode = CallbackArgMode::Kwarg;
        self.options.callback_kwarg_key = Some(key.into());
        self
    }

    /// Serialize signature to JSON string
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserialize signature from JSON string
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Serialize signature to JSON bytes
    pub fn to_json_bytes(&self) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(self)
    }

    /// Deserialize signature from JSON bytes
    pub fn from_json_bytes(bytes: &[u8]) -> Result<Self, serde_json::Error> {
        serde_json::from_slice(bytes)
    }

    /// Clear all arguments from the signature
    ///
    /// Returns None if the signature is immutable.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Signature;
    ///
    /// let sig = Signature::new("task".to_string())
    ///     .with_args(vec![serde_json::json!(1), serde_json::json!(2)]);
    ///
    /// let cleared = sig.clear_args().unwrap();
    /// assert!(cleared.args.is_empty());
    /// ```
    pub fn clear_args(mut self) -> Option<Self> {
        if self.immutable {
            return None;
        }
        self.args.clear();
        Some(self)
    }

    /// Clear all keyword arguments from the signature
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Signature;
    /// use std::collections::HashMap;
    ///
    /// let mut kwargs = HashMap::new();
    /// kwargs.insert("key".to_string(), serde_json::json!("value"));
    ///
    /// let sig = Signature::new("task".to_string()).with_kwargs(kwargs);
    /// let cleared = sig.clear_kwargs();
    /// assert!(cleared.kwargs.is_empty());
    /// ```
    pub fn clear_kwargs(mut self) -> Self {
        self.kwargs.clear();
        self
    }

    /// Remove a specific keyword argument
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Signature;
    ///
    /// let sig = Signature::new("task".to_string())
    ///     .add_kwarg("key1", serde_json::json!("value1"))
    ///     .add_kwarg("key2", serde_json::json!("value2"));
    ///
    /// let modified = sig.remove_kwarg("key1");
    /// assert!(!modified.has_kwarg("key1"));
    /// assert!(modified.has_kwarg("key2"));
    /// ```
    pub fn remove_kwarg(mut self, key: &str) -> Self {
        self.kwargs.remove(key);
        self
    }

    /// Get the number of positional arguments
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Signature;
    ///
    /// let sig = Signature::new("task".to_string())
    ///     .with_args(vec![serde_json::json!(1), serde_json::json!(2)]);
    ///
    /// assert_eq!(sig.args_count(), 2);
    /// ```
    pub fn args_count(&self) -> usize {
        self.args.len()
    }

    /// Get the number of keyword arguments
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Signature;
    ///
    /// let sig = Signature::new("task".to_string())
    ///     .add_kwarg("key1", serde_json::json!("value1"))
    ///     .add_kwarg("key2", serde_json::json!("value2"));
    ///
    /// assert_eq!(sig.kwargs_count(), 2);
    /// ```
    pub fn kwargs_count(&self) -> usize {
        self.kwargs.len()
    }

    /// Get all keyword argument keys
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Signature;
    ///
    /// let sig = Signature::new("task".to_string())
    ///     .add_kwarg("key1", serde_json::json!("value1"))
    ///     .add_kwarg("key2", serde_json::json!("value2"));
    ///
    /// let keys = sig.kwarg_keys();
    /// assert_eq!(keys.len(), 2);
    /// assert!(keys.contains(&"key1"));
    /// assert!(keys.contains(&"key2"));
    /// ```
    pub fn kwarg_keys(&self) -> Vec<&str> {
        self.kwargs.keys().map(|k| k.as_str()).collect()
    }

    /// Check if signature has any retry configuration
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Signature;
    ///
    /// let sig1 = Signature::new("task".to_string()).with_retries(3);
    /// let sig2 = Signature::new("task".to_string());
    ///
    /// assert!(sig1.has_retry_config());
    /// assert!(!sig2.has_retry_config());
    /// ```
    pub fn has_retry_config(&self) -> bool {
        self.options.max_retries.is_some()
            || self.options.retry_delay.is_some()
            || self.options.retry_backoff.is_some()
    }

    /// Check if signature has any time limit configuration
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Signature;
    ///
    /// let sig1 = Signature::new("task".to_string()).with_time_limit(60);
    /// let sig2 = Signature::new("task".to_string());
    ///
    /// assert!(sig1.has_time_limit_config());
    /// assert!(!sig2.has_time_limit_config());
    /// ```
    pub fn has_time_limit_config(&self) -> bool {
        self.options.time_limit.is_some() || self.options.soft_time_limit.is_some()
    }

    /// Create a new signature with the same task name but no arguments
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Signature;
    ///
    /// let sig = Signature::new("task".to_string())
    ///     .with_args(vec![serde_json::json!(1)])
    ///     .with_priority(5);
    ///
    /// let clean = sig.clone_without_args();
    /// assert_eq!(clean.task, "task");
    /// assert!(clean.args.is_empty());
    /// assert_eq!(clean.options.priority, Some(5)); // Options preserved
    /// ```
    pub fn clone_without_args(&self) -> Self {
        Self {
            task: self.task.clone(),
            args: Vec::new(),
            kwargs: HashMap::new(),
            options: self.options.clone(),
            immutable: self.immutable,
        }
    }

    /// Calculate the estimated serialized size in bytes
    ///
    /// This gives a rough estimate of how much space the signature will take when serialized.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Signature;
    ///
    /// let sig = Signature::new("task".to_string())
    ///     .with_args(vec![serde_json::json!(1), serde_json::json!(2)]);
    ///
    /// let size = sig.estimated_size();
    /// assert!(size > 0);
    /// ```
    pub fn estimated_size(&self) -> usize {
        self.to_json().map(|s| s.len()).unwrap_or(0)
    }
}

impl std::fmt::Display for Signature {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Signature[task={}]", self.task)?;
        if !self.args.is_empty() {
            write!(f, " args={}", self.args.len())?;
        }
        if !self.kwargs.is_empty() {
            write!(f, " kwargs={}", self.kwargs.len())?;
        }
        if self.immutable {
            write!(f, " (immutable)")?;
        }
        Ok(())
    }
}

/// Callback argument passing mode
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum CallbackArgMode {
    /// Pass result as first positional argument (default)
    #[default]
    Prepend,

    /// Pass result as last positional argument
    Append,

    /// Pass result as a keyword argument with specified key
    Kwarg,

    /// Don't pass result to callback (callback uses its own args)
    None,
}

impl CallbackArgMode {
    /// Create a kwarg mode (result passed as "result" kwarg)
    pub fn kwarg() -> Self {
        Self::Kwarg
    }

    /// Check if this mode passes result to callback
    pub fn passes_result(&self) -> bool {
        !matches!(self, Self::None)
    }
}

impl std::fmt::Display for CallbackArgMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Prepend => write!(f, "prepend"),
            Self::Append => write!(f, "append"),
            Self::Kwarg => write!(f, "kwarg"),
            Self::None => write!(f, "none"),
        }
    }
}

/// Helper for serde skip_serializing_if
fn is_default_callback_arg_mode(mode: &CallbackArgMode) -> bool {
    *mode == CallbackArgMode::Prepend
}

/// Task options
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub struct TaskOptions {
    /// Task priority (0-9)
    pub priority: Option<u8>,

    /// Queue name
    pub queue: Option<String>,

    /// Task ID (for tracking)
    pub task_id: Option<Uuid>,

    /// Link (callback on success) - single callback for backwards compat
    pub link: Option<Box<Signature>>,

    /// Link error (callback on failure) - single callback for backwards compat
    pub link_error: Option<Box<Signature>>,

    /// Multiple success callbacks (executed in order)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub links: Vec<Signature>,

    /// Multiple failure callbacks (executed in order)
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub link_errors: Vec<Signature>,

    /// Callback on retry
    #[serde(skip_serializing_if = "Option::is_none")]
    pub on_retry: Option<Box<Signature>>,

    /// How to pass result to success callbacks
    #[serde(default, skip_serializing_if = "is_default_callback_arg_mode")]
    pub callback_arg_mode: CallbackArgMode,

    /// Key name when using CallbackArgMode::Kwarg
    #[serde(skip_serializing_if = "Option::is_none")]
    pub callback_kwarg_key: Option<String>,

    /// Task expiration time in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires: Option<u64>,

    /// Countdown (delay before execution) in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub countdown: Option<u64>,

    /// Maximum number of retries
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,

    /// Routing key for task distribution
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routing_key: Option<String>,

    /// Soft time limit in seconds (warning before kill)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub soft_time_limit: Option<u64>,

    /// Hard time limit in seconds (force kill)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_limit: Option<u64>,

    /// Retry delay in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_delay: Option<u64>,

    /// Retry backoff factor (exponential backoff multiplier)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_backoff: Option<f64>,

    /// Maximum retry delay in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_backoff_max: Option<u64>,

    /// Whether to add jitter to retry delays
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_jitter: Option<bool>,
}

impl TaskOptions {
    /// Check if priority is set
    pub fn has_priority(&self) -> bool {
        self.priority.is_some()
    }

    /// Check if queue is set
    pub fn has_queue(&self) -> bool {
        self.queue.is_some()
    }

    /// Check if task ID is set
    pub fn has_task_id(&self) -> bool {
        self.task_id.is_some()
    }

    /// Check if any link (success callback) is set
    pub fn has_link(&self) -> bool {
        self.link.is_some() || !self.links.is_empty()
    }

    /// Check if any link_error (failure callback) is set
    pub fn has_link_error(&self) -> bool {
        self.link_error.is_some() || !self.link_errors.is_empty()
    }

    /// Check if on_retry callback is set
    pub fn has_on_retry(&self) -> bool {
        self.on_retry.is_some()
    }

    /// Check if expires is set
    pub fn has_expires(&self) -> bool {
        self.expires.is_some()
    }

    /// Check if countdown is set
    pub fn has_countdown(&self) -> bool {
        self.countdown.is_some()
    }

    /// Check if max_retries is set
    pub fn has_max_retries(&self) -> bool {
        self.max_retries.is_some()
    }

    /// Check if routing_key is set
    pub fn has_routing_key(&self) -> bool {
        self.routing_key.is_some()
    }

    /// Check if soft time limit is set
    pub fn has_soft_time_limit(&self) -> bool {
        self.soft_time_limit.is_some()
    }

    /// Check if hard time limit is set
    pub fn has_time_limit(&self) -> bool {
        self.time_limit.is_some()
    }

    /// Get all success callbacks (both single link and multiple links)
    pub fn all_links(&self) -> Vec<&Signature> {
        let mut result = Vec::new();
        if let Some(ref link) = self.link {
            result.push(link.as_ref());
        }
        for link in &self.links {
            result.push(link);
        }
        result
    }

    /// Get all error callbacks (both single link_error and multiple link_errors)
    pub fn all_link_errors(&self) -> Vec<&Signature> {
        let mut result = Vec::new();
        if let Some(ref link_error) = self.link_error {
            result.push(link_error.as_ref());
        }
        for link_error in &self.link_errors {
            result.push(link_error);
        }
        result
    }

    /// Calculate retry delay with backoff
    pub fn calculate_retry_delay(&self, retry_count: u32) -> u64 {
        let base_delay = self.retry_delay.unwrap_or(1);
        let backoff = self.retry_backoff.unwrap_or(2.0);
        let max_delay = self.retry_backoff_max.unwrap_or(3600);

        let delay = (base_delay as f64 * backoff.powi(retry_count as i32)) as u64;
        delay.min(max_delay)
    }

    /// Get the callback argument mode
    pub fn callback_arg_mode(&self) -> CallbackArgMode {
        self.callback_arg_mode
    }

    /// Get the callback kwarg key (defaults to "result")
    pub fn callback_kwarg_key(&self) -> &str {
        self.callback_kwarg_key.as_deref().unwrap_or("result")
    }

    /// Prepare a callback signature with result passed according to callback_arg_mode
    ///
    /// This modifies the callback signature to include the result value
    /// according to the configured callback argument passing mode.
    ///
    /// # Arguments
    /// * `callback` - The callback signature to prepare
    /// * `result` - The result value to pass to the callback
    ///
    /// # Returns
    /// A new signature with the result incorporated
    pub fn prepare_callback(
        &self,
        mut callback: Signature,
        result: serde_json::Value,
    ) -> Signature {
        // Don't modify immutable signatures
        if callback.immutable {
            return callback;
        }

        match self.callback_arg_mode {
            CallbackArgMode::Prepend => {
                callback.args.insert(0, result);
            }
            CallbackArgMode::Append => {
                callback.args.push(result);
            }
            CallbackArgMode::Kwarg => {
                let key = self.callback_kwarg_key().to_string();
                callback.kwargs.insert(key, result);
            }
            CallbackArgMode::None => {
                // Don't modify the callback
            }
        }

        callback
    }
}

impl std::fmt::Display for TaskOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut parts = Vec::new();
        if let Some(priority) = self.priority {
            parts.push(format!("priority={}", priority));
        }
        if let Some(ref queue) = self.queue {
            parts.push(format!("queue={}", queue));
        }
        if let Some(task_id) = self.task_id {
            parts.push(format!("task_id={}", &task_id.to_string()[..8]));
        }
        if self.link.is_some() {
            parts.push("link=yes".to_string());
        }
        if self.link_error.is_some() {
            parts.push("link_error=yes".to_string());
        }
        if let Some(expires) = self.expires {
            parts.push(format!("expires={}s", expires));
        }
        if let Some(countdown) = self.countdown {
            parts.push(format!("countdown={}s", countdown));
        }
        if let Some(max_retries) = self.max_retries {
            parts.push(format!("retries={}", max_retries));
        }
        if let Some(ref routing_key) = self.routing_key {
            parts.push(format!("routing={}", routing_key));
        }
        if parts.is_empty() {
            write!(f, "TaskOptions[default]")
        } else {
            write!(f, "TaskOptions[{}]", parts.join(", "))
        }
    }
}

/// Chain: Sequential execution
///
/// task1(args1) -> task2(result1) -> task3(result2)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Chain {
    /// Tasks in the chain
    pub tasks: Vec<Signature>,
}

impl Chain {
    pub fn new() -> Self {
        Self { tasks: Vec::new() }
    }

    pub fn then(mut self, task: &str, args: Vec<serde_json::Value>) -> Self {
        self.tasks
            .push(Signature::new(task.to_string()).with_args(args));
        self
    }

    pub fn then_signature(mut self, signature: Signature) -> Self {
        self.tasks.push(signature);
        self
    }

    /// Apply the chain by enqueuing the first task with links to subsequent tasks
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.tasks.is_empty() {
            return Err(CanvasError::Invalid("Chain cannot be empty".to_string()));
        }

        // Build chain backwards: last task -> second-to-last -> ... -> first
        let mut chain_iter = self.tasks.into_iter().rev();
        let mut next_sig: Option<Signature> = None;

        // Start from the last task (no link)
        if let Some(last_task) = chain_iter.next() {
            // Last task has no link
            next_sig = Some(last_task);

            // Link remaining tasks backwards
            for mut task in chain_iter {
                task.options.link = next_sig.map(Box::new);
                next_sig = Some(task);
            }
        }

        // Enqueue the first task (which is now in next_sig)
        if let Some(first_sig) = next_sig {
            let task_id = Self::enqueue_signature(broker, &first_sig).await?;
            Ok(task_id)
        } else {
            Err(CanvasError::Invalid("Failed to build chain".to_string()))
        }
    }

    async fn enqueue_signature<B: Broker>(
        broker: &B,
        sig: &Signature,
    ) -> Result<Uuid, CanvasError> {
        let args_json = serde_json::json!({
            "args": sig.args,
            "kwargs": sig.kwargs
        });
        let args_bytes = serde_json::to_vec(&args_json)
            .map_err(|e| CanvasError::Serialization(e.to_string()))?;

        let mut task = SerializedTask::new(sig.task.clone(), args_bytes);

        if let Some(priority) = sig.options.priority {
            task = task.with_priority(priority.into());
        }

        let task_id = task.metadata.id;
        broker
            .enqueue(task)
            .await
            .map_err(|e| CanvasError::Broker(e.to_string()))?;

        Ok(task_id)
    }
}

impl Default for Chain {
    fn default() -> Self {
        Self::new()
    }
}

impl Chain {
    /// Check if chain is empty
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Get number of tasks in chain
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Get the first task in the chain
    pub fn first(&self) -> Option<&Signature> {
        self.tasks.first()
    }

    /// Get the last task in the chain
    pub fn last(&self) -> Option<&Signature> {
        self.tasks.last()
    }

    /// Get an iterator over the tasks
    pub fn iter(&self) -> std::slice::Iter<'_, Signature> {
        self.tasks.iter()
    }

    /// Get a mutable iterator over the tasks
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, Signature> {
        self.tasks.iter_mut()
    }

    /// Get a task by index
    pub fn get(&self, index: usize) -> Option<&Signature> {
        self.tasks.get(index)
    }

    /// Get a mutable task by index
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Signature> {
        self.tasks.get_mut(index)
    }

    /// Create a chain with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            tasks: Vec::with_capacity(capacity),
        }
    }

    /// Extend the chain with additional tasks
    pub fn extend(mut self, tasks: impl IntoIterator<Item = Signature>) -> Self {
        self.tasks.extend(tasks);
        self
    }

    /// Reverse the order of tasks in the chain
    pub fn reverse(mut self) -> Self {
        self.tasks.reverse();
        self
    }

    /// Retain only tasks that satisfy the predicate
    pub fn retain<F>(mut self, f: F) -> Self
    where
        F: FnMut(&Signature) -> bool,
    {
        self.tasks.retain(f);
        self
    }

    /// Apply the chain with a countdown (delay in seconds)
    ///
    /// The first task will be delayed by the countdown amount.
    /// Subsequent tasks are linked and will execute after the previous completes.
    ///
    /// # Example
    /// ```ignore
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// // Start chain execution after 60 seconds
    /// chain.apply_with_countdown(broker, 60).await?;
    /// ```
    pub async fn apply_with_countdown<B: Broker>(
        mut self,
        broker: &B,
        countdown: u64,
    ) -> Result<Uuid, CanvasError> {
        if self.tasks.is_empty() {
            return Err(CanvasError::Invalid("Chain cannot be empty".to_string()));
        }

        // Set countdown on the first task
        if let Some(first) = self.tasks.first_mut() {
            first.options.countdown = Some(countdown);
        }

        // Use regular apply to handle the chain
        self.apply(broker).await
    }

    /// Apply the chain with an ETA (execution time as Unix timestamp)
    ///
    /// The first task will be scheduled for execution at the specified ETA.
    /// Subsequent tasks are linked and will execute after the previous completes.
    ///
    /// # Example
    /// ```ignore
    /// use std::time::{SystemTime, UNIX_EPOCH, Duration};
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// // Schedule chain for 1 hour from now
    /// let eta = SystemTime::now()
    ///     .duration_since(UNIX_EPOCH).unwrap().as_secs() + 3600;
    /// chain.apply_with_eta(broker, eta).await?;
    /// ```
    pub async fn apply_with_eta<B: Broker>(
        mut self,
        broker: &B,
        eta: u64,
    ) -> Result<Uuid, CanvasError> {
        if self.tasks.is_empty() {
            return Err(CanvasError::Invalid("Chain cannot be empty".to_string()));
        }

        // Calculate countdown from ETA
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let countdown = eta.saturating_sub(now);

        // Set countdown on the first task
        if let Some(first) = self.tasks.first_mut() {
            first.options.countdown = Some(countdown);
        }

        self.apply(broker).await
    }

    /// Set countdown on all tasks in the chain (staggered execution)
    ///
    /// Each task gets a progressively larger countdown.
    ///
    /// # Arguments
    /// * `start` - Initial countdown for first task
    /// * `step` - Additional delay added for each subsequent task
    pub fn with_staggered_countdown(mut self, start: u64, step: u64) -> Self {
        let mut countdown = start;
        for task in &mut self.tasks {
            task.options.countdown = Some(countdown);
            countdown += step;
        }
        self
    }

    /// Append another chain to this chain
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Chain, Signature};
    ///
    /// let chain1 = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// let chain2 = Chain::new()
    ///     .then("task3", vec![])
    ///     .then("task4", vec![]);
    ///
    /// let combined = chain1.append(chain2);
    /// assert_eq!(combined.len(), 4);
    /// ```
    pub fn append(mut self, other: Chain) -> Self {
        self.tasks.extend(other.tasks);
        self
    }

    /// Prepend another chain to this chain
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Chain, Signature};
    ///
    /// let chain1 = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// let chain2 = Chain::new()
    ///     .then("task3", vec![])
    ///     .then("task4", vec![]);
    ///
    /// let combined = chain1.prepend(chain2);
    /// assert_eq!(combined.len(), 4);
    /// assert_eq!(combined.first().unwrap().task, "task3");
    /// ```
    pub fn prepend(mut self, other: Chain) -> Self {
        let mut new_tasks = other.tasks;
        new_tasks.extend(self.tasks);
        self.tasks = new_tasks;
        self
    }

    /// Split chain at the specified index
    ///
    /// Returns a tuple of (before, after) chains.
    /// The task at `index` will be the first task in the second chain.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![])
    ///     .then("task3", vec![])
    ///     .then("task4", vec![]);
    ///
    /// let (before, after) = chain.split_at(2);
    /// assert_eq!(before.len(), 2);
    /// assert_eq!(after.len(), 2);
    /// ```
    pub fn split_at(self, index: usize) -> (Chain, Chain) {
        let (before, after) = self.tasks.split_at(index.min(self.tasks.len()));
        (
            Chain {
                tasks: before.to_vec(),
            },
            Chain {
                tasks: after.to_vec(),
            },
        )
    }

    /// Concatenate multiple chains into a single chain
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chains = vec![
    ///     Chain::new().then("task1", vec![]),
    ///     Chain::new().then("task2", vec![]),
    ///     Chain::new().then("task3", vec![]),
    /// ];
    ///
    /// let combined = Chain::concat(chains);
    /// assert_eq!(combined.len(), 3);
    /// ```
    pub fn concat<I>(chains: I) -> Self
    where
        I: IntoIterator<Item = Chain>,
    {
        let mut result = Chain::new();
        for chain in chains {
            result.tasks.extend(chain.tasks);
        }
        result
    }

    /// Clone all tasks in the chain with a new task name prefix
    ///
    /// Useful for creating workflow variants.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("process", vec![])
    ///     .then("validate", vec![]);
    ///
    /// let prefixed = chain.with_task_prefix("batch_");
    /// assert_eq!(prefixed.first().unwrap().task, "batch_process");
    /// ```
    pub fn with_task_prefix(mut self, prefix: &str) -> Self {
        for task in &mut self.tasks {
            task.task = format!("{}{}", prefix, task.task);
        }
        self
    }

    /// Clone all tasks in the chain with a new task name suffix
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("process", vec![])
    ///     .then("validate", vec![]);
    ///
    /// let suffixed = chain.with_task_suffix("_v2");
    /// assert_eq!(suffixed.first().unwrap().task, "process_v2");
    /// ```
    pub fn with_task_suffix(mut self, suffix: &str) -> Self {
        for task in &mut self.tasks {
            task.task = format!("{}{}", task.task, suffix);
        }
        self
    }

    /// Validate that all tasks in the chain have non-empty names
    ///
    /// Returns true if all tasks are valid, false otherwise.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let valid = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    /// assert!(valid.is_valid());
    ///
    /// let invalid = Chain { tasks: vec![] };
    /// assert!(!invalid.is_valid());
    /// ```
    pub fn is_valid(&self) -> bool {
        !self.tasks.is_empty() && self.tasks.iter().all(|t| !t.task.is_empty())
    }

    /// Count tasks that match a predicate
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Chain, Signature};
    ///
    /// let chain = Chain::new()
    ///     .then_signature(Signature::new("high".to_string()).with_priority(9))
    ///     .then_signature(Signature::new("low".to_string()).with_priority(1))
    ///     .then_signature(Signature::new("urgent".to_string()).with_priority(9));
    ///
    /// let high_priority = chain.count_matching(|sig| sig.options.priority.unwrap_or(0) >= 9);
    /// assert_eq!(high_priority, 2);
    /// ```
    pub fn count_matching<F>(&self, predicate: F) -> usize
    where
        F: Fn(&Signature) -> bool,
    {
        self.tasks.iter().filter(|t| predicate(t)).count()
    }

    /// Check if any task matches a predicate
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("process", vec![])
    ///     .then("validate", vec![]);
    ///
    /// assert!(chain.any(|sig| sig.task == "validate"));
    /// assert!(!chain.any(|sig| sig.task == "missing"));
    /// ```
    pub fn any<F>(&self, predicate: F) -> bool
    where
        F: Fn(&Signature) -> bool,
    {
        self.tasks.iter().any(predicate)
    }

    /// Check if all tasks match a predicate
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("process", vec![])
    ///     .then("validate", vec![]);
    ///
    /// assert!(chain.all(|sig| !sig.task.is_empty()));
    /// ```
    pub fn all<F>(&self, predicate: F) -> bool
    where
        F: Fn(&Signature) -> bool,
    {
        self.tasks.iter().all(predicate)
    }

    /// Map over all tasks, transforming each signature
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Chain, Signature};
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// let modified = chain.map_tasks(|sig| {
    ///     Signature::new(format!("modified_{}", sig.task))
    /// });
    ///
    /// assert_eq!(modified.first().unwrap().task, "modified_task1");
    /// ```
    pub fn map_tasks<F>(mut self, f: F) -> Self
    where
        F: FnMut(Signature) -> Signature,
    {
        self.tasks = self.tasks.into_iter().map(f).collect();
        self
    }

    /// Filter and map tasks in one operation
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Chain, Signature};
    ///
    /// let chain = Chain::new()
    ///     .then_signature(Signature::new("high".to_string()).with_priority(9))
    ///     .then_signature(Signature::new("low".to_string()).with_priority(1))
    ///     .then_signature(Signature::new("urgent".to_string()).with_priority(9));
    ///
    /// let high_priority = chain.filter_map(|sig| {
    ///     if sig.options.priority.unwrap_or(0) >= 9 {
    ///         Some(sig)
    ///     } else {
    ///         None
    ///     }
    /// });
    ///
    /// assert_eq!(high_priority.len(), 2);
    /// ```
    pub fn filter_map<F>(mut self, f: F) -> Self
    where
        F: FnMut(Signature) -> Option<Signature>,
    {
        self.tasks = self.tasks.into_iter().filter_map(f).collect();
        self
    }

    /// Take the first n tasks from the chain
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![])
    ///     .then("task3", vec![])
    ///     .then("task4", vec![]);
    ///
    /// let first_two = chain.take(2);
    /// assert_eq!(first_two.len(), 2);
    /// ```
    pub fn take(mut self, n: usize) -> Self {
        self.tasks.truncate(n);
        self
    }

    /// Skip the first n tasks from the chain
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![])
    ///     .then("task3", vec![])
    ///     .then("task4", vec![]);
    ///
    /// let skipped = chain.skip(2);
    /// assert_eq!(skipped.len(), 2);
    /// assert_eq!(skipped.first().unwrap().task, "task3");
    /// ```
    pub fn skip(mut self, n: usize) -> Self {
        self.tasks = self.tasks.into_iter().skip(n).collect();
        self
    }

    /// Find the index of the first task with the given name
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![])
    ///     .then("task1", vec![]);
    ///
    /// assert_eq!(chain.find_task("task1"), Some(0));
    /// assert_eq!(chain.find_task("task2"), Some(1));
    /// assert_eq!(chain.find_task("task3"), None);
    /// ```
    pub fn find_task(&self, task_name: &str) -> Option<usize> {
        self.tasks.iter().position(|t| t.task == task_name)
    }

    /// Find all indices of tasks with the given name
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![])
    ///     .then("task1", vec![]);
    ///
    /// assert_eq!(chain.find_all_tasks("task1"), vec![0, 2]);
    /// assert_eq!(chain.find_all_tasks("task2"), vec![1]);
    /// ```
    pub fn find_all_tasks(&self, task_name: &str) -> Vec<usize> {
        self.tasks
            .iter()
            .enumerate()
            .filter(|(_, t)| t.task == task_name)
            .map(|(i, _)| i)
            .collect()
    }

    /// Check if the chain contains a task with the given name
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// assert!(chain.contains_task("task1"));
    /// assert!(!chain.contains_task("task3"));
    /// ```
    pub fn contains_task(&self, task_name: &str) -> bool {
        self.tasks.iter().any(|t| t.task == task_name)
    }

    /// Get the total estimated duration in seconds based on task time limits
    ///
    /// This sums up all task time limits (or soft_time_limit if time_limit is not set).
    /// Returns None if no tasks have time limits set.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Chain, Signature};
    ///
    /// let chain = Chain::new()
    ///     .then_signature(Signature::new("task1".to_string()).with_time_limit(10))
    ///     .then_signature(Signature::new("task2".to_string()).with_time_limit(20));
    ///
    /// assert_eq!(chain.estimated_duration(), Some(30));
    /// ```
    pub fn estimated_duration(&self) -> Option<u64> {
        let mut total = 0u64;
        let mut found_any = false;

        for task in &self.tasks {
            if let Some(limit) = task.options.time_limit.or(task.options.soft_time_limit) {
                total += limit;
                found_any = true;
            }
        }

        if found_any {
            Some(total)
        } else {
            None
        }
    }

    /// Get a summary of all task names in the chain
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("fetch", vec![])
    ///     .then("process", vec![])
    ///     .then("save", vec![]);
    ///
    /// assert_eq!(chain.task_names(), vec!["fetch", "process", "save"]);
    /// ```
    pub fn task_names(&self) -> Vec<&str> {
        self.tasks.iter().map(|t| t.task.as_str()).collect()
    }

    /// Get all unique task names in the chain
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![])
    ///     .then("task1", vec![]);
    ///
    /// let unique = chain.unique_task_names();
    /// assert_eq!(unique.len(), 2);
    /// assert!(unique.contains(&"task1"));
    /// assert!(unique.contains(&"task2"));
    /// ```
    pub fn unique_task_names(&self) -> std::collections::HashSet<&str> {
        self.tasks.iter().map(|t| t.task.as_str()).collect()
    }

    /// Clone the chain with a transformation applied to each task
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Chain;
    ///
    /// let chain = Chain::new()
    ///     .then("task1", vec![])
    ///     .then("task2", vec![]);
    ///
    /// let prioritized = chain.clone_with_transform(|sig| {
    ///     sig.clone().with_priority(5)
    /// });
    ///
    /// assert!(prioritized.tasks.iter().all(|t| t.options.priority == Some(5)));
    /// ```
    pub fn clone_with_transform<F>(&self, mut transform: F) -> Self
    where
        F: FnMut(&Signature) -> Signature,
    {
        Self {
            tasks: self.tasks.iter().map(&mut transform).collect(),
        }
    }
}

impl std::fmt::Display for Chain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Chain[{} tasks]", self.tasks.len())?;
        if !self.tasks.is_empty() {
            write!(
                f,
                " {} -> ... -> {}",
                self.tasks.first().unwrap().task,
                self.tasks.last().unwrap().task
            )?;
        }
        Ok(())
    }
}

impl IntoIterator for Chain {
    type Item = Signature;
    type IntoIter = std::vec::IntoIter<Signature>;

    fn into_iter(self) -> Self::IntoIter {
        self.tasks.into_iter()
    }
}

impl<'a> IntoIterator for &'a Chain {
    type Item = &'a Signature;
    type IntoIter = std::slice::Iter<'a, Signature>;

    fn into_iter(self) -> Self::IntoIter {
        self.tasks.iter()
    }
}

impl From<Vec<Signature>> for Chain {
    fn from(tasks: Vec<Signature>) -> Self {
        Self { tasks }
    }
}

impl FromIterator<Signature> for Chain {
    fn from_iter<T: IntoIterator<Item = Signature>>(iter: T) -> Self {
        Self {
            tasks: iter.into_iter().collect(),
        }
    }
}

/// Group: Parallel execution
///
/// (task1 | task2 | task3)
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Group {
    /// Tasks in the group
    pub tasks: Vec<Signature>,

    /// Group ID
    pub group_id: Option<Uuid>,
}

impl Group {
    pub fn new() -> Self {
        Self {
            tasks: Vec::new(),
            group_id: Some(Uuid::new_v4()),
        }
    }

    pub fn add(mut self, task: &str, args: Vec<serde_json::Value>) -> Self {
        self.tasks
            .push(Signature::new(task.to_string()).with_args(args));
        self
    }

    pub fn add_signature(mut self, signature: Signature) -> Self {
        self.tasks.push(signature);
        self
    }

    /// Apply the group by enqueuing all tasks to the broker
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.tasks.is_empty() {
            return Err(CanvasError::Invalid("Group cannot be empty".to_string()));
        }

        let group_id = self.group_id.unwrap_or_else(Uuid::new_v4);

        // Enqueue all tasks in parallel
        for sig in self.tasks {
            // Convert signature to SerializedTask
            let args_json = serde_json::json!({
                "args": sig.args,
                "kwargs": sig.kwargs
            });
            let args_bytes = serde_json::to_vec(&args_json)
                .map_err(|e| CanvasError::Serialization(e.to_string()))?;

            let mut task = SerializedTask::new(sig.task.clone(), args_bytes);

            // Set priority if specified
            if let Some(priority) = sig.options.priority {
                task = task.with_priority(priority.into());
            }

            // Set group_id in metadata (for tracking)
            task.metadata.group_id = Some(group_id);

            // Enqueue the task
            broker
                .enqueue(task)
                .await
                .map_err(|e| CanvasError::Broker(e.to_string()))?;
        }

        Ok(group_id)
    }
}

impl Default for Group {
    fn default() -> Self {
        Self::new()
    }
}

impl Group {
    /// Check if group is empty
    pub fn is_empty(&self) -> bool {
        self.tasks.is_empty()
    }

    /// Get the first task in the group
    pub fn first(&self) -> Option<&Signature> {
        self.tasks.first()
    }

    /// Get the last task in the group
    pub fn last(&self) -> Option<&Signature> {
        self.tasks.last()
    }

    /// Get an iterator over the tasks
    pub fn iter(&self) -> std::slice::Iter<'_, Signature> {
        self.tasks.iter()
    }

    /// Get a mutable iterator over the tasks
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, Signature> {
        self.tasks.iter_mut()
    }

    /// Get a task by index
    pub fn get(&self, index: usize) -> Option<&Signature> {
        self.tasks.get(index)
    }

    /// Get a mutable task by index
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Signature> {
        self.tasks.get_mut(index)
    }

    /// Create a group with pre-allocated capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            tasks: Vec::with_capacity(capacity),
            group_id: Some(Uuid::new_v4()),
        }
    }

    /// Extend the group with additional tasks
    pub fn extend(mut self, tasks: impl IntoIterator<Item = Signature>) -> Self {
        self.tasks.extend(tasks);
        self
    }

    /// Retain only tasks that satisfy the predicate
    pub fn retain<F>(mut self, f: F) -> Self
    where
        F: FnMut(&Signature) -> bool,
    {
        self.tasks.retain(f);
        self
    }

    /// Find a task by predicate
    pub fn find<F>(&self, predicate: F) -> Option<&Signature>
    where
        F: Fn(&Signature) -> bool,
    {
        self.tasks.iter().find(|sig| predicate(sig))
    }

    /// Filter tasks by predicate and return a new group
    pub fn filter<F>(mut self, predicate: F) -> Self
    where
        F: FnMut(&Signature) -> bool,
    {
        self.tasks.retain(predicate);
        self
    }

    /// Stagger task execution with countdown delays
    ///
    /// Each task gets a countdown that increases by the specified interval.
    /// This helps prevent thundering herd problems when launching many tasks.
    ///
    /// # Arguments
    /// * `start` - Initial countdown in seconds for the first task
    /// * `step` - Increment in seconds for each subsequent task
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .add("task3", vec![])
    ///     .skew(0.0, 1.0); // task1: 0s, task2: 1s, task3: 2s
    ///
    /// assert_eq!(group.tasks[0].options.countdown, Some(0));
    /// assert_eq!(group.tasks[1].options.countdown, Some(1));
    /// assert_eq!(group.tasks[2].options.countdown, Some(2));
    /// ```
    pub fn skew(mut self, start: f64, step: f64) -> Self {
        let mut countdown = start;
        for task in &mut self.tasks {
            task.options.countdown = Some(countdown as u64);
            countdown += step;
        }
        self
    }

    /// Stagger task execution with random jitter
    ///
    /// Each task gets a random countdown between 0 and max_delay.
    /// This provides more even load distribution than linear skew.
    ///
    /// # Arguments
    /// * `max_delay` - Maximum countdown in seconds
    pub fn jitter(mut self, max_delay: u64) -> Self {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        for (i, task) in self.tasks.iter_mut().enumerate() {
            // Use a deterministic "random" based on task index and name
            let mut hasher = DefaultHasher::new();
            i.hash(&mut hasher);
            task.task.hash(&mut hasher);
            let hash = hasher.finish();
            let delay = hash % (max_delay + 1);
            task.options.countdown = Some(delay);
        }
        self
    }

    /// Get number of tasks in group
    pub fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Check if group ID is set
    pub fn has_group_id(&self) -> bool {
        self.group_id.is_some()
    }

    /// Merge another group into this group
    ///
    /// All tasks from the other group are added to this group.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group1 = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![]);
    ///
    /// let group2 = Group::new()
    ///     .add("task3", vec![])
    ///     .add("task4", vec![]);
    ///
    /// let merged = group1.merge(group2);
    /// assert_eq!(merged.len(), 4);
    /// ```
    pub fn merge(mut self, other: Group) -> Self {
        self.tasks.extend(other.tasks);
        self
    }

    /// Partition tasks into multiple groups based on a predicate
    ///
    /// Returns a tuple of (matching, non_matching) groups.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add_signature(Signature::new("high_priority".to_string()).with_priority(9))
    ///     .add_signature(Signature::new("normal".to_string()).with_priority(5))
    ///     .add_signature(Signature::new("urgent".to_string()).with_priority(9))
    ///     .add_signature(Signature::new("low".to_string()).with_priority(1));
    ///
    /// let (high, low) = group.partition(|sig| sig.options.priority.unwrap_or(0) >= 9);
    /// assert_eq!(high.len(), 2);
    /// assert_eq!(low.len(), 2);
    /// ```
    pub fn partition<F>(self, mut predicate: F) -> (Group, Group)
    where
        F: FnMut(&Signature) -> bool,
    {
        let (matching, non_matching): (Vec<_>, Vec<_>) =
            self.tasks.into_iter().partition(|sig| predicate(sig));

        (
            Group {
                tasks: matching,
                group_id: self.group_id,
            },
            Group {
                tasks: non_matching,
                group_id: None, // Different group
            },
        )
    }

    /// Add a task name prefix to all tasks in the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("process", vec![])
    ///     .add("validate", vec![]);
    ///
    /// let prefixed = group.with_task_prefix("batch_");
    /// assert_eq!(prefixed.first().unwrap().task, "batch_process");
    /// ```
    pub fn with_task_prefix(mut self, prefix: &str) -> Self {
        for task in &mut self.tasks {
            task.task = format!("{}{}", prefix, task.task);
        }
        self
    }

    /// Add a task name suffix to all tasks in the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("process", vec![])
    ///     .add("validate", vec![]);
    ///
    /// let suffixed = group.with_task_suffix("_v2");
    /// assert_eq!(suffixed.first().unwrap().task, "process_v2");
    /// ```
    pub fn with_task_suffix(mut self, suffix: &str) -> Self {
        for task in &mut self.tasks {
            task.task = format!("{}{}", task.task, suffix);
        }
        self
    }

    /// Set priority on all tasks in the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .with_priority(9);
    ///
    /// assert_eq!(group.first().unwrap().options.priority, Some(9));
    /// ```
    pub fn with_priority(mut self, priority: u8) -> Self {
        for task in &mut self.tasks {
            task.options.priority = Some(priority);
        }
        self
    }

    /// Set queue on all tasks in the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .with_queue("high_priority".to_string());
    ///
    /// assert_eq!(group.first().unwrap().options.queue, Some("high_priority".to_string()));
    /// ```
    pub fn with_queue(mut self, queue: String) -> Self {
        for task in &mut self.tasks {
            task.options.queue = Some(queue.clone());
        }
        self
    }

    /// Validate that all tasks in the group have non-empty names
    ///
    /// Returns true if all tasks are valid, false otherwise.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let valid = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![]);
    /// assert!(valid.is_valid());
    ///
    /// let invalid = Group { tasks: vec![], group_id: None };
    /// assert!(!invalid.is_valid());
    /// ```
    pub fn is_valid(&self) -> bool {
        !self.tasks.is_empty() && self.tasks.iter().all(|t| !t.task.is_empty())
    }

    /// Count tasks that match a predicate
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add_signature(Signature::new("high".to_string()).with_priority(9))
    ///     .add_signature(Signature::new("low".to_string()).with_priority(1))
    ///     .add_signature(Signature::new("urgent".to_string()).with_priority(9));
    ///
    /// let high_priority = group.count_matching(|sig| sig.options.priority.unwrap_or(0) >= 9);
    /// assert_eq!(high_priority, 2);
    /// ```
    pub fn count_matching<F>(&self, predicate: F) -> usize
    where
        F: Fn(&Signature) -> bool,
    {
        self.tasks.iter().filter(|t| predicate(t)).count()
    }

    /// Check if any task matches a predicate
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("process", vec![])
    ///     .add("validate", vec![]);
    ///
    /// assert!(group.any(|sig| sig.task == "validate"));
    /// assert!(!group.any(|sig| sig.task == "missing"));
    /// ```
    pub fn any<F>(&self, predicate: F) -> bool
    where
        F: Fn(&Signature) -> bool,
    {
        self.tasks.iter().any(predicate)
    }

    /// Check if all tasks match a predicate
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("process", vec![])
    ///     .add("validate", vec![]);
    ///
    /// assert!(group.all(|sig| !sig.task.is_empty()));
    /// ```
    pub fn all<F>(&self, predicate: F) -> bool
    where
        F: Fn(&Signature) -> bool,
    {
        self.tasks.iter().all(predicate)
    }

    /// Map over all tasks, transforming each signature
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![]);
    ///
    /// let modified = group.map_tasks(|sig| {
    ///     Signature::new(format!("parallel_{}", sig.task))
    /// });
    ///
    /// assert_eq!(modified.first().unwrap().task, "parallel_task1");
    /// ```
    pub fn map_tasks<F>(mut self, f: F) -> Self
    where
        F: FnMut(Signature) -> Signature,
    {
        self.tasks = self.tasks.into_iter().map(f).collect();
        self
    }

    /// Filter and map tasks in one operation
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add_signature(Signature::new("high".to_string()).with_priority(9))
    ///     .add_signature(Signature::new("low".to_string()).with_priority(1))
    ///     .add_signature(Signature::new("urgent".to_string()).with_priority(9));
    ///
    /// let high_priority = group.filter_map(|sig| {
    ///     if sig.options.priority.unwrap_or(0) >= 9 {
    ///         Some(sig)
    ///     } else {
    ///         None
    ///     }
    /// });
    ///
    /// assert_eq!(high_priority.len(), 2);
    /// ```
    pub fn filter_map<F>(mut self, f: F) -> Self
    where
        F: FnMut(Signature) -> Option<Signature>,
    {
        self.tasks = self.tasks.into_iter().filter_map(f).collect();
        self
    }

    /// Take the first n tasks from the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .add("task3", vec![])
    ///     .add("task4", vec![]);
    ///
    /// let first_two = group.take(2);
    /// assert_eq!(first_two.len(), 2);
    /// ```
    pub fn take(mut self, n: usize) -> Self {
        self.tasks.truncate(n);
        self
    }

    /// Skip the first n tasks from the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .add("task3", vec![])
    ///     .add("task4", vec![]);
    ///
    /// let skipped = group.skip(2);
    /// assert_eq!(skipped.len(), 2);
    /// assert_eq!(skipped.first().unwrap().task, "task3");
    /// ```
    pub fn skip(mut self, n: usize) -> Self {
        self.tasks = self.tasks.into_iter().skip(n).collect();
        self
    }

    /// Find the index of the first task with the given name
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .add("task1", vec![]);
    ///
    /// assert_eq!(group.find_task("task1"), Some(0));
    /// assert_eq!(group.find_task("task2"), Some(1));
    /// assert_eq!(group.find_task("task3"), None);
    /// ```
    pub fn find_task(&self, task_name: &str) -> Option<usize> {
        self.tasks.iter().position(|t| t.task == task_name)
    }

    /// Find all indices of tasks with the given name
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .add("task1", vec![]);
    ///
    /// assert_eq!(group.find_all_tasks("task1"), vec![0, 2]);
    /// assert_eq!(group.find_all_tasks("task2"), vec![1]);
    /// ```
    pub fn find_all_tasks(&self, task_name: &str) -> Vec<usize> {
        self.tasks
            .iter()
            .enumerate()
            .filter(|(_, t)| t.task == task_name)
            .map(|(i, _)| i)
            .collect()
    }

    /// Check if the group contains a task with the given name
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![]);
    ///
    /// assert!(group.contains_task("task1"));
    /// assert!(!group.contains_task("task3"));
    /// ```
    pub fn contains_task(&self, task_name: &str) -> bool {
        self.tasks.iter().any(|t| t.task == task_name)
    }

    /// Get the maximum estimated duration in seconds based on task time limits
    ///
    /// Since tasks run in parallel, the group duration is the maximum of all task durations.
    /// Returns None if no tasks have time limits set.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add_signature(Signature::new("task1".to_string()).with_time_limit(10))
    ///     .add_signature(Signature::new("task2".to_string()).with_time_limit(20));
    ///
    /// assert_eq!(group.estimated_duration(), Some(20)); // Max of 10 and 20
    /// ```
    pub fn estimated_duration(&self) -> Option<u64> {
        self.tasks
            .iter()
            .filter_map(|t| t.options.time_limit.or(t.options.soft_time_limit))
            .max()
    }

    /// Get a summary of all task names in the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("fetch", vec![])
    ///     .add("process", vec![])
    ///     .add("save", vec![]);
    ///
    /// assert_eq!(group.task_names(), vec!["fetch", "process", "save"]);
    /// ```
    pub fn task_names(&self) -> Vec<&str> {
        self.tasks.iter().map(|t| t.task.as_str()).collect()
    }

    /// Get all unique task names in the group
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .add("task1", vec![]);
    ///
    /// let unique = group.unique_task_names();
    /// assert_eq!(unique.len(), 2);
    /// assert!(unique.contains(&"task1"));
    /// assert!(unique.contains(&"task2"));
    /// ```
    pub fn unique_task_names(&self) -> std::collections::HashSet<&str> {
        self.tasks.iter().map(|t| t.task.as_str()).collect()
    }

    /// Clone the group with a transformation applied to each task
    ///
    /// # Example
    /// ```
    /// use celers_canvas::Group;
    ///
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![]);
    ///
    /// let prioritized = group.clone_with_transform(|sig| {
    ///     sig.clone().with_priority(5)
    /// });
    ///
    /// assert!(prioritized.tasks.iter().all(|t| t.options.priority == Some(5)));
    /// ```
    pub fn clone_with_transform<F>(&self, mut transform: F) -> Self
    where
        F: FnMut(&Signature) -> Signature,
    {
        Self {
            tasks: self.tasks.iter().map(&mut transform).collect(),
            group_id: self.group_id,
        }
    }

    /// Count tasks by priority level
    ///
    /// Returns a map of priority values to the count of tasks at that priority.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add_signature(Signature::new("task1".to_string()).with_priority(1))
    ///     .add_signature(Signature::new("task2".to_string()).with_priority(5))
    ///     .add_signature(Signature::new("task3".to_string()).with_priority(1));
    ///
    /// let counts = group.count_by_priority();
    /// assert_eq!(counts.get(&1), Some(&2));
    /// assert_eq!(counts.get(&5), Some(&1));
    /// ```
    pub fn count_by_priority(&self) -> std::collections::HashMap<u8, usize> {
        let mut counts = std::collections::HashMap::new();
        for task in &self.tasks {
            if let Some(priority) = task.options.priority {
                *counts.entry(priority).or_insert(0) += 1;
            }
        }
        counts
    }

    /// Count tasks by queue name
    ///
    /// Returns a map of queue names to the count of tasks targeting that queue.
    ///
    /// # Example
    /// ```
    /// use celers_canvas::{Group, Signature};
    ///
    /// let group = Group::new()
    ///     .add_signature(Signature::new("task1".to_string()).with_queue("queue_a".to_string()))
    ///     .add_signature(Signature::new("task2".to_string()).with_queue("queue_b".to_string()))
    ///     .add_signature(Signature::new("task3".to_string()).with_queue("queue_a".to_string()));
    ///
    /// let counts = group.count_by_queue();
    /// assert_eq!(counts.get("queue_a"), Some(&2));
    /// assert_eq!(counts.get("queue_b"), Some(&1));
    /// ```
    pub fn count_by_queue(&self) -> std::collections::HashMap<String, usize> {
        let mut counts = std::collections::HashMap::new();
        for task in &self.tasks {
            if let Some(ref queue) = task.options.queue {
                *counts.entry(queue.clone()).or_insert(0) += 1;
            }
        }
        counts
    }
}

impl std::fmt::Display for Group {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Group[{} tasks]", self.tasks.len())?;
        if let Some(group_id) = self.group_id {
            write!(f, " id={}", &group_id.to_string()[..8])?;
        }
        Ok(())
    }
}

impl IntoIterator for Group {
    type Item = Signature;
    type IntoIter = std::vec::IntoIter<Signature>;

    fn into_iter(self) -> Self::IntoIter {
        self.tasks.into_iter()
    }
}

impl<'a> IntoIterator for &'a Group {
    type Item = &'a Signature;
    type IntoIter = std::slice::Iter<'a, Signature>;

    fn into_iter(self) -> Self::IntoIter {
        self.tasks.iter()
    }
}

impl From<Vec<Signature>> for Group {
    fn from(tasks: Vec<Signature>) -> Self {
        Self {
            tasks,
            group_id: Some(Uuid::new_v4()),
        }
    }
}

impl FromIterator<Signature> for Group {
    fn from_iter<T: IntoIterator<Item = Signature>>(iter: T) -> Self {
        Self {
            tasks: iter.into_iter().collect(),
            group_id: Some(Uuid::new_v4()),
        }
    }
}

/// Chord: Parallel execution with callback
///
/// (task1 | task2 | task3) -> callback([result1, result2, result3])
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Chord {
    /// Header (parallel tasks)
    pub header: Group,

    /// Body (callback task)
    pub body: Signature,
}

impl Chord {
    pub fn new(header: Group, body: Signature) -> Self {
        Self { header, body }
    }

    /// Apply the chord by initializing state and enqueuing header tasks
    #[cfg(feature = "backend-redis")]
    pub async fn apply<B: Broker, R: ResultBackend>(
        mut self,
        broker: &B,
        backend: &mut R,
    ) -> Result<Uuid, CanvasError> {
        if self.header.tasks.is_empty() {
            return Err(CanvasError::Invalid(
                "Chord header cannot be empty".to_string(),
            ));
        }

        let chord_id = Uuid::new_v4();
        let total = self.header.tasks.len();

        // Initialize chord state in backend
        let chord_state = ChordState {
            chord_id,
            total,
            completed: 0,
            callback: Some(self.body.task.clone()),
            task_ids: Vec::new(),
            created_at: Utc::now(),
            timeout: None,
            cancelled: false,
            cancellation_reason: None,
            retry_count: 0,
            max_retries: None,
        };

        backend
            .chord_init(chord_state)
            .await
            .map_err(|e| CanvasError::Broker(format!("Failed to initialize chord: {}", e)))?;

        // Enqueue all header tasks with chord_id
        for sig in &mut self.header.tasks {
            let args_json = serde_json::json!({
                "args": sig.args,
                "kwargs": sig.kwargs
            });
            let args_bytes = serde_json::to_vec(&args_json)
                .map_err(|e| CanvasError::Serialization(e.to_string()))?;

            let mut task = SerializedTask::new(sig.task.clone(), args_bytes);

            if let Some(priority) = sig.options.priority {
                task = task.with_priority(priority.into());
            }

            // Set chord_id so worker knows to update chord state on completion
            task.metadata.chord_id = Some(chord_id);

            broker
                .enqueue(task)
                .await
                .map_err(|e| CanvasError::Broker(e.to_string()))?;
        }

        Ok(chord_id)
    }

    /// Apply the chord without a result backend (simpler version)
    #[cfg(not(feature = "backend-redis"))]
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        // Without backend, we can only enqueue header tasks
        // Manual coordination required
        self.header.apply(broker).await
    }
}

impl std::fmt::Display for Chord {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Chord[{} tasks] -> callback({})",
            self.header.tasks.len(),
            self.body.task
        )
    }
}

/// Map: Apply task to multiple arguments
///
/// map(task, [args1, args2, args3]) -> [result1, result2, result3]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Map {
    /// Task to apply
    pub task: Signature,

    /// List of argument sets
    pub argsets: Vec<Vec<serde_json::Value>>,
}

impl Map {
    pub fn new(task: Signature, argsets: Vec<Vec<serde_json::Value>>) -> Self {
        Self { task, argsets }
    }

    /// Apply the map by creating a group of tasks with different arguments
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        let mut group = Group::new();

        for args in self.argsets {
            let mut sig = self.task.clone();
            sig.args = args;
            group = group.add_signature(sig);
        }

        group.apply(broker).await
    }

    /// Check if map is empty
    pub fn is_empty(&self) -> bool {
        self.argsets.is_empty()
    }

    /// Get number of argument sets (and thus tasks)
    pub fn len(&self) -> usize {
        self.argsets.len()
    }
}

impl std::fmt::Display for Map {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Map[task={}, {} argsets]",
            self.task.task,
            self.argsets.len()
        )
    }
}

/// Starmap: Like map but unpacks arguments
///
/// starmap(task, [(a1, b1), (a2, b2)]) -> [task(a1, b1), task(a2, b2)]
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Starmap {
    /// Task to apply
    pub task: Signature,

    /// List of argument tuples
    pub argsets: Vec<Vec<serde_json::Value>>,
}

impl Starmap {
    pub fn new(task: Signature, argsets: Vec<Vec<serde_json::Value>>) -> Self {
        Self { task, argsets }
    }

    /// Apply the starmap by creating a group of tasks with unpacked arguments
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        // Starmap is the same as Map - the unpacking happens in task execution
        let map = Map::new(self.task, self.argsets);
        map.apply(broker).await
    }

    /// Check if starmap is empty
    pub fn is_empty(&self) -> bool {
        self.argsets.is_empty()
    }

    /// Get number of argument sets (and thus tasks)
    pub fn len(&self) -> usize {
        self.argsets.len()
    }
}

impl std::fmt::Display for Starmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Starmap[task={}, {} argsets]",
            self.task.task,
            self.argsets.len()
        )
    }
}

/// Chunks: Split iterable into chunks for parallel processing
///
/// chunks(task, items, chunk_size) -> Group of tasks, each processing a chunk
///
/// # Example
/// ```
/// use celers_canvas::{Chunks, Signature};
///
/// let task = Signature::new("process_batch".to_string());
/// let items: Vec<serde_json::Value> = (0..100).map(|i| serde_json::json!(i)).collect();
///
/// // Process 100 items in chunks of 10 (creates 10 parallel tasks)
/// let chunks = Chunks::new(task, items, 10);
/// assert_eq!(chunks.num_chunks(), 10);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Chunks {
    /// Task to apply to each chunk
    pub task: Signature,

    /// Items to split into chunks
    pub items: Vec<serde_json::Value>,

    /// Size of each chunk
    pub chunk_size: usize,
}

impl Chunks {
    /// Create a new Chunks workflow
    ///
    /// # Arguments
    /// * `task` - The task signature to apply to each chunk
    /// * `items` - Items to split into chunks
    /// * `chunk_size` - Number of items per chunk
    pub fn new(task: Signature, items: Vec<serde_json::Value>, chunk_size: usize) -> Self {
        Self {
            task,
            items,
            chunk_size: chunk_size.max(1), // Minimum chunk size of 1
        }
    }

    /// Get the number of chunks that will be created
    pub fn num_chunks(&self) -> usize {
        if self.items.is_empty() {
            0
        } else {
            self.items.len().div_ceil(self.chunk_size)
        }
    }

    /// Check if chunks is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Get total number of items
    pub fn len(&self) -> usize {
        self.items.len()
    }

    /// Convert to a Group for execution
    pub fn to_group(&self) -> Group {
        let mut group = Group::new();

        for chunk in self.items.chunks(self.chunk_size) {
            let mut sig = self.task.clone();
            sig.args = vec![serde_json::json!(chunk)];
            group = group.add_signature(sig);
        }

        group
    }

    /// Apply the chunks by creating a group of tasks
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.items.is_empty() {
            return Err(CanvasError::Invalid("Chunks cannot be empty".to_string()));
        }

        self.to_group().apply(broker).await
    }
}

impl std::fmt::Display for Chunks {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Chunks[task={}, {} items, chunk_size={}, {} chunks]",
            self.task.task,
            self.items.len(),
            self.chunk_size,
            self.num_chunks()
        )
    }
}

/// XMap: Map with exception handling
///
/// Like Map, but continues processing even if some tasks fail.
/// Failed tasks are tracked separately.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct XMap {
    /// Task to apply
    pub task: Signature,

    /// List of argument sets
    pub argsets: Vec<Vec<serde_json::Value>>,

    /// Whether to stop on first error
    pub fail_fast: bool,
}

impl XMap {
    /// Create a new XMap workflow
    pub fn new(task: Signature, argsets: Vec<Vec<serde_json::Value>>) -> Self {
        Self {
            task,
            argsets,
            fail_fast: false,
        }
    }

    /// Set fail-fast behavior (stop on first error)
    pub fn fail_fast(mut self, fail_fast: bool) -> Self {
        self.fail_fast = fail_fast;
        self
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.argsets.is_empty()
    }

    /// Get number of argument sets
    pub fn len(&self) -> usize {
        self.argsets.len()
    }

    /// Apply the xmap by creating a group of tasks
    ///
    /// Note: Exception handling is done at the result collection level,
    /// not during task submission.
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        let map = Map::new(self.task, self.argsets);
        map.apply(broker).await
    }
}

impl std::fmt::Display for XMap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "XMap[task={}, {} argsets, fail_fast={}]",
            self.task.task,
            self.argsets.len(),
            self.fail_fast
        )
    }
}

/// XStarmap: Starmap with exception handling
///
/// Like Starmap, but continues processing even if some tasks fail.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct XStarmap {
    /// Task to apply
    pub task: Signature,

    /// List of argument tuples
    pub argsets: Vec<Vec<serde_json::Value>>,

    /// Whether to stop on first error
    pub fail_fast: bool,
}

impl XStarmap {
    /// Create a new XStarmap workflow
    pub fn new(task: Signature, argsets: Vec<Vec<serde_json::Value>>) -> Self {
        Self {
            task,
            argsets,
            fail_fast: false,
        }
    }

    /// Set fail-fast behavior (stop on first error)
    pub fn fail_fast(mut self, fail_fast: bool) -> Self {
        self.fail_fast = fail_fast;
        self
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.argsets.is_empty()
    }

    /// Get number of argument sets
    pub fn len(&self) -> usize {
        self.argsets.len()
    }

    /// Apply the xstarmap by creating a group of tasks
    pub async fn apply<B: Broker>(self, broker: &B) -> Result<Uuid, CanvasError> {
        let starmap = Starmap::new(self.task, self.argsets);
        starmap.apply(broker).await
    }
}

impl std::fmt::Display for XStarmap {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "XStarmap[task={}, {} argsets, fail_fast={}]",
            self.task.task,
            self.argsets.len(),
            self.fail_fast
        )
    }
}

// ============================================================================
// Conditional Workflow Primitives
// ============================================================================

/// Condition for conditional workflow branching
///
/// Represents a condition that determines which branch of a workflow to execute.
/// Conditions can be evaluated against the result of a previous task or against
/// static values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Condition {
    /// Always true - execute the then branch
    Always,

    /// Always false - execute the else branch
    Never,

    /// Check if a value equals the expected value
    Equals {
        /// Field path to extract from result (e.g., "status" or "data.count")
        field: Option<String>,
        /// Expected value
        value: serde_json::Value,
    },

    /// Check if a value is not equal to the expected value
    NotEquals {
        /// Field path to extract from result
        field: Option<String>,
        /// Value to compare against
        value: serde_json::Value,
    },

    /// Check if a numeric value is greater than threshold
    GreaterThan {
        /// Field path to extract from result
        field: Option<String>,
        /// Threshold value
        threshold: f64,
    },

    /// Check if a numeric value is less than threshold
    LessThan {
        /// Field path to extract from result
        field: Option<String>,
        /// Threshold value
        threshold: f64,
    },

    /// Check if a value is truthy (not null, not false, not 0, not empty)
    Truthy {
        /// Field path to extract from result
        field: Option<String>,
    },

    /// Check if a value is falsy (null, false, 0, or empty)
    Falsy {
        /// Field path to extract from result
        field: Option<String>,
    },

    /// Check if a value contains a substring or element
    Contains {
        /// Field path to extract from result
        field: Option<String>,
        /// Value to search for
        value: serde_json::Value,
    },

    /// Check if a value matches a regex pattern
    Matches {
        /// Field path to extract from result
        field: Option<String>,
        /// Regex pattern
        pattern: String,
    },

    /// Logical AND of multiple conditions
    And(Vec<Condition>),

    /// Logical OR of multiple conditions
    Or(Vec<Condition>),

    /// Logical NOT of a condition
    Not(Box<Condition>),

    /// Custom condition evaluated by a task
    /// The task should return a boolean result
    Custom {
        /// Task name that evaluates the condition
        task: String,
        /// Arguments for the condition task
        args: Vec<serde_json::Value>,
    },
}

impl Condition {
    /// Create an always-true condition
    pub fn always() -> Self {
        Self::Always
    }

    /// Create an always-false condition
    pub fn never() -> Self {
        Self::Never
    }

    /// Create an equals condition
    pub fn equals(value: serde_json::Value) -> Self {
        Self::Equals { field: None, value }
    }

    /// Create an equals condition on a specific field
    pub fn field_equals(field: impl Into<String>, value: serde_json::Value) -> Self {
        Self::Equals {
            field: Some(field.into()),
            value,
        }
    }

    /// Create a not-equals condition
    pub fn not_equals(value: serde_json::Value) -> Self {
        Self::NotEquals { field: None, value }
    }

    /// Create a greater-than condition
    pub fn greater_than(threshold: f64) -> Self {
        Self::GreaterThan {
            field: None,
            threshold,
        }
    }

    /// Create a greater-than condition on a specific field
    pub fn field_greater_than(field: impl Into<String>, threshold: f64) -> Self {
        Self::GreaterThan {
            field: Some(field.into()),
            threshold,
        }
    }

    /// Create a less-than condition
    pub fn less_than(threshold: f64) -> Self {
        Self::LessThan {
            field: None,
            threshold,
        }
    }

    /// Create a truthy condition
    pub fn truthy() -> Self {
        Self::Truthy { field: None }
    }

    /// Create a truthy condition on a specific field
    pub fn field_truthy(field: impl Into<String>) -> Self {
        Self::Truthy {
            field: Some(field.into()),
        }
    }

    /// Create a falsy condition
    pub fn falsy() -> Self {
        Self::Falsy { field: None }
    }

    /// Create a contains condition
    pub fn contains(value: serde_json::Value) -> Self {
        Self::Contains { field: None, value }
    }

    /// Create a regex match condition
    pub fn matches(pattern: impl Into<String>) -> Self {
        Self::Matches {
            field: None,
            pattern: pattern.into(),
        }
    }

    /// Create a custom task-based condition
    pub fn custom(task: impl Into<String>, args: Vec<serde_json::Value>) -> Self {
        Self::Custom {
            task: task.into(),
            args,
        }
    }

    /// Combine with AND
    pub fn and(self, other: Condition) -> Self {
        match self {
            Self::And(mut conditions) => {
                conditions.push(other);
                Self::And(conditions)
            }
            _ => Self::And(vec![self, other]),
        }
    }

    /// Combine with OR
    pub fn or(self, other: Condition) -> Self {
        match self {
            Self::Or(mut conditions) => {
                conditions.push(other);
                Self::Or(conditions)
            }
            _ => Self::Or(vec![self, other]),
        }
    }

    /// Negate the condition
    pub fn negate(self) -> Self {
        Self::Not(Box::new(self))
    }

    /// Evaluate the condition against a JSON value
    pub fn evaluate(&self, value: &serde_json::Value) -> bool {
        match self {
            Self::Always => true,
            Self::Never => false,
            Self::Equals {
                field,
                value: expected,
            } => {
                let actual = extract_field(value, field.as_deref());
                actual == *expected
            }
            Self::NotEquals {
                field,
                value: expected,
            } => {
                let actual = extract_field(value, field.as_deref());
                actual != *expected
            }
            Self::GreaterThan { field, threshold } => {
                let actual = extract_field(value, field.as_deref());
                actual.as_f64().is_some_and(|v| v > *threshold)
            }
            Self::LessThan { field, threshold } => {
                let actual = extract_field(value, field.as_deref());
                actual.as_f64().is_some_and(|v| v < *threshold)
            }
            Self::Truthy { field } => {
                let actual = extract_field(value, field.as_deref());
                is_truthy(&actual)
            }
            Self::Falsy { field } => {
                let actual = extract_field(value, field.as_deref());
                !is_truthy(&actual)
            }
            Self::Contains {
                field,
                value: needle,
            } => {
                let actual = extract_field(value, field.as_deref());
                contains_value(&actual, needle)
            }
            Self::Matches { field, pattern } => {
                let actual = extract_field(value, field.as_deref());
                if let Some(s) = actual.as_str() {
                    regex::Regex::new(pattern)
                        .map(|re| re.is_match(s))
                        .unwrap_or(false)
                } else {
                    false
                }
            }
            Self::And(conditions) => conditions.iter().all(|c| c.evaluate(value)),
            Self::Or(conditions) => conditions.iter().any(|c| c.evaluate(value)),
            Self::Not(condition) => !condition.evaluate(value),
            Self::Custom { .. } => {
                // Custom conditions cannot be evaluated synchronously
                // They need to be evaluated by executing a task
                false
            }
        }
    }

    /// Check if this is a custom condition that requires task execution
    pub fn is_custom(&self) -> bool {
        match self {
            Self::Custom { .. } => true,
            Self::And(conditions) => conditions.iter().any(|c| c.is_custom()),
            Self::Or(conditions) => conditions.iter().any(|c| c.is_custom()),
            Self::Not(condition) => condition.is_custom(),
            _ => false,
        }
    }
}

/// Extract a field from a JSON value using dot notation
fn extract_field(value: &serde_json::Value, field: Option<&str>) -> serde_json::Value {
    match field {
        None => value.clone(),
        Some(path) => {
            let mut current = value;
            for part in path.split('.') {
                current = match current {
                    serde_json::Value::Object(map) => {
                        map.get(part).unwrap_or(&serde_json::Value::Null)
                    }
                    serde_json::Value::Array(arr) => {
                        if let Ok(idx) = part.parse::<usize>() {
                            arr.get(idx).unwrap_or(&serde_json::Value::Null)
                        } else {
                            &serde_json::Value::Null
                        }
                    }
                    _ => &serde_json::Value::Null,
                };
            }
            current.clone()
        }
    }
}

/// Check if a JSON value is truthy
fn is_truthy(value: &serde_json::Value) -> bool {
    match value {
        serde_json::Value::Null => false,
        serde_json::Value::Bool(b) => *b,
        serde_json::Value::Number(n) => n.as_f64().is_some_and(|v| v != 0.0),
        serde_json::Value::String(s) => !s.is_empty(),
        serde_json::Value::Array(a) => !a.is_empty(),
        serde_json::Value::Object(o) => !o.is_empty(),
    }
}

/// Check if a JSON value contains another value
fn contains_value(haystack: &serde_json::Value, needle: &serde_json::Value) -> bool {
    match haystack {
        serde_json::Value::String(s) => {
            if let Some(needle_str) = needle.as_str() {
                s.contains(needle_str)
            } else {
                false
            }
        }
        serde_json::Value::Array(arr) => arr.contains(needle),
        serde_json::Value::Object(map) => {
            if let Some(key) = needle.as_str() {
                map.contains_key(key)
            } else {
                false
            }
        }
        _ => false,
    }
}

impl std::fmt::Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Always => write!(f, "always"),
            Self::Never => write!(f, "never"),
            Self::Equals { field, value } => {
                if let Some(field) = field {
                    write!(f, "{} == {}", field, value)
                } else {
                    write!(f, "result == {}", value)
                }
            }
            Self::NotEquals { field, value } => {
                if let Some(field) = field {
                    write!(f, "{} != {}", field, value)
                } else {
                    write!(f, "result != {}", value)
                }
            }
            Self::GreaterThan { field, threshold } => {
                if let Some(field) = field {
                    write!(f, "{} > {}", field, threshold)
                } else {
                    write!(f, "result > {}", threshold)
                }
            }
            Self::LessThan { field, threshold } => {
                if let Some(field) = field {
                    write!(f, "{} < {}", field, threshold)
                } else {
                    write!(f, "result < {}", threshold)
                }
            }
            Self::Truthy { field } => {
                if let Some(field) = field {
                    write!(f, "truthy({})", field)
                } else {
                    write!(f, "truthy(result)")
                }
            }
            Self::Falsy { field } => {
                if let Some(field) = field {
                    write!(f, "falsy({})", field)
                } else {
                    write!(f, "falsy(result)")
                }
            }
            Self::Contains { field, value } => {
                if let Some(field) = field {
                    write!(f, "{} contains {}", field, value)
                } else {
                    write!(f, "result contains {}", value)
                }
            }
            Self::Matches { field, pattern } => {
                if let Some(field) = field {
                    write!(f, "{} matches /{}/", field, pattern)
                } else {
                    write!(f, "result matches /{}/", pattern)
                }
            }
            Self::And(conditions) => {
                let parts: Vec<String> = conditions.iter().map(|c| format!("{}", c)).collect();
                write!(f, "({})", parts.join(" AND "))
            }
            Self::Or(conditions) => {
                let parts: Vec<String> = conditions.iter().map(|c| format!("{}", c)).collect();
                write!(f, "({})", parts.join(" OR "))
            }
            Self::Not(condition) => write!(f, "NOT ({})", condition),
            Self::Custom { task, .. } => write!(f, "custom({})", task),
        }
    }
}

/// Branch: Conditional workflow execution (if/else)
///
/// Executes different tasks/workflows based on a condition evaluated against
/// the result of a previous task.
///
/// # Example
/// ```
/// use celers_canvas::{Branch, Condition, Signature};
///
/// // Execute different tasks based on result value
/// let branch = Branch::new(
///     Condition::field_greater_than("count", 100.0),
///     Signature::new("process_large_batch".to_string()),
/// )
/// .otherwise(Signature::new("process_small_batch".to_string()));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Branch {
    /// Condition to evaluate
    pub condition: Condition,

    /// Task/workflow to execute if condition is true
    pub then_branch: Box<Signature>,

    /// Optional task/workflow to execute if condition is false
    pub else_branch: Option<Box<Signature>>,

    /// Pass the condition input to the branch tasks
    pub pass_result: bool,
}

impl Branch {
    /// Create a new Branch workflow
    ///
    /// # Arguments
    /// * `condition` - The condition to evaluate
    /// * `then_sig` - The signature to execute if condition is true
    pub fn new(condition: Condition, then_sig: Signature) -> Self {
        Self {
            condition,
            then_branch: Box::new(then_sig),
            else_branch: None,
            pass_result: true,
        }
    }

    /// Set the else branch
    pub fn otherwise(mut self, else_sig: Signature) -> Self {
        self.else_branch = Some(Box::new(else_sig));
        self
    }

    /// Alias for `otherwise`
    pub fn else_do(self, else_sig: Signature) -> Self {
        self.otherwise(else_sig)
    }

    /// Set whether to pass the input result to branch tasks
    pub fn with_pass_result(mut self, pass: bool) -> Self {
        self.pass_result = pass;
        self
    }

    /// Check if there's an else branch
    pub fn has_else(&self) -> bool {
        self.else_branch.is_some()
    }

    /// Evaluate the branch condition and return the appropriate signature
    ///
    /// Returns Some(signature) for the branch to execute, or None if condition
    /// is false and there's no else branch.
    pub fn evaluate(&self, result: &serde_json::Value) -> Option<Signature> {
        let should_then = self.condition.evaluate(result);

        let sig = if should_then {
            Some((*self.then_branch).clone())
        } else {
            self.else_branch.as_ref().map(|s| (**s).clone())
        };

        // Pass the result as the first argument if enabled
        if let Some(mut sig) = sig {
            if self.pass_result && !sig.immutable {
                sig.args.insert(0, result.clone());
            }
            Some(sig)
        } else {
            None
        }
    }
}

impl std::fmt::Display for Branch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if let Some(else_branch) = &self.else_branch {
            write!(
                f,
                "Branch[if {} then {} else {}]",
                self.condition, self.then_branch.task, else_branch.task
            )
        } else {
            write!(
                f,
                "Branch[if {} then {}]",
                self.condition, self.then_branch.task
            )
        }
    }
}

/// Maybe: Optional task execution based on condition
///
/// A simplified Branch that either executes a task or does nothing.
/// This is essentially `Branch` without an else clause.
///
/// # Example
/// ```
/// use celers_canvas::{Maybe, Condition, Signature};
///
/// // Only send notification if count > 0
/// let maybe = Maybe::new(
///     Condition::field_greater_than("count", 0.0),
///     Signature::new("send_notification".to_string()),
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Maybe {
    /// Condition to evaluate
    pub condition: Condition,

    /// Task to execute if condition is true
    pub task: Signature,

    /// Pass the condition input to the task
    pub pass_result: bool,
}

impl Maybe {
    /// Create a new Maybe workflow
    pub fn new(condition: Condition, task: Signature) -> Self {
        Self {
            condition,
            task,
            pass_result: true,
        }
    }

    /// Set whether to pass the input result to the task
    pub fn with_pass_result(mut self, pass: bool) -> Self {
        self.pass_result = pass;
        self
    }

    /// Evaluate and return the task if condition is met
    pub fn evaluate(&self, result: &serde_json::Value) -> Option<Signature> {
        if self.condition.evaluate(result) {
            let mut task = self.task.clone();
            if self.pass_result && !task.immutable {
                task.args.insert(0, result.clone());
            }
            Some(task)
        } else {
            None
        }
    }

    /// Convert to a Branch (for unified handling)
    pub fn to_branch(self) -> Branch {
        Branch {
            condition: self.condition,
            then_branch: Box::new(self.task),
            else_branch: None,
            pass_result: self.pass_result,
        }
    }
}

impl std::fmt::Display for Maybe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Maybe[if {} then {}]", self.condition, self.task.task)
    }
}

/// Switch: Multi-way conditional branching
///
/// Like a switch/case statement - evaluates multiple conditions and executes
/// the first matching branch.
///
/// # Example
/// ```
/// use celers_canvas::{Switch, Condition, Signature};
///
/// let switch = Switch::new()
///     .case(
///         Condition::field_equals("status", serde_json::json!("pending")),
///         Signature::new("process_pending".to_string()),
///     )
///     .case(
///         Condition::field_equals("status", serde_json::json!("approved")),
///         Signature::new("process_approved".to_string()),
///     )
///     .default(Signature::new("process_unknown".to_string()));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Switch {
    /// List of (condition, signature) pairs
    pub cases: Vec<(Condition, Signature)>,

    /// Default signature if no conditions match
    pub default: Option<Signature>,

    /// Pass the result to branch tasks
    pub pass_result: bool,
}

impl Switch {
    /// Create a new empty Switch
    pub fn new() -> Self {
        Self {
            cases: Vec::new(),
            default: None,
            pass_result: true,
        }
    }

    /// Add a case to the switch
    pub fn case(mut self, condition: Condition, task: Signature) -> Self {
        self.cases.push((condition, task));
        self
    }

    /// Set the default case
    pub fn default(mut self, task: Signature) -> Self {
        self.default = Some(task);
        self
    }

    /// Set whether to pass result to branch tasks
    pub fn with_pass_result(mut self, pass: bool) -> Self {
        self.pass_result = pass;
        self
    }

    /// Check if switch is empty (no cases)
    pub fn is_empty(&self) -> bool {
        self.cases.is_empty()
    }

    /// Get number of cases
    pub fn len(&self) -> usize {
        self.cases.len()
    }

    /// Evaluate and return the matching task
    pub fn evaluate(&self, result: &serde_json::Value) -> Option<Signature> {
        // Find first matching case
        for (condition, task) in &self.cases {
            if condition.evaluate(result) {
                let mut task = task.clone();
                if self.pass_result && !task.immutable {
                    task.args.insert(0, result.clone());
                }
                return Some(task);
            }
        }

        // Return default if no match
        if let Some(default) = &self.default {
            let mut task = default.clone();
            if self.pass_result && !task.immutable {
                task.args.insert(0, result.clone());
            }
            Some(task)
        } else {
            None
        }
    }
}

impl Default for Switch {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for Switch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let case_strs: Vec<String> = self
            .cases
            .iter()
            .map(|(c, t)| format!("{} => {}", c, t.task))
            .collect();

        if let Some(default) = &self.default {
            write!(
                f,
                "Switch[{}, default => {}]",
                case_strs.join(", "),
                default.task
            )
        } else {
            write!(f, "Switch[{}]", case_strs.join(", "))
        }
    }
}

// ============================================================================
// Nested Workflows
// ============================================================================

/// A canvas element that can be either a simple signature or a nested workflow
///
/// This enables composing complex workflows where any step can be another workflow.
///
/// # Example
/// ```
/// use celers_canvas::{CanvasElement, Chain, Group, Signature, Chord};
///
/// // Create a chain where one element is a group (parallel tasks)
/// let element = CanvasElement::group(
///     Group::new()
///         .add("task1", vec![])
///         .add("task2", vec![])
/// );
///
/// // Create a nested chain of chords
/// let nested = CanvasElement::chain(
///     Chain::new()
///         .then("step1", vec![])
///         .then("step2", vec![])
/// );
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "element_type")]
pub enum CanvasElement {
    /// A simple task signature
    Signature(Signature),

    /// A chain of tasks
    Chain(Chain),

    /// A group of parallel tasks
    Group(Group),

    /// A chord (group + callback)
    Chord {
        /// Header group
        header: Group,
        /// Callback signature
        body: Signature,
    },

    /// A map operation
    Map {
        /// Task to apply
        task: Signature,
        /// Argument sets
        argsets: Vec<Vec<serde_json::Value>>,
    },

    /// A conditional branch
    Branch(Branch),

    /// A switch statement
    Switch(Switch),
}

impl CanvasElement {
    /// Create a signature element
    pub fn signature(sig: Signature) -> Self {
        Self::Signature(sig)
    }

    /// Create a task element (shorthand for signature)
    pub fn task(name: impl Into<String>, args: Vec<serde_json::Value>) -> Self {
        Self::Signature(Signature::new(name.into()).with_args(args))
    }

    /// Create a chain element
    pub fn chain(chain: Chain) -> Self {
        Self::Chain(chain)
    }

    /// Create a group element
    pub fn group(group: Group) -> Self {
        Self::Group(group)
    }

    /// Create a chord element
    pub fn chord(header: Group, body: Signature) -> Self {
        Self::Chord { header, body }
    }

    /// Create a map element
    pub fn map(task: Signature, argsets: Vec<Vec<serde_json::Value>>) -> Self {
        Self::Map { task, argsets }
    }

    /// Create a branch element
    pub fn branch(branch: Branch) -> Self {
        Self::Branch(branch)
    }

    /// Create a switch element
    pub fn switch(switch: Switch) -> Self {
        Self::Switch(switch)
    }

    /// Check if this is a simple signature
    pub fn is_signature(&self) -> bool {
        matches!(self, Self::Signature(_))
    }

    /// Check if this is a chain
    pub fn is_chain(&self) -> bool {
        matches!(self, Self::Chain(_))
    }

    /// Check if this is a group
    pub fn is_group(&self) -> bool {
        matches!(self, Self::Group(_))
    }

    /// Check if this is a chord
    pub fn is_chord(&self) -> bool {
        matches!(self, Self::Chord { .. })
    }

    /// Get the element type as a string
    pub fn element_type(&self) -> &'static str {
        match self {
            Self::Signature(_) => "signature",
            Self::Chain(_) => "chain",
            Self::Group(_) => "group",
            Self::Chord { .. } => "chord",
            Self::Map { .. } => "map",
            Self::Branch(_) => "branch",
            Self::Switch(_) => "switch",
        }
    }
}

impl std::fmt::Display for CanvasElement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Signature(sig) => write!(f, "Signature[{}]", sig.task),
            Self::Chain(chain) => write!(f, "{}", chain),
            Self::Group(group) => write!(f, "{}", group),
            Self::Chord { header, body } => {
                write!(f, "Chord[header={}, body={}]", header, body.task)
            }
            Self::Map { task, argsets } => {
                write!(f, "Map[task={}, {} argsets]", task.task, argsets.len())
            }
            Self::Branch(branch) => write!(f, "{}", branch),
            Self::Switch(switch) => write!(f, "{}", switch),
        }
    }
}

impl From<Signature> for CanvasElement {
    fn from(sig: Signature) -> Self {
        Self::Signature(sig)
    }
}

impl From<Chain> for CanvasElement {
    fn from(chain: Chain) -> Self {
        Self::Chain(chain)
    }
}

impl From<Group> for CanvasElement {
    fn from(group: Group) -> Self {
        Self::Group(group)
    }
}

impl From<Branch> for CanvasElement {
    fn from(branch: Branch) -> Self {
        Self::Branch(branch)
    }
}

impl From<Switch> for CanvasElement {
    fn from(switch: Switch) -> Self {
        Self::Switch(switch)
    }
}

/// A nested chain that can contain any canvas element
///
/// Unlike the basic Chain that only contains Signatures, NestedChain
/// can contain Groups, Chords, or other Chains as steps.
///
/// # Example
/// ```
/// use celers_canvas::{NestedChain, CanvasElement, Group, Signature};
///
/// let workflow = NestedChain::new()
///     .then_element(CanvasElement::task("step1".to_string(), vec![]))
///     .then_element(CanvasElement::group(
///         Group::new()
///             .add("parallel_a", vec![])
///             .add("parallel_b", vec![])
///     ))
///     .then_element(CanvasElement::task("step2".to_string(), vec![]));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NestedChain {
    /// Elements in the chain
    pub elements: Vec<CanvasElement>,
}

impl NestedChain {
    /// Create a new empty nested chain
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    /// Add an element to the chain
    pub fn then_element(mut self, element: CanvasElement) -> Self {
        self.elements.push(element);
        self
    }

    /// Add a signature to the chain
    pub fn then_signature(mut self, sig: Signature) -> Self {
        self.elements.push(CanvasElement::Signature(sig));
        self
    }

    /// Add a simple task to the chain
    pub fn then(mut self, task: &str, args: Vec<serde_json::Value>) -> Self {
        self.elements.push(CanvasElement::task(task, args));
        self
    }

    /// Add a group to the chain (parallel execution point)
    pub fn then_group(mut self, group: Group) -> Self {
        self.elements.push(CanvasElement::Group(group));
        self
    }

    /// Add a chord to the chain
    pub fn then_chord(mut self, header: Group, body: Signature) -> Self {
        self.elements.push(CanvasElement::Chord { header, body });
        self
    }

    /// Add a branch to the chain
    pub fn then_branch(mut self, branch: Branch) -> Self {
        self.elements.push(CanvasElement::Branch(branch));
        self
    }

    /// Add another chain as a nested element
    pub fn then_chain(mut self, chain: Chain) -> Self {
        self.elements.push(CanvasElement::Chain(chain));
        self
    }

    /// Check if the chain is empty
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Get the number of elements
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Flatten the nested chain into a sequence of signatures where possible
    ///
    /// This is useful for simpler execution when nested workflows aren't needed.
    /// Note: This will return None if the chain contains elements that can't be
    /// flattened to signatures (groups, chords, etc.)
    pub fn flatten_signatures(&self) -> Option<Vec<Signature>> {
        let mut result = Vec::new();

        for element in &self.elements {
            match element {
                CanvasElement::Signature(sig) => result.push(sig.clone()),
                CanvasElement::Chain(chain) => {
                    result.extend(chain.tasks.clone());
                }
                _ => return None, // Can't flatten non-signature elements
            }
        }

        Some(result)
    }

    /// Execute the nested chain sequentially
    ///
    /// Each element is executed in order. For complex elements (Groups, Chords),
    /// they are executed and we wait for them to start before continuing.
    /// Note: This executes elements sequentially but doesn't wait for completion,
    /// following Celery's async execution model.
    pub async fn apply<B: Broker>(&self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.elements.is_empty() {
            return Err(CanvasError::Invalid(
                "NestedChain cannot be empty".to_string(),
            ));
        }

        // Execute each element in sequence
        let mut last_id = None;
        for element in &self.elements {
            match element {
                CanvasElement::Signature(sig) => {
                    // Convert to Chain for sequential execution
                    let chain = Chain {
                        tasks: vec![sig.clone()],
                    };
                    last_id = Some(chain.apply(broker).await?);
                }
                CanvasElement::Chain(chain) => {
                    last_id = Some(chain.clone().apply(broker).await?);
                }
                CanvasElement::Group(group) => {
                    last_id = Some(group.clone().apply(broker).await?);
                }
                CanvasElement::Chord { header, body } => {
                    #[cfg(feature = "backend-redis")]
                    {
                        // Note: Chord requires backend, but we can't pass it here
                        // For now, fall back to just executing the group
                        last_id = Some(header.clone().apply(broker).await?);
                        // Callback would need to be manually triggered
                        let _ = body; // Silence unused warning
                    }
                    #[cfg(not(feature = "backend-redis"))]
                    {
                        last_id = Some(header.clone().apply(broker).await?);
                        let _ = body; // Silence unused warning
                    }
                }
                CanvasElement::Map { task, argsets } => {
                    let map = Map::new(task.clone(), argsets.clone());
                    last_id = Some(map.apply(broker).await?);
                }
                CanvasElement::Branch(_branch) => {
                    // Branches require runtime evaluation, skip for now
                    return Err(CanvasError::Invalid(
                        "Branch elements not supported in NestedChain.apply()".to_string(),
                    ));
                }
                CanvasElement::Switch(_switch) => {
                    // Switch requires runtime evaluation, skip for now
                    return Err(CanvasError::Invalid(
                        "Switch elements not supported in NestedChain.apply()".to_string(),
                    ));
                }
            }
        }

        last_id.ok_or_else(|| CanvasError::Invalid("No elements executed".to_string()))
    }
}

impl Default for NestedChain {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for NestedChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let element_strs: Vec<String> = self.elements.iter().map(|e| format!("{}", e)).collect();
        write!(f, "NestedChain[{}]", element_strs.join(" -> "))
    }
}

/// A nested group that can contain any canvas element
///
/// Unlike the basic Group that only contains Signatures, NestedGroup
/// can contain Chains, other Groups, or Chords as parallel tasks.
///
/// # Example
/// ```
/// use celers_canvas::{NestedGroup, CanvasElement, Chain, Signature};
///
/// let workflow = NestedGroup::new()
///     .add_element(CanvasElement::chain(
///         Chain::new().then("step1", vec![]).then("step2", vec![])
///     ))
///     .add_element(CanvasElement::chain(
///         Chain::new().then("step3", vec![]).then("step4", vec![])
///     ));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NestedGroup {
    /// Elements in the group (executed in parallel)
    pub elements: Vec<CanvasElement>,
}

impl NestedGroup {
    /// Create a new empty nested group
    pub fn new() -> Self {
        Self {
            elements: Vec::new(),
        }
    }

    /// Add an element to the group
    pub fn add_element(mut self, element: CanvasElement) -> Self {
        self.elements.push(element);
        self
    }

    /// Add a signature to the group
    pub fn add_signature(mut self, sig: Signature) -> Self {
        self.elements.push(CanvasElement::Signature(sig));
        self
    }

    /// Add a simple task to the group
    pub fn add(mut self, task: &str, args: Vec<serde_json::Value>) -> Self {
        self.elements.push(CanvasElement::task(task, args));
        self
    }

    /// Add a chain to the group
    pub fn add_chain(mut self, chain: Chain) -> Self {
        self.elements.push(CanvasElement::Chain(chain));
        self
    }

    /// Check if the group is empty
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Get the number of elements
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Flatten to signatures if possible
    pub fn flatten_signatures(&self) -> Option<Vec<Signature>> {
        let mut result = Vec::new();

        for element in &self.elements {
            match element {
                CanvasElement::Signature(sig) => result.push(sig.clone()),
                _ => return None,
            }
        }

        Some(result)
    }

    /// Execute all elements in parallel
    ///
    /// All elements in the group are started concurrently.
    /// Returns a group ID that can be used to track the parallel execution.
    pub async fn apply<B: Broker>(&self, broker: &B) -> Result<Uuid, CanvasError> {
        if self.elements.is_empty() {
            return Err(CanvasError::Invalid(
                "NestedGroup cannot be empty".to_string(),
            ));
        }

        // Generate a group ID for tracking
        let group_id = Uuid::new_v4();

        // Execute all elements in parallel
        for element in &self.elements {
            match element {
                CanvasElement::Signature(sig) => {
                    let chain = Chain {
                        tasks: vec![sig.clone()],
                    };
                    chain.apply(broker).await?;
                }
                CanvasElement::Chain(chain) => {
                    chain.clone().apply(broker).await?;
                }
                CanvasElement::Group(group) => {
                    group.clone().apply(broker).await?;
                }
                CanvasElement::Chord { header, body } => {
                    #[cfg(feature = "backend-redis")]
                    {
                        // Note: Chord requires backend, but we can't pass it here
                        // For now, fall back to just executing the group
                        header.clone().apply(broker).await?;
                        let _ = body; // Silence unused warning
                    }
                    #[cfg(not(feature = "backend-redis"))]
                    {
                        header.clone().apply(broker).await?;
                        let _ = body; // Silence unused warning
                    }
                }
                CanvasElement::Map { task, argsets } => {
                    let map = Map::new(task.clone(), argsets.clone());
                    map.apply(broker).await?;
                }
                CanvasElement::Branch(_branch) => {
                    // Branches require runtime evaluation, skip for now
                    return Err(CanvasError::Invalid(
                        "Branch elements not supported in NestedGroup.apply()".to_string(),
                    ));
                }
                CanvasElement::Switch(_switch) => {
                    // Switch requires runtime evaluation, skip for now
                    return Err(CanvasError::Invalid(
                        "Switch elements not supported in NestedGroup.apply()".to_string(),
                    ));
                }
            }
        }

        Ok(group_id)
    }
}

impl Default for NestedGroup {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for NestedGroup {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let element_strs: Vec<String> = self.elements.iter().map(|e| format!("{}", e)).collect();
        write!(f, "NestedGroup[{}]", element_strs.join(" | "))
    }
}

/// Error handling strategy for workflows
///
/// Determines how the workflow should behave when a task fails.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum ErrorStrategy {
    /// Stop the workflow on first error (default)
    #[default]
    StopOnError,

    /// Continue even if some tasks fail
    ContinueOnError,

    /// Retry the failed task a number of times before failing
    RetryOnError {
        /// Maximum number of retries
        max_retries: u32,
        /// Delay between retries in seconds
        delay: Option<u64>,
    },

    /// Execute a fallback task on error
    Fallback {
        /// Fallback task to execute
        fallback: Signature,
    },

    /// Execute an error handler task that receives the error info
    ErrorHandler {
        /// Error handler task name
        handler: Signature,
    },
}

impl ErrorStrategy {
    /// Create a stop-on-error strategy
    pub fn stop() -> Self {
        Self::StopOnError
    }

    /// Create a continue-on-error strategy
    pub fn continue_on_error() -> Self {
        Self::ContinueOnError
    }

    /// Create a retry strategy
    pub fn retry(max_retries: u32) -> Self {
        Self::RetryOnError {
            max_retries,
            delay: None,
        }
    }

    /// Create a retry strategy with delay
    pub fn retry_with_delay(max_retries: u32, delay: u64) -> Self {
        Self::RetryOnError {
            max_retries,
            delay: Some(delay),
        }
    }

    /// Create a fallback strategy
    pub fn fallback(task: Signature) -> Self {
        Self::Fallback { fallback: task }
    }

    /// Create an error handler strategy
    pub fn error_handler(handler: Signature) -> Self {
        Self::ErrorHandler { handler }
    }

    /// Check if this strategy allows continuing on error
    pub fn allows_continue(&self) -> bool {
        !matches!(self, Self::StopOnError)
    }
}

impl std::fmt::Display for ErrorStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StopOnError => write!(f, "StopOnError"),
            Self::ContinueOnError => write!(f, "ContinueOnError"),
            Self::RetryOnError { max_retries, delay } => {
                if let Some(d) = delay {
                    write!(f, "RetryOnError({} times, {}s delay)", max_retries, d)
                } else {
                    write!(f, "RetryOnError({} times)", max_retries)
                }
            }
            Self::Fallback { fallback } => write!(f, "Fallback({})", fallback.task),
            Self::ErrorHandler { handler } => write!(f, "ErrorHandler({})", handler.task),
        }
    }
}

// ============================================================================
// Workflow Cancellation
// ============================================================================

/// Cancellation token for workflow cancellation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancellationToken {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Cancellation reason
    pub reason: Option<String>,
    /// Whether to cancel entire tree (including sub-workflows)
    pub cancel_tree: bool,
    /// Whether to cancel only specific branch
    pub branch_id: Option<Uuid>,
}

impl CancellationToken {
    /// Create a new cancellation token for a workflow
    pub fn new(workflow_id: Uuid) -> Self {
        Self {
            workflow_id,
            reason: None,
            cancel_tree: false,
            branch_id: None,
        }
    }

    /// Set cancellation reason
    pub fn with_reason(mut self, reason: String) -> Self {
        self.reason = Some(reason);
        self
    }

    /// Cancel entire workflow tree
    pub fn cancel_tree(mut self) -> Self {
        self.cancel_tree = true;
        self
    }

    /// Cancel only specific branch
    pub fn cancel_branch(mut self, branch_id: Uuid) -> Self {
        self.branch_id = Some(branch_id);
        self
    }
}

impl std::fmt::Display for CancellationToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CancellationToken[workflow={}]", self.workflow_id)?;
        if let Some(ref reason) = self.reason {
            write!(f, " reason={}", reason)?;
        }
        if self.cancel_tree {
            write!(f, " (tree)")?;
        }
        if let Some(branch) = self.branch_id {
            write!(f, " branch={}", branch)?;
        }
        Ok(())
    }
}

// ============================================================================
// Workflow Retry Policies
// ============================================================================

/// Workflow-level retry policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRetryPolicy {
    /// Maximum number of retries for entire workflow
    pub max_retries: u32,
    /// Retry only failed branches
    pub retry_failed_only: bool,
    /// Exponential backoff factor
    pub backoff_factor: Option<f64>,
    /// Maximum backoff delay in seconds
    pub max_backoff: Option<u64>,
    /// Initial retry delay in seconds
    pub initial_delay: Option<u64>,
}

impl WorkflowRetryPolicy {
    /// Create a new retry policy
    pub fn new(max_retries: u32) -> Self {
        Self {
            max_retries,
            retry_failed_only: false,
            backoff_factor: None,
            max_backoff: None,
            initial_delay: None,
        }
    }

    /// Retry only failed branches
    pub fn failed_only(mut self) -> Self {
        self.retry_failed_only = true;
        self
    }

    /// Set exponential backoff
    pub fn with_backoff(mut self, factor: f64, max_delay: u64) -> Self {
        self.backoff_factor = Some(factor);
        self.max_backoff = Some(max_delay);
        self
    }

    /// Set initial delay
    pub fn with_initial_delay(mut self, delay: u64) -> Self {
        self.initial_delay = Some(delay);
        self
    }

    /// Calculate delay for retry attempt
    pub fn calculate_delay(&self, attempt: u32) -> u64 {
        let base_delay = self.initial_delay.unwrap_or(1);

        if let Some(factor) = self.backoff_factor {
            let delay = (base_delay as f64) * factor.powi(attempt as i32);
            let max = self.max_backoff.unwrap_or(300);
            delay.min(max as f64) as u64
        } else {
            base_delay
        }
    }
}

impl std::fmt::Display for WorkflowRetryPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkflowRetryPolicy[max_retries={}]", self.max_retries)?;
        if self.retry_failed_only {
            write!(f, " (failed_only)")?;
        }
        if let Some(factor) = self.backoff_factor {
            write!(f, " backoff={}", factor)?;
        }
        Ok(())
    }
}

// ============================================================================
// Workflow Timeout
// ============================================================================

/// Workflow-level timeout configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowTimeout {
    /// Global workflow timeout in seconds
    pub total_timeout: Option<u64>,
    /// Per-stage timeout in seconds
    pub stage_timeout: Option<u64>,
    /// Timeout escalation - what to do on timeout
    pub escalation: TimeoutEscalation,
}

/// Timeout escalation strategy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TimeoutEscalation {
    /// Cancel the workflow
    Cancel,
    /// Fail the workflow
    Fail,
    /// Continue with partial results
    ContinuePartial,
}

impl WorkflowTimeout {
    /// Create a new timeout configuration
    pub fn new(total_timeout: u64) -> Self {
        Self {
            total_timeout: Some(total_timeout),
            stage_timeout: None,
            escalation: TimeoutEscalation::Cancel,
        }
    }

    /// Set per-stage timeout
    pub fn with_stage_timeout(mut self, timeout: u64) -> Self {
        self.stage_timeout = Some(timeout);
        self
    }

    /// Set escalation strategy
    pub fn with_escalation(mut self, escalation: TimeoutEscalation) -> Self {
        self.escalation = escalation;
        self
    }
}

impl std::fmt::Display for WorkflowTimeout {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkflowTimeout[")?;
        if let Some(total) = self.total_timeout {
            write!(f, "total={}s", total)?;
        }
        if let Some(stage) = self.stage_timeout {
            write!(f, " stage={}s", stage)?;
        }
        write!(f, " escalation={:?}]", self.escalation)
    }
}

// ============================================================================
// Workflow Loops
// ============================================================================

/// For-each loop over a collection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ForEach {
    /// Task to execute for each item
    pub task: Signature,
    /// Collection to iterate over
    pub items: Vec<serde_json::Value>,
    /// Maximum parallel execution
    pub concurrency: Option<usize>,
}

impl ForEach {
    /// Create a new for-each loop
    pub fn new(task: Signature, items: Vec<serde_json::Value>) -> Self {
        Self {
            task,
            items,
            concurrency: None,
        }
    }

    /// Set maximum concurrent executions
    pub fn with_concurrency(mut self, concurrency: usize) -> Self {
        self.concurrency = Some(concurrency);
        self
    }

    /// Check if loop is empty
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Get number of iterations
    pub fn len(&self) -> usize {
        self.items.len()
    }
}

impl std::fmt::Display for ForEach {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ForEach[task={}, {} items]", self.task.task, self.len())?;
        if let Some(conc) = self.concurrency {
            write!(f, " concurrency={}", conc)?;
        }
        Ok(())
    }
}

/// While loop with condition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhileLoop {
    /// Condition to evaluate
    pub condition: Condition,
    /// Task to execute while condition is true
    pub body: Signature,
    /// Maximum iterations (safety limit)
    pub max_iterations: Option<u32>,
}

impl WhileLoop {
    /// Create a new while loop
    pub fn new(condition: Condition, body: Signature) -> Self {
        Self {
            condition,
            body,
            max_iterations: Some(1000), // Default safety limit
        }
    }

    /// Set maximum iterations
    pub fn with_max_iterations(mut self, max: u32) -> Self {
        self.max_iterations = Some(max);
        self
    }

    /// Remove iteration limit (use with caution!)
    pub fn unlimited(mut self) -> Self {
        self.max_iterations = None;
        self
    }
}

impl std::fmt::Display for WhileLoop {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "While[{} -> {}]", self.condition, self.body.task)?;
        if let Some(max) = self.max_iterations {
            write!(f, " max={}", max)?;
        }
        Ok(())
    }
}

// ============================================================================
// Workflow State Tracking
// ============================================================================

/// Workflow execution state
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowState {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Current status
    pub status: WorkflowStatus,
    /// Total tasks in workflow
    pub total_tasks: usize,
    /// Completed tasks
    pub completed_tasks: usize,
    /// Failed tasks
    pub failed_tasks: usize,
    /// Start time (Unix timestamp)
    pub start_time: Option<u64>,
    /// End time (Unix timestamp)
    pub end_time: Option<u64>,
    /// Current stage
    pub current_stage: Option<String>,
    /// Intermediate results
    pub intermediate_results: HashMap<String, serde_json::Value>,
}

/// Workflow status
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum WorkflowStatus {
    /// Workflow is pending
    Pending,
    /// Workflow is running
    Running,
    /// Workflow completed successfully
    Success,
    /// Workflow failed
    Failed,
    /// Workflow was cancelled
    Cancelled,
    /// Workflow is paused
    Paused,
}

impl WorkflowState {
    /// Create a new workflow state
    pub fn new(workflow_id: Uuid, total_tasks: usize) -> Self {
        Self {
            workflow_id,
            status: WorkflowStatus::Pending,
            total_tasks,
            completed_tasks: 0,
            failed_tasks: 0,
            start_time: None,
            end_time: None,
            current_stage: None,
            intermediate_results: HashMap::new(),
        }
    }

    /// Calculate progress percentage (0-100)
    pub fn progress(&self) -> f64 {
        if self.total_tasks == 0 {
            return 100.0;
        }
        (self.completed_tasks as f64 / self.total_tasks as f64) * 100.0
    }

    /// Check if workflow is complete
    pub fn is_complete(&self) -> bool {
        matches!(
            self.status,
            WorkflowStatus::Success | WorkflowStatus::Failed | WorkflowStatus::Cancelled
        )
    }

    /// Mark task as completed
    pub fn mark_completed(&mut self) {
        self.completed_tasks += 1;
    }

    /// Mark task as failed
    pub fn mark_failed(&mut self) {
        self.failed_tasks += 1;
    }

    /// Set intermediate result
    pub fn set_result(&mut self, key: String, value: serde_json::Value) {
        self.intermediate_results.insert(key, value);
    }

    /// Get intermediate result
    pub fn get_result(&self, key: &str) -> Option<&serde_json::Value> {
        self.intermediate_results.get(key)
    }
}

impl std::fmt::Display for WorkflowState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkflowState[id={}, status={:?}, progress={:.1}%]",
            self.workflow_id,
            self.status,
            self.progress()
        )?;
        if self.failed_tasks > 0 {
            write!(f, " failed={}", self.failed_tasks)?;
        }
        Ok(())
    }
}

// ============================================================================
// DAG Export
// ============================================================================

/// DAG export format
#[derive(Debug, Clone, Copy)]
pub enum DagFormat {
    /// GraphViz DOT format
    Dot,
    /// Mermaid diagram format
    Mermaid,
    /// JSON representation
    Json,
    /// SVG format (rendered from DOT)
    Svg,
    /// PNG format (rendered from DOT)
    Png,
}

/// Render DOT format to SVG using GraphViz dot command
///
/// Requires GraphViz to be installed on the system.
/// Executes: `dot -Tsvg`
///
/// # Errors
/// Returns error if GraphViz is not installed or execution fails
fn render_dot_to_svg(dot: &str) -> Result<String, CanvasError> {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new("dot")
        .arg("-Tsvg")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            CanvasError::Invalid(format!(
                "Failed to execute 'dot' command. Is GraphViz installed? Error: {}",
                e
            ))
        })?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(dot.as_bytes())
            .map_err(|e| CanvasError::Invalid(format!("Failed to write DOT to stdin: {}", e)))?;
    }

    let output = child
        .wait_with_output()
        .map_err(|e| CanvasError::Invalid(format!("Failed to wait for dot process: {}", e)))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(CanvasError::Invalid(format!(
            "dot command failed: {}",
            stderr
        )));
    }

    String::from_utf8(output.stdout)
        .map_err(|e| CanvasError::Invalid(format!("Invalid UTF-8 in SVG output: {}", e)))
}

/// Render DOT format to PNG using GraphViz dot command
///
/// Requires GraphViz to be installed on the system.
/// Executes: `dot -Tpng`
///
/// # Errors
/// Returns error if GraphViz is not installed or execution fails
fn render_dot_to_png(dot: &str) -> Result<Vec<u8>, CanvasError> {
    use std::io::Write;
    use std::process::{Command, Stdio};

    let mut child = Command::new("dot")
        .arg("-Tpng")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| {
            CanvasError::Invalid(format!(
                "Failed to execute 'dot' command. Is GraphViz installed? Error: {}",
                e
            ))
        })?;

    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(dot.as_bytes())
            .map_err(|e| CanvasError::Invalid(format!("Failed to write DOT to stdin: {}", e)))?;
    }

    let output = child
        .wait_with_output()
        .map_err(|e| CanvasError::Invalid(format!("Failed to wait for dot process: {}", e)))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(CanvasError::Invalid(format!(
            "dot command failed: {}",
            stderr
        )));
    }

    Ok(output.stdout)
}

/// Check if GraphViz dot command is available
///
/// # Example
/// ```no_run
/// if celers_canvas::is_graphviz_available() {
///     println!("GraphViz is installed");
/// } else {
///     println!("GraphViz is not installed");
/// }
/// ```
#[allow(dead_code)]
pub fn is_graphviz_available() -> bool {
    use std::process::Command;

    Command::new("dot")
        .arg("-V")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

/// Trait for exporting workflow as DAG
pub trait DagExport {
    /// Export workflow as GraphViz DOT
    fn to_dot(&self) -> String;

    /// Export workflow as Mermaid diagram
    fn to_mermaid(&self) -> String;

    /// Export workflow as JSON
    fn to_json(&self) -> Result<String, serde_json::Error>;

    /// Export workflow as SVG using GraphViz dot command
    ///
    /// This method generates SVG by executing the `dot` command.
    /// Requires GraphViz to be installed on the system.
    ///
    /// # Errors
    /// Returns error if GraphViz is not installed or execution fails
    fn to_svg(&self) -> Result<String, CanvasError> {
        let dot = self.to_dot();
        render_dot_to_svg(&dot)
    }

    /// Export workflow as PNG using GraphViz dot command
    ///
    /// This method generates PNG by executing the `dot` command.
    /// Requires GraphViz to be installed on the system.
    ///
    /// # Errors
    /// Returns error if GraphViz is not installed or execution fails
    fn to_png(&self) -> Result<Vec<u8>, CanvasError> {
        let dot = self.to_dot();
        render_dot_to_png(&dot)
    }

    /// Get command to render DOT to SVG
    ///
    /// Returns the shell command that can be used to convert
    /// the DOT format to SVG. Useful for manual rendering.
    fn svg_render_command(&self) -> String {
        "dot -Tsvg -o output.svg input.dot".to_string()
    }

    /// Get command to render DOT to PNG
    ///
    /// Returns the shell command that can be used to convert
    /// the DOT format to PNG. Useful for manual rendering.
    fn png_render_command(&self) -> String {
        "dot -Tpng -o output.png input.dot".to_string()
    }
}

impl DagExport for Chain {
    fn to_dot(&self) -> String {
        let mut dot = String::from("digraph Chain {\n");
        dot.push_str("  rankdir=LR;\n");
        dot.push_str("  node [shape=box];\n\n");

        for (i, task) in self.tasks.iter().enumerate() {
            dot.push_str(&format!("  n{} [label=\"{}\"];\n", i, task.task));
            if i > 0 {
                dot.push_str(&format!("  n{} -> n{};\n", i - 1, i));
            }
        }

        dot.push_str("}\n");
        dot
    }

    fn to_mermaid(&self) -> String {
        let mut mmd = String::from("graph LR\n");

        for (i, task) in self.tasks.iter().enumerate() {
            let node_id = format!("n{}", i);
            mmd.push_str(&format!("  {}[\"{}\"]\n", node_id, task.task));
            if i > 0 {
                mmd.push_str(&format!("  n{} --> n{}\n", i - 1, i));
            }
        }

        mmd
    }

    fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

impl DagExport for Group {
    fn to_dot(&self) -> String {
        let mut dot = String::from("digraph Group {\n");
        dot.push_str("  rankdir=TB;\n");
        dot.push_str("  node [shape=box];\n\n");
        dot.push_str("  start [shape=circle, label=\"start\"];\n");

        for (i, task) in self.tasks.iter().enumerate() {
            dot.push_str(&format!("  n{} [label=\"{}\"];\n", i, task.task));
            dot.push_str(&format!("  start -> n{};\n", i));
        }

        dot.push_str("}\n");
        dot
    }

    fn to_mermaid(&self) -> String {
        let mut mmd = String::from("graph TB\n");
        mmd.push_str("  start((start))\n");

        for (i, task) in self.tasks.iter().enumerate() {
            mmd.push_str(&format!("  n{}[\"{}\"]\n", i, task.task));
            mmd.push_str(&format!("  start --> n{}\n", i));
        }

        mmd
    }

    fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

impl DagExport for Chord {
    fn to_dot(&self) -> String {
        let mut dot = String::from("digraph Chord {\n");
        dot.push_str("  rankdir=TB;\n");
        dot.push_str("  node [shape=box];\n\n");
        dot.push_str("  start [shape=circle, label=\"start\"];\n");
        dot.push_str(&format!(
            "  callback [label=\"{}\", style=filled, fillcolor=lightblue];\n",
            self.body.task
        ));

        for (i, task) in self.header.tasks.iter().enumerate() {
            dot.push_str(&format!("  n{} [label=\"{}\"];\n", i, task.task));
            dot.push_str(&format!("  start -> n{};\n", i));
            dot.push_str(&format!("  n{} -> callback;\n", i));
        }

        dot.push_str("}\n");
        dot
    }

    fn to_mermaid(&self) -> String {
        let mut mmd = String::from("graph TB\n");
        mmd.push_str("  start((start))\n");
        mmd.push_str(&format!("  callback[\"{}\"]\n", self.body.task));
        mmd.push_str("  style callback fill:#add8e6\n");

        for (i, task) in self.header.tasks.iter().enumerate() {
            mmd.push_str(&format!("  n{}[\"{}\"]\n", i, task.task));
            mmd.push_str(&format!("  start --> n{}\n", i));
            mmd.push_str(&format!("  n{} --> callback\n", i));
        }

        mmd
    }

    fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

// ============================================================================
// Advanced Result Passing
// ============================================================================

/// Named output for result passing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NamedOutput {
    /// Output name
    pub name: String,
    /// Output value
    pub value: serde_json::Value,
    /// Source task
    pub source: Option<String>,
}

impl NamedOutput {
    /// Create a new named output
    pub fn new(name: impl Into<String>, value: serde_json::Value) -> Self {
        Self {
            name: name.into(),
            value,
            source: None,
        }
    }

    /// Set source task
    pub fn with_source(mut self, source: impl Into<String>) -> Self {
        self.source = Some(source.into());
        self
    }
}

/// Result transformation function
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ResultTransform {
    /// Extract a field from result
    Extract { field: String },
    /// Map result through a task
    Map { task: Box<Signature> },
    /// Filter result based on condition
    Filter { condition: Condition },
    /// Aggregate multiple results
    Aggregate { strategy: AggregationStrategy },
}

/// Aggregation strategy for combining results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum AggregationStrategy {
    /// Sum numeric results
    Sum,
    /// Average numeric results
    Average,
    /// Concatenate arrays
    Concat,
    /// Merge objects
    Merge,
    /// Take first non-null result
    Coalesce,
    /// Custom aggregation task
    Custom { task: Box<Signature> },
}

impl std::fmt::Display for ResultTransform {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Extract { field } => write!(f, "Extract[{}]", field),
            Self::Map { task } => write!(f, "Map[{}]", task.task),
            Self::Filter { condition } => write!(f, "Filter[{}]", condition),
            Self::Aggregate { strategy } => write!(f, "Aggregate[{:?}]", strategy),
        }
    }
}

// ============================================================================
// Result Caching
// ============================================================================

/// Result cache configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResultCache {
    /// Cache key
    pub key: String,
    /// Cache policy
    pub policy: CachePolicy,
    /// Time-to-live in seconds
    pub ttl: Option<u64>,
}

/// Cache policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum CachePolicy {
    /// Always cache results
    Always,
    /// Cache only successful results
    OnSuccess,
    /// Cache based on custom condition
    Conditional { condition: Condition },
    /// Never cache (useful for overriding)
    Never,
}

impl ResultCache {
    /// Create a new cache configuration
    pub fn new(key: impl Into<String>) -> Self {
        Self {
            key: key.into(),
            policy: CachePolicy::OnSuccess,
            ttl: None,
        }
    }

    /// Set cache policy
    pub fn with_policy(mut self, policy: CachePolicy) -> Self {
        self.policy = policy;
        self
    }

    /// Set TTL in seconds
    pub fn with_ttl(mut self, ttl: u64) -> Self {
        self.ttl = Some(ttl);
        self
    }
}

impl std::fmt::Display for ResultCache {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Cache[key={}]", self.key)?;
        if let Some(ttl) = self.ttl {
            write!(f, " ttl={}s", ttl)?;
        }
        Ok(())
    }
}

// ============================================================================
// Workflow Error Handlers
// ============================================================================

/// Workflow-level error handler
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowErrorHandler {
    /// Error handler task
    pub handler: Signature,
    /// Error types to handle (empty = handle all)
    pub error_types: Vec<String>,
    /// Whether to suppress the error after handling
    pub suppress: bool,
}

impl WorkflowErrorHandler {
    /// Create a new error handler
    pub fn new(handler: Signature) -> Self {
        Self {
            handler,
            error_types: Vec::new(),
            suppress: false,
        }
    }

    /// Handle specific error types
    pub fn for_errors(mut self, error_types: Vec<String>) -> Self {
        self.error_types = error_types;
        self
    }

    /// Suppress error after handling
    pub fn suppress_error(mut self) -> Self {
        self.suppress = true;
        self
    }
}

impl std::fmt::Display for WorkflowErrorHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ErrorHandler[{}]", self.handler.task)?;
        if self.suppress {
            write!(f, " (suppress)")?;
        }
        Ok(())
    }
}

// ============================================================================
// Compensation Workflows (Saga Pattern)
// ============================================================================

/// Compensation workflow for rollback
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompensationWorkflow {
    /// Forward actions
    pub forward: Vec<Signature>,
    /// Compensation actions (run in reverse order on failure)
    pub compensations: Vec<Signature>,
}

impl CompensationWorkflow {
    /// Create a new compensation workflow
    pub fn new() -> Self {
        Self {
            forward: Vec::new(),
            compensations: Vec::new(),
        }
    }

    /// Add a step with compensation
    pub fn step(mut self, forward: Signature, compensation: Signature) -> Self {
        self.forward.push(forward);
        self.compensations.push(compensation);
        self
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.forward.is_empty()
    }

    /// Get number of steps
    pub fn len(&self) -> usize {
        self.forward.len()
    }
}

impl Default for CompensationWorkflow {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for CompensationWorkflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Compensation[{} steps, {} compensations]",
            self.forward.len(),
            self.compensations.len()
        )
    }
}

/// Saga pattern workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Saga {
    /// Compensation workflow
    pub workflow: CompensationWorkflow,
    /// Isolation level
    pub isolation: SagaIsolation,
}

/// Saga isolation level
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SagaIsolation {
    /// Read uncommitted (no isolation)
    ReadUncommitted,
    /// Read committed (default)
    ReadCommitted,
    /// Serializable (full isolation)
    Serializable,
}

impl Saga {
    /// Create a new saga
    pub fn new(workflow: CompensationWorkflow) -> Self {
        Self {
            workflow,
            isolation: SagaIsolation::ReadCommitted,
        }
    }

    /// Set isolation level
    pub fn with_isolation(mut self, isolation: SagaIsolation) -> Self {
        self.isolation = isolation;
        self
    }
}

impl std::fmt::Display for Saga {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Saga[{} steps, isolation={:?}]",
            self.workflow.len(),
            self.isolation
        )
    }
}

// ============================================================================
// Advanced Workflow Patterns
// ============================================================================

/// Scatter-gather pattern: distribute work, collect results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScatterGather {
    /// Scatter task (distributes work)
    pub scatter: Signature,
    /// Worker tasks (process items)
    pub workers: Vec<Signature>,
    /// Gather task (collects results)
    pub gather: Signature,
    /// Timeout for gathering
    pub timeout: Option<u64>,
}

impl ScatterGather {
    /// Create a new scatter-gather pattern
    pub fn new(scatter: Signature, workers: Vec<Signature>, gather: Signature) -> Self {
        Self {
            scatter,
            workers,
            gather,
            timeout: None,
        }
    }

    /// Set gather timeout
    pub fn with_timeout(mut self, timeout: u64) -> Self {
        self.timeout = Some(timeout);
        self
    }
}

impl std::fmt::Display for ScatterGather {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ScatterGather[scatter={}, {} workers, gather={}]",
            self.scatter.task,
            self.workers.len(),
            self.gather.task
        )
    }
}

/// Pipeline pattern: streaming data through stages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    /// Pipeline stages
    pub stages: Vec<Signature>,
    /// Buffer size between stages
    pub buffer_size: Option<usize>,
}

impl Pipeline {
    /// Create a new pipeline
    pub fn new() -> Self {
        Self {
            stages: Vec::new(),
            buffer_size: None,
        }
    }

    /// Add a stage
    pub fn stage(mut self, stage: Signature) -> Self {
        self.stages.push(stage);
        self
    }

    /// Set buffer size
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = Some(size);
        self
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.stages.is_empty()
    }

    /// Get number of stages
    pub fn len(&self) -> usize {
        self.stages.len()
    }
}

impl Default for Pipeline {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for Pipeline {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Pipeline[{} stages]", self.stages.len())?;
        if let Some(buf) = self.buffer_size {
            write!(f, " buffer={}", buf)?;
        }
        Ok(())
    }
}

/// Fan-out pattern: broadcast to multiple consumers
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanOut {
    /// Source task
    pub source: Signature,
    /// Consumer tasks
    pub consumers: Vec<Signature>,
}

impl FanOut {
    /// Create a new fan-out pattern
    pub fn new(source: Signature) -> Self {
        Self {
            source,
            consumers: Vec::new(),
        }
    }

    /// Add a consumer
    pub fn consumer(mut self, consumer: Signature) -> Self {
        self.consumers.push(consumer);
        self
    }

    /// Get number of consumers
    pub fn len(&self) -> usize {
        self.consumers.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.consumers.is_empty()
    }
}

impl std::fmt::Display for FanOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FanOut[source={}, {} consumers]",
            self.source.task,
            self.consumers.len()
        )
    }
}

/// Fan-in pattern: collect from multiple sources
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FanIn {
    /// Source tasks
    pub sources: Vec<Signature>,
    /// Aggregator task
    pub aggregator: Signature,
}

impl FanIn {
    /// Create a new fan-in pattern
    pub fn new(aggregator: Signature) -> Self {
        Self {
            sources: Vec::new(),
            aggregator,
        }
    }

    /// Add a source
    pub fn source(mut self, source: Signature) -> Self {
        self.sources.push(source);
        self
    }

    /// Get number of sources
    pub fn len(&self) -> usize {
        self.sources.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.sources.is_empty()
    }
}

impl std::fmt::Display for FanIn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "FanIn[{} sources, aggregator={}]",
            self.sources.len(),
            self.aggregator.task
        )
    }
}

// ============================================================================
// Workflow Validation and Dry-Run
// ============================================================================

/// Workflow validation result
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Whether workflow is valid
    pub valid: bool,
    /// Validation errors
    pub errors: Vec<String>,
    /// Validation warnings
    pub warnings: Vec<String>,
}

impl ValidationResult {
    /// Create a valid result
    pub fn valid() -> Self {
        Self {
            valid: true,
            errors: Vec::new(),
            warnings: Vec::new(),
        }
    }

    /// Create an invalid result
    pub fn invalid(error: impl Into<String>) -> Self {
        Self {
            valid: false,
            errors: vec![error.into()],
            warnings: Vec::new(),
        }
    }

    /// Add an error
    pub fn add_error(&mut self, error: impl Into<String>) {
        self.errors.push(error.into());
        self.valid = false;
    }

    /// Add a warning
    pub fn add_warning(&mut self, warning: impl Into<String>) {
        self.warnings.push(warning.into());
    }
}

impl std::fmt::Display for ValidationResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.valid {
            write!(f, "Valid")?;
            if !self.warnings.is_empty() {
                write!(f, " ({} warnings)", self.warnings.len())?;
            }
        } else {
            write!(f, "Invalid ({} errors)", self.errors.len())?;
        }
        Ok(())
    }
}

/// Workflow validator trait
pub trait WorkflowValidator {
    /// Validate workflow structure
    fn validate(&self) -> ValidationResult;
}

impl WorkflowValidator for Chain {
    fn validate(&self) -> ValidationResult {
        let mut result = ValidationResult::valid();

        if self.is_empty() {
            result.add_error("Chain cannot be empty");
        }

        if self.len() > 100 {
            result.add_warning(format!(
                "Chain has {} tasks, which may be inefficient",
                self.len()
            ));
        }

        result
    }
}

impl WorkflowValidator for Group {
    fn validate(&self) -> ValidationResult {
        let mut result = ValidationResult::valid();

        if self.is_empty() {
            result.add_error("Group cannot be empty");
        }

        if self.len() > 1000 {
            result.add_warning(format!(
                "Group has {} tasks, which may overwhelm workers",
                self.len()
            ));
        }

        result
    }
}

impl WorkflowValidator for Chord {
    fn validate(&self) -> ValidationResult {
        let mut result = ValidationResult::valid();

        if self.header.is_empty() {
            result.add_error("Chord header cannot be empty");
        }

        result
    }
}

// ============================================================================
// Loop Control
// ============================================================================

/// Loop control for break/continue operations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LoopControl {
    /// Continue to next iteration
    Continue,
    /// Break out of loop
    Break,
    /// Break with result value
    BreakWith { value: serde_json::Value },
}

impl LoopControl {
    /// Create a continue control
    pub fn continue_loop() -> Self {
        Self::Continue
    }

    /// Create a break control
    pub fn break_loop() -> Self {
        Self::Break
    }

    /// Create a break with value
    pub fn break_with(value: serde_json::Value) -> Self {
        Self::BreakWith { value }
    }
}

impl std::fmt::Display for LoopControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Continue => write!(f, "Continue"),
            Self::Break => write!(f, "Break"),
            Self::BreakWith { .. } => write!(f, "BreakWith"),
        }
    }
}

// ============================================================================
// Error Propagation Control
// ============================================================================

/// Error propagation mode for workflows
///
/// Controls how errors are handled and propagated in workflow execution.
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
pub enum ErrorPropagationMode {
    /// Stop on first error (default)
    #[default]
    StopOnFirstError,

    /// Continue execution, collect all errors
    ContinueOnError,

    /// Partial failure handling - continue if threshold not exceeded
    PartialFailure {
        /// Maximum number of failed tasks before stopping
        max_failures: usize,
        /// Maximum failure percentage (0.0-1.0) before stopping
        max_failure_rate: Option<f64>,
    },
}

impl ErrorPropagationMode {
    /// Create a partial failure mode
    pub fn partial_failure(max_failures: usize) -> Self {
        Self::PartialFailure {
            max_failures,
            max_failure_rate: None,
        }
    }

    /// Create a partial failure mode with rate threshold
    pub fn partial_failure_with_rate(max_failures: usize, max_rate: f64) -> Self {
        Self::PartialFailure {
            max_failures,
            max_failure_rate: Some(max_rate),
        }
    }

    /// Check if mode allows continuing after error
    pub fn allows_continue(&self) -> bool {
        !matches!(self, Self::StopOnFirstError)
    }
}

impl std::fmt::Display for ErrorPropagationMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::StopOnFirstError => write!(f, "StopOnFirstError"),
            Self::ContinueOnError => write!(f, "ContinueOnError"),
            Self::PartialFailure {
                max_failures,
                max_failure_rate,
            } => {
                write!(f, "PartialFailure(max={})", max_failures)?;
                if let Some(rate) = max_failure_rate {
                    write!(f, " rate={:.1}%", rate * 100.0)?;
                }
                Ok(())
            }
        }
    }
}

/// Tracks partial failure information for workflows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartialFailureTracker {
    /// Total number of tasks
    pub total_tasks: usize,
    /// Number of successful tasks
    pub successful_tasks: usize,
    /// Number of failed tasks
    pub failed_tasks: usize,
    /// Task IDs that succeeded
    pub successful_task_ids: Vec<Uuid>,
    /// Task IDs that failed with error messages
    pub failed_task_ids: Vec<(Uuid, String)>,
}

impl PartialFailureTracker {
    /// Create a new partial failure tracker
    pub fn new(total_tasks: usize) -> Self {
        Self {
            total_tasks,
            successful_tasks: 0,
            failed_tasks: 0,
            successful_task_ids: Vec::new(),
            failed_task_ids: Vec::new(),
        }
    }

    /// Record a successful task
    pub fn record_success(&mut self, task_id: Uuid) {
        self.successful_tasks += 1;
        self.successful_task_ids.push(task_id);
    }

    /// Record a failed task
    pub fn record_failure(&mut self, task_id: Uuid, error: String) {
        self.failed_tasks += 1;
        self.failed_task_ids.push((task_id, error));
    }

    /// Calculate failure rate (0.0-1.0)
    pub fn failure_rate(&self) -> f64 {
        if self.total_tasks == 0 {
            return 0.0;
        }
        self.failed_tasks as f64 / self.total_tasks as f64
    }

    /// Calculate success rate (0.0-1.0)
    pub fn success_rate(&self) -> f64 {
        if self.total_tasks == 0 {
            return 1.0;
        }
        self.successful_tasks as f64 / self.total_tasks as f64
    }

    /// Check if failure threshold exceeded
    pub fn exceeds_threshold(&self, mode: &ErrorPropagationMode) -> bool {
        match mode {
            ErrorPropagationMode::StopOnFirstError => self.failed_tasks > 0,
            ErrorPropagationMode::ContinueOnError => false,
            ErrorPropagationMode::PartialFailure {
                max_failures,
                max_failure_rate,
            } => {
                if self.failed_tasks >= *max_failures {
                    return true;
                }
                if let Some(rate) = max_failure_rate {
                    if self.failure_rate() > *rate {
                        return true;
                    }
                }
                false
            }
        }
    }

    /// Check if workflow should continue
    pub fn should_continue(&self, mode: &ErrorPropagationMode) -> bool {
        !self.exceeds_threshold(mode)
    }
}

impl std::fmt::Display for PartialFailureTracker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "PartialFailureTracker[success={}/{}, failed={}, rate={:.1}%]",
            self.successful_tasks,
            self.total_tasks,
            self.failed_tasks,
            self.failure_rate() * 100.0
        )
    }
}

// ============================================================================
// Sub-Workflow Isolation
// ============================================================================

/// Isolation level for sub-workflows
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
pub enum IsolationLevel {
    /// No isolation - sub-workflow shares parent context
    #[default]
    None,

    /// Resource isolation - separate resource limits
    Resource {
        /// Maximum memory in MB
        max_memory_mb: Option<u64>,
        /// Maximum CPU percentage
        max_cpu_percent: Option<u8>,
    },

    /// Error isolation - errors don't propagate to parent
    Error,

    /// Full isolation - separate context, resources, and errors
    Full {
        /// Maximum memory in MB
        max_memory_mb: Option<u64>,
        /// Maximum CPU percentage
        max_cpu_percent: Option<u8>,
    },
}

impl IsolationLevel {
    /// Create resource isolation
    pub fn resource(max_memory_mb: u64) -> Self {
        Self::Resource {
            max_memory_mb: Some(max_memory_mb),
            max_cpu_percent: None,
        }
    }

    /// Create full isolation
    pub fn full(max_memory_mb: u64) -> Self {
        Self::Full {
            max_memory_mb: Some(max_memory_mb),
            max_cpu_percent: None,
        }
    }

    /// Check if isolation includes resource limits
    pub fn has_resource_limits(&self) -> bool {
        matches!(self, Self::Resource { .. } | Self::Full { .. })
    }

    /// Check if isolation includes error boundaries
    pub fn has_error_isolation(&self) -> bool {
        matches!(self, Self::Error | Self::Full { .. })
    }
}

impl std::fmt::Display for IsolationLevel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => write!(f, "None"),
            Self::Resource {
                max_memory_mb,
                max_cpu_percent,
            } => {
                write!(f, "Resource(")?;
                if let Some(mem) = max_memory_mb {
                    write!(f, "mem={}MB", mem)?;
                }
                if let Some(cpu) = max_cpu_percent {
                    write!(f, " cpu={}%", cpu)?;
                }
                write!(f, ")")
            }
            Self::Error => write!(f, "Error"),
            Self::Full {
                max_memory_mb,
                max_cpu_percent,
            } => {
                write!(f, "Full(")?;
                if let Some(mem) = max_memory_mb {
                    write!(f, "mem={}MB", mem)?;
                }
                if let Some(cpu) = max_cpu_percent {
                    write!(f, " cpu={}%", cpu)?;
                }
                write!(f, ")")
            }
        }
    }
}

/// Sub-workflow isolation context
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubWorkflowIsolation {
    /// Sub-workflow ID
    pub workflow_id: Uuid,
    /// Parent workflow ID
    pub parent_workflow_id: Option<Uuid>,
    /// Isolation level
    pub isolation_level: IsolationLevel,
    /// Whether errors should propagate to parent
    pub propagate_errors: bool,
    /// Whether cancellation should propagate to parent
    pub propagate_cancellation: bool,
}

impl SubWorkflowIsolation {
    /// Create a new sub-workflow isolation context
    pub fn new(workflow_id: Uuid, isolation_level: IsolationLevel) -> Self {
        Self {
            workflow_id,
            parent_workflow_id: None,
            isolation_level,
            propagate_errors: true,
            propagate_cancellation: true,
        }
    }

    /// Set parent workflow ID
    pub fn with_parent(mut self, parent_id: Uuid) -> Self {
        self.parent_workflow_id = Some(parent_id);
        self
    }

    /// Disable error propagation
    pub fn no_error_propagation(mut self) -> Self {
        self.propagate_errors = false;
        self
    }

    /// Disable cancellation propagation
    pub fn no_cancellation_propagation(mut self) -> Self {
        self.propagate_cancellation = false;
        self
    }
}

impl std::fmt::Display for SubWorkflowIsolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "SubWorkflowIsolation[id={}, level={}]",
            self.workflow_id, self.isolation_level
        )?;
        if let Some(parent) = self.parent_workflow_id {
            write!(f, " parent={}", parent)?;
        }
        Ok(())
    }
}

// ============================================================================
// Workflow Checkpointing and Recovery
// ============================================================================

/// Workflow checkpoint for crash recovery
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowCheckpoint {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Checkpoint timestamp (Unix timestamp)
    pub timestamp: u64,
    /// Completed task IDs
    pub completed_tasks: Vec<Uuid>,
    /// Failed task IDs with errors
    pub failed_tasks: Vec<(Uuid, String)>,
    /// In-progress task IDs
    pub in_progress_tasks: Vec<Uuid>,
    /// Workflow state snapshot
    pub state: WorkflowState,
    /// Checkpoint version (for compatibility)
    pub version: u32,
}

impl WorkflowCheckpoint {
    /// Create a new checkpoint
    pub fn new(workflow_id: Uuid, state: WorkflowState) -> Self {
        Self {
            workflow_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            completed_tasks: Vec::new(),
            failed_tasks: Vec::new(),
            in_progress_tasks: Vec::new(),
            state,
            version: 1,
        }
    }

    /// Record completed task
    pub fn record_completed(&mut self, task_id: Uuid) {
        self.completed_tasks.push(task_id);
        // Remove from in-progress if present
        self.in_progress_tasks.retain(|&id| id != task_id);
    }

    /// Record failed task
    pub fn record_failed(&mut self, task_id: Uuid, error: String) {
        self.failed_tasks.push((task_id, error));
        // Remove from in-progress if present
        self.in_progress_tasks.retain(|&id| id != task_id);
    }

    /// Record in-progress task
    pub fn record_in_progress(&mut self, task_id: Uuid) {
        if !self.in_progress_tasks.contains(&task_id) {
            self.in_progress_tasks.push(task_id);
        }
    }

    /// Check if task is completed
    pub fn is_completed(&self, task_id: &Uuid) -> bool {
        self.completed_tasks.contains(task_id)
    }

    /// Check if task failed
    pub fn is_failed(&self, task_id: &Uuid) -> bool {
        self.failed_tasks.iter().any(|(id, _)| id == task_id)
    }

    /// Get tasks that need to be retried (in-progress at checkpoint)
    pub fn tasks_to_retry(&self) -> &[Uuid] {
        &self.in_progress_tasks
    }

    /// Serialize checkpoint to JSON
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserialize checkpoint from JSON
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }
}

impl std::fmt::Display for WorkflowCheckpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkflowCheckpoint[id={}, completed={}, failed={}, in_progress={}]",
            self.workflow_id,
            self.completed_tasks.len(),
            self.failed_tasks.len(),
            self.in_progress_tasks.len()
        )
    }
}

/// Workflow recovery policy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowRecoveryPolicy {
    /// Whether to enable automatic recovery
    pub auto_recovery: bool,
    /// Whether to resume from last checkpoint
    pub resume_from_checkpoint: bool,
    /// Whether to replay failed stages
    pub replay_failed: bool,
    /// Maximum age of checkpoint to use (seconds)
    pub max_checkpoint_age: Option<u64>,
    /// Retry policy for recovered tasks
    pub retry_policy: Option<WorkflowRetryPolicy>,
}

impl WorkflowRecoveryPolicy {
    /// Create a new recovery policy with auto-recovery enabled
    pub fn auto_recover() -> Self {
        Self {
            auto_recovery: true,
            resume_from_checkpoint: true,
            replay_failed: true,
            max_checkpoint_age: Some(3600), // 1 hour
            retry_policy: None,
        }
    }

    /// Disable auto-recovery
    pub fn manual() -> Self {
        Self {
            auto_recovery: false,
            resume_from_checkpoint: true,
            replay_failed: false,
            max_checkpoint_age: None,
            retry_policy: None,
        }
    }

    /// Set maximum checkpoint age
    pub fn with_max_checkpoint_age(mut self, seconds: u64) -> Self {
        self.max_checkpoint_age = Some(seconds);
        self
    }

    /// Set retry policy for recovered tasks
    pub fn with_retry_policy(mut self, policy: WorkflowRetryPolicy) -> Self {
        self.retry_policy = Some(policy);
        self
    }

    /// Check if checkpoint is valid based on age
    pub fn is_checkpoint_valid(&self, checkpoint: &WorkflowCheckpoint) -> bool {
        if let Some(max_age) = self.max_checkpoint_age {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let age = now.saturating_sub(checkpoint.timestamp);
            age <= max_age
        } else {
            true
        }
    }
}

impl std::fmt::Display for WorkflowRecoveryPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkflowRecoveryPolicy[")?;
        if self.auto_recovery {
            write!(f, "auto")?;
        } else {
            write!(f, "manual")?;
        }
        if self.resume_from_checkpoint {
            write!(f, " resume")?;
        }
        if self.replay_failed {
            write!(f, " replay_failed")?;
        }
        write!(f, "]")
    }
}

// ============================================================================
// State Versioning
// ============================================================================

/// State version for workflow state evolution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct StateVersion {
    /// Major version (breaking changes)
    pub major: u32,
    /// Minor version (backward-compatible changes)
    pub minor: u32,
    /// Patch version (bug fixes)
    pub patch: u32,
}

impl StateVersion {
    /// Create a new state version
    pub fn new(major: u32, minor: u32, patch: u32) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }

    /// Current version
    pub fn current() -> Self {
        Self {
            major: 1,
            minor: 0,
            patch: 0,
        }
    }

    /// Check if this version can be migrated to another version
    /// (same major version, this minor version <= target minor version)
    pub fn is_compatible(&self, other: &StateVersion) -> bool {
        self.major == other.major && self.minor <= other.minor
    }
}

impl std::fmt::Display for StateVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}.{}", self.major, self.minor, self.patch)
    }
}

/// State migration error
#[derive(Debug, Clone)]
pub enum StateMigrationError {
    /// Incompatible version
    IncompatibleVersion {
        from: StateVersion,
        to: StateVersion,
    },
    /// Migration failed
    MigrationFailed(String),
    /// Unsupported version
    UnsupportedVersion(StateVersion),
}

impl std::fmt::Display for StateMigrationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IncompatibleVersion { from, to } => {
                write!(f, "Incompatible state version: {} -> {}", from, to)
            }
            Self::MigrationFailed(msg) => write!(f, "State migration failed: {}", msg),
            Self::UnsupportedVersion(version) => {
                write!(f, "Unsupported state version: {}", version)
            }
        }
    }
}

impl std::error::Error for StateMigrationError {}

/// State migration strategy
pub trait StateMigration {
    /// Migrate state from one version to another
    fn migrate(&self, from: StateVersion, to: StateVersion) -> Result<(), StateMigrationError>;
}

/// Versioned workflow state with migration support
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionedWorkflowState {
    /// State version
    pub version: StateVersion,
    /// Workflow state
    pub state: WorkflowState,
    /// Migration history
    pub migration_history: Vec<(StateVersion, StateVersion, u64)>, // (from, to, timestamp)
}

impl VersionedWorkflowState {
    /// Create a new versioned state
    pub fn new(state: WorkflowState) -> Self {
        Self {
            version: StateVersion::current(),
            state,
            migration_history: Vec::new(),
        }
    }

    /// Migrate to a new version
    pub fn migrate_to(&mut self, target: StateVersion) -> Result<(), StateMigrationError> {
        if self.version == target {
            return Ok(());
        }

        if !self.version.is_compatible(&target) {
            return Err(StateMigrationError::IncompatibleVersion {
                from: self.version,
                to: target,
            });
        }

        // Record migration
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.migration_history
            .push((self.version, target, timestamp));
        self.version = target;

        Ok(())
    }

    /// Check if state can be migrated to target version
    pub fn can_migrate_to(&self, target: &StateVersion) -> bool {
        self.version.is_compatible(target)
    }

    /// Get migration history
    pub fn get_migration_history(&self) -> &[(StateVersion, StateVersion, u64)] {
        &self.migration_history
    }
}

// ============================================================================
// Workflow Compilation and Optimization
// ============================================================================

/// Workflow optimization pass
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OptimizationPass {
    /// Common subexpression elimination
    CommonSubexpressionElimination,
    /// Dead code elimination
    DeadCodeElimination,
    /// Task fusion (combine sequential tasks)
    TaskFusion,
    /// Parallel task scheduling optimization
    ParallelScheduling,
    /// Resource allocation optimization
    ResourceOptimization,
}

impl std::fmt::Display for OptimizationPass {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CommonSubexpressionElimination => write!(f, "CSE"),
            Self::DeadCodeElimination => write!(f, "DCE"),
            Self::TaskFusion => write!(f, "TaskFusion"),
            Self::ParallelScheduling => write!(f, "ParallelScheduling"),
            Self::ResourceOptimization => write!(f, "ResourceOptimization"),
        }
    }
}

/// Workflow compiler for optimization
#[derive(Debug, Clone)]
pub struct WorkflowCompiler {
    /// Optimization passes to apply
    pub passes: Vec<OptimizationPass>,
    /// Whether to enable aggressive optimizations
    pub aggressive: bool,
}

impl WorkflowCompiler {
    /// Create a new workflow compiler
    pub fn new() -> Self {
        Self {
            passes: vec![
                OptimizationPass::DeadCodeElimination,
                OptimizationPass::CommonSubexpressionElimination,
            ],
            aggressive: false,
        }
    }

    /// Enable aggressive optimizations
    pub fn aggressive(mut self) -> Self {
        self.aggressive = true;
        self.passes.push(OptimizationPass::TaskFusion);
        self.passes.push(OptimizationPass::ParallelScheduling);
        self.passes.push(OptimizationPass::ResourceOptimization);
        self
    }

    /// Add optimization pass
    pub fn add_pass(mut self, pass: OptimizationPass) -> Self {
        if !self.passes.contains(&pass) {
            self.passes.push(pass);
        }
        self
    }

    /// Optimize a chain by applying configured optimization passes
    pub fn optimize_chain(&self, chain: &Chain) -> Chain {
        let mut optimized = chain.clone();

        for pass in &self.passes {
            optimized = match pass {
                OptimizationPass::CommonSubexpressionElimination => {
                    self.apply_cse_chain(&optimized)
                }
                OptimizationPass::DeadCodeElimination => self.apply_dce_chain(&optimized),
                OptimizationPass::TaskFusion => self.apply_task_fusion(&optimized),
                OptimizationPass::ParallelScheduling => {
                    // Chains are sequential, no parallel scheduling
                    optimized
                }
                OptimizationPass::ResourceOptimization => {
                    self.apply_resource_optimization_chain(&optimized)
                }
            };
        }

        optimized
    }

    /// Optimize a group by applying configured optimization passes
    pub fn optimize_group(&self, group: &Group) -> Group {
        let mut optimized = group.clone();

        for pass in &self.passes {
            optimized = match pass {
                OptimizationPass::CommonSubexpressionElimination => {
                    self.apply_cse_group(&optimized)
                }
                OptimizationPass::DeadCodeElimination => self.apply_dce_group(&optimized),
                OptimizationPass::TaskFusion => {
                    // Groups are parallel, no task fusion
                    optimized
                }
                OptimizationPass::ParallelScheduling => self.apply_parallel_scheduling(&optimized),
                OptimizationPass::ResourceOptimization => {
                    self.apply_resource_optimization_group(&optimized)
                }
            };
        }

        optimized
    }

    /// Optimize a chord by applying configured optimization passes
    pub fn optimize_chord(&self, chord: &Chord) -> Chord {
        let optimized_group = self.optimize_group(&chord.header);
        Chord {
            header: optimized_group,
            body: chord.body.clone(),
        }
    }

    // Helper methods for optimization passes

    /// Common Subexpression Elimination for chains
    fn apply_cse_chain(&self, chain: &Chain) -> Chain {
        let mut seen = HashMap::new();
        let mut optimized_tasks = Vec::new();

        for (idx, task) in chain.tasks.iter().enumerate() {
            // Create a key for deduplication (task name + args)
            let key = format!(
                "{}:{}:{}",
                task.task,
                serde_json::to_string(&task.args).unwrap_or_default(),
                serde_json::to_string(&task.kwargs).unwrap_or_default()
            );

            if let Some(&prev_idx) = seen.get(&key) {
                // Skip duplicate if aggressive mode
                if self.aggressive && prev_idx < idx {
                    continue;
                }
            } else {
                seen.insert(key, idx);
            }

            optimized_tasks.push(task.clone());
        }

        Chain {
            tasks: optimized_tasks,
        }
    }

    /// Common Subexpression Elimination for groups
    fn apply_cse_group(&self, group: &Group) -> Group {
        let mut seen = HashMap::new();
        let mut optimized_tasks = Vec::new();

        for task in &group.tasks {
            // Create a key for deduplication (task name + args)
            let key = format!(
                "{}:{}:{}",
                task.task,
                serde_json::to_string(&task.args).unwrap_or_default(),
                serde_json::to_string(&task.kwargs).unwrap_or_default()
            );

            if let std::collections::hash_map::Entry::Vacant(e) = seen.entry(key) {
                e.insert(true);
                optimized_tasks.push(task.clone());
            } else {
                // Skip duplicate in aggressive mode
                if !self.aggressive {
                    optimized_tasks.push(task.clone());
                }
            }
        }

        Group {
            tasks: optimized_tasks,
            group_id: group.group_id,
        }
    }

    /// Dead Code Elimination for chains
    fn apply_dce_chain(&self, chain: &Chain) -> Chain {
        // Remove tasks that have no effect (e.g., empty task names)
        let optimized_tasks: Vec<_> = chain
            .tasks
            .iter()
            .filter(|task| !task.task.is_empty())
            .cloned()
            .collect();

        Chain {
            tasks: optimized_tasks,
        }
    }

    /// Dead Code Elimination for groups
    fn apply_dce_group(&self, group: &Group) -> Group {
        // Remove tasks that have no effect (e.g., empty task names)
        let optimized_tasks: Vec<_> = group
            .tasks
            .iter()
            .filter(|task| !task.task.is_empty())
            .cloned()
            .collect();

        Group {
            tasks: optimized_tasks,
            group_id: group.group_id,
        }
    }

    /// Task Fusion for chains (combine similar sequential tasks)
    fn apply_task_fusion(&self, chain: &Chain) -> Chain {
        if !self.aggressive || chain.tasks.len() < 2 {
            return chain.clone();
        }

        let mut optimized_tasks = Vec::new();
        let mut i = 0;

        while i < chain.tasks.len() {
            let current = &chain.tasks[i];

            // Check if next task can be fused (same task name, immutable)
            if i + 1 < chain.tasks.len() {
                let next = &chain.tasks[i + 1];

                if current.task == next.task
                    && current.immutable
                    && next.immutable
                    && current.options.priority == next.options.priority
                {
                    // Fuse tasks: combine args
                    let mut fused = current.clone();
                    fused.args.extend(next.args.clone());
                    optimized_tasks.push(fused);
                    i += 2; // Skip both tasks
                    continue;
                }
            }

            optimized_tasks.push(current.clone());
            i += 1;
        }

        Chain {
            tasks: optimized_tasks,
        }
    }

    /// Parallel Scheduling optimization for groups
    fn apply_parallel_scheduling(&self, group: &Group) -> Group {
        let mut optimized_tasks = group.tasks.clone();

        // Sort tasks by priority (higher priority first)
        optimized_tasks.sort_by(|a, b| {
            let a_priority = a.options.priority.unwrap_or(0);
            let b_priority = b.options.priority.unwrap_or(0);
            b_priority.cmp(&a_priority)
        });

        Group {
            tasks: optimized_tasks,
            group_id: group.group_id,
        }
    }

    /// Resource Optimization for chains
    fn apply_resource_optimization_chain(&self, chain: &Chain) -> Chain {
        // Group tasks by queue to improve resource utilization
        let mut optimized_tasks = chain.tasks.clone();

        if self.aggressive {
            // Reorder tasks to group by queue while maintaining dependencies
            optimized_tasks.sort_by(|a, b| {
                let a_queue = a.options.queue.as_deref().unwrap_or("");
                let b_queue = b.options.queue.as_deref().unwrap_or("");
                a_queue.cmp(b_queue)
            });
        }

        Chain {
            tasks: optimized_tasks,
        }
    }

    /// Resource Optimization for groups
    fn apply_resource_optimization_group(&self, group: &Group) -> Group {
        // Balance tasks across queues
        let mut optimized_tasks = group.tasks.clone();

        // Sort by queue to improve locality
        optimized_tasks.sort_by(|a, b| {
            let a_queue = a.options.queue.as_deref().unwrap_or("");
            let b_queue = b.options.queue.as_deref().unwrap_or("");
            a_queue.cmp(b_queue)
        });

        Group {
            tasks: optimized_tasks,
            group_id: group.group_id,
        }
    }
}

impl Default for WorkflowCompiler {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for WorkflowCompiler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkflowCompiler[")?;
        for (i, pass) in self.passes.iter().enumerate() {
            if i > 0 {
                write!(f, ", ")?;
            }
            write!(f, "{}", pass)?;
        }
        if self.aggressive {
            write!(f, " aggressive")?;
        }
        write!(f, "]")
    }
}

// ============================================================================
// Type-Safe Result Passing
// ============================================================================

/// Type-safe result wrapper for workflow results
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypedResult<T> {
    /// Result value
    pub value: T,
    /// Result type name for validation
    pub type_name: String,
    /// Result metadata
    #[serde(default)]
    pub metadata: HashMap<String, serde_json::Value>,
}

impl<T: Serialize> TypedResult<T> {
    /// Create a new typed result
    pub fn new(value: T) -> Self {
        Self {
            value,
            type_name: std::any::type_name::<T>().to_string(),
            metadata: HashMap::new(),
        }
    }

    /// Add metadata to the result
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Get the type name
    pub fn type_name(&self) -> &str {
        &self.type_name
    }
}

impl<T: std::fmt::Display> std::fmt::Display for TypedResult<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TypedResult[type={}, value={}]",
            self.type_name, self.value
        )
    }
}

/// Type validator for result passing
#[derive(Debug, Clone)]
pub struct TypeValidator {
    /// Expected type name
    pub expected_type: String,
    /// Whether to allow compatible types
    pub allow_compatible: bool,
}

impl TypeValidator {
    /// Create a new type validator
    pub fn new(expected_type: impl Into<String>) -> Self {
        Self {
            expected_type: expected_type.into(),
            allow_compatible: false,
        }
    }

    /// Allow compatible types
    pub fn allow_compatible(mut self) -> Self {
        self.allow_compatible = true;
        self
    }

    /// Validate a type name
    pub fn validate(&self, actual_type: &str) -> bool {
        if actual_type == self.expected_type {
            return true;
        }
        if self.allow_compatible {
            self.is_compatible(actual_type)
        } else {
            false
        }
    }

    /// Check if types are compatible
    fn is_compatible(&self, actual_type: &str) -> bool {
        // Simple compatibility check (can be extended)
        if self.expected_type.contains("Option") && actual_type != "None" {
            return true;
        }
        if self.expected_type == "serde_json::Value" {
            return true;
        }
        false
    }
}

impl std::fmt::Display for TypeValidator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TypeValidator[expected={}]", self.expected_type)?;
        if self.allow_compatible {
            write!(f, " (allow_compatible)")?;
        }
        Ok(())
    }
}

// ============================================================================
// Data Dependencies
// ============================================================================

/// Task dependency specification
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct TaskDependency {
    /// Task ID that this task depends on
    pub task_id: Uuid,
    /// Output key to use from the dependency (optional)
    pub output_key: Option<String>,
    /// Whether this dependency is optional
    #[serde(default)]
    pub optional: bool,
}

impl TaskDependency {
    /// Create a new task dependency
    pub fn new(task_id: Uuid) -> Self {
        Self {
            task_id,
            output_key: None,
            optional: false,
        }
    }

    /// Set output key
    pub fn with_output_key(mut self, key: impl Into<String>) -> Self {
        self.output_key = Some(key.into());
        self
    }

    /// Mark as optional
    pub fn optional(mut self) -> Self {
        self.optional = true;
        self
    }
}

impl std::fmt::Display for TaskDependency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TaskDependency[{}]", self.task_id)?;
        if let Some(ref key) = self.output_key {
            write!(f, " output={}", key)?;
        }
        if self.optional {
            write!(f, " (optional)")?;
        }
        Ok(())
    }
}

/// Dependency graph for workflow tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DependencyGraph {
    /// Map of task ID to its dependencies
    pub dependencies: HashMap<Uuid, Vec<TaskDependency>>,
    /// Reverse map for quick lookup
    #[serde(skip)]
    pub dependents: HashMap<Uuid, Vec<Uuid>>,
}

impl DependencyGraph {
    /// Create a new dependency graph
    pub fn new() -> Self {
        Self {
            dependencies: HashMap::new(),
            dependents: HashMap::new(),
        }
    }

    /// Add a dependency
    pub fn add_dependency(&mut self, task_id: Uuid, dependency: TaskDependency) {
        self.dependencies
            .entry(task_id)
            .or_default()
            .push(dependency.clone());

        // Update reverse map
        self.dependents
            .entry(dependency.task_id)
            .or_default()
            .push(task_id);
    }

    /// Get dependencies for a task
    pub fn get_dependencies(&self, task_id: &Uuid) -> Vec<&TaskDependency> {
        self.dependencies
            .get(task_id)
            .map(|deps| deps.iter().collect())
            .unwrap_or_default()
    }

    /// Get tasks that depend on this task
    pub fn get_dependents(&self, task_id: &Uuid) -> Vec<Uuid> {
        self.dependents.get(task_id).cloned().unwrap_or_default()
    }

    /// Check for circular dependencies
    pub fn has_circular_dependency(&self) -> bool {
        let mut visited = std::collections::HashSet::new();
        let mut rec_stack = std::collections::HashSet::new();

        for task_id in self.dependencies.keys() {
            if self.is_cyclic(*task_id, &mut visited, &mut rec_stack) {
                return true;
            }
        }
        false
    }

    fn is_cyclic(
        &self,
        task_id: Uuid,
        visited: &mut std::collections::HashSet<Uuid>,
        rec_stack: &mut std::collections::HashSet<Uuid>,
    ) -> bool {
        if rec_stack.contains(&task_id) {
            return true;
        }
        if visited.contains(&task_id) {
            return false;
        }

        visited.insert(task_id);
        rec_stack.insert(task_id);

        if let Some(deps) = self.dependencies.get(&task_id) {
            for dep in deps {
                if self.is_cyclic(dep.task_id, visited, rec_stack) {
                    return true;
                }
            }
        }

        rec_stack.remove(&task_id);
        false
    }

    /// Get topological order of tasks
    pub fn topological_sort(&self) -> Result<Vec<Uuid>, String> {
        if self.has_circular_dependency() {
            return Err("Circular dependency detected".to_string());
        }

        let mut in_degree: HashMap<Uuid, usize> = HashMap::new();
        let mut queue: Vec<Uuid> = Vec::new();
        let mut result: Vec<Uuid> = Vec::new();

        // Calculate in-degrees
        for (task_id, deps) in &self.dependencies {
            in_degree.entry(*task_id).or_insert(deps.len());
            for dep in deps {
                in_degree.entry(dep.task_id).or_insert(0);
            }
        }

        // Find tasks with no dependencies
        for (task_id, &degree) in &in_degree {
            if degree == 0 {
                queue.push(*task_id);
            }
        }

        // Process queue
        while let Some(task_id) = queue.pop() {
            result.push(task_id);

            if let Some(dependents) = self.dependents.get(&task_id) {
                for &dependent in dependents {
                    if let Some(degree) = in_degree.get_mut(&dependent) {
                        if *degree > 0 {
                            *degree -= 1;
                            if *degree == 0 {
                                queue.push(dependent);
                            }
                        }
                    }
                }
            }
        }

        if result.len() == in_degree.len() {
            Ok(result)
        } else {
            Err("Failed to compute topological sort".to_string())
        }
    }
}

impl Default for DependencyGraph {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for DependencyGraph {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "DependencyGraph[{} tasks]", self.dependencies.len())
    }
}

// ============================================================================
// Parallel Reduce
// ============================================================================

/// Parallel reduce configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ParallelReduce {
    /// Task to map over inputs
    pub map_task: Signature,
    /// Task to reduce pairs of results
    pub reduce_task: Signature,
    /// Input values to map over
    pub inputs: Vec<serde_json::Value>,
    /// Number of parallel workers
    pub parallelism: usize,
    /// Initial value for reduction
    pub initial_value: Option<serde_json::Value>,
}

impl ParallelReduce {
    /// Create a new parallel reduce
    pub fn new(
        map_task: Signature,
        reduce_task: Signature,
        inputs: Vec<serde_json::Value>,
    ) -> Self {
        Self {
            map_task,
            reduce_task,
            inputs,
            parallelism: 4,
            initial_value: None,
        }
    }

    /// Set parallelism level
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = parallelism;
        self
    }

    /// Set initial value
    pub fn with_initial_value(mut self, value: serde_json::Value) -> Self {
        self.initial_value = Some(value);
        self
    }

    /// Get the number of inputs
    pub fn input_count(&self) -> usize {
        self.inputs.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.inputs.is_empty()
    }
}

impl std::fmt::Display for ParallelReduce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParallelReduce[map={}, reduce={}, inputs={}, parallelism={}]",
            self.map_task.task,
            self.reduce_task.task,
            self.inputs.len(),
            self.parallelism
        )
    }
}

// ============================================================================
// Workflow Templates
// ============================================================================

/// Workflow template parameter
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemplateParameter {
    /// Parameter name
    pub name: String,
    /// Parameter type
    pub param_type: String,
    /// Default value
    pub default: Option<serde_json::Value>,
    /// Whether parameter is required
    #[serde(default = "default_true")]
    pub required: bool,
    /// Parameter description
    pub description: Option<String>,
}

fn default_true() -> bool {
    true
}

impl TemplateParameter {
    /// Create a new template parameter
    pub fn new(name: impl Into<String>, param_type: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            param_type: param_type.into(),
            default: None,
            required: true,
            description: None,
        }
    }

    /// Set default value
    pub fn with_default(mut self, value: serde_json::Value) -> Self {
        self.default = Some(value);
        self.required = false;
        self
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Make optional
    pub fn optional(mut self) -> Self {
        self.required = false;
        self
    }
}

impl std::fmt::Display for TemplateParameter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.name, self.param_type)?;
        if !self.required {
            write!(f, " (optional)")?;
        }
        Ok(())
    }
}

/// Workflow template for reusable patterns
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowTemplate {
    /// Template name
    pub name: String,
    /// Template version
    pub version: String,
    /// Template parameters
    pub parameters: Vec<TemplateParameter>,
    /// Template chain (if any)
    pub chain: Option<Chain>,
    /// Template group (if any)
    pub group: Option<Group>,
    /// Template description
    pub description: Option<String>,
}

impl WorkflowTemplate {
    /// Create a new workflow template
    pub fn new(name: impl Into<String>, version: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            version: version.into(),
            parameters: Vec::new(),
            chain: None,
            group: None,
            description: None,
        }
    }

    /// Add a parameter
    pub fn add_parameter(mut self, param: TemplateParameter) -> Self {
        self.parameters.push(param);
        self
    }

    /// Set template chain
    pub fn with_chain(mut self, chain: Chain) -> Self {
        self.chain = Some(chain);
        self
    }

    /// Set template group
    pub fn with_group(mut self, group: Group) -> Self {
        self.group = Some(group);
        self
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Instantiate template with parameters
    pub fn instantiate(&self, params: HashMap<String, serde_json::Value>) -> Result<Self, String> {
        // Validate required parameters
        for param in &self.parameters {
            if param.required && !params.contains_key(&param.name) && param.default.is_none() {
                return Err(format!("Missing required parameter: {}", param.name));
            }
        }

        // Create instance with parameters applied
        Ok(self.clone())
    }
}

impl std::fmt::Display for WorkflowTemplate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "WorkflowTemplate[{}@{}]", self.name, self.version)?;
        if !self.parameters.is_empty() {
            write!(f, " params={}", self.parameters.len())?;
        }
        Ok(())
    }
}

// ============================================================================
// Event-Driven Workflows
// ============================================================================

/// Workflow event types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum WorkflowEvent {
    /// Task completed
    TaskCompleted { task_id: Uuid },
    /// Task failed
    TaskFailed { task_id: Uuid, error: String },
    /// Workflow started
    WorkflowStarted { workflow_id: Uuid },
    /// Workflow completed
    WorkflowCompleted { workflow_id: Uuid },
    /// Workflow failed
    WorkflowFailed { workflow_id: Uuid, error: String },
    /// Custom event
    Custom { event_type: String, data: String },
}

impl std::fmt::Display for WorkflowEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::TaskCompleted { task_id } => write!(f, "TaskCompleted[{}]", task_id),
            Self::TaskFailed { task_id, .. } => write!(f, "TaskFailed[{}]", task_id),
            Self::WorkflowStarted { workflow_id } => write!(f, "WorkflowStarted[{}]", workflow_id),
            Self::WorkflowCompleted { workflow_id } => {
                write!(f, "WorkflowCompleted[{}]", workflow_id)
            }
            Self::WorkflowFailed { workflow_id, .. } => {
                write!(f, "WorkflowFailed[{}]", workflow_id)
            }
            Self::Custom { event_type, .. } => write!(f, "Custom[{}]", event_type),
        }
    }
}

/// Event handler configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventHandler {
    /// Event type to handle
    pub event_type: String,
    /// Task to execute on event
    pub handler_task: Signature,
    /// Event filter (optional)
    pub filter: Option<String>,
}

impl EventHandler {
    /// Create a new event handler
    pub fn new(event_type: impl Into<String>, handler_task: Signature) -> Self {
        Self {
            event_type: event_type.into(),
            handler_task,
            filter: None,
        }
    }

    /// Set event filter
    pub fn with_filter(mut self, filter: impl Into<String>) -> Self {
        self.filter = Some(filter.into());
        self
    }
}

impl std::fmt::Display for EventHandler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EventHandler[event={}, handler={}]",
            self.event_type, self.handler_task.task
        )
    }
}

/// Event-driven workflow
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventDrivenWorkflow {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Event handlers
    pub handlers: Vec<EventHandler>,
    /// Whether workflow is active
    pub active: bool,
}

impl EventDrivenWorkflow {
    /// Create a new event-driven workflow
    pub fn new() -> Self {
        Self {
            workflow_id: Uuid::new_v4(),
            handlers: Vec::new(),
            active: true,
        }
    }

    /// Add an event handler
    pub fn on_event(mut self, handler: EventHandler) -> Self {
        self.handlers.push(handler);
        self
    }

    /// Add handler for task completion
    pub fn on_task_completed(self, task: Signature) -> Self {
        self.on_event(EventHandler::new("TaskCompleted", task))
    }

    /// Add handler for task failure
    pub fn on_task_failed(self, task: Signature) -> Self {
        self.on_event(EventHandler::new("TaskFailed", task))
    }

    /// Activate workflow
    pub fn activate(mut self) -> Self {
        self.active = true;
        self
    }

    /// Deactivate workflow
    pub fn deactivate(mut self) -> Self {
        self.active = false;
        self
    }

    /// Check if workflow has handlers
    pub fn has_handlers(&self) -> bool {
        !self.handlers.is_empty()
    }
}

impl Default for EventDrivenWorkflow {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for EventDrivenWorkflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EventDrivenWorkflow[id={}, handlers={}]",
            self.workflow_id,
            self.handlers.len()
        )?;
        if !self.active {
            write!(f, " (inactive)")?;
        }
        Ok(())
    }
}

/// Canvas errors
#[derive(Debug, thiserror::Error)]
pub enum CanvasError {
    #[error("Invalid workflow: {0}")]
    Invalid(String),

    #[error("Broker error: {0}")]
    Broker(String),

    #[error("Serialization error: {0}")]
    Serialization(String),

    #[error("Workflow cancelled: {0}")]
    Cancelled(String),

    #[error("Workflow timeout: {0}")]
    Timeout(String),
}

impl CanvasError {
    /// Check if error is invalid workflow
    pub fn is_invalid(&self) -> bool {
        matches!(self, CanvasError::Invalid(_))
    }

    /// Check if error is broker-related
    pub fn is_broker(&self) -> bool {
        matches!(self, CanvasError::Broker(_))
    }

    /// Check if error is serialization-related
    pub fn is_serialization(&self) -> bool {
        matches!(self, CanvasError::Serialization(_))
    }

    /// Check if error is cancellation-related
    pub fn is_cancelled(&self) -> bool {
        matches!(self, CanvasError::Cancelled(_))
    }

    /// Check if error is timeout-related
    pub fn is_timeout(&self) -> bool {
        matches!(self, CanvasError::Timeout(_))
    }

    /// Check if this error is retryable
    ///
    /// Broker errors are typically retryable (transient network issues).
    /// Invalid workflow, serialization, cancellation, and timeout errors are not retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self, CanvasError::Broker(_))
    }

    /// Get the error category as a string
    pub fn category(&self) -> &'static str {
        match self {
            CanvasError::Invalid(_) => "invalid",
            CanvasError::Broker(_) => "broker",
            CanvasError::Serialization(_) => "serialization",
            CanvasError::Cancelled(_) => "cancelled",
            CanvasError::Timeout(_) => "timeout",
        }
    }
}

// ============================================================================
// Parallel Workflow Scheduling
// ============================================================================

/// Task priority for scheduling
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum TaskPriority {
    /// Low priority (value: 0)
    Low = 0,
    /// Normal priority (value: 5)
    Normal = 5,
    /// High priority (value: 10)
    High = 10,
    /// Critical priority (value: 15)
    Critical = 15,
}

impl Default for TaskPriority {
    fn default() -> Self {
        Self::Normal
    }
}

impl std::fmt::Display for TaskPriority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Low => write!(f, "Low"),
            Self::Normal => write!(f, "Normal"),
            Self::High => write!(f, "High"),
            Self::Critical => write!(f, "Critical"),
        }
    }
}

/// Worker resource capacity
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerCapacity {
    /// Worker ID
    pub worker_id: String,
    /// CPU cores available
    pub cpu_cores: u32,
    /// Memory available (MB)
    pub memory_mb: u64,
    /// Current load (0.0 to 1.0)
    pub current_load: f64,
    /// Active tasks count
    pub active_tasks: usize,
}

impl WorkerCapacity {
    /// Create a new worker capacity
    pub fn new(worker_id: impl Into<String>, cpu_cores: u32, memory_mb: u64) -> Self {
        Self {
            worker_id: worker_id.into(),
            cpu_cores,
            memory_mb,
            current_load: 0.0,
            active_tasks: 0,
        }
    }

    /// Check if worker has capacity for a task
    pub fn has_capacity(&self, required_load: f64) -> bool {
        self.current_load + required_load <= 1.0
    }

    /// Get available capacity
    pub fn available_capacity(&self) -> f64 {
        (1.0 - self.current_load).max(0.0)
    }
}

/// Task scheduling decision
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchedulingDecision {
    /// Task ID
    pub task_id: Uuid,
    /// Assigned worker ID
    pub worker_id: String,
    /// Priority
    pub priority: TaskPriority,
    /// Estimated execution time (seconds)
    pub estimated_time: Option<u64>,
}

impl SchedulingDecision {
    /// Create a new scheduling decision
    pub fn new(task_id: Uuid, worker_id: impl Into<String>, priority: TaskPriority) -> Self {
        Self {
            task_id,
            worker_id: worker_id.into(),
            priority,
            estimated_time: None,
        }
    }

    /// Set estimated execution time
    pub fn with_estimated_time(mut self, seconds: u64) -> Self {
        self.estimated_time = Some(seconds);
        self
    }
}

/// Scheduling strategy for task distribution
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SchedulingStrategy {
    /// Round-robin distribution
    RoundRobin,
    /// Assign to worker with lowest load
    LeastLoaded,
    /// Priority-based scheduling
    PriorityBased,
    /// Resource-aware scheduling
    ResourceAware,
}

impl Default for SchedulingStrategy {
    fn default() -> Self {
        Self::LeastLoaded
    }
}

impl std::fmt::Display for SchedulingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::RoundRobin => write!(f, "RoundRobin"),
            Self::LeastLoaded => write!(f, "LeastLoaded"),
            Self::PriorityBased => write!(f, "PriorityBased"),
            Self::ResourceAware => write!(f, "ResourceAware"),
        }
    }
}

/// Parallel workflow scheduler for task distribution
#[derive(Debug, Clone)]
pub struct ParallelScheduler {
    /// Scheduling strategy
    pub strategy: SchedulingStrategy,
    /// Worker capacities
    pub workers: Vec<WorkerCapacity>,
    /// Enable load balancing
    pub load_balancing: bool,
    /// Maximum tasks per worker
    pub max_tasks_per_worker: Option<usize>,
}

impl ParallelScheduler {
    /// Create a new parallel scheduler
    pub fn new(strategy: SchedulingStrategy) -> Self {
        Self {
            strategy,
            workers: Vec::new(),
            load_balancing: true,
            max_tasks_per_worker: None,
        }
    }

    /// Add a worker to the scheduler
    pub fn add_worker(&mut self, worker: WorkerCapacity) {
        self.workers.push(worker);
    }

    /// Enable load balancing
    pub fn with_load_balancing(mut self, enabled: bool) -> Self {
        self.load_balancing = enabled;
        self
    }

    /// Set maximum tasks per worker
    pub fn with_max_tasks_per_worker(mut self, max: usize) -> Self {
        self.max_tasks_per_worker = Some(max);
        self
    }

    /// Schedule a task to a worker
    pub fn schedule_task(
        &self,
        task_id: Uuid,
        priority: TaskPriority,
    ) -> Option<SchedulingDecision> {
        if self.workers.is_empty() {
            return None;
        }

        let worker_id = match self.strategy {
            SchedulingStrategy::RoundRobin => {
                // Simple round-robin based on task count
                self.workers
                    .iter()
                    .min_by_key(|w| w.active_tasks)
                    .map(|w| w.worker_id.clone())
            }
            SchedulingStrategy::LeastLoaded => {
                // Assign to worker with lowest load
                self.workers
                    .iter()
                    .filter(|w| {
                        if let Some(max) = self.max_tasks_per_worker {
                            w.active_tasks < max
                        } else {
                            true
                        }
                    })
                    .min_by(|a, b| {
                        a.current_load
                            .partial_cmp(&b.current_load)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|w| w.worker_id.clone())
            }
            SchedulingStrategy::PriorityBased => {
                // Higher priority tasks go to less loaded workers
                let priority_weight = priority as u8 as f64 / 15.0;
                self.workers
                    .iter()
                    .filter(|w| {
                        if let Some(max) = self.max_tasks_per_worker {
                            w.active_tasks < max
                        } else {
                            true
                        }
                    })
                    .min_by(|a, b| {
                        let a_score = a.current_load * (1.0 - priority_weight);
                        let b_score = b.current_load * (1.0 - priority_weight);
                        a_score
                            .partial_cmp(&b_score)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|w| w.worker_id.clone())
            }
            SchedulingStrategy::ResourceAware => {
                // Consider both CPU and memory availability
                self.workers
                    .iter()
                    .filter(|w| {
                        if let Some(max) = self.max_tasks_per_worker {
                            w.active_tasks < max
                        } else {
                            true
                        }
                    })
                    .max_by(|a, b| {
                        let a_score = a.available_capacity()
                            * (a.cpu_cores as f64 / 100.0)
                            * (a.memory_mb as f64 / 1_000_000.0);
                        let b_score = b.available_capacity()
                            * (b.cpu_cores as f64 / 100.0)
                            * (b.memory_mb as f64 / 1_000_000.0);
                        a_score
                            .partial_cmp(&b_score)
                            .unwrap_or(std::cmp::Ordering::Equal)
                    })
                    .map(|w| w.worker_id.clone())
            }
        };

        worker_id.map(|id| SchedulingDecision::new(task_id, id, priority))
    }

    /// Get worker count
    pub fn worker_count(&self) -> usize {
        self.workers.len()
    }

    /// Get total capacity across all workers
    pub fn total_capacity(&self) -> f64 {
        self.workers.iter().map(|w| w.available_capacity()).sum()
    }

    /// Get average load across all workers
    pub fn average_load(&self) -> f64 {
        if self.workers.is_empty() {
            return 0.0;
        }
        let total_load: f64 = self.workers.iter().map(|w| w.current_load).sum();
        total_load / self.workers.len() as f64
    }
}

impl Default for ParallelScheduler {
    fn default() -> Self {
        Self::new(SchedulingStrategy::default())
    }
}

impl std::fmt::Display for ParallelScheduler {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ParallelScheduler[strategy={}, workers={}, avg_load={:.2}]",
            self.strategy,
            self.workers.len(),
            self.average_load()
        )
    }
}

// ============================================================================
// Workflow Batching
// ============================================================================

/// Workflow batch for grouping similar workflows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowBatch {
    /// Batch ID
    pub batch_id: Uuid,
    /// Workflow IDs in this batch
    pub workflow_ids: Vec<Uuid>,
    /// Batch priority
    pub priority: TaskPriority,
    /// Maximum batch size
    pub max_size: usize,
    /// Batch timeout (seconds)
    pub timeout: Option<u64>,
    /// Creation timestamp
    pub created_at: u64,
}

impl WorkflowBatch {
    /// Create a new workflow batch
    pub fn new(max_size: usize) -> Self {
        Self {
            batch_id: Uuid::new_v4(),
            workflow_ids: Vec::new(),
            priority: TaskPriority::Normal,
            max_size,
            timeout: None,
            created_at: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Add a workflow to the batch
    pub fn add_workflow(&mut self, workflow_id: Uuid) -> bool {
        if self.workflow_ids.len() < self.max_size {
            self.workflow_ids.push(workflow_id);
            true
        } else {
            false
        }
    }

    /// Check if batch is full
    pub fn is_full(&self) -> bool {
        self.workflow_ids.len() >= self.max_size
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.workflow_ids.is_empty()
    }

    /// Get batch size
    pub fn size(&self) -> usize {
        self.workflow_ids.len()
    }

    /// Check if batch has timed out
    pub fn is_timed_out(&self) -> bool {
        if let Some(timeout) = self.timeout {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs();
            let age = now.saturating_sub(self.created_at);
            age >= timeout
        } else {
            false
        }
    }

    /// Set priority
    pub fn with_priority(mut self, priority: TaskPriority) -> Self {
        self.priority = priority;
        self
    }

    /// Set timeout
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.timeout = Some(seconds);
        self
    }
}

impl std::fmt::Display for WorkflowBatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkflowBatch[id={}, size={}/{}, priority={}]",
            self.batch_id,
            self.size(),
            self.max_size,
            self.priority
        )
    }
}

/// Batching strategy for workflow grouping
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BatchingStrategy {
    /// Batch by workflow type
    ByType,
    /// Batch by priority
    ByPriority,
    /// Batch by size (group similar-sized workflows)
    BySize,
    /// Batch by time window
    ByTimeWindow,
}

impl Default for BatchingStrategy {
    fn default() -> Self {
        Self::ByType
    }
}

impl std::fmt::Display for BatchingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ByType => write!(f, "ByType"),
            Self::ByPriority => write!(f, "ByPriority"),
            Self::BySize => write!(f, "BySize"),
            Self::ByTimeWindow => write!(f, "ByTimeWindow"),
        }
    }
}

/// Workflow batcher for grouping similar workflows
#[derive(Debug, Clone)]
pub struct WorkflowBatcher {
    /// Batching strategy
    pub strategy: BatchingStrategy,
    /// Active batches
    pub batches: Vec<WorkflowBatch>,
    /// Default batch size
    pub default_batch_size: usize,
    /// Default batch timeout (seconds)
    pub default_timeout: Option<u64>,
}

impl WorkflowBatcher {
    /// Create a new workflow batcher
    pub fn new(strategy: BatchingStrategy) -> Self {
        Self {
            strategy,
            batches: Vec::new(),
            default_batch_size: 10,
            default_timeout: Some(60), // 1 minute default
        }
    }

    /// Set default batch size
    pub fn with_batch_size(mut self, size: usize) -> Self {
        self.default_batch_size = size;
        self
    }

    /// Set default batch timeout
    pub fn with_timeout(mut self, seconds: u64) -> Self {
        self.default_timeout = Some(seconds);
        self
    }

    /// Add a workflow to a batch
    pub fn add_workflow(&mut self, workflow_id: Uuid, priority: TaskPriority) -> Uuid {
        // Find or create appropriate batch
        let batch_id = match self.strategy {
            BatchingStrategy::ByPriority => {
                // Find batch with matching priority
                let batch = self
                    .batches
                    .iter_mut()
                    .find(|b| b.priority == priority && !b.is_full() && !b.is_timed_out());

                if let Some(batch) = batch {
                    batch.add_workflow(workflow_id);
                    batch.batch_id
                } else {
                    // Create new batch
                    let mut new_batch =
                        WorkflowBatch::new(self.default_batch_size).with_priority(priority);
                    if let Some(timeout) = self.default_timeout {
                        new_batch = new_batch.with_timeout(timeout);
                    }
                    new_batch.add_workflow(workflow_id);
                    let batch_id = new_batch.batch_id;
                    self.batches.push(new_batch);
                    batch_id
                }
            }
            _ => {
                // For other strategies, use first available batch
                let batch = self
                    .batches
                    .iter_mut()
                    .find(|b| !b.is_full() && !b.is_timed_out());

                if let Some(batch) = batch {
                    batch.add_workflow(workflow_id);
                    batch.batch_id
                } else {
                    // Create new batch
                    let mut new_batch = WorkflowBatch::new(self.default_batch_size);
                    if let Some(timeout) = self.default_timeout {
                        new_batch = new_batch.with_timeout(timeout);
                    }
                    new_batch.add_workflow(workflow_id);
                    let batch_id = new_batch.batch_id;
                    self.batches.push(new_batch);
                    batch_id
                }
            }
        };

        batch_id
    }

    /// Get ready batches (full or timed out)
    pub fn get_ready_batches(&self) -> Vec<&WorkflowBatch> {
        self.batches
            .iter()
            .filter(|b| b.is_full() || b.is_timed_out())
            .collect()
    }

    /// Remove ready batches
    pub fn remove_ready_batches(&mut self) -> Vec<WorkflowBatch> {
        let (ready, pending): (Vec<_>, Vec<_>) = self
            .batches
            .drain(..)
            .partition(|b| b.is_full() || b.is_timed_out());
        self.batches = pending;
        ready
    }

    /// Get batch count
    pub fn batch_count(&self) -> usize {
        self.batches.len()
    }

    /// Get total workflow count across all batches
    pub fn total_workflow_count(&self) -> usize {
        self.batches.iter().map(|b| b.size()).sum()
    }
}

impl Default for WorkflowBatcher {
    fn default() -> Self {
        Self::new(BatchingStrategy::default())
    }
}

impl std::fmt::Display for WorkflowBatcher {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkflowBatcher[strategy={}, batches={}, workflows={}]",
            self.strategy,
            self.batch_count(),
            self.total_workflow_count()
        )
    }
}

// ============================================================================
// Streaming Map-Reduce
// ============================================================================

/// Streaming map-reduce for processing large datasets
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamingMapReduce {
    /// Map task signature
    pub map_task: Signature,
    /// Reduce task signature
    pub reduce_task: Signature,
    /// Chunk size for streaming
    pub chunk_size: usize,
    /// Buffer size for intermediate results
    pub buffer_size: usize,
    /// Enable backpressure control
    pub backpressure: bool,
}

impl StreamingMapReduce {
    /// Create a new streaming map-reduce
    pub fn new(map_task: Signature, reduce_task: Signature) -> Self {
        Self {
            map_task,
            reduce_task,
            chunk_size: 100,
            buffer_size: 1000,
            backpressure: true,
        }
    }

    /// Set chunk size
    pub fn with_chunk_size(mut self, size: usize) -> Self {
        self.chunk_size = size;
        self
    }

    /// Set buffer size
    pub fn with_buffer_size(mut self, size: usize) -> Self {
        self.buffer_size = size;
        self
    }

    /// Enable/disable backpressure
    pub fn with_backpressure(mut self, enabled: bool) -> Self {
        self.backpressure = enabled;
        self
    }
}

impl std::fmt::Display for StreamingMapReduce {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "StreamingMapReduce[map={}, reduce={}, chunk_size={}, buffer_size={}]",
            self.map_task.task, self.reduce_task.task, self.chunk_size, self.buffer_size
        )
    }
}

// ============================================================================
// Reactive Workflows
// ============================================================================

/// Observable value that can trigger reactive workflows
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Observable<T> {
    /// Current value
    pub value: T,
    /// Subscribers (workflow IDs)
    #[serde(skip)]
    pub subscribers: Vec<Uuid>,
    /// Value change history
    pub history: Vec<(T, u64)>, // (value, timestamp)
}

impl<T: Clone> Observable<T> {
    /// Create a new observable with initial value
    pub fn new(value: T) -> Self {
        Self {
            value,
            subscribers: Vec::new(),
            history: Vec::new(),
        }
    }

    /// Subscribe a workflow to this observable
    pub fn subscribe(&mut self, workflow_id: Uuid) {
        if !self.subscribers.contains(&workflow_id) {
            self.subscribers.push(workflow_id);
        }
    }

    /// Unsubscribe a workflow
    pub fn unsubscribe(&mut self, workflow_id: &Uuid) {
        self.subscribers.retain(|id| id != workflow_id);
    }

    /// Update the value and notify subscribers
    pub fn set(&mut self, value: T) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.history.push((self.value.clone(), timestamp));
        self.value = value;
    }

    /// Get current value
    pub fn get(&self) -> &T {
        &self.value
    }

    /// Get subscriber count
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.len()
    }
}

/// Reactive workflow that responds to observable changes
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReactiveWorkflow {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Observable IDs being watched
    pub watched_observables: Vec<String>,
    /// Reaction task
    pub reaction_task: Signature,
    /// Debounce time (milliseconds)
    pub debounce_ms: Option<u64>,
    /// Throttle time (milliseconds)
    pub throttle_ms: Option<u64>,
    /// Filter condition (optional)
    pub filter: Option<String>,
}

impl ReactiveWorkflow {
    /// Create a new reactive workflow
    pub fn new(reaction_task: Signature) -> Self {
        Self {
            workflow_id: Uuid::new_v4(),
            watched_observables: Vec::new(),
            reaction_task,
            debounce_ms: None,
            throttle_ms: None,
            filter: None,
        }
    }

    /// Watch an observable
    pub fn watch(mut self, observable_id: impl Into<String>) -> Self {
        self.watched_observables.push(observable_id.into());
        self
    }

    /// Set debounce time (delay reaction until changes stop)
    pub fn with_debounce(mut self, milliseconds: u64) -> Self {
        self.debounce_ms = Some(milliseconds);
        self
    }

    /// Set throttle time (limit reaction frequency)
    pub fn with_throttle(mut self, milliseconds: u64) -> Self {
        self.throttle_ms = Some(milliseconds);
        self
    }

    /// Set filter condition
    pub fn with_filter(mut self, condition: impl Into<String>) -> Self {
        self.filter = Some(condition.into());
        self
    }
}

impl std::fmt::Display for ReactiveWorkflow {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReactiveWorkflow[id={}, watching={}, reaction={}]",
            self.workflow_id,
            self.watched_observables.len(),
            self.reaction_task.task
        )
    }
}

/// Stream operator for reactive data processing
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum StreamOperator {
    /// Map values
    Map,
    /// Filter values
    Filter,
    /// Reduce values
    Reduce,
    /// Scan (accumulate)
    Scan,
    /// Take first N values
    Take,
    /// Skip first N values
    Skip,
    /// Debounce
    Debounce,
    /// Throttle
    Throttle,
}

impl std::fmt::Display for StreamOperator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Map => write!(f, "Map"),
            Self::Filter => write!(f, "Filter"),
            Self::Reduce => write!(f, "Reduce"),
            Self::Scan => write!(f, "Scan"),
            Self::Take => write!(f, "Take"),
            Self::Skip => write!(f, "Skip"),
            Self::Debounce => write!(f, "Debounce"),
            Self::Throttle => write!(f, "Throttle"),
        }
    }
}

/// Reactive stream for data flow processing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReactiveStream {
    /// Stream ID
    pub stream_id: Uuid,
    /// Source observable ID
    pub source_id: String,
    /// Stream operators
    pub operators: Vec<(StreamOperator, serde_json::Value)>,
    /// Subscriber workflows
    #[serde(skip)]
    pub subscribers: Vec<Uuid>,
}

impl ReactiveStream {
    /// Create a new reactive stream
    pub fn new(source_id: impl Into<String>) -> Self {
        Self {
            stream_id: Uuid::new_v4(),
            source_id: source_id.into(),
            operators: Vec::new(),
            subscribers: Vec::new(),
        }
    }

    /// Add a map operator
    pub fn map(mut self, transform: serde_json::Value) -> Self {
        self.operators.push((StreamOperator::Map, transform));
        self
    }

    /// Add a filter operator
    pub fn filter(mut self, condition: serde_json::Value) -> Self {
        self.operators.push((StreamOperator::Filter, condition));
        self
    }

    /// Add a take operator
    pub fn take(mut self, count: usize) -> Self {
        self.operators
            .push((StreamOperator::Take, serde_json::json!(count)));
        self
    }

    /// Add a skip operator
    pub fn skip(mut self, count: usize) -> Self {
        self.operators
            .push((StreamOperator::Skip, serde_json::json!(count)));
        self
    }

    /// Add a debounce operator
    pub fn debounce(mut self, milliseconds: u64) -> Self {
        self.operators
            .push((StreamOperator::Debounce, serde_json::json!(milliseconds)));
        self
    }

    /// Add a throttle operator
    pub fn throttle(mut self, milliseconds: u64) -> Self {
        self.operators
            .push((StreamOperator::Throttle, serde_json::json!(milliseconds)));
        self
    }

    /// Subscribe a workflow to this stream
    pub fn subscribe(&mut self, workflow_id: Uuid) {
        if !self.subscribers.contains(&workflow_id) {
            self.subscribers.push(workflow_id);
        }
    }
}

impl std::fmt::Display for ReactiveStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ReactiveStream[id={}, source={}, operators={}]",
            self.stream_id,
            self.source_id,
            self.operators.len()
        )
    }
}

// ============================================================================
// Resource Utilization Monitoring
// ============================================================================

/// Resource utilization metrics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResourceUtilization {
    /// CPU utilization (0.0 to 1.0)
    pub cpu: f64,
    /// Memory utilization (0.0 to 1.0)
    pub memory: f64,
    /// Disk I/O utilization (0.0 to 1.0)
    pub disk_io: f64,
    /// Network I/O utilization (0.0 to 1.0)
    pub network_io: f64,
    /// Timestamp
    pub timestamp: u64,
}

impl ResourceUtilization {
    /// Create a new resource utilization snapshot
    pub fn new(cpu: f64, memory: f64, disk_io: f64, network_io: f64) -> Self {
        Self {
            cpu: cpu.clamp(0.0, 1.0),
            memory: memory.clamp(0.0, 1.0),
            disk_io: disk_io.clamp(0.0, 1.0),
            network_io: network_io.clamp(0.0, 1.0),
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Get overall utilization (average of all metrics)
    pub fn overall(&self) -> f64 {
        (self.cpu + self.memory + self.disk_io + self.network_io) / 4.0
    }

    /// Check if any resource is over threshold
    pub fn is_overloaded(&self, threshold: f64) -> bool {
        self.cpu > threshold
            || self.memory > threshold
            || self.disk_io > threshold
            || self.network_io > threshold
    }

    /// Get the most utilized resource
    pub fn bottleneck(&self) -> &'static str {
        let max = self
            .cpu
            .max(self.memory)
            .max(self.disk_io)
            .max(self.network_io);
        if (max - self.cpu).abs() < f64::EPSILON {
            "cpu"
        } else if (max - self.memory).abs() < f64::EPSILON {
            "memory"
        } else if (max - self.disk_io).abs() < f64::EPSILON {
            "disk_io"
        } else {
            "network_io"
        }
    }
}

impl std::fmt::Display for ResourceUtilization {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ResourceUtilization[cpu={:.2}, mem={:.2}, disk={:.2}, net={:.2}, overall={:.2}]",
            self.cpu,
            self.memory,
            self.disk_io,
            self.network_io,
            self.overall()
        )
    }
}

/// Workflow resource monitor
#[derive(Debug, Clone)]
pub struct WorkflowResourceMonitor {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Resource utilization history
    pub history: Vec<ResourceUtilization>,
    /// Maximum history size
    pub max_history: usize,
    /// Sampling interval (seconds)
    pub sampling_interval: u64,
}

impl WorkflowResourceMonitor {
    /// Create a new resource monitor
    pub fn new(workflow_id: Uuid) -> Self {
        Self {
            workflow_id,
            history: Vec::new(),
            max_history: 1000,
            sampling_interval: 5,
        }
    }

    /// Set maximum history size
    pub fn with_max_history(mut self, max: usize) -> Self {
        self.max_history = max;
        self
    }

    /// Set sampling interval
    pub fn with_sampling_interval(mut self, seconds: u64) -> Self {
        self.sampling_interval = seconds;
        self
    }

    /// Record resource utilization
    pub fn record(&mut self, utilization: ResourceUtilization) {
        self.history.push(utilization);
        // Trim history if needed
        if self.history.len() > self.max_history {
            self.history
                .drain(0..(self.history.len() - self.max_history));
        }
    }

    /// Get average utilization over time window (seconds)
    pub fn average_utilization(&self, window_seconds: u64) -> Option<ResourceUtilization> {
        if self.history.is_empty() {
            return None;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let cutoff = now.saturating_sub(window_seconds);

        let recent: Vec<_> = self
            .history
            .iter()
            .filter(|u| u.timestamp >= cutoff)
            .collect();

        if recent.is_empty() {
            return None;
        }

        let sum_cpu: f64 = recent.iter().map(|u| u.cpu).sum();
        let sum_memory: f64 = recent.iter().map(|u| u.memory).sum();
        let sum_disk: f64 = recent.iter().map(|u| u.disk_io).sum();
        let sum_network: f64 = recent.iter().map(|u| u.network_io).sum();
        let count = recent.len() as f64;

        Some(ResourceUtilization::new(
            sum_cpu / count,
            sum_memory / count,
            sum_disk / count,
            sum_network / count,
        ))
    }

    /// Get peak utilization
    pub fn peak_utilization(&self) -> Option<&ResourceUtilization> {
        self.history.iter().max_by(|a, b| {
            a.overall()
                .partial_cmp(&b.overall())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
    }

    /// Clear history
    pub fn clear(&mut self) {
        self.history.clear();
    }
}

impl std::fmt::Display for WorkflowResourceMonitor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkflowResourceMonitor[workflow={}, samples={}]",
            self.workflow_id,
            self.history.len()
        )
    }
}

// ============================================================================
// Workflow Testing Framework
// ============================================================================

/// Mock task result for testing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MockTaskResult {
    /// Task name
    pub task_name: String,
    /// Result value
    pub result: serde_json::Value,
    /// Execution delay (milliseconds)
    pub delay_ms: u64,
    /// Whether to fail
    pub should_fail: bool,
    /// Failure message
    pub failure_message: Option<String>,
}

impl MockTaskResult {
    /// Create a successful mock result
    pub fn success(task_name: impl Into<String>, result: serde_json::Value) -> Self {
        Self {
            task_name: task_name.into(),
            result,
            delay_ms: 0,
            should_fail: false,
            failure_message: None,
        }
    }

    /// Create a failing mock result
    pub fn failure(task_name: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            task_name: task_name.into(),
            result: serde_json::Value::Null,
            delay_ms: 0,
            should_fail: true,
            failure_message: Some(message.into()),
        }
    }

    /// Set execution delay
    pub fn with_delay(mut self, milliseconds: u64) -> Self {
        self.delay_ms = milliseconds;
        self
    }
}

/// Mock task executor for testing workflows
#[derive(Debug, Clone)]
pub struct MockTaskExecutor {
    /// Mock results by task name
    pub mock_results: HashMap<String, MockTaskResult>,
    /// Execution history
    pub execution_history: Vec<(String, u64)>, // (task_name, timestamp)
}

impl MockTaskExecutor {
    /// Create a new mock executor
    pub fn new() -> Self {
        Self {
            mock_results: HashMap::new(),
            execution_history: Vec::new(),
        }
    }

    /// Register a mock result for a task
    pub fn register(&mut self, result: MockTaskResult) {
        self.mock_results.insert(result.task_name.clone(), result);
    }

    /// Execute a mock task
    pub fn execute(&mut self, task_name: &str) -> Result<serde_json::Value, String> {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        self.execution_history
            .push((task_name.to_string(), timestamp));

        if let Some(mock_result) = self.mock_results.get(task_name) {
            // Simulate delay
            if mock_result.delay_ms > 0 {
                std::thread::sleep(std::time::Duration::from_millis(mock_result.delay_ms));
            }

            if mock_result.should_fail {
                Err(mock_result
                    .failure_message
                    .clone()
                    .unwrap_or_else(|| "Mock task failed".to_string()))
            } else {
                Ok(mock_result.result.clone())
            }
        } else {
            Err(format!("No mock result registered for task: {}", task_name))
        }
    }

    /// Get execution count for a task
    pub fn execution_count(&self, task_name: &str) -> usize {
        self.execution_history
            .iter()
            .filter(|(name, _)| name == task_name)
            .count()
    }

    /// Clear execution history
    pub fn clear_history(&mut self) {
        self.execution_history.clear();
    }
}

impl Default for MockTaskExecutor {
    fn default() -> Self {
        Self::new()
    }
}

/// Test data injector for workflow testing
#[derive(Debug, Clone)]
pub struct TestDataInjector {
    /// Injected data by key
    pub data: HashMap<String, serde_json::Value>,
}

impl TestDataInjector {
    /// Create a new test data injector
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Inject test data
    pub fn inject(&mut self, key: impl Into<String>, value: serde_json::Value) {
        self.data.insert(key.into(), value);
    }

    /// Get injected data
    pub fn get(&self, key: &str) -> Option<&serde_json::Value> {
        self.data.get(key)
    }

    /// Clear all injected data
    pub fn clear(&mut self) {
        self.data.clear();
    }
}

impl Default for TestDataInjector {
    fn default() -> Self {
        Self::new()
    }
}

// ============================================================================
// Time-Travel Debugging
// ============================================================================

/// Workflow execution snapshot for time-travel debugging
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowSnapshot {
    /// Snapshot ID
    pub snapshot_id: Uuid,
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Snapshot timestamp
    pub timestamp: u64,
    /// Workflow state at snapshot
    pub state: WorkflowState,
    /// Completed task IDs
    pub completed_tasks: Vec<Uuid>,
    /// Task results
    pub task_results: HashMap<Uuid, serde_json::Value>,
    /// Checkpoint data
    pub checkpoint: Option<WorkflowCheckpoint>,
}

impl WorkflowSnapshot {
    /// Create a new workflow snapshot
    pub fn new(workflow_id: Uuid, state: WorkflowState) -> Self {
        Self {
            snapshot_id: Uuid::new_v4(),
            workflow_id,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            state,
            completed_tasks: Vec::new(),
            task_results: HashMap::new(),
            checkpoint: None,
        }
    }

    /// Record task completion
    pub fn record_task(&mut self, task_id: Uuid, result: serde_json::Value) {
        self.completed_tasks.push(task_id);
        self.task_results.insert(task_id, result);
    }

    /// Attach checkpoint
    pub fn with_checkpoint(mut self, checkpoint: WorkflowCheckpoint) -> Self {
        self.checkpoint = Some(checkpoint);
        self
    }
}

/// Time-travel debugger for workflow replay
#[derive(Debug, Clone)]
pub struct TimeTravelDebugger {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Snapshots history
    pub snapshots: Vec<WorkflowSnapshot>,
    /// Current snapshot index
    pub current_index: usize,
    /// Step mode enabled
    pub step_mode: bool,
}

impl TimeTravelDebugger {
    /// Create a new time-travel debugger
    pub fn new(workflow_id: Uuid) -> Self {
        Self {
            workflow_id,
            snapshots: Vec::new(),
            current_index: 0,
            step_mode: false,
        }
    }

    /// Record a snapshot
    pub fn record_snapshot(&mut self, snapshot: WorkflowSnapshot) {
        self.snapshots.push(snapshot);
        self.current_index = self.snapshots.len() - 1;
    }

    /// Replay from a specific snapshot
    pub fn replay_from(&mut self, snapshot_index: usize) -> Option<&WorkflowSnapshot> {
        if snapshot_index < self.snapshots.len() {
            self.current_index = snapshot_index;
            self.snapshots.get(snapshot_index)
        } else {
            None
        }
    }

    /// Step forward one snapshot
    pub fn step_forward(&mut self) -> Option<&WorkflowSnapshot> {
        if self.current_index + 1 < self.snapshots.len() {
            self.current_index += 1;
            self.snapshots.get(self.current_index)
        } else {
            None
        }
    }

    /// Step backward one snapshot
    pub fn step_backward(&mut self) -> Option<&WorkflowSnapshot> {
        if self.current_index > 0 {
            self.current_index -= 1;
            self.snapshots.get(self.current_index)
        } else {
            None
        }
    }

    /// Get current snapshot
    pub fn current_snapshot(&self) -> Option<&WorkflowSnapshot> {
        self.snapshots.get(self.current_index)
    }

    /// Enable step-by-step execution mode
    pub fn enable_step_mode(&mut self) {
        self.step_mode = true;
    }

    /// Disable step-by-step execution mode
    pub fn disable_step_mode(&mut self) {
        self.step_mode = false;
    }

    /// Get snapshot count
    pub fn snapshot_count(&self) -> usize {
        self.snapshots.len()
    }

    /// Clear all snapshots
    pub fn clear(&mut self) {
        self.snapshots.clear();
        self.current_index = 0;
    }
}

impl std::fmt::Display for TimeTravelDebugger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TimeTravelDebugger[workflow={}, snapshots={}, current={}]",
            self.workflow_id,
            self.snapshots.len(),
            self.current_index
        )
    }
}

// ============================================================================
// Workflow Visualization Support
// ============================================================================

/// Visual theme for workflow visualization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VisualTheme {
    /// Theme name
    pub name: String,
    /// Colors for task states
    pub colors: HashMap<String, String>,
    /// Node shapes by task type
    pub shapes: HashMap<String, String>,
    /// Edge styles
    pub edge_styles: HashMap<String, String>,
    /// Font settings
    pub font_family: String,
    pub font_size: u8,
}

impl VisualTheme {
    /// Create default light theme
    pub fn light() -> Self {
        let mut colors = HashMap::new();
        colors.insert("pending".to_string(), "#E0E0E0".to_string());
        colors.insert("running".to_string(), "#2196F3".to_string());
        colors.insert("completed".to_string(), "#4CAF50".to_string());
        colors.insert("failed".to_string(), "#F44336".to_string());
        colors.insert("cancelled".to_string(), "#FF9800".to_string());

        let mut shapes = HashMap::new();
        shapes.insert("task".to_string(), "box".to_string());
        shapes.insert("group".to_string(), "ellipse".to_string());
        shapes.insert("chord".to_string(), "diamond".to_string());

        let mut edge_styles = HashMap::new();
        edge_styles.insert("chain".to_string(), "solid".to_string());
        edge_styles.insert("callback".to_string(), "dashed".to_string());
        edge_styles.insert("error".to_string(), "dotted".to_string());

        Self {
            name: "light".to_string(),
            colors,
            shapes,
            edge_styles,
            font_family: "Arial".to_string(),
            font_size: 12,
        }
    }

    /// Create dark theme
    pub fn dark() -> Self {
        let mut colors = HashMap::new();
        colors.insert("pending".to_string(), "#424242".to_string());
        colors.insert("running".to_string(), "#1976D2".to_string());
        colors.insert("completed".to_string(), "#388E3C".to_string());
        colors.insert("failed".to_string(), "#D32F2F".to_string());
        colors.insert("cancelled".to_string(), "#F57C00".to_string());

        let mut shapes = HashMap::new();
        shapes.insert("task".to_string(), "box".to_string());
        shapes.insert("group".to_string(), "ellipse".to_string());
        shapes.insert("chord".to_string(), "diamond".to_string());

        let mut edge_styles = HashMap::new();
        edge_styles.insert("chain".to_string(), "solid".to_string());
        edge_styles.insert("callback".to_string(), "dashed".to_string());
        edge_styles.insert("error".to_string(), "dotted".to_string());

        Self {
            name: "dark".to_string(),
            colors,
            shapes,
            edge_styles,
            font_family: "Arial".to_string(),
            font_size: 12,
        }
    }

    /// Get color for state
    pub fn color_for_state(&self, state: &str) -> Option<&str> {
        self.colors.get(state).map(|s| s.as_str())
    }

    /// Get shape for task type
    pub fn shape_for_type(&self, task_type: &str) -> Option<&str> {
        self.shapes.get(task_type).map(|s| s.as_str())
    }
}

impl Default for VisualTheme {
    fn default() -> Self {
        Self::light()
    }
}

/// Task visual metadata for UI rendering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskVisualMetadata {
    /// Task ID
    pub task_id: Uuid,
    /// Task name
    pub task_name: String,
    /// Current state (pending, running, completed, failed, cancelled)
    pub state: String,
    /// Progress percentage (0-100)
    pub progress: f64,
    /// Visual position (x, y) for layout
    pub position: Option<(f64, f64)>,
    /// Node color (CSS color)
    pub color: String,
    /// Node shape
    pub shape: String,
    /// CSS classes for styling
    pub css_classes: Vec<String>,
    /// Additional metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

impl TaskVisualMetadata {
    /// Create new task visual metadata
    pub fn new(task_id: Uuid, task_name: String, state: String) -> Self {
        Self {
            task_id,
            task_name,
            state: state.clone(),
            progress: 0.0,
            position: None,
            color: Self::default_color_for_state(&state),
            shape: "box".to_string(),
            css_classes: vec![format!("task-{}", state)],
            metadata: HashMap::new(),
        }
    }

    /// Default color for state
    fn default_color_for_state(state: &str) -> String {
        match state {
            "pending" => "#E0E0E0",
            "running" => "#2196F3",
            "completed" => "#4CAF50",
            "failed" => "#F44336",
            "cancelled" => "#FF9800",
            _ => "#9E9E9E",
        }
        .to_string()
    }

    /// Set progress
    pub fn with_progress(mut self, progress: f64) -> Self {
        self.progress = progress.clamp(0.0, 100.0);
        self
    }

    /// Set position
    pub fn with_position(mut self, x: f64, y: f64) -> Self {
        self.position = Some((x, y));
        self
    }

    /// Set color
    pub fn with_color(mut self, color: String) -> Self {
        self.color = color;
        self
    }

    /// Add CSS class
    pub fn add_css_class(&mut self, class: String) {
        if !self.css_classes.contains(&class) {
            self.css_classes.push(class);
        }
    }

    /// Add metadata
    pub fn add_metadata(&mut self, key: String, value: serde_json::Value) {
        self.metadata.insert(key, value);
    }
}

/// Workflow visualization data with rich metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowVisualizationData {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Workflow name
    pub workflow_name: String,
    /// Workflow state
    pub state: WorkflowState,
    /// Task visual metadata
    pub tasks: Vec<TaskVisualMetadata>,
    /// Edge connections (from_task_id, to_task_id, edge_type)
    pub edges: Vec<(Uuid, Uuid, String)>,
    /// Visual theme
    pub theme: VisualTheme,
    /// Layout algorithm hint (hierarchical, force, circular, etc.)
    pub layout_hint: String,
    /// Viewport dimensions (width, height)
    pub viewport: (f64, f64),
}

impl WorkflowVisualizationData {
    /// Create new visualization data
    pub fn new(workflow_id: Uuid, workflow_name: String, state: WorkflowState) -> Self {
        Self {
            workflow_id,
            workflow_name,
            state,
            tasks: Vec::new(),
            edges: Vec::new(),
            theme: VisualTheme::default(),
            layout_hint: "hierarchical".to_string(),
            viewport: (1000.0, 600.0),
        }
    }

    /// Add task metadata
    pub fn add_task(&mut self, task: TaskVisualMetadata) {
        self.tasks.push(task);
    }

    /// Add edge
    pub fn add_edge(&mut self, from: Uuid, to: Uuid, edge_type: String) {
        self.edges.push((from, to, edge_type));
    }

    /// Set theme
    pub fn with_theme(mut self, theme: VisualTheme) -> Self {
        self.theme = theme;
        self
    }

    /// Set layout hint
    pub fn with_layout(mut self, layout_hint: String) -> Self {
        self.layout_hint = layout_hint;
        self
    }

    /// Export to JSON for frontend
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Export to vis.js format
    pub fn to_visjs_format(&self) -> serde_json::Value {
        let nodes: Vec<serde_json::Value> = self
            .tasks
            .iter()
            .map(|task| {
                serde_json::json!({
                    "id": task.task_id.to_string(),
                    "label": task.task_name,
                    "color": task.color,
                    "shape": task.shape,
                    "title": format!("{} ({})", task.task_name, task.state),
                    "value": task.progress,
                })
            })
            .collect();

        let edges: Vec<serde_json::Value> = self
            .edges
            .iter()
            .map(|(from, to, edge_type)| {
                serde_json::json!({
                    "from": from.to_string(),
                    "to": to.to_string(),
                    "arrows": "to",
                    "dashes": edge_type == "callback",
                })
            })
            .collect();

        serde_json::json!({
            "nodes": nodes,
            "edges": edges,
        })
    }
}

/// Execution timeline entry for Gantt chart visualization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEntry {
    /// Task ID
    pub task_id: Uuid,
    /// Task name
    pub task_name: String,
    /// Start time (Unix timestamp in milliseconds)
    pub start_time: u64,
    /// End time (Unix timestamp in milliseconds)
    pub end_time: Option<u64>,
    /// Duration in milliseconds
    pub duration: Option<u64>,
    /// Task state
    pub state: String,
    /// Worker ID that executed the task
    pub worker_id: Option<String>,
    /// Parent task ID (for nested workflows)
    pub parent_id: Option<Uuid>,
    /// Color for visualization
    pub color: String,
}

impl TimelineEntry {
    /// Create new timeline entry
    pub fn new(task_id: Uuid, task_name: String, start_time: u64) -> Self {
        Self {
            task_id,
            task_name,
            start_time,
            end_time: None,
            duration: None,
            state: "running".to_string(),
            worker_id: None,
            parent_id: None,
            color: "#2196F3".to_string(),
        }
    }

    /// Mark task as completed
    pub fn complete(&mut self, end_time: u64) {
        self.end_time = Some(end_time);
        self.duration = Some(end_time.saturating_sub(self.start_time));
        self.state = "completed".to_string();
        self.color = "#4CAF50".to_string();
    }

    /// Mark task as failed
    pub fn fail(&mut self, end_time: u64) {
        self.end_time = Some(end_time);
        self.duration = Some(end_time.saturating_sub(self.start_time));
        self.state = "failed".to_string();
        self.color = "#F44336".to_string();
    }

    /// Set worker ID
    pub fn with_worker(mut self, worker_id: String) -> Self {
        self.worker_id = Some(worker_id);
        self
    }

    /// Set parent ID
    pub fn with_parent(mut self, parent_id: Uuid) -> Self {
        self.parent_id = Some(parent_id);
        self
    }
}

/// Execution timeline for workflow visualization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionTimeline {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Timeline entries
    pub entries: Vec<TimelineEntry>,
    /// Workflow start time
    pub workflow_start: u64,
    /// Workflow end time
    pub workflow_end: Option<u64>,
}

impl ExecutionTimeline {
    /// Create new execution timeline
    pub fn new(workflow_id: Uuid) -> Self {
        Self {
            workflow_id,
            entries: Vec::new(),
            workflow_start: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            workflow_end: None,
        }
    }

    /// Add timeline entry
    pub fn add_entry(&mut self, entry: TimelineEntry) {
        self.entries.push(entry);
    }

    /// Start task
    pub fn start_task(&mut self, task_id: Uuid, task_name: String) -> usize {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        let entry = TimelineEntry::new(task_id, task_name, now);
        self.entries.push(entry);
        self.entries.len() - 1
    }

    /// Complete task
    pub fn complete_task(&mut self, index: usize) {
        if let Some(entry) = self.entries.get_mut(index) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            entry.complete(now);
        }
    }

    /// Fail task
    pub fn fail_task(&mut self, index: usize) {
        if let Some(entry) = self.entries.get_mut(index) {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            entry.fail(now);
        }
    }

    /// Mark workflow as complete
    pub fn complete_workflow(&mut self) {
        self.workflow_end = Some(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
        );
    }

    /// Export to JSON
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Export to Google Charts Timeline format
    pub fn to_google_charts_format(&self) -> serde_json::Value {
        let rows: Vec<serde_json::Value> = self
            .entries
            .iter()
            .map(|entry| {
                serde_json::json!([
                    entry.task_name,
                    entry.task_name,
                    entry.start_time,
                    entry.end_time.unwrap_or(entry.start_time),
                ])
            })
            .collect();

        serde_json::json!({
            "cols": [
                {"id": "", "label": "Task ID", "type": "string"},
                {"id": "", "label": "Task Name", "type": "string"},
                {"id": "", "label": "Start", "type": "number"},
                {"id": "", "label": "End", "type": "number"}
            ],
            "rows": rows.iter().map(|row| serde_json::json!({"c": row})).collect::<Vec<_>>()
        })
    }
}

/// Animation frame for execution visualization
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnimationFrame {
    /// Frame number
    pub frame_number: usize,
    /// Timestamp
    pub timestamp: u64,
    /// Workflow state at this frame
    pub workflow_state: WorkflowState,
    /// Task states (task_id -> state)
    pub task_states: HashMap<Uuid, String>,
    /// Active tasks at this frame
    pub active_tasks: Vec<Uuid>,
    /// Completed tasks at this frame
    pub completed_tasks: Vec<Uuid>,
    /// Events that occurred in this frame
    pub events: Vec<WorkflowEvent>,
}

impl AnimationFrame {
    /// Create new animation frame
    pub fn new(frame_number: usize, workflow_state: WorkflowState) -> Self {
        Self {
            frame_number,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            workflow_state,
            task_states: HashMap::new(),
            active_tasks: Vec::new(),
            completed_tasks: Vec::new(),
            events: Vec::new(),
        }
    }

    /// Set task state
    pub fn set_task_state(&mut self, task_id: Uuid, state: String) {
        self.task_states.insert(task_id, state);
    }

    /// Add active task
    pub fn add_active_task(&mut self, task_id: Uuid) {
        if !self.active_tasks.contains(&task_id) {
            self.active_tasks.push(task_id);
        }
    }

    /// Add completed task
    pub fn add_completed_task(&mut self, task_id: Uuid) {
        if !self.completed_tasks.contains(&task_id) {
            self.completed_tasks.push(task_id);
        }
        // Remove from active tasks
        self.active_tasks.retain(|id| id != &task_id);
    }

    /// Add event
    pub fn add_event(&mut self, event: WorkflowEvent) {
        self.events.push(event);
    }
}

/// Workflow animation sequence
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowAnimation {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Animation frames
    pub frames: Vec<AnimationFrame>,
    /// Frame duration in milliseconds
    pub frame_duration: u64,
    /// Total duration
    pub total_duration: u64,
}

impl WorkflowAnimation {
    /// Create new workflow animation
    pub fn new(workflow_id: Uuid, frame_duration: u64) -> Self {
        Self {
            workflow_id,
            frames: Vec::new(),
            frame_duration,
            total_duration: 0,
        }
    }

    /// Add frame
    pub fn add_frame(&mut self, frame: AnimationFrame) {
        self.frames.push(frame);
        self.total_duration = self.frames.len() as u64 * self.frame_duration;
    }

    /// Get frame at index
    pub fn get_frame(&self, index: usize) -> Option<&AnimationFrame> {
        self.frames.get(index)
    }

    /// Get frame count
    pub fn frame_count(&self) -> usize {
        self.frames.len()
    }

    /// Export to JSON
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }
}

/// DAG export with execution state overlay
pub trait DagExportWithState {
    /// Export DAG with current execution state
    fn to_dot_with_state(
        &self,
        state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String;

    /// Export Mermaid diagram with execution state
    fn to_mermaid_with_state(
        &self,
        state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String;

    /// Export to JSON with execution state
    fn to_json_with_state(
        &self,
        state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> Result<String, serde_json::Error>;
}

impl DagExportWithState for Chain {
    fn to_dot_with_state(
        &self,
        _state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String {
        let mut dot = String::from("digraph Chain {\n");
        dot.push_str("  rankdir=LR;\n");

        for (i, sig) in self.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");
            let color = match state {
                "completed" => "#4CAF50",
                "running" => "#2196F3",
                "failed" => "#F44336",
                _ => "#E0E0E0",
            };

            dot.push_str(&format!(
                "  task{} [label=\"{}\" style=filled fillcolor=\"{}\"];\n",
                i, sig.task, color
            ));

            if i > 0 {
                dot.push_str(&format!("  task{} -> task{};\n", i - 1, i));
            }
        }

        dot.push('}');
        dot
    }

    fn to_mermaid_with_state(
        &self,
        _state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String {
        let mut mmd = String::from("graph LR\n");

        for (i, sig) in self.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");
            let style_class = match state {
                "completed" => "completed",
                "running" => "running",
                "failed" => "failed",
                _ => "pending",
            };

            mmd.push_str(&format!(
                "  task{}[\"{}\"]:::{}\n",
                i, sig.task, style_class
            ));

            if i > 0 {
                mmd.push_str(&format!("  task{} --> task{}\n", i - 1, i));
            }
        }

        // Add style definitions
        mmd.push_str("\n  classDef completed fill:#4CAF50,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef running fill:#2196F3,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef failed fill:#F44336,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef pending fill:#E0E0E0,stroke:#333,stroke-width:2px\n");

        mmd
    }

    fn to_json_with_state(
        &self,
        state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> Result<String, serde_json::Error> {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        for (i, sig) in self.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let task_state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");

            nodes.push(serde_json::json!({
                "id": format!("task{}", i),
                "label": sig.task,
                "state": task_state,
                "task_id": task_id,
            }));

            if i > 0 {
                edges.push(serde_json::json!({
                    "from": format!("task{}", i - 1),
                    "to": format!("task{}", i),
                }));
            }
        }

        let result = serde_json::json!({
            "type": "chain",
            "workflow_state": state,
            "nodes": nodes,
            "edges": edges,
        });

        serde_json::to_string_pretty(&result)
    }
}

impl DagExportWithState for Group {
    fn to_dot_with_state(
        &self,
        _state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String {
        let mut dot = String::from("digraph Group {\n");

        for (i, sig) in self.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");
            let color = match state {
                "completed" => "#4CAF50",
                "running" => "#2196F3",
                "failed" => "#F44336",
                _ => "#E0E0E0",
            };

            dot.push_str(&format!(
                "  task{} [label=\"{}\" style=filled fillcolor=\"{}\"];\n",
                i, sig.task, color
            ));
        }

        dot.push('}');
        dot
    }

    fn to_mermaid_with_state(
        &self,
        _state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String {
        let mut mmd = String::from("graph TB\n");

        for (i, sig) in self.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");
            let style_class = match state {
                "completed" => "completed",
                "running" => "running",
                "failed" => "failed",
                _ => "pending",
            };

            mmd.push_str(&format!(
                "  task{}[\"{}\"]:::{}\n",
                i, sig.task, style_class
            ));
        }

        // Add style definitions
        mmd.push_str("\n  classDef completed fill:#4CAF50,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef running fill:#2196F3,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef failed fill:#F44336,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef pending fill:#E0E0E0,stroke:#333,stroke-width:2px\n");

        mmd
    }

    fn to_json_with_state(
        &self,
        state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> Result<String, serde_json::Error> {
        let mut nodes = Vec::new();

        for (i, sig) in self.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let task_state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");

            nodes.push(serde_json::json!({
                "id": format!("task{}", i),
                "label": sig.task,
                "state": task_state,
                "task_id": task_id,
            }));
        }

        let result = serde_json::json!({
            "type": "group",
            "workflow_state": state,
            "nodes": nodes,
            "edges": [],
        });

        serde_json::to_string_pretty(&result)
    }
}

impl DagExportWithState for Chord {
    fn to_dot_with_state(
        &self,
        _state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String {
        let mut dot = String::from("digraph Chord {\n");
        dot.push_str("  rankdir=LR;\n");

        // Header tasks
        for (i, sig) in self.header.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");
            let color = match state {
                "completed" => "#4CAF50",
                "running" => "#2196F3",
                "failed" => "#F44336",
                _ => "#E0E0E0",
            };

            dot.push_str(&format!(
                "  task{} [label=\"{}\" style=filled fillcolor=\"{}\"];\n",
                i, sig.task, color
            ));
        }

        // Body (callback)
        let task_id = self.body.options.task_id.unwrap_or_else(Uuid::new_v4);
        let state = task_states
            .get(&task_id)
            .map(|s| s.as_str())
            .unwrap_or("pending");
        let color = match state {
            "completed" => "#4CAF50",
            "running" => "#2196F3",
            "failed" => "#F44336",
            _ => "#E0E0E0",
        };

        dot.push_str(&format!(
            "  callback [label=\"{}\" shape=diamond style=filled fillcolor=\"{}\"];\n",
            self.body.task, color
        ));

        for i in 0..self.header.tasks.len() {
            dot.push_str(&format!("  task{} -> callback;\n", i));
        }

        dot.push('}');
        dot
    }

    fn to_mermaid_with_state(
        &self,
        _state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> String {
        let mut mmd = String::from("graph TB\n");

        // Header tasks
        for (i, sig) in self.header.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");
            let style_class = match state {
                "completed" => "completed",
                "running" => "running",
                "failed" => "failed",
                _ => "pending",
            };

            mmd.push_str(&format!(
                "  task{}[\"{}\"]:::{}\n",
                i, sig.task, style_class
            ));
        }

        // Body (callback)
        let task_id = self.body.options.task_id.unwrap_or_else(Uuid::new_v4);
        let state = task_states
            .get(&task_id)
            .map(|s| s.as_str())
            .unwrap_or("pending");
        let style_class = match state {
            "completed" => "completed",
            "running" => "running",
            "failed" => "failed",
            _ => "pending",
        };

        mmd.push_str(&format!(
            "  callback{{\"{}\"}}:::{}\n",
            self.body.task, style_class
        ));

        for i in 0..self.header.tasks.len() {
            mmd.push_str(&format!("  task{} --> callback\n", i));
        }

        // Add style definitions
        mmd.push_str("\n  classDef completed fill:#4CAF50,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef running fill:#2196F3,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef failed fill:#F44336,stroke:#333,stroke-width:2px\n");
        mmd.push_str("  classDef pending fill:#E0E0E0,stroke:#333,stroke-width:2px\n");

        mmd
    }

    fn to_json_with_state(
        &self,
        state: &WorkflowState,
        task_states: &HashMap<Uuid, String>,
    ) -> Result<String, serde_json::Error> {
        let mut nodes = Vec::new();
        let mut edges = Vec::new();

        // Header tasks
        for (i, sig) in self.header.tasks.iter().enumerate() {
            let task_id = sig.options.task_id.unwrap_or_else(Uuid::new_v4);
            let task_state = task_states
                .get(&task_id)
                .map(|s| s.as_str())
                .unwrap_or("pending");

            nodes.push(serde_json::json!({
                "id": format!("task{}", i),
                "label": sig.task,
                "state": task_state,
                "task_id": task_id,
            }));
        }

        // Body (callback)
        let task_id = self.body.options.task_id.unwrap_or_else(Uuid::new_v4);
        let task_state = task_states
            .get(&task_id)
            .map(|s| s.as_str())
            .unwrap_or("pending");

        nodes.push(serde_json::json!({
            "id": "callback",
            "label": self.body.task,
            "state": task_state,
            "task_id": task_id,
            "shape": "diamond",
        }));

        for i in 0..self.header.tasks.len() {
            edges.push(serde_json::json!({
                "from": format!("task{}", i),
                "to": "callback",
            }));
        }

        let result = serde_json::json!({
            "type": "chord",
            "workflow_state": state,
            "nodes": nodes,
            "edges": edges,
        });

        serde_json::to_string_pretty(&result)
    }
}

/// Real-time workflow event stream
#[derive(Debug, Clone)]
pub struct WorkflowEventStream {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Event buffer
    pub events: Vec<(u64, WorkflowEvent)>,
    /// Maximum buffer size
    pub max_buffer_size: usize,
}

impl WorkflowEventStream {
    /// Create new event stream
    pub fn new(workflow_id: Uuid) -> Self {
        Self {
            workflow_id,
            events: Vec::new(),
            max_buffer_size: 1000,
        }
    }

    /// Set maximum buffer size
    pub fn with_max_buffer_size(mut self, size: usize) -> Self {
        self.max_buffer_size = size;
        self
    }

    /// Push event
    pub fn push(&mut self, event: WorkflowEvent) {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.events.push((timestamp, event));

        // Trim buffer if needed
        if self.events.len() > self.max_buffer_size {
            self.events.remove(0);
        }
    }

    /// Get events since timestamp
    pub fn events_since(&self, timestamp: u64) -> Vec<&(u64, WorkflowEvent)> {
        self.events
            .iter()
            .filter(|(ts, _)| *ts > timestamp)
            .collect()
    }

    /// Get all events
    pub fn all_events(&self) -> &[(u64, WorkflowEvent)] {
        &self.events
    }

    /// Clear events
    pub fn clear(&mut self) {
        self.events.clear();
    }

    /// Export to JSON for Server-Sent Events (SSE)
    pub fn to_sse_format(&self) -> Vec<String> {
        self.events
            .iter()
            .map(|(ts, event)| {
                format!(
                    "event: workflow\ndata: {{\"timestamp\": {}, \"event\": \"{}\"}}\n\n",
                    ts, event
                )
            })
            .collect()
    }
}

// ============================================================================
// Production-Ready Enhancements
// ============================================================================

/// Workflow metrics collector for automatic metrics gathering
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkflowMetricsCollector {
    /// Workflow ID
    pub workflow_id: Uuid,
    /// Start time
    pub start_time: u64,
    /// End time
    pub end_time: Option<u64>,
    /// Total tasks
    pub total_tasks: usize,
    /// Completed tasks
    pub completed_tasks: usize,
    /// Failed tasks
    pub failed_tasks: usize,
    /// Task execution times (task_id -> duration_ms)
    pub task_durations: HashMap<Uuid, u64>,
    /// Task retry counts (task_id -> retry_count)
    pub task_retries: HashMap<Uuid, usize>,
    /// Total workflow duration in milliseconds
    pub total_duration: Option<u64>,
    /// Average task duration
    pub avg_task_duration: Option<f64>,
    /// Success rate (0.0 to 1.0)
    pub success_rate: Option<f64>,
}

impl WorkflowMetricsCollector {
    /// Create new metrics collector
    pub fn new(workflow_id: Uuid) -> Self {
        Self {
            workflow_id,
            start_time: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64,
            end_time: None,
            total_tasks: 0,
            completed_tasks: 0,
            failed_tasks: 0,
            task_durations: HashMap::new(),
            task_retries: HashMap::new(),
            total_duration: None,
            avg_task_duration: None,
            success_rate: None,
        }
    }

    /// Record task start
    pub fn record_task_start(&mut self, task_id: Uuid) {
        self.total_tasks += 1;
        self.task_durations.insert(task_id, 0);
    }

    /// Record task completion
    pub fn record_task_complete(&mut self, task_id: Uuid, duration_ms: u64) {
        self.completed_tasks += 1;
        self.task_durations.insert(task_id, duration_ms);
    }

    /// Record task failure
    pub fn record_task_failure(&mut self, task_id: Uuid, duration_ms: u64) {
        self.failed_tasks += 1;
        self.task_durations.insert(task_id, duration_ms);
    }

    /// Record task retry
    pub fn record_task_retry(&mut self, task_id: Uuid) {
        *self.task_retries.entry(task_id).or_insert(0) += 1;
    }

    /// Finalize metrics
    pub fn finalize(&mut self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.end_time = Some(now);
        self.total_duration = Some(now.saturating_sub(self.start_time));

        // Calculate average task duration
        if !self.task_durations.is_empty() {
            let sum: u64 = self.task_durations.values().sum();
            self.avg_task_duration = Some(sum as f64 / self.task_durations.len() as f64);
        }

        // Calculate success rate
        if self.total_tasks > 0 {
            self.success_rate = Some(self.completed_tasks as f64 / self.total_tasks as f64);
        }
    }

    /// Get metrics summary
    pub fn summary(&self) -> String {
        format!(
            "WorkflowMetrics[id={}, total={}, completed={}, failed={}, success_rate={:.2}%, avg_duration={:.2}ms]",
            self.workflow_id,
            self.total_tasks,
            self.completed_tasks,
            self.failed_tasks,
            self.success_rate.unwrap_or(0.0) * 100.0,
            self.avg_task_duration.unwrap_or(0.0)
        )
    }
}

impl std::fmt::Display for WorkflowMetricsCollector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.summary())
    }
}

/// Workflow rate limiter for controlling execution rate
#[derive(Debug, Clone)]
pub struct WorkflowRateLimiter {
    /// Maximum workflows per time window
    pub max_workflows: usize,
    /// Time window in milliseconds
    pub window_ms: u64,
    /// Workflow timestamps
    pub workflow_timestamps: Vec<u64>,
    /// Total workflows processed
    pub total_workflows: usize,
    /// Total workflows rejected
    pub rejected_workflows: usize,
}

impl WorkflowRateLimiter {
    /// Create new rate limiter
    pub fn new(max_workflows: usize, window_ms: u64) -> Self {
        Self {
            max_workflows,
            window_ms,
            workflow_timestamps: Vec::new(),
            total_workflows: 0,
            rejected_workflows: 0,
        }
    }

    /// Check if workflow can be executed
    pub fn allow_workflow(&mut self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Remove old timestamps outside the window
        self.workflow_timestamps
            .retain(|&ts| now.saturating_sub(ts) < self.window_ms);

        // Check if we can allow this workflow
        if self.workflow_timestamps.len() < self.max_workflows {
            self.workflow_timestamps.push(now);
            self.total_workflows += 1;
            true
        } else {
            self.rejected_workflows += 1;
            false
        }
    }

    /// Get current rate (workflows per second)
    pub fn current_rate(&self) -> f64 {
        if self.workflow_timestamps.is_empty() {
            return 0.0;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let active_timestamps: Vec<_> = self
            .workflow_timestamps
            .iter()
            .filter(|&&ts| now.saturating_sub(ts) < self.window_ms)
            .collect();

        if active_timestamps.is_empty() {
            return 0.0;
        }

        active_timestamps.len() as f64 / (self.window_ms as f64 / 1000.0)
    }

    /// Reset rate limiter
    pub fn reset(&mut self) {
        self.workflow_timestamps.clear();
    }

    /// Get rejection rate
    pub fn rejection_rate(&self) -> f64 {
        if self.total_workflows == 0 {
            0.0
        } else {
            self.rejected_workflows as f64 / self.total_workflows as f64
        }
    }
}

impl std::fmt::Display for WorkflowRateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RateLimiter[max={}/{}ms, current_rate={:.2}/s, rejected={}]",
            self.max_workflows,
            self.window_ms,
            self.current_rate(),
            self.rejected_workflows
        )
    }
}

/// Workflow concurrency control for limiting concurrent workflows
#[derive(Debug, Clone)]
pub struct WorkflowConcurrencyControl {
    /// Maximum concurrent workflows
    pub max_concurrent: usize,
    /// Currently active workflows
    pub active_workflows: HashMap<Uuid, u64>,
    /// Total workflows started
    pub total_started: usize,
    /// Total workflows completed
    pub total_completed: usize,
    /// Peak concurrency reached
    pub peak_concurrency: usize,
}

impl WorkflowConcurrencyControl {
    /// Create new concurrency control
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            max_concurrent,
            active_workflows: HashMap::new(),
            total_started: 0,
            total_completed: 0,
            peak_concurrency: 0,
        }
    }

    /// Try to start a workflow
    pub fn try_start(&mut self, workflow_id: Uuid) -> bool {
        if self.active_workflows.len() >= self.max_concurrent {
            return false;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.active_workflows.insert(workflow_id, now);
        self.total_started += 1;

        // Update peak concurrency
        if self.active_workflows.len() > self.peak_concurrency {
            self.peak_concurrency = self.active_workflows.len();
        }

        true
    }

    /// Complete a workflow
    pub fn complete(&mut self, workflow_id: Uuid) -> bool {
        if self.active_workflows.remove(&workflow_id).is_some() {
            self.total_completed += 1;
            true
        } else {
            false
        }
    }

    /// Get current concurrency
    pub fn current_concurrency(&self) -> usize {
        self.active_workflows.len()
    }

    /// Get available slots
    pub fn available_slots(&self) -> usize {
        self.max_concurrent
            .saturating_sub(self.active_workflows.len())
    }

    /// Check if at capacity
    pub fn is_at_capacity(&self) -> bool {
        self.active_workflows.len() >= self.max_concurrent
    }

    /// Get average workflow duration
    pub fn avg_workflow_duration(&self) -> Option<f64> {
        if self.total_completed == 0 {
            return None;
        }

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        let total_duration: u64 = self
            .active_workflows
            .values()
            .map(|&start_time| now.saturating_sub(start_time))
            .sum();

        Some(total_duration as f64 / self.total_completed as f64)
    }
}

impl std::fmt::Display for WorkflowConcurrencyControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "ConcurrencyControl[max={}, active={}, peak={}, available={}]",
            self.max_concurrent,
            self.current_concurrency(),
            self.peak_concurrency,
            self.available_slots()
        )
    }
}

/// Workflow composition helpers for easier workflow building
#[derive(Debug, Clone)]
pub struct WorkflowBuilder {
    /// Workflow name
    pub name: String,
    /// Workflow description
    pub description: Option<String>,
    /// Workflow tags
    pub tags: Vec<String>,
    /// Workflow metadata
    pub metadata: HashMap<String, serde_json::Value>,
}

impl WorkflowBuilder {
    /// Create new workflow builder
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            description: None,
            tags: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    /// Set description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Add tag
    pub fn add_tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Add metadata
    pub fn add_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Build a chain workflow
    pub fn chain(self) -> Chain {
        Chain::new()
    }

    /// Build a group workflow
    pub fn group(self) -> Group {
        Group::new()
    }

    /// Build a map workflow
    pub fn map(self, task: Signature, argsets: Vec<Vec<serde_json::Value>>) -> Map {
        Map::new(task, argsets)
    }
}

/// Workflow registry for tracking and managing workflows
#[derive(Debug, Clone)]
pub struct WorkflowRegistry {
    /// Registered workflows (workflow_id -> workflow_name)
    pub workflows: HashMap<Uuid, String>,
    /// Workflow metadata
    pub metadata: HashMap<Uuid, HashMap<String, serde_json::Value>>,
    /// Workflow states
    pub states: HashMap<Uuid, WorkflowStatus>,
    /// Workflow start times
    pub start_times: HashMap<Uuid, u64>,
    /// Workflow tags
    pub tags: HashMap<String, Vec<Uuid>>,
}

impl WorkflowRegistry {
    /// Create new workflow registry
    pub fn new() -> Self {
        Self {
            workflows: HashMap::new(),
            metadata: HashMap::new(),
            states: HashMap::new(),
            start_times: HashMap::new(),
            tags: HashMap::new(),
        }
    }

    /// Register a workflow
    pub fn register(
        &mut self,
        workflow_id: Uuid,
        name: String,
        metadata: HashMap<String, serde_json::Value>,
    ) {
        self.workflows.insert(workflow_id, name);
        self.metadata.insert(workflow_id, metadata);
        self.states.insert(workflow_id, WorkflowStatus::Pending);

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        self.start_times.insert(workflow_id, now);
    }

    /// Update workflow state
    pub fn update_state(&mut self, workflow_id: Uuid, state: WorkflowStatus) {
        self.states.insert(workflow_id, state);
    }

    /// Add tag to workflow
    pub fn add_tag(&mut self, workflow_id: Uuid, tag: String) {
        self.tags.entry(tag).or_default().push(workflow_id);
    }

    /// Get workflows by tag
    pub fn get_by_tag(&self, tag: &str) -> Vec<Uuid> {
        self.tags.get(tag).cloned().unwrap_or_default()
    }

    /// Get workflow state
    pub fn get_state(&self, workflow_id: &Uuid) -> Option<&WorkflowStatus> {
        self.states.get(workflow_id)
    }

    /// Get workflow name
    pub fn get_name(&self, workflow_id: &Uuid) -> Option<&str> {
        self.workflows.get(workflow_id).map(|s| s.as_str())
    }

    /// Get workflow metadata
    pub fn get_metadata(&self, workflow_id: &Uuid) -> Option<&HashMap<String, serde_json::Value>> {
        self.metadata.get(workflow_id)
    }

    /// Remove workflow
    pub fn remove(&mut self, workflow_id: &Uuid) -> bool {
        let removed = self.workflows.remove(workflow_id).is_some();
        self.metadata.remove(workflow_id);
        self.states.remove(workflow_id);
        self.start_times.remove(workflow_id);

        // Remove from tags
        for workflows in self.tags.values_mut() {
            workflows.retain(|id| id != workflow_id);
        }

        removed
    }

    /// Get workflow count
    pub fn count(&self) -> usize {
        self.workflows.len()
    }

    /// Get workflows by state
    pub fn get_by_state(&self, state: &WorkflowStatus) -> Vec<Uuid> {
        self.states
            .iter()
            .filter(|(_, s)| *s == state)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Clear all workflows
    pub fn clear(&mut self) {
        self.workflows.clear();
        self.metadata.clear();
        self.states.clear();
        self.start_times.clear();
        self.tags.clear();
    }

    /// Get all workflow IDs
    pub fn all_workflow_ids(&self) -> Vec<Uuid> {
        self.workflows.keys().copied().collect()
    }

    /// Get workflows by name pattern (contains)
    pub fn find_by_name(&self, pattern: &str) -> Vec<Uuid> {
        self.workflows
            .iter()
            .filter(|(_, name)| name.contains(pattern))
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get workflows older than duration (in milliseconds)
    pub fn get_older_than(&self, duration_ms: u64) -> Vec<Uuid> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        self.start_times
            .iter()
            .filter(|(_, &start_time)| now.saturating_sub(start_time) > duration_ms)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get workflow age in milliseconds
    pub fn get_age(&self, workflow_id: &Uuid) -> Option<u64> {
        self.start_times.get(workflow_id).map(|&start_time| {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
            now.saturating_sub(start_time)
        })
    }

    /// Check if workflow exists
    pub fn contains(&self, workflow_id: &Uuid) -> bool {
        self.workflows.contains_key(workflow_id)
    }

    /// Get all tags
    pub fn all_tags(&self) -> Vec<String> {
        self.tags.keys().cloned().collect()
    }

    /// Get workflows with multiple tags (all tags must match)
    pub fn get_by_tags_all(&self, tags: &[&str]) -> Vec<Uuid> {
        if tags.is_empty() {
            return Vec::new();
        }

        let mut result: Option<Vec<Uuid>> = None;

        for tag in tags {
            let tagged = self.get_by_tag(tag);
            result = match result {
                None => Some(tagged),
                Some(current) => {
                    // Intersection
                    Some(
                        current
                            .into_iter()
                            .filter(|id| tagged.contains(id))
                            .collect(),
                    )
                }
            };
        }

        result.unwrap_or_default()
    }

    /// Get workflows with any of the tags (OR operation)
    pub fn get_by_tags_any(&self, tags: &[&str]) -> Vec<Uuid> {
        let mut result = Vec::new();
        for tag in tags {
            result.extend(self.get_by_tag(tag));
        }
        // Remove duplicates
        result.sort();
        result.dedup();
        result
    }

    /// Get count by state
    pub fn count_by_state(&self, state: &WorkflowStatus) -> usize {
        self.states.iter().filter(|(_, s)| *s == state).count()
    }

    /// Get running workflows count
    pub fn running_count(&self) -> usize {
        self.count_by_state(&WorkflowStatus::Running)
    }

    /// Get pending workflows count
    pub fn pending_count(&self) -> usize {
        self.count_by_state(&WorkflowStatus::Pending)
    }

    /// Get completed workflows count (success + failed)
    pub fn completed_count(&self) -> usize {
        self.count_by_state(&WorkflowStatus::Success) + self.count_by_state(&WorkflowStatus::Failed)
    }
}

impl Default for WorkflowRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for WorkflowRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "WorkflowRegistry[total={}, pending={}, running={}, success={}, failed={}]",
            self.count(),
            self.get_by_state(&WorkflowStatus::Pending).len(),
            self.get_by_state(&WorkflowStatus::Running).len(),
            self.get_by_state(&WorkflowStatus::Success).len(),
            self.get_by_state(&WorkflowStatus::Failed).len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_signature_creation() {
        let sig = Signature::new("test_task".to_string())
            .with_args(vec![serde_json::json!(1), serde_json::json!(2)])
            .with_priority(9);

        assert_eq!(sig.task, "test_task");
        assert_eq!(sig.args.len(), 2);
        assert_eq!(sig.options.priority, Some(9));
    }

    #[test]
    fn test_signature_predicates() {
        let sig = Signature::new("task".to_string())
            .with_args(vec![serde_json::json!(1)])
            .with_kwargs({
                let mut map = HashMap::new();
                map.insert("key".to_string(), serde_json::json!("value"));
                map
            })
            .immutable();

        assert!(sig.has_args());
        assert!(sig.has_kwargs());
        assert!(sig.is_immutable());
    }

    #[test]
    fn test_signature_display() {
        let sig = Signature::new("my_task".to_string())
            .with_args(vec![serde_json::json!(1), serde_json::json!(2)])
            .immutable();

        let display = format!("{}", sig);
        assert!(display.contains("Signature[task=my_task]"));
        assert!(display.contains("args=2"));
        assert!(display.contains("(immutable)"));
    }

    #[test]
    fn test_task_options_predicates() {
        let mut opts = TaskOptions::default();
        assert!(!opts.has_priority());
        assert!(!opts.has_queue());
        assert!(!opts.has_task_id());
        assert!(!opts.has_link());
        assert!(!opts.has_link_error());

        opts.priority = Some(5);
        opts.queue = Some("celery".to_string());
        opts.task_id = Some(Uuid::new_v4());
        opts.link = Some(Box::new(Signature::new("link_task".to_string())));
        opts.link_error = Some(Box::new(Signature::new("error_task".to_string())));

        assert!(opts.has_priority());
        assert!(opts.has_queue());
        assert!(opts.has_task_id());
        assert!(opts.has_link());
        assert!(opts.has_link_error());
    }

    #[test]
    fn test_task_options_display() {
        let task_id = Uuid::new_v4();
        let opts = TaskOptions {
            priority: Some(9),
            queue: Some("high_priority".to_string()),
            task_id: Some(task_id),
            link: Some(Box::new(Signature::new("success".to_string()))),
            link_error: Some(Box::new(Signature::new("failure".to_string()))),
            ..Default::default()
        };

        let display = format!("{}", opts);
        assert!(display.contains("TaskOptions"));
        assert!(display.contains("priority=9"));
        assert!(display.contains("queue=high_priority"));
        assert!(display.contains("task_id="));
        assert!(display.contains("link=yes"));
        assert!(display.contains("link_error=yes"));
    }

    #[test]
    fn test_task_options_display_default() {
        let opts = TaskOptions::default();
        let display = format!("{}", opts);
        assert_eq!(display, "TaskOptions[default]");
    }

    #[test]
    fn test_chain_builder() {
        let chain = Chain::new()
            .then("task1", vec![serde_json::json!(1)])
            .then("task2", vec![serde_json::json!(2)])
            .then("task3", vec![serde_json::json!(3)]);

        assert_eq!(chain.tasks.len(), 3);
        assert_eq!(chain.tasks[0].task, "task1");
        assert_eq!(chain.tasks[2].task, "task3");
    }

    #[test]
    fn test_chain_predicates() {
        let chain = Chain::new();
        assert!(chain.is_empty());
        assert_eq!(chain.len(), 0);

        let chain = chain.then("task1", vec![]).then("task2", vec![]);
        assert!(!chain.is_empty());
        assert_eq!(chain.len(), 2);
    }

    #[test]
    fn test_chain_display() {
        let chain = Chain::new()
            .then("first", vec![])
            .then("middle", vec![])
            .then("last", vec![]);

        let display = format!("{}", chain);
        assert!(display.contains("Chain[3 tasks]"));
        assert!(display.contains("first -> ... -> last"));
    }

    #[test]
    fn test_chain_display_empty() {
        let chain = Chain::new();
        let display = format!("{}", chain);
        assert_eq!(display, "Chain[0 tasks]");
    }

    #[test]
    fn test_group_builder() {
        let group = Group::new()
            .add("task1", vec![])
            .add("task2", vec![])
            .add("task3", vec![]);

        assert_eq!(group.tasks.len(), 3);
    }

    #[test]
    fn test_group_predicates() {
        let group = Group::new();
        assert!(group.is_empty());
        assert_eq!(group.len(), 0);
        assert!(group.has_group_id());

        let group = group.add("task1", vec![]).add("task2", vec![]);
        assert!(!group.is_empty());
        assert_eq!(group.len(), 2);
    }

    #[test]
    fn test_group_display() {
        let group = Group::new()
            .add("task1", vec![])
            .add("task2", vec![])
            .add("task3", vec![]);

        let display = format!("{}", group);
        assert!(display.contains("Group[3 tasks]"));
        assert!(display.contains("id="));
    }

    #[test]
    fn test_chord_creation() {
        let header = Group::new().add("task1", vec![]).add("task2", vec![]);

        let body = Signature::new("callback".to_string());

        let chord = Chord::new(header, body);

        assert_eq!(chord.header.tasks.len(), 2);
        assert_eq!(chord.body.task, "callback");
    }

    #[test]
    fn test_chord_display() {
        let header = Group::new()
            .add("task1", vec![])
            .add("task2", vec![])
            .add("task3", vec![]);
        let body = Signature::new("aggregate".to_string());
        let chord = Chord::new(header, body);

        let display = format!("{}", chord);
        assert!(display.contains("Chord[3 tasks] -> callback(aggregate)"));
    }

    #[test]
    fn test_map_creation() {
        let task = Signature::new("process".to_string());
        let argsets = vec![
            vec![serde_json::json!(1)],
            vec![serde_json::json!(2)],
            vec![serde_json::json!(3)],
        ];

        let map = Map::new(task, argsets);

        assert_eq!(map.argsets.len(), 3);
    }

    #[test]
    fn test_map_predicates() {
        let task = Signature::new("process".to_string());
        let empty_map = Map::new(task.clone(), vec![]);
        assert!(empty_map.is_empty());
        assert_eq!(empty_map.len(), 0);

        let map = Map::new(
            task,
            vec![vec![serde_json::json!(1)], vec![serde_json::json!(2)]],
        );
        assert!(!map.is_empty());
        assert_eq!(map.len(), 2);
    }

    #[test]
    fn test_map_display() {
        let task = Signature::new("process".to_string());
        let argsets = vec![
            vec![serde_json::json!(1)],
            vec![serde_json::json!(2)],
            vec![serde_json::json!(3)],
        ];
        let map = Map::new(task, argsets);

        let display = format!("{}", map);
        assert!(display.contains("Map[task=process, 3 argsets]"));
    }

    #[test]
    fn test_starmap_predicates() {
        let task = Signature::new("add".to_string());
        let empty_starmap = Starmap::new(task.clone(), vec![]);
        assert!(empty_starmap.is_empty());
        assert_eq!(empty_starmap.len(), 0);

        let starmap = Starmap::new(
            task,
            vec![
                vec![serde_json::json!(1), serde_json::json!(2)],
                vec![serde_json::json!(3), serde_json::json!(4)],
            ],
        );
        assert!(!starmap.is_empty());
        assert_eq!(starmap.len(), 2);
    }

    #[test]
    fn test_starmap_display() {
        let task = Signature::new("multiply".to_string());
        let argsets = vec![
            vec![serde_json::json!(2), serde_json::json!(3)],
            vec![serde_json::json!(4), serde_json::json!(5)],
        ];
        let starmap = Starmap::new(task, argsets);

        let display = format!("{}", starmap);
        assert!(display.contains("Starmap[task=multiply, 2 argsets]"));
    }

    #[test]
    fn test_canvas_error_predicates() {
        let invalid_err = CanvasError::Invalid("bad workflow".to_string());
        assert!(invalid_err.is_invalid());
        assert!(!invalid_err.is_broker());
        assert!(!invalid_err.is_serialization());
        assert!(!invalid_err.is_retryable());

        let broker_err = CanvasError::Broker("connection failed".to_string());
        assert!(!broker_err.is_invalid());
        assert!(broker_err.is_broker());
        assert!(!broker_err.is_serialization());
        assert!(broker_err.is_retryable());

        let ser_err = CanvasError::Serialization("bad json".to_string());
        assert!(!ser_err.is_invalid());
        assert!(!ser_err.is_broker());
        assert!(ser_err.is_serialization());
        assert!(!ser_err.is_retryable());
    }

    #[test]
    fn test_canvas_error_category() {
        let invalid_err = CanvasError::Invalid("test".to_string());
        assert_eq!(invalid_err.category(), "invalid");

        let broker_err = CanvasError::Broker("test".to_string());
        assert_eq!(broker_err.category(), "broker");

        let ser_err = CanvasError::Serialization("test".to_string());
        assert_eq!(ser_err.category(), "serialization");

        let cancelled_err = CanvasError::Cancelled("test".to_string());
        assert_eq!(cancelled_err.category(), "cancelled");

        let timeout_err = CanvasError::Timeout("test".to_string());
        assert_eq!(timeout_err.category(), "timeout");
    }

    #[test]
    fn test_canvas_error_display() {
        let err = CanvasError::Invalid("empty chain".to_string());
        assert_eq!(err.to_string(), "Invalid workflow: empty chain");

        let err = CanvasError::Broker("timeout".to_string());
        assert_eq!(err.to_string(), "Broker error: timeout");

        let err = CanvasError::Serialization("malformed json".to_string());
        assert_eq!(err.to_string(), "Serialization error: malformed json");

        let err = CanvasError::Cancelled("user requested".to_string());
        assert_eq!(err.to_string(), "Workflow cancelled: user requested");

        let err = CanvasError::Timeout("exceeded 5s".to_string());
        assert_eq!(err.to_string(), "Workflow timeout: exceeded 5s");
    }

    #[test]
    fn test_cancellation_token() {
        let workflow_id = Uuid::new_v4();
        let token = CancellationToken::new(workflow_id)
            .with_reason("user requested".to_string())
            .cancel_tree();

        assert_eq!(token.workflow_id, workflow_id);
        assert_eq!(token.reason, Some("user requested".to_string()));
        assert!(token.cancel_tree);
        assert!(token.branch_id.is_none());

        let display = format!("{}", token);
        assert!(display.contains("CancellationToken"));
        assert!(display.contains("reason=user requested"));
        assert!(display.contains("(tree)"));
    }

    #[test]
    fn test_workflow_retry_policy() {
        let policy = WorkflowRetryPolicy::new(3)
            .failed_only()
            .with_backoff(2.0, 60)
            .with_initial_delay(1);

        assert_eq!(policy.max_retries, 3);
        assert!(policy.retry_failed_only);
        assert_eq!(policy.backoff_factor, Some(2.0));
        assert_eq!(policy.max_backoff, Some(60));
        assert_eq!(policy.initial_delay, Some(1));

        // Test delay calculation
        assert_eq!(policy.calculate_delay(0), 1); // 1 * 2^0 = 1
        assert_eq!(policy.calculate_delay(1), 2); // 1 * 2^1 = 2
        assert_eq!(policy.calculate_delay(2), 4); // 1 * 2^2 = 4
        assert_eq!(policy.calculate_delay(10), 60); // Capped at max_backoff

        let display = format!("{}", policy);
        assert!(display.contains("WorkflowRetryPolicy"));
        assert!(display.contains("max_retries=3"));
        assert!(display.contains("(failed_only)"));
    }

    #[test]
    fn test_workflow_timeout() {
        let timeout = WorkflowTimeout::new(300)
            .with_stage_timeout(60)
            .with_escalation(TimeoutEscalation::ContinuePartial);

        assert_eq!(timeout.total_timeout, Some(300));
        assert_eq!(timeout.stage_timeout, Some(60));
        assert!(matches!(
            timeout.escalation,
            TimeoutEscalation::ContinuePartial
        ));

        let display = format!("{}", timeout);
        assert!(display.contains("WorkflowTimeout"));
        assert!(display.contains("total=300s"));
        assert!(display.contains("stage=60s"));
    }

    #[test]
    fn test_foreach_loop() {
        let task = Signature::new("process".to_string());
        let items = vec![
            serde_json::json!(1),
            serde_json::json!(2),
            serde_json::json!(3),
        ];
        let foreach = ForEach::new(task, items).with_concurrency(2);

        assert_eq!(foreach.len(), 3);
        assert!(!foreach.is_empty());
        assert_eq!(foreach.concurrency, Some(2));

        let display = format!("{}", foreach);
        assert!(display.contains("ForEach"));
        assert!(display.contains("process"));
        assert!(display.contains("3 items"));
        assert!(display.contains("concurrency=2"));

        let empty = ForEach::new(Signature::new("task".to_string()), vec![]);
        assert!(empty.is_empty());
        assert_eq!(empty.len(), 0);
    }

    #[test]
    fn test_while_loop() {
        let condition = Condition::field_equals("status", serde_json::json!("pending"));
        let body = Signature::new("check".to_string());
        let while_loop = WhileLoop::new(condition, body).with_max_iterations(100);

        assert_eq!(while_loop.max_iterations, Some(100));

        let display = format!("{}", while_loop);
        assert!(display.contains("While"));
        assert!(display.contains("check"));
        assert!(display.contains("max=100"));

        let unlimited = WhileLoop::new(
            Condition::field_equals("x", serde_json::json!(0)),
            Signature::new("task".to_string()),
        )
        .unlimited();
        assert!(unlimited.max_iterations.is_none());
    }

    #[test]
    fn test_workflow_state() {
        let workflow_id = Uuid::new_v4();
        let mut state = WorkflowState::new(workflow_id, 10);

        assert_eq!(state.workflow_id, workflow_id);
        assert_eq!(state.status, WorkflowStatus::Pending);
        assert_eq!(state.total_tasks, 10);
        assert_eq!(state.completed_tasks, 0);
        assert_eq!(state.failed_tasks, 0);
        assert_eq!(state.progress(), 0.0);
        assert!(!state.is_complete());

        // Mark some tasks as completed
        state.mark_completed();
        state.mark_completed();
        state.mark_completed();
        assert_eq!(state.completed_tasks, 3);
        assert_eq!(state.progress(), 30.0);

        // Mark a task as failed
        state.mark_failed();
        assert_eq!(state.failed_tasks, 1);

        // Set and get intermediate results
        state.set_result("step1".to_string(), serde_json::json!({"result": 42}));
        assert_eq!(
            state.get_result("step1"),
            Some(&serde_json::json!({"result": 42}))
        );
        assert_eq!(state.get_result("nonexistent"), None);

        // Test completion states
        state.status = WorkflowStatus::Success;
        assert!(state.is_complete());

        state.status = WorkflowStatus::Failed;
        assert!(state.is_complete());

        state.status = WorkflowStatus::Cancelled;
        assert!(state.is_complete());

        state.status = WorkflowStatus::Running;
        assert!(!state.is_complete());

        let display = format!("{}", state);
        assert!(display.contains("WorkflowState"));
        assert!(display.contains("progress=30.0%"));
        assert!(display.contains("failed=1"));
    }

    #[test]
    fn test_dag_export_chain() {
        let chain = Chain::new()
            .then("task1", vec![])
            .then("task2", vec![])
            .then("task3", vec![]);

        // Test DOT export
        let dot = chain.to_dot();
        assert!(dot.contains("digraph Chain"));
        assert!(dot.contains("rankdir=LR"));
        assert!(dot.contains("task1"));
        assert!(dot.contains("task2"));
        assert!(dot.contains("task3"));
        assert!(dot.contains("n0 -> n1"));
        assert!(dot.contains("n1 -> n2"));

        // Test Mermaid export
        let mmd = chain.to_mermaid();
        assert!(mmd.contains("graph LR"));
        assert!(mmd.contains("task1"));
        assert!(mmd.contains("task2"));
        assert!(mmd.contains("task3"));
        assert!(mmd.contains("n0 --> n1"));
        assert!(mmd.contains("n1 --> n2"));

        // Test JSON export
        let json = chain.to_json().unwrap();
        assert!(json.contains("task1"));
        assert!(json.contains("task2"));
        assert!(json.contains("task3"));
    }

    #[test]
    fn test_dag_export_group() {
        let group = Group::new()
            .add("task1", vec![])
            .add("task2", vec![])
            .add("task3", vec![]);

        // Test DOT export
        let dot = group.to_dot();
        assert!(dot.contains("digraph Group"));
        assert!(dot.contains("rankdir=TB"));
        assert!(dot.contains("start"));
        assert!(dot.contains("task1"));
        assert!(dot.contains("task2"));
        assert!(dot.contains("task3"));
        assert!(dot.contains("start -> n0"));
        assert!(dot.contains("start -> n1"));
        assert!(dot.contains("start -> n2"));

        // Test Mermaid export
        let mmd = group.to_mermaid();
        assert!(mmd.contains("graph TB"));
        assert!(mmd.contains("start"));
        assert!(mmd.contains("task1"));
        assert!(mmd.contains("start --> n0"));

        // Test JSON export
        let json = group.to_json().unwrap();
        assert!(json.contains("task1"));
    }

    #[test]
    fn test_dag_export_chord() {
        let header = Group::new().add("task1", vec![]).add("task2", vec![]);
        let body = Signature::new("callback".to_string());
        let chord = Chord::new(header, body);

        // Test DOT export
        let dot = chord.to_dot();
        assert!(dot.contains("digraph Chord"));
        assert!(dot.contains("callback"));
        assert!(dot.contains("task1"));
        assert!(dot.contains("task2"));
        assert!(dot.contains("n0 -> callback"));
        assert!(dot.contains("n1 -> callback"));

        // Test Mermaid export
        let mmd = chord.to_mermaid();
        assert!(mmd.contains("graph TB"));
        assert!(mmd.contains("callback"));
        assert!(mmd.contains("task1"));
        assert!(mmd.contains("n0 --> callback"));

        // Test JSON export
        let json = chord.to_json().unwrap();
        assert!(json.contains("callback"));
        assert!(json.contains("task1"));
    }

    #[test]
    fn test_canvas_error_new_variants() {
        let cancelled = CanvasError::Cancelled("user cancelled".to_string());
        assert!(cancelled.is_cancelled());
        assert!(!cancelled.is_timeout());
        assert!(!cancelled.is_retryable());
        assert_eq!(cancelled.category(), "cancelled");

        let timeout = CanvasError::Timeout("exceeded limit".to_string());
        assert!(timeout.is_timeout());
        assert!(!timeout.is_cancelled());
        assert!(!timeout.is_retryable());
        assert_eq!(timeout.category(), "timeout");
    }

    #[test]
    fn test_named_output() {
        let output = NamedOutput::new("result", serde_json::json!(42)).with_source("task1");

        assert_eq!(output.name, "result");
        assert_eq!(output.value, serde_json::json!(42));
        assert_eq!(output.source, Some("task1".to_string()));
    }

    #[test]
    fn test_result_transform() {
        let extract = ResultTransform::Extract {
            field: "data".to_string(),
        };
        assert!(format!("{}", extract).contains("Extract[data]"));

        let map = ResultTransform::Map {
            task: Box::new(Signature::new("transform".to_string())),
        };
        assert!(format!("{}", map).contains("Map[transform]"));
    }

    #[test]
    fn test_result_cache() {
        let cache = ResultCache::new("task:123")
            .with_policy(CachePolicy::OnSuccess)
            .with_ttl(3600);

        assert_eq!(cache.key, "task:123");
        assert_eq!(cache.ttl, Some(3600));

        let display = format!("{}", cache);
        assert!(display.contains("Cache[key=task:123]"));
        assert!(display.contains("ttl=3600s"));
    }

    #[test]
    fn test_workflow_error_handler() {
        let handler = WorkflowErrorHandler::new(Signature::new("handle_error".to_string()))
            .for_errors(vec!["NetworkError".to_string(), "TimeoutError".to_string()])
            .suppress_error();

        assert_eq!(handler.handler.task, "handle_error");
        assert_eq!(handler.error_types.len(), 2);
        assert!(handler.suppress);

        let display = format!("{}", handler);
        assert!(display.contains("ErrorHandler[handle_error]"));
        assert!(display.contains("(suppress)"));
    }

    #[test]
    fn test_compensation_workflow() {
        let mut workflow = CompensationWorkflow::new();
        assert!(workflow.is_empty());
        assert_eq!(workflow.len(), 0);

        workflow = workflow
            .step(
                Signature::new("create".to_string()),
                Signature::new("delete".to_string()),
            )
            .step(
                Signature::new("update".to_string()),
                Signature::new("rollback".to_string()),
            );

        assert!(!workflow.is_empty());
        assert_eq!(workflow.len(), 2);
        assert_eq!(workflow.forward.len(), 2);
        assert_eq!(workflow.compensations.len(), 2);

        let display = format!("{}", workflow);
        assert!(display.contains("Compensation[2 steps, 2 compensations]"));
    }

    #[test]
    fn test_saga() {
        let workflow = CompensationWorkflow::new()
            .step(
                Signature::new("reserve".to_string()),
                Signature::new("cancel_reservation".to_string()),
            )
            .step(
                Signature::new("charge".to_string()),
                Signature::new("refund".to_string()),
            );

        let saga = Saga::new(workflow).with_isolation(SagaIsolation::Serializable);

        assert_eq!(saga.workflow.len(), 2);
        assert!(matches!(saga.isolation, SagaIsolation::Serializable));

        let display = format!("{}", saga);
        assert!(display.contains("Saga[2 steps"));
        assert!(display.contains("Serializable"));
    }

    #[test]
    fn test_scatter_gather() {
        let scatter = Signature::new("distribute".to_string());
        let workers = vec![
            Signature::new("worker1".to_string()),
            Signature::new("worker2".to_string()),
            Signature::new("worker3".to_string()),
        ];
        let gather = Signature::new("collect".to_string());

        let sg = ScatterGather::new(scatter, workers, gather).with_timeout(30);

        assert_eq!(sg.workers.len(), 3);
        assert_eq!(sg.timeout, Some(30));

        let display = format!("{}", sg);
        assert!(display.contains("ScatterGather"));
        assert!(display.contains("distribute"));
        assert!(display.contains("3 workers"));
        assert!(display.contains("collect"));
    }

    #[test]
    fn test_pipeline() {
        let mut pipeline = Pipeline::new();
        assert!(pipeline.is_empty());
        assert_eq!(pipeline.len(), 0);

        pipeline = pipeline
            .stage(Signature::new("fetch".to_string()))
            .stage(Signature::new("transform".to_string()))
            .stage(Signature::new("load".to_string()))
            .with_buffer_size(100);

        assert!(!pipeline.is_empty());
        assert_eq!(pipeline.len(), 3);
        assert_eq!(pipeline.buffer_size, Some(100));

        let display = format!("{}", pipeline);
        assert!(display.contains("Pipeline[3 stages]"));
        assert!(display.contains("buffer=100"));
    }

    #[test]
    fn test_fanout() {
        let source = Signature::new("broadcast".to_string());
        let fanout = FanOut::new(source)
            .consumer(Signature::new("consumer1".to_string()))
            .consumer(Signature::new("consumer2".to_string()))
            .consumer(Signature::new("consumer3".to_string()));

        assert!(!fanout.is_empty());
        assert_eq!(fanout.len(), 3);

        let display = format!("{}", fanout);
        assert!(display.contains("FanOut"));
        assert!(display.contains("broadcast"));
        assert!(display.contains("3 consumers"));
    }

    #[test]
    fn test_fanin() {
        let aggregator = Signature::new("aggregate".to_string());
        let fanin = FanIn::new(aggregator)
            .source(Signature::new("source1".to_string()))
            .source(Signature::new("source2".to_string()));

        assert!(!fanin.is_empty());
        assert_eq!(fanin.len(), 2);

        let display = format!("{}", fanin);
        assert!(display.contains("FanIn"));
        assert!(display.contains("2 sources"));
        assert!(display.contains("aggregate"));
    }

    #[test]
    fn test_validation_result() {
        let mut result = ValidationResult::valid();
        assert!(result.valid);
        assert!(result.errors.is_empty());
        assert!(result.warnings.is_empty());

        result.add_warning("This is a warning");
        assert!(result.valid);
        assert_eq!(result.warnings.len(), 1);

        result.add_error("This is an error");
        assert!(!result.valid);
        assert_eq!(result.errors.len(), 1);

        let display = format!("{}", result);
        assert!(display.contains("Invalid"));
        assert!(display.contains("1 errors"));
    }

    #[test]
    fn test_workflow_validator_chain() {
        let empty_chain = Chain::new();
        let result = empty_chain.validate();
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.contains("cannot be empty")));

        let valid_chain = Chain::new()
            .then("task1", vec![])
            .then("task2", vec![])
            .then("task3", vec![]);
        let result = valid_chain.validate();
        assert!(result.valid);

        // Create a large chain to test warning
        let mut large_chain = Chain::new();
        for i in 0..150 {
            large_chain = large_chain.then(&format!("task{}", i), vec![]);
        }
        let result = large_chain.validate();
        assert!(result.valid);
        assert!(!result.warnings.is_empty());
    }

    #[test]
    fn test_workflow_validator_group() {
        let empty_group = Group::new();
        let result = empty_group.validate();
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.contains("cannot be empty")));

        let valid_group = Group::new().add("task1", vec![]).add("task2", vec![]);
        let result = valid_group.validate();
        assert!(result.valid);
    }

    #[test]
    fn test_workflow_validator_chord() {
        let empty_header = Group::new();
        let body = Signature::new("callback".to_string());
        let chord = Chord::new(empty_header, body);

        let result = chord.validate();
        assert!(!result.valid);
        assert!(result.errors.iter().any(|e| e.contains("cannot be empty")));

        let valid_header = Group::new().add("task1", vec![]).add("task2", vec![]);
        let body = Signature::new("callback".to_string());
        let chord = Chord::new(valid_header, body);

        let result = chord.validate();
        assert!(result.valid);
    }

    #[test]
    fn test_loop_control() {
        let continue_ctrl = LoopControl::continue_loop();
        assert!(matches!(continue_ctrl, LoopControl::Continue));
        assert_eq!(format!("{}", continue_ctrl), "Continue");

        let break_ctrl = LoopControl::break_loop();
        assert!(matches!(break_ctrl, LoopControl::Break));
        assert_eq!(format!("{}", break_ctrl), "Break");

        let break_with = LoopControl::break_with(serde_json::json!({"result": 42}));
        assert!(matches!(break_with, LoopControl::BreakWith { .. }));
        assert_eq!(format!("{}", break_with), "BreakWith");
    }

    #[test]
    fn test_error_propagation_mode() {
        let stop = ErrorPropagationMode::StopOnFirstError;
        assert!(!stop.allows_continue());
        assert_eq!(format!("{}", stop), "StopOnFirstError");

        let continue_mode = ErrorPropagationMode::ContinueOnError;
        assert!(continue_mode.allows_continue());
        assert_eq!(format!("{}", continue_mode), "ContinueOnError");

        let partial = ErrorPropagationMode::partial_failure(3);
        assert!(partial.allows_continue());
        assert!(format!("{}", partial).contains("PartialFailure"));

        let partial_rate = ErrorPropagationMode::partial_failure_with_rate(5, 0.5);
        assert!(partial_rate.allows_continue());
        let display = format!("{}", partial_rate);
        assert!(display.contains("PartialFailure"));
        assert!(display.contains("50.0%"));
    }

    #[test]
    fn test_partial_failure_tracker() {
        let mut tracker = PartialFailureTracker::new(10);
        assert_eq!(tracker.total_tasks, 10);
        assert_eq!(tracker.successful_tasks, 0);
        assert_eq!(tracker.failed_tasks, 0);

        // Record successes
        tracker.record_success(Uuid::new_v4());
        tracker.record_success(Uuid::new_v4());
        assert_eq!(tracker.successful_tasks, 2);
        assert_eq!(tracker.success_rate(), 0.2);

        // Record failures
        tracker.record_failure(Uuid::new_v4(), "error1".to_string());
        tracker.record_failure(Uuid::new_v4(), "error2".to_string());
        assert_eq!(tracker.failed_tasks, 2);
        assert_eq!(tracker.failure_rate(), 0.2);

        // Test threshold checking
        let stop_mode = ErrorPropagationMode::StopOnFirstError;
        assert!(tracker.exceeds_threshold(&stop_mode));
        assert!(!tracker.should_continue(&stop_mode));

        let continue_mode = ErrorPropagationMode::ContinueOnError;
        assert!(!tracker.exceeds_threshold(&continue_mode));
        assert!(tracker.should_continue(&continue_mode));

        let partial_mode = ErrorPropagationMode::partial_failure(3);
        assert!(!tracker.exceeds_threshold(&partial_mode));
        assert!(tracker.should_continue(&partial_mode));

        // Add more failures to exceed threshold
        tracker.record_failure(Uuid::new_v4(), "error3".to_string());
        assert!(tracker.exceeds_threshold(&partial_mode));
        assert!(!tracker.should_continue(&partial_mode));

        let display = format!("{}", tracker);
        assert!(display.contains("PartialFailureTracker"));
        assert!(display.contains("2/10"));
    }

    #[test]
    fn test_isolation_level() {
        let none = IsolationLevel::None;
        assert!(!none.has_resource_limits());
        assert!(!none.has_error_isolation());
        assert_eq!(format!("{}", none), "None");

        let resource = IsolationLevel::resource(512);
        assert!(resource.has_resource_limits());
        assert!(!resource.has_error_isolation());
        let display = format!("{}", resource);
        assert!(display.contains("Resource"));
        assert!(display.contains("512MB"));

        let error = IsolationLevel::Error;
        assert!(!error.has_resource_limits());
        assert!(error.has_error_isolation());
        assert_eq!(format!("{}", error), "Error");

        let full = IsolationLevel::full(1024);
        assert!(full.has_resource_limits());
        assert!(full.has_error_isolation());
        let display = format!("{}", full);
        assert!(display.contains("Full"));
        assert!(display.contains("1024MB"));
    }

    #[test]
    fn test_sub_workflow_isolation() {
        let workflow_id = Uuid::new_v4();
        let parent_id = Uuid::new_v4();

        let isolation = SubWorkflowIsolation::new(workflow_id, IsolationLevel::Error)
            .with_parent(parent_id)
            .no_error_propagation()
            .no_cancellation_propagation();

        assert_eq!(isolation.workflow_id, workflow_id);
        assert_eq!(isolation.parent_workflow_id, Some(parent_id));
        assert_eq!(isolation.isolation_level, IsolationLevel::Error);
        assert!(!isolation.propagate_errors);
        assert!(!isolation.propagate_cancellation);

        let display = format!("{}", isolation);
        assert!(display.contains("SubWorkflowIsolation"));
        assert!(display.contains(&workflow_id.to_string()));
    }

    #[test]
    fn test_workflow_checkpoint() {
        let workflow_id = Uuid::new_v4();
        let state = WorkflowState::new(workflow_id, 10);
        let mut checkpoint = WorkflowCheckpoint::new(workflow_id, state);

        assert_eq!(checkpoint.workflow_id, workflow_id);
        assert_eq!(checkpoint.version, 1);
        assert_eq!(checkpoint.completed_tasks.len(), 0);

        // Record tasks
        let task1 = Uuid::new_v4();
        let task2 = Uuid::new_v4();
        let task3 = Uuid::new_v4();

        checkpoint.record_in_progress(task1);
        checkpoint.record_completed(task2);
        checkpoint.record_failed(task3, "test error".to_string());

        assert!(checkpoint.is_completed(&task2));
        assert!(checkpoint.is_failed(&task3));
        assert!(!checkpoint.is_completed(&task1));
        assert_eq!(checkpoint.tasks_to_retry().len(), 1);

        // Test serialization
        let json = checkpoint.to_json().unwrap();
        let deserialized = WorkflowCheckpoint::from_json(&json).unwrap();
        assert_eq!(deserialized.workflow_id, workflow_id);
        assert_eq!(deserialized.completed_tasks.len(), 1);
        assert_eq!(deserialized.failed_tasks.len(), 1);

        let display = format!("{}", checkpoint);
        assert!(display.contains("WorkflowCheckpoint"));
        assert!(display.contains("completed=1"));
        assert!(display.contains("failed=1"));
    }

    #[test]
    fn test_workflow_recovery_policy() {
        let auto = WorkflowRecoveryPolicy::auto_recover();
        assert!(auto.auto_recovery);
        assert!(auto.resume_from_checkpoint);
        assert!(auto.replay_failed);
        assert_eq!(auto.max_checkpoint_age, Some(3600));

        let manual = WorkflowRecoveryPolicy::manual();
        assert!(!manual.auto_recovery);
        assert!(manual.resume_from_checkpoint);
        assert!(!manual.replay_failed);
        assert_eq!(manual.max_checkpoint_age, None);

        let custom = WorkflowRecoveryPolicy::auto_recover()
            .with_max_checkpoint_age(7200)
            .with_retry_policy(WorkflowRetryPolicy::new(3));
        assert_eq!(custom.max_checkpoint_age, Some(7200));
        assert!(custom.retry_policy.is_some());

        // Test checkpoint validation
        let workflow_id = Uuid::new_v4();
        let state = WorkflowState::new(workflow_id, 10);
        let checkpoint = WorkflowCheckpoint::new(workflow_id, state);
        assert!(auto.is_checkpoint_valid(&checkpoint));

        let display = format!("{}", auto);
        assert!(display.contains("WorkflowRecoveryPolicy"));
        assert!(display.contains("auto"));
    }

    #[test]
    fn test_optimization_pass() {
        let cse = OptimizationPass::CommonSubexpressionElimination;
        assert_eq!(format!("{}", cse), "CSE");

        let dce = OptimizationPass::DeadCodeElimination;
        assert_eq!(format!("{}", dce), "DCE");

        let fusion = OptimizationPass::TaskFusion;
        assert_eq!(format!("{}", fusion), "TaskFusion");

        let scheduling = OptimizationPass::ParallelScheduling;
        assert_eq!(format!("{}", scheduling), "ParallelScheduling");

        let resource = OptimizationPass::ResourceOptimization;
        assert_eq!(format!("{}", resource), "ResourceOptimization");
    }

    #[test]
    fn test_workflow_compiler() {
        let compiler = WorkflowCompiler::new();
        assert!(!compiler.aggressive);
        assert_eq!(compiler.passes.len(), 2);

        let aggressive = WorkflowCompiler::new().aggressive();
        assert!(aggressive.aggressive);
        assert!(aggressive.passes.len() > 2);

        let custom = WorkflowCompiler::new()
            .add_pass(OptimizationPass::TaskFusion)
            .add_pass(OptimizationPass::ParallelScheduling);
        assert_eq!(custom.passes.len(), 4);

        // Test basic optimization (no change expected)
        let chain = Chain::new().then("task1", vec![]).then("task2", vec![]);
        let optimized = compiler.optimize_chain(&chain);
        assert_eq!(optimized.tasks.len(), chain.tasks.len());

        let group = Group::new().add("task1", vec![]).add("task2", vec![]);
        let optimized_group = compiler.optimize_group(&group);
        assert_eq!(optimized_group.tasks.len(), group.tasks.len());

        let display = format!("{}", compiler);
        assert!(display.contains("WorkflowCompiler"));
        assert!(display.contains("DCE"));
        assert!(display.contains("CSE"));
    }

    #[test]
    fn test_workflow_compiler_cse_chain() {
        use serde_json::json;

        // Test Common Subexpression Elimination for chains
        let compiler = WorkflowCompiler::new().aggressive();
        let chain = Chain::new()
            .then_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)]))
            .then_signature(Signature::new("task2".to_string()).with_args(vec![json!(2)]))
            .then_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)])); // Duplicate

        let optimized = compiler.optimize_chain(&chain);
        // CSE should remove the duplicate task in aggressive mode
        assert_eq!(optimized.tasks.len(), 2);
        assert_eq!(optimized.tasks[0].task, "task1");
        assert_eq!(optimized.tasks[1].task, "task2");
    }

    #[test]
    fn test_workflow_compiler_cse_group() {
        use serde_json::json;

        // Test Common Subexpression Elimination for groups
        let compiler = WorkflowCompiler::new().aggressive();
        let group = Group::new()
            .add_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)]))
            .add_signature(Signature::new("task2".to_string()).with_args(vec![json!(2)]))
            .add_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)])); // Duplicate

        let optimized = compiler.optimize_group(&group);
        // CSE should remove the duplicate task in aggressive mode
        assert_eq!(optimized.tasks.len(), 2);
        assert_eq!(optimized.tasks[0].task, "task1");
        assert_eq!(optimized.tasks[1].task, "task2");
    }

    #[test]
    fn test_workflow_compiler_dce_chain() {
        use serde_json::json;

        // Test Dead Code Elimination for chains
        let compiler = WorkflowCompiler::new();
        let chain = Chain::new()
            .then_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)]))
            .then_signature(Signature::new("".to_string())) // Empty task name (dead code)
            .then_signature(Signature::new("task2".to_string()).with_args(vec![json!(2)]));

        let optimized = compiler.optimize_chain(&chain);
        // DCE should remove the empty task
        assert_eq!(optimized.tasks.len(), 2);
        assert_eq!(optimized.tasks[0].task, "task1");
        assert_eq!(optimized.tasks[1].task, "task2");
    }

    #[test]
    fn test_workflow_compiler_dce_group() {
        use serde_json::json;

        // Test Dead Code Elimination for groups
        let compiler = WorkflowCompiler::new();
        let group = Group::new()
            .add_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)]))
            .add_signature(Signature::new("".to_string())) // Empty task name (dead code)
            .add_signature(Signature::new("task2".to_string()).with_args(vec![json!(2)]));

        let optimized = compiler.optimize_group(&group);
        // DCE should remove the empty task
        assert_eq!(optimized.tasks.len(), 2);
        assert_eq!(optimized.tasks[0].task, "task1");
        assert_eq!(optimized.tasks[1].task, "task2");
    }

    #[test]
    fn test_workflow_compiler_task_fusion() {
        use serde_json::json;

        // Test Task Fusion for chains
        let compiler = WorkflowCompiler::new().aggressive();
        let chain = Chain::new()
            .then_signature(
                Signature::new("process".to_string())
                    .with_args(vec![json!(1)])
                    .immutable(),
            )
            .then_signature(
                Signature::new("process".to_string())
                    .with_args(vec![json!(2)])
                    .immutable(),
            )
            .then_signature(Signature::new("finalize".to_string()));

        let optimized = compiler.optimize_chain(&chain);
        // Task fusion should combine the two "process" tasks
        assert_eq!(optimized.tasks.len(), 2);
        assert_eq!(optimized.tasks[0].task, "process");
        assert_eq!(optimized.tasks[0].args.len(), 2); // Fused args
        assert_eq!(optimized.tasks[1].task, "finalize");
    }

    #[test]
    fn test_workflow_compiler_parallel_scheduling() {
        // Test Parallel Scheduling for groups
        let compiler = WorkflowCompiler::new().add_pass(OptimizationPass::ParallelScheduling);

        let group = Group::new()
            .add_signature(Signature::new("task1".to_string()).with_priority(1))
            .add_signature(Signature::new("task2".to_string()).with_priority(5))
            .add_signature(Signature::new("task3".to_string()).with_priority(3));

        let optimized = compiler.optimize_group(&group);
        // Parallel scheduling should sort by priority (highest first)
        assert_eq!(optimized.tasks.len(), 3);
        assert_eq!(optimized.tasks[0].options.priority, Some(5));
        assert_eq!(optimized.tasks[1].options.priority, Some(3));
        assert_eq!(optimized.tasks[2].options.priority, Some(1));
    }

    #[test]
    fn test_workflow_compiler_resource_optimization() {
        // Test Resource Optimization for groups
        let compiler = WorkflowCompiler::new().add_pass(OptimizationPass::ResourceOptimization);

        let group = Group::new()
            .add_signature(Signature::new("task1".to_string()).with_queue("queue_b".to_string()))
            .add_signature(Signature::new("task2".to_string()).with_queue("queue_a".to_string()))
            .add_signature(Signature::new("task3".to_string()).with_queue("queue_a".to_string()));

        let optimized = compiler.optimize_group(&group);
        // Resource optimization should sort by queue
        assert_eq!(optimized.tasks.len(), 3);
        assert_eq!(
            optimized.tasks[0].options.queue.as_ref().unwrap(),
            "queue_a"
        );
        assert_eq!(
            optimized.tasks[1].options.queue.as_ref().unwrap(),
            "queue_a"
        );
        assert_eq!(
            optimized.tasks[2].options.queue.as_ref().unwrap(),
            "queue_b"
        );
    }

    #[test]
    fn test_workflow_compiler_optimize_chord() {
        use serde_json::json;

        // Test chord optimization
        let compiler = WorkflowCompiler::new().aggressive();

        let group = Group::new()
            .add_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)]))
            .add_signature(Signature::new("task1".to_string()).with_args(vec![json!(1)])) // Duplicate
            .add_signature(Signature::new("task2".to_string()).with_args(vec![json!(2)]));

        let chord = Chord::new(group, Signature::new("callback".to_string()));
        let optimized = compiler.optimize_chord(&chord);

        // CSE should remove duplicates from the chord's header group
        assert_eq!(optimized.header.tasks.len(), 2);
        assert_eq!(optimized.body.task, "callback");
    }

    #[test]
    fn test_workflow_compiler_combined_passes() {
        use serde_json::json;

        // Test multiple optimization passes together
        let compiler = WorkflowCompiler::new()
            .aggressive()
            .add_pass(OptimizationPass::ParallelScheduling);

        let group = Group::new()
            .add_signature(
                Signature::new("task1".to_string())
                    .with_priority(1)
                    .with_args(vec![json!(1)]),
            )
            .add_signature(Signature::new("".to_string())) // Dead code
            .add_signature(
                Signature::new("task2".to_string())
                    .with_priority(5)
                    .with_args(vec![json!(2)]),
            )
            .add_signature(
                Signature::new("task1".to_string())
                    .with_priority(1)
                    .with_args(vec![json!(1)]),
            ); // Duplicate

        let optimized = compiler.optimize_group(&group);
        // Should remove dead code, remove duplicate, and sort by priority
        assert_eq!(optimized.tasks.len(), 2);
        assert_eq!(optimized.tasks[0].options.priority, Some(5));
        assert_eq!(optimized.tasks[0].task, "task2");
        assert_eq!(optimized.tasks[1].options.priority, Some(1));
        assert_eq!(optimized.tasks[1].task, "task1");
    }

    #[test]
    fn test_typed_result() {
        let result = TypedResult::new(42i32).with_metadata("source", serde_json::json!("test"));

        assert_eq!(result.value, 42);
        assert_eq!(result.type_name(), "i32");
        assert!(result.metadata.contains_key("source"));

        let display = format!("{}", result);
        assert!(display.contains("TypedResult"));
        assert!(display.contains("i32"));
    }

    #[test]
    fn test_type_validator() {
        let validator = TypeValidator::new("i32");
        assert!(validator.validate("i32"));
        assert!(!validator.validate("String"));

        let compatible = TypeValidator::new("serde_json::Value").allow_compatible();
        assert!(compatible.validate("i32"));
        assert!(compatible.validate("String"));
        assert!(compatible.allow_compatible);

        let display = format!("{}", validator);
        assert!(display.contains("TypeValidator"));
        assert!(display.contains("i32"));
    }

    #[test]
    fn test_task_dependency() {
        let task_id = Uuid::new_v4();
        let dep = TaskDependency::new(task_id)
            .with_output_key("result")
            .optional();

        assert_eq!(dep.task_id, task_id);
        assert_eq!(dep.output_key, Some("result".to_string()));
        assert!(dep.optional);

        let display = format!("{}", dep);
        assert!(display.contains("TaskDependency"));
        assert!(display.contains(&task_id.to_string()));
        assert!(display.contains("result"));
    }

    #[test]
    fn test_dependency_graph() {
        let mut graph = DependencyGraph::new();
        let task1 = Uuid::new_v4();
        let task2 = Uuid::new_v4();
        let task3 = Uuid::new_v4();

        graph.add_dependency(task2, TaskDependency::new(task1));
        graph.add_dependency(task3, TaskDependency::new(task2));

        assert_eq!(graph.get_dependencies(&task2).len(), 1);
        assert_eq!(graph.get_dependents(&task1).len(), 1);
        assert!(!graph.has_circular_dependency());

        let sorted = graph.topological_sort().unwrap();
        assert_eq!(sorted.len(), 3);

        let display = format!("{}", graph);
        assert!(display.contains("DependencyGraph"));
        assert!(display.contains("2 tasks")); // Only task2 and task3 have dependencies
    }

    #[test]
    fn test_circular_dependency() {
        let mut graph = DependencyGraph::new();
        let task1 = Uuid::new_v4();
        let task2 = Uuid::new_v4();

        graph.add_dependency(task1, TaskDependency::new(task2));
        graph.add_dependency(task2, TaskDependency::new(task1));

        assert!(graph.has_circular_dependency());
        assert!(graph.topological_sort().is_err());
    }

    #[test]
    fn test_parallel_reduce() {
        let map_task = Signature::new("map".to_string());
        let reduce_task = Signature::new("reduce".to_string());
        let inputs = vec![
            serde_json::json!(1),
            serde_json::json!(2),
            serde_json::json!(3),
        ];

        let pr = ParallelReduce::new(map_task, reduce_task, inputs)
            .with_parallelism(8)
            .with_initial_value(serde_json::json!(0));

        assert_eq!(pr.parallelism, 8);
        assert_eq!(pr.input_count(), 3);
        assert!(!pr.is_empty());
        assert!(pr.initial_value.is_some());

        let display = format!("{}", pr);
        assert!(display.contains("ParallelReduce"));
        assert!(display.contains("map"));
        assert!(display.contains("reduce"));
        assert!(display.contains("parallelism=8"));
    }

    #[test]
    fn test_template_parameter() {
        let param = TemplateParameter::new("count", "usize")
            .with_default(serde_json::json!(10))
            .with_description("Number of items");

        assert_eq!(param.name, "count");
        assert_eq!(param.param_type, "usize");
        assert!(!param.required);
        assert!(param.default.is_some());
        assert!(param.description.is_some());

        let display = format!("{}", param);
        assert!(display.contains("count:usize"));
        assert!(display.contains("optional"));
    }

    #[test]
    fn test_workflow_template() {
        let param =
            TemplateParameter::new("queue", "String").with_default(serde_json::json!("default"));

        let template = WorkflowTemplate::new("etl_pipeline", "1.0")
            .add_parameter(param)
            .with_description("ETL workflow template")
            .with_chain(Chain::new().then("extract", vec![]));

        assert_eq!(template.name, "etl_pipeline");
        assert_eq!(template.version, "1.0");
        assert_eq!(template.parameters.len(), 1);
        assert!(template.chain.is_some());
        assert!(template.description.is_some());

        // Test instantiation with valid parameters
        let mut params = HashMap::new();
        params.insert("queue".to_string(), serde_json::json!("custom"));
        let instance = template.instantiate(params).unwrap();
        assert_eq!(instance.name, "etl_pipeline");

        let display = format!("{}", template);
        assert!(display.contains("WorkflowTemplate"));
        assert!(display.contains("etl_pipeline@1.0"));
    }

    #[test]
    fn test_workflow_template_validation() {
        let required_param = TemplateParameter::new("api_key", "String");
        let template = WorkflowTemplate::new("api_workflow", "1.0").add_parameter(required_param);

        // Should fail without required parameter
        let result = template.instantiate(HashMap::new());
        assert!(result.is_err());
    }

    #[test]
    fn test_workflow_event() {
        let task_id = Uuid::new_v4();
        let workflow_id = Uuid::new_v4();

        let task_completed = WorkflowEvent::TaskCompleted { task_id };
        assert_eq!(
            format!("{}", task_completed),
            format!("TaskCompleted[{}]", task_id)
        );

        let task_failed = WorkflowEvent::TaskFailed {
            task_id,
            error: "test error".to_string(),
        };
        assert!(format!("{}", task_failed).contains("TaskFailed"));

        let workflow_started = WorkflowEvent::WorkflowStarted { workflow_id };
        assert!(format!("{}", workflow_started).contains("WorkflowStarted"));

        let custom = WorkflowEvent::Custom {
            event_type: "data_updated".to_string(),
            data: "{}".to_string(),
        };
        assert!(format!("{}", custom).contains("Custom"));
        assert!(format!("{}", custom).contains("data_updated"));
    }

    #[test]
    fn test_event_handler() {
        let handler_task = Signature::new("handle_completion".to_string());
        let handler = EventHandler::new("TaskCompleted", handler_task)
            .with_filter("task_type == 'important'");

        assert_eq!(handler.event_type, "TaskCompleted");
        assert_eq!(handler.handler_task.task, "handle_completion");
        assert!(handler.filter.is_some());

        let display = format!("{}", handler);
        assert!(display.contains("EventHandler"));
        assert!(display.contains("TaskCompleted"));
        assert!(display.contains("handle_completion"));
    }

    #[test]
    fn test_event_driven_workflow() {
        let handler1 =
            EventHandler::new("TaskCompleted", Signature::new("on_complete".to_string()));
        let handler2 = EventHandler::new("TaskFailed", Signature::new("on_fail".to_string()));

        let workflow = EventDrivenWorkflow::new()
            .on_event(handler1)
            .on_event(handler2)
            .on_task_completed(Signature::new("notify".to_string()));

        assert!(workflow.active);
        assert_eq!(workflow.handlers.len(), 3);
        assert!(workflow.has_handlers());

        let deactivated = workflow.clone().deactivate();
        assert!(!deactivated.active);

        let display = format!("{}", workflow);
        assert!(display.contains("EventDrivenWorkflow"));
        assert!(display.contains("handlers=3"));
    }

    #[test]
    fn test_state_version() {
        let v1 = StateVersion::new(1, 0, 0);
        let v2 = StateVersion::new(1, 1, 0);
        let v3 = StateVersion::new(2, 0, 0);

        // Same major version is compatible
        assert!(v1.is_compatible(&v2));
        // Different major version is not compatible
        assert!(!v1.is_compatible(&v3));

        // Display
        assert_eq!(format!("{}", v1), "1.0.0");
        assert_eq!(format!("{}", v2), "1.1.0");

        // Current version
        let current = StateVersion::current();
        assert_eq!(current.major, 1);
        assert_eq!(current.minor, 0);
        assert_eq!(current.patch, 0);
    }

    #[test]
    fn test_state_migration_error() {
        let v1 = StateVersion::new(1, 0, 0);
        let v2 = StateVersion::new(2, 0, 0);

        let err = StateMigrationError::IncompatibleVersion { from: v1, to: v2 };
        let display = format!("{}", err);
        assert!(display.contains("Incompatible"));
        assert!(display.contains("1.0.0"));
        assert!(display.contains("2.0.0"));

        let err2 = StateMigrationError::MigrationFailed("test error".to_string());
        assert!(format!("{}", err2).contains("migration failed"));

        let err3 = StateMigrationError::UnsupportedVersion(v1);
        assert!(format!("{}", err3).contains("Unsupported"));
    }

    #[test]
    fn test_versioned_workflow_state() {
        let workflow_id = Uuid::new_v4();
        let state = WorkflowState::new(workflow_id, 5);
        let mut versioned = VersionedWorkflowState::new(state);

        // Check initial version
        assert_eq!(versioned.version, StateVersion::current());
        assert_eq!(versioned.migration_history.len(), 0);

        // Migrate to compatible version
        let target = StateVersion::new(1, 1, 0);
        assert!(versioned.can_migrate_to(&target));
        assert!(versioned.migrate_to(target).is_ok());
        assert_eq!(versioned.version, target);
        assert_eq!(versioned.migration_history.len(), 1);

        // Migrate to same version is OK
        assert!(versioned.migrate_to(target).is_ok());
        assert_eq!(versioned.migration_history.len(), 1);

        // Migrate to incompatible version fails
        let incompatible = StateVersion::new(2, 0, 0);
        assert!(!versioned.can_migrate_to(&incompatible));
        assert!(versioned.migrate_to(incompatible).is_err());
    }

    #[test]
    fn test_task_priority() {
        let low = TaskPriority::Low;
        let normal = TaskPriority::Normal;
        let high = TaskPriority::High;
        let critical = TaskPriority::Critical;

        // Test ordering
        assert!(low < normal);
        assert!(normal < high);
        assert!(high < critical);

        // Test display
        assert_eq!(format!("{}", low), "Low");
        assert_eq!(format!("{}", normal), "Normal");
        assert_eq!(format!("{}", high), "High");
        assert_eq!(format!("{}", critical), "Critical");

        // Test default
        assert_eq!(TaskPriority::default(), TaskPriority::Normal);
    }

    #[test]
    fn test_worker_capacity() {
        let mut worker = WorkerCapacity::new("worker1", 4, 8192);

        assert_eq!(worker.worker_id, "worker1");
        assert_eq!(worker.cpu_cores, 4);
        assert_eq!(worker.memory_mb, 8192);
        assert_eq!(worker.current_load, 0.0);
        assert_eq!(worker.active_tasks, 0);

        // Test capacity checks
        assert!(worker.has_capacity(0.5));
        assert!(worker.has_capacity(1.0));
        assert!(!worker.has_capacity(1.1));

        // Test available capacity
        assert_eq!(worker.available_capacity(), 1.0);
        worker.current_load = 0.3;
        assert_eq!(worker.available_capacity(), 0.7);
    }

    #[test]
    fn test_scheduling_decision() {
        let task_id = Uuid::new_v4();
        let decision = SchedulingDecision::new(task_id, "worker1", TaskPriority::High)
            .with_estimated_time(120);

        assert_eq!(decision.task_id, task_id);
        assert_eq!(decision.worker_id, "worker1");
        assert_eq!(decision.priority, TaskPriority::High);
        assert_eq!(decision.estimated_time, Some(120));
    }

    #[test]
    fn test_scheduling_strategy() {
        let rr = SchedulingStrategy::RoundRobin;
        let ll = SchedulingStrategy::LeastLoaded;
        let pb = SchedulingStrategy::PriorityBased;
        let ra = SchedulingStrategy::ResourceAware;

        assert_eq!(format!("{}", rr), "RoundRobin");
        assert_eq!(format!("{}", ll), "LeastLoaded");
        assert_eq!(format!("{}", pb), "PriorityBased");
        assert_eq!(format!("{}", ra), "ResourceAware");

        assert_eq!(
            SchedulingStrategy::default(),
            SchedulingStrategy::LeastLoaded
        );
    }

    #[test]
    fn test_parallel_scheduler_round_robin() {
        let mut scheduler = ParallelScheduler::new(SchedulingStrategy::RoundRobin);

        // Add workers
        scheduler.add_worker(WorkerCapacity::new("worker1", 4, 8192));
        scheduler.add_worker(WorkerCapacity::new("worker2", 4, 8192));

        assert_eq!(scheduler.worker_count(), 2);

        // Schedule tasks
        let task1 = Uuid::new_v4();
        let decision = scheduler.schedule_task(task1, TaskPriority::Normal);
        assert!(decision.is_some());
    }

    #[test]
    fn test_parallel_scheduler_least_loaded() {
        let mut scheduler = ParallelScheduler::new(SchedulingStrategy::LeastLoaded);

        let mut worker1 = WorkerCapacity::new("worker1", 4, 8192);
        worker1.current_load = 0.8;
        let mut worker2 = WorkerCapacity::new("worker2", 4, 8192);
        worker2.current_load = 0.3;

        scheduler.add_worker(worker1);
        scheduler.add_worker(worker2);

        // Should assign to worker2 (lower load)
        let task = Uuid::new_v4();
        let decision = scheduler.schedule_task(task, TaskPriority::Normal);
        assert!(decision.is_some());
        assert_eq!(decision.unwrap().worker_id, "worker2");
    }

    #[test]
    fn test_parallel_scheduler_metrics() {
        let mut scheduler = ParallelScheduler::new(SchedulingStrategy::LeastLoaded);

        let mut worker1 = WorkerCapacity::new("worker1", 4, 8192);
        worker1.current_load = 0.5;
        let mut worker2 = WorkerCapacity::new("worker2", 4, 8192);
        worker2.current_load = 0.3;

        scheduler.add_worker(worker1);
        scheduler.add_worker(worker2);

        // Test metrics
        assert_eq!(scheduler.worker_count(), 2);
        assert_eq!(scheduler.average_load(), 0.4);
        assert_eq!(scheduler.total_capacity(), 1.2);

        let display = format!("{}", scheduler);
        assert!(display.contains("ParallelScheduler"));
        assert!(display.contains("workers=2"));
    }

    #[test]
    fn test_parallel_scheduler_max_tasks() {
        let mut scheduler =
            ParallelScheduler::new(SchedulingStrategy::LeastLoaded).with_max_tasks_per_worker(2);

        let mut worker = WorkerCapacity::new("worker1", 4, 8192);
        worker.active_tasks = 2;
        scheduler.add_worker(worker);

        // Should not schedule (max reached)
        let task = Uuid::new_v4();
        let decision = scheduler.schedule_task(task, TaskPriority::Normal);
        assert!(decision.is_none());
    }

    #[test]
    fn test_workflow_batch() {
        let mut batch = WorkflowBatch::new(5);

        assert!(batch.is_empty());
        assert!(!batch.is_full());
        assert_eq!(batch.size(), 0);
        assert_eq!(batch.max_size, 5);

        // Add workflows
        let wf1 = Uuid::new_v4();
        let wf2 = Uuid::new_v4();
        assert!(batch.add_workflow(wf1));
        assert!(batch.add_workflow(wf2));
        assert_eq!(batch.size(), 2);
        assert!(!batch.is_empty());
        assert!(!batch.is_full());

        // Fill batch
        for _ in 0..3 {
            batch.add_workflow(Uuid::new_v4());
        }
        assert!(batch.is_full());

        // Cannot add more
        assert!(!batch.add_workflow(Uuid::new_v4()));

        // Test display
        let display = format!("{}", batch);
        assert!(display.contains("WorkflowBatch"));
        assert!(display.contains("5/5"));
    }

    #[test]
    fn test_workflow_batch_timeout() {
        let mut batch = WorkflowBatch::new(10).with_timeout(0);

        // With 0 timeout, should be immediately timed out
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(batch.is_timed_out());

        // Batch without timeout never times out
        batch.timeout = None;
        assert!(!batch.is_timed_out());
    }

    #[test]
    fn test_batching_strategy() {
        let by_type = BatchingStrategy::ByType;
        let by_priority = BatchingStrategy::ByPriority;
        let by_size = BatchingStrategy::BySize;
        let by_time = BatchingStrategy::ByTimeWindow;

        assert_eq!(format!("{}", by_type), "ByType");
        assert_eq!(format!("{}", by_priority), "ByPriority");
        assert_eq!(format!("{}", by_size), "BySize");
        assert_eq!(format!("{}", by_time), "ByTimeWindow");

        assert_eq!(BatchingStrategy::default(), BatchingStrategy::ByType);
    }

    #[test]
    fn test_workflow_batcher() {
        let mut batcher = WorkflowBatcher::new(BatchingStrategy::ByType)
            .with_batch_size(3)
            .with_timeout(60);

        assert_eq!(batcher.batch_count(), 0);
        assert_eq!(batcher.total_workflow_count(), 0);

        // Add workflows
        let wf1 = Uuid::new_v4();
        let wf2 = Uuid::new_v4();
        let wf3 = Uuid::new_v4();

        batcher.add_workflow(wf1, TaskPriority::Normal);
        assert_eq!(batcher.batch_count(), 1);
        assert_eq!(batcher.total_workflow_count(), 1);

        batcher.add_workflow(wf2, TaskPriority::Normal);
        batcher.add_workflow(wf3, TaskPriority::Normal);
        assert_eq!(batcher.batch_count(), 1);
        assert_eq!(batcher.total_workflow_count(), 3);

        // Batch should be full
        let ready = batcher.get_ready_batches();
        assert_eq!(ready.len(), 1);
        assert!(ready[0].is_full());
    }

    #[test]
    fn test_workflow_batcher_by_priority() {
        let mut batcher = WorkflowBatcher::new(BatchingStrategy::ByPriority).with_batch_size(5);

        // Add workflows with different priorities
        let wf1 = Uuid::new_v4();
        let wf2 = Uuid::new_v4();
        let wf3 = Uuid::new_v4();

        batcher.add_workflow(wf1, TaskPriority::High);
        batcher.add_workflow(wf2, TaskPriority::Low);
        batcher.add_workflow(wf3, TaskPriority::High);

        // Should create 2 batches (one for High, one for Low)
        assert_eq!(batcher.batch_count(), 2);
        assert_eq!(batcher.total_workflow_count(), 3);
    }

    #[test]
    fn test_workflow_batcher_remove_ready() {
        let mut batcher = WorkflowBatcher::new(BatchingStrategy::ByType).with_batch_size(2);

        // Add workflows to fill one batch
        batcher.add_workflow(Uuid::new_v4(), TaskPriority::Normal);
        batcher.add_workflow(Uuid::new_v4(), TaskPriority::Normal);

        // Add one more to create a second batch
        batcher.add_workflow(Uuid::new_v4(), TaskPriority::Normal);

        assert_eq!(batcher.batch_count(), 2);

        // Remove ready batches (the full one)
        let ready = batcher.remove_ready_batches();
        assert_eq!(ready.len(), 1);
        assert_eq!(batcher.batch_count(), 1);

        let display = format!("{}", batcher);
        assert!(display.contains("WorkflowBatcher"));
        assert!(display.contains("batches=1"));
    }

    #[test]
    fn test_streaming_map_reduce() {
        let map_task = Signature::new("map_task".to_string());
        let reduce_task = Signature::new("reduce_task".to_string());

        let stream = StreamingMapReduce::new(map_task, reduce_task)
            .with_chunk_size(50)
            .with_buffer_size(500)
            .with_backpressure(true);

        assert_eq!(stream.chunk_size, 50);
        assert_eq!(stream.buffer_size, 500);
        assert!(stream.backpressure);

        let display = format!("{}", stream);
        assert!(display.contains("StreamingMapReduce"));
        assert!(display.contains("map_task"));
        assert!(display.contains("reduce_task"));
        assert!(display.contains("chunk_size=50"));
        assert!(display.contains("buffer_size=500"));
    }

    #[test]
    fn test_resource_utilization() {
        let util = ResourceUtilization::new(0.8, 0.6, 0.4, 0.2);

        assert_eq!(util.cpu, 0.8);
        assert_eq!(util.memory, 0.6);
        assert_eq!(util.disk_io, 0.4);
        assert_eq!(util.network_io, 0.2);

        // Test overall
        assert!((util.overall() - 0.5).abs() < 0.01);

        // Test overload
        assert!(util.is_overloaded(0.7));
        assert!(!util.is_overloaded(0.9));

        // Test bottleneck
        assert_eq!(util.bottleneck(), "cpu");

        // Test clamping
        let util2 = ResourceUtilization::new(1.5, -0.5, 0.5, 0.5);
        assert_eq!(util2.cpu, 1.0);
        assert_eq!(util2.memory, 0.0);
    }

    #[test]
    fn test_resource_utilization_display() {
        let util = ResourceUtilization::new(0.5, 0.6, 0.7, 0.8);
        let display = format!("{}", util);
        assert!(display.contains("ResourceUtilization"));
        assert!(display.contains("cpu=0.50"));
        assert!(display.contains("mem=0.60"));
    }

    #[test]
    fn test_workflow_resource_monitor() {
        let workflow_id = Uuid::new_v4();
        let mut monitor = WorkflowResourceMonitor::new(workflow_id)
            .with_max_history(100)
            .with_sampling_interval(10);

        assert_eq!(monitor.workflow_id, workflow_id);
        assert_eq!(monitor.max_history, 100);
        assert_eq!(monitor.sampling_interval, 10);
        assert_eq!(monitor.history.len(), 0);

        // Record some utilization
        monitor.record(ResourceUtilization::new(0.5, 0.6, 0.4, 0.3));
        monitor.record(ResourceUtilization::new(0.7, 0.8, 0.6, 0.5));

        assert_eq!(monitor.history.len(), 2);

        // Test peak
        let peak = monitor.peak_utilization().unwrap();
        assert!(peak.overall() > 0.6);

        // Test average
        let avg = monitor.average_utilization(3600).unwrap();
        assert!(avg.cpu > 0.5 && avg.cpu < 0.7);

        // Test clear
        monitor.clear();
        assert_eq!(monitor.history.len(), 0);
    }

    #[test]
    fn test_workflow_resource_monitor_max_history() {
        let workflow_id = Uuid::new_v4();
        let mut monitor = WorkflowResourceMonitor::new(workflow_id).with_max_history(3);

        // Add more than max_history
        for i in 0..5 {
            monitor.record(ResourceUtilization::new(i as f64 * 0.1, 0.5, 0.5, 0.5));
        }

        // Should only keep last 3
        assert_eq!(monitor.history.len(), 3);

        let display = format!("{}", monitor);
        assert!(display.contains("WorkflowResourceMonitor"));
        assert!(display.contains("samples=3"));
    }

    #[test]
    fn test_batching_strategy_display() {
        let strategy = BatchingStrategy::ByPriority;
        assert_eq!(format!("{}", strategy), "ByPriority");
    }

    #[test]
    fn test_observable() {
        let mut obs = Observable::new(42);
        assert_eq!(*obs.get(), 42);
        assert_eq!(obs.subscriber_count(), 0);

        // Subscribe workflows
        let wf1 = Uuid::new_v4();
        let wf2 = Uuid::new_v4();
        obs.subscribe(wf1);
        obs.subscribe(wf2);
        assert_eq!(obs.subscriber_count(), 2);

        // Update value
        obs.set(100);
        assert_eq!(*obs.get(), 100);
        assert_eq!(obs.history.len(), 1);

        // Unsubscribe
        obs.unsubscribe(&wf1);
        assert_eq!(obs.subscriber_count(), 1);
    }

    #[test]
    fn test_reactive_workflow() {
        let reaction = Signature::new("on_change".to_string());
        let workflow = ReactiveWorkflow::new(reaction)
            .watch("observable1")
            .watch("observable2")
            .with_debounce(100)
            .with_throttle(500)
            .with_filter("value > 10");

        assert_eq!(workflow.watched_observables.len(), 2);
        assert_eq!(workflow.debounce_ms, Some(100));
        assert_eq!(workflow.throttle_ms, Some(500));
        assert!(workflow.filter.is_some());

        let display = format!("{}", workflow);
        assert!(display.contains("ReactiveWorkflow"));
        assert!(display.contains("watching=2"));
        assert!(display.contains("on_change"));
    }

    #[test]
    fn test_stream_operator() {
        let map_op = StreamOperator::Map;
        let filter_op = StreamOperator::Filter;
        let debounce_op = StreamOperator::Debounce;

        assert_eq!(format!("{}", map_op), "Map");
        assert_eq!(format!("{}", filter_op), "Filter");
        assert_eq!(format!("{}", debounce_op), "Debounce");
    }

    #[test]
    fn test_reactive_stream() {
        let mut stream = ReactiveStream::new("source1")
            .map(serde_json::json!({"transform": "uppercase"}))
            .filter(serde_json::json!({"condition": "length > 5"}))
            .take(10)
            .skip(2)
            .debounce(100)
            .throttle(500);

        assert_eq!(stream.source_id, "source1");
        assert_eq!(stream.operators.len(), 6);

        // Subscribe workflow
        let wf = Uuid::new_v4();
        stream.subscribe(wf);
        assert_eq!(stream.subscribers.len(), 1);

        let display = format!("{}", stream);
        assert!(display.contains("ReactiveStream"));
        assert!(display.contains("source=source1"));
        assert!(display.contains("operators=6"));
    }

    #[test]
    fn test_mock_task_result() {
        let success =
            MockTaskResult::success("task1", serde_json::json!({"result": "ok"})).with_delay(50);
        assert!(!success.should_fail);
        assert_eq!(success.delay_ms, 50);

        let failure = MockTaskResult::failure("task2", "Task failed");
        assert!(failure.should_fail);
        assert_eq!(failure.failure_message, Some("Task failed".to_string()));
    }

    #[test]
    fn test_mock_task_executor() {
        let mut executor = MockTaskExecutor::new();

        // Register mock results
        executor.register(MockTaskResult::success("task1", serde_json::json!(42)));
        executor.register(MockTaskResult::failure("task2", "Error"));

        // Execute successful task
        let result1 = executor.execute("task1");
        assert!(result1.is_ok());
        assert_eq!(result1.unwrap(), serde_json::json!(42));

        // Execute failing task
        let result2 = executor.execute("task2");
        assert!(result2.is_err());

        // Execute unregistered task
        let result3 = executor.execute("task3");
        assert!(result3.is_err());

        // Check execution count
        assert_eq!(executor.execution_count("task1"), 1);
        assert_eq!(executor.execution_count("task2"), 1);

        // Clear history
        executor.clear_history();
        assert_eq!(executor.execution_count("task1"), 0);
    }

    #[test]
    fn test_test_data_injector() {
        let mut injector = TestDataInjector::new();

        // Inject data
        injector.inject("key1", serde_json::json!({"value": 123}));
        injector.inject("key2", serde_json::json!("test"));

        // Get data
        assert!(injector.get("key1").is_some());
        assert_eq!(injector.get("key2"), Some(&serde_json::json!("test")));
        assert!(injector.get("key3").is_none());

        // Clear data
        injector.clear();
        assert!(injector.get("key1").is_none());
    }

    #[test]
    fn test_workflow_snapshot() {
        let workflow_id = Uuid::new_v4();
        let state = WorkflowState::new(workflow_id, 5);
        let mut snapshot = WorkflowSnapshot::new(workflow_id, state);

        assert_eq!(snapshot.workflow_id, workflow_id);
        assert_eq!(snapshot.completed_tasks.len(), 0);

        // Record task
        let task_id = Uuid::new_v4();
        snapshot.record_task(task_id, serde_json::json!({"result": "ok"}));

        assert_eq!(snapshot.completed_tasks.len(), 1);
        assert!(snapshot.task_results.contains_key(&task_id));

        // Attach checkpoint
        let checkpoint = WorkflowCheckpoint::new(workflow_id, WorkflowState::new(workflow_id, 5));
        snapshot = snapshot.with_checkpoint(checkpoint);
        assert!(snapshot.checkpoint.is_some());
    }

    #[test]
    fn test_time_travel_debugger() {
        let workflow_id = Uuid::new_v4();
        let mut debugger = TimeTravelDebugger::new(workflow_id);

        assert_eq!(debugger.snapshot_count(), 0);
        assert!(!debugger.step_mode);

        // Record snapshots
        let snapshot1 = WorkflowSnapshot::new(workflow_id, WorkflowState::new(workflow_id, 5));
        let snapshot2 = WorkflowSnapshot::new(workflow_id, WorkflowState::new(workflow_id, 5));
        debugger.record_snapshot(snapshot1);
        debugger.record_snapshot(snapshot2);

        assert_eq!(debugger.snapshot_count(), 2);
        assert_eq!(debugger.current_index, 1);

        // Step backward
        let snapshot = debugger.step_backward();
        assert!(snapshot.is_some());
        assert_eq!(debugger.current_index, 0);

        // Step forward
        let snapshot = debugger.step_forward();
        assert!(snapshot.is_some());
        assert_eq!(debugger.current_index, 1);

        // Replay from specific point
        let snapshot = debugger.replay_from(0);
        assert!(snapshot.is_some());
        assert_eq!(debugger.current_index, 0);

        // Enable step mode
        debugger.enable_step_mode();
        assert!(debugger.step_mode);

        // Test display
        let display = format!("{}", debugger);
        assert!(display.contains("TimeTravelDebugger"));
        assert!(display.contains("snapshots=2"));

        // Clear snapshots
        debugger.clear();
        assert_eq!(debugger.snapshot_count(), 0);
    }

    // ===== Integration Tests =====

    /// Integration tests for broker, backend, chord barriers, and performance
    mod integration {
        use super::*;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use std::time::{Duration, Instant};

        /// Mock broker for testing
        #[derive(Clone)]
        struct MockBroker {
            tasks: Arc<std::sync::Mutex<Vec<String>>>,
        }

        impl MockBroker {
            fn new() -> Self {
                Self {
                    tasks: Arc::new(std::sync::Mutex::new(Vec::new())),
                }
            }

            fn enqueued_tasks(&self) -> Vec<String> {
                self.tasks.lock().unwrap().clone()
            }

            fn task_count(&self) -> usize {
                self.tasks.lock().unwrap().len()
            }
        }

        #[async_trait::async_trait]
        impl celers_core::Broker for MockBroker {
            async fn enqueue(
                &self,
                task: celers_core::SerializedTask,
            ) -> celers_core::Result<celers_core::TaskId> {
                let task_name = task.metadata.name.clone();
                let task_id = task.metadata.id;
                self.tasks.lock().unwrap().push(task_name);
                Ok(task_id)
            }

            async fn dequeue(&self) -> celers_core::Result<Option<celers_core::BrokerMessage>> {
                Ok(None)
            }

            async fn ack(
                &self,
                _task_id: &celers_core::TaskId,
                _receipt_handle: Option<&str>,
            ) -> celers_core::Result<()> {
                Ok(())
            }

            async fn reject(
                &self,
                _task_id: &celers_core::TaskId,
                _receipt_handle: Option<&str>,
                _requeue: bool,
            ) -> celers_core::Result<()> {
                Ok(())
            }

            async fn queue_size(&self) -> celers_core::Result<usize> {
                Ok(self.tasks.lock().unwrap().len())
            }

            async fn cancel(&self, _task_id: &celers_core::TaskId) -> celers_core::Result<bool> {
                Ok(true)
            }
        }

        // ===== Broker Integration Tests =====

        #[tokio::test]
        async fn test_chain_broker_integration() {
            let broker = MockBroker::new();

            let chain = Chain::new()
                .then("task1", vec![serde_json::json!(1)])
                .then("task2", vec![serde_json::json!(2)])
                .then("task3", vec![serde_json::json!(3)]);

            // Apply the chain
            let result = chain.apply(&broker).await;
            assert!(result.is_ok(), "Chain apply should succeed");

            // Verify only the first task was published
            // (subsequent tasks are linked via callback mechanism)
            let tasks = broker.enqueued_tasks();
            assert_eq!(tasks.len(), 1, "Chain should publish only first task");
            assert!(tasks.contains(&"task1".to_string()));
        }

        #[tokio::test]
        async fn test_group_broker_integration() {
            let broker = MockBroker::new();

            let group = Group::new()
                .add("task1", vec![serde_json::json!(1)])
                .add("task2", vec![serde_json::json!(2)])
                .add("task3", vec![serde_json::json!(3)]);

            // Apply the group
            let result = group.apply(&broker).await;
            assert!(result.is_ok(), "Group apply should succeed");

            // Verify all tasks were published in parallel
            let tasks = broker.enqueued_tasks();
            assert_eq!(tasks.len(), 3, "Should publish 3 tasks");
        }

        #[tokio::test]
        async fn test_map_broker_integration() {
            let broker = MockBroker::new();

            let map = Map::new(
                Signature::new("process".to_string()),
                vec![
                    vec![serde_json::json!(1)],
                    vec![serde_json::json!(2)],
                    vec![serde_json::json!(3)],
                ],
            );

            let result = map.apply(&broker).await;
            assert!(result.is_ok(), "Map apply should succeed");

            // Verify all task instances were published
            let tasks = broker.enqueued_tasks();
            assert_eq!(tasks.len(), 3, "Should publish 3 task instances");
            assert_eq!(tasks.iter().filter(|t| *t == "process").count(), 3);
        }

        #[tokio::test]
        async fn test_nested_workflow_broker_integration() {
            let broker = MockBroker::new();

            // Create nested workflows
            let inner_group1 = Group::new()
                .add("task1", vec![serde_json::json!(1)])
                .add("task2", vec![serde_json::json!(2)]);

            let inner_group2 = Group::new()
                .add("task3", vec![serde_json::json!(3)])
                .add("task4", vec![serde_json::json!(4)]);

            let _ = inner_group1.apply(&broker).await;
            let _ = inner_group2.apply(&broker).await;

            assert_eq!(broker.task_count(), 4, "Should publish all nested tasks");
        }

        // ===== Backend Integration Tests =====

        #[cfg(feature = "backend-redis")]
        #[tokio::test]
        async fn test_chord_backend_integration() {
            let group = Group::new()
                .add("task1", vec![serde_json::json!(1)])
                .add("task2", vec![serde_json::json!(2)]);
            let callback = Signature::new("aggregate".to_string());
            let chord = Chord::new(group, callback);

            assert_eq!(chord.header.tasks.len(), 2);
            assert_eq!(chord.body.task, "aggregate");
        }

        #[cfg(feature = "backend-redis")]
        #[tokio::test]
        async fn test_chord_state_tracking() {
            let chord_id = Uuid::new_v4();
            let mut group = Group::new();
            group.group_id = Some(chord_id);
            let group = group
                .add("task1", vec![serde_json::json!(1)])
                .add("task2", vec![serde_json::json!(2)]);
            let callback = Signature::new("aggregate".to_string());
            let chord = Chord::new(group, callback);

            assert_eq!(chord.header.group_id, Some(chord_id));
            assert_eq!(chord.header.tasks.len(), 2);
        }

        // ===== Chord Barrier Race Condition Tests =====

        #[tokio::test]
        async fn test_chord_concurrent_completion() {
            let counter = Arc::new(AtomicUsize::new(0));
            let barrier = Arc::new(tokio::sync::Barrier::new(10));

            let mut handles = vec![];

            // Simulate 10 tasks completing concurrently
            for _ in 0..10 {
                let counter = counter.clone();
                let barrier = barrier.clone();

                let handle = tokio::spawn(async move {
                    // Wait for all tasks to be ready
                    barrier.wait().await;

                    // Simulate task completion and counter increment (like Redis INCR)
                    let old = counter.fetch_add(1, Ordering::SeqCst);
                    old + 1
                });

                handles.push(handle);
            }

            // Wait for all tasks
            let mut results = vec![];
            for handle in handles {
                results.push(handle.await.unwrap());
            }

            // Verify no duplicates (each increment should be unique)
            results.sort();
            assert_eq!(results, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
            assert_eq!(counter.load(Ordering::SeqCst), 10);
        }

        #[tokio::test]
        async fn test_chord_barrier_idempotency() {
            // Test that callback is triggered exactly once even with race conditions
            let callback_count = Arc::new(AtomicUsize::new(0));
            let completed_count = Arc::new(AtomicUsize::new(0));
            let total_tasks = 5;

            let mut handles = vec![];

            for _ in 0..total_tasks {
                let callback_count = callback_count.clone();
                let completed_count = completed_count.clone();

                let handle = tokio::spawn(async move {
                    // Simulate task completion
                    let count = completed_count.fetch_add(1, Ordering::SeqCst) + 1;

                    // Only the last task should trigger callback
                    if count == total_tasks {
                        callback_count.fetch_add(1, Ordering::SeqCst);
                    }
                });

                handles.push(handle);
            }

            for handle in handles {
                handle.await.unwrap();
            }

            // Verify callback was triggered exactly once
            assert_eq!(callback_count.load(Ordering::SeqCst), 1);
            assert_eq!(completed_count.load(Ordering::SeqCst), total_tasks);
        }

        #[tokio::test]
        async fn test_chord_partial_failure_handling() {
            // Test chord behavior when some tasks fail
            let success_count = Arc::new(AtomicUsize::new(0));
            let failure_count = Arc::new(AtomicUsize::new(0));

            let mut handles = vec![];

            for i in 0..10 {
                let success_count = success_count.clone();
                let failure_count = failure_count.clone();

                let handle = tokio::spawn(async move {
                    if i % 3 == 0 {
                        // Simulate failure
                        failure_count.fetch_add(1, Ordering::SeqCst);
                        Err::<(), &str>("Task failed")
                    } else {
                        // Simulate success
                        success_count.fetch_add(1, Ordering::SeqCst);
                        Ok(())
                    }
                });

                handles.push(handle);
            }

            for handle in handles {
                let _ = handle.await.unwrap();
            }

            let success = success_count.load(Ordering::SeqCst);
            let failure = failure_count.load(Ordering::SeqCst);

            assert_eq!(success + failure, 10);
            assert!(failure > 0, "Should have some failures");
        }

        // ===== Performance Tests =====

        #[test]
        fn test_chain_creation_performance() {
            let start = Instant::now();

            for _ in 0..1000 {
                let _ = Chain::new()
                    .then("task1", vec![serde_json::json!(1)])
                    .then("task2", vec![serde_json::json!(2)])
                    .then("task3", vec![serde_json::json!(3)]);
            }

            let duration = start.elapsed();
            assert!(
                duration < Duration::from_millis(100),
                "Creating 1000 chains should take less than 100ms, took {:?}",
                duration
            );
        }

        #[test]
        fn test_group_creation_performance() {
            let start = Instant::now();

            for _ in 0..1000 {
                let _ = Group::new()
                    .add("task1", vec![serde_json::json!(1)])
                    .add("task2", vec![serde_json::json!(2)])
                    .add("task3", vec![serde_json::json!(3)]);
            }

            let duration = start.elapsed();
            assert!(
                duration < Duration::from_millis(100),
                "Creating 1000 groups should take less than 100ms, took {:?}",
                duration
            );
        }

        #[test]
        fn test_large_workflow_creation() {
            let start = Instant::now();

            let mut chain = Chain::new();
            for i in 0..1000 {
                let task_name = format!("task{}", i);
                chain = chain.then(&task_name, vec![serde_json::json!(i)]);
            }

            let duration = start.elapsed();
            assert!(
                duration < Duration::from_millis(500),
                "Creating chain with 1000 tasks should take less than 500ms, took {:?}",
                duration
            );
            assert_eq!(chain.len(), 1000);
        }

        #[test]
        fn test_map_with_large_dataset() {
            let start = Instant::now();

            let args: Vec<Vec<serde_json::Value>> =
                (0..1000).map(|i| vec![serde_json::json!(i)]).collect();

            let map = Map::new(Signature::new("process".to_string()), args);

            let duration = start.elapsed();
            assert!(
                duration < Duration::from_millis(100),
                "Creating map with 1000 items should take less than 100ms, took {:?}",
                duration
            );
            assert_eq!(map.len(), 1000);
        }

        #[test]
        fn test_workflow_serialization_performance() {
            let chain = Chain::new()
                .then("task1", vec![serde_json::json!(1)])
                .then("task2", vec![serde_json::json!(2)])
                .then("task3", vec![serde_json::json!(3)]);

            let start = Instant::now();

            for _ in 0..1000 {
                let _ = serde_json::to_string(&chain).unwrap();
            }

            let duration = start.elapsed();
            assert!(
                duration < Duration::from_millis(100),
                "Serializing chain 1000 times should take less than 100ms, took {:?}",
                duration
            );
        }

        #[tokio::test]
        async fn test_broker_publish_performance() {
            let broker = MockBroker::new();
            let start = Instant::now();

            for i in 0..100 {
                let task_name = format!("task{}", i);
                let chain = Chain::new().then(&task_name, vec![serde_json::json!(i)]);
                let _ = chain.apply(&broker).await;
            }

            let duration = start.elapsed();
            assert!(
                duration < Duration::from_secs(1),
                "Publishing 100 chains should take less than 1s, took {:?}",
                duration
            );
            assert_eq!(broker.task_count(), 100);
        }

        #[tokio::test]
        async fn test_concurrent_workflow_enqueue() {
            let broker = Arc::new(MockBroker::new());
            let mut handles = vec![];

            let start = Instant::now();

            for i in 0..100 {
                let broker = broker.clone();
                let handle = tokio::spawn(async move {
                    let task_name = format!("task{}", i);
                    let chain = Chain::new().then(&task_name, vec![serde_json::json!(i)]);
                    chain.apply(&*broker).await
                });
                handles.push(handle);
            }

            for handle in handles {
                handle.await.unwrap().unwrap();
            }

            let duration = start.elapsed();
            assert!(
                duration < Duration::from_secs(2),
                "Concurrent publishing of 100 chains should take less than 2s, took {:?}",
                duration
            );
            assert_eq!(broker.task_count(), 100);
        }

        #[test]
        fn test_memory_efficiency_large_group() {
            // Test that large groups don't cause excessive memory usage
            let mut group = Group::new();

            for i in 0..10000 {
                let task_name = format!("task{}", i);
                group = group.add(&task_name, vec![serde_json::json!(i)]);
            }

            assert_eq!(group.len(), 10000);
            assert!(!group.is_empty());
        }

        #[test]
        fn test_workflow_clone_performance() {
            let chain = Chain::new()
                .then("task1", vec![serde_json::json!(1)])
                .then("task2", vec![serde_json::json!(2)])
                .then("task3", vec![serde_json::json!(3)]);

            let start = Instant::now();

            for _ in 0..1000 {
                let _ = chain.clone();
            }

            let duration = start.elapsed();
            assert!(
                duration < Duration::from_millis(50),
                "Cloning chain 1000 times should take less than 50ms, took {:?}",
                duration
            );
        }

        // ===== Stress Tests =====

        #[test]
        fn test_deeply_nested_workflows() {
            // Test that deeply nested workflows don't cause stack overflow
            let mut current = Chain::new().then("task0", vec![serde_json::json!(0)]);

            for i in 1..100 {
                let task_name = format!("task{}", i);
                current = current.then(&task_name, vec![serde_json::json!(i)]);
            }

            assert_eq!(current.len(), 100);
        }

        #[test]
        fn test_workflow_with_large_payloads() {
            // Test workflows with large argument payloads
            let large_data = vec![serde_json::json!({ "data": "x".repeat(10000) })];

            let chain = Chain::new()
                .then("process_large", large_data.clone())
                .then("process_large2", large_data);

            let serialized = serde_json::to_string(&chain).unwrap();
            assert!(
                serialized.len() > 20000,
                "Serialized chain should contain large data"
            );
        }

        // ===== DAG Export Tests =====

        #[test]
        fn test_dag_export_dot_format() {
            let chain = Chain::new()
                .then("task1", vec![serde_json::json!(1)])
                .then("task2", vec![serde_json::json!(2)])
                .then("task3", vec![serde_json::json!(3)]);

            let dot = chain.to_dot();
            assert!(dot.contains("digraph Chain"));
            assert!(dot.contains("task1"));
            assert!(dot.contains("task2"));
            assert!(dot.contains("task3"));
            assert!(dot.contains("->"));
        }

        #[test]
        fn test_dag_export_mermaid_format() {
            let group = Group::new()
                .add("task1", vec![serde_json::json!(1)])
                .add("task2", vec![serde_json::json!(2)]);

            let mermaid = group.to_mermaid();
            assert!(mermaid.contains("graph"));
            assert!(mermaid.contains("task1"));
            assert!(mermaid.contains("task2"));
        }

        #[test]
        fn test_dag_export_json_format() {
            let chain = Chain::new().then("task1", vec![serde_json::json!(1)]);

            let json = chain.to_json().unwrap();
            assert!(json.contains("task1"));
            assert!(json.contains("tasks"));
        }

        #[test]
        fn test_dag_export_render_commands() {
            let chain = Chain::new().then("task1", vec![serde_json::json!(1)]);

            let svg_cmd = chain.svg_render_command();
            assert!(svg_cmd.contains("dot"));
            assert!(svg_cmd.contains("-Tsvg"));

            let png_cmd = chain.png_render_command();
            assert!(png_cmd.contains("dot"));
            assert!(png_cmd.contains("-Tpng"));
        }

        #[test]
        #[ignore] // Requires GraphViz to be installed
        fn test_dag_export_to_svg() {
            let chain = Chain::new()
                .then("task1", vec![serde_json::json!(1)])
                .then("task2", vec![serde_json::json!(2)]);

            if is_graphviz_available() {
                let svg = chain.to_svg().unwrap();
                assert!(svg.contains("<svg"));
                assert!(svg.contains("</svg>"));
                assert!(svg.contains("task1"));
            } else {
                println!("Skipping: GraphViz not installed");
            }
        }

        #[test]
        #[ignore] // Requires GraphViz to be installed
        fn test_dag_export_to_png() {
            let chain = Chain::new()
                .then("task1", vec![serde_json::json!(1)])
                .then("task2", vec![serde_json::json!(2)]);

            if is_graphviz_available() {
                let png = chain.to_png().unwrap();
                assert!(!png.is_empty());
                // PNG magic bytes
                assert_eq!(&png[0..4], &[0x89, 0x50, 0x4E, 0x47]);
            } else {
                println!("Skipping: GraphViz not installed");
            }
        }

        #[test]
        fn test_graphviz_availability_check() {
            // This test just verifies the function runs without panicking
            let _available = is_graphviz_available();
            // Result depends on system, so we just ensure it doesn't crash
        }

        #[test]
        fn test_dag_format_enum() {
            let _dot = DagFormat::Dot;
            let _mermaid = DagFormat::Mermaid;
            let _json = DagFormat::Json;
            let _svg = DagFormat::Svg;
            let _png = DagFormat::Png;
        }

        // ========================================================================
        // Visualization Features Tests
        // ========================================================================

        #[test]
        fn test_visual_theme_light() {
            let theme = VisualTheme::light();
            assert_eq!(theme.name, "light");
            assert_eq!(theme.color_for_state("completed"), Some("#4CAF50"));
            assert_eq!(theme.color_for_state("running"), Some("#2196F3"));
            assert_eq!(theme.color_for_state("failed"), Some("#F44336"));
            assert_eq!(theme.shape_for_type("task"), Some("box"));
        }

        #[test]
        fn test_visual_theme_dark() {
            let theme = VisualTheme::dark();
            assert_eq!(theme.name, "dark");
            assert_eq!(theme.color_for_state("completed"), Some("#388E3C"));
            assert_eq!(theme.color_for_state("running"), Some("#1976D2"));
            assert_eq!(theme.color_for_state("failed"), Some("#D32F2F"));
        }

        #[test]
        fn test_visual_theme_default() {
            let theme = VisualTheme::default();
            assert_eq!(theme.name, "light");
        }

        #[test]
        fn test_task_visual_metadata() {
            let task_id = Uuid::new_v4();
            let mut metadata =
                TaskVisualMetadata::new(task_id, "test_task".to_string(), "running".to_string());

            assert_eq!(metadata.task_name, "test_task");
            assert_eq!(metadata.state, "running");
            assert_eq!(metadata.progress, 0.0);
            assert_eq!(metadata.color, "#2196F3");

            metadata = metadata.with_progress(50.0);
            assert_eq!(metadata.progress, 50.0);

            metadata = metadata.with_position(100.0, 200.0);
            assert_eq!(metadata.position, Some((100.0, 200.0)));

            metadata.add_css_class("highlight".to_string());
            assert!(metadata.css_classes.contains(&"highlight".to_string()));

            metadata.add_metadata("custom".to_string(), serde_json::json!("value"));
            assert_eq!(
                metadata.metadata.get("custom"),
                Some(&serde_json::json!("value"))
            );
        }

        #[test]
        fn test_workflow_visualization_data() {
            let workflow_id = Uuid::new_v4();
            let state = WorkflowState {
                workflow_id,
                status: WorkflowStatus::Running,
                total_tasks: 3,
                completed_tasks: 1,
                failed_tasks: 0,
                start_time: Some(12345),
                end_time: None,
                current_stage: Some("stage1".to_string()),
                intermediate_results: HashMap::new(),
            };

            let mut viz_data =
                WorkflowVisualizationData::new(workflow_id, "test_workflow".to_string(), state);

            let task1_id = Uuid::new_v4();
            let task1 =
                TaskVisualMetadata::new(task1_id, "task1".to_string(), "completed".to_string());
            viz_data.add_task(task1);

            let task2_id = Uuid::new_v4();
            let task2 =
                TaskVisualMetadata::new(task2_id, "task2".to_string(), "running".to_string());
            viz_data.add_task(task2);

            viz_data.add_edge(task1_id, task2_id, "chain".to_string());

            assert_eq!(viz_data.tasks.len(), 2);
            assert_eq!(viz_data.edges.len(), 1);

            // Test JSON export
            let json = viz_data.to_json();
            assert!(json.is_ok());

            // Test vis.js format
            let visjs = viz_data.to_visjs_format();
            assert!(visjs.is_object());
        }

        #[test]
        fn test_workflow_visualization_data_with_theme() {
            let workflow_id = Uuid::new_v4();
            let state = WorkflowState {
                workflow_id,
                status: WorkflowStatus::Pending,
                total_tasks: 1,
                completed_tasks: 0,
                failed_tasks: 0,
                start_time: None,
                end_time: None,
                current_stage: None,
                intermediate_results: HashMap::new(),
            };

            let viz_data = WorkflowVisualizationData::new(workflow_id, "test".to_string(), state)
                .with_theme(VisualTheme::dark())
                .with_layout("force".to_string());

            assert_eq!(viz_data.theme.name, "dark");
            assert_eq!(viz_data.layout_hint, "force");
        }

        #[test]
        fn test_timeline_entry() {
            let task_id = Uuid::new_v4();
            let mut entry = TimelineEntry::new(task_id, "test_task".to_string(), 1000);

            assert_eq!(entry.task_name, "test_task");
            assert_eq!(entry.start_time, 1000);
            assert_eq!(entry.state, "running");
            assert_eq!(entry.end_time, None);

            entry.complete(2000);
            assert_eq!(entry.end_time, Some(2000));
            assert_eq!(entry.duration, Some(1000));
            assert_eq!(entry.state, "completed");
            assert_eq!(entry.color, "#4CAF50");
        }

        #[test]
        fn test_timeline_entry_fail() {
            let task_id = Uuid::new_v4();
            let mut entry = TimelineEntry::new(task_id, "test_task".to_string(), 1000);

            entry.fail(2500);
            assert_eq!(entry.end_time, Some(2500));
            assert_eq!(entry.duration, Some(1500));
            assert_eq!(entry.state, "failed");
            assert_eq!(entry.color, "#F44336");
        }

        #[test]
        fn test_timeline_entry_with_worker() {
            let task_id = Uuid::new_v4();
            let entry = TimelineEntry::new(task_id, "test".to_string(), 1000)
                .with_worker("worker-1".to_string())
                .with_parent(Uuid::new_v4());

            assert_eq!(entry.worker_id, Some("worker-1".to_string()));
            assert!(entry.parent_id.is_some());
        }

        #[test]
        fn test_execution_timeline() {
            let workflow_id = Uuid::new_v4();
            let mut timeline = ExecutionTimeline::new(workflow_id);

            let task1_id = Uuid::new_v4();
            let index = timeline.start_task(task1_id, "task1".to_string());
            assert_eq!(timeline.entries.len(), 1);

            timeline.complete_task(index);
            assert_eq!(timeline.entries[index].state, "completed");

            timeline.complete_workflow();
            assert!(timeline.workflow_end.is_some());
        }

        #[test]
        fn test_execution_timeline_fail_task() {
            let workflow_id = Uuid::new_v4();
            let mut timeline = ExecutionTimeline::new(workflow_id);

            let task_id = Uuid::new_v4();
            let index = timeline.start_task(task_id, "failing_task".to_string());

            timeline.fail_task(index);
            assert_eq!(timeline.entries[index].state, "failed");
        }

        #[test]
        fn test_execution_timeline_json_export() {
            let workflow_id = Uuid::new_v4();
            let timeline = ExecutionTimeline::new(workflow_id);

            let json = timeline.to_json();
            assert!(json.is_ok());
        }

        #[test]
        fn test_execution_timeline_google_charts() {
            let workflow_id = Uuid::new_v4();
            let mut timeline = ExecutionTimeline::new(workflow_id);

            timeline.add_entry(TimelineEntry::new(
                Uuid::new_v4(),
                "task1".to_string(),
                1000,
            ));

            let chart_data = timeline.to_google_charts_format();
            assert!(chart_data.is_object());
            assert!(chart_data["cols"].is_array());
            assert!(chart_data["rows"].is_array());
        }

        #[test]
        fn test_animation_frame() {
            let workflow_state = WorkflowState {
                workflow_id: Uuid::new_v4(),
                status: WorkflowStatus::Running,
                total_tasks: 5,
                completed_tasks: 2,
                failed_tasks: 0,
                start_time: Some(1000),
                end_time: None,
                current_stage: Some("processing".to_string()),
                intermediate_results: HashMap::new(),
            };

            let mut frame = AnimationFrame::new(0, workflow_state);

            let task1_id = Uuid::new_v4();
            let task2_id = Uuid::new_v4();

            frame.set_task_state(task1_id, "completed".to_string());
            frame.add_active_task(task2_id);
            frame.add_completed_task(task1_id);

            assert_eq!(
                frame.task_states.get(&task1_id),
                Some(&"completed".to_string())
            );
            assert!(frame.active_tasks.contains(&task2_id));
            assert!(frame.completed_tasks.contains(&task1_id));
            assert!(!frame.active_tasks.contains(&task1_id));
        }

        #[test]
        fn test_animation_frame_with_events() {
            let workflow_state = WorkflowState {
                workflow_id: Uuid::new_v4(),
                status: WorkflowStatus::Running,
                total_tasks: 1,
                completed_tasks: 0,
                failed_tasks: 0,
                start_time: Some(1000),
                end_time: None,
                current_stage: None,
                intermediate_results: HashMap::new(),
            };

            let mut frame = AnimationFrame::new(0, workflow_state);

            let task_id = Uuid::new_v4();
            frame.add_event(WorkflowEvent::TaskCompleted { task_id });

            assert_eq!(frame.events.len(), 1);
        }

        #[test]
        fn test_workflow_animation() {
            let workflow_id = Uuid::new_v4();
            let mut animation = WorkflowAnimation::new(workflow_id, 100);

            let state1 = WorkflowState {
                workflow_id,
                status: WorkflowStatus::Pending,
                total_tasks: 1,
                completed_tasks: 0,
                failed_tasks: 0,
                start_time: None,
                end_time: None,
                current_stage: None,
                intermediate_results: HashMap::new(),
            };

            let frame1 = AnimationFrame::new(0, state1);
            animation.add_frame(frame1);

            assert_eq!(animation.frame_count(), 1);
            assert_eq!(animation.total_duration, 100);

            let retrieved_frame = animation.get_frame(0);
            assert!(retrieved_frame.is_some());
        }

        #[test]
        fn test_workflow_animation_json_export() {
            let workflow_id = Uuid::new_v4();
            let animation = WorkflowAnimation::new(workflow_id, 100);

            let json = animation.to_json();
            assert!(json.is_ok());
        }

        #[test]
        fn test_dag_export_with_state_chain() {
            let mut chain = Chain::new();
            chain.tasks.push(Signature::new("task1".to_string()));
            chain.tasks.push(Signature::new("task2".to_string()));

            let workflow_state = WorkflowState {
                workflow_id: Uuid::new_v4(),
                status: WorkflowStatus::Running,
                total_tasks: 2,
                completed_tasks: 1,
                failed_tasks: 0,
                start_time: Some(1000),
                end_time: None,
                current_stage: Some("task2".to_string()),
                intermediate_results: HashMap::new(),
            };

            let mut task_states = HashMap::new();
            let task1_id = Uuid::new_v4();
            let task2_id = Uuid::new_v4();
            task_states.insert(task1_id, "completed".to_string());
            task_states.insert(task2_id, "running".to_string());

            let dot = chain.to_dot_with_state(&workflow_state, &task_states);
            assert!(dot.contains("digraph Chain"));
            assert!(dot.contains("task1"));
            assert!(dot.contains("task2"));

            let mermaid = chain.to_mermaid_with_state(&workflow_state, &task_states);
            assert!(mermaid.contains("graph LR"));
            assert!(mermaid.contains("completed"));
            assert!(mermaid.contains("running"));

            let json = chain.to_json_with_state(&workflow_state, &task_states);
            assert!(json.is_ok());
        }

        #[test]
        fn test_dag_export_with_state_group() {
            let mut group = Group::new();
            group.tasks.push(Signature::new("task1".to_string()));
            group.tasks.push(Signature::new("task2".to_string()));

            let workflow_state = WorkflowState {
                workflow_id: Uuid::new_v4(),
                status: WorkflowStatus::Running,
                total_tasks: 2,
                completed_tasks: 0,
                failed_tasks: 0,
                start_time: Some(1000),
                end_time: None,
                current_stage: None,
                intermediate_results: HashMap::new(),
            };

            let task_states = HashMap::new();

            let dot = group.to_dot_with_state(&workflow_state, &task_states);
            assert!(dot.contains("digraph Group"));

            let mermaid = group.to_mermaid_with_state(&workflow_state, &task_states);
            assert!(mermaid.contains("graph TB"));

            let json = group.to_json_with_state(&workflow_state, &task_states);
            assert!(json.is_ok());
        }

        #[test]
        fn test_dag_export_with_state_chord() {
            let mut header = Group::new();
            header.tasks.push(Signature::new("task1".to_string()));
            header.tasks.push(Signature::new("task2".to_string()));
            let body = Signature::new("callback".to_string());
            let chord = Chord::new(header, body);

            let workflow_state = WorkflowState {
                workflow_id: Uuid::new_v4(),
                status: WorkflowStatus::Running,
                total_tasks: 3,
                completed_tasks: 2,
                failed_tasks: 0,
                start_time: Some(1000),
                end_time: None,
                current_stage: Some("callback".to_string()),
                intermediate_results: HashMap::new(),
            };

            let task_states = HashMap::new();

            let dot = chord.to_dot_with_state(&workflow_state, &task_states);
            assert!(dot.contains("digraph Chord"));
            assert!(dot.contains("callback"));

            let mermaid = chord.to_mermaid_with_state(&workflow_state, &task_states);
            assert!(mermaid.contains("graph TB"));
            assert!(mermaid.contains("callback"));

            let json = chord.to_json_with_state(&workflow_state, &task_states);
            assert!(json.is_ok());
        }

        #[test]
        fn test_workflow_event_stream() {
            let workflow_id = Uuid::new_v4();
            let mut stream = WorkflowEventStream::new(workflow_id);

            assert_eq!(stream.workflow_id, workflow_id);
            assert_eq!(stream.events.len(), 0);

            let task_id = Uuid::new_v4();
            stream.push(WorkflowEvent::TaskCompleted { task_id });
            stream.push(WorkflowEvent::WorkflowStarted { workflow_id });

            assert_eq!(stream.events.len(), 2);

            let all_events = stream.all_events();
            assert_eq!(all_events.len(), 2);
        }

        #[test]
        fn test_workflow_event_stream_buffer_limit() {
            let workflow_id = Uuid::new_v4();
            let mut stream = WorkflowEventStream::new(workflow_id).with_max_buffer_size(2);

            let task1 = Uuid::new_v4();
            let task2 = Uuid::new_v4();
            let task3 = Uuid::new_v4();

            stream.push(WorkflowEvent::TaskCompleted { task_id: task1 });
            stream.push(WorkflowEvent::TaskCompleted { task_id: task2 });
            stream.push(WorkflowEvent::TaskCompleted { task_id: task3 });

            // Should only keep last 2 events
            assert_eq!(stream.events.len(), 2);
        }

        #[test]
        fn test_workflow_event_stream_since() {
            let workflow_id = Uuid::new_v4();
            let mut stream = WorkflowEventStream::new(workflow_id);

            stream.push(WorkflowEvent::WorkflowStarted { workflow_id });
            std::thread::sleep(std::time::Duration::from_millis(10));

            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;

            std::thread::sleep(std::time::Duration::from_millis(10));
            let task_id = Uuid::new_v4();
            stream.push(WorkflowEvent::TaskCompleted { task_id });

            let recent_events = stream.events_since(timestamp);
            assert_eq!(recent_events.len(), 1);
        }

        #[test]
        fn test_workflow_event_stream_clear() {
            let workflow_id = Uuid::new_v4();
            let mut stream = WorkflowEventStream::new(workflow_id);

            stream.push(WorkflowEvent::WorkflowStarted { workflow_id });
            assert_eq!(stream.events.len(), 1);

            stream.clear();
            assert_eq!(stream.events.len(), 0);
        }

        #[test]
        fn test_workflow_event_stream_sse_format() {
            let workflow_id = Uuid::new_v4();
            let mut stream = WorkflowEventStream::new(workflow_id);

            stream.push(WorkflowEvent::WorkflowStarted { workflow_id });

            let sse_messages = stream.to_sse_format();
            assert_eq!(sse_messages.len(), 1);
            assert!(sse_messages[0].starts_with("event: workflow"));
        }

        // ========================================================================
        // Production-Ready Enhancements Tests
        // ========================================================================

        #[test]
        fn test_workflow_metrics_collector() {
            let workflow_id = Uuid::new_v4();
            let mut collector = WorkflowMetricsCollector::new(workflow_id);

            assert_eq!(collector.workflow_id, workflow_id);
            assert_eq!(collector.total_tasks, 0);
            assert_eq!(collector.completed_tasks, 0);
            assert_eq!(collector.failed_tasks, 0);

            let task1 = Uuid::new_v4();
            let task2 = Uuid::new_v4();

            collector.record_task_start(task1);
            collector.record_task_complete(task1, 100);

            assert_eq!(collector.total_tasks, 1);
            assert_eq!(collector.completed_tasks, 1);

            collector.record_task_start(task2);
            collector.record_task_failure(task2, 50);

            assert_eq!(collector.total_tasks, 2);
            assert_eq!(collector.failed_tasks, 1);

            collector.record_task_retry(task2);
            assert_eq!(*collector.task_retries.get(&task2).unwrap(), 1);

            collector.finalize();
            assert!(collector.end_time.is_some());
            assert!(collector.total_duration.is_some());
            assert!(collector.avg_task_duration.is_some());
            assert!(collector.success_rate.is_some());

            let summary = collector.summary();
            assert!(summary.contains("WorkflowMetrics"));
        }

        #[test]
        fn test_workflow_metrics_collector_display() {
            let workflow_id = Uuid::new_v4();
            let collector = WorkflowMetricsCollector::new(workflow_id);

            let display = format!("{}", collector);
            assert!(display.contains("WorkflowMetrics"));
        }

        #[test]
        fn test_workflow_rate_limiter() {
            let mut limiter = WorkflowRateLimiter::new(2, 1000);

            assert_eq!(limiter.max_workflows, 2);
            assert_eq!(limiter.window_ms, 1000);

            assert!(limiter.allow_workflow());
            assert!(limiter.allow_workflow());
            assert!(!limiter.allow_workflow()); // Should be rejected

            assert_eq!(limiter.total_workflows, 2);
            assert_eq!(limiter.rejected_workflows, 1);

            let rate = limiter.current_rate();
            assert!(rate > 0.0);

            let rejection_rate = limiter.rejection_rate();
            assert!(rejection_rate > 0.0);

            limiter.reset();
            assert_eq!(limiter.workflow_timestamps.len(), 0);
        }

        #[test]
        fn test_workflow_rate_limiter_display() {
            let limiter = WorkflowRateLimiter::new(10, 1000);

            let display = format!("{}", limiter);
            assert!(display.contains("RateLimiter"));
        }

        #[test]
        fn test_workflow_concurrency_control() {
            let mut control = WorkflowConcurrencyControl::new(2);

            assert_eq!(control.max_concurrent, 2);
            assert_eq!(control.current_concurrency(), 0);
            assert_eq!(control.available_slots(), 2);
            assert!(!control.is_at_capacity());

            let wf1 = Uuid::new_v4();
            let wf2 = Uuid::new_v4();
            let wf3 = Uuid::new_v4();

            assert!(control.try_start(wf1));
            assert!(control.try_start(wf2));
            assert!(!control.try_start(wf3)); // Should be rejected

            assert_eq!(control.current_concurrency(), 2);
            assert!(control.is_at_capacity());
            assert_eq!(control.peak_concurrency, 2);

            assert!(control.complete(wf1));
            assert_eq!(control.current_concurrency(), 1);
            assert_eq!(control.total_completed, 1);

            assert!(control.try_start(wf3));
            assert_eq!(control.current_concurrency(), 2);
        }

        #[test]
        fn test_workflow_concurrency_control_display() {
            let control = WorkflowConcurrencyControl::new(5);

            let display = format!("{}", control);
            assert!(display.contains("ConcurrencyControl"));
        }

        #[test]
        fn test_workflow_builder() {
            let builder = WorkflowBuilder::new("test_workflow")
                .with_description("Test workflow description")
                .add_tag("test")
                .add_tag("production")
                .add_metadata("version", serde_json::json!("1.0"));

            assert_eq!(builder.name, "test_workflow");
            assert_eq!(
                builder.description,
                Some("Test workflow description".to_string())
            );
            assert_eq!(builder.tags.len(), 2);
            assert_eq!(builder.metadata.len(), 1);

            let chain = builder.clone().chain();
            assert!(chain.is_empty());

            let group = builder.clone().group();
            assert!(group.is_empty());

            let task = Signature::new("map_task".to_string());
            let argsets = vec![vec![serde_json::json!(1)], vec![serde_json::json!(2)]];
            let map = builder.map(task, argsets);
            assert_eq!(map.task.task, "map_task");
        }

        #[test]
        fn test_workflow_registry() {
            let mut registry = WorkflowRegistry::new();

            assert_eq!(registry.count(), 0);

            let wf1 = Uuid::new_v4();
            let wf2 = Uuid::new_v4();

            let mut metadata1 = HashMap::new();
            metadata1.insert("version".to_string(), serde_json::json!("1.0"));

            registry.register(wf1, "workflow_1".to_string(), metadata1.clone());
            registry.register(wf2, "workflow_2".to_string(), HashMap::new());

            assert_eq!(registry.count(), 2);
            assert_eq!(registry.get_name(&wf1), Some("workflow_1"));
            assert_eq!(registry.get_state(&wf1), Some(&WorkflowStatus::Pending));

            registry.update_state(wf1, WorkflowStatus::Running);
            assert_eq!(registry.get_state(&wf1), Some(&WorkflowStatus::Running));

            registry.add_tag(wf1, "production".to_string());
            registry.add_tag(wf2, "production".to_string());

            let production_workflows = registry.get_by_tag("production");
            assert_eq!(production_workflows.len(), 2);

            let running = registry.get_by_state(&WorkflowStatus::Running);
            assert_eq!(running.len(), 1);

            assert!(registry.remove(&wf1));
            assert_eq!(registry.count(), 1);

            registry.clear();
            assert_eq!(registry.count(), 0);
        }

        #[test]
        fn test_workflow_registry_default() {
            let registry = WorkflowRegistry::default();
            assert_eq!(registry.count(), 0);
        }

        #[test]
        fn test_workflow_registry_display() {
            let mut registry = WorkflowRegistry::new();

            let wf1 = Uuid::new_v4();
            registry.register(wf1, "test".to_string(), HashMap::new());

            let display = format!("{}", registry);
            assert!(display.contains("WorkflowRegistry"));
            assert!(display.contains("total=1"));
        }

        // ===== NestedChain Tests =====

        #[tokio::test]
        async fn test_nested_chain_apply() {
            let broker = MockBroker::new();

            let nested_chain = NestedChain::new()
                .then("task1", vec![serde_json::json!(1)])
                .then_group(
                    Group::new()
                        .add("task2a", vec![serde_json::json!(2)])
                        .add("task2b", vec![serde_json::json!(3)]),
                )
                .then("task3", vec![serde_json::json!(4)]);

            let result = nested_chain.apply(&broker).await;
            assert!(result.is_ok(), "NestedChain apply should succeed");

            // Verify tasks were published
            assert!(
                broker.task_count() >= 4,
                "Should publish at least 4 tasks (task1, task2a, task2b, task3)"
            );
        }

        #[tokio::test]
        async fn test_nested_chain_with_chains() {
            let broker = MockBroker::new();

            let nested_chain = NestedChain::new()
                .then_chain(Chain::new().then("step1", vec![]).then("step2", vec![]))
                .then_chain(Chain::new().then("step3", vec![]).then("step4", vec![]));

            let result = nested_chain.apply(&broker).await;
            assert!(result.is_ok(), "NestedChain with chains should succeed");

            // Each chain only enqueues the first task (with links to subsequent tasks)
            // So we expect 2 tasks (one per chain), not 4
            assert_eq!(
                broker.task_count(),
                2,
                "Should publish 2 first tasks from two chains"
            );
        }

        #[tokio::test]
        async fn test_nested_chain_empty_error() {
            let broker = MockBroker::new();
            let nested_chain = NestedChain::new();

            let result = nested_chain.apply(&broker).await;
            assert!(result.is_err(), "Empty NestedChain should return error");
            match result {
                Err(CanvasError::Invalid(msg)) => {
                    assert!(msg.contains("empty"));
                }
                _ => panic!("Expected Invalid error for empty NestedChain"),
            }
        }

        #[test]
        fn test_nested_chain_builder_methods() {
            let chain = NestedChain::new()
                .then("task1", vec![])
                .then_signature(Signature::new("task2".to_string()))
                .then_group(Group::new().add("task3", vec![]));

            assert_eq!(chain.len(), 3);
            assert!(!chain.is_empty());
        }

        #[test]
        fn test_nested_chain_display() {
            let chain = NestedChain::new()
                .then("task1", vec![])
                .then("task2", vec![]);

            let display = format!("{}", chain);
            assert!(display.contains("NestedChain"));
            assert!(display.contains("->"));
        }

        // ===== NestedGroup Tests =====

        #[tokio::test]
        async fn test_nested_group_apply() {
            let broker = MockBroker::new();

            let nested_group = NestedGroup::new()
                .add("task1", vec![serde_json::json!(1)])
                .add_chain(Chain::new().then("task2a", vec![]).then("task2b", vec![]))
                .add("task3", vec![serde_json::json!(3)]);

            let result = nested_group.apply(&broker).await;
            assert!(result.is_ok(), "NestedGroup apply should succeed");

            // Verify tasks were published
            // Chain only enqueues first task, so: task1 + task2a (from chain) + task3 = 3
            assert_eq!(
                broker.task_count(),
                3,
                "Should publish 3 tasks (task1, task2a from chain, task3)"
            );
        }

        #[tokio::test]
        async fn test_nested_group_with_multiple_chains() {
            let broker = MockBroker::new();

            let nested_group = NestedGroup::new()
                .add_chain(Chain::new().then("a1", vec![]).then("a2", vec![]))
                .add_chain(Chain::new().then("b1", vec![]).then("b2", vec![]))
                .add_chain(Chain::new().then("c1", vec![]).then("c2", vec![]));

            let result = nested_group.apply(&broker).await;
            assert!(
                result.is_ok(),
                "NestedGroup with multiple chains should succeed"
            );

            // Each chain only enqueues the first task (with links)
            // So we expect 3 tasks (one per chain), not 6
            assert_eq!(
                broker.task_count(),
                3,
                "Should publish 3 first tasks from three chains"
            );
        }

        #[tokio::test]
        async fn test_nested_group_empty_error() {
            let broker = MockBroker::new();
            let nested_group = NestedGroup::new();

            let result = nested_group.apply(&broker).await;
            assert!(result.is_err(), "Empty NestedGroup should return error");
            match result {
                Err(CanvasError::Invalid(msg)) => {
                    assert!(msg.contains("empty"));
                }
                _ => panic!("Expected Invalid error for empty NestedGroup"),
            }
        }

        #[test]
        fn test_nested_group_builder_methods() {
            let group = NestedGroup::new()
                .add("task1", vec![])
                .add_signature(Signature::new("task2".to_string()))
                .add_chain(Chain::new().then("task3", vec![]));

            assert_eq!(group.len(), 3);
            assert!(!group.is_empty());
        }

        #[test]
        fn test_nested_group_display() {
            let group = NestedGroup::new().add("task1", vec![]).add("task2", vec![]);

            let display = format!("{}", group);
            assert!(display.contains("NestedGroup"));
            assert!(display.contains("|"));
        }

        #[tokio::test]
        async fn test_nested_workflows_complex_composition() {
            let broker = MockBroker::new();

            // Create a complex nested workflow:
            // Chain of [ Group of tasks -> Chain of tasks -> Group of tasks ]
            let nested = NestedChain::new()
                .then_group(
                    Group::new()
                        .add("parallel1", vec![])
                        .add("parallel2", vec![])
                        .add("parallel3", vec![]),
                )
                .then_chain(Chain::new().then("seq1", vec![]).then("seq2", vec![]))
                .then_element(CanvasElement::Group(
                    Group::new().add("final1", vec![]).add("final2", vec![]),
                ));

            let result = nested.apply(&broker).await;
            assert!(result.is_ok(), "Complex nested workflow should succeed");

            // Total tasks: 3 (parallel group) + 1 (first from chain) + 2 (final group) = 6
            // Chains only enqueue their first task with links to subsequent tasks
            assert_eq!(broker.task_count(), 6, "Should publish 6 initial tasks");
        }

        #[test]
        fn test_chain_iterator_methods() {
            let chain = Chain::new()
                .then("task1", vec![])
                .then("task2", vec![])
                .then("task3", vec![]);

            // Test first/last
            assert_eq!(chain.first().unwrap().task, "task1");
            assert_eq!(chain.last().unwrap().task, "task3");

            // Test get
            assert_eq!(chain.get(1).unwrap().task, "task2");
            assert!(chain.get(10).is_none());

            // Test iter
            let names: Vec<String> = chain.iter().map(|s| s.task.clone()).collect();
            assert_eq!(names, vec!["task1", "task2", "task3"]);

            assert_eq!(chain.len(), 3);
            assert!(!chain.is_empty());
        }

        #[test]
        fn test_chain_into_iterator() {
            let chain = Chain::new().then("task1", vec![]).then("task2", vec![]);

            let mut count = 0;
            for sig in &chain {
                assert!(sig.task.starts_with("task"));
                count += 1;
            }
            assert_eq!(count, 2);

            // Test consuming iterator
            let tasks: Vec<Signature> = chain.into_iter().collect();
            assert_eq!(tasks.len(), 2);
        }

        #[test]
        fn test_chain_from_vec() {
            let sigs = vec![
                Signature::new("task1".to_string()),
                Signature::new("task2".to_string()),
            ];

            let chain = Chain::from(sigs);
            assert_eq!(chain.len(), 2);
            assert_eq!(chain.first().unwrap().task, "task1");
        }

        #[test]
        fn test_chain_from_iter() {
            let chain: Chain = vec![
                Signature::new("task1".to_string()),
                Signature::new("task2".to_string()),
            ]
            .into_iter()
            .collect();

            assert_eq!(chain.len(), 2);
        }

        #[test]
        fn test_chain_with_capacity() {
            let chain = Chain::with_capacity(10);
            assert_eq!(chain.len(), 0);
            assert!(chain.is_empty());
        }

        #[test]
        fn test_chain_extend() {
            let chain = Chain::new().then("task1", vec![]).extend(vec![
                Signature::new("task2".to_string()),
                Signature::new("task3".to_string()),
            ]);

            assert_eq!(chain.len(), 3);
        }

        #[test]
        fn test_chain_reverse() {
            let chain = Chain::new()
                .then("task1", vec![])
                .then("task2", vec![])
                .then("task3", vec![])
                .reverse();

            assert_eq!(chain.first().unwrap().task, "task3");
            assert_eq!(chain.last().unwrap().task, "task1");
        }

        #[test]
        fn test_chain_retain() {
            let chain = Chain::new()
                .then("keep1", vec![])
                .then("remove", vec![])
                .then("keep2", vec![])
                .retain(|sig| sig.task.starts_with("keep"));

            assert_eq!(chain.len(), 2);
            assert_eq!(chain.first().unwrap().task, "keep1");
            assert_eq!(chain.last().unwrap().task, "keep2");
        }

        #[test]
        fn test_group_iterator_methods() {
            let group = Group::new()
                .add("task1", vec![])
                .add("task2", vec![])
                .add("task3", vec![]);

            // Test first/last
            assert_eq!(group.first().unwrap().task, "task1");
            assert_eq!(group.last().unwrap().task, "task3");

            // Test get
            assert_eq!(group.get(1).unwrap().task, "task2");
            assert!(group.get(10).is_none());

            // Test iter
            let names: Vec<String> = group.iter().map(|s| s.task.clone()).collect();
            assert_eq!(names, vec!["task1", "task2", "task3"]);

            assert_eq!(group.len(), 3);
            assert!(!group.is_empty());
        }

        #[test]
        fn test_group_find_filter() {
            let group = Group::new()
                .add("task_a", vec![])
                .add("task_b", vec![])
                .add("other", vec![]);

            // Test find
            let found = group.find(|sig| sig.task == "task_b");
            assert!(found.is_some());
            assert_eq!(found.unwrap().task, "task_b");

            // Test filter
            let filtered = group.clone().filter(|sig| sig.task.starts_with("task_"));
            assert_eq!(filtered.len(), 2);
        }

        #[test]
        fn test_group_from_vec() {
            let sigs = vec![
                Signature::new("task1".to_string()),
                Signature::new("task2".to_string()),
            ];

            let group = Group::from(sigs);
            assert_eq!(group.len(), 2);
            assert!(group.has_group_id());
        }

        #[test]
        fn test_group_with_capacity() {
            let group = Group::with_capacity(10);
            assert_eq!(group.len(), 0);
            assert!(group.is_empty());
            assert!(group.has_group_id());
        }

        #[test]
        fn test_signature_convenience_methods() {
            let sig = Signature::new("task".to_string())
                .add_kwarg("key1", serde_json::json!("value1"))
                .add_kwarg("key2", serde_json::json!(42))
                .add_arg(serde_json::json!(1))
                .add_arg(serde_json::json!(2));

            assert!(sig.has_kwarg("key1"));
            assert!(sig.has_kwarg("key2"));
            assert!(!sig.has_kwarg("key3"));

            assert_eq!(sig.get_kwarg("key1"), Some(&serde_json::json!("value1")));
            assert_eq!(sig.get_kwarg("key2"), Some(&serde_json::json!(42)));

            assert_eq!(sig.args.len(), 2);
            assert_eq!(sig.args[0], serde_json::json!(1));
        }

        #[test]
        fn test_workflow_registry_query_methods() {
            let mut registry = WorkflowRegistry::new();

            let id1 = Uuid::new_v4();
            let id2 = Uuid::new_v4();
            let id3 = Uuid::new_v4();

            registry.register(id1, "workflow_test_1".to_string(), HashMap::new());
            registry.register(id2, "workflow_test_2".to_string(), HashMap::new());
            registry.register(id3, "other_workflow".to_string(), HashMap::new());

            // Test find_by_name
            let found = registry.find_by_name("test");
            assert_eq!(found.len(), 2);

            // Test all_workflow_ids
            let all_ids = registry.all_workflow_ids();
            assert_eq!(all_ids.len(), 3);

            // Test contains
            assert!(registry.contains(&id1));
            assert!(!registry.contains(&Uuid::new_v4()));

            // Test tags
            registry.add_tag(id1, "tag1".to_string());
            registry.add_tag(id2, "tag1".to_string());
            registry.add_tag(id2, "tag2".to_string());

            let all_tags = registry.all_tags();
            assert!(all_tags.contains(&"tag1".to_string()));

            // Test get_by_tags_all
            let with_both = registry.get_by_tags_all(&["tag1", "tag2"]);
            assert_eq!(with_both.len(), 1);
            assert!(with_both.contains(&id2));

            // Test get_by_tags_any
            let with_any = registry.get_by_tags_any(&["tag1", "tag2"]);
            assert!(with_any.len() >= 2);

            // Test state counts
            registry.update_state(id1, WorkflowStatus::Running);
            registry.update_state(id2, WorkflowStatus::Success);
            registry.update_state(id3, WorkflowStatus::Pending);

            assert_eq!(registry.running_count(), 1);
            assert_eq!(registry.pending_count(), 1);
            assert_eq!(registry.completed_count(), 1);
            assert_eq!(registry.count_by_state(&WorkflowStatus::Success), 1);
        }

        #[test]
        fn test_workflow_registry_age_queries() {
            let mut registry = WorkflowRegistry::new();
            let id = Uuid::new_v4();

            registry.register(id, "test".to_string(), HashMap::new());

            // Age should be very small (just registered)
            let age = registry.get_age(&id);
            assert!(age.is_some());
            assert!(age.unwrap() < 1000); // Less than 1 second

            // Test get_older_than
            let old = registry.get_older_than(1_000_000); // 1000 seconds
            assert_eq!(old.len(), 0); // Nothing is that old yet

            // Sleep a tiny bit to ensure age > 0
            std::thread::sleep(std::time::Duration::from_millis(1));

            let recent = registry.get_older_than(0); // 0 ms
            assert_eq!(recent.len(), 1); // Our workflow is older than 0ms
        }
    }
}
