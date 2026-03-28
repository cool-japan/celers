use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use uuid::Uuid;

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
