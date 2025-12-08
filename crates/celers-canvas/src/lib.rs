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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Group: Parallel execution
///
/// (task1 | task2 | task3)
#[derive(Debug, Clone, Serialize, Deserialize)]
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

/// Chord: Parallel execution with callback
///
/// (task1 | task2 | task3) -> callback([result1, result2, result3])
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
#[derive(Debug, Clone, Serialize, Deserialize)]
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
}

/// Trait for exporting workflow as DAG
pub trait DagExport {
    /// Export workflow as GraphViz DOT
    fn to_dot(&self) -> String;

    /// Export workflow as Mermaid diagram
    fn to_mermaid(&self) -> String;

    /// Export workflow as JSON
    fn to_json(&self) -> Result<String, serde_json::Error>;
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

    /// Optimize a chain (placeholder - actual implementation would analyze and transform)
    pub fn optimize_chain(&self, chain: &Chain) -> Chain {
        // Placeholder: In a real implementation, this would analyze the chain
        // and apply optimization passes
        chain.clone()
    }

    /// Optimize a group (placeholder - actual implementation would analyze and transform)
    pub fn optimize_group(&self, group: &Group) -> Group {
        // Placeholder: In a real implementation, this would analyze the group
        // and apply optimization passes
        group.clone()
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

        // Test optimization (placeholder)
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
}
