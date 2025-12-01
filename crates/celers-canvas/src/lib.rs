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

        let countdown = if eta > now { eta - now } else { 0 };

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

/// Canvas errors
#[derive(Debug, thiserror::Error)]
pub enum CanvasError {
    #[error("Invalid workflow: {0}")]
    Invalid(String),

    #[error("Broker error: {0}")]
    Broker(String),

    #[error("Serialization error: {0}")]
    Serialization(String),
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

    /// Check if this error is retryable
    ///
    /// Broker errors are typically retryable (transient network issues).
    /// Invalid workflow and serialization errors are not retryable.
    pub fn is_retryable(&self) -> bool {
        matches!(self, CanvasError::Broker(_))
    }

    /// Get the error category as a string
    pub fn category(&self) -> &'static str {
        match self {
            CanvasError::Invalid(_) => "invalid",
            CanvasError::Broker(_) => "broker",
            CanvasError::Serialization(_) => "serialization",
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
    }

    #[test]
    fn test_canvas_error_display() {
        let err = CanvasError::Invalid("empty chain".to_string());
        assert_eq!(err.to_string(), "Invalid workflow: empty chain");

        let err = CanvasError::Broker("timeout".to_string());
        assert_eq!(err.to_string(), "Broker error: timeout");

        let err = CanvasError::Serialization("malformed json".to_string());
        assert_eq!(err.to_string(), "Serialization error: malformed json");
    }
}
