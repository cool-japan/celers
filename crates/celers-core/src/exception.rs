//! Exception Handling for Task Execution
//!
//! This module provides comprehensive exception handling capabilities for task execution,
//! including exception classification, custom handlers, traceback preservation, and
//! cross-language exception serialization.
//!
//! # Example
//!
//! ```rust
//! use celers_core::exception::{
//!     TaskException, ExceptionCategory, ExceptionPolicy, ExceptionAction,
//! };
//!
//! // Create an exception with traceback
//! let exception = TaskException::new("ValueError", "Invalid input: negative number")
//!     .with_traceback(vec![
//!         ("process_data".to_string(), "tasks.py".to_string(), 42),
//!         ("validate_input".to_string(), "validators.py".to_string(), 15),
//!     ])
//!     .with_category(ExceptionCategory::Retryable);
//!
//! // Create a policy for handling exceptions
//! let policy = ExceptionPolicy::new()
//!     .ignore_on(&["NotFoundError"])
//!     .retry_on(&["TimeoutError", "ConnectionError"])
//!     .fail_on(&["ValidationError"]);
//!
//! // Determine action based on exception
//! let action = policy.get_action(&exception);
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;

/// Category of an exception that determines retry behavior
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ExceptionCategory {
    /// Exception is retryable (transient errors like network issues)
    Retryable,
    /// Exception is fatal and should not be retried
    Fatal,
    /// Exception result should be ignored (task considered successful)
    Ignorable,
    /// Exception requires manual intervention
    RequiresIntervention,
    /// Unknown category - use default policy
    Unknown,
}

impl Default for ExceptionCategory {
    fn default() -> Self {
        Self::Unknown
    }
}

impl fmt::Display for ExceptionCategory {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retryable => write!(f, "RETRYABLE"),
            Self::Fatal => write!(f, "FATAL"),
            Self::Ignorable => write!(f, "IGNORABLE"),
            Self::RequiresIntervention => write!(f, "REQUIRES_INTERVENTION"),
            Self::Unknown => write!(f, "UNKNOWN"),
        }
    }
}

/// Action to take when an exception occurs
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExceptionAction {
    /// Retry the task
    Retry,
    /// Fail the task and move to DLQ
    Fail,
    /// Ignore the exception and mark task as successful
    Ignore,
    /// Reject the task (don't retry, don't move to DLQ)
    Reject,
    /// Defer to default retry policy
    Default,
}

impl Default for ExceptionAction {
    fn default() -> Self {
        Self::Default
    }
}

impl fmt::Display for ExceptionAction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Retry => write!(f, "RETRY"),
            Self::Fail => write!(f, "FAIL"),
            Self::Ignore => write!(f, "IGNORE"),
            Self::Reject => write!(f, "REJECT"),
            Self::Default => write!(f, "DEFAULT"),
        }
    }
}

/// A single frame in an exception traceback
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TracebackFrame {
    /// Function or method name
    pub function: String,
    /// File path or module name
    pub file: String,
    /// Line number
    pub line: u32,
    /// Optional column number
    pub column: Option<u32>,
    /// Optional local variables (for debugging)
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub locals: HashMap<String, String>,
}

impl TracebackFrame {
    /// Create a new traceback frame
    pub fn new(function: impl Into<String>, file: impl Into<String>, line: u32) -> Self {
        Self {
            function: function.into(),
            file: file.into(),
            line,
            column: None,
            locals: HashMap::new(),
        }
    }

    /// Add column information
    #[must_use]
    pub fn with_column(mut self, column: u32) -> Self {
        self.column = Some(column);
        self
    }

    /// Add a local variable
    #[must_use]
    pub fn with_local(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.locals.insert(name.into(), value.into());
        self
    }
}

impl fmt::Display for TracebackFrame {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "  File \"{}\", line {}", self.file, self.line)?;
        if let Some(col) = self.column {
            write!(f, ", column {col}")?;
        }
        write!(f, ", in {}", self.function)?;
        Ok(())
    }
}

/// A structured exception from task execution
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskException {
    /// Exception type name (e.g., "`ValueError`", "`TimeoutError`")
    pub exc_type: String,
    /// Exception message
    pub exc_message: String,
    /// Full traceback as structured frames
    #[serde(default)]
    pub traceback: Vec<TracebackFrame>,
    /// Raw traceback string (for Python compatibility)
    #[serde(default)]
    pub traceback_str: Option<String>,
    /// Exception category
    #[serde(default)]
    pub category: ExceptionCategory,
    /// Cause exception (for chained exceptions)
    #[serde(default)]
    pub cause: Option<Box<TaskException>>,
    /// Context exception (for exception context)
    #[serde(default)]
    pub context: Option<Box<TaskException>>,
    /// Additional metadata
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub metadata: HashMap<String, serde_json::Value>,
    /// Timestamp when exception occurred (Unix timestamp)
    #[serde(default)]
    pub timestamp: Option<f64>,
    /// Worker hostname where exception occurred
    #[serde(default)]
    pub hostname: Option<String>,
    /// Task ID that raised the exception
    #[serde(default)]
    pub task_id: Option<String>,
}

impl TaskException {
    /// Create a new task exception
    pub fn new(exc_type: impl Into<String>, exc_message: impl Into<String>) -> Self {
        Self {
            exc_type: exc_type.into(),
            exc_message: exc_message.into(),
            traceback: Vec::new(),
            traceback_str: None,
            category: ExceptionCategory::Unknown,
            cause: None,
            context: None,
            metadata: HashMap::new(),
            timestamp: None,
            hostname: None,
            task_id: None,
        }
    }

    /// Create from a Rust error
    pub fn from_error<E: std::error::Error>(error: &E) -> Self {
        let exc_type = std::any::type_name::<E>()
            .rsplit("::")
            .next()
            .unwrap_or("Error")
            .to_string();

        let mut exception = Self::new(exc_type, error.to_string());

        // Capture error chain as cause
        if let Some(source) = error.source() {
            exception.cause = Some(Box::new(Self::from_error_dyn(source)));
        }

        exception
    }

    /// Create from a dynamic error reference
    fn from_error_dyn(error: &dyn std::error::Error) -> Self {
        let exc_type = "Error".to_string();
        let mut exception = Self::new(exc_type, error.to_string());

        if let Some(source) = error.source() {
            exception.cause = Some(Box::new(Self::from_error_dyn(source)));
        }

        exception
    }

    /// Add structured traceback frames
    #[must_use]
    pub fn with_traceback(mut self, frames: Vec<(String, String, u32)>) -> Self {
        self.traceback = frames
            .into_iter()
            .map(|(func, file, line)| TracebackFrame::new(func, file, line))
            .collect();
        self
    }

    /// Add traceback frames
    #[must_use]
    pub fn with_traceback_frames(mut self, frames: Vec<TracebackFrame>) -> Self {
        self.traceback = frames;
        self
    }

    /// Add raw traceback string
    #[must_use]
    pub fn with_traceback_str(mut self, traceback: impl Into<String>) -> Self {
        self.traceback_str = Some(traceback.into());
        self
    }

    /// Set exception category
    #[must_use]
    pub fn with_category(mut self, category: ExceptionCategory) -> Self {
        self.category = category;
        self
    }

    /// Set cause exception
    #[must_use]
    pub fn with_cause(mut self, cause: TaskException) -> Self {
        self.cause = Some(Box::new(cause));
        self
    }

    /// Set context exception
    #[must_use]
    pub fn with_context(mut self, context: TaskException) -> Self {
        self.context = Some(Box::new(context));
        self
    }

    /// Add metadata
    #[must_use]
    pub fn with_metadata(mut self, key: impl Into<String>, value: serde_json::Value) -> Self {
        self.metadata.insert(key.into(), value);
        self
    }

    /// Set timestamp
    #[must_use]
    pub fn with_timestamp(mut self, timestamp: f64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Set timestamp to now
    #[must_use]
    pub fn with_timestamp_now(mut self) -> Self {
        use std::time::{SystemTime, UNIX_EPOCH};
        self.timestamp = Some(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs_f64(),
        );
        self
    }

    /// Set hostname
    #[must_use]
    pub fn with_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.hostname = Some(hostname.into());
        self
    }

    /// Set task ID
    #[must_use]
    pub fn with_task_id(mut self, task_id: impl Into<String>) -> Self {
        self.task_id = Some(task_id.into());
        self
    }

    /// Check if this exception is retryable
    #[inline]
    #[must_use]
    pub const fn is_retryable(&self) -> bool {
        self.category as u8 == ExceptionCategory::Retryable as u8
    }

    /// Check if this exception is fatal
    #[inline]
    #[must_use]
    pub const fn is_fatal(&self) -> bool {
        self.category as u8 == ExceptionCategory::Fatal as u8
    }

    /// Check if this exception should be ignored
    #[inline]
    #[must_use]
    pub const fn is_ignorable(&self) -> bool {
        self.category as u8 == ExceptionCategory::Ignorable as u8
    }

    /// Get the full exception chain as a vector
    #[must_use]
    pub fn exception_chain(&self) -> Vec<&TaskException> {
        let mut chain = vec![self];
        let mut current = self;

        while let Some(cause) = &current.cause {
            chain.push(cause);
            current = cause;
        }

        chain
    }

    /// Format the traceback as a string
    #[must_use]
    pub fn format_traceback(&self) -> String {
        use std::fmt::Write;

        if let Some(ref tb_str) = self.traceback_str {
            return tb_str.clone();
        }

        if self.traceback.is_empty() {
            return String::new();
        }

        let mut result = String::from("Traceback (most recent call last):\n");
        for frame in &self.traceback {
            let _ = writeln!(result, "{frame}");
        }
        let _ = write!(result, "{}: {}", self.exc_type, self.exc_message);
        result
    }

    /// Serialize to JSON for cross-language compatibility
    ///
    /// # Errors
    ///
    /// Returns an error if serialization fails.
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string(self)
    }

    /// Deserialize from JSON
    ///
    /// # Errors
    ///
    /// Returns an error if deserialization fails.
    pub fn from_json(json: &str) -> Result<Self, serde_json::Error> {
        serde_json::from_str(json)
    }

    /// Convert to Celery-compatible format
    #[must_use]
    pub fn to_celery_format(&self) -> serde_json::Value {
        serde_json::json!({
            "exc_type": self.exc_type,
            "exc_message": self.exc_message,
            "exc_module": self.metadata.get("module").cloned().unwrap_or(serde_json::Value::Null),
            "traceback": self.traceback_str.clone().unwrap_or_else(|| self.format_traceback()),
        })
    }
}

impl fmt::Display for TaskException {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.exc_type, self.exc_message)
    }
}

impl std::error::Error for TaskException {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.cause
            .as_ref()
            .map(|e| e.as_ref() as &dyn std::error::Error)
    }
}

/// Policy for handling exceptions
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExceptionPolicy {
    /// Exception types to retry on (by type name pattern)
    #[serde(default)]
    pub retry_on: Vec<String>,
    /// Exception types to ignore (task succeeds despite exception)
    #[serde(default)]
    pub ignore_on: Vec<String>,
    /// Exception types to fail on (no retry, go to DLQ)
    #[serde(default)]
    pub fail_on: Vec<String>,
    /// Exception types to reject (no retry, no DLQ)
    #[serde(default)]
    pub reject_on: Vec<String>,
    /// Default action for unmatched exceptions
    #[serde(default)]
    pub default_action: ExceptionAction,
    /// Whether to preserve full traceback
    #[serde(default = "default_true")]
    pub preserve_traceback: bool,
    /// Maximum traceback depth to preserve
    #[serde(default)]
    pub max_traceback_depth: Option<usize>,
    /// Custom metadata to include in exceptions
    #[serde(default, skip_serializing_if = "HashMap::is_empty")]
    pub include_metadata: HashMap<String, bool>,
}

fn default_true() -> bool {
    true
}

impl Default for ExceptionPolicy {
    fn default() -> Self {
        Self {
            retry_on: Vec::new(),
            ignore_on: Vec::new(),
            fail_on: Vec::new(),
            reject_on: Vec::new(),
            default_action: ExceptionAction::default(),
            preserve_traceback: true, // Default to preserving traceback
            max_traceback_depth: None,
            include_metadata: HashMap::new(),
        }
    }
}

impl ExceptionPolicy {
    /// Create a new exception policy with defaults
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set exception types to retry on
    #[must_use]
    pub fn retry_on(mut self, types: &[&str]) -> Self {
        self.retry_on = types.iter().map(std::string::ToString::to_string).collect();
        self
    }

    /// Add exception type to retry on
    #[must_use]
    pub fn add_retry_on(mut self, exc_type: impl Into<String>) -> Self {
        self.retry_on.push(exc_type.into());
        self
    }

    /// Set exception types to ignore
    #[must_use]
    pub fn ignore_on(mut self, types: &[&str]) -> Self {
        self.ignore_on = types.iter().map(std::string::ToString::to_string).collect();
        self
    }

    /// Add exception type to ignore
    #[must_use]
    pub fn add_ignore_on(mut self, exc_type: impl Into<String>) -> Self {
        self.ignore_on.push(exc_type.into());
        self
    }

    /// Set exception types to fail on
    #[must_use]
    pub fn fail_on(mut self, types: &[&str]) -> Self {
        self.fail_on = types.iter().map(std::string::ToString::to_string).collect();
        self
    }

    /// Add exception type to fail on
    #[must_use]
    pub fn add_fail_on(mut self, exc_type: impl Into<String>) -> Self {
        self.fail_on.push(exc_type.into());
        self
    }

    /// Set exception types to reject
    #[must_use]
    pub fn reject_on(mut self, types: &[&str]) -> Self {
        self.reject_on = types.iter().map(std::string::ToString::to_string).collect();
        self
    }

    /// Add exception type to reject
    #[must_use]
    pub fn add_reject_on(mut self, exc_type: impl Into<String>) -> Self {
        self.reject_on.push(exc_type.into());
        self
    }

    /// Set default action
    #[must_use]
    pub fn with_default_action(mut self, action: ExceptionAction) -> Self {
        self.default_action = action;
        self
    }

    /// Set traceback preservation
    #[must_use]
    pub fn with_traceback(mut self, preserve: bool) -> Self {
        self.preserve_traceback = preserve;
        self
    }

    /// Set maximum traceback depth
    #[must_use]
    pub fn with_max_traceback_depth(mut self, depth: usize) -> Self {
        self.max_traceback_depth = Some(depth);
        self
    }

    /// Get the action to take for an exception
    #[must_use]
    pub fn get_action(&self, exception: &TaskException) -> ExceptionAction {
        let exc_type = &exception.exc_type;

        // Check ignore first (takes precedence)
        if self.matches_pattern(exc_type, &self.ignore_on) {
            return ExceptionAction::Ignore;
        }

        // Check reject
        if self.matches_pattern(exc_type, &self.reject_on) {
            return ExceptionAction::Reject;
        }

        // Check fail
        if self.matches_pattern(exc_type, &self.fail_on) {
            return ExceptionAction::Fail;
        }

        // Check retry
        if self.matches_pattern(exc_type, &self.retry_on) {
            return ExceptionAction::Retry;
        }
        // Use category-based decision if available
        match exception.category {
            ExceptionCategory::Retryable => ExceptionAction::Retry,
            ExceptionCategory::Fatal | ExceptionCategory::RequiresIntervention => {
                ExceptionAction::Fail
            }
            ExceptionCategory::Ignorable => ExceptionAction::Ignore,
            ExceptionCategory::Unknown => self.default_action,
        }
    }

    /// Check if exception type matches any pattern
    #[allow(clippy::unused_self)]
    fn matches_pattern(&self, exc_type: &str, patterns: &[String]) -> bool {
        for pattern in patterns {
            if pattern == exc_type {
                return true;
            }
            // Support contains pattern (*middle*)
            if let Some(rest) = pattern.strip_prefix('*') {
                if let Some(middle) = rest.strip_suffix('*') {
                    if !middle.is_empty() && exc_type.contains(middle) {
                        return true;
                    }
                } else {
                    // Suffix pattern (*suffix)
                    if exc_type.ends_with(rest) {
                        return true;
                    }
                }
            }
            // Prefix pattern (prefix*)
            if let Some(prefix) = pattern.strip_suffix('*') {
                if exc_type.starts_with(prefix) {
                    return true;
                }
            }
        }
        false
    }

    /// Process an exception according to policy
    #[must_use]
    pub fn process_exception(&self, mut exception: TaskException) -> TaskException {
        // Truncate traceback if needed
        if let Some(max_depth) = self.max_traceback_depth {
            if exception.traceback.len() > max_depth {
                exception.traceback.truncate(max_depth);
            }
        }

        // Clear traceback if not preserving
        if !self.preserve_traceback {
            exception.traceback.clear();
            exception.traceback_str = None;
        }

        exception
    }
}

/// Trait for custom exception handlers
pub trait ExceptionHandler: Send + Sync {
    /// Handle an exception and return the action to take
    fn handle(&self, exception: &TaskException) -> ExceptionAction;

    /// Transform an exception (e.g., add metadata, modify traceback)
    fn transform(&self, exception: TaskException) -> TaskException {
        exception
    }

    /// Called before the exception is processed
    fn on_exception(&self, _exception: &TaskException) {}

    /// Get handler name for logging
    fn name(&self) -> &'static str {
        "ExceptionHandler"
    }
}

/// A chain of exception handlers
#[derive(Default)]
pub struct ExceptionHandlerChain {
    handlers: Vec<Box<dyn ExceptionHandler>>,
}

impl ExceptionHandlerChain {
    /// Create a new handler chain
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a handler to the chain
    #[must_use]
    pub fn add_handler<H: ExceptionHandler + 'static>(mut self, handler: H) -> Self {
        self.handlers.push(Box::new(handler));
        self
    }

    /// Handle an exception through the chain
    ///
    /// Returns the first non-Default action, or Default if all handlers return Default
    #[must_use]
    pub fn handle(&self, exception: &TaskException) -> ExceptionAction {
        for handler in &self.handlers {
            handler.on_exception(exception);
            let action = handler.handle(exception);
            if action != ExceptionAction::Default {
                return action;
            }
        }
        ExceptionAction::Default
    }

    /// Transform an exception through all handlers
    #[must_use]
    pub fn transform(&self, mut exception: TaskException) -> TaskException {
        for handler in &self.handlers {
            exception = handler.transform(exception);
        }
        exception
    }
}

/// Built-in handler that logs exceptions
pub struct LoggingExceptionHandler {
    /// Log level for exceptions
    pub log_level: tracing::Level,
}

impl Default for LoggingExceptionHandler {
    fn default() -> Self {
        Self {
            log_level: tracing::Level::ERROR,
        }
    }
}

impl LoggingExceptionHandler {
    /// Create a new logging handler
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Set log level
    #[must_use]
    pub fn with_level(mut self, level: tracing::Level) -> Self {
        self.log_level = level;
        self
    }
}

impl ExceptionHandler for LoggingExceptionHandler {
    fn handle(&self, _exception: &TaskException) -> ExceptionAction {
        ExceptionAction::Default
    }

    fn on_exception(&self, exception: &TaskException) {
        match self.log_level {
            tracing::Level::ERROR => {
                tracing::error!(
                    exc_type = %exception.exc_type,
                    exc_message = %exception.exc_message,
                    task_id = ?exception.task_id,
                    "Task exception occurred"
                );
            }
            tracing::Level::WARN => {
                tracing::warn!(
                    exc_type = %exception.exc_type,
                    exc_message = %exception.exc_message,
                    task_id = ?exception.task_id,
                    "Task exception occurred"
                );
            }
            _ => {
                tracing::info!(
                    exc_type = %exception.exc_type,
                    exc_message = %exception.exc_message,
                    task_id = ?exception.task_id,
                    "Task exception occurred"
                );
            }
        }
    }

    fn name(&self) -> &'static str {
        "LoggingExceptionHandler"
    }
}

/// Built-in handler based on exception policy
pub struct PolicyExceptionHandler {
    policy: ExceptionPolicy,
}

impl PolicyExceptionHandler {
    /// Create a handler from a policy
    #[must_use]
    pub fn new(policy: ExceptionPolicy) -> Self {
        Self { policy }
    }
}

impl ExceptionHandler for PolicyExceptionHandler {
    fn handle(&self, exception: &TaskException) -> ExceptionAction {
        self.policy.get_action(exception)
    }

    fn transform(&self, exception: TaskException) -> TaskException {
        self.policy.process_exception(exception)
    }

    fn name(&self) -> &'static str {
        "PolicyExceptionHandler"
    }
}

/// Common exception types for categorization
pub mod exception_types {
    /// Network-related exceptions (typically retryable)
    pub const NETWORK_EXCEPTIONS: &[&str] = &[
        "ConnectionError",
        "TimeoutError",
        "ConnectionRefused",
        "ConnectionReset",
        "BrokenPipe",
        "NetworkError",
        "SocketError",
        "DNSError",
    ];

    /// Database-related exceptions (often retryable)
    pub const DATABASE_EXCEPTIONS: &[&str] = &[
        "DatabaseError",
        "OperationalError",
        "InterfaceError",
        "ConnectionPoolError",
        "DeadlockError",
        "LockTimeout",
    ];

    /// Validation exceptions (typically not retryable)
    pub const VALIDATION_EXCEPTIONS: &[&str] = &[
        "ValidationError",
        "ValueError",
        "TypeError",
        "InvalidArgument",
        "SchemaError",
    ];

    /// Resource exceptions (may be retryable)
    pub const RESOURCE_EXCEPTIONS: &[&str] = &[
        "ResourceExhausted",
        "QuotaExceeded",
        "RateLimitExceeded",
        "OutOfMemory",
        "DiskFull",
    ];

    /// Authentication exceptions (typically not retryable)
    pub const AUTH_EXCEPTIONS: &[&str] = &[
        "AuthenticationError",
        "AuthorizationError",
        "PermissionDenied",
        "TokenExpired",
        "InvalidCredentials",
    ];
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_exception_creation() {
        let exc = TaskException::new("ValueError", "Invalid input");
        assert_eq!(exc.exc_type, "ValueError");
        assert_eq!(exc.exc_message, "Invalid input");
        assert_eq!(exc.category, ExceptionCategory::Unknown);
    }

    #[test]
    fn test_task_exception_with_traceback() {
        let exc = TaskException::new("RuntimeError", "Something went wrong").with_traceback(vec![
            ("main".to_string(), "app.rs".to_string(), 10),
            ("process".to_string(), "tasks.rs".to_string(), 25),
        ]);

        assert_eq!(exc.traceback.len(), 2);
        assert_eq!(exc.traceback[0].function, "main");
        assert_eq!(exc.traceback[0].file, "app.rs");
        assert_eq!(exc.traceback[0].line, 10);
    }

    #[test]
    fn test_task_exception_with_cause() {
        let cause = TaskException::new("IOError", "File not found");
        let exc = TaskException::new("ProcessingError", "Failed to process file").with_cause(cause);

        assert!(exc.cause.is_some());
        assert_eq!(exc.cause.as_ref().unwrap().exc_type, "IOError");
    }

    #[test]
    fn test_exception_chain() {
        let root = TaskException::new("RootError", "Root cause");
        let middle = TaskException::new("MiddleError", "Middle error").with_cause(root);
        let top = TaskException::new("TopError", "Top level error").with_cause(middle);

        let chain = top.exception_chain();
        assert_eq!(chain.len(), 3);
        assert_eq!(chain[0].exc_type, "TopError");
        assert_eq!(chain[1].exc_type, "MiddleError");
        assert_eq!(chain[2].exc_type, "RootError");
    }

    #[test]
    fn test_exception_category() {
        let retryable = TaskException::new("TimeoutError", "Request timed out")
            .with_category(ExceptionCategory::Retryable);
        let fatal = TaskException::new("ValidationError", "Invalid data")
            .with_category(ExceptionCategory::Fatal);

        assert!(retryable.is_retryable());
        assert!(!retryable.is_fatal());
        assert!(fatal.is_fatal());
        assert!(!fatal.is_retryable());
    }

    #[test]
    fn test_exception_policy_retry_on() {
        let policy = ExceptionPolicy::new().retry_on(&["TimeoutError", "ConnectionError"]);

        let timeout_exc = TaskException::new("TimeoutError", "Timed out");
        let validation_exc = TaskException::new("ValidationError", "Invalid");

        assert_eq!(policy.get_action(&timeout_exc), ExceptionAction::Retry);
        assert_eq!(policy.get_action(&validation_exc), ExceptionAction::Default);
    }

    #[test]
    fn test_exception_policy_ignore_on() {
        let policy = ExceptionPolicy::new().ignore_on(&["NotFoundError"]);

        let not_found = TaskException::new("NotFoundError", "Resource not found");
        assert_eq!(policy.get_action(&not_found), ExceptionAction::Ignore);
    }

    #[test]
    fn test_exception_policy_fail_on() {
        let policy = ExceptionPolicy::new().fail_on(&["ValidationError"]);

        let validation = TaskException::new("ValidationError", "Invalid input");
        assert_eq!(policy.get_action(&validation), ExceptionAction::Fail);
    }

    #[test]
    fn test_exception_policy_pattern_matching() {
        let policy = ExceptionPolicy::new()
            .retry_on(&["*Error"])
            .fail_on(&["Validation*"]);

        let timeout = TaskException::new("TimeoutError", "Timed out");
        let validation = TaskException::new("ValidationFailed", "Invalid");

        // fail_on takes precedence due to order in get_action
        assert_eq!(policy.get_action(&validation), ExceptionAction::Fail);
        assert_eq!(policy.get_action(&timeout), ExceptionAction::Retry);
    }

    #[test]
    fn test_exception_policy_category_fallback() {
        let policy = ExceptionPolicy::new();

        let retryable =
            TaskException::new("CustomError", "Error").with_category(ExceptionCategory::Retryable);
        let fatal =
            TaskException::new("CustomError", "Error").with_category(ExceptionCategory::Fatal);

        assert_eq!(policy.get_action(&retryable), ExceptionAction::Retry);
        assert_eq!(policy.get_action(&fatal), ExceptionAction::Fail);
    }

    #[test]
    fn test_exception_policy_default_action() {
        let policy = ExceptionPolicy::new().with_default_action(ExceptionAction::Retry);

        let unknown = TaskException::new("UnknownError", "Unknown");
        assert_eq!(policy.get_action(&unknown), ExceptionAction::Retry);
    }

    #[test]
    fn test_exception_policy_traceback_truncation() {
        let policy = ExceptionPolicy::new().with_max_traceback_depth(2);

        let exc = TaskException::new("Error", "Error").with_traceback(vec![
            ("f1".to_string(), "a.rs".to_string(), 1),
            ("f2".to_string(), "b.rs".to_string(), 2),
            ("f3".to_string(), "c.rs".to_string(), 3),
            ("f4".to_string(), "d.rs".to_string(), 4),
        ]);

        let processed = policy.process_exception(exc);
        assert_eq!(processed.traceback.len(), 2);
    }

    #[test]
    fn test_exception_policy_no_traceback() {
        let policy = ExceptionPolicy::new().with_traceback(false);

        let exc = TaskException::new("Error", "Error")
            .with_traceback(vec![("f1".to_string(), "a.rs".to_string(), 1)])
            .with_traceback_str("Traceback...");

        let processed = policy.process_exception(exc);
        assert!(processed.traceback.is_empty());
        assert!(processed.traceback_str.is_none());
    }

    #[test]
    fn test_traceback_frame_display() {
        let frame = TracebackFrame::new("process_data", "tasks.py", 42).with_column(10);

        let display = format!("{frame}");
        assert!(display.contains("tasks.py"));
        assert!(display.contains("42"));
        assert!(display.contains("process_data"));
        assert!(display.contains("column 10"));
    }

    #[test]
    fn test_task_exception_serialization() {
        let exc = TaskException::new("TestError", "Test message")
            .with_category(ExceptionCategory::Retryable)
            .with_traceback(vec![("test".to_string(), "test.rs".to_string(), 1)]);

        let json = exc.to_json().unwrap();
        let parsed = TaskException::from_json(&json).unwrap();

        assert_eq!(parsed.exc_type, "TestError");
        assert_eq!(parsed.exc_message, "Test message");
        assert_eq!(parsed.category, ExceptionCategory::Retryable);
        assert_eq!(parsed.traceback.len(), 1);
    }

    #[test]
    fn test_task_exception_celery_format() {
        let exc = TaskException::new("ValueError", "Invalid value")
            .with_traceback_str("Traceback (most recent call last):\n  File \"test.py\"");

        let celery = exc.to_celery_format();
        assert_eq!(celery["exc_type"], "ValueError");
        assert_eq!(celery["exc_message"], "Invalid value");
    }

    #[test]
    fn test_exception_display() {
        let exc = TaskException::new("ValueError", "Invalid input");
        assert_eq!(format!("{exc}"), "ValueError: Invalid input");
    }

    #[test]
    fn test_exception_handler_chain() {
        struct RetryHandler;
        impl ExceptionHandler for RetryHandler {
            fn handle(&self, exc: &TaskException) -> ExceptionAction {
                if exc.exc_type.contains("Timeout") {
                    ExceptionAction::Retry
                } else {
                    ExceptionAction::Default
                }
            }
            fn name(&self) -> &'static str {
                "RetryHandler"
            }
        }

        struct FailHandler;
        impl ExceptionHandler for FailHandler {
            fn handle(&self, exc: &TaskException) -> ExceptionAction {
                if exc.exc_type.contains("Fatal") {
                    ExceptionAction::Fail
                } else {
                    ExceptionAction::Default
                }
            }
            fn name(&self) -> &'static str {
                "FailHandler"
            }
        }

        let chain = ExceptionHandlerChain::new()
            .add_handler(RetryHandler)
            .add_handler(FailHandler);

        let timeout = TaskException::new("TimeoutError", "Timed out");
        let fatal = TaskException::new("FatalError", "Fatal");
        let unknown = TaskException::new("UnknownError", "Unknown");

        assert_eq!(chain.handle(&timeout), ExceptionAction::Retry);
        assert_eq!(chain.handle(&fatal), ExceptionAction::Fail);
        assert_eq!(chain.handle(&unknown), ExceptionAction::Default);
    }

    #[test]
    fn test_policy_exception_handler() {
        let policy = ExceptionPolicy::new()
            .retry_on(&["TimeoutError"])
            .fail_on(&["ValidationError"]);

        let handler = PolicyExceptionHandler::new(policy);

        let timeout = TaskException::new("TimeoutError", "Timed out");
        let validation = TaskException::new("ValidationError", "Invalid");

        assert_eq!(handler.handle(&timeout), ExceptionAction::Retry);
        assert_eq!(handler.handle(&validation), ExceptionAction::Fail);
    }
}
