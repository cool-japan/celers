//! Message transformation middleware
//!
//! This module provides a middleware pattern for transforming messages through
//! a pipeline of transformations (validation, signing, encryption, etc.).
//!
//! # Example
//!
//! ```
//! use celers_protocol::middleware::{MessagePipeline, ValidationMiddleware};
//! use celers_protocol::{Message, TaskArgs};
//! use uuid::Uuid;
//!
//! let task_id = Uuid::new_v4();
//! let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
//! let msg = Message::new("tasks.add".to_string(), task_id, body);
//!
//! let mut pipeline = MessagePipeline::new();
//! pipeline.add(Box::new(ValidationMiddleware));
//!
//! let result = pipeline.process(msg);
//! assert!(result.is_ok());
//! ```

use crate::Message;
use std::fmt;

/// Middleware error
#[derive(Debug, Clone)]
pub enum MiddlewareError {
    /// Validation failed
    Validation(String),
    /// Transformation failed
    Transformation(String),
    /// Processing error
    Processing(String),
}

impl fmt::Display for MiddlewareError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            MiddlewareError::Validation(msg) => write!(f, "Validation error: {}", msg),
            MiddlewareError::Transformation(msg) => write!(f, "Transformation error: {}", msg),
            MiddlewareError::Processing(msg) => write!(f, "Processing error: {}", msg),
        }
    }
}

impl From<crate::ValidationError> for MiddlewareError {
    fn from(err: crate::ValidationError) -> Self {
        MiddlewareError::Validation(err.to_string())
    }
}

impl std::error::Error for MiddlewareError {}

/// Middleware trait for message processing
pub trait Middleware: Send + Sync {
    /// Process a message
    fn process(&self, message: Message) -> Result<Message, MiddlewareError>;

    /// Get the name of this middleware
    fn name(&self) -> &'static str;

    /// Check if this middleware should be skipped for a message
    fn should_skip(&self, _message: &Message) -> bool {
        false
    }
}

/// Message processing pipeline
pub struct MessagePipeline {
    middlewares: Vec<Box<dyn Middleware>>,
}

impl MessagePipeline {
    /// Create a new empty pipeline
    pub fn new() -> Self {
        Self {
            middlewares: Vec::new(),
        }
    }

    /// Add a middleware to the pipeline
    pub fn add(&mut self, middleware: Box<dyn Middleware>) {
        self.middlewares.push(middleware);
    }

    /// Process a message through the pipeline
    pub fn process(&self, mut message: Message) -> Result<Message, MiddlewareError> {
        for middleware in &self.middlewares {
            if !middleware.should_skip(&message) {
                message = middleware.process(message)?;
            }
        }
        Ok(message)
    }

    /// Get the number of middlewares in the pipeline
    pub fn len(&self) -> usize {
        self.middlewares.len()
    }

    /// Check if the pipeline is empty
    pub fn is_empty(&self) -> bool {
        self.middlewares.is_empty()
    }

    /// Clear all middlewares
    pub fn clear(&mut self) {
        self.middlewares.clear();
    }
}

impl Default for MessagePipeline {
    fn default() -> Self {
        Self::new()
    }
}

/// Validation middleware
pub struct ValidationMiddleware;

impl Middleware for ValidationMiddleware {
    fn process(&self, message: Message) -> Result<Message, MiddlewareError> {
        message.validate().map_err(MiddlewareError::from)?;
        Ok(message)
    }

    fn name(&self) -> &'static str {
        "validation"
    }
}

/// Size limit middleware
pub struct SizeLimitMiddleware {
    max_size: usize,
}

impl SizeLimitMiddleware {
    /// Create a new size limit middleware
    pub fn new(max_size: usize) -> Self {
        Self { max_size }
    }
}

impl Middleware for SizeLimitMiddleware {
    fn process(&self, message: Message) -> Result<Message, MiddlewareError> {
        if message.body.len() > self.max_size {
            return Err(MiddlewareError::Validation(format!(
                "Message body too large: {} bytes (max {})",
                message.body.len(),
                self.max_size
            )));
        }
        Ok(message)
    }

    fn name(&self) -> &'static str {
        "size_limit"
    }
}

/// Retry count middleware
pub struct RetryLimitMiddleware {
    max_retries: u32,
}

impl RetryLimitMiddleware {
    /// Create a new retry limit middleware
    pub fn new(max_retries: u32) -> Self {
        Self { max_retries }
    }
}

impl Middleware for RetryLimitMiddleware {
    fn process(&self, message: Message) -> Result<Message, MiddlewareError> {
        if let Some(retries) = message.headers.retries {
            if retries > self.max_retries {
                return Err(MiddlewareError::Validation(format!(
                    "Too many retries: {} (max {})",
                    retries, self.max_retries
                )));
            }
        }
        Ok(message)
    }

    fn name(&self) -> &'static str {
        "retry_limit"
    }
}

/// Content type validation middleware
pub struct ContentTypeMiddleware {
    allowed_types: Vec<String>,
}

impl ContentTypeMiddleware {
    /// Create a new content type middleware
    pub fn new(allowed_types: Vec<String>) -> Self {
        Self { allowed_types }
    }

    /// Create middleware that only allows JSON
    pub fn json_only() -> Self {
        Self {
            allowed_types: vec!["application/json".to_string()],
        }
    }
}

impl Middleware for ContentTypeMiddleware {
    fn process(&self, message: Message) -> Result<Message, MiddlewareError> {
        if !self.allowed_types.contains(&message.content_type) {
            return Err(MiddlewareError::Validation(format!(
                "Content type '{}' not allowed. Allowed types: {:?}",
                message.content_type, self.allowed_types
            )));
        }
        Ok(message)
    }

    fn name(&self) -> &'static str {
        "content_type"
    }
}

/// Task name filter middleware
pub struct TaskNameFilterMiddleware {
    allowed_patterns: Vec<String>,
}

impl TaskNameFilterMiddleware {
    /// Create a new task name filter middleware
    pub fn new(allowed_patterns: Vec<String>) -> Self {
        Self { allowed_patterns }
    }

    /// Check if a task name matches any allowed pattern
    fn is_allowed(&self, task_name: &str) -> bool {
        self.allowed_patterns.iter().any(|pattern| {
            if pattern.ends_with('*') {
                task_name.starts_with(&pattern[..pattern.len() - 1])
            } else {
                task_name == pattern
            }
        })
    }
}

impl Middleware for TaskNameFilterMiddleware {
    fn process(&self, message: Message) -> Result<Message, MiddlewareError> {
        if !self.is_allowed(&message.headers.task) {
            return Err(MiddlewareError::Validation(format!(
                "Task '{}' not allowed by filter",
                message.headers.task
            )));
        }
        Ok(message)
    }

    fn name(&self) -> &'static str {
        "task_name_filter"
    }
}

/// Priority enforcement middleware
pub struct PriorityMiddleware {
    default_priority: u8,
    enforce_limits: bool,
}

impl PriorityMiddleware {
    /// Create a new priority middleware
    pub fn new(default_priority: u8, enforce_limits: bool) -> Self {
        Self {
            default_priority,
            enforce_limits,
        }
    }
}

impl Middleware for PriorityMiddleware {
    fn process(&self, mut message: Message) -> Result<Message, MiddlewareError> {
        if message.properties.priority.is_none() {
            message.properties.priority = Some(self.default_priority);
        } else if self.enforce_limits {
            if let Some(priority) = message.properties.priority {
                if priority > 9 {
                    return Err(MiddlewareError::Validation(format!(
                        "Priority {} exceeds maximum of 9",
                        priority
                    )));
                }
            }
        }
        Ok(message)
    }

    fn name(&self) -> &'static str {
        "priority"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::TaskArgs;
    use uuid::Uuid;

    #[test]
    fn test_pipeline_new() {
        let pipeline = MessagePipeline::new();
        assert_eq!(pipeline.len(), 0);
        assert!(pipeline.is_empty());
    }

    #[test]
    fn test_pipeline_add() {
        let mut pipeline = MessagePipeline::new();
        pipeline.add(Box::new(ValidationMiddleware));
        assert_eq!(pipeline.len(), 1);
        assert!(!pipeline.is_empty());
    }

    #[test]
    fn test_pipeline_clear() {
        let mut pipeline = MessagePipeline::new();
        pipeline.add(Box::new(ValidationMiddleware));
        pipeline.clear();
        assert_eq!(pipeline.len(), 0);
    }

    #[test]
    fn test_validation_middleware() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body);

        let middleware = ValidationMiddleware;
        let result = middleware.process(msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_size_limit_middleware_ok() {
        let task_id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let msg = Message::new("tasks.test".to_string(), task_id, body);

        let middleware = SizeLimitMiddleware::new(1000);
        let result = middleware.process(msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_size_limit_middleware_exceeded() {
        let task_id = Uuid::new_v4();
        let body = vec![0u8; 1000];
        let msg = Message::new("tasks.test".to_string(), task_id, body);

        let middleware = SizeLimitMiddleware::new(100);
        let result = middleware.process(msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_retry_limit_middleware_ok() {
        let task_id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let mut msg = Message::new("tasks.test".to_string(), task_id, body);
        msg.headers.retries = Some(5);

        let middleware = RetryLimitMiddleware::new(10);
        let result = middleware.process(msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_retry_limit_middleware_exceeded() {
        let task_id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let mut msg = Message::new("tasks.test".to_string(), task_id, body);
        msg.headers.retries = Some(15);

        let middleware = RetryLimitMiddleware::new(10);
        let result = middleware.process(msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_content_type_middleware_allowed() {
        let task_id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let msg = Message::new("tasks.test".to_string(), task_id, body);

        let middleware = ContentTypeMiddleware::json_only();
        let result = middleware.process(msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_content_type_middleware_blocked() {
        let task_id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let mut msg = Message::new("tasks.test".to_string(), task_id, body);
        msg.content_type = "application/pickle".to_string();

        let middleware = ContentTypeMiddleware::json_only();
        let result = middleware.process(msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_task_name_filter_exact_match() {
        let task_id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let msg = Message::new("tasks.allowed".to_string(), task_id, body);

        let middleware = TaskNameFilterMiddleware::new(vec!["tasks.allowed".to_string()]);
        let result = middleware.process(msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_task_name_filter_wildcard() {
        let task_id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let msg = Message::new("tasks.something.add".to_string(), task_id, body);

        let middleware = TaskNameFilterMiddleware::new(vec!["tasks.*".to_string()]);
        let result = middleware.process(msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_task_name_filter_blocked() {
        let task_id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let msg = Message::new("forbidden.task".to_string(), task_id, body);

        let middleware = TaskNameFilterMiddleware::new(vec!["tasks.*".to_string()]);
        let result = middleware.process(msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_priority_middleware_default() {
        let task_id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let msg = Message::new("tasks.test".to_string(), task_id, body);

        let middleware = PriorityMiddleware::new(5, false);
        let result = middleware.process(msg).unwrap();
        assert_eq!(result.properties.priority, Some(5));
    }

    #[test]
    fn test_priority_middleware_enforce_limits() {
        let task_id = Uuid::new_v4();
        let body = vec![1, 2, 3];
        let msg = Message::new("tasks.test".to_string(), task_id, body).with_priority(15);

        let middleware = PriorityMiddleware::new(5, true);
        let result = middleware.process(msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_pipeline_process() {
        let task_id = Uuid::new_v4();
        let body = serde_json::to_vec(&TaskArgs::new()).unwrap();
        let msg = Message::new("tasks.add".to_string(), task_id, body);

        let mut pipeline = MessagePipeline::new();
        pipeline.add(Box::new(ValidationMiddleware));
        pipeline.add(Box::new(SizeLimitMiddleware::new(10000)));

        let result = pipeline.process(msg);
        assert!(result.is_ok());
    }

    #[test]
    fn test_pipeline_process_failure() {
        let task_id = Uuid::new_v4();
        let body = vec![0u8; 1000];
        let msg = Message::new("tasks.test".to_string(), task_id, body);

        let mut pipeline = MessagePipeline::new();
        pipeline.add(Box::new(ValidationMiddleware));
        pipeline.add(Box::new(SizeLimitMiddleware::new(100)));

        let result = pipeline.process(msg);
        assert!(result.is_err());
    }

    #[test]
    fn test_middleware_error_display() {
        let err = MiddlewareError::Validation("test error".to_string());
        assert_eq!(err.to_string(), "Validation error: test error");

        let err = MiddlewareError::Transformation("transform failed".to_string());
        assert_eq!(err.to_string(), "Transformation error: transform failed");

        let err = MiddlewareError::Processing("process error".to_string());
        assert_eq!(err.to_string(), "Processing error: process error");
    }
}
