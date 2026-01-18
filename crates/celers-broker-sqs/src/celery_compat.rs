//! Celery Protocol Compatibility for SQS
//!
//! This module provides full compatibility with Python Celery's SQS transport (via Kombu).
//!
//! # Features
//!
//! - Celery header to SQS MessageAttribute mapping
//! - Multi-queue priority strategy (SQS doesn't support message-level priority)
//! - Kombu-compatible queue naming
//! - Bidirectional message conversion
//!
//! # Example
//!
//! ```ignore
//! use celers_broker_sqs::celery_compat::{CeleryAttributeMapper, QueueNamingStrategy};
//!
//! // Map Message headers to SQS attributes
//! let mapper = CeleryAttributeMapper::new();
//! let attributes = mapper.serialize_headers(&message)?;
//!
//! // Deserialize SQS attributes back to headers
//! let headers = mapper.deserialize_headers(&sqs_attributes)?;
//! ```

use aws_sdk_sqs::types::MessageAttributeValue;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;
use uuid::Uuid;

/// Celery compatibility errors
#[derive(Error, Debug)]
pub enum CeleryCompatError {
    /// Failed to serialize attribute
    #[error("Failed to serialize attribute '{0}': {1}")]
    SerializationError(String, String),

    /// Failed to deserialize attribute
    #[error("Failed to deserialize attribute '{0}': {1}")]
    DeserializationError(String, String),

    /// Invalid attribute value
    #[error("Invalid attribute value for '{0}': {1}")]
    InvalidValue(String, String),

    /// Missing required attribute
    #[error("Missing required attribute: {0}")]
    MissingAttribute(String),

    /// AWS SDK error
    #[error("AWS SDK error: {0}")]
    AwsSdkError(String),
}

/// Result type for Celery compatibility operations
pub type Result<T> = std::result::Result<T, CeleryCompatError>;

/// Celery attribute names as used in SQS MessageAttributes
pub mod attribute_names {
    /// Task name attribute
    pub const TASK: &str = "celery-task";
    /// Task ID attribute
    pub const ID: &str = "celery-id";
    /// Language attribute (py, rust)
    pub const LANG: &str = "celery-lang";
    /// Root task ID for workflow tracking
    pub const ROOT_ID: &str = "celery-root-id";
    /// Parent task ID for nested tasks
    pub const PARENT_ID: &str = "celery-parent-id";
    /// Group ID for grouped tasks
    pub const GROUP: &str = "celery-group";
    /// Retry count
    pub const RETRIES: &str = "celery-retries";
    /// ETA (Estimated Time of Arrival)
    pub const ETA: &str = "celery-eta";
    /// Expiration time
    pub const EXPIRES: &str = "celery-expires";
    /// Priority (0-9)
    pub const PRIORITY: &str = "celery-priority";
    /// Correlation ID for RPC
    pub const CORRELATION_ID: &str = "celery-correlation-id";
    /// Content type
    pub const CONTENT_TYPE: &str = "celery-content-type";
    /// Content encoding
    pub const CONTENT_ENCODING: &str = "celery-content-encoding";
}

/// Celery headers extracted from SQS MessageAttributes
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CeleryHeaders {
    /// Task name
    pub task: Option<String>,
    /// Task ID
    pub id: Option<Uuid>,
    /// Language (py, rust)
    pub lang: Option<String>,
    /// Root task ID
    pub root_id: Option<Uuid>,
    /// Parent task ID
    pub parent_id: Option<Uuid>,
    /// Group ID
    pub group: Option<Uuid>,
    /// Retry count
    pub retries: Option<u32>,
    /// ETA
    pub eta: Option<DateTime<Utc>>,
    /// Expiration
    pub expires: Option<DateTime<Utc>>,
    /// Priority
    pub priority: Option<u8>,
    /// Correlation ID
    pub correlation_id: Option<String>,
    /// Content type
    pub content_type: Option<String>,
    /// Content encoding
    pub content_encoding: Option<String>,
}

impl CeleryHeaders {
    /// Create new empty headers
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Check if headers have workflow tracking info
    #[inline]
    pub fn has_workflow_info(&self) -> bool {
        self.root_id.is_some() || self.parent_id.is_some() || self.group.is_some()
    }

    /// Check if headers are from Python Celery
    #[inline]
    pub fn is_python_origin(&self) -> bool {
        self.lang.as_deref() == Some("py")
    }

    /// Check if headers are from Rust CeleRS
    #[inline]
    pub fn is_rust_origin(&self) -> bool {
        self.lang.as_deref() == Some("rust")
    }
}

/// Maps Celery headers to/from SQS MessageAttributes
#[derive(Debug, Clone, Default)]
pub struct CeleryAttributeMapper {
    /// Include optional attributes even if None
    pub include_empty: bool,
}

impl CeleryAttributeMapper {
    /// Create a new mapper
    #[inline]
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Create mapper that includes empty attributes
    #[inline]
    #[must_use]
    pub fn with_empty_attributes(mut self) -> Self {
        self.include_empty = true;
        self
    }

    /// Serialize a Message to SQS MessageAttributes
    ///
    /// Converts Celery protocol headers and properties to SQS MessageAttributes
    /// for Python Celery compatibility.
    pub fn serialize_message(
        &self,
        message: &celers_protocol::Message,
    ) -> Result<HashMap<String, MessageAttributeValue>> {
        let mut attrs = HashMap::new();

        // Required: Task name
        attrs.insert(
            attribute_names::TASK.to_string(),
            self.build_string_attribute(&message.headers.task)?,
        );

        // Required: Task ID
        attrs.insert(
            attribute_names::ID.to_string(),
            self.build_string_attribute(&message.headers.id.to_string())?,
        );

        // Required: Language
        attrs.insert(
            attribute_names::LANG.to_string(),
            self.build_string_attribute(&message.headers.lang)?,
        );

        // Optional: Root ID
        if let Some(ref root_id) = message.headers.root_id {
            attrs.insert(
                attribute_names::ROOT_ID.to_string(),
                self.build_string_attribute(&root_id.to_string())?,
            );
        }

        // Optional: Parent ID
        if let Some(ref parent_id) = message.headers.parent_id {
            attrs.insert(
                attribute_names::PARENT_ID.to_string(),
                self.build_string_attribute(&parent_id.to_string())?,
            );
        }

        // Optional: Group ID
        if let Some(ref group) = message.headers.group {
            attrs.insert(
                attribute_names::GROUP.to_string(),
                self.build_string_attribute(&group.to_string())?,
            );
        }

        // Optional: Retries
        if let Some(retries) = message.headers.retries {
            attrs.insert(
                attribute_names::RETRIES.to_string(),
                self.build_number_attribute(retries)?,
            );
        }

        // Optional: ETA
        if let Some(ref eta) = message.headers.eta {
            attrs.insert(
                attribute_names::ETA.to_string(),
                self.build_string_attribute(&eta.to_rfc3339())?,
            );
        }

        // Optional: Expires
        if let Some(ref expires) = message.headers.expires {
            attrs.insert(
                attribute_names::EXPIRES.to_string(),
                self.build_string_attribute(&expires.to_rfc3339())?,
            );
        }

        // Optional: Priority (from properties)
        if let Some(priority) = message.properties.priority {
            attrs.insert(
                attribute_names::PRIORITY.to_string(),
                self.build_number_attribute(priority)?,
            );
        }

        // Optional: Correlation ID
        if let Some(ref correlation_id) = message.properties.correlation_id {
            attrs.insert(
                attribute_names::CORRELATION_ID.to_string(),
                self.build_string_attribute(correlation_id)?,
            );
        }

        // Content type and encoding
        attrs.insert(
            attribute_names::CONTENT_TYPE.to_string(),
            self.build_string_attribute(&message.content_type)?,
        );

        attrs.insert(
            attribute_names::CONTENT_ENCODING.to_string(),
            self.build_string_attribute(&message.content_encoding)?,
        );

        Ok(attrs)
    }

    /// Deserialize SQS MessageAttributes to CeleryHeaders
    ///
    /// Converts SQS MessageAttributes back to Celery protocol headers.
    pub fn deserialize_attributes(
        &self,
        attrs: &HashMap<String, aws_sdk_sqs::types::MessageAttributeValue>,
    ) -> Result<CeleryHeaders> {
        let mut headers = CeleryHeaders::new();

        // Task name
        if let Some(attr) = attrs.get(attribute_names::TASK) {
            headers.task = attr.string_value().map(|s| s.to_string());
        }

        // Task ID
        if let Some(attr) = attrs.get(attribute_names::ID) {
            if let Some(id_str) = attr.string_value() {
                headers.id = Uuid::parse_str(id_str).ok();
            }
        }

        // Language
        if let Some(attr) = attrs.get(attribute_names::LANG) {
            headers.lang = attr.string_value().map(|s| s.to_string());
        }

        // Root ID
        if let Some(attr) = attrs.get(attribute_names::ROOT_ID) {
            if let Some(id_str) = attr.string_value() {
                headers.root_id = Uuid::parse_str(id_str).ok();
            }
        }

        // Parent ID
        if let Some(attr) = attrs.get(attribute_names::PARENT_ID) {
            if let Some(id_str) = attr.string_value() {
                headers.parent_id = Uuid::parse_str(id_str).ok();
            }
        }

        // Group ID
        if let Some(attr) = attrs.get(attribute_names::GROUP) {
            if let Some(id_str) = attr.string_value() {
                headers.group = Uuid::parse_str(id_str).ok();
            }
        }

        // Retries
        if let Some(attr) = attrs.get(attribute_names::RETRIES) {
            if let Some(val_str) = attr.string_value() {
                headers.retries = val_str.parse().ok();
            }
        }

        // ETA
        if let Some(attr) = attrs.get(attribute_names::ETA) {
            if let Some(eta_str) = attr.string_value() {
                headers.eta = DateTime::parse_from_rfc3339(eta_str)
                    .ok()
                    .map(|dt| dt.with_timezone(&Utc));
            }
        }

        // Expires
        if let Some(attr) = attrs.get(attribute_names::EXPIRES) {
            if let Some(exp_str) = attr.string_value() {
                headers.expires = DateTime::parse_from_rfc3339(exp_str)
                    .ok()
                    .map(|dt| dt.with_timezone(&Utc));
            }
        }

        // Priority
        if let Some(attr) = attrs.get(attribute_names::PRIORITY) {
            if let Some(val_str) = attr.string_value() {
                headers.priority = val_str.parse().ok();
            }
        }

        // Correlation ID
        if let Some(attr) = attrs.get(attribute_names::CORRELATION_ID) {
            headers.correlation_id = attr.string_value().map(|s| s.to_string());
        }

        // Content type
        if let Some(attr) = attrs.get(attribute_names::CONTENT_TYPE) {
            headers.content_type = attr.string_value().map(|s| s.to_string());
        }

        // Content encoding
        if let Some(attr) = attrs.get(attribute_names::CONTENT_ENCODING) {
            headers.content_encoding = attr.string_value().map(|s| s.to_string());
        }

        Ok(headers)
    }

    /// Build a String type MessageAttributeValue
    fn build_string_attribute(&self, value: &str) -> Result<MessageAttributeValue> {
        MessageAttributeValue::builder()
            .data_type("String")
            .string_value(value)
            .build()
            .map_err(|e| CeleryCompatError::AwsSdkError(e.to_string()))
    }

    /// Build a Number type MessageAttributeValue
    fn build_number_attribute<T: ToString>(&self, value: T) -> Result<MessageAttributeValue> {
        MessageAttributeValue::builder()
            .data_type("Number")
            .string_value(value.to_string())
            .build()
            .map_err(|e| CeleryCompatError::AwsSdkError(e.to_string()))
    }
}

/// Queue naming strategy for Kombu compatibility
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum QueueNamingStrategy {
    /// Kombu-compatible naming: {prefix}_{queue_name}
    /// Dots are replaced with hyphens
    Kombu {
        /// Queue name prefix
        prefix: String,
    },

    /// Custom naming with template
    /// Supports: {queue}, {priority}
    Custom {
        /// Template string
        template: String,
    },

    /// Direct naming (no transformation)
    #[default]
    Direct,
}

impl QueueNamingStrategy {
    /// Create Kombu-compatible naming strategy
    #[inline]
    #[must_use]
    pub fn kombu(prefix: impl Into<String>) -> Self {
        Self::Kombu {
            prefix: prefix.into(),
        }
    }

    /// Create custom naming strategy
    #[inline]
    #[must_use]
    pub fn custom(template: impl Into<String>) -> Self {
        Self::Custom {
            template: template.into(),
        }
    }

    /// Apply naming strategy to a queue name
    pub fn apply(&self, queue_name: &str) -> String {
        match self {
            Self::Kombu { prefix } => {
                let sanitized = queue_name.replace('.', "-");
                if prefix.is_empty() {
                    sanitized
                } else {
                    format!("{}_{}", prefix, sanitized)
                }
            }
            Self::Custom { template } => template.replace("{queue}", queue_name),
            Self::Direct => queue_name.to_string(),
        }
    }

    /// Apply naming strategy with priority suffix
    pub fn apply_with_priority(&self, queue_name: &str, priority: u8) -> String {
        let base = self.apply(queue_name);
        if priority == 0 {
            base
        } else {
            format!("{}-priority-{}", base, priority)
        }
    }
}

/// Priority levels for multi-queue strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PriorityLevel {
    /// Low priority (0-2)
    Low,
    /// Normal priority (3-5)
    Normal,
    /// High priority (6-8)
    High,
    /// Critical priority (9)
    Critical,
}

impl PriorityLevel {
    /// Get the numeric value for this priority level
    #[inline]
    pub fn value(&self) -> u8 {
        match self {
            Self::Low => 0,
            Self::Normal => 5,
            Self::High => 7,
            Self::Critical => 9,
        }
    }

    /// Get priority level from numeric value
    #[inline]
    pub fn from_value(value: u8) -> Self {
        match value {
            0..=2 => Self::Low,
            3..=5 => Self::Normal,
            6..=8 => Self::High,
            9.. => Self::Critical,
        }
    }

    /// Get all priority levels
    #[inline]
    pub fn all() -> &'static [PriorityLevel] {
        &[
            PriorityLevel::Low,
            PriorityLevel::Normal,
            PriorityLevel::High,
            PriorityLevel::Critical,
        ]
    }
}

/// Multi-queue priority manager
///
/// SQS doesn't support message-level priority, so we use separate queues
/// for different priority levels (matching Kombu pattern).
#[derive(Debug, Clone)]
pub struct PriorityQueueManager {
    /// Base queue name
    pub base_queue: String,
    /// Queue naming strategy
    pub naming_strategy: QueueNamingStrategy,
    /// Priority levels to use (default: all)
    pub priority_levels: Vec<PriorityLevel>,
}

impl PriorityQueueManager {
    /// Create a new priority queue manager
    #[inline]
    #[must_use]
    pub fn new(base_queue: impl Into<String>) -> Self {
        Self {
            base_queue: base_queue.into(),
            naming_strategy: QueueNamingStrategy::default(),
            priority_levels: PriorityLevel::all().to_vec(),
        }
    }

    /// Set the naming strategy
    #[inline]
    #[must_use]
    pub fn with_naming_strategy(mut self, strategy: QueueNamingStrategy) -> Self {
        self.naming_strategy = strategy;
        self
    }

    /// Set custom priority levels
    #[inline]
    #[must_use]
    pub fn with_priority_levels(mut self, levels: Vec<PriorityLevel>) -> Self {
        self.priority_levels = levels;
        self
    }

    /// Get the queue name for a given priority
    pub fn get_queue_for_priority(&self, priority: u8) -> String {
        let level = PriorityLevel::from_value(priority);
        self.naming_strategy
            .apply_with_priority(&self.base_queue, level.value())
    }

    /// Get all queue names (for consumption)
    pub fn get_all_queues(&self) -> Vec<String> {
        self.priority_levels
            .iter()
            .map(|level| {
                self.naming_strategy
                    .apply_with_priority(&self.base_queue, level.value())
            })
            .collect()
    }

    /// Get queue names in priority order (highest first)
    pub fn get_queues_by_priority(&self) -> Vec<String> {
        let mut levels = self.priority_levels.clone();
        levels.sort_by_key(|b| std::cmp::Reverse(b.value()));
        levels
            .iter()
            .map(|level| {
                self.naming_strategy
                    .apply_with_priority(&self.base_queue, level.value())
            })
            .collect()
    }
}

/// Celery SQS configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CelerySqsConfig {
    /// Queue naming strategy
    pub naming_strategy: QueueNamingStrategy,
    /// Enable priority queue support
    pub enable_priority_queues: bool,
    /// Priority levels to use
    pub priority_levels: Vec<u8>,
    /// Default visibility timeout (seconds)
    pub visibility_timeout: u32,
    /// Auto-extend visibility before timeout
    pub auto_extend_visibility: bool,
    /// Extension threshold (seconds before timeout)
    pub extend_threshold: u32,
}

impl Default for CelerySqsConfig {
    fn default() -> Self {
        Self {
            naming_strategy: QueueNamingStrategy::default(),
            enable_priority_queues: false,
            priority_levels: vec![0, 3, 6, 9],
            visibility_timeout: 1800, // 30 minutes
            auto_extend_visibility: false,
            extend_threshold: 300, // 5 minutes before timeout
        }
    }
}

impl CelerySqsConfig {
    /// Create with Kombu-compatible settings
    #[must_use]
    pub fn kombu_compatible(prefix: impl Into<String>) -> Self {
        Self {
            naming_strategy: QueueNamingStrategy::kombu(prefix),
            enable_priority_queues: true,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_queue_naming_direct() {
        let strategy = QueueNamingStrategy::Direct;
        assert_eq!(strategy.apply("my-queue"), "my-queue");
        assert_eq!(strategy.apply("my.queue.name"), "my.queue.name");
    }

    #[test]
    fn test_queue_naming_kombu() {
        let strategy = QueueNamingStrategy::kombu("celery");
        assert_eq!(strategy.apply("tasks"), "celery_tasks");
        assert_eq!(strategy.apply("tasks.high"), "celery_tasks-high");
    }

    #[test]
    fn test_queue_naming_with_priority() {
        let strategy = QueueNamingStrategy::Direct;
        assert_eq!(strategy.apply_with_priority("queue", 0), "queue");
        assert_eq!(strategy.apply_with_priority("queue", 5), "queue-priority-5");
        assert_eq!(strategy.apply_with_priority("queue", 9), "queue-priority-9");
    }

    #[test]
    fn test_priority_level_from_value() {
        assert_eq!(PriorityLevel::from_value(0), PriorityLevel::Low);
        assert_eq!(PriorityLevel::from_value(2), PriorityLevel::Low);
        assert_eq!(PriorityLevel::from_value(3), PriorityLevel::Normal);
        assert_eq!(PriorityLevel::from_value(5), PriorityLevel::Normal);
        assert_eq!(PriorityLevel::from_value(6), PriorityLevel::High);
        assert_eq!(PriorityLevel::from_value(8), PriorityLevel::High);
        assert_eq!(PriorityLevel::from_value(9), PriorityLevel::Critical);
    }

    #[test]
    fn test_priority_queue_manager() {
        let manager =
            PriorityQueueManager::new("celery").with_naming_strategy(QueueNamingStrategy::Direct);

        assert_eq!(manager.get_queue_for_priority(0), "celery");
        assert_eq!(manager.get_queue_for_priority(5), "celery-priority-5");
        assert_eq!(manager.get_queue_for_priority(9), "celery-priority-9");

        let all_queues = manager.get_all_queues();
        assert_eq!(all_queues.len(), 4);
    }

    #[test]
    fn test_celery_headers_origin() {
        let mut headers = CeleryHeaders::new();
        assert!(!headers.is_python_origin());
        assert!(!headers.is_rust_origin());

        headers.lang = Some("py".to_string());
        assert!(headers.is_python_origin());

        headers.lang = Some("rust".to_string());
        assert!(headers.is_rust_origin());
    }

    #[test]
    fn test_celery_headers_workflow_info() {
        let mut headers = CeleryHeaders::new();
        assert!(!headers.has_workflow_info());

        headers.root_id = Some(Uuid::new_v4());
        assert!(headers.has_workflow_info());
    }

    #[test]
    fn test_celery_sqs_config_default() {
        let config = CelerySqsConfig::default();
        assert_eq!(config.visibility_timeout, 1800);
        assert!(!config.enable_priority_queues);
    }

    #[test]
    fn test_celery_sqs_config_kombu() {
        let config = CelerySqsConfig::kombu_compatible("celery");
        assert!(config.enable_priority_queues);
        match config.naming_strategy {
            QueueNamingStrategy::Kombu { prefix } => assert_eq!(prefix, "celery"),
            _ => panic!("Expected Kombu naming strategy"),
        }
    }
}
