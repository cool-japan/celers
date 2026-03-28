//! Message implementation for Celery protocol messages.

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::{
    Message, MessageHeaders, MessageProperties, ValidationError, CONTENT_TYPE_JSON, ENCODING_UTF8,
};

impl Message {
    /// Create a new message with JSON body
    pub fn new(task: String, id: Uuid, body: Vec<u8>) -> Self {
        Self {
            headers: MessageHeaders::new(task, id),
            properties: MessageProperties::default(),
            body,
            content_type: CONTENT_TYPE_JSON.to_string(),
            content_encoding: ENCODING_UTF8.to_string(),
        }
    }

    /// Set priority (0-9)
    #[must_use]
    pub fn with_priority(mut self, priority: u8) -> Self {
        self.properties.priority = Some(priority);
        self
    }

    /// Set parent task ID
    #[must_use]
    pub fn with_parent(mut self, parent_id: Uuid) -> Self {
        self.headers.parent_id = Some(parent_id);
        self
    }

    /// Set root task ID
    #[must_use]
    pub fn with_root(mut self, root_id: Uuid) -> Self {
        self.headers.root_id = Some(root_id);
        self
    }

    /// Set group ID
    #[must_use]
    pub fn with_group(mut self, group: Uuid) -> Self {
        self.headers.group = Some(group);
        self
    }

    /// Set ETA (delayed execution)
    #[must_use]
    pub fn with_eta(mut self, eta: DateTime<Utc>) -> Self {
        self.headers.eta = Some(eta);
        self
    }

    /// Set expiration
    #[must_use]
    pub fn with_expires(mut self, expires: DateTime<Utc>) -> Self {
        self.headers.expires = Some(expires);
        self
    }

    /// Set retry count
    #[must_use]
    pub fn with_retries(mut self, retries: u32) -> Self {
        self.headers.retries = Some(retries);
        self
    }

    /// Set correlation ID (for RPC-style calls)
    #[must_use]
    pub fn with_correlation_id(mut self, correlation_id: String) -> Self {
        self.properties.correlation_id = Some(correlation_id);
        self
    }

    /// Set reply-to queue (for results)
    #[must_use]
    pub fn with_reply_to(mut self, reply_to: String) -> Self {
        self.properties.reply_to = Some(reply_to);
        self
    }

    /// Set delivery mode (1 = non-persistent, 2 = persistent)
    #[must_use]
    pub fn with_delivery_mode(mut self, mode: u8) -> Self {
        self.properties.delivery_mode = mode;
        self
    }

    /// Validate the complete message
    ///
    /// Validates:
    /// - Headers (task name, retries, eta/expires)
    /// - Properties (delivery mode, priority)
    /// - Content type format
    /// - Body size
    pub fn validate(&self) -> Result<(), ValidationError> {
        // Validate headers
        self.headers.validate()?;

        // Validate properties
        self.properties.validate()?;

        // Validate content type
        if self.content_type.is_empty() {
            return Err(ValidationError::EmptyContentType);
        }

        // Validate body
        if self.body.is_empty() {
            return Err(ValidationError::EmptyBody);
        }

        if self.body.len() > 10_485_760 {
            // 10MB limit
            return Err(ValidationError::BodyTooLarge {
                size: self.body.len(),
                max: 10_485_760,
            });
        }

        Ok(())
    }

    /// Validate with custom body size limit
    pub fn validate_with_limit(&self, max_body_bytes: usize) -> Result<(), ValidationError> {
        self.headers.validate()?;
        self.properties.validate()?;

        if self.content_type.is_empty() {
            return Err(ValidationError::EmptyContentType);
        }

        if self.body.is_empty() {
            return Err(ValidationError::EmptyBody);
        }

        if self.body.len() > max_body_bytes {
            return Err(ValidationError::BodyTooLarge {
                size: self.body.len(),
                max: max_body_bytes,
            });
        }

        Ok(())
    }

    /// Check if the message has an ETA (delayed execution)
    #[inline(always)]
    pub fn has_eta(&self) -> bool {
        self.headers.eta.is_some()
    }

    /// Check if the message has an expiration time
    #[inline(always)]
    pub fn has_expires(&self) -> bool {
        self.headers.expires.is_some()
    }

    /// Check if the message is part of a group
    #[inline(always)]
    pub fn has_group(&self) -> bool {
        self.headers.group.is_some()
    }

    /// Check if the message has a parent task
    #[inline(always)]
    pub fn has_parent(&self) -> bool {
        self.headers.parent_id.is_some()
    }

    /// Check if the message has a root task
    #[inline(always)]
    pub fn has_root(&self) -> bool {
        self.headers.root_id.is_some()
    }

    /// Check if the message is persistent
    #[inline(always)]
    pub fn is_persistent(&self) -> bool {
        self.properties.delivery_mode == 2
    }

    /// Get the task ID
    #[inline(always)]
    pub fn task_id(&self) -> uuid::Uuid {
        self.headers.id
    }

    /// Get the task name
    #[inline(always)]
    pub fn task_name(&self) -> &str {
        &self.headers.task
    }

    /// Get the content type as a string slice
    #[inline(always)]
    pub fn content_type_str(&self) -> &str {
        &self.content_type
    }

    /// Get the content encoding as a string slice
    #[inline(always)]
    pub fn content_encoding_str(&self) -> &str {
        &self.content_encoding
    }

    /// Get the message body size in bytes
    #[inline(always)]
    pub fn body_size(&self) -> usize {
        self.body.len()
    }

    /// Check if the message body is empty
    #[inline(always)]
    pub fn has_empty_body(&self) -> bool {
        self.body.is_empty()
    }

    /// Get the retry count (0 if not set)
    #[inline(always)]
    pub fn retry_count(&self) -> u32 {
        self.headers.retries.unwrap_or(0)
    }

    /// Get the priority (None if not set)
    #[inline(always)]
    pub fn priority(&self) -> Option<u8> {
        self.properties.priority
    }

    /// Check if message has a correlation ID
    #[inline(always)]
    pub fn has_correlation_id(&self) -> bool {
        self.properties.correlation_id.is_some()
    }

    /// Get the correlation ID
    #[inline]
    pub fn correlation_id(&self) -> Option<&str> {
        self.properties.correlation_id.as_deref()
    }

    /// Get the reply-to queue
    #[inline]
    pub fn reply_to(&self) -> Option<&str> {
        self.properties.reply_to.as_deref()
    }

    /// Check if this is a workflow message (has parent, root, or group)
    #[inline(always)]
    pub fn is_workflow_message(&self) -> bool {
        self.has_parent() || self.has_root() || self.has_group()
    }

    /// Clone the message with a new task ID
    #[must_use]
    pub fn with_new_id(&self) -> Self {
        let mut cloned = self.clone();
        cloned.headers.id = Uuid::new_v4();
        cloned
    }

    /// Create a builder from this message (for modification)
    ///
    /// Note: This creates a new builder with the message's metadata.
    /// The body (args/kwargs) must be set separately on the builder.
    pub fn to_builder(&self) -> crate::builder::MessageBuilder {
        let mut builder = crate::builder::MessageBuilder::new(&self.headers.task);

        // Set basic properties
        builder = builder.id(self.headers.id);

        // Set optional fields
        if let Some(priority) = self.properties.priority {
            builder = builder.priority(priority);
        }
        if let Some(parent_id) = self.headers.parent_id {
            builder = builder.parent(parent_id);
        }
        if let Some(root_id) = self.headers.root_id {
            builder = builder.root(root_id);
        }
        if let Some(group) = self.headers.group {
            builder = builder.group(group);
        }
        if let Some(eta) = self.headers.eta {
            builder = builder.eta(eta);
        }
        if let Some(expires) = self.headers.expires {
            builder = builder.expires(expires);
        }

        builder
    }

    /// Check if the message is ready for immediate execution (not delayed)
    #[inline]
    pub fn is_ready_for_execution(&self) -> bool {
        match self.headers.eta {
            None => true,
            Some(eta) => chrono::Utc::now() >= eta,
        }
    }

    /// Check if the message has not expired yet
    #[inline]
    pub fn is_not_expired(&self) -> bool {
        match self.headers.expires {
            None => true,
            Some(expires) => chrono::Utc::now() < expires,
        }
    }

    /// Check if the message should be processed (not expired and ready for execution)
    #[inline]
    pub fn should_process(&self) -> bool {
        self.is_ready_for_execution() && self.is_not_expired()
    }

    /// Set ETA to now + duration (builder pattern)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_protocol::Message;
    /// use uuid::Uuid;
    /// use chrono::Duration;
    ///
    /// let msg = Message::new("task".to_string(), Uuid::new_v4(), vec![])
    ///     .with_eta_delay(Duration::minutes(5));
    /// assert!(msg.has_eta());
    /// ```
    #[must_use]
    pub fn with_eta_delay(mut self, delay: chrono::Duration) -> Self {
        self.headers.eta = Some(chrono::Utc::now() + delay);
        self
    }

    /// Set expiration to now + duration (builder pattern)
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_protocol::Message;
    /// use uuid::Uuid;
    /// use chrono::Duration;
    ///
    /// let msg = Message::new("task".to_string(), Uuid::new_v4(), vec![])
    ///     .with_expires_in(Duration::hours(1));
    /// assert!(msg.has_expires());
    /// ```
    #[must_use]
    pub fn with_expires_in(mut self, duration: chrono::Duration) -> Self {
        self.headers.expires = Some(chrono::Utc::now() + duration);
        self
    }

    /// Get the time remaining until ETA (None if no ETA or already past)
    #[inline]
    pub fn time_until_eta(&self) -> Option<chrono::Duration> {
        self.headers.eta.and_then(|eta| {
            let now = chrono::Utc::now();
            if eta > now {
                Some(eta - now)
            } else {
                None
            }
        })
    }

    /// Get the time remaining until expiration (None if no expiration or already expired)
    #[inline]
    pub fn time_until_expiration(&self) -> Option<chrono::Duration> {
        self.headers.expires.and_then(|expires| {
            let now = chrono::Utc::now();
            if expires > now {
                Some(expires - now)
            } else {
                None
            }
        })
    }

    /// Increment the retry count (returns new count)
    pub fn increment_retry(&mut self) -> u32 {
        let new_count = self.headers.retries.unwrap_or(0) + 1;
        self.headers.retries = Some(new_count);
        new_count
    }
}
