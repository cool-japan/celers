//! Priority, message options, and extended producer types.

use async_trait::async_trait;
use celers_protocol::Message;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

use crate::{Producer, Result};

// =============================================================================
// Message Options
// =============================================================================

/// Priority levels for messages
///
/// # Examples
///
/// ```
/// use celers_kombu::Priority;
///
/// let normal = Priority::Normal;
/// assert_eq!(normal.as_u8(), 5);
/// assert_eq!(normal.to_string(), "normal");
///
/// let high = Priority::High;
/// assert!(high > normal);
/// assert_eq!(high.as_u8(), 7);
///
/// // Convert from numeric value
/// let priority = Priority::from_u8(8);
/// assert_eq!(priority, Priority::High);
///
/// // Default is Normal
/// let default = Priority::default();
/// assert_eq!(default, Priority::Normal);
/// ```
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
)]
pub enum Priority {
    /// Lowest priority (0)
    Lowest = 0,
    /// Low priority (3)
    Low = 3,
    /// Normal priority (5)
    #[default]
    Normal = 5,
    /// High priority (7)
    High = 7,
    /// Highest priority (9)
    Highest = 9,
}

impl std::fmt::Display for Priority {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Priority::Lowest => write!(f, "lowest"),
            Priority::Low => write!(f, "low"),
            Priority::Normal => write!(f, "normal"),
            Priority::High => write!(f, "high"),
            Priority::Highest => write!(f, "highest"),
        }
    }
}

impl Priority {
    /// Convert to numeric value (0-9)
    pub fn as_u8(&self) -> u8 {
        *self as u8
    }

    /// Create from numeric value (clamped to 0-9)
    pub fn from_u8(value: u8) -> Self {
        match value {
            0..=1 => Priority::Lowest,
            2..=4 => Priority::Low,
            5 => Priority::Normal,
            6..=8 => Priority::High,
            _ => Priority::Highest,
        }
    }
}

/// Message-level options
///
/// # Examples
///
/// ```
/// use celers_kombu::{MessageOptions, Priority};
/// use std::time::Duration;
///
/// let options = MessageOptions::new()
///     .with_priority(Priority::High)
///     .with_ttl(Duration::from_secs(3600))
///     .with_correlation_id("req-123".to_string())
///     .with_reply_to("response_queue".to_string());
///
/// assert_eq!(options.priority, Some(Priority::High));
/// assert_eq!(options.ttl, Some(Duration::from_secs(3600)));
/// assert_eq!(options.correlation_id, Some("req-123".to_string()));
///
/// // Check if message should be signed
/// let secure_options = MessageOptions::new()
///     .with_signing(b"secret-key".to_vec());
/// assert!(secure_options.should_sign());
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MessageOptions {
    /// Message priority
    pub priority: Option<Priority>,
    /// Message TTL (time-to-live)
    pub ttl: Option<Duration>,
    /// Message expiration timestamp (absolute)
    pub expires_at: Option<u64>,
    /// Delay before message becomes visible
    pub delay: Option<Duration>,
    /// Correlation ID for request/response patterns
    pub correlation_id: Option<String>,
    /// Reply-to queue for RPC patterns
    pub reply_to: Option<String>,
    /// Custom headers
    pub headers: HashMap<String, String>,
    /// Enable message signing (HMAC)
    pub sign: bool,
    /// Signing key for HMAC (if signing is enabled)
    pub signing_key: Option<Vec<u8>>,
    /// Enable message encryption (AES-256-GCM)
    pub encrypt: bool,
    /// Encryption key (32 bytes for AES-256)
    pub encryption_key: Option<Vec<u8>>,
    /// Compression hint
    pub compress: bool,
}

impl MessageOptions {
    /// Create new message options
    pub fn new() -> Self {
        Self::default()
    }

    /// Set priority
    pub fn with_priority(mut self, priority: Priority) -> Self {
        self.priority = Some(priority);
        self
    }

    /// Set TTL
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Set expiration timestamp (Unix timestamp in seconds)
    pub fn with_expires_at(mut self, timestamp: u64) -> Self {
        self.expires_at = Some(timestamp);
        self
    }

    /// Set delay
    pub fn with_delay(mut self, delay: Duration) -> Self {
        self.delay = Some(delay);
        self
    }

    /// Set correlation ID
    pub fn with_correlation_id(mut self, id: impl Into<String>) -> Self {
        self.correlation_id = Some(id.into());
        self
    }

    /// Set reply-to queue
    pub fn with_reply_to(mut self, queue: impl Into<String>) -> Self {
        self.reply_to = Some(queue.into());
        self
    }

    /// Add a custom header
    pub fn with_header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Enable message signing with HMAC
    pub fn with_signing(mut self, key: Vec<u8>) -> Self {
        self.sign = true;
        self.signing_key = Some(key);
        self
    }

    /// Enable message encryption with AES-256-GCM
    pub fn with_encryption(mut self, key: Vec<u8>) -> Self {
        self.encrypt = true;
        self.encryption_key = Some(key);
        self
    }

    /// Enable compression
    pub fn with_compression(mut self) -> Self {
        self.compress = true;
        self
    }

    /// Check if message has expired (based on expires_at)
    pub fn is_expired(&self, current_timestamp: u64) -> bool {
        self.expires_at.is_some_and(|exp| current_timestamp > exp)
    }

    /// Check if message should be delayed
    pub fn should_delay(&self) -> bool {
        self.delay.is_some()
    }

    /// Check if message should be signed
    pub fn should_sign(&self) -> bool {
        self.sign && self.signing_key.is_some()
    }

    /// Check if message should be encrypted
    pub fn should_encrypt(&self) -> bool {
        self.encrypt && self.encryption_key.is_some()
    }

    /// Check if message should be compressed
    pub fn should_compress(&self) -> bool {
        self.compress
    }
}

// =============================================================================
// Extended Producer Trait
// =============================================================================

/// Extended producer trait with message options support
#[async_trait]
pub trait ExtendedProducer: Producer {
    /// Publish a message with options
    async fn publish_with_options(
        &mut self,
        queue: &str,
        message: Message,
        options: MessageOptions,
    ) -> Result<()>;

    /// Publish a message with routing and options
    async fn publish_with_routing_and_options(
        &mut self,
        exchange: &str,
        routing_key: &str,
        message: Message,
        options: MessageOptions,
    ) -> Result<()>;
}
