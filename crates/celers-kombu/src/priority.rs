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
#[derive(Clone, Default, Serialize, Deserialize)]
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
    /// Signing key for HMAC (if signing is enabled).
    ///
    /// Raw HMAC key material. `MessageOptions` derives `Serialize`/
    /// `Deserialize` for general ergonomics, but this field opts out
    /// (`#[serde(skip)]`) so a config dump, log, or on-the-wire transport
    /// of `MessageOptions` never carries the key -- populate it in code
    /// via [`MessageOptions::with_signing`] instead. It is also redacted
    /// from `Debug` output (see the manual `impl Debug` below) and zeroed
    /// in place when the options are dropped (see `Drop`).
    #[serde(skip)]
    pub signing_key: Option<Vec<u8>>,
    /// Enable message encryption (AES-256-GCM)
    pub encrypt: bool,
    /// Encryption key (32 bytes for AES-256).
    ///
    /// Same handling as `signing_key`: skipped by `Serialize`/
    /// `Deserialize`, redacted from `Debug`, and zeroed on drop.
    #[serde(skip)]
    pub encryption_key: Option<Vec<u8>>,
    /// Compression hint
    pub compress: bool,
}

impl std::fmt::Debug for MessageOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Hand-written so `signing_key`/`encryption_key` -- raw HMAC/
        // AES-256 key material -- never appear in `{:?}` output (panic
        // messages, log lines, error contexts). Presence is still shown
        // (`Some(<redacted>)` vs `None`) since that's useful for
        // debugging and leaks nothing.
        f.debug_struct("MessageOptions")
            .field("priority", &self.priority)
            .field("ttl", &self.ttl)
            .field("expires_at", &self.expires_at)
            .field("delay", &self.delay)
            .field("correlation_id", &self.correlation_id)
            .field("reply_to", &self.reply_to)
            .field("headers", &self.headers)
            .field("sign", &self.sign)
            .field(
                "signing_key",
                &self.signing_key.as_ref().map(|_| "<redacted>"),
            )
            .field("encrypt", &self.encrypt)
            .field(
                "encryption_key",
                &self.encryption_key.as_ref().map(|_| "<redacted>"),
            )
            .field("compress", &self.compress)
            .finish()
    }
}

impl Drop for MessageOptions {
    fn drop(&mut self) {
        self.zeroize_secrets();
    }
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

    /// Overwrite any held key material with zeros in place.
    ///
    /// Called from `Drop` so key bytes don't linger in freed heap memory;
    /// exposed as its own method so the zeroing logic is unit-testable
    /// directly (asserting on memory contents *after* an actual drop
    /// would require reading freed memory, which is undefined behavior).
    fn zeroize_secrets(&mut self) {
        if let Some(key) = self.signing_key.as_mut() {
            for byte in key.iter_mut() {
                *byte = 0;
            }
            // Prevent the compiler from proving these writes are dead
            // (the buffer may be freed immediately after) and eliding
            // them.
            std::hint::black_box(key.as_slice());
        }
        if let Some(key) = self.encryption_key.as_mut() {
            for byte in key.iter_mut() {
                *byte = 0;
            }
            std::hint::black_box(key.as_slice());
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn message_options_serialization_omits_secret_key_bytes() {
        let options = MessageOptions::new()
            .with_signing(vec![0xAA, 0xBB, 0xCC, 0xDD])
            .with_encryption(vec![0x11, 0x22, 0x33, 0x44]);

        let json = serde_json::to_string(&options).expect("MessageOptions must serialize");

        // Neither the field names nor the raw byte arrays should appear.
        assert!(!json.contains("signing_key"));
        assert!(!json.contains("encryption_key"));
        assert!(!json.contains("[170,187,204,221]"));
        assert!(!json.contains("[17,34,51,68]"));

        // Round-tripping through JSON must not reconstruct the keys --
        // they were never written out in the first place.
        let round_tripped: MessageOptions =
            serde_json::from_str(&json).expect("skip-serialized struct must still deserialize");
        assert!(round_tripped.signing_key.is_none());
        assert!(round_tripped.encryption_key.is_none());
        // Non-secret fields must still round-trip normally.
        assert!(round_tripped.sign);
        assert!(round_tripped.encrypt);
    }

    #[test]
    fn message_options_debug_redacts_secret_keys_but_shows_presence() {
        let with_keys = MessageOptions::new()
            .with_signing(vec![1, 2, 3, 4])
            .with_encryption(vec![5, 6, 7, 8]);
        let debug_output = format!("{with_keys:?}");
        assert!(!debug_output.contains("1, 2, 3, 4"));
        assert!(!debug_output.contains("5, 6, 7, 8"));
        assert!(debug_output.contains("redacted"));
        // Presence information (Some vs None) is still visible.
        assert!(debug_output.contains("signing_key: Some"));
        assert!(debug_output.contains("encryption_key: Some"));

        let without_keys = MessageOptions::new();
        let debug_output = format!("{without_keys:?}");
        assert!(debug_output.contains("signing_key: None"));
        assert!(debug_output.contains("encryption_key: None"));
    }

    #[test]
    fn zeroize_secrets_clears_key_material_in_place() {
        let mut options = MessageOptions::new()
            .with_signing(vec![9, 9, 9, 9])
            .with_encryption(vec![7, 7, 7]);

        options.zeroize_secrets();

        assert_eq!(options.signing_key, Some(vec![0, 0, 0, 0]));
        assert_eq!(options.encryption_key, Some(vec![0, 0, 0]));
    }

    #[test]
    fn zeroize_secrets_is_a_no_op_without_keys() {
        // Must not panic when no keys are set.
        let mut options = MessageOptions::new();
        options.zeroize_secrets();
        assert!(options.signing_key.is_none());
        assert!(options.encryption_key.is_none());
    }

    #[test]
    fn message_options_drop_does_not_panic() {
        // Exercises the actual `Drop` impl (as opposed to calling
        // `zeroize_secrets` directly).
        let options = MessageOptions::new()
            .with_signing(vec![1, 2, 3])
            .with_encryption(vec![4, 5, 6]);
        drop(options);
    }
}
