//! Quota management types.

use async_trait::async_trait;

use crate::Result;

// =============================================================================
// Quota Management (Resource Limits)
// =============================================================================

/// Quota configuration
///
/// # Examples
///
/// ```
/// use celers_kombu::QuotaConfig;
/// use std::time::Duration;
///
/// let quota = QuotaConfig::new()
///     .with_max_messages(10000)
///     .with_max_bytes(10 * 1024 * 1024)
///     .with_max_rate(100.0);
///
/// assert_eq!(quota.max_messages, Some(10000));
/// assert_eq!(quota.max_bytes, Some(10 * 1024 * 1024));
/// assert_eq!(quota.max_rate_per_sec, Some(100.0));
/// ```
#[derive(Debug, Clone)]
pub struct QuotaConfig {
    /// Maximum number of messages
    pub max_messages: Option<usize>,
    /// Maximum total bytes
    pub max_bytes: Option<usize>,
    /// Maximum rate (messages per second)
    pub max_rate_per_sec: Option<f64>,
    /// Per-consumer message limit
    pub max_messages_per_consumer: Option<usize>,
    /// Quota enforcement action
    pub enforcement: QuotaEnforcement,
}

impl QuotaConfig {
    /// Create new quota configuration
    pub fn new() -> Self {
        Self {
            max_messages: None,
            max_bytes: None,
            max_rate_per_sec: None,
            max_messages_per_consumer: None,
            enforcement: QuotaEnforcement::Reject,
        }
    }

    /// Set maximum messages
    pub fn with_max_messages(mut self, max: usize) -> Self {
        self.max_messages = Some(max);
        self
    }

    /// Set maximum bytes
    pub fn with_max_bytes(mut self, max: usize) -> Self {
        self.max_bytes = Some(max);
        self
    }

    /// Set maximum rate
    pub fn with_max_rate(mut self, rate: f64) -> Self {
        self.max_rate_per_sec = Some(rate);
        self
    }

    /// Set per-consumer limit
    pub fn with_max_per_consumer(mut self, max: usize) -> Self {
        self.max_messages_per_consumer = Some(max);
        self
    }

    /// Set enforcement action
    pub fn with_enforcement(mut self, enforcement: QuotaEnforcement) -> Self {
        self.enforcement = enforcement;
        self
    }
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self::new()
    }
}

/// Quota enforcement action
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QuotaEnforcement {
    /// Reject new messages when quota exceeded
    Reject,
    /// Throttle (slow down) when quota exceeded
    Throttle,
    /// Warn but allow (soft limit)
    Warn,
}

/// Quota usage statistics
#[derive(Debug, Clone, Default)]
pub struct QuotaUsage {
    /// Current message count
    pub message_count: usize,
    /// Current bytes used
    pub bytes_used: usize,
    /// Current rate (messages/sec)
    pub current_rate: f64,
    /// Quota exceeded flag
    pub exceeded: bool,
}

impl QuotaUsage {
    /// Check if message quota is exceeded
    pub fn is_message_quota_exceeded(&self, config: &QuotaConfig) -> bool {
        if let Some(max) = config.max_messages {
            return self.message_count >= max;
        }
        false
    }

    /// Check if bytes quota is exceeded
    pub fn is_bytes_quota_exceeded(&self, config: &QuotaConfig) -> bool {
        if let Some(max) = config.max_bytes {
            return self.bytes_used >= max;
        }
        false
    }

    /// Check if rate quota is exceeded
    pub fn is_rate_quota_exceeded(&self, config: &QuotaConfig) -> bool {
        if let Some(max) = config.max_rate_per_sec {
            return self.current_rate >= max;
        }
        false
    }

    /// Get usage percentage
    pub fn usage_percent(&self, config: &QuotaConfig) -> Option<f64> {
        config.max_messages.map(|max| {
            if max == 0 {
                100.0
            } else {
                (self.message_count as f64 / max as f64) * 100.0
            }
        })
    }
}

/// Quota management trait
#[async_trait]
pub trait QuotaManager: Send + Sync {
    /// Set quota for a queue
    async fn set_quota(&mut self, queue: &str, config: QuotaConfig) -> Result<()>;

    /// Get quota configuration
    async fn get_quota(&mut self, queue: &str) -> Result<QuotaConfig>;

    /// Get quota usage
    async fn quota_usage(&mut self, queue: &str) -> Result<QuotaUsage>;

    /// Reset quota counters
    async fn reset_quota(&mut self, queue: &str) -> Result<()>;

    /// Check if operation is allowed under quota
    async fn check_quota(&mut self, queue: &str, message_size: usize) -> Result<bool>;
}
