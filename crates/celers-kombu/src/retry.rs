//! Connection retry policy.

use std::time::Duration;

// =============================================================================
// Connection Retry Policy
// =============================================================================

/// Retry policy for connection attempts
///
/// # Examples
///
/// ```
/// use celers_kombu::RetryPolicy;
/// use std::time::Duration;
///
/// // Default policy
/// let policy = RetryPolicy::new();
/// assert_eq!(policy.max_retries, Some(5));
/// assert!(policy.should_retry(3));
/// assert!(!policy.should_retry(6));
///
/// // Custom policy with exponential backoff
/// let policy = RetryPolicy::new()
///     .with_max_retries(3)
///     .with_initial_delay(Duration::from_millis(100))
///     .with_backoff_multiplier(2.0);
///
/// let delay = policy.delay_for_attempt(0);
/// assert_eq!(delay, Duration::from_millis(100));
///
/// // Infinite retries
/// let policy = RetryPolicy::infinite();
/// assert!(policy.should_retry(1000));
///
/// // No retry
/// let policy = RetryPolicy::no_retry();
/// assert!(!policy.should_retry(0));
/// ```
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (None = infinite)
    pub max_retries: Option<u32>,
    /// Initial delay between retries
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Multiplier for exponential backoff
    pub backoff_multiplier: f64,
    /// Whether to add jitter to delays
    pub jitter: bool,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: Some(5),
            initial_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(30),
            backoff_multiplier: 2.0,
            jitter: true,
        }
    }
}

impl RetryPolicy {
    /// Create a new retry policy
    pub fn new() -> Self {
        Self::default()
    }

    /// Set maximum retries (None = infinite)
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = Some(max_retries);
        self
    }

    /// Set infinite retries
    pub fn with_infinite_retries(mut self) -> Self {
        self.max_retries = None;
        self
    }

    /// Set initial delay
    pub fn with_initial_delay(mut self, delay: Duration) -> Self {
        self.initial_delay = delay;
        self
    }

    /// Set maximum delay
    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay = delay;
        self
    }

    /// Set backoff multiplier
    pub fn with_backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.backoff_multiplier = multiplier;
        self
    }

    /// Enable or disable jitter
    pub fn with_jitter(mut self, jitter: bool) -> Self {
        self.jitter = jitter;
        self
    }

    /// Calculate delay for a specific attempt (0-indexed)
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        let base_delay =
            self.initial_delay.as_millis() as f64 * self.backoff_multiplier.powi(attempt as i32);
        let delay_ms = base_delay.min(self.max_delay.as_millis() as f64) as u64;
        Duration::from_millis(delay_ms)
    }

    /// Check if we should retry after this attempt (0-indexed)
    pub fn should_retry(&self, attempt: u32) -> bool {
        match self.max_retries {
            None => true, // Infinite retries
            Some(max) => attempt < max,
        }
    }

    /// Create policy that never retries
    pub fn no_retry() -> Self {
        Self {
            max_retries: Some(0),
            ..Default::default()
        }
    }

    /// Create policy with infinite retries
    pub fn infinite() -> Self {
        Self {
            max_retries: None,
            ..Default::default()
        }
    }

    /// Create policy with fixed delay
    pub fn fixed_delay(delay: Duration, max_retries: u32) -> Self {
        Self {
            max_retries: Some(max_retries),
            initial_delay: delay,
            max_delay: delay,
            backoff_multiplier: 1.0,
            jitter: false,
        }
    }
}
