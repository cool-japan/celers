use std::time::Duration;

/// Retry strategy configuration
#[derive(Debug, Clone)]
pub struct RetryStrategy {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Initial delay between retries
    pub initial_delay: Duration,
    /// Maximum delay between retries
    pub max_delay: Duration,
    /// Backoff multiplier
    pub backoff_multiplier: f64,
    /// Whether to add jitter
    pub use_jitter: bool,
}

impl RetryStrategy {
    /// Creates a new retry strategy with exponential backoff
    pub fn exponential_backoff(max_retries: u32, initial_delay: Duration) -> Self {
        Self {
            max_retries,
            initial_delay,
            max_delay: Duration::from_secs(300), // 5 minutes
            backoff_multiplier: 2.0,
            use_jitter: true,
        }
    }

    /// Creates a linear backoff strategy
    pub fn linear_backoff(max_retries: u32, delay: Duration) -> Self {
        Self {
            max_retries,
            initial_delay: delay,
            max_delay: delay,
            backoff_multiplier: 1.0,
            use_jitter: false,
        }
    }

    /// Creates a fixed delay strategy
    pub fn fixed_delay(max_retries: u32, delay: Duration) -> Self {
        Self {
            max_retries,
            initial_delay: delay,
            max_delay: delay,
            backoff_multiplier: 1.0,
            use_jitter: false,
        }
    }

    /// Creates a fibonacci backoff strategy
    pub fn fibonacci_backoff(max_retries: u32, base_delay: Duration) -> Self {
        Self {
            max_retries,
            initial_delay: base_delay,
            max_delay: Duration::from_secs(600), // 10 minutes
            backoff_multiplier: 1.618,           // Golden ratio
            use_jitter: true,
        }
    }

    /// Calculates the delay for a specific retry attempt
    pub fn calculate_delay(&self, attempt: u32) -> Duration {
        if attempt == 0 {
            return Duration::from_secs(0);
        }

        let base_delay = if self.backoff_multiplier == 1.0 {
            self.initial_delay
        } else {
            let multiplier = self.backoff_multiplier.powi(attempt as i32 - 1);
            let delay_ms = self.initial_delay.as_millis() as f64 * multiplier;
            Duration::from_millis(delay_ms.min(self.max_delay.as_millis() as f64) as u64)
        };

        if self.use_jitter {
            // Add ±25% jitter
            let jitter_factor = 0.75 + (rand::random::<f64>() * 0.5);
            let delay_ms = (base_delay.as_millis() as f64 * jitter_factor) as u64;
            Duration::from_millis(delay_ms)
        } else {
            base_delay
        }
    }

    /// Sets maximum delay
    pub fn with_max_delay(mut self, max_delay: Duration) -> Self {
        self.max_delay = max_delay;
        self
    }

    /// Sets whether to use jitter
    pub fn with_jitter(mut self, use_jitter: bool) -> Self {
        self.use_jitter = use_jitter;
        self
    }

    /// Sets backoff multiplier
    pub fn with_multiplier(mut self, multiplier: f64) -> Self {
        self.backoff_multiplier = multiplier;
        self
    }
}

impl Default for RetryStrategy {
    fn default() -> Self {
        Self::exponential_backoff(3, Duration::from_secs(1))
    }
}

/// Retry decision based on error type
pub trait RetryPolicy: Send + Sync {
    /// Determines if a retry should be attempted for the given error
    fn should_retry(&self, error: &str, attempt: u32) -> bool;
}

/// Default retry policy that retries on all errors
pub struct DefaultRetryPolicy {
    max_retries: u32,
}

impl DefaultRetryPolicy {
    /// Creates a new default retry policy
    pub fn new(max_retries: u32) -> Self {
        Self { max_retries }
    }
}

impl RetryPolicy for DefaultRetryPolicy {
    fn should_retry(&self, _error: &str, attempt: u32) -> bool {
        attempt < self.max_retries
    }
}

/// Retry policy that only retries on specific error patterns
pub struct ErrorPatternRetryPolicy {
    max_retries: u32,
    retryable_patterns: Vec<String>,
}

impl ErrorPatternRetryPolicy {
    /// Creates a new error pattern retry policy
    pub fn new(max_retries: u32, retryable_patterns: Vec<String>) -> Self {
        Self {
            max_retries,
            retryable_patterns,
        }
    }
}

impl RetryPolicy for ErrorPatternRetryPolicy {
    fn should_retry(&self, error: &str, attempt: u32) -> bool {
        if attempt >= self.max_retries {
            return false;
        }
        self.retryable_patterns
            .iter()
            .any(|pattern| error.contains(pattern))
    }
}
