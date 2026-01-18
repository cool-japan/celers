//! Retry strategy utilities
//!
//! This module provides sophisticated retry strategies for message processing.

use crate::Message;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// Retry strategy for failed tasks
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RetryStrategy {
    /// No retry
    None,
    /// Fixed delay between retries
    Fixed {
        /// Delay in seconds
        delay_secs: u32,
    },
    /// Exponential backoff with optional jitter
    Exponential {
        /// Base delay in seconds
        base_delay_secs: u32,
        /// Maximum delay in seconds
        max_delay_secs: u32,
        /// Multiplier for each retry (default: 2.0)
        multiplier: f64,
    },
    /// Linear backoff
    Linear {
        /// Initial delay in seconds
        initial_delay_secs: u32,
        /// Increment per retry in seconds
        increment_secs: u32,
        /// Maximum delay in seconds
        max_delay_secs: u32,
    },
    /// Custom delay for each retry attempt
    Custom {
        /// Delays for each retry attempt (in seconds)
        delays: Vec<u32>,
    },
}

impl RetryStrategy {
    /// Create a fixed delay retry strategy
    pub fn fixed(delay_secs: u32) -> Self {
        Self::Fixed { delay_secs }
    }

    /// Create an exponential backoff retry strategy
    pub fn exponential(base_delay_secs: u32, max_delay_secs: u32) -> Self {
        Self::Exponential {
            base_delay_secs,
            max_delay_secs,
            multiplier: 2.0,
        }
    }

    /// Create a linear backoff retry strategy
    pub fn linear(initial_delay_secs: u32, increment_secs: u32, max_delay_secs: u32) -> Self {
        Self::Linear {
            initial_delay_secs,
            increment_secs,
            max_delay_secs,
        }
    }

    /// Create a custom retry strategy with specific delays
    pub fn custom(delays: Vec<u32>) -> Self {
        Self::Custom { delays }
    }

    /// Calculate the delay for a specific retry attempt
    pub fn calculate_delay(&self, retry_count: u32) -> Option<Duration> {
        match self {
            RetryStrategy::None => None,
            RetryStrategy::Fixed { delay_secs } => Some(Duration::seconds(*delay_secs as i64)),
            RetryStrategy::Exponential {
                base_delay_secs,
                max_delay_secs,
                multiplier,
            } => {
                let delay = (*base_delay_secs as f64 * multiplier.powi(retry_count as i32))
                    .min(*max_delay_secs as f64);
                Some(Duration::seconds(delay as i64))
            }
            RetryStrategy::Linear {
                initial_delay_secs,
                increment_secs,
                max_delay_secs,
            } => {
                let delay =
                    (initial_delay_secs + increment_secs * retry_count).min(*max_delay_secs);
                Some(Duration::seconds(delay as i64))
            }
            RetryStrategy::Custom { delays } => delays
                .get(retry_count as usize)
                .map(|&d| Duration::seconds(d as i64)),
        }
    }

    /// Get the ETA for the next retry
    pub fn next_eta(&self, retry_count: u32) -> Option<DateTime<Utc>> {
        self.calculate_delay(retry_count)
            .map(|delay| Utc::now() + delay)
    }
}

impl Default for RetryStrategy {
    fn default() -> Self {
        Self::exponential(1, 3600) // 1 second base, 1 hour max
    }
}

/// Retry policy with maximum retry limits
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    strategy: RetryStrategy,
    max_retries: u32,
    retry_on_timeout: bool,
    retry_on_rate_limit: bool,
}

impl RetryPolicy {
    /// Create a new retry policy
    pub fn new(strategy: RetryStrategy, max_retries: u32) -> Self {
        Self {
            strategy,
            max_retries,
            retry_on_timeout: true,
            retry_on_rate_limit: true,
        }
    }

    /// Set whether to retry on timeout errors
    #[must_use]
    pub fn with_retry_on_timeout(mut self, retry: bool) -> Self {
        self.retry_on_timeout = retry;
        self
    }

    /// Set whether to retry on rate limit errors
    #[must_use]
    pub fn with_retry_on_rate_limit(mut self, retry: bool) -> Self {
        self.retry_on_rate_limit = retry;
        self
    }

    /// Check if a message should be retried
    pub fn should_retry(&self, message: &Message) -> bool {
        let current_retries = message.headers.retries.unwrap_or(0);
        current_retries < self.max_retries
    }

    /// Calculate the next ETA for a retry
    pub fn next_retry_eta(&self, message: &Message) -> Option<DateTime<Utc>> {
        let retry_count = message.headers.retries.unwrap_or(0);
        if self.should_retry(message) {
            self.strategy.next_eta(retry_count)
        } else {
            None
        }
    }

    /// Create a retry message with updated retry count and ETA
    pub fn create_retry_message(&self, message: &Message) -> Option<Message> {
        if !self.should_retry(message) {
            return None;
        }

        let mut retry_msg = message.clone();
        let current_retries = retry_msg.headers.retries.unwrap_or(0);
        retry_msg.headers.retries = Some(current_retries + 1);

        if let Some(eta) = self.strategy.next_eta(current_retries) {
            retry_msg.headers.eta = Some(eta);
        }

        Some(retry_msg)
    }

    /// Get the retry strategy
    pub fn strategy(&self) -> &RetryStrategy {
        &self.strategy
    }

    /// Get the maximum number of retries
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::new(RetryStrategy::default(), 3)
    }
}

/// Retry statistics
#[derive(Debug, Clone, Default)]
pub struct RetryStats {
    /// Total number of retries attempted
    pub total_retries: u64,
    /// Number of successful retries
    pub successful_retries: u64,
    /// Number of failed retries
    pub failed_retries: u64,
    /// Number of messages that exceeded max retries
    pub max_retries_exceeded: u64,
}

impl RetryStats {
    /// Create new retry statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a successful retry
    pub fn record_success(&mut self) {
        self.total_retries += 1;
        self.successful_retries += 1;
    }

    /// Record a failed retry
    pub fn record_failure(&mut self) {
        self.total_retries += 1;
        self.failed_retries += 1;
    }

    /// Record a message that exceeded max retries
    pub fn record_max_exceeded(&mut self) {
        self.max_retries_exceeded += 1;
    }

    /// Get the success rate as a percentage
    pub fn success_rate(&self) -> f64 {
        if self.total_retries == 0 {
            0.0
        } else {
            (self.successful_retries as f64 / self.total_retries as f64) * 100.0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::builder::MessageBuilder;

    fn create_test_message() -> Message {
        MessageBuilder::new("tasks.test").build().unwrap()
    }

    #[test]
    fn test_fixed_retry_strategy() {
        let strategy = RetryStrategy::fixed(5);
        assert_eq!(strategy.calculate_delay(0), Some(Duration::seconds(5)));
        assert_eq!(strategy.calculate_delay(1), Some(Duration::seconds(5)));
        assert_eq!(strategy.calculate_delay(5), Some(Duration::seconds(5)));
    }

    #[test]
    fn test_exponential_retry_strategy() {
        let strategy = RetryStrategy::exponential(1, 60);
        assert_eq!(strategy.calculate_delay(0), Some(Duration::seconds(1)));
        assert_eq!(strategy.calculate_delay(1), Some(Duration::seconds(2)));
        assert_eq!(strategy.calculate_delay(2), Some(Duration::seconds(4)));
        assert_eq!(strategy.calculate_delay(3), Some(Duration::seconds(8)));

        // Test max cap
        assert_eq!(strategy.calculate_delay(10), Some(Duration::seconds(60)));
    }

    #[test]
    fn test_linear_retry_strategy() {
        let strategy = RetryStrategy::linear(5, 10, 100);
        assert_eq!(strategy.calculate_delay(0), Some(Duration::seconds(5)));
        assert_eq!(strategy.calculate_delay(1), Some(Duration::seconds(15)));
        assert_eq!(strategy.calculate_delay(2), Some(Duration::seconds(25)));

        // Test max cap
        assert_eq!(strategy.calculate_delay(10), Some(Duration::seconds(100)));
    }

    #[test]
    fn test_custom_retry_strategy() {
        let strategy = RetryStrategy::custom(vec![1, 5, 10, 30]);
        assert_eq!(strategy.calculate_delay(0), Some(Duration::seconds(1)));
        assert_eq!(strategy.calculate_delay(1), Some(Duration::seconds(5)));
        assert_eq!(strategy.calculate_delay(2), Some(Duration::seconds(10)));
        assert_eq!(strategy.calculate_delay(3), Some(Duration::seconds(30)));
        assert_eq!(strategy.calculate_delay(4), None); // Beyond custom delays
    }

    #[test]
    fn test_retry_policy_should_retry() {
        let policy = RetryPolicy::new(RetryStrategy::fixed(5), 3);
        let mut msg = create_test_message();

        assert!(policy.should_retry(&msg));

        msg.headers.retries = Some(2);
        assert!(policy.should_retry(&msg));

        msg.headers.retries = Some(3);
        assert!(!policy.should_retry(&msg));
    }

    #[test]
    fn test_retry_policy_create_retry_message() {
        let policy = RetryPolicy::new(RetryStrategy::fixed(5), 3);
        let msg = create_test_message();

        let retry_msg = policy.create_retry_message(&msg).unwrap();
        assert_eq!(retry_msg.headers.retries, Some(1));
        assert!(retry_msg.headers.eta.is_some());

        // Test max retries
        let mut max_msg = msg.clone();
        max_msg.headers.retries = Some(3);
        assert!(policy.create_retry_message(&max_msg).is_none());
    }

    #[test]
    fn test_retry_policy_next_retry_eta() {
        let policy = RetryPolicy::new(RetryStrategy::fixed(10), 3);
        let msg = create_test_message();

        let eta = policy.next_retry_eta(&msg);
        assert!(eta.is_some());

        let now = Utc::now();
        let eta_time = eta.unwrap();
        let diff = (eta_time - now).num_seconds();
        assert!((9..=11).contains(&diff)); // ~10 seconds
    }

    #[test]
    fn test_retry_stats() {
        let mut stats = RetryStats::new();

        stats.record_success();
        stats.record_success();
        stats.record_failure();
        stats.record_max_exceeded();

        assert_eq!(stats.total_retries, 3);
        assert_eq!(stats.successful_retries, 2);
        assert_eq!(stats.failed_retries, 1);
        assert_eq!(stats.max_retries_exceeded, 1);

        // Use approximate equality for floating point
        let rate = stats.success_rate();
        assert!((rate - 66.66666666666667).abs() < 0.0001);
    }

    #[test]
    fn test_retry_strategy_none() {
        let strategy = RetryStrategy::None;
        assert_eq!(strategy.calculate_delay(0), None);
        assert_eq!(strategy.calculate_delay(5), None);
    }

    #[test]
    fn test_default_retry_policy() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.max_retries(), 3);
        assert!(policy.retry_on_timeout);
        assert!(policy.retry_on_rate_limit);
    }
}
