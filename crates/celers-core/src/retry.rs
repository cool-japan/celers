//! Retry strategies for task execution
//!
//! This module provides various retry strategies that determine how long to wait
//! between task retry attempts.

use rand::Rng;
use serde::{Deserialize, Serialize};

/// Retry strategy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RetryStrategy {
    /// Fixed delay between retries
    Fixed {
        /// Delay in seconds
        delay: u64,
    },

    /// Linear backoff (delay increases linearly)
    Linear {
        /// Initial delay in seconds
        initial: u64,
        /// Increment per retry in seconds
        increment: u64,
        /// Maximum delay in seconds
        max_delay: Option<u64>,
    },

    /// Exponential backoff (delay doubles each retry)
    Exponential {
        /// Initial delay in seconds
        initial: u64,
        /// Multiplier (default 2.0)
        multiplier: f64,
        /// Maximum delay in seconds
        max_delay: Option<u64>,
    },

    /// Polynomial backoff (delay = initial * retry^power)
    Polynomial {
        /// Initial delay in seconds
        initial: u64,
        /// Power (exponent)
        power: f64,
        /// Maximum delay in seconds
        max_delay: Option<u64>,
    },

    /// Fibonacci backoff (delays follow Fibonacci sequence)
    Fibonacci {
        /// Initial delay in seconds (F(1))
        initial: u64,
        /// Maximum delay in seconds
        max_delay: Option<u64>,
    },

    /// Decorrelated jitter (AWS recommended for distributed systems)
    /// delay = random(base, previous_delay * 3)
    DecorrelatedJitter {
        /// Base delay in seconds
        base: u64,
        /// Maximum delay in seconds
        max_delay: u64,
    },

    /// Full jitter (random between 0 and exponential delay)
    FullJitter {
        /// Initial delay in seconds
        initial: u64,
        /// Multiplier (default 2.0)
        multiplier: f64,
        /// Maximum delay in seconds
        max_delay: Option<u64>,
    },

    /// Equal jitter (half fixed, half random)
    EqualJitter {
        /// Initial delay in seconds
        initial: u64,
        /// Multiplier (default 2.0)
        multiplier: f64,
        /// Maximum delay in seconds
        max_delay: Option<u64>,
    },

    /// Custom delays specified explicitly for each retry
    Custom {
        /// Delays for each retry attempt
        delays: Vec<u64>,
        /// Delay to use after all custom delays exhausted
        fallback: u64,
    },

    /// No delay between retries
    Immediate,
}

impl Default for RetryStrategy {
    fn default() -> Self {
        Self::Exponential {
            initial: 1,
            multiplier: 2.0,
            max_delay: Some(3600),
        }
    }
}

impl RetryStrategy {
    /// Create a fixed delay strategy
    #[must_use]
    pub fn fixed(delay: u64) -> Self {
        Self::Fixed { delay }
    }

    /// Create a linear backoff strategy
    #[must_use]
    pub fn linear(initial: u64, increment: u64) -> Self {
        Self::Linear {
            initial,
            increment,
            max_delay: None,
        }
    }

    /// Create a linear backoff strategy with max delay
    #[must_use]
    pub fn linear_with_max(initial: u64, increment: u64, max_delay: u64) -> Self {
        Self::Linear {
            initial,
            increment,
            max_delay: Some(max_delay),
        }
    }

    /// Create an exponential backoff strategy
    #[must_use]
    pub fn exponential(initial: u64, multiplier: f64) -> Self {
        Self::Exponential {
            initial,
            multiplier,
            max_delay: None,
        }
    }

    /// Create an exponential backoff strategy with max delay
    #[must_use]
    pub fn exponential_with_max(initial: u64, multiplier: f64, max_delay: u64) -> Self {
        Self::Exponential {
            initial,
            multiplier,
            max_delay: Some(max_delay),
        }
    }

    /// Create a polynomial backoff strategy
    #[must_use]
    pub fn polynomial(initial: u64, power: f64) -> Self {
        Self::Polynomial {
            initial,
            power,
            max_delay: None,
        }
    }

    /// Create a Fibonacci backoff strategy
    #[must_use]
    pub fn fibonacci(initial: u64) -> Self {
        Self::Fibonacci {
            initial,
            max_delay: None,
        }
    }

    /// Create a decorrelated jitter strategy (AWS recommended)
    #[must_use]
    pub fn decorrelated_jitter(base: u64, max_delay: u64) -> Self {
        Self::DecorrelatedJitter { base, max_delay }
    }

    /// Create a full jitter strategy
    #[must_use]
    pub fn full_jitter(initial: u64, multiplier: f64, max_delay: u64) -> Self {
        Self::FullJitter {
            initial,
            multiplier,
            max_delay: Some(max_delay),
        }
    }

    /// Create an equal jitter strategy
    #[must_use]
    pub fn equal_jitter(initial: u64, multiplier: f64, max_delay: u64) -> Self {
        Self::EqualJitter {
            initial,
            multiplier,
            max_delay: Some(max_delay),
        }
    }

    /// Create a custom delays strategy
    #[must_use]
    pub fn custom(delays: Vec<u64>, fallback: u64) -> Self {
        Self::Custom { delays, fallback }
    }

    /// Create an immediate retry strategy (no delay)
    #[must_use]
    pub fn immediate() -> Self {
        Self::Immediate
    }

    /// Calculate the delay for a given retry attempt
    ///
    /// # Arguments
    /// * `retry_count` - Current retry attempt (0-based)
    /// * `previous_delay` - Previous delay (used by decorrelated jitter)
    ///
    /// # Returns
    /// Delay in seconds before the next retry
    #[must_use]
    pub fn calculate_delay(&self, retry_count: u32, previous_delay: Option<u64>) -> u64 {
        match self {
            Self::Fixed { delay } => *delay,

            Self::Linear {
                initial,
                increment,
                max_delay,
            } => {
                let delay = initial + (increment * retry_count as u64);
                max_delay.map_or(delay, |max| delay.min(max))
            }

            Self::Exponential {
                initial,
                multiplier,
                max_delay,
            } => {
                let delay = (*initial as f64 * multiplier.powi(retry_count as i32)) as u64;
                max_delay.map_or(delay, |max| delay.min(max))
            }

            Self::Polynomial {
                initial,
                power,
                max_delay,
            } => {
                let delay = (*initial as f64 * (retry_count as f64 + 1.0).powf(*power)) as u64;
                max_delay.map_or(delay, |max| delay.min(max))
            }

            Self::Fibonacci { initial, max_delay } => {
                // F(2)=1, F(3)=2, F(4)=3, F(5)=5, F(6)=8...
                // Use retry_count + 2 to get the proper sequence starting at 1
                let delay = *initial * fibonacci_number(retry_count + 2);
                max_delay.map_or(delay, |max| delay.min(max))
            }

            Self::DecorrelatedJitter { base, max_delay } => {
                let prev = previous_delay.unwrap_or(*base);
                let upper = (prev * 3).min(*max_delay);
                let lower = *base;
                if upper <= lower {
                    lower
                } else {
                    rand::rng().random_range(lower..=upper)
                }
            }

            Self::FullJitter {
                initial,
                multiplier,
                max_delay,
            } => {
                let exp_delay = (*initial as f64 * multiplier.powi(retry_count as i32)) as u64;
                let capped = max_delay.map_or(exp_delay, |max| exp_delay.min(max));
                if capped == 0 {
                    0
                } else {
                    rand::rng().random_range(0..=capped)
                }
            }

            Self::EqualJitter {
                initial,
                multiplier,
                max_delay,
            } => {
                let exp_delay = (*initial as f64 * multiplier.powi(retry_count as i32)) as u64;
                let capped = max_delay.map_or(exp_delay, |max| exp_delay.min(max));
                let half = capped / 2;
                if half == 0 {
                    half
                } else {
                    half + rand::rng().random_range(0..=half)
                }
            }

            Self::Custom { delays, fallback } => delays
                .get(retry_count as usize)
                .copied()
                .unwrap_or(*fallback),

            Self::Immediate => 0,
        }
    }

    /// Get the strategy name as a string
    #[must_use]
    pub fn name(&self) -> &'static str {
        match self {
            Self::Fixed { .. } => "fixed",
            Self::Linear { .. } => "linear",
            Self::Exponential { .. } => "exponential",
            Self::Polynomial { .. } => "polynomial",
            Self::Fibonacci { .. } => "fibonacci",
            Self::DecorrelatedJitter { .. } => "decorrelated_jitter",
            Self::FullJitter { .. } => "full_jitter",
            Self::EqualJitter { .. } => "equal_jitter",
            Self::Custom { .. } => "custom",
            Self::Immediate => "immediate",
        }
    }

    /// Check if this strategy uses randomness
    #[must_use]
    pub fn is_jittered(&self) -> bool {
        matches!(
            self,
            Self::DecorrelatedJitter { .. } | Self::FullJitter { .. } | Self::EqualJitter { .. }
        )
    }
}

impl std::fmt::Display for RetryStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fixed { delay } => write!(f, "Fixed({}s)", delay),
            Self::Linear {
                initial, increment, ..
            } => write!(f, "Linear({}s + {}s/retry)", initial, increment),
            Self::Exponential {
                initial,
                multiplier,
                ..
            } => write!(f, "Exponential({}s * {}^n)", initial, multiplier),
            Self::Polynomial { initial, power, .. } => {
                write!(f, "Polynomial({}s * n^{})", initial, power)
            }
            Self::Fibonacci { initial, .. } => write!(f, "Fibonacci({}s)", initial),
            Self::DecorrelatedJitter { base, max_delay } => {
                write!(f, "DecorrelatedJitter(base={}s, max={}s)", base, max_delay)
            }
            Self::FullJitter {
                initial,
                multiplier,
                ..
            } => write!(f, "FullJitter({}s * {}^n)", initial, multiplier),
            Self::EqualJitter {
                initial,
                multiplier,
                ..
            } => write!(f, "EqualJitter({}s * {}^n)", initial, multiplier),
            Self::Custom { delays, fallback } => {
                write!(f, "Custom({} delays, fallback={}s)", delays.len(), fallback)
            }
            Self::Immediate => write!(f, "Immediate"),
        }
    }
}

/// Calculate the nth Fibonacci number
fn fibonacci_number(n: u32) -> u64 {
    if n <= 1 {
        return n as u64;
    }

    let mut a = 0u64;
    let mut b = 1u64;

    for _ in 2..=n {
        let temp = a + b;
        a = b;
        b = temp;
    }

    b
}

/// Retry policy configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of retries
    pub max_retries: u32,

    /// Retry strategy
    pub strategy: RetryStrategy,

    /// Exceptions/error patterns to retry on (empty means retry all)
    #[serde(default)]
    pub retry_on: Vec<String>,

    /// Exceptions/error patterns to not retry on
    #[serde(default)]
    pub dont_retry_on: Vec<String>,

    /// Whether to retry on timeout errors
    #[serde(default = "default_true")]
    pub retry_on_timeout: bool,

    /// Whether to preserve the original task on failure (don't move to DLQ)
    #[serde(default)]
    pub preserve_on_failure: bool,
}

fn default_true() -> bool {
    true
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            strategy: RetryStrategy::default(),
            retry_on: Vec::new(),
            dont_retry_on: Vec::new(),
            retry_on_timeout: true,
            preserve_on_failure: false,
        }
    }
}

impl RetryPolicy {
    /// Create a new retry policy
    #[must_use]
    pub fn new(max_retries: u32, strategy: RetryStrategy) -> Self {
        Self {
            max_retries,
            strategy,
            ..Default::default()
        }
    }

    /// Create a policy with no retries
    #[must_use]
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            strategy: RetryStrategy::Immediate,
            ..Default::default()
        }
    }

    /// Set the maximum number of retries
    #[must_use]
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Set the retry strategy
    #[must_use]
    pub fn with_strategy(mut self, strategy: RetryStrategy) -> Self {
        self.strategy = strategy;
        self
    }

    /// Add error patterns to retry on
    #[must_use]
    pub fn retry_on(mut self, patterns: Vec<String>) -> Self {
        self.retry_on = patterns;
        self
    }

    /// Add error patterns to not retry on
    #[must_use]
    pub fn dont_retry_on(mut self, patterns: Vec<String>) -> Self {
        self.dont_retry_on = patterns;
        self
    }

    /// Set whether to retry on timeout
    #[must_use]
    pub fn with_retry_on_timeout(mut self, retry: bool) -> Self {
        self.retry_on_timeout = retry;
        self
    }

    /// Check if we should retry for the given error
    #[must_use]
    pub fn should_retry(&self, error: &str, retry_count: u32) -> bool {
        // Check if we've exceeded max retries
        if retry_count >= self.max_retries {
            return false;
        }

        // Check dont_retry_on patterns first (takes precedence)
        for pattern in &self.dont_retry_on {
            if error.contains(pattern) {
                return false;
            }
        }

        // If retry_on is empty, retry all errors
        if self.retry_on.is_empty() {
            return true;
        }

        // Check retry_on patterns
        for pattern in &self.retry_on {
            if error.contains(pattern) {
                return true;
            }
        }

        false
    }

    /// Get the delay before the next retry
    #[must_use]
    pub fn get_retry_delay(&self, retry_count: u32, previous_delay: Option<u64>) -> u64 {
        self.strategy.calculate_delay(retry_count, previous_delay)
    }

    /// Check if this policy allows retries
    #[must_use]
    pub fn allows_retry(&self) -> bool {
        self.max_retries > 0
    }
}

impl std::fmt::Display for RetryPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RetryPolicy(max={}, strategy={})",
            self.max_retries, self.strategy
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed_delay() {
        let strategy = RetryStrategy::fixed(5);
        assert_eq!(strategy.calculate_delay(0, None), 5);
        assert_eq!(strategy.calculate_delay(1, None), 5);
        assert_eq!(strategy.calculate_delay(10, None), 5);
    }

    #[test]
    fn test_linear_backoff() {
        let strategy = RetryStrategy::linear(1, 2);
        assert_eq!(strategy.calculate_delay(0, None), 1);
        assert_eq!(strategy.calculate_delay(1, None), 3);
        assert_eq!(strategy.calculate_delay(2, None), 5);
        assert_eq!(strategy.calculate_delay(3, None), 7);
    }

    #[test]
    fn test_linear_with_max() {
        let strategy = RetryStrategy::linear_with_max(1, 2, 5);
        assert_eq!(strategy.calculate_delay(0, None), 1);
        assert_eq!(strategy.calculate_delay(1, None), 3);
        assert_eq!(strategy.calculate_delay(2, None), 5);
        assert_eq!(strategy.calculate_delay(3, None), 5);
        assert_eq!(strategy.calculate_delay(10, None), 5);
    }

    #[test]
    fn test_exponential_backoff() {
        let strategy = RetryStrategy::exponential(1, 2.0);
        assert_eq!(strategy.calculate_delay(0, None), 1);
        assert_eq!(strategy.calculate_delay(1, None), 2);
        assert_eq!(strategy.calculate_delay(2, None), 4);
        assert_eq!(strategy.calculate_delay(3, None), 8);
    }

    #[test]
    fn test_exponential_with_max() {
        let strategy = RetryStrategy::exponential_with_max(1, 2.0, 5);
        assert_eq!(strategy.calculate_delay(0, None), 1);
        assert_eq!(strategy.calculate_delay(1, None), 2);
        assert_eq!(strategy.calculate_delay(2, None), 4);
        assert_eq!(strategy.calculate_delay(3, None), 5);
    }

    #[test]
    fn test_fibonacci_backoff() {
        let strategy = RetryStrategy::fibonacci(1);
        assert_eq!(strategy.calculate_delay(0, None), 1); // F(1) = 1
        assert_eq!(strategy.calculate_delay(1, None), 2); // F(2) = 1
        assert_eq!(strategy.calculate_delay(2, None), 3); // F(3) = 2
        assert_eq!(strategy.calculate_delay(3, None), 5); // F(4) = 3
        assert_eq!(strategy.calculate_delay(4, None), 8); // F(5) = 5
    }

    #[test]
    fn test_custom_delays() {
        let strategy = RetryStrategy::custom(vec![1, 5, 10, 30], 60);
        assert_eq!(strategy.calculate_delay(0, None), 1);
        assert_eq!(strategy.calculate_delay(1, None), 5);
        assert_eq!(strategy.calculate_delay(2, None), 10);
        assert_eq!(strategy.calculate_delay(3, None), 30);
        assert_eq!(strategy.calculate_delay(4, None), 60);
        assert_eq!(strategy.calculate_delay(10, None), 60);
    }

    #[test]
    fn test_immediate() {
        let strategy = RetryStrategy::immediate();
        assert_eq!(strategy.calculate_delay(0, None), 0);
        assert_eq!(strategy.calculate_delay(10, None), 0);
    }

    #[test]
    fn test_retry_policy_should_retry() {
        let policy = RetryPolicy::new(3, RetryStrategy::fixed(1))
            .retry_on(vec!["timeout".to_string(), "connection".to_string()])
            .dont_retry_on(vec!["fatal".to_string()]);

        assert!(policy.should_retry("connection refused", 0));
        assert!(policy.should_retry("timeout error", 1));
        assert!(!policy.should_retry("fatal error", 0));
        assert!(!policy.should_retry("connection error", 3)); // Max retries reached
    }

    #[test]
    fn test_retry_policy_empty_retry_on() {
        let policy = RetryPolicy::new(3, RetryStrategy::fixed(1));

        // Empty retry_on means retry all errors
        assert!(policy.should_retry("any error", 0));
        assert!(policy.should_retry("another error", 1));
    }

    #[test]
    fn test_fibonacci_numbers() {
        assert_eq!(fibonacci_number(0), 0);
        assert_eq!(fibonacci_number(1), 1);
        assert_eq!(fibonacci_number(2), 1);
        assert_eq!(fibonacci_number(3), 2);
        assert_eq!(fibonacci_number(4), 3);
        assert_eq!(fibonacci_number(5), 5);
        assert_eq!(fibonacci_number(6), 8);
        assert_eq!(fibonacci_number(10), 55);
    }

    #[test]
    fn test_strategy_names() {
        assert_eq!(RetryStrategy::fixed(1).name(), "fixed");
        assert_eq!(RetryStrategy::linear(1, 1).name(), "linear");
        assert_eq!(RetryStrategy::exponential(1, 2.0).name(), "exponential");
        assert_eq!(RetryStrategy::fibonacci(1).name(), "fibonacci");
        assert_eq!(RetryStrategy::immediate().name(), "immediate");
    }

    #[test]
    fn test_strategy_display() {
        assert_eq!(format!("{}", RetryStrategy::fixed(5)), "Fixed(5s)");
        assert_eq!(
            format!("{}", RetryStrategy::linear(1, 2)),
            "Linear(1s + 2s/retry)"
        );
        assert_eq!(
            format!("{}", RetryStrategy::exponential(1, 2.0)),
            "Exponential(1s * 2^n)"
        );
    }

    mod proptests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn test_fixed_delay_is_constant(delay in 1u64..10000, attempt in 0u32..100) {
                let strategy = RetryStrategy::fixed(delay);
                let calculated_delay = strategy.calculate_delay(attempt, Some(delay));
                prop_assert_eq!(calculated_delay, delay);
            }

            #[test]
            fn test_linear_delay_increases_linearly(
                initial in 100u64..1000,
                increment in 100u64..1000,
                attempt in 0u32..50
            ) {
                let strategy = RetryStrategy::linear(initial, increment);
                let expected = initial + (increment * attempt as u64);
                let calculated = strategy.calculate_delay(attempt, None);
                prop_assert_eq!(calculated, expected);
            }

            #[test]
            fn test_exponential_delay_grows(
                initial in 100u64..1000,
                multiplier in 1.5f64..3.0,
                attempt in 0u32..10
            ) {
                let strategy = RetryStrategy::exponential(initial, multiplier);
                let delay1 = strategy.calculate_delay(attempt, None);
                let delay2 = strategy.calculate_delay(attempt + 1, Some(delay1));

                // Exponential should grow (or stay same if at max)
                prop_assert!(delay2 >= delay1);
            }

            #[test]
            fn test_exponential_with_max_respects_limit(
                initial in 100u64..1000,
                multiplier in 2.0f64..4.0,
                max_delay in 5000u64..10000,
                attempt in 0u32..20
            ) {
                let strategy = RetryStrategy::exponential_with_max(initial, multiplier, max_delay);
                let calculated = strategy.calculate_delay(attempt, None);
                prop_assert!(calculated <= max_delay);
            }

            #[test]
            fn test_fibonacci_delay_grows(
                initial in 100u64..1000,
                attempt in 1u32..15
            ) {
                let strategy = RetryStrategy::fibonacci(initial);
                let delay1 = strategy.calculate_delay(attempt, None);
                let delay2 = strategy.calculate_delay(attempt + 1, Some(delay1));

                // Fibonacci should grow
                prop_assert!(delay2 >= delay1);
            }

            #[test]
            fn test_immediate_is_always_zero(attempt in 0u32..1000) {
                let strategy = RetryStrategy::immediate();
                let delay = strategy.calculate_delay(attempt, None);
                prop_assert_eq!(delay, 0);
            }

            #[test]
            fn test_full_jitter_within_bounds(
                initial in 100u64..1000,
                multiplier in 2.0f64..3.0,
                max_delay in 10000u64..20000,
                attempt in 0u32..10
            ) {
                let strategy = RetryStrategy::full_jitter(initial, multiplier, max_delay);
                let delay = strategy.calculate_delay(attempt, None);

                // Full jitter should be between 0 and exponential delay (capped at max)
                prop_assert!(delay <= max_delay);
            }

            #[test]
            fn test_decorrelated_jitter_within_bounds(
                base in 100u64..1000,
                max_delay in 10000u64..20000,
                attempt in 0u32..50,
                prev_delay in 100u64..5000
            ) {
                let strategy = RetryStrategy::decorrelated_jitter(base, max_delay);
                let delay = strategy.calculate_delay(attempt, Some(prev_delay));

                // Decorrelated jitter should be within bounds
                prop_assert!(delay <= max_delay);
                prop_assert!(delay >= base);
            }

            #[test]
            fn test_polynomial_delay_grows(
                initial in 100u64..1000,
                power in 1.0f64..3.0,
                attempt in 1u32..10
            ) {
                let strategy = RetryStrategy::polynomial(initial, power);
                let delay1 = strategy.calculate_delay(attempt, None);
                let delay2 = strategy.calculate_delay(attempt + 1, Some(delay1));

                // Polynomial should grow for power >= 1
                if power >= 1.0 {
                    prop_assert!(delay2 >= delay1);
                }
            }

            #[test]
            fn test_custom_strategy_uses_provided_delays(
                delays in prop::collection::vec(100u64..5000, 1..10),
                fallback in 1000u64..5000,
                attempt in 0u32..20
            ) {
                let strategy = RetryStrategy::custom(delays.clone(), fallback);
                let calculated = strategy.calculate_delay(attempt, None);

                if (attempt as usize) < delays.len() {
                    prop_assert_eq!(calculated, delays[attempt as usize]);
                } else {
                    // Should use fallback when attempt exceeds length
                    prop_assert_eq!(calculated, fallback);
                }
            }

            #[test]
            fn test_retry_policy_respects_max_retries(
                max_retries in 0u32..100,
                current_retry in 0u32..150
            ) {
                let policy = RetryPolicy::new(max_retries, RetryStrategy::fixed(1000));

                let should_retry = policy.should_retry("test error", current_retry);

                if current_retry < max_retries {
                    prop_assert!(should_retry);
                } else {
                    prop_assert!(!should_retry);
                }
            }
        }
    }
}
