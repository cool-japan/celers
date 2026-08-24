//! Retry strategies for task execution
//!
//! This module provides various retry strategies that determine how long to wait
//! between task retry attempts.
//!
//! All delay arithmetic saturates rather than overflowing, and every computed
//! delay is clamped to at most [`MAX_RETRY_DELAY_SECS`], so no configuration —
//! however absurd — can panic in debug, wrap in release, or produce a
//! `u64::MAX`-second delay.

use rand::RngExt;
use serde::{Deserialize, Serialize};

/// Hard ceiling applied to every computed retry delay: 30 days in seconds.
///
/// Used whenever a strategy has no explicit `max_delay`. Without it, an
/// exponential strategy at a high retry count saturates `f64::INFINITY as u64` to
/// `u64::MAX`, i.e. a delay of ~584 billion years — indistinguishable from the
/// task never being retried at all, and impossible to represent downstream.
pub const MAX_RETRY_DELAY_SECS: u64 = 30 * 24 * 60 * 60;

/// `2^64` as an `f64`, used as the saturation boundary for `f64 -> u64`.
const U64_MAX_AS_F64: f64 = 18_446_744_073_709_551_616.0;

/// Convert a computed floating-point delay to whole seconds without overflow.
///
/// Non-finite and negative values collapse to `0`; anything at or beyond `2^64`
/// saturates to `u64::MAX` (and is then clamped by the caller).
// Justification for the lossy cast: the value is checked for finiteness, clamped
// at zero and compared against `2^64` immediately before the conversion.
#[allow(clippy::cast_possible_truncation, clippy::cast_sign_loss)]
#[inline]
fn seconds_from_f64(value: f64) -> u64 {
    if !value.is_finite() || value <= 0.0 {
        0
    } else if value >= U64_MAX_AS_F64 {
        u64::MAX
    } else {
        value as u64
    }
}

/// Widen a `u64` delay for floating-point backoff math.
// Justification: retry delays are seconds and far below `2^53` after clamping;
// the widening is exact for every value the caller can observe.
#[allow(clippy::cast_precision_loss)]
#[inline]
fn seconds_as_f64(seconds: u64) -> f64 {
    seconds as f64
}

/// Raise `multiplier` to the power of `retry_count` without wrapping the exponent.
///
/// `powi` takes an `i32`; a `retry_count` above `i32::MAX` would wrap to a
/// negative exponent and silently invert the backoff. Anything that large is
/// clamped, and the result is saturated by the caller anyway.
#[inline]
fn multiplier_pow(multiplier: f64, retry_count: u32) -> f64 {
    let exponent = i32::try_from(retry_count).unwrap_or(i32::MAX);
    multiplier.powi(exponent)
}

/// Clamp a computed delay.
///
/// An explicit `max_delay` is always honoured, even if it is larger than
/// [`MAX_RETRY_DELAY_SECS`]; when none is configured the default ceiling applies
/// so an unbounded strategy cannot return a `u64::MAX`-second delay.
#[inline]
fn clamp_delay(delay: u64, max_delay: Option<u64>) -> u64 {
    delay.min(max_delay.unwrap_or(MAX_RETRY_DELAY_SECS))
}

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
    /// delay = random(base, `previous_delay` * 3)
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
    /// Delay in seconds before the next retry.
    ///
    /// Every arithmetic step saturates, and computed delays are clamped to the
    /// strategy's `max_delay` or, when it has none, to [`MAX_RETRY_DELAY_SECS`].
    /// No `retry_count` — including `u32::MAX` — can panic or wrap.
    #[must_use]
    pub fn calculate_delay(&self, retry_count: u32, previous_delay: Option<u64>) -> u64 {
        match self {
            Self::Fixed { delay } => *delay,

            Self::Linear {
                initial,
                increment,
                max_delay,
            } => {
                let delay =
                    initial.saturating_add(increment.saturating_mul(u64::from(retry_count)));
                clamp_delay(delay, *max_delay)
            }

            Self::Exponential {
                initial,
                multiplier,
                max_delay,
            } => {
                let delay = seconds_from_f64(
                    seconds_as_f64(*initial) * multiplier_pow(*multiplier, retry_count),
                );
                clamp_delay(delay, *max_delay)
            }

            Self::Polynomial {
                initial,
                power,
                max_delay,
            } => {
                let delay = seconds_from_f64(
                    seconds_as_f64(*initial) * (f64::from(retry_count) + 1.0).powf(*power),
                );
                clamp_delay(delay, *max_delay)
            }

            Self::Fibonacci { initial, max_delay } => {
                // F(2)=1, F(3)=2, F(4)=3, F(5)=5, F(6)=8...
                // Use retry_count + 2 to get the proper sequence starting at 1
                let index = retry_count.saturating_add(2);
                let delay = initial.saturating_mul(fibonacci_number(index));
                clamp_delay(delay, *max_delay)
            }

            Self::DecorrelatedJitter { base, max_delay } => {
                let prev = previous_delay.unwrap_or(*base);
                let upper = prev.saturating_mul(3).min(*max_delay);
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
                let exp_delay = seconds_from_f64(
                    seconds_as_f64(*initial) * multiplier_pow(*multiplier, retry_count),
                );
                let capped = clamp_delay(exp_delay, *max_delay);
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
                let exp_delay = seconds_from_f64(
                    seconds_as_f64(*initial) * multiplier_pow(*multiplier, retry_count),
                );
                let capped = clamp_delay(exp_delay, *max_delay);
                let half = capped / 2;
                if half == 0 {
                    half
                } else {
                    half + rand::rng().random_range(0..=half)
                }
            }

            Self::Custom { delays, fallback } => usize::try_from(retry_count)
                .ok()
                .and_then(|index| delays.get(index))
                .copied()
                .unwrap_or(*fallback),

            Self::Immediate => 0,
        }
    }

    /// Get the strategy name as a string
    #[inline]
    #[must_use]
    pub const fn name(&self) -> &'static str {
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
    #[inline]
    #[must_use]
    pub const fn is_jittered(&self) -> bool {
        matches!(
            self,
            Self::DecorrelatedJitter { .. } | Self::FullJitter { .. } | Self::EqualJitter { .. }
        )
    }
}

impl std::fmt::Display for RetryStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Fixed { delay } => write!(f, "Fixed({delay}s)"),
            Self::Linear {
                initial, increment, ..
            } => write!(f, "Linear({initial}s + {increment}s/retry)"),
            Self::Exponential {
                initial,
                multiplier,
                ..
            } => write!(f, "Exponential({initial}s * {multiplier}^n)"),
            Self::Polynomial { initial, power, .. } => {
                write!(f, "Polynomial({initial}s * n^{power})")
            }
            Self::Fibonacci { initial, .. } => write!(f, "Fibonacci({initial}s)"),
            Self::DecorrelatedJitter { base, max_delay } => {
                write!(f, "DecorrelatedJitter(base={base}s, max={max_delay}s)")
            }
            Self::FullJitter {
                initial,
                multiplier,
                ..
            } => write!(f, "FullJitter({initial}s * {multiplier}^n)"),
            Self::EqualJitter {
                initial,
                multiplier,
                ..
            } => write!(f, "EqualJitter({initial}s * {multiplier}^n)"),
            Self::Custom { delays, fallback } => {
                write!(f, "Custom({} delays, fallback={}s)", delays.len(), fallback)
            }
            Self::Immediate => write!(f, "Immediate"),
        }
    }
}

/// Calculate the nth Fibonacci number, saturating at `u64::MAX`.
///
/// `F(94)` already exceeds `u64::MAX`, so the naive accumulation panicked in
/// debug builds and wrapped in release for `n >= 94` — reachable from a retry
/// count of 92 with a `max_retries` config that has no upper bound.
fn fibonacci_number(n: u32) -> u64 {
    if n <= 1 {
        return u64::from(n);
    }

    let mut a = 0u64;
    let mut b = 1u64;

    for _ in 2..=n {
        let Some(next) = a.checked_add(b) else {
            return u64::MAX;
        };
        a = b;
        b = next;
    }

    b
}

/// Coarse classification of a task failure, used to apply kind-specific retry
/// rules such as [`RetryPolicy::retry_on_timeout`].
///
/// Celery keys `autoretry_for` off exception *types*; substring matching on the
/// error message (which `retry_on` / `dont_retry_on` still use) is fragile by
/// comparison, so this enum gives callers a structured alternative for the cases
/// the policy treats specially.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum RetryErrorKind {
    /// The task exceeded its time limit.
    Timeout,
    /// The task was revoked; retrying is pointless.
    Revoked,
    /// A (de)serialization failure: deterministic, so retrying rarely helps.
    Serialization,
    /// A broker or transport failure: typically transient.
    Broker,
    /// A configuration failure: deterministic.
    Configuration,
    /// Anything else.
    #[default]
    Other,
}

impl RetryErrorKind {
    /// Classify a [`crate::CelersError`].
    #[must_use]
    pub const fn from_error(error: &crate::CelersError) -> Self {
        match error {
            crate::CelersError::Timeout(_) => Self::Timeout,
            crate::CelersError::TaskRevoked(_) => Self::Revoked,
            crate::CelersError::Serialization(_) | crate::CelersError::Deserialization(_) => {
                Self::Serialization
            }
            crate::CelersError::Broker(_) => Self::Broker,
            crate::CelersError::Configuration(_) => Self::Configuration,
            _ => Self::Other,
        }
    }

    /// Best-effort classification of a free-form error message.
    ///
    /// Used by [`RetryPolicy::should_retry`], whose only input is a string. The
    /// phrases below cover the messages produced by this workspace
    /// (`CelersError::Timeout` renders as "Task timeout: ...") as well as the
    /// common runtime wordings.
    #[must_use]
    pub fn classify(error: &str) -> Self {
        let lowered = error.to_lowercase();
        if lowered.contains("timeout")
            || lowered.contains("timed out")
            || lowered.contains("deadline exceeded")
        {
            Self::Timeout
        } else if lowered.contains("revoked") {
            Self::Revoked
        } else if lowered.contains("serialization") || lowered.contains("deserialization") {
            Self::Serialization
        } else if lowered.contains("broker error") {
            Self::Broker
        } else if lowered.contains("configuration error") {
            Self::Configuration
        } else {
            Self::Other
        }
    }
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

    /// Set whether a permanently failed task is preserved instead of being moved
    /// to the dead-letter queue.
    #[must_use]
    pub fn with_preserve_on_failure(mut self, preserve: bool) -> Self {
        self.preserve_on_failure = preserve;
        self
    }

    /// Check if we should retry for the given error
    ///
    /// The error kind is inferred from the message with
    /// [`RetryErrorKind::classify`]; use [`Self::should_retry_kind`] when the
    /// caller already knows the kind, and [`Self::should_retry_error`] when it
    /// holds a [`crate::CelersError`].
    #[must_use]
    pub fn should_retry(&self, error: &str, retry_count: u32) -> bool {
        self.should_retry_kind(error, RetryErrorKind::classify(error), retry_count)
    }

    /// Check if we should retry a [`crate::CelersError`]
    #[must_use]
    pub fn should_retry_error(&self, error: &crate::CelersError, retry_count: u32) -> bool {
        self.should_retry_kind(
            &error.to_string(),
            RetryErrorKind::from_error(error),
            retry_count,
        )
    }

    /// Check if we should retry, given an already-classified error kind
    ///
    /// This is where `retry_on_timeout` takes effect: a timeout is never retried
    /// when the policy disables it, regardless of the `retry_on` patterns.
    #[must_use]
    pub fn should_retry_kind(&self, error: &str, kind: RetryErrorKind, retry_count: u32) -> bool {
        // Check if we've exceeded max retries
        if retry_count >= self.max_retries {
            return false;
        }

        // A policy that opts out of timeout retries short-circuits everything
        // else: the failure is by definition not transient for this task.
        if kind == RetryErrorKind::Timeout && !self.retry_on_timeout {
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

    /// Whether a task that has exhausted its retries should be moved to the
    /// dead-letter queue.
    ///
    /// Returns `false` when `preserve_on_failure` is set, which is the whole
    /// point of that flag: the original task stays where it is instead of being
    /// dead-lettered.
    #[inline]
    #[must_use]
    pub const fn should_dead_letter(&self) -> bool {
        !self.preserve_on_failure
    }

    /// Whether the original task must be preserved on permanent failure.
    #[inline]
    #[must_use]
    pub const fn preserves_on_failure(&self) -> bool {
        self.preserve_on_failure
    }

    /// Get the delay before the next retry
    #[inline]
    #[must_use]
    pub fn get_retry_delay(&self, retry_count: u32, previous_delay: Option<u64>) -> u64 {
        self.strategy.calculate_delay(retry_count, previous_delay)
    }

    /// Check if this policy allows retries
    #[inline]
    #[must_use]
    pub const fn allows_retry(&self) -> bool {
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

    // ------------------------------------------------------------------
    // Regression tests
    // ------------------------------------------------------------------

    fn all_strategies() -> Vec<RetryStrategy> {
        vec![
            RetryStrategy::Fixed { delay: 7 },
            RetryStrategy::Linear {
                initial: u64::MAX - 1,
                increment: u64::MAX / 2,
                max_delay: None,
            },
            RetryStrategy::Exponential {
                initial: u64::MAX,
                multiplier: 10.0,
                max_delay: None,
            },
            RetryStrategy::Polynomial {
                initial: u64::MAX,
                power: 12.0,
                max_delay: None,
            },
            RetryStrategy::Fibonacci {
                initial: u64::MAX,
                max_delay: None,
            },
            RetryStrategy::DecorrelatedJitter {
                base: 1,
                max_delay: 600,
            },
            RetryStrategy::FullJitter {
                initial: u64::MAX,
                multiplier: 3.0,
                max_delay: None,
            },
            RetryStrategy::EqualJitter {
                initial: u64::MAX,
                multiplier: 3.0,
                max_delay: None,
            },
            RetryStrategy::Custom {
                delays: vec![1, 2, 3],
                fallback: 9,
            },
            RetryStrategy::Immediate,
        ]
    }

    /// Regression: `Fibonacci`, `Linear` and `DecorrelatedJitter` used unchecked
    /// `+`/`*`, panicking in debug and wrapping in release at high retry counts.
    #[test]
    fn test_delay_arithmetic_never_overflows() {
        let counts = [0u32, 1, 50, 91, 92, 93, 94, 1_000, u32::MAX - 1, u32::MAX];
        for strategy in all_strategies() {
            for &count in &counts {
                // Feed a hostile previous_delay too (DecorrelatedJitter reads it).
                for previous in [None, Some(0), Some(u64::MAX)] {
                    let delay = strategy.calculate_delay(count, previous);
                    assert!(
                        delay < u64::MAX,
                        "{strategy} at retry {count} produced a saturated delay"
                    );
                }
            }
        }
    }

    #[test]
    fn test_computed_delays_are_clamped_to_a_sane_ceiling() {
        // Strategies without an explicit max_delay fall back to the default cap
        // instead of yielding a u64::MAX-second delay.
        let unbounded = [
            RetryStrategy::Exponential {
                initial: 1,
                multiplier: 2.0,
                max_delay: None,
            },
            RetryStrategy::Linear {
                initial: 1,
                increment: u64::MAX / 4,
                max_delay: None,
            },
            RetryStrategy::Fibonacci {
                initial: 1,
                max_delay: None,
            },
            RetryStrategy::Polynomial {
                initial: 1,
                power: 20.0,
                max_delay: None,
            },
        ];
        for strategy in unbounded {
            let delay = strategy.calculate_delay(200, None);
            assert!(
                delay <= MAX_RETRY_DELAY_SECS,
                "{strategy} produced {delay}s, above the {MAX_RETRY_DELAY_SECS}s ceiling"
            );
        }

        // An explicit max_delay is still honoured verbatim, even above the cap.
        let explicit = RetryStrategy::Exponential {
            initial: 1,
            multiplier: 2.0,
            max_delay: Some(MAX_RETRY_DELAY_SECS * 4),
        };
        assert_eq!(
            explicit.calculate_delay(200, None),
            MAX_RETRY_DELAY_SECS * 4
        );
    }

    #[test]
    fn test_fibonacci_number_saturates() {
        assert_eq!(fibonacci_number(0), 0);
        assert_eq!(fibonacci_number(1), 1);
        assert_eq!(fibonacci_number(10), 55);
        assert_eq!(fibonacci_number(93), 12_200_160_415_121_876_738);
        // F(94) exceeds u64::MAX.
        assert_eq!(fibonacci_number(94), u64::MAX);
        assert_eq!(fibonacci_number(u32::MAX), u64::MAX);
    }

    /// Regression: `retry_on_timeout` was configurable but never read.
    #[test]
    fn test_retry_on_timeout_is_honoured() {
        let policy = RetryPolicy::new(5, RetryStrategy::Immediate).with_retry_on_timeout(false);
        assert!(!policy.should_retry("Task timeout: soft limit exceeded", 0));
        assert!(!policy.should_retry("operation timed out", 0));
        assert!(!policy.should_retry_kind("anything", RetryErrorKind::Timeout, 0));
        assert!(
            !policy.should_retry_error(&crate::CelersError::Timeout("hard limit".to_string()), 0)
        );
        // Non-timeout failures are unaffected.
        assert!(policy.should_retry("connection reset", 0));

        // The default policy still retries timeouts.
        let default_policy = RetryPolicy::new(5, RetryStrategy::Immediate);
        assert!(default_policy.should_retry("Task timeout: soft limit exceeded", 0));
        assert!(default_policy.should_retry_kind("anything", RetryErrorKind::Timeout, 0));

        // A disabled timeout retry beats an explicit retry_on match.
        let explicit = RetryPolicy::new(5, RetryStrategy::Immediate)
            .with_retry_on_timeout(false)
            .retry_on(vec!["timeout".to_string()]);
        assert!(!explicit.should_retry("Task timeout: soft limit exceeded", 0));
    }

    #[test]
    fn test_preserve_on_failure_is_readable() {
        let default_policy = RetryPolicy::default();
        assert!(!default_policy.preserves_on_failure());
        assert!(default_policy.should_dead_letter());

        let preserving = RetryPolicy::default().with_preserve_on_failure(true);
        assert!(preserving.preserves_on_failure());
        assert!(
            !preserving.should_dead_letter(),
            "a preserving policy must not dead-letter the original task"
        );
    }

    #[test]
    fn test_retry_error_kind_classification() {
        assert_eq!(
            RetryErrorKind::from_error(&crate::CelersError::Timeout("x".into())),
            RetryErrorKind::Timeout
        );
        assert_eq!(
            RetryErrorKind::from_error(&crate::CelersError::Broker("x".into())),
            RetryErrorKind::Broker
        );
        assert_eq!(
            RetryErrorKind::from_error(&crate::CelersError::Serialization("x".into())),
            RetryErrorKind::Serialization
        );
        assert_eq!(
            RetryErrorKind::from_error(&crate::CelersError::Other("x".into())),
            RetryErrorKind::Other
        );

        assert_eq!(
            RetryErrorKind::classify("Task timeout: 30s"),
            RetryErrorKind::Timeout
        );
        assert_eq!(
            RetryErrorKind::classify("DEADLINE EXCEEDED"),
            RetryErrorKind::Timeout
        );
        assert_eq!(
            RetryErrorKind::classify("connection reset"),
            RetryErrorKind::Other
        );
    }

    mod proptests {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn test_no_strategy_panics_or_exceeds_ceiling(
                attempt in 0u32..=u32::MAX,
                previous in proptest::option::of(0u64..=u64::MAX),
            ) {
                for strategy in super::all_strategies() {
                    let delay = strategy.calculate_delay(attempt, previous);
                    // Only the caller-supplied Fixed/Custom values bypass the
                    // ceiling; every computed strategy must respect it.
                    if !matches!(
                        strategy,
                        RetryStrategy::Fixed { .. }
                            | RetryStrategy::Custom { .. }
                            | RetryStrategy::DecorrelatedJitter { .. }
                    ) {
                        prop_assert!(delay <= MAX_RETRY_DELAY_SECS);
                    }
                    prop_assert!(delay < u64::MAX);
                }
            }
        }

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
                let expected = initial + (increment * u64::from(attempt));
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
