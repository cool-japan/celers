//! Retry strategies for task execution
//!
//! This module provides various retry strategies that can be configured per worker
//! or per task. Supported strategies include:
//!
//! - **Exponential Backoff**: Exponentially increase delay between retries (default)
//! - **Linear Backoff**: Linearly increase delay between retries
//! - **Fixed Delay**: Use a fixed delay between all retries
//! - **Custom**: User-defined retry logic via closure
//!
//! # Examples
//!
//! ```
//! use celers_worker::{RetryStrategy, RetryConfig};
//! use std::time::Duration;
//!
//! // Exponential backoff (default)
//! let exponential = RetryStrategy::Exponential {
//!     base_delay: Duration::from_secs(1),
//!     max_delay: Duration::from_secs(60),
//!     multiplier: 2.0,
//! };
//!
//! // Linear backoff
//! let linear = RetryStrategy::Linear {
//!     initial_delay: Duration::from_secs(1),
//!     increment: Duration::from_secs(2),
//!     max_delay: Duration::from_secs(30),
//! };
//!
//! // Fixed delay
//! let fixed = RetryStrategy::Fixed {
//!     delay: Duration::from_secs(5),
//! };
//!
//! // Calculate delay for retry attempt
//! let delay = exponential.calculate_delay(3); // 4th attempt (0-indexed)
//! ```

use std::sync::Arc;
use std::time::Duration;

/// Retry strategy configuration
#[derive(Clone)]
pub enum RetryStrategy {
    /// Exponential backoff: delay = base_delay * multiplier^retry_count (capped at max_delay)
    Exponential {
        /// Initial delay before first retry
        base_delay: Duration,
        /// Maximum delay between retries
        max_delay: Duration,
        /// Multiplier for exponential growth (typically 2.0)
        multiplier: f64,
    },

    /// Linear backoff: delay = initial_delay + (increment * retry_count) (capped at max_delay)
    Linear {
        /// Initial delay before first retry
        initial_delay: Duration,
        /// Amount to increment delay with each retry
        increment: Duration,
        /// Maximum delay between retries
        max_delay: Duration,
    },

    /// Fixed delay: same delay for all retries
    Fixed {
        /// Fixed delay between retries
        delay: Duration,
    },

    /// Custom retry strategy using a user-provided function
    ///
    /// The function receives the retry count (0-indexed) and returns the delay duration
    Custom(Arc<dyn Fn(u32) -> Duration + Send + Sync>),
}

impl RetryStrategy {
    /// Calculate the delay for a given retry attempt
    ///
    /// # Arguments
    ///
    /// * `retry_count` - The retry attempt number (0-indexed, so 0 = first retry)
    ///
    /// # Returns
    ///
    /// The duration to wait before the next retry attempt
    pub fn calculate_delay(&self, retry_count: u32) -> Duration {
        match self {
            RetryStrategy::Exponential {
                base_delay,
                max_delay,
                multiplier,
            } => {
                let delay_ms = base_delay.as_millis() as f64 * multiplier.powi(retry_count as i32);
                let delay_ms = delay_ms.min(max_delay.as_millis() as f64);
                Duration::from_millis(delay_ms as u64)
            }
            RetryStrategy::Linear {
                initial_delay,
                increment,
                max_delay,
            } => {
                let delay = *initial_delay + increment.saturating_mul(retry_count);
                delay.min(*max_delay)
            }
            RetryStrategy::Fixed { delay } => *delay,
            RetryStrategy::Custom(func) => func(retry_count),
        }
    }

    /// Create a default exponential backoff strategy
    ///
    /// - Base delay: 1 second
    /// - Max delay: 60 seconds
    /// - Multiplier: 2.0
    pub fn default_exponential() -> Self {
        RetryStrategy::Exponential {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        }
    }

    /// Create a fast exponential backoff strategy
    ///
    /// - Base delay: 100ms
    /// - Max delay: 10 seconds
    /// - Multiplier: 2.0
    pub fn fast_exponential() -> Self {
        RetryStrategy::Exponential {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
        }
    }

    /// Create a slow linear backoff strategy
    ///
    /// - Initial delay: 5 seconds
    /// - Increment: 5 seconds per retry
    /// - Max delay: 60 seconds
    pub fn slow_linear() -> Self {
        RetryStrategy::Linear {
            initial_delay: Duration::from_secs(5),
            increment: Duration::from_secs(5),
            max_delay: Duration::from_secs(60),
        }
    }

    /// Validate the retry strategy configuration
    ///
    /// Returns an error if the configuration is invalid
    pub fn validate(&self) -> Result<(), String> {
        match self {
            RetryStrategy::Exponential {
                base_delay,
                max_delay,
                multiplier,
            } => {
                if base_delay.is_zero() {
                    return Err("Exponential base_delay must be greater than zero".to_string());
                }
                if max_delay.is_zero() {
                    return Err("Exponential max_delay must be greater than zero".to_string());
                }
                if *multiplier <= 0.0 {
                    return Err("Exponential multiplier must be greater than zero".to_string());
                }
                if base_delay > max_delay {
                    return Err("Exponential base_delay cannot exceed max_delay".to_string());
                }
                Ok(())
            }
            RetryStrategy::Linear {
                initial_delay,
                increment,
                max_delay,
            } => {
                if initial_delay.is_zero() {
                    return Err("Linear initial_delay must be greater than zero".to_string());
                }
                if max_delay.is_zero() {
                    return Err("Linear max_delay must be greater than zero".to_string());
                }
                if increment.is_zero() {
                    return Err("Linear increment must be greater than zero".to_string());
                }
                if initial_delay > max_delay {
                    return Err("Linear initial_delay cannot exceed max_delay".to_string());
                }
                Ok(())
            }
            RetryStrategy::Fixed { delay } => {
                if delay.is_zero() {
                    return Err("Fixed delay must be greater than zero".to_string());
                }
                Ok(())
            }
            RetryStrategy::Custom(_) => {
                // Cannot validate custom strategies at config time
                Ok(())
            }
        }
    }

    /// Check if this strategy is aggressive (fast retries)
    pub fn is_aggressive(&self) -> bool {
        match self {
            RetryStrategy::Exponential { base_delay, .. } => base_delay.as_millis() < 1000,
            RetryStrategy::Linear { initial_delay, .. } => initial_delay.as_millis() < 1000,
            RetryStrategy::Fixed { delay } => delay.as_millis() < 1000,
            RetryStrategy::Custom(_) => false, // Cannot determine for custom
        }
    }

    /// Check if this strategy is conservative (slow retries)
    pub fn is_conservative(&self) -> bool {
        match self {
            RetryStrategy::Exponential { base_delay, .. } => base_delay.as_secs() >= 5,
            RetryStrategy::Linear { initial_delay, .. } => initial_delay.as_secs() >= 5,
            RetryStrategy::Fixed { delay } => delay.as_secs() >= 5,
            RetryStrategy::Custom(_) => false, // Cannot determine for custom
        }
    }
}

impl std::fmt::Debug for RetryStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetryStrategy::Exponential {
                base_delay,
                max_delay,
                multiplier,
            } => f
                .debug_struct("Exponential")
                .field("base_delay", base_delay)
                .field("max_delay", max_delay)
                .field("multiplier", multiplier)
                .finish(),
            RetryStrategy::Linear {
                initial_delay,
                increment,
                max_delay,
            } => f
                .debug_struct("Linear")
                .field("initial_delay", initial_delay)
                .field("increment", increment)
                .field("max_delay", max_delay)
                .finish(),
            RetryStrategy::Fixed { delay } => {
                f.debug_struct("Fixed").field("delay", delay).finish()
            }
            RetryStrategy::Custom(_) => f.debug_struct("Custom").finish_non_exhaustive(),
        }
    }
}

impl Default for RetryStrategy {
    fn default() -> Self {
        Self::default_exponential()
    }
}

impl std::fmt::Display for RetryStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RetryStrategy::Exponential {
                base_delay,
                max_delay,
                multiplier,
            } => write!(
                f,
                "Exponential[base={}ms, max={}ms, mult={}]",
                base_delay.as_millis(),
                max_delay.as_millis(),
                multiplier
            ),
            RetryStrategy::Linear {
                initial_delay,
                increment,
                max_delay,
            } => write!(
                f,
                "Linear[initial={}ms, inc={}ms, max={}ms]",
                initial_delay.as_millis(),
                increment.as_millis(),
                max_delay.as_millis()
            ),
            RetryStrategy::Fixed { delay } => write!(f, "Fixed[delay={}ms]", delay.as_millis()),
            RetryStrategy::Custom(_) => write!(f, "Custom"),
        }
    }
}

/// Full retry configuration including strategy and limits
#[derive(Debug, Clone)]
pub struct RetryConfig {
    /// Maximum number of retry attempts
    pub max_retries: u32,
    /// Retry strategy to use
    pub strategy: RetryStrategy,
    /// Whether to add jitter to retry delays (reduces thundering herd)
    pub jitter: bool,
    /// Jitter range as a fraction of the delay (0.0 - 1.0)
    /// For example, 0.1 means +/- 10% random variation
    pub jitter_fraction: f64,
}

impl RetryConfig {
    /// Create a new retry configuration
    pub fn new(max_retries: u32, strategy: RetryStrategy) -> Self {
        Self {
            max_retries,
            strategy,
            jitter: false,
            jitter_fraction: 0.1,
        }
    }

    /// Enable jitter to prevent thundering herd
    pub fn with_jitter(mut self, enabled: bool) -> Self {
        self.jitter = enabled;
        self
    }

    /// Set the jitter fraction (0.0 - 1.0)
    pub fn with_jitter_fraction(mut self, fraction: f64) -> Self {
        self.jitter_fraction = fraction.clamp(0.0, 1.0);
        self
    }

    /// Calculate the delay for a given retry attempt with optional jitter
    pub fn calculate_delay(&self, retry_count: u32) -> Duration {
        let base_delay = self.strategy.calculate_delay(retry_count);

        if self.jitter && self.jitter_fraction > 0.0 {
            self.apply_jitter(base_delay)
        } else {
            base_delay
        }
    }

    /// Apply jitter to a delay
    fn apply_jitter(&self, delay: Duration) -> Duration {
        use rand::RngExt;
        let mut rng = rand::rng();

        // Calculate jitter range
        let delay_ms = delay.as_millis() as f64;
        let jitter_range = delay_ms * self.jitter_fraction;

        // Apply random jitter in range [-jitter_range, +jitter_range]
        let jitter = rng.random_range(-jitter_range..=jitter_range);
        let final_delay_ms = (delay_ms + jitter).max(0.0);

        Duration::from_millis(final_delay_ms as u64)
    }

    /// Validate the retry configuration
    pub fn validate(&self) -> Result<(), String> {
        self.strategy.validate()?;

        if self.jitter_fraction < 0.0 || self.jitter_fraction > 1.0 {
            return Err("Jitter fraction must be between 0.0 and 1.0".to_string());
        }

        Ok(())
    }

    /// Check if retries should continue for the given attempt
    pub fn should_retry(&self, retry_count: u32) -> bool {
        retry_count < self.max_retries
    }
}

impl Default for RetryConfig {
    fn default() -> Self {
        Self {
            max_retries: 3,
            strategy: RetryStrategy::default(),
            jitter: false,
            jitter_fraction: 0.1,
        }
    }
}

impl std::fmt::Display for RetryConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "RetryConfig[max={}, strategy={}, jitter={}",
            self.max_retries, self.strategy, self.jitter
        )?;
        if self.jitter {
            write!(f, " ({}%)", (self.jitter_fraction * 100.0) as u32)?;
        }
        write!(f, "]")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exponential_backoff() {
        let strategy = RetryStrategy::Exponential {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        };

        assert_eq!(strategy.calculate_delay(0), Duration::from_secs(1));
        assert_eq!(strategy.calculate_delay(1), Duration::from_secs(2));
        assert_eq!(strategy.calculate_delay(2), Duration::from_secs(4));
        assert_eq!(strategy.calculate_delay(3), Duration::from_secs(8));
        assert_eq!(strategy.calculate_delay(4), Duration::from_secs(16));
        assert_eq!(strategy.calculate_delay(5), Duration::from_secs(32));
        assert_eq!(strategy.calculate_delay(6), Duration::from_secs(60)); // capped
        assert_eq!(strategy.calculate_delay(10), Duration::from_secs(60)); // capped
    }

    #[test]
    fn test_linear_backoff() {
        let strategy = RetryStrategy::Linear {
            initial_delay: Duration::from_secs(1),
            increment: Duration::from_secs(2),
            max_delay: Duration::from_secs(10),
        };

        assert_eq!(strategy.calculate_delay(0), Duration::from_secs(1));
        assert_eq!(strategy.calculate_delay(1), Duration::from_secs(3));
        assert_eq!(strategy.calculate_delay(2), Duration::from_secs(5));
        assert_eq!(strategy.calculate_delay(3), Duration::from_secs(7));
        assert_eq!(strategy.calculate_delay(4), Duration::from_secs(9));
        assert_eq!(strategy.calculate_delay(5), Duration::from_secs(10)); // capped
        assert_eq!(strategy.calculate_delay(10), Duration::from_secs(10)); // capped
    }

    #[test]
    fn test_fixed_delay() {
        let strategy = RetryStrategy::Fixed {
            delay: Duration::from_secs(5),
        };

        assert_eq!(strategy.calculate_delay(0), Duration::from_secs(5));
        assert_eq!(strategy.calculate_delay(1), Duration::from_secs(5));
        assert_eq!(strategy.calculate_delay(5), Duration::from_secs(5));
        assert_eq!(strategy.calculate_delay(100), Duration::from_secs(5));
    }

    #[test]
    fn test_custom_strategy() {
        let strategy = RetryStrategy::Custom(Arc::new(|retry_count| {
            Duration::from_secs((retry_count + 1) as u64 * 3)
        }));

        assert_eq!(strategy.calculate_delay(0), Duration::from_secs(3));
        assert_eq!(strategy.calculate_delay(1), Duration::from_secs(6));
        assert_eq!(strategy.calculate_delay(2), Duration::from_secs(9));
    }

    #[test]
    fn test_strategy_validation() {
        // Valid exponential
        let strategy = RetryStrategy::Exponential {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        };
        assert!(strategy.validate().is_ok());

        // Invalid exponential - zero base_delay
        let strategy = RetryStrategy::Exponential {
            base_delay: Duration::ZERO,
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        };
        assert!(strategy.validate().is_err());

        // Invalid exponential - base > max
        let strategy = RetryStrategy::Exponential {
            base_delay: Duration::from_secs(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        };
        assert!(strategy.validate().is_err());

        // Valid linear
        let strategy = RetryStrategy::Linear {
            initial_delay: Duration::from_secs(1),
            increment: Duration::from_secs(1),
            max_delay: Duration::from_secs(10),
        };
        assert!(strategy.validate().is_ok());

        // Invalid linear - zero increment
        let strategy = RetryStrategy::Linear {
            initial_delay: Duration::from_secs(1),
            increment: Duration::ZERO,
            max_delay: Duration::from_secs(10),
        };
        assert!(strategy.validate().is_err());

        // Valid fixed
        let strategy = RetryStrategy::Fixed {
            delay: Duration::from_secs(5),
        };
        assert!(strategy.validate().is_ok());

        // Invalid fixed - zero delay
        let strategy = RetryStrategy::Fixed {
            delay: Duration::ZERO,
        };
        assert!(strategy.validate().is_err());
    }

    #[test]
    fn test_strategy_predicates() {
        let aggressive = RetryStrategy::Exponential {
            base_delay: Duration::from_millis(500),
            max_delay: Duration::from_secs(10),
            multiplier: 2.0,
        };
        assert!(aggressive.is_aggressive());
        assert!(!aggressive.is_conservative());

        let conservative = RetryStrategy::Linear {
            initial_delay: Duration::from_secs(10),
            increment: Duration::from_secs(5),
            max_delay: Duration::from_secs(60),
        };
        assert!(!conservative.is_aggressive());
        assert!(conservative.is_conservative());
    }

    #[test]
    fn test_retry_config_should_retry() {
        let config = RetryConfig {
            max_retries: 3,
            strategy: RetryStrategy::default(),
            jitter: false,
            jitter_fraction: 0.1,
        };

        assert!(config.should_retry(0));
        assert!(config.should_retry(1));
        assert!(config.should_retry(2));
        assert!(!config.should_retry(3));
        assert!(!config.should_retry(4));
    }

    #[test]
    fn test_retry_config_with_jitter() {
        let config = RetryConfig::new(5, RetryStrategy::default())
            .with_jitter(true)
            .with_jitter_fraction(0.2);

        assert_eq!(config.max_retries, 5);
        assert!(config.jitter);
        assert_eq!(config.jitter_fraction, 0.2);

        // Jitter should produce delays close to the base delay but with variation
        let base_delay = config.strategy.calculate_delay(0);
        let jittered_delay = config.calculate_delay(0);

        // Should be within jitter range (base +/- 20%)
        let base_ms = base_delay.as_millis() as i64;
        let jittered_ms = jittered_delay.as_millis() as i64;
        let diff = (jittered_ms - base_ms).abs();
        let max_diff = (base_ms as f64 * 0.2) as i64;

        assert!(
            diff <= max_diff,
            "Jitter difference {} exceeds max {}",
            diff,
            max_diff
        );
    }

    #[test]
    fn test_retry_config_validation() {
        let config = RetryConfig::default();
        assert!(config.validate().is_ok());

        let config = RetryConfig {
            jitter_fraction: 1.5,
            ..Default::default()
        };
        assert!(config.validate().is_err());

        let config = RetryConfig {
            jitter_fraction: -0.1,
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_default_strategies() {
        let exp = RetryStrategy::default_exponential();
        assert_eq!(exp.calculate_delay(0), Duration::from_secs(1));

        let fast = RetryStrategy::fast_exponential();
        assert_eq!(fast.calculate_delay(0), Duration::from_millis(100));

        let slow = RetryStrategy::slow_linear();
        assert_eq!(slow.calculate_delay(0), Duration::from_secs(5));
    }

    #[test]
    fn test_strategy_display() {
        let exp = RetryStrategy::Exponential {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        };
        assert!(format!("{}", exp).contains("Exponential"));

        let lin = RetryStrategy::Linear {
            initial_delay: Duration::from_secs(1),
            increment: Duration::from_secs(2),
            max_delay: Duration::from_secs(30),
        };
        assert!(format!("{}", lin).contains("Linear"));

        let fixed = RetryStrategy::Fixed {
            delay: Duration::from_secs(5),
        };
        assert!(format!("{}", fixed).contains("Fixed"));
    }

    #[test]
    fn test_retry_config_display() {
        let config = RetryConfig::default();
        let display = format!("{}", config);
        assert!(display.contains("RetryConfig"));
        assert!(display.contains("max=3"));

        let config = RetryConfig::new(5, RetryStrategy::default())
            .with_jitter(true)
            .with_jitter_fraction(0.15);
        let display = format!("{}", config);
        assert!(display.contains("jitter=true"));
        assert!(display.contains("15%"));
    }
}
