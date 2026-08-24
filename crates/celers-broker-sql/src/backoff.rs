//! Overflow-safe retry backoff computation.
//!
//! The previous formula was `2_i64.pow(retry_count as u32).min(3600)`. `.min`
//! runs *after* the exponentiation, so it clamps nothing: `i64::pow` panics on
//! overflow in both debug and release profiles, and `retry_count` is bounded
//! only by the caller-supplied `max_retries`. A task enqueued with
//! `max_retries >= 64` therefore panicked the worker mid-`reject`, leaving the
//! row stuck in `processing`. A negative `retry_count` (corrupted or manually
//! edited row) wrapped through `as u32` into a huge exponent and panicked on
//! the first retry.

/// Upper bound on a single retry delay (1 hour).
pub(crate) const MAX_BACKOFF_SECONDS: i64 = 3600;

/// Largest exponent worth computing: `1 << 12 == 4096`, already above
/// [`MAX_BACKOFF_SECONDS`], and far below the `1 << 62` overflow edge.
const MAX_BACKOFF_EXPONENT: i32 = 12;

/// Exponential backoff in seconds for a task that has been retried
/// `retry_count` times, clamped to `[1, MAX_BACKOFF_SECONDS]`.
///
/// The exponent is clamped *before* the shift, so no input — including a
/// negative or absurdly large `retry_count` — can overflow or panic.
pub(crate) fn retry_backoff_seconds(retry_count: i32) -> i64 {
    let exponent = retry_count.clamp(0, MAX_BACKOFF_EXPONENT) as u32;
    (1_i64 << exponent).min(MAX_BACKOFF_SECONDS)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_doubles_over_the_early_retries() {
        assert_eq!(retry_backoff_seconds(0), 1);
        assert_eq!(retry_backoff_seconds(1), 2);
        assert_eq!(retry_backoff_seconds(2), 4);
        assert_eq!(retry_backoff_seconds(10), 1024);
    }

    #[test]
    fn backoff_is_clamped_to_one_hour() {
        assert_eq!(retry_backoff_seconds(11), 2048);
        assert_eq!(retry_backoff_seconds(12), MAX_BACKOFF_SECONDS);
        assert_eq!(retry_backoff_seconds(13), MAX_BACKOFF_SECONDS);
    }

    /// `2_i64.pow(64)` panicked here.
    #[test]
    fn large_retry_count_does_not_panic() {
        assert_eq!(retry_backoff_seconds(64), MAX_BACKOFF_SECONDS);
        assert_eq!(retry_backoff_seconds(i32::MAX), MAX_BACKOFF_SECONDS);
    }

    /// A negative column value wrapped through `as u32` and panicked here.
    #[test]
    fn negative_retry_count_does_not_panic() {
        assert_eq!(retry_backoff_seconds(-1), 1);
        assert_eq!(retry_backoff_seconds(i32::MIN), 1);
    }

    #[test]
    fn backoff_is_always_within_bounds() {
        for retry_count in -100..1000 {
            let seconds = retry_backoff_seconds(retry_count);
            assert!(
                (1..=MAX_BACKOFF_SECONDS).contains(&seconds),
                "retry_count {retry_count} produced out-of-range backoff {seconds}"
            );
        }
    }
}
