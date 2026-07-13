//! Rate-limit-aware execution planning for Canvas fan-out.
//!
//! When a [`Group`] (or the head of a [`Chain`]) fans a workflow out into `N`
//! members, dispatching them all at once can blow straight through a configured
//! rate limit. This module derives *staggered dispatch delays* (countdowns) from
//! a [`RateLimitConfig`] so that enqueuing `N` members respects the configured
//! tasks-per-second rate.
//!
//! It is the rate-derived analogue of [`Group::skew`](crate::Group::skew): where
//! `skew` spaces members by a *fixed* step, the helpers here space members by
//! `1 / rate` and honour the bucket's burst capacity.
//!
//! # Token-bucket-derived schedule
//!
//! The schedule mirrors the semantics of [`celers_core::TokenBucket`]: the bucket
//! starts full with [`RateLimitConfig::effective_burst`] tokens, then refills at
//! `rate` tokens per second.
//!
//! For member index `i` (0-based) and a bucket of `burst` tokens refilling at
//! `rate`/sec, the dispatch delay is:
//!
//! ```text
//! delay(i) = 0                        if i < burst   (covered by the initial burst)
//! delay(i) = (i - burst + 1) / rate   if i >= burst  (must wait for a refill)
//! ```
//!
//! This is:
//!
//! - **monotonic** — `delay(i) <= delay(i + 1)`,
//! - **rate-respecting** — past the burst, consecutive members are spaced by
//!   exactly `1 / rate` seconds,
//! - **zero-first** — `delay(0) == 0` (the first member always dispatches
//!   immediately), and
//! - **deterministic** — it depends only on `i`, `rate`, and `burst` (no clock,
//!   no randomness).
//!
//! # Example
//!
//! ```
//! use celers_canvas::rate_limited_countdowns;
//! use celers_core::RateLimitConfig;
//! use std::time::Duration;
//!
//! // 10 tasks/sec, burst of 1 => strictly serial 1/10s spacing.
//! let config = RateLimitConfig::new(10.0).with_burst(1);
//! let delays = rate_limited_countdowns(4, &config);
//!
//! assert_eq!(delays[0], Duration::ZERO);
//! assert_eq!(delays[1], Duration::from_millis(100));
//! assert_eq!(delays[2], Duration::from_millis(200));
//! assert_eq!(delays[3], Duration::from_millis(300));
//! ```

use crate::Group;
use celers_core::RateLimitConfig;
use std::time::Duration;

/// Compute the per-member, token-bucket-derived dispatch delays for a fan-out of
/// `n` members under the given rate limit.
///
/// Returns a vector of `n` [`Duration`]s where element `i` is the countdown to
/// apply to the `i`-th member so that, collectively, dispatching all `n` members
/// respects `config`'s tasks-per-second rate (with an initial burst).
///
/// The schedule is the deterministic token-bucket schedule documented at the
/// module level: the first [`RateLimitConfig::effective_burst`] members
/// get a zero delay, and each member beyond the burst is spaced by `1 / rate`
/// seconds from the previous one.
///
/// # Edge cases
///
/// - `n == 0` returns an empty vector.
/// - A non-positive or non-finite `rate` has no meaningful spacing, so every
///   member gets [`Duration::ZERO`] (dispatch immediately). This keeps the
///   function total and panic-free.
/// - When `n <= effective_burst` (under capacity) every member gets
///   [`Duration::ZERO`]: the burst absorbs the whole fan-out.
///
/// # Determinism
///
/// The result is a pure function of `n` and `config`; it never reads the clock
/// or any source of randomness.
///
/// # Example
///
/// ```
/// use celers_canvas::rate_limited_countdowns;
/// use celers_core::RateLimitConfig;
/// use std::time::Duration;
///
/// // 5 tasks/sec, burst of 2: first two are immediate, then 1/5s spacing.
/// let config = RateLimitConfig::new(5.0).with_burst(2);
/// let delays = rate_limited_countdowns(5, &config);
///
/// assert_eq!(delays[0], Duration::ZERO);
/// assert_eq!(delays[1], Duration::ZERO);
/// assert_eq!(delays[2], Duration::from_millis(200));
/// assert_eq!(delays[3], Duration::from_millis(400));
/// assert_eq!(delays[4], Duration::from_millis(600));
/// ```
#[must_use]
pub fn rate_limited_countdowns(n: usize, config: &RateLimitConfig) -> Vec<Duration> {
    if n == 0 {
        return Vec::new();
    }

    let rate = config.rate;
    // A non-positive or non-finite rate gives no usable spacing; dispatch all
    // members immediately rather than producing NaN/infinite delays.
    if !rate.is_finite() || rate <= 0.0 {
        return vec![Duration::ZERO; n];
    }

    let burst = config.effective_burst() as usize;
    let mut delays = Vec::with_capacity(n);
    for i in 0..n {
        if i < burst {
            delays.push(Duration::ZERO);
        } else {
            // Number of refills that must have happened for token `i` to exist.
            let refills = (i - burst + 1) as f64;
            let seconds = refills / rate;
            delays.push(Duration::from_secs_f64(seconds));
        }
    }
    delays
}

/// Compute rate-derived dispatch delays as **whole seconds** of countdown.
///
/// This is the integer-second projection of [`rate_limited_countdowns`], matching
/// the granularity of [`Signature::options.countdown`](crate::TaskOptions), which
/// is stored as `u64` seconds. Each [`Duration`] is rounded *down* to whole
/// seconds — the same truncating behaviour as [`Group::skew`].
///
/// Sub-second spacing (e.g. a `10`/sec rate, whose spacing is `0.1s`) therefore
/// collapses to `0`-second countdowns until a full second of delay has
/// accumulated. Use [`rate_limited_countdowns`] directly when you need
/// sub-second precision.
///
/// # Example
///
/// ```
/// use celers_canvas::rate_limited_countdown_secs;
/// use celers_core::RateLimitConfig;
///
/// // 1 task/sec, burst of 1 => 0,1,2,3 seconds.
/// let config = RateLimitConfig::new(1.0).with_burst(1);
/// assert_eq!(rate_limited_countdown_secs(4, &config), vec![0, 1, 2, 3]);
/// ```
#[must_use]
pub fn rate_limited_countdown_secs(n: usize, config: &RateLimitConfig) -> Vec<u64> {
    rate_limited_countdowns(n, config)
        .into_iter()
        .map(|d| d.as_secs())
        .collect()
}

impl Group {
    /// Stagger this group's members using a rate-derived schedule.
    ///
    /// Each member's [`countdown`](crate::TaskOptions::countdown) is set from
    /// [`rate_limited_countdown_secs`] so that dispatching the whole group
    /// respects `config`'s tasks-per-second rate (with an initial burst). This is
    /// the rate-derived counterpart to [`Group::skew`] and composes with the
    /// existing [`apply`](Group::apply) machinery: the per-member countdowns are
    /// the same `options.countdown` field `apply` already honours.
    ///
    /// Because `options.countdown` is whole seconds, sub-second spacing is
    /// truncated (see [`rate_limited_countdown_secs`]). For sub-second precision
    /// in planning, call [`Group::rate_limited_countdowns`] (which does not lose
    /// precision) and schedule with [`Duration`] yourself.
    ///
    /// This is additive and consuming, like [`Group::skew`]; it never changes the
    /// set of members, only their countdowns.
    ///
    /// # Example
    ///
    /// ```
    /// use celers_canvas::Group;
    /// use celers_core::RateLimitConfig;
    ///
    /// // 1 task/sec, burst 1: members staggered 0s, 1s, 2s.
    /// let config = RateLimitConfig::new(1.0).with_burst(1);
    /// let group = Group::new()
    ///     .add("task1", vec![])
    ///     .add("task2", vec![])
    ///     .add("task3", vec![])
    ///     .with_rate_limit(&config);
    ///
    /// assert_eq!(group.tasks[0].options.countdown, Some(0));
    /// assert_eq!(group.tasks[1].options.countdown, Some(1));
    /// assert_eq!(group.tasks[2].options.countdown, Some(2));
    /// ```
    #[must_use]
    pub fn with_rate_limit(mut self, config: &RateLimitConfig) -> Self {
        let countdowns = rate_limited_countdown_secs(self.tasks.len(), config);
        for (task, countdown) in self.tasks.iter_mut().zip(countdowns) {
            task.options.countdown = Some(countdown);
        }
        self
    }

    /// Compute the sub-second-precise rate-derived dispatch delays for this
    /// group's members without mutating it.
    ///
    /// This is a thin convenience wrapper over [`rate_limited_countdowns`] using
    /// the group's current member count. Unlike [`Group::with_rate_limit`], it
    /// preserves sub-second spacing, so it is the right tool when you intend to
    /// schedule the members with [`Duration`] precision rather than the
    /// whole-second `options.countdown` field.
    ///
    /// # Example
    ///
    /// ```
    /// use celers_canvas::Group;
    /// use celers_core::RateLimitConfig;
    /// use std::time::Duration;
    ///
    /// let config = RateLimitConfig::new(10.0).with_burst(1);
    /// let group = Group::new()
    ///     .add("a", vec![])
    ///     .add("b", vec![])
    ///     .add("c", vec![]);
    ///
    /// let delays = group.rate_limited_countdowns(&config);
    /// assert_eq!(delays[0], Duration::ZERO);
    /// assert_eq!(delays[1], Duration::from_millis(100));
    /// assert_eq!(delays[2], Duration::from_millis(200));
    /// ```
    #[must_use]
    pub fn rate_limited_countdowns(&self, config: &RateLimitConfig) -> Vec<Duration> {
        rate_limited_countdowns(self.tasks.len(), config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Group;
    use celers_core::RateLimitConfig;

    /// Helper: build a config with an explicit rate and burst.
    fn cfg(rate: f64, burst: u32) -> RateLimitConfig {
        RateLimitConfig::new(rate).with_burst(burst)
    }

    #[test]
    fn ten_per_second_burst_one_is_serial_and_spaced() {
        // Rate 10/sec, burst 1 => fully serial dispatch, spaced by 1/10s.
        let config = cfg(10.0, 1);
        let delays = rate_limited_countdowns(5, &config);

        assert_eq!(delays.len(), 5);
        // First is zero.
        assert_eq!(delays[0], Duration::ZERO);
        // Spaced by exactly 1/rate = 100ms.
        for (i, delay) in delays.iter().enumerate().skip(1) {
            assert_eq!(*delay, Duration::from_millis(100 * i as u64));
        }
    }

    #[test]
    fn schedule_is_monotonic() {
        let config = cfg(7.0, 3);
        let delays = rate_limited_countdowns(20, &config);
        for window in delays.windows(2) {
            assert!(
                window[1] >= window[0],
                "delays must be non-decreasing: {:?} then {:?}",
                window[0],
                window[1]
            );
        }
    }

    #[test]
    fn consecutive_spacing_is_one_over_rate_past_burst() {
        // Past the burst, every gap must equal 1/rate.
        let rate = 4.0;
        let burst = 2u32;
        let config = cfg(rate, burst);
        let delays = rate_limited_countdowns(10, &config);
        let expected_gap = Duration::from_secs_f64(1.0 / rate);

        for i in (burst as usize + 1)..delays.len() {
            let gap = delays[i] - delays[i - 1];
            // Allow a 1ns tolerance for float -> Duration conversion.
            let diff = gap.abs_diff(expected_gap);
            assert!(
                diff <= Duration::from_nanos(1),
                "gap {gap:?} should equal {expected_gap:?}"
            );
        }
    }

    #[test]
    fn first_member_is_always_zero() {
        for &(rate, burst, n) in &[(1.0, 1, 1), (10.0, 1, 8), (3.5, 5, 12), (100.0, 50, 2)] {
            let config = cfg(rate, burst);
            let delays = rate_limited_countdowns(n, &config);
            assert_eq!(delays[0], Duration::ZERO, "first delay must be zero");
        }
    }

    #[test]
    fn deterministic_across_calls() {
        let config = cfg(13.0, 4);
        let a = rate_limited_countdowns(50, &config);
        let b = rate_limited_countdowns(50, &config);
        assert_eq!(a, b, "identical inputs must give identical schedules");
    }

    #[test]
    fn zero_members_is_empty() {
        let config = cfg(10.0, 1);
        assert!(rate_limited_countdowns(0, &config).is_empty());
        assert!(rate_limited_countdown_secs(0, &config).is_empty());
    }

    #[test]
    fn under_capacity_all_zero() {
        // n <= burst: the burst absorbs the whole fan-out, all delays zero.
        let config = cfg(2.0, 10);
        let delays = rate_limited_countdowns(10, &config);
        assert!(delays.iter().all(|d| *d == Duration::ZERO));

        // Exactly at capacity is still all-zero.
        let exactly = rate_limited_countdowns(10, &cfg(2.0, 10));
        assert!(exactly.iter().all(|d| *d == Duration::ZERO));
    }

    #[test]
    fn over_capacity_only_excess_is_delayed() {
        // burst 3, n 6: first 3 immediate, then 3 spaced refills.
        let rate = 5.0;
        let config = cfg(rate, 3);
        let delays = rate_limited_countdowns(6, &config);
        assert_eq!(delays[0], Duration::ZERO);
        assert_eq!(delays[1], Duration::ZERO);
        assert_eq!(delays[2], Duration::ZERO);
        assert_eq!(delays[3], Duration::from_secs_f64(1.0 / rate));
        assert_eq!(delays[4], Duration::from_secs_f64(2.0 / rate));
        assert_eq!(delays[5], Duration::from_secs_f64(3.0 / rate));
    }

    #[test]
    fn non_positive_rate_dispatches_immediately() {
        // Zero and negative rates have no meaningful spacing.
        for rate in [0.0, -1.0, -100.0] {
            let config = RateLimitConfig::new(rate).with_burst(1);
            let delays = rate_limited_countdowns(5, &config);
            assert_eq!(delays.len(), 5);
            assert!(
                delays.iter().all(|d| *d == Duration::ZERO),
                "non-positive rate {rate} must yield all-zero delays"
            );
        }
    }

    #[test]
    fn non_finite_rate_dispatches_immediately() {
        for rate in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let config = RateLimitConfig::new(rate).with_burst(1);
            let delays = rate_limited_countdowns(4, &config);
            assert!(delays.iter().all(|d| *d == Duration::ZERO));
        }
    }

    #[test]
    fn default_burst_follows_ceil_of_rate() {
        // No explicit burst => effective_burst == ceil(rate).
        // rate 2.0 => burst 2, so first two are immediate.
        let config = RateLimitConfig::new(2.0);
        assert_eq!(config.effective_burst(), 2);
        let delays = rate_limited_countdowns(4, &config);
        assert_eq!(delays[0], Duration::ZERO);
        assert_eq!(delays[1], Duration::ZERO);
        assert_eq!(delays[2], Duration::from_secs_f64(0.5));
        assert_eq!(delays[3], Duration::from_secs_f64(1.0));
    }

    #[test]
    fn whole_second_projection_truncates() {
        // 1/sec, burst 1 => clean whole seconds 0,1,2,3.
        let config = cfg(1.0, 1);
        assert_eq!(rate_limited_countdown_secs(4, &config), vec![0, 1, 2, 3]);

        // 10/sec, burst 1 => 0.0,0.1,0.2,... all truncate to 0 until 1s.
        let fast = cfg(10.0, 1);
        let secs = rate_limited_countdown_secs(12, &fast);
        // Indices 0..=10 are < 1.0s, index 10 is exactly 1.0s.
        assert_eq!(secs[0], 0);
        assert_eq!(secs[9], 0); // 0.9s
        assert_eq!(secs[10], 1); // 1.0s
        assert_eq!(secs[11], 1); // 1.1s
    }

    #[test]
    fn group_with_rate_limit_sets_countdowns() {
        let config = cfg(1.0, 1);
        let group = Group::new()
            .add("task1", vec![])
            .add("task2", vec![])
            .add("task3", vec![])
            .with_rate_limit(&config);

        assert_eq!(group.tasks[0].options.countdown, Some(0));
        assert_eq!(group.tasks[1].options.countdown, Some(1));
        assert_eq!(group.tasks[2].options.countdown, Some(2));
    }

    #[test]
    fn group_with_rate_limit_matches_skew_for_unit_rate() {
        // Rate 1/sec with burst 1 is exactly skew(0, 1) at whole-second granularity.
        let config = cfg(1.0, 1);
        let rate_group = Group::new()
            .add("a", vec![])
            .add("b", vec![])
            .add("c", vec![])
            .add("d", vec![])
            .with_rate_limit(&config);
        let skew_group = Group::new()
            .add("a", vec![])
            .add("b", vec![])
            .add("c", vec![])
            .add("d", vec![])
            .skew(0.0, 1.0);

        let rate_counts: Vec<_> = rate_group
            .tasks
            .iter()
            .map(|t| t.options.countdown)
            .collect();
        let skew_counts: Vec<_> = skew_group
            .tasks
            .iter()
            .map(|t| t.options.countdown)
            .collect();
        assert_eq!(rate_counts, skew_counts);
    }

    #[test]
    fn group_with_rate_limit_empty_is_noop() {
        let config = cfg(10.0, 1);
        let group = Group::new().with_rate_limit(&config);
        assert!(group.is_empty());
    }

    #[test]
    fn group_rate_limited_countdowns_uses_member_count() {
        let config = cfg(10.0, 1);
        let group = Group::new()
            .add("a", vec![])
            .add("b", vec![])
            .add("c", vec![]);
        let delays = group.rate_limited_countdowns(&config);
        assert_eq!(delays.len(), 3);
        assert_eq!(delays[0], Duration::ZERO);
        assert_eq!(delays[1], Duration::from_millis(100));
        assert_eq!(delays[2], Duration::from_millis(200));
    }

    #[test]
    fn group_rate_limit_preserves_membership() {
        let config = cfg(5.0, 2);
        let group = Group::new()
            .add("alpha", vec![])
            .add("beta", vec![])
            .with_rate_limit(&config);
        assert_eq!(group.task_names(), vec!["alpha", "beta"]);
        // group_id is untouched by rate-limit staggering.
        assert!(group.has_group_id());
    }
}
