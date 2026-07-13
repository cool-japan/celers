//! Adaptive polling intervals for the worker dequeue/poll loop.
//!
//! When a worker polls an empty queue it should not hammer the broker at a
//! fixed, aggressive cadence. Instead, this module provides a deterministic
//! poll-interval *controller* that:
//!
//! - **Backs off** (increases the interval, bounded by a configured maximum)
//!   when the queue is repeatedly empty, and
//! - **Speeds up** (decreases the interval toward a configured minimum) as soon
//!   as work is found, more aggressively when the observed queue depth is high.
//!
//! The decision math is **deterministic given its inputs** — it never reads a
//! clock. The wall-clock [`std::time::Duration`] that the loop should sleep for
//! is *returned* from [`AdaptivePoll::record`], but the controller's internal
//! transition only depends on the sequence of [`PollOutcome`] values fed to it
//! and the immutable [`AdaptivePollConfig`]. This makes the back-off / speed-up
//! behaviour exhaustively unit-testable without timing flakiness.
//!
//! # Example
//!
//! ```
//! use celers_worker::adaptive_poll::{AdaptivePoll, AdaptivePollConfig, PollOutcome};
//! use std::time::Duration;
//!
//! let config = AdaptivePollConfig {
//!     min_interval_ms: 50,
//!     max_interval_ms: 1_000,
//!     backoff_numerator: 2,
//!     backoff_denominator: 1,
//!     empty_streak_before_backoff: 1,
//!     high_depth_threshold: 8,
//! };
//! let mut poll = AdaptivePoll::new(config);
//!
//! // Repeated empties grow the interval (bounded by the max).
//! assert_eq!(poll.record(PollOutcome::Empty), Duration::from_millis(100));
//! assert_eq!(poll.record(PollOutcome::Empty), Duration::from_millis(200));
//!
//! // A shallow result eases the interval back down by halving (200 -> 100).
//! assert_eq!(poll.record(PollOutcome::found(1)), Duration::from_millis(100));
//!
//! // A deep result (>= high_depth_threshold) snaps straight to the minimum.
//! assert_eq!(poll.record(PollOutcome::found(8)), Duration::from_millis(50));
//! ```

use std::time::Duration;

/// How a single poll of the queue turned out.
///
/// This is the sole runtime input to the [`AdaptivePoll`] controller, which is
/// what keeps the interval math deterministic and clock-free.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollOutcome {
    /// The poll returned no work.
    Empty,
    /// The poll returned `depth` ready items (`depth >= 1`).
    Found {
        /// Number of items returned by the poll (observed queue depth lower bound).
        depth: usize,
    },
    /// The poll failed (broker/transport error). Treated as a back-off signal so
    /// a flapping broker is not hammered, but tracked separately for stats.
    Error,
}

impl PollOutcome {
    /// Construct a [`PollOutcome::Found`] for `depth` items.
    ///
    /// A `depth` of `0` is normalised to [`PollOutcome::Empty`] so callers can
    /// pass a raw count without a branch.
    #[inline]
    #[must_use]
    pub const fn found(depth: usize) -> Self {
        if depth == 0 {
            Self::Empty
        } else {
            Self::Found { depth }
        }
    }

    /// Whether this outcome represents a successful poll that returned work.
    #[inline]
    #[must_use]
    pub const fn is_found(&self) -> bool {
        matches!(self, Self::Found { .. })
    }

    /// Whether this outcome should drive the interval *upward* (back off).
    ///
    /// Both [`PollOutcome::Empty`] and [`PollOutcome::Error`] back off.
    #[inline]
    #[must_use]
    pub const fn is_backoff(&self) -> bool {
        matches!(self, Self::Empty | Self::Error)
    }

    /// The observed depth for a [`PollOutcome::Found`], else `0`.
    #[inline]
    #[must_use]
    pub const fn depth(&self) -> usize {
        match self {
            Self::Found { depth } => *depth,
            _ => 0,
        }
    }
}

/// Configuration for the [`AdaptivePoll`] controller.
///
/// The back-off growth factor is expressed as the rational
/// `backoff_numerator / backoff_denominator` (e.g. `2/1` doubles, `3/2` grows by
/// 50%) so the math stays in integer space and is fully deterministic — no
/// floating-point rounding to reason about across platforms.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptivePollConfig {
    /// Lower bound for the poll interval, in milliseconds. Also the value the
    /// controller starts at and collapses to when work is found at high depth.
    pub min_interval_ms: u64,

    /// Upper bound for the poll interval, in milliseconds. Back-off never
    /// produces a value above this.
    pub max_interval_ms: u64,

    /// Numerator of the multiplicative back-off factor (`>= denominator` for the
    /// interval to actually grow).
    pub backoff_numerator: u64,

    /// Denominator of the multiplicative back-off factor (must be non-zero).
    pub backoff_denominator: u64,

    /// Number of *consecutive* back-off outcomes that must be observed before
    /// the interval is actually grown. `1` means grow on the first empty poll;
    /// larger values keep the interval at the minimum through short idle blips.
    pub empty_streak_before_backoff: u32,

    /// Observed queue depth at or above which a successful poll snaps the
    /// interval all the way back to [`Self::min_interval_ms`]. Below this, a
    /// successful poll merely halves the interval (still bounded by the min) so
    /// the controller eases in rather than oscillating.
    pub high_depth_threshold: usize,
}

impl Default for AdaptivePollConfig {
    fn default() -> Self {
        Self {
            min_interval_ms: 50,
            max_interval_ms: 5_000,
            backoff_numerator: 2,
            backoff_denominator: 1,
            empty_streak_before_backoff: 1,
            high_depth_threshold: 16,
        }
    }
}

impl AdaptivePollConfig {
    /// Validate the configuration, returning a human-readable error on failure.
    ///
    /// # Errors
    ///
    /// Returns `Err` when the bounds are degenerate (zero/empty min, max below
    /// min), the back-off factor is not strictly growing, or the back-off
    /// denominator is zero.
    pub fn validate(&self) -> Result<(), String> {
        if self.min_interval_ms == 0 {
            return Err("min_interval_ms must be greater than 0".to_string());
        }
        if self.max_interval_ms < self.min_interval_ms {
            return Err("max_interval_ms must be >= min_interval_ms".to_string());
        }
        if self.backoff_denominator == 0 {
            return Err("backoff_denominator must be greater than 0".to_string());
        }
        if self.backoff_numerator <= self.backoff_denominator {
            return Err(
                "backoff_numerator must be > backoff_denominator for back-off to grow the interval"
                    .to_string(),
            );
        }
        if self.empty_streak_before_backoff == 0 {
            return Err("empty_streak_before_backoff must be greater than 0".to_string());
        }
        Ok(())
    }

    /// A configuration tuned for low-latency workloads: short min, modest max,
    /// double on each empty poll.
    #[must_use]
    pub fn low_latency() -> Self {
        Self {
            min_interval_ms: 10,
            max_interval_ms: 500,
            backoff_numerator: 2,
            backoff_denominator: 1,
            empty_streak_before_backoff: 1,
            high_depth_threshold: 8,
        }
    }

    /// A configuration tuned to minimise broker traffic on idle queues: longer
    /// max interval and gentler 1.5x growth.
    #[must_use]
    pub fn broker_friendly() -> Self {
        Self {
            min_interval_ms: 100,
            max_interval_ms: 30_000,
            backoff_numerator: 3,
            backoff_denominator: 2,
            empty_streak_before_backoff: 2,
            high_depth_threshold: 32,
        }
    }
}

/// Snapshot of [`AdaptivePoll`] state for observability.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AdaptivePollStats {
    /// The interval the controller will next instruct the loop to sleep for, in ms.
    pub current_interval_ms: u64,
    /// Length of the current run of consecutive back-off outcomes.
    pub consecutive_backoffs: u32,
    /// Total empty polls observed since construction/reset.
    pub total_empty: u64,
    /// Total error polls observed since construction/reset.
    pub total_errors: u64,
    /// Total successful (work-returning) polls observed since construction/reset.
    pub total_found: u64,
}

impl std::fmt::Display for AdaptivePollStats {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "AdaptivePollStats(interval={}ms, backoffs={}, empty={}, errors={}, found={})",
            self.current_interval_ms,
            self.consecutive_backoffs,
            self.total_empty,
            self.total_errors,
            self.total_found
        )
    }
}

/// Deterministic adaptive poll-interval controller.
///
/// Feed it one [`PollOutcome`] per loop iteration via [`AdaptivePoll::record`];
/// it returns the [`Duration`] the loop should sleep before polling again. See
/// the [module documentation](self) for the back-off / speed-up rules.
#[derive(Debug, Clone)]
pub struct AdaptivePoll {
    config: AdaptivePollConfig,
    current_ms: u64,
    consecutive_backoffs: u32,
    total_empty: u64,
    total_errors: u64,
    total_found: u64,
}

impl AdaptivePoll {
    /// Create a new controller starting at the configured minimum interval.
    ///
    /// The supplied configuration is sanitised: degenerate bounds are repaired
    /// (so construction is infallible and cannot panic), but callers wanting to
    /// surface misconfiguration should call [`AdaptivePollConfig::validate`]
    /// first.
    #[must_use]
    pub fn new(config: AdaptivePollConfig) -> Self {
        let config = Self::sanitize(config);
        Self {
            current_ms: config.min_interval_ms,
            consecutive_backoffs: 0,
            total_empty: 0,
            total_errors: 0,
            total_found: 0,
            config,
        }
    }

    /// Create a controller from an existing fixed poll interval.
    ///
    /// The fixed value becomes the minimum, the maximum is derived as a bounded
    /// multiple of it, and the controller starts at the minimum. This is the
    /// migration path for the worker's legacy `poll_interval_ms` field.
    #[must_use]
    pub fn from_fixed_interval(poll_interval_ms: u64) -> Self {
        let min = poll_interval_ms.max(1);
        // Allow growing up to 16x the fixed interval, capped to keep idle
        // back-off bounded even for already-large fixed intervals.
        let max = min.saturating_mul(16).min(60_000).max(min);
        Self::new(AdaptivePollConfig {
            min_interval_ms: min,
            max_interval_ms: max,
            backoff_numerator: 2,
            backoff_denominator: 1,
            empty_streak_before_backoff: 1,
            high_depth_threshold: 16,
        })
    }

    /// Repair a possibly-degenerate config so internal invariants always hold.
    fn sanitize(mut config: AdaptivePollConfig) -> AdaptivePollConfig {
        if config.min_interval_ms == 0 {
            config.min_interval_ms = 1;
        }
        if config.max_interval_ms < config.min_interval_ms {
            config.max_interval_ms = config.min_interval_ms;
        }
        if config.backoff_denominator == 0 {
            config.backoff_denominator = 1;
        }
        if config.backoff_numerator <= config.backoff_denominator {
            // Guarantee strictly-growing back-off; fall back to doubling.
            config.backoff_numerator = config.backoff_denominator.saturating_mul(2);
        }
        if config.empty_streak_before_backoff == 0 {
            config.empty_streak_before_backoff = 1;
        }
        config
    }

    /// Record the result of a poll and return the next sleep [`Duration`].
    ///
    /// This is the single state transition. It is deterministic: the returned
    /// duration is purely a function of `outcome`, the prior calls, and the
    /// immutable config — no clock is read.
    pub fn record(&mut self, outcome: PollOutcome) -> Duration {
        match outcome {
            PollOutcome::Found { depth } => {
                self.total_found = self.total_found.saturating_add(1);
                self.consecutive_backoffs = 0;
                self.current_ms = self.speed_up(depth);
            }
            PollOutcome::Empty => {
                self.total_empty = self.total_empty.saturating_add(1);
                self.on_backoff();
            }
            PollOutcome::Error => {
                self.total_errors = self.total_errors.saturating_add(1);
                self.on_backoff();
            }
        }
        self.current()
    }

    /// Apply a back-off step shared by [`PollOutcome::Empty`]/[`PollOutcome::Error`].
    fn on_backoff(&mut self) {
        self.consecutive_backoffs = self.consecutive_backoffs.saturating_add(1);
        if self.consecutive_backoffs >= self.config.empty_streak_before_backoff {
            self.current_ms = self.grow(self.current_ms);
        }
    }

    /// Grow `interval_ms` by the rational back-off factor, clamped to the max.
    ///
    /// Uses `u128` intermediates so very large intervals cannot overflow before
    /// the clamp is applied. Guarantees the interval strictly increases (by at
    /// least 1ms) until the maximum is reached, even if integer division would
    /// otherwise leave it unchanged for tiny values.
    fn grow(&self, interval_ms: u64) -> u64 {
        if interval_ms >= self.config.max_interval_ms {
            return self.config.max_interval_ms;
        }
        let grown = (u128::from(interval_ms) * u128::from(self.config.backoff_numerator))
            / u128::from(self.config.backoff_denominator);
        let grown = u64::try_from(grown).unwrap_or(u64::MAX);
        // Ensure forward progress despite integer truncation.
        let grown = grown.max(interval_ms.saturating_add(1));
        grown.min(self.config.max_interval_ms)
    }

    /// Compute the interval after a successful poll of the given `depth`.
    ///
    /// High depth snaps straight to the minimum (we are behind, poll hard); a
    /// shallow result halves the interval (eased re-engagement). Always bounded
    /// below by the minimum.
    fn speed_up(&self, depth: usize) -> u64 {
        if depth >= self.config.high_depth_threshold {
            self.config.min_interval_ms
        } else {
            let halved = self.current_ms / 2;
            halved.max(self.config.min_interval_ms)
        }
    }

    /// The interval the controller will next instruct the loop to sleep for.
    #[inline]
    #[must_use]
    pub fn current(&self) -> Duration {
        Duration::from_millis(self.current_ms)
    }

    /// The interval the controller will next sleep for, in milliseconds.
    #[inline]
    #[must_use]
    pub const fn current_ms(&self) -> u64 {
        self.current_ms
    }

    /// Length of the current run of consecutive back-off outcomes.
    #[inline]
    #[must_use]
    pub const fn consecutive_backoffs(&self) -> u32 {
        self.consecutive_backoffs
    }

    /// The controller's immutable configuration.
    #[inline]
    #[must_use]
    pub const fn config(&self) -> &AdaptivePollConfig {
        &self.config
    }

    /// Reset the interval to the minimum and clear the back-off streak.
    ///
    /// Cumulative counters in [`AdaptivePollStats`] are preserved; use
    /// [`AdaptivePoll::reset_stats`] to clear those too.
    pub fn reset(&mut self) {
        self.current_ms = self.config.min_interval_ms;
        self.consecutive_backoffs = 0;
    }

    /// Reset the cumulative outcome counters (does not touch the interval).
    pub fn reset_stats(&mut self) {
        self.total_empty = 0;
        self.total_errors = 0;
        self.total_found = 0;
    }

    /// Snapshot the current state for observability.
    #[must_use]
    pub const fn stats(&self) -> AdaptivePollStats {
        AdaptivePollStats {
            current_interval_ms: self.current_ms,
            consecutive_backoffs: self.consecutive_backoffs,
            total_empty: self.total_empty,
            total_errors: self.total_errors,
            total_found: self.total_found,
        }
    }
}

impl Default for AdaptivePoll {
    fn default() -> Self {
        Self::new(AdaptivePollConfig::default())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> AdaptivePollConfig {
        AdaptivePollConfig {
            min_interval_ms: 50,
            max_interval_ms: 1_000,
            backoff_numerator: 2,
            backoff_denominator: 1,
            empty_streak_before_backoff: 1,
            high_depth_threshold: 8,
        }
    }

    #[test]
    fn test_config_validate_ok() {
        assert!(cfg().validate().is_ok());
        assert!(AdaptivePollConfig::default().validate().is_ok());
        assert!(AdaptivePollConfig::low_latency().validate().is_ok());
        assert!(AdaptivePollConfig::broker_friendly().validate().is_ok());
    }

    #[test]
    fn test_config_validate_errors() {
        let mut c = cfg();
        c.min_interval_ms = 0;
        assert!(c.validate().is_err());

        let mut c = cfg();
        c.max_interval_ms = 10; // < min
        assert!(c.validate().is_err());

        let mut c = cfg();
        c.backoff_denominator = 0;
        assert!(c.validate().is_err());

        let mut c = cfg();
        c.backoff_numerator = 1; // == denominator, not growing
        c.backoff_denominator = 1;
        assert!(c.validate().is_err());

        let mut c = cfg();
        c.empty_streak_before_backoff = 0;
        assert!(c.validate().is_err());
    }

    #[test]
    fn test_starts_at_minimum() {
        let poll = AdaptivePoll::new(cfg());
        assert_eq!(poll.current_ms(), 50);
        assert_eq!(poll.current(), Duration::from_millis(50));
        assert_eq!(poll.consecutive_backoffs(), 0);
    }

    #[test]
    fn test_poll_outcome_found_normalizes_zero() {
        assert_eq!(PollOutcome::found(0), PollOutcome::Empty);
        assert_eq!(PollOutcome::found(3), PollOutcome::Found { depth: 3 });
        assert!(PollOutcome::found(3).is_found());
        assert!(!PollOutcome::found(0).is_found());
        assert_eq!(PollOutcome::found(7).depth(), 7);
        assert_eq!(PollOutcome::Empty.depth(), 0);
        assert!(PollOutcome::Empty.is_backoff());
        assert!(PollOutcome::Error.is_backoff());
        assert!(!PollOutcome::found(1).is_backoff());
    }

    #[test]
    fn test_interval_grows_on_consecutive_empties_up_to_max() {
        let mut poll = AdaptivePoll::new(cfg());
        // Deterministic doubling sequence: 50 -> 100 -> 200 -> 400 -> 800 -> 1000 (capped)
        let expected = [100u64, 200, 400, 800, 1_000, 1_000, 1_000];
        for exp in expected {
            let got = poll.record(PollOutcome::Empty);
            assert_eq!(got, Duration::from_millis(exp), "interval after empty");
        }
        // Saturated at the configured maximum.
        assert_eq!(poll.current_ms(), 1_000);
    }

    #[test]
    fn test_error_also_backs_off() {
        let mut poll = AdaptivePoll::new(cfg());
        assert_eq!(poll.record(PollOutcome::Error), Duration::from_millis(100));
        assert_eq!(poll.record(PollOutcome::Error), Duration::from_millis(200));
        assert_eq!(poll.stats().total_errors, 2);
        assert_eq!(poll.stats().total_empty, 0);
    }

    #[test]
    fn test_found_high_depth_snaps_to_min() {
        let mut poll = AdaptivePoll::new(cfg());
        // Grow first.
        poll.record(PollOutcome::Empty); // 100
        poll.record(PollOutcome::Empty); // 200
        poll.record(PollOutcome::Empty); // 400
        assert_eq!(poll.current_ms(), 400);

        // High depth (>= threshold 8) snaps straight to the minimum.
        let got = poll.record(PollOutcome::found(8));
        assert_eq!(got, Duration::from_millis(50));
        assert_eq!(poll.consecutive_backoffs(), 0);
    }

    #[test]
    fn test_found_low_depth_halves_toward_min() {
        let mut poll = AdaptivePoll::new(cfg());
        poll.record(PollOutcome::Empty); // 100
        poll.record(PollOutcome::Empty); // 200
        poll.record(PollOutcome::Empty); // 400
        poll.record(PollOutcome::Empty); // 800
        assert_eq!(poll.current_ms(), 800);

        // Low depth (< threshold) halves: 800 -> 400 -> 200 -> 100 -> 50 -> 50(min)
        let expected = [400u64, 200, 100, 50, 50];
        for exp in expected {
            let got = poll.record(PollOutcome::found(1));
            assert_eq!(got, Duration::from_millis(exp), "halving toward min");
        }
        assert_eq!(poll.current_ms(), 50);
    }

    #[test]
    fn test_streak_threshold_delays_backoff() {
        let mut poll = AdaptivePoll::new(AdaptivePollConfig {
            empty_streak_before_backoff: 3,
            ..cfg()
        });
        // First two empties do NOT grow the interval (streak < 3).
        assert_eq!(poll.record(PollOutcome::Empty), Duration::from_millis(50));
        assert_eq!(poll.record(PollOutcome::Empty), Duration::from_millis(50));
        // Third empty crosses the threshold and grows.
        assert_eq!(poll.record(PollOutcome::Empty), Duration::from_millis(100));
        // Subsequent empties keep growing.
        assert_eq!(poll.record(PollOutcome::Empty), Duration::from_millis(200));
    }

    #[test]
    fn test_fractional_backoff_factor_makes_progress() {
        // 3/2 growth from a tiny min must still strictly increase each step.
        let mut poll = AdaptivePoll::new(AdaptivePollConfig {
            min_interval_ms: 1,
            max_interval_ms: 100,
            backoff_numerator: 3,
            backoff_denominator: 2,
            empty_streak_before_backoff: 1,
            high_depth_threshold: 8,
        });
        // 1 -> 2 (max(1, 1*3/2=1)+forced+1) ... verify strict monotonic growth.
        let mut prev = poll.current_ms();
        for _ in 0..20 {
            let now = poll.record(PollOutcome::Empty).as_millis() as u64;
            assert!(
                now > prev || now == 100,
                "must grow or be capped: {prev}->{now}"
            );
            prev = now;
        }
        assert_eq!(poll.current_ms(), 100);
    }

    #[test]
    fn test_reset_returns_to_min() {
        let mut poll = AdaptivePoll::new(cfg());
        poll.record(PollOutcome::Empty);
        poll.record(PollOutcome::Empty);
        assert!(poll.current_ms() > 50);
        poll.reset();
        assert_eq!(poll.current_ms(), 50);
        assert_eq!(poll.consecutive_backoffs(), 0);
    }

    #[test]
    fn test_stats_accumulate_and_reset() {
        let mut poll = AdaptivePoll::new(cfg());
        poll.record(PollOutcome::Empty);
        poll.record(PollOutcome::Error);
        poll.record(PollOutcome::found(2));
        let s = poll.stats();
        assert_eq!(s.total_empty, 1);
        assert_eq!(s.total_errors, 1);
        assert_eq!(s.total_found, 1);
        assert!(!s.to_string().is_empty());

        poll.reset_stats();
        let s = poll.stats();
        assert_eq!(s.total_empty, 0);
        assert_eq!(s.total_errors, 0);
        assert_eq!(s.total_found, 0);
    }

    #[test]
    fn test_from_fixed_interval() {
        let poll = AdaptivePoll::from_fixed_interval(200);
        assert_eq!(poll.current_ms(), 200);
        assert_eq!(poll.config().min_interval_ms, 200);
        assert_eq!(poll.config().max_interval_ms, 3_200); // 200 * 16
    }

    #[test]
    fn test_from_fixed_interval_caps_max() {
        // Large fixed interval: max capped at 60_000, never below min.
        let poll = AdaptivePoll::from_fixed_interval(10_000);
        assert_eq!(poll.config().min_interval_ms, 10_000);
        assert_eq!(poll.config().max_interval_ms, 60_000);

        let poll = AdaptivePoll::from_fixed_interval(0);
        assert_eq!(poll.config().min_interval_ms, 1);
    }

    #[test]
    fn test_sanitize_repairs_degenerate_config() {
        // Degenerate config must not panic and must yield a working controller.
        let mut poll = AdaptivePoll::new(AdaptivePollConfig {
            min_interval_ms: 0,
            max_interval_ms: 0,
            backoff_numerator: 1,
            backoff_denominator: 0,
            empty_streak_before_backoff: 0,
            high_depth_threshold: 0,
        });
        assert!(poll.current_ms() >= 1);
        // Back-off still makes forward progress.
        let before = poll.current_ms();
        let after = poll.record(PollOutcome::Empty).as_millis() as u64;
        assert!(after >= before);
    }

    #[test]
    fn test_deterministic_sequence_is_reproducible() {
        // Same input sequence -> identical interval trajectory (no clock).
        let seq = [
            PollOutcome::Empty,
            PollOutcome::Empty,
            PollOutcome::found(1),
            PollOutcome::Empty,
            PollOutcome::Error,
            PollOutcome::found(20),
            PollOutcome::Empty,
        ];
        let run = || {
            let mut p = AdaptivePoll::new(cfg());
            seq.iter()
                .map(|o| p.record(*o).as_millis() as u64)
                .collect::<Vec<_>>()
        };
        assert_eq!(run(), run());
    }
}
