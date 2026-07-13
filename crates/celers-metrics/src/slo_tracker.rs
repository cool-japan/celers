//! Stateful SLA/SLO tracking, error-budget accounting, and breach alerting.
//!
//! This module complements the lightweight, stateless helpers in
//! [`crate::slo`] (`SloTarget`, `check_slo_compliance`, `calculate_error_budget`)
//! with a *stateful*, rolling-window service-level-objective tracker.
//!
//! The model follows the Google SRE error-budget methodology:
//!
//! * An **SLO** ([`Slo`]) pairs a service-level indicator ([`SloKind`]) with a
//!   target *attainment* (objective) and a rolling evaluation window
//!   ([`SloWindow`]).
//! * Each observation is classified as **good** or **bad** for the indicator
//!   (a successful task, or a request served under a latency threshold).
//! * The **error budget** ([`ErrorBudget`]) is the number of bad events the
//!   objective permits over the window. Consuming it produces an attainment
//!   below target; exhausting it raises a breach.
//! * The **burn rate** is the observed bad-event rate divided by the budgeted
//!   (allowed) bad-event rate. A burn rate above `1.0` means the budget is
//!   being spent faster than the window can sustain.
//!
//! All arithmetic is implemented directly with `f64`/integer math; there are no
//! external statistical dependencies.
//!
//! ```
//! use celers_metrics::{Slo, SloKind, SloTracker, SloState};
//!
//! // 99% availability over a rolling window of the last 1000 events.
//! let slo = Slo::availability("task-success", 0.99).with_event_window(1000);
//! let mut tracker = SloTracker::new(slo);
//!
//! // 990 successes, 10 failures => exactly on budget (10 allowed).
//! for _ in 0..990 { tracker.record_success(); }
//! for _ in 0..10 { tracker.record_failure(); }
//!
//! let status = tracker.status();
//! assert_eq!(status.attainment, 0.99);
//! assert_eq!(status.budget.allowed, 10);
//! assert_eq!(status.budget.consumed, 10);
//! // On-budget but fully consumed: the objective is still met (>=), yet the
//! // budget is exhausted, so the tracker flags it for attention.
//! assert!(matches!(status.state, SloState::Exhausted | SloState::Breaching));
//!
//! // One more failure tips attainment below the objective.
//! tracker.record_failure();
//! assert!(tracker.status().attainment < 0.99);
//! assert_eq!(tracker.status().state, SloState::Breaching);
//! ```

use crate::slo::calculate_percentile;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

// ============================================================================
// SLO definition
// ============================================================================

/// The kind of service-level indicator an [`Slo`] is built on.
///
/// Each kind defines how a raw observation is reduced to a *good*/*bad*
/// decision for error-budget accounting.
#[derive(Debug, Clone, PartialEq)]
pub enum SloKind {
    /// Availability / success-rate indicator.
    ///
    /// An observation is *good* when it represents a successful unit of work
    /// (e.g. a task that completed without permanently failing). The objective
    /// is the minimum fraction of good events over the window.
    Availability,
    /// Latency indicator expressed as "fraction of requests served under a
    /// threshold".
    ///
    /// An observation (a latency, in seconds) is *good* when it is less than or
    /// equal to `threshold_seconds`. The objective is the minimum fraction of
    /// requests that must be served within the threshold. This is the standard
    /// SRE way of turning a latency target into an error budget.
    ///
    /// In addition to the good/bad ratio, the tracker keeps the raw latency
    /// samples in the window so a true rolling percentile (e.g. p99) can be
    /// reported via [`SloStatus::observed_percentile`].
    LatencyThreshold {
        /// Latency threshold in seconds; observations at or below this value
        /// are counted as good.
        threshold_seconds: f64,
        /// Percentile (0.0..=1.0) reported for the rolling latency samples,
        /// e.g. `0.99` for p99. Used only for reporting; the good/bad
        /// classification always uses `threshold_seconds`.
        report_percentile: f64,
    },
}

/// The rolling window over which an [`Slo`] is evaluated.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SloWindow {
    /// Evaluate over the most recent `n` events.
    Events(usize),
    /// Evaluate over the most recent `seconds` of wall-clock time.
    ///
    /// Observations carry a Unix timestamp (seconds); samples older than the
    /// window are evicted before each computation.
    Seconds(u64),
}

impl SloWindow {
    /// Returns the event capacity if this is an event window.
    pub fn event_capacity(&self) -> Option<usize> {
        match self {
            SloWindow::Events(n) => Some(*n),
            SloWindow::Seconds(_) => None,
        }
    }

    /// Returns the time span in seconds if this is a time window.
    pub fn seconds(&self) -> Option<u64> {
        match self {
            SloWindow::Seconds(s) => Some(*s),
            SloWindow::Events(_) => None,
        }
    }
}

/// A service-level objective: an indicator, a target attainment, and a window.
#[derive(Debug, Clone)]
pub struct Slo {
    /// Human-readable name (used in alerts and reports).
    pub name: String,
    /// The indicator this objective is built on.
    pub kind: SloKind,
    /// Target attainment in `0.0..=1.0` (e.g. `0.99` for 99%).
    ///
    /// This is the minimum acceptable fraction of *good* events over the
    /// window. The complementary value `1.0 - objective` is the allowed
    /// error fraction that defines the error budget.
    pub objective: f64,
    /// The rolling window over which attainment is measured.
    pub window: SloWindow,
    /// Minimum number of observations required before a verdict other than
    /// [`SloState::Warming`] is produced. Prevents noisy verdicts on tiny
    /// samples.
    pub warmup_events: usize,
    /// Burn-rate threshold above which the SLO is flagged [`SloState::AtRisk`]
    /// even while the objective is technically still met. A common fast-burn
    /// alert threshold is `2.0` (spending budget twice as fast as sustainable).
    pub burn_rate_alert: f64,
}

impl Slo {
    /// Create an availability (success-rate) SLO with the given objective.
    ///
    /// Defaults to a 1000-event rolling window with a 1-event warm-up and a
    /// burn-rate alert threshold of `2.0`.
    pub fn availability(name: impl Into<String>, objective: f64) -> Self {
        Self {
            name: name.into(),
            kind: SloKind::Availability,
            objective: objective.clamp(0.0, 1.0),
            window: SloWindow::Events(1000),
            warmup_events: 1,
            burn_rate_alert: 2.0,
        }
    }

    /// Create a latency-threshold SLO: at least `objective` fraction of
    /// requests must be served within `threshold_seconds`.
    ///
    /// `report_percentile` controls which rolling percentile is surfaced in
    /// [`SloStatus::observed_percentile`] (e.g. `0.99`).
    pub fn latency(
        name: impl Into<String>,
        threshold_seconds: f64,
        objective: f64,
        report_percentile: f64,
    ) -> Self {
        Self {
            name: name.into(),
            kind: SloKind::LatencyThreshold {
                threshold_seconds,
                report_percentile: report_percentile.clamp(0.0, 1.0),
            },
            objective: objective.clamp(0.0, 1.0),
            window: SloWindow::Events(1000),
            warmup_events: 1,
            burn_rate_alert: 2.0,
        }
    }

    /// Set an event-count rolling window.
    pub fn with_event_window(mut self, events: usize) -> Self {
        self.window = SloWindow::Events(events.max(1));
        self
    }

    /// Set a time-based rolling window (seconds).
    pub fn with_time_window(mut self, seconds: u64) -> Self {
        self.window = SloWindow::Seconds(seconds.max(1));
        self
    }

    /// Set the minimum number of observations required before a non-warming
    /// verdict is produced.
    pub fn with_warmup(mut self, warmup_events: usize) -> Self {
        self.warmup_events = warmup_events;
        self
    }

    /// Set the burn-rate threshold for the [`SloState::AtRisk`] verdict.
    pub fn with_burn_rate_alert(mut self, threshold: f64) -> Self {
        self.burn_rate_alert = threshold.max(0.0);
        self
    }

    /// The allowed error fraction (`1.0 - objective`).
    pub fn allowed_error_fraction(&self) -> f64 {
        (1.0 - self.objective).clamp(0.0, 1.0)
    }
}

// ============================================================================
// Error budget
// ============================================================================

/// Snapshot of error-budget accounting over the current window.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ErrorBudget {
    /// Total observations in the window.
    pub total: u64,
    /// Number of bad events permitted by the objective over `total`
    /// observations: `floor(total * (1 - objective))`.
    pub allowed: u64,
    /// Number of bad events actually observed in the window.
    pub consumed: u64,
    /// Remaining budget (`allowed - consumed`, floored at 0).
    pub remaining: u64,
    /// Fraction of the budget still available in `0.0..=1.0`
    /// (`1.0` = untouched, `0.0` = fully spent or over-spent).
    pub remaining_fraction: f64,
    /// Burn rate: observed error rate divided by the allowed error rate.
    ///
    /// `1.0` means the budget is being spent exactly at the sustainable pace,
    /// `> 1.0` means faster (budget will be exhausted before the window
    /// rolls), `< 1.0` means slower. `None` when the objective is `1.0`
    /// (no error budget exists, so burn rate is undefined).
    pub burn_rate: Option<f64>,
}

impl ErrorBudget {
    /// Compute the error budget from window totals and an objective.
    ///
    /// `objective` is the target attainment (e.g. `0.99`); the allowed error
    /// fraction is `1 - objective`.
    pub fn compute(total: u64, bad: u64, objective: f64) -> Self {
        let objective = objective.clamp(0.0, 1.0);
        let allowed_fraction = 1.0 - objective;
        // Allowed bad events over the window (floor: partial events are not
        // "earned" budget). This makes "99% over 1000" allow exactly 10.
        //
        // A small epsilon absorbs floating-point representation error so that a
        // product that is mathematically an integer (e.g. `50 * 0.20`, which
        // evaluates to 9.999999999999998 in f64) is not erroneously floored
        // down by one.
        let allowed = (total as f64 * allowed_fraction + 1e-9).floor() as u64;
        let consumed = bad;
        let remaining = allowed.saturating_sub(consumed);

        let remaining_fraction = if allowed == 0 {
            // No budget at all: untouched only if nothing was consumed.
            if consumed == 0 {
                1.0
            } else {
                0.0
            }
        } else {
            (1.0 - (consumed as f64 / allowed as f64)).clamp(0.0, 1.0)
        };

        let burn_rate = if allowed_fraction <= 0.0 || total == 0 {
            None
        } else {
            let observed_error_rate = bad as f64 / total as f64;
            Some(observed_error_rate / allowed_fraction)
        };

        Self {
            total,
            allowed,
            consumed,
            remaining,
            remaining_fraction,
            burn_rate,
        }
    }

    /// Whether the budget is fully (or over-) consumed.
    pub fn is_exhausted(&self) -> bool {
        self.consumed >= self.allowed
    }
}

// ============================================================================
// SLO status
// ============================================================================

/// Discrete state of an SLO at evaluation time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SloState {
    /// Not enough observations yet (below `warmup_events`).
    Warming,
    /// Objective met and error budget comfortably available.
    Healthy,
    /// Objective still met, but the error budget is being burned faster than
    /// the configured [`Slo::burn_rate_alert`] threshold.
    AtRisk,
    /// The error budget is fully consumed while the objective is (still
    /// nominally) met — the next bad event will breach. Worth alerting on.
    Exhausted,
    /// The measured attainment is below the objective: the SLO is violated.
    Breaching,
}

impl SloState {
    /// Whether this state should raise an operational alert.
    pub fn is_alerting(&self) -> bool {
        matches!(
            self,
            SloState::AtRisk | SloState::Exhausted | SloState::Breaching
        )
    }
}

/// A full evaluation of an [`Slo`] over its current window.
#[derive(Debug, Clone)]
pub struct SloStatus {
    /// The discrete verdict.
    pub state: SloState,
    /// Measured attainment (fraction of good events) over the window in
    /// `0.0..=1.0`. `1.0` when the window is empty.
    pub attainment: f64,
    /// The objective copied from the [`Slo`] for convenience.
    pub objective: f64,
    /// Error-budget accounting for the window.
    pub budget: ErrorBudget,
    /// For latency SLOs, the reported rolling percentile of the latency
    /// samples (seconds). `None` for availability SLOs or empty windows.
    pub observed_percentile: Option<f64>,
    /// A human-readable summary suitable for logs / alert payloads.
    pub message: String,
}

impl SloStatus {
    /// Whether the SLO currently warrants an alert.
    pub fn is_alerting(&self) -> bool {
        self.state.is_alerting()
    }

    /// Whether the measured attainment meets or exceeds the objective.
    pub fn is_meeting_objective(&self) -> bool {
        self.attainment >= self.objective
    }
}

// ============================================================================
// Observation buffer
// ============================================================================

/// One classified observation in the rolling window.
#[derive(Debug, Clone, Copy)]
struct Observation {
    /// Unix timestamp in seconds (used for time windows).
    timestamp: u64,
    /// `true` if the observation was good for the indicator.
    good: bool,
    /// Raw latency value (seconds) for latency SLOs; `None` otherwise.
    latency: Option<f64>,
}

// ============================================================================
// SLO tracker
// ============================================================================

/// Stateful, rolling-window evaluator for a single [`Slo`].
///
/// Feed observations with [`record_success`](SloTracker::record_success) /
/// [`record_failure`](SloTracker::record_failure) (availability) or
/// [`record_latency`](SloTracker::record_latency) (latency), then read
/// [`status`](SloTracker::status) for attainment, error budget, burn rate, and
/// a breach verdict.
///
/// The tracker is not internally synchronised; wrap it in a `Mutex` for shared
/// use (mirroring the convention used by [`crate::history::MetricHistory`]).
#[derive(Debug)]
pub struct SloTracker {
    slo: Slo,
    window: VecDeque<Observation>,
    /// Running count of good observations currently in `window`.
    good_count: u64,
    /// Monotonic logical clock used when no explicit timestamp is supplied,
    /// guaranteeing deterministic ordering/eviction in tests.
    logical_clock: u64,
}

impl SloTracker {
    /// Create a tracker for the given SLO.
    pub fn new(slo: Slo) -> Self {
        let capacity = slo.window.event_capacity().unwrap_or(1024);
        Self {
            slo,
            window: VecDeque::with_capacity(capacity),
            good_count: 0,
            logical_clock: 0,
        }
    }

    /// Borrow the underlying SLO definition.
    pub fn slo(&self) -> &Slo {
        &self.slo
    }

    /// Number of observations currently retained in the window.
    pub fn len(&self) -> usize {
        self.window.len()
    }

    /// Whether the window currently holds no observations.
    pub fn is_empty(&self) -> bool {
        self.window.is_empty()
    }

    /// Drop all retained observations.
    pub fn reset(&mut self) {
        self.window.clear();
        self.good_count = 0;
    }

    fn now_seconds() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    fn next_timestamp(&mut self) -> u64 {
        match self.slo.window {
            SloWindow::Seconds(_) => Self::now_seconds(),
            // For event windows the timestamp is only for ordering; a logical
            // clock keeps tests fully deterministic.
            SloWindow::Events(_) => {
                let ts = self.logical_clock;
                self.logical_clock = self.logical_clock.saturating_add(1);
                ts
            }
        }
    }

    fn push(&mut self, obs: Observation) {
        if obs.good {
            self.good_count = self.good_count.saturating_add(1);
        }
        self.window.push_back(obs);
        self.enforce_window();
    }

    /// Evict observations that fall outside the configured window.
    fn enforce_window(&mut self) {
        match self.slo.window {
            SloWindow::Events(capacity) => {
                while self.window.len() > capacity {
                    if let Some(front) = self.window.pop_front() {
                        if front.good {
                            self.good_count = self.good_count.saturating_sub(1);
                        }
                    }
                }
            }
            SloWindow::Seconds(span) => {
                let now = Self::now_seconds();
                let cutoff = now.saturating_sub(span);
                while let Some(front) = self.window.front() {
                    if front.timestamp < cutoff {
                        let front = self.window.pop_front();
                        if let Some(f) = front {
                            if f.good {
                                self.good_count = self.good_count.saturating_sub(1);
                            }
                        }
                    } else {
                        break;
                    }
                }
            }
        }
    }

    /// Record a successful unit of work (a *good* event for availability SLOs).
    pub fn record_success(&mut self) {
        let timestamp = self.next_timestamp();
        self.push(Observation {
            timestamp,
            good: true,
            latency: None,
        });
    }

    /// Record a failed unit of work (a *bad* event for availability SLOs).
    pub fn record_failure(&mut self) {
        let timestamp = self.next_timestamp();
        self.push(Observation {
            timestamp,
            good: false,
            latency: None,
        });
    }

    /// Record a boolean outcome directly (`true` = good, `false` = bad).
    pub fn record_outcome(&mut self, good: bool) {
        if good {
            self.record_success();
        } else {
            self.record_failure();
        }
    }

    /// Record a latency observation (seconds).
    ///
    /// For [`SloKind::LatencyThreshold`] SLOs the value is classified good when
    /// it is at or below the threshold and retained for rolling-percentile
    /// reporting. For availability SLOs the latency is ignored for
    /// classification (treated as a good event) but still retained.
    pub fn record_latency(&mut self, latency_seconds: f64) {
        let good = match self.slo.kind {
            SloKind::LatencyThreshold {
                threshold_seconds, ..
            } => latency_seconds <= threshold_seconds,
            SloKind::Availability => true,
        };
        let timestamp = self.next_timestamp();
        self.push(Observation {
            timestamp,
            good,
            latency: Some(latency_seconds),
        });
    }

    /// Current attainment (fraction of good events) over the window.
    ///
    /// Returns `1.0` for an empty window (nothing has gone wrong yet).
    pub fn attainment(&self) -> f64 {
        let total = self.window.len() as u64;
        if total == 0 {
            return 1.0;
        }
        self.good_count as f64 / total as f64
    }

    /// Current error-budget accounting over the window.
    pub fn error_budget(&self) -> ErrorBudget {
        let total = self.window.len() as u64;
        let bad = total.saturating_sub(self.good_count);
        ErrorBudget::compute(total, bad, self.slo.objective)
    }

    /// Reported rolling percentile of latency samples, if applicable.
    fn observed_percentile(&self) -> Option<f64> {
        if let SloKind::LatencyThreshold {
            report_percentile, ..
        } = self.slo.kind
        {
            let mut samples: Vec<f64> = self
                .window
                .iter()
                .filter_map(|o| o.latency)
                .collect::<Vec<_>>();
            if samples.is_empty() {
                return None;
            }
            samples.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
            calculate_percentile(&samples, report_percentile)
        } else {
            None
        }
    }

    /// Evaluate the SLO over the current window.
    pub fn status(&self) -> SloStatus {
        let total = self.window.len();
        let attainment = self.attainment();
        let budget = self.error_budget();
        let observed_percentile = self.observed_percentile();
        let objective = self.slo.objective;

        let state = if total < self.slo.warmup_events.max(1) {
            SloState::Warming
        } else if attainment < objective {
            SloState::Breaching
        } else if budget.is_exhausted() {
            // Objective still met (>=) but no budget left for the next failure.
            SloState::Exhausted
        } else if budget
            .burn_rate
            .map(|b| b > self.slo.burn_rate_alert)
            .unwrap_or(false)
        {
            SloState::AtRisk
        } else {
            SloState::Healthy
        };

        let message =
            Self::render_message(&self.slo, state, attainment, &budget, observed_percentile);

        SloStatus {
            state,
            attainment,
            objective,
            budget,
            observed_percentile,
            message,
        }
    }

    fn render_message(
        slo: &Slo,
        state: SloState,
        attainment: f64,
        budget: &ErrorBudget,
        observed_percentile: Option<f64>,
    ) -> String {
        let pct = attainment * 100.0;
        let target = slo.objective * 100.0;
        let burn = budget
            .burn_rate
            .map(|b| format!("{b:.2}x"))
            .unwrap_or_else(|| "n/a".to_string());
        let base = match state {
            SloState::Warming => format!(
                "SLO '{}' warming up ({} / {} observations)",
                slo.name,
                budget.total,
                slo.warmup_events.max(1)
            ),
            SloState::Healthy => format!(
                "SLO '{}' healthy: attainment {pct:.3}% >= target {target:.3}%, \
                 budget {}/{} remaining (burn {burn})",
                slo.name, budget.remaining, budget.allowed
            ),
            SloState::AtRisk => format!(
                "SLO '{}' at risk: attainment {pct:.3}% >= target {target:.3}% but \
                 burn rate {burn} exceeds {:.2}x ({}/{} budget remaining)",
                slo.name, slo.burn_rate_alert, budget.remaining, budget.allowed
            ),
            SloState::Exhausted => format!(
                "SLO '{}' error budget exhausted: attainment {pct:.3}% (target {target:.3}%), \
                 {}/{} budget consumed (burn {burn})",
                slo.name, budget.consumed, budget.allowed
            ),
            SloState::Breaching => format!(
                "SLO '{}' BREACH: attainment {pct:.3}% < target {target:.3}% \
                 ({}/{} budget consumed, burn {burn})",
                slo.name, budget.consumed, budget.allowed
            ),
        };
        match observed_percentile {
            Some(p) => format!("{base}; observed latency percentile {p:.4}s"),
            None => base,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn feed(tracker: &mut SloTracker, good: usize, bad: usize) {
        for _ in 0..good {
            tracker.record_success();
        }
        for _ in 0..bad {
            tracker.record_failure();
        }
    }

    #[test]
    fn error_budget_99pct_over_1000_allows_10() {
        // 99% objective over exactly 1000 events => 10 allowed failures.
        let budget = ErrorBudget::compute(1000, 10, 0.99);
        assert_eq!(budget.total, 1000);
        assert_eq!(budget.allowed, 10);
        assert_eq!(budget.consumed, 10);
        assert_eq!(budget.remaining, 0);
        assert!(budget.is_exhausted());
        // remaining fraction is 0 when fully consumed
        assert!(budget.remaining_fraction.abs() < 1e-12);
    }

    #[test]
    fn error_budget_11_failures_breaches() {
        // 11 failures over 1000 with a 99% objective: over budget.
        let budget = ErrorBudget::compute(1000, 11, 0.99);
        assert_eq!(budget.allowed, 10);
        assert_eq!(budget.consumed, 11);
        assert_eq!(budget.remaining, 0);
        assert!(budget.is_exhausted());
        // attainment 989/1000 = 0.989 < 0.99 -> breach is detected by tracker.
    }

    #[test]
    fn error_budget_half_consumed() {
        // 5 of 10 allowed failures used => 50% remaining.
        let budget = ErrorBudget::compute(1000, 5, 0.99);
        assert_eq!(budget.allowed, 10);
        assert_eq!(budget.consumed, 5);
        assert_eq!(budget.remaining, 5);
        assert!((budget.remaining_fraction - 0.5).abs() < 1e-12);
        assert!(!budget.is_exhausted());
    }

    #[test]
    fn error_budget_untouched() {
        let budget = ErrorBudget::compute(1000, 0, 0.99);
        assert_eq!(budget.consumed, 0);
        assert_eq!(budget.remaining, 10);
        assert!((budget.remaining_fraction - 1.0).abs() < 1e-12);
        assert!(!budget.is_exhausted());
    }

    #[test]
    fn burn_rate_math() {
        // 99% objective => allowed error fraction 0.01.
        // Observe 20 failures / 1000 = 0.02 error rate => burn rate 2.0.
        let budget = ErrorBudget::compute(1000, 20, 0.99);
        let burn = budget
            .burn_rate
            .expect("burn rate defined for objective < 1");
        assert!((burn - 2.0).abs() < 1e-9, "burn rate was {burn}");

        // On-budget consumption burns at exactly 1.0x.
        let on_budget = ErrorBudget::compute(1000, 10, 0.99);
        assert!((on_budget.burn_rate.unwrap() - 1.0).abs() < 1e-9);

        // Half the allowed rate => 0.5x.
        let slow = ErrorBudget::compute(1000, 5, 0.99);
        assert!((slow.burn_rate.unwrap() - 0.5).abs() < 1e-9);
    }

    #[test]
    fn allowed_budget_robust_to_float_error() {
        // 1 - 0.80 == 0.19999999999999996 in f64, so 50 * (1-0.80) evaluates to
        // 9.999999999999998; a naive floor would give 9. The epsilon guard must
        // recover the mathematically correct allowance of 10.
        let budget = ErrorBudget::compute(50, 9, 0.80);
        assert_eq!(budget.allowed, 10, "float-floor off-by-one regression");
        assert_eq!(budget.remaining, 1);
        assert!(!budget.is_exhausted());

        // Sanity: a true fractional product still floors down.
        let frac = ErrorBudget::compute(7, 0, 0.90); // 7 * 0.1 = 0.7 -> floor 0
        assert_eq!(frac.allowed, 0);
    }

    #[test]
    fn burn_rate_undefined_for_perfect_objective() {
        // Objective 1.0 means no error budget exists -> burn rate undefined.
        let budget = ErrorBudget::compute(100, 1, 1.0);
        assert_eq!(budget.allowed, 0);
        assert!(budget.burn_rate.is_none());
        assert!(budget.is_exhausted());
    }

    #[test]
    fn tracker_met_when_on_budget() {
        let slo = Slo::availability("svc", 0.99).with_event_window(1000);
        let mut tracker = SloTracker::new(slo);
        feed(&mut tracker, 990, 10);

        let status = tracker.status();
        assert!((status.attainment - 0.99).abs() < 1e-12);
        assert!(status.is_meeting_objective());
        assert_eq!(status.budget.allowed, 10);
        assert_eq!(status.budget.consumed, 10);
        // Exactly on budget: objective met but budget exhausted.
        assert_eq!(status.state, SloState::Exhausted);
        assert!(status.is_alerting());
    }

    #[test]
    fn tracker_breaches_when_over_budget() {
        let slo = Slo::availability("svc", 0.99).with_event_window(1000);
        let mut tracker = SloTracker::new(slo);
        feed(&mut tracker, 989, 11);

        let status = tracker.status();
        assert!(status.attainment < 0.99);
        assert!(!status.is_meeting_objective());
        assert_eq!(status.state, SloState::Breaching);
        assert!(status.is_alerting());
        assert!(status.message.contains("BREACH"));
    }

    #[test]
    fn tracker_healthy_with_budget_to_spare() {
        let slo = Slo::availability("svc", 0.99).with_event_window(1000);
        let mut tracker = SloTracker::new(slo);
        feed(&mut tracker, 998, 2); // 2 of 10 used, burn 0.2x

        let status = tracker.status();
        assert!(status.is_meeting_objective());
        assert_eq!(status.state, SloState::Healthy);
        assert!(!status.is_alerting());
        assert_eq!(status.budget.remaining, 8);
    }

    #[test]
    fn tracker_at_risk_on_elevated_burn() {
        // AtRisk requires: objective met, budget NOT exhausted, and burn rate
        // above the configured alert threshold. On a count window the budget
        // scales with the observed total, so "burn > 1.0" implies the budget is
        // already over-consumed. The AtRisk branch is therefore meaningful for
        // burn-rate alert thresholds below 1.0 (early-warning fast-burn alerts),
        // which we exercise here.
        //
        // objective 0.80 over a 100-event window, alert at 0.85.
        // Feed 41 good / 9 bad: attainment 0.82 >= 0.80 (meets); allowed over
        // 50 = floor(50*0.2) = 10; consumed 9 -> remaining 1 (not exhausted);
        // error_rate 0.18 / allowed_fraction 0.20 -> burn 0.90 > 0.85.
        let slo = Slo::availability("svc", 0.80)
            .with_event_window(100)
            .with_burn_rate_alert(0.85);
        let mut tracker = SloTracker::new(slo);
        feed(&mut tracker, 41, 9);

        let status = tracker.status();
        assert!(
            status.is_meeting_objective(),
            "attainment {}",
            status.attainment
        );
        assert!(!status.budget.is_exhausted());
        let burn = status.budget.burn_rate.expect("burn defined");
        assert!(burn > 0.85, "burn {burn}");
        assert_eq!(status.state, SloState::AtRisk);
        assert!(status.is_alerting());
        assert!(status.message.contains("at risk"));
    }

    #[test]
    fn tracker_warming_below_warmup() {
        let slo = Slo::availability("svc", 0.99)
            .with_event_window(1000)
            .with_warmup(50);
        let mut tracker = SloTracker::new(slo);
        feed(&mut tracker, 5, 0);
        let status = tracker.status();
        assert_eq!(status.state, SloState::Warming);
        assert!(!status.is_alerting());
    }

    #[test]
    fn rolling_event_window_evicts_old_observations() {
        let slo = Slo::availability("svc", 0.99).with_event_window(100);
        let mut tracker = SloTracker::new(slo);
        // 100 failures fill the window, then 100 successes push them all out.
        feed(&mut tracker, 0, 100);
        assert!(tracker.attainment().abs() < 1e-12); // all bad
        feed(&mut tracker, 100, 0);
        assert_eq!(tracker.len(), 100);
        assert!((tracker.attainment() - 1.0).abs() < 1e-12); // all good now
        assert_eq!(tracker.status().state, SloState::Healthy);
    }

    #[test]
    fn latency_slo_classifies_and_reports_percentile() {
        // 95% of requests must be <= 0.5s; report p99.
        let slo = Slo::latency("api", 0.5, 0.95, 0.99).with_event_window(1000);
        let mut tracker = SloTracker::new(slo);
        // 970 fast (0.1s), 30 slow (1.0s) => 97% good >= 95% objective.
        for _ in 0..970 {
            tracker.record_latency(0.1);
        }
        for _ in 0..30 {
            tracker.record_latency(1.0);
        }
        let status = tracker.status();
        assert!((status.attainment - 0.97).abs() < 1e-9);
        assert!(status.is_meeting_objective());
        // p99 should land in the slow bucket (>= 0.5s).
        let p99 = status
            .observed_percentile
            .expect("latency percentile present");
        assert!(p99 >= 0.5, "p99 was {p99}");
    }

    #[test]
    fn latency_slo_breaches_when_too_many_slow() {
        let slo = Slo::latency("api", 0.5, 0.95, 0.99).with_event_window(1000);
        let mut tracker = SloTracker::new(slo);
        // 900 fast, 100 slow => 90% good < 95% objective -> breach.
        for _ in 0..900 {
            tracker.record_latency(0.1);
        }
        for _ in 0..100 {
            tracker.record_latency(1.0);
        }
        let status = tracker.status();
        assert!(status.attainment < 0.95);
        assert_eq!(status.state, SloState::Breaching);
    }

    #[test]
    fn empty_window_is_perfect_attainment() {
        let slo = Slo::availability("svc", 0.99);
        let tracker = SloTracker::new(slo);
        let status = tracker.status();
        // Empty window: warming (below warmup of 1? warmup=1, total=0 -> warming)
        assert_eq!(status.state, SloState::Warming);
        assert!((status.attainment - 1.0).abs() < 1e-12);
    }

    #[test]
    fn reset_clears_window() {
        let slo = Slo::availability("svc", 0.99).with_event_window(100);
        let mut tracker = SloTracker::new(slo);
        feed(&mut tracker, 10, 5);
        assert_eq!(tracker.len(), 15);
        tracker.reset();
        assert!(tracker.is_empty());
        assert!((tracker.attainment() - 1.0).abs() < 1e-12);
    }

    #[test]
    fn record_outcome_dispatches() {
        let slo = Slo::availability("svc", 0.5)
            .with_event_window(10)
            .with_warmup(1);
        let mut tracker = SloTracker::new(slo);
        tracker.record_outcome(true);
        tracker.record_outcome(false);
        tracker.record_outcome(true);
        assert_eq!(tracker.len(), 3);
        assert!((tracker.attainment() - (2.0 / 3.0)).abs() < 1e-12);
    }
}
