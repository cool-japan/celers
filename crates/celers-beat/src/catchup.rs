//! Missed-task catch-up logic.
//!
//! When a beat scheduler is stopped (a crash, a deploy, a paused leader) and
//! later resumes, some scheduled occurrences may have been *missed* — their
//! firing instant elapsed while nothing was running. This module computes those
//! missed occurrences from a [`Schedule`], the task's `last_run` and the
//! current time, and then applies a [`CatchupPolicy`] to decide which of them
//! should actually be executed on resume.
//!
//! The occurrence enumeration reuses
//! [`enumerate_next_occurrences`],
//! the same primitive used by conflict detection, so catch-up and conflict
//! detection share one definition of "what fires when".
//!
//! # Policies
//!
//! [`CatchupPolicy`] (this module's, distinct from
//! [`crate::history::CatchupPolicy`] which models interval-count catch-up for a
//! different code path) offers three behaviours:
//!
//! * [`CatchupPolicy::FireAll`] — run **every** missed occurrence, in order.
//! * [`CatchupPolicy::FireLatestOnly`] — collapse all misses into a single run
//!   of the most recent missed occurrence (the classic "we fell behind, just
//!   run once to get current" behaviour).
//! * [`CatchupPolicy::Skip`] — drop all missed occurrences; the next run will be
//!   the next *future* occurrence only.
//!
//! # Example
//!
//! ```
//! use celers_beat::catchup::{compute_missed_occurrences, CatchupPolicy};
//! use celers_beat::Schedule;
//! use chrono::{TimeZone, Utc};
//!
//! // Fires every hour on the hour.
//! # #[cfg(feature = "cron")]
//! # {
//! let sched = Schedule::crontab("0", "*", "*", "*", "*");
//! let last_run = Utc.with_ymd_and_hms(2026, 6, 13, 0, 0, 0).unwrap();
//! let now = Utc.with_ymd_and_hms(2026, 6, 13, 3, 30, 0).unwrap();
//!
//! // Missed the 01:00, 02:00 and 03:00 runs.
//! let missed = compute_missed_occurrences(&sched, last_run, now).unwrap();
//! assert_eq!(missed.len(), 3);
//!
//! // FireLatestOnly collapses them to a single 03:00 run.
//! let to_run = CatchupPolicy::FireLatestOnly.apply(&missed);
//! assert_eq!(to_run.len(), 1);
//! assert_eq!(to_run[0], Utc.with_ymd_and_hms(2026, 6, 13, 3, 0, 0).unwrap());
//! # }
//! ```

use crate::config::ScheduleError;
use crate::conflict::enumerate_next_occurrences;
use crate::schedule::Schedule;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Upper bound on how many missed occurrences will be materialised. A schedule
/// with a very short interval that has not run for a long time could otherwise
/// generate an unbounded list; this cap keeps the computation finite. Callers
/// who hit the cap will see exactly this many of the *earliest* missed
/// occurrences (see [`MissedOccurrences::truncated`]).
pub const MAX_MISSED_OCCURRENCES: usize = 100_000;

/// Policy controlling how missed occurrences are turned into runs on resume.
///
/// This is intentionally distinct from [`crate::history::CatchupPolicy`]: that
/// type answers "how many interval ticks do we replay" for the interval-based
/// fast path, whereas this type operates on a concrete, schedule-agnostic list
/// of missed instants.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CatchupPolicy {
    /// Execute every missed occurrence, preserving order.
    FireAll,
    /// Execute a single run for the most recent missed occurrence only.
    #[default]
    FireLatestOnly,
    /// Drop all missed occurrences; only future occurrences will run.
    Skip,
}

impl CatchupPolicy {
    /// Apply this policy to an ordered slice of missed occurrences, returning
    /// the occurrences that should actually be executed (also in ascending
    /// order).
    ///
    /// * `FireAll` returns a clone of the whole slice.
    /// * `FireLatestOnly` returns at most the last element.
    /// * `Skip` returns an empty vector.
    pub fn apply(&self, missed: &[DateTime<Utc>]) -> Vec<DateTime<Utc>> {
        match self {
            CatchupPolicy::FireAll => missed.to_vec(),
            CatchupPolicy::FireLatestOnly => missed.last().copied().into_iter().collect(),
            CatchupPolicy::Skip => Vec::new(),
        }
    }

    /// Human-readable label, handy for logging and metrics.
    pub fn as_str(&self) -> &'static str {
        match self {
            CatchupPolicy::FireAll => "fire_all",
            CatchupPolicy::FireLatestOnly => "fire_latest_only",
            CatchupPolicy::Skip => "skip",
        }
    }
}

impl std::fmt::Display for CatchupPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The set of occurrences that elapsed between a task's `last_run` and `now`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MissedOccurrences {
    /// The missed firing instants, ascending. Every entry satisfies
    /// `last_run < instant <= now`.
    pub instants: Vec<DateTime<Utc>>,
    /// `true` if enumeration stopped at [`MAX_MISSED_OCCURRENCES`] before
    /// reaching `now` (so there may be additional, unlisted misses).
    pub truncated: bool,
}

impl MissedOccurrences {
    /// Number of missed occurrences captured.
    pub fn len(&self) -> usize {
        self.instants.len()
    }

    /// Whether nothing was missed.
    pub fn is_empty(&self) -> bool {
        self.instants.is_empty()
    }

    /// The earliest missed occurrence, if any.
    pub fn earliest(&self) -> Option<DateTime<Utc>> {
        self.instants.first().copied()
    }

    /// The most recent missed occurrence, if any.
    pub fn latest(&self) -> Option<DateTime<Utc>> {
        self.instants.last().copied()
    }

    /// Apply a [`CatchupPolicy`] directly to these occurrences.
    pub fn apply_policy(&self, policy: CatchupPolicy) -> Vec<DateTime<Utc>> {
        policy.apply(&self.instants)
    }
}

/// Compute the occurrences of `schedule` that were missed in the half-open
/// interval `(last_run, now]`.
///
/// "Missed" means the occurrence's firing instant is strictly after `last_run`
/// and on or before `now`. Occurrences exactly at `now` are considered due (and
/// therefore missed if not yet run); occurrences strictly in the future are not
/// included.
///
/// The enumeration starts from `last_run` and walks forward using
/// [`enumerate_next_occurrences`], stopping at the first occurrence past `now`.
///
/// # Errors
///
/// Propagates any [`ScheduleError`] from evaluating the schedule.
pub fn compute_missed(
    schedule: &Schedule,
    last_run: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<MissedOccurrences, ScheduleError> {
    if now <= last_run {
        return Ok(MissedOccurrences {
            instants: Vec::new(),
            truncated: false,
        });
    }

    let mut instants = Vec::new();
    let mut cursor = last_run;
    let mut truncated = false;

    // Walk forward one occurrence at a time. We enumerate in small batches so
    // we never materialise more than the cap, while still bounding the loop.
    loop {
        let next = enumerate_next_occurrences(schedule, cursor, 1)?;
        let Some(instant) = next.into_iter().next() else {
            // No further occurrences (e.g. a one-time schedule already past, or
            // a solar horizon exhausted).
            break;
        };
        if instant > now {
            break;
        }
        instants.push(instant);
        cursor = instant;
        if instants.len() >= MAX_MISSED_OCCURRENCES {
            // Check whether there is at least one more occurrence within the
            // window; if so, flag truncation.
            let peek = enumerate_next_occurrences(schedule, cursor, 1)?;
            if let Some(next_instant) = peek.into_iter().next() {
                if next_instant <= now {
                    truncated = true;
                }
            }
            break;
        }
    }

    Ok(MissedOccurrences {
        instants,
        truncated,
    })
}

/// Convenience: compute missed occurrences and immediately apply a
/// [`CatchupPolicy`], returning just the instants to run.
///
/// Equivalent to `compute_missed(..).map(|m| policy.apply(&m.instants))`.
pub fn compute_missed_occurrences(
    schedule: &Schedule,
    last_run: DateTime<Utc>,
    now: DateTime<Utc>,
) -> Result<Vec<DateTime<Utc>>, ScheduleError> {
    Ok(compute_missed(schedule, last_run, now)?.instants)
}

/// Compute missed occurrences for `schedule` and apply `policy`, returning the
/// concrete instants that should be executed on resume.
///
/// This is the primary entry point combining steps 4's "compute missed
/// occurrences" and "apply a `CatchupPolicy`".
pub fn catch_up(
    schedule: &Schedule,
    last_run: DateTime<Utc>,
    now: DateTime<Utc>,
    policy: CatchupPolicy,
) -> Result<Vec<DateTime<Utc>>, ScheduleError> {
    let missed = compute_missed(schedule, last_run, now)?;
    Ok(policy.apply(&missed.instants))
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn ts(y: i32, mo: u32, d: u32, h: u32, mi: u32, s: u32) -> DateTime<Utc> {
        Utc.with_ymd_and_hms(y, mo, d, h, mi, s)
            .single()
            .expect("valid timestamp")
    }

    #[test]
    fn policy_apply_fire_all() {
        let missed = vec![
            ts(2026, 6, 13, 1, 0, 0),
            ts(2026, 6, 13, 2, 0, 0),
            ts(2026, 6, 13, 3, 0, 0),
        ];
        assert_eq!(CatchupPolicy::FireAll.apply(&missed), missed);
    }

    #[test]
    fn policy_apply_latest_only() {
        let missed = vec![
            ts(2026, 6, 13, 1, 0, 0),
            ts(2026, 6, 13, 2, 0, 0),
            ts(2026, 6, 13, 3, 0, 0),
        ];
        let out = CatchupPolicy::FireLatestOnly.apply(&missed);
        assert_eq!(out, vec![ts(2026, 6, 13, 3, 0, 0)]);
    }

    #[test]
    fn policy_apply_skip() {
        let missed = vec![ts(2026, 6, 13, 1, 0, 0)];
        assert!(CatchupPolicy::Skip.apply(&missed).is_empty());
    }

    #[test]
    fn policy_apply_on_empty() {
        let empty: Vec<DateTime<Utc>> = Vec::new();
        assert!(CatchupPolicy::FireAll.apply(&empty).is_empty());
        assert!(CatchupPolicy::FireLatestOnly.apply(&empty).is_empty());
        assert!(CatchupPolicy::Skip.apply(&empty).is_empty());
    }

    #[test]
    fn policy_default_is_latest_only() {
        assert_eq!(CatchupPolicy::default(), CatchupPolicy::FireLatestOnly);
    }

    #[test]
    fn policy_display_and_str() {
        assert_eq!(CatchupPolicy::FireAll.as_str(), "fire_all");
        assert_eq!(
            CatchupPolicy::FireLatestOnly.to_string(),
            "fire_latest_only"
        );
        assert_eq!(CatchupPolicy::Skip.to_string(), "skip");
    }

    #[test]
    fn missed_interval_simple() {
        // Every 60s. last_run at 00:00:00, now at 00:03:30.
        let sched = Schedule::interval(60);
        let last_run = ts(2026, 6, 13, 0, 0, 0);
        let now = ts(2026, 6, 13, 0, 3, 30);
        let missed = compute_missed(&sched, last_run, now).expect("missed");
        // Occurrences at 00:01, 00:02, 00:03 are <= now; 00:04 is future.
        assert_eq!(missed.len(), 3);
        assert_eq!(missed.earliest(), Some(ts(2026, 6, 13, 0, 1, 0)));
        assert_eq!(missed.latest(), Some(ts(2026, 6, 13, 0, 3, 0)));
        assert!(!missed.truncated);
    }

    #[test]
    fn missed_includes_occurrence_exactly_at_now() {
        let sched = Schedule::interval(60);
        let last_run = ts(2026, 6, 13, 0, 0, 0);
        // now lands exactly on an occurrence.
        let now = ts(2026, 6, 13, 0, 2, 0);
        let missed = compute_missed(&sched, last_run, now).expect("missed");
        // 00:01 and 00:02 (== now) are both missed.
        assert_eq!(missed.len(), 2);
        assert_eq!(missed.latest(), Some(now));
    }

    #[test]
    fn missed_none_when_now_before_last_run() {
        let sched = Schedule::interval(60);
        let last_run = ts(2026, 6, 13, 1, 0, 0);
        let now = ts(2026, 6, 13, 0, 0, 0);
        let missed = compute_missed(&sched, last_run, now).expect("missed");
        assert!(missed.is_empty());
    }

    #[test]
    fn missed_none_when_no_time_elapsed() {
        let sched = Schedule::interval(60);
        let t = ts(2026, 6, 13, 1, 0, 0);
        let missed = compute_missed(&sched, t, t).expect("missed");
        assert!(missed.is_empty());
    }

    #[test]
    fn missed_onetime_in_window() {
        let run_at = ts(2026, 6, 13, 1, 0, 0);
        let sched = Schedule::onetime(run_at);
        let last_run = ts(2026, 6, 13, 0, 0, 0);
        let now = ts(2026, 6, 13, 2, 0, 0);
        let missed = compute_missed(&sched, last_run, now).expect("missed");
        assert_eq!(missed.instants, vec![run_at]);
    }

    #[test]
    fn missed_onetime_in_future_is_empty() {
        let run_at = ts(2026, 6, 13, 5, 0, 0);
        let sched = Schedule::onetime(run_at);
        let last_run = ts(2026, 6, 13, 0, 0, 0);
        let now = ts(2026, 6, 13, 2, 0, 0);
        let missed = compute_missed(&sched, last_run, now).expect("missed");
        assert!(missed.is_empty());
    }

    #[test]
    fn catch_up_combines_compute_and_policy() {
        let sched = Schedule::interval(60);
        let last_run = ts(2026, 6, 13, 0, 0, 0);
        let now = ts(2026, 6, 13, 0, 3, 30);

        let all = catch_up(&sched, last_run, now, CatchupPolicy::FireAll).expect("all");
        assert_eq!(all.len(), 3);

        let latest =
            catch_up(&sched, last_run, now, CatchupPolicy::FireLatestOnly).expect("latest");
        assert_eq!(latest, vec![ts(2026, 6, 13, 0, 3, 0)]);

        let skip = catch_up(&sched, last_run, now, CatchupPolicy::Skip).expect("skip");
        assert!(skip.is_empty());
    }

    #[test]
    fn compute_missed_occurrences_helper_matches() {
        let sched = Schedule::interval(60);
        let last_run = ts(2026, 6, 13, 0, 0, 0);
        let now = ts(2026, 6, 13, 0, 3, 0);
        let list = compute_missed_occurrences(&sched, last_run, now).expect("list");
        let full = compute_missed(&sched, last_run, now).expect("full");
        assert_eq!(list, full.instants);
    }

    #[test]
    fn apply_policy_via_missed_struct() {
        let sched = Schedule::interval(60);
        let last_run = ts(2026, 6, 13, 0, 0, 0);
        let now = ts(2026, 6, 13, 0, 3, 0);
        let missed = compute_missed(&sched, last_run, now).expect("missed");
        assert_eq!(missed.apply_policy(CatchupPolicy::FireLatestOnly).len(), 1);
        assert_eq!(missed.apply_policy(CatchupPolicy::FireAll).len(), 3);
    }

    #[cfg(feature = "cron")]
    #[test]
    fn missed_weekday_schedule_skips_weekend() {
        // Weekday schedule at 09:00 (Mon-Fri). 2026-06-12 is a Friday.
        // last_run = Fri 2026-06-12 09:00. now = Mon 2026-06-15 10:00.
        // Saturday 13th and Sunday 14th must NOT count as missed; only Monday
        // 15th 09:00 is missed.
        let sched = Schedule::crontab("0", "9", "1-5", "*", "*");
        let last_run = ts(2026, 6, 12, 9, 0, 0);
        let now = ts(2026, 6, 15, 10, 0, 0);
        let missed = compute_missed(&sched, last_run, now).expect("missed");
        assert_eq!(missed.len(), 1);
        assert_eq!(missed.latest(), Some(ts(2026, 6, 15, 9, 0, 0)));
    }

    #[cfg(feature = "cron")]
    #[test]
    fn missed_hourly_three_runs() {
        let sched = Schedule::crontab("0", "*", "*", "*", "*");
        let last_run = ts(2026, 6, 13, 0, 0, 0);
        let now = ts(2026, 6, 13, 3, 30, 0);
        let missed = compute_missed(&sched, last_run, now).expect("missed");
        assert_eq!(missed.len(), 3);
        assert_eq!(missed.instants[0], ts(2026, 6, 13, 1, 0, 0));
        assert_eq!(missed.instants[2], ts(2026, 6, 13, 3, 0, 0));

        // FireLatestOnly -> single 03:00 run.
        let latest = CatchupPolicy::FireLatestOnly.apply(&missed.instants);
        assert_eq!(latest, vec![ts(2026, 6, 13, 3, 0, 0)]);
    }
}
