//! Schedule occurrence enumeration and conflict detection.
//!
//! This module provides two related capabilities built directly on top of the
//! existing [`Schedule::next_run`](crate::schedule::Schedule::next_run) logic:
//!
//! * [`enumerate_next_occurrences`] — produce the next `count` firing instants
//!   of a [`Schedule`] strictly after a reference time. This single helper is
//!   the shared primitive used both here (for conflict detection) and by the
//!   missed-task catch-up logic in [`crate::catchup`].
//! * [`detect_schedule_conflicts`] / [`detect_named_schedule_conflicts`] —
//!   given two or more schedules, find the instants at which they fire at the
//!   same moment within a bounded look-ahead window and report the conflicting
//!   pairs.
//!
//! Unlike the task-oriented conflict detection on
//! [`BeatScheduler`](crate::scheduler::BeatScheduler) (which reasons about
//! estimated execution windows of registered tasks), this module operates on
//! bare [`Schedule`] values and reports *exact-instant* collisions, which is
//! useful for validating new schedules before they are registered.
//!
//! Everything is implemented with [`chrono`]; no external dependencies.
//!
//! # Example
//!
//! ```
//! use celers_beat::conflict::{detect_schedule_conflicts, enumerate_next_occurrences};
//! use celers_beat::Schedule;
//! use chrono::{TimeZone, Utc};
//!
//! // Two interval schedules: one every 60s, one every 90s. They collide every
//! // 180 seconds (LCM of 60 and 90).
//! let after = Utc.with_ymd_and_hms(2026, 6, 13, 0, 0, 0).unwrap();
//! let a = Schedule::interval(60);
//! let b = Schedule::interval(90);
//!
//! let occ = enumerate_next_occurrences(&a, after, 3).unwrap();
//! assert_eq!(occ.len(), 3);
//!
//! // Look ahead 10 minutes (600s); expect collisions at +180s and +360s and
//! // +540s.
//! let conflicts = detect_schedule_conflicts(&[a, b], after, 600).unwrap();
//! assert_eq!(conflicts.len(), 3);
//! ```

use crate::config::ScheduleError;
use crate::schedule::Schedule;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

/// Hard cap on how many occurrences any single enumeration will produce. This
/// protects against schedules with a very short interval combined with a very
/// long window from allocating without bound.
const MAX_OCCURRENCES: usize = 100_000;

/// Enumerate the next `count` firing instants of `schedule`, all strictly after
/// `after`, in ascending order.
///
/// The enumeration reuses [`Schedule::next_run`] so it honours every schedule
/// type the crate supports (interval, crontab, solar, one-time). Semantics:
///
/// * **Interval / Crontab / Solar** — each successive occurrence is computed by
///   feeding the previous occurrence back into `next_run`, which always returns
///   a strictly-later instant. If a schedule type cannot produce a further
///   occurrence (e.g. a solar event search exhausts its horizon) enumeration
///   stops early and returns what it has so far.
/// * **One-time** — yields at most one occurrence: the scheduled instant, and
///   only when it is strictly after `after`.
///
/// `count == 0` returns an empty vector. The returned vector never exceeds
/// `MAX_OCCURRENCES` entries.
///
/// # Errors
///
/// Returns the first [`ScheduleError`] produced by the underlying
/// `next_run` evaluation (for example an invalid cron expression). A one-time
/// schedule that lies in the past is *not* an error — it simply yields no
/// occurrences.
pub fn enumerate_next_occurrences(
    schedule: &Schedule,
    after: DateTime<Utc>,
    count: usize,
) -> Result<Vec<DateTime<Utc>>, ScheduleError> {
    if count == 0 {
        return Ok(Vec::new());
    }
    let cap = count.min(MAX_OCCURRENCES);

    // One-time schedules are special: they fire at most once and `next_run`
    // only returns the instant when `last_run` is `None`. Handle them directly
    // so we never spuriously error or loop.
    if let Schedule::OneTime { run_at } = schedule {
        if *run_at > after {
            return Ok(vec![*run_at]);
        }
        return Ok(Vec::new());
    }

    let mut out = Vec::with_capacity(cap);
    let mut cursor = after;
    for _ in 0..cap {
        match schedule.next_run(Some(cursor)) {
            Ok(next) => {
                // Defensive: a well-behaved schedule returns a strictly-later
                // instant. If it ever fails to advance, stop to avoid an
                // infinite loop rather than emitting duplicates.
                if next <= cursor {
                    break;
                }
                out.push(next);
                cursor = next;
            }
            Err(ScheduleError::Invalid(_)) => {
                // No further future execution (e.g. solar horizon exhausted).
                break;
            }
            Err(other) => return Err(other),
        }
    }
    Ok(out)
}

/// Enumerate all occurrences of `schedule` that fall within the half-open
/// window `(after, after + window]`.
///
/// This is a convenience wrapper over [`enumerate_next_occurrences`] that stops
/// as soon as an occurrence passes the window boundary. The window length is
/// expressed in seconds and must be positive; a non-positive window yields an
/// empty vector.
pub fn enumerate_occurrences_in_window(
    schedule: &Schedule,
    after: DateTime<Utc>,
    window_seconds: i64,
) -> Result<Vec<DateTime<Utc>>, ScheduleError> {
    if window_seconds <= 0 {
        return Ok(Vec::new());
    }
    let end = after + Duration::seconds(window_seconds);

    if let Schedule::OneTime { run_at } = schedule {
        if *run_at > after && *run_at <= end {
            return Ok(vec![*run_at]);
        }
        return Ok(Vec::new());
    }

    let mut out = Vec::new();
    let mut cursor = after;
    for _ in 0..MAX_OCCURRENCES {
        match schedule.next_run(Some(cursor)) {
            Ok(next) => {
                if next <= cursor || next > end {
                    break;
                }
                out.push(next);
                cursor = next;
            }
            Err(ScheduleError::Invalid(_)) => break,
            Err(other) => return Err(other),
        }
    }
    Ok(out)
}

/// A single detected collision between two schedules: both fire at exactly the
/// same instant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OccurrenceConflict {
    /// Index of the first schedule in the input slice.
    pub first_index: usize,
    /// Index of the second schedule in the input slice (`first_index < second_index`).
    pub second_index: usize,
    /// Optional human-readable name of the first schedule.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_name: Option<String>,
    /// Optional human-readable name of the second schedule.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub second_name: Option<String>,
    /// The instant at which both schedules fire.
    pub instant: DateTime<Utc>,
}

impl OccurrenceConflict {
    /// Construct a conflict between two indexed schedules.
    pub fn new(first_index: usize, second_index: usize, instant: DateTime<Utc>) -> Self {
        Self {
            first_index,
            second_index,
            first_name: None,
            second_name: None,
            instant,
        }
    }

    /// Attach human-readable names to the conflicting schedules.
    pub fn with_names(
        mut self,
        first_name: impl Into<String>,
        second_name: impl Into<String>,
    ) -> Self {
        self.first_name = Some(first_name.into());
        self.second_name = Some(second_name.into());
        self
    }
}

impl std::fmt::Display for OccurrenceConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let first = self
            .first_name
            .clone()
            .unwrap_or_else(|| format!("#{}", self.first_index));
        let second = self
            .second_name
            .clone()
            .unwrap_or_else(|| format!("#{}", self.second_index));
        write!(
            f,
            "Conflict: {} <-> {} both fire at {}",
            first,
            second,
            self.instant.format("%Y-%m-%d %H:%M:%S UTC")
        )
    }
}

/// Detect exact-instant conflicts among two or more schedules within a
/// look-ahead window.
///
/// All schedules are enumerated over the half-open window
/// `(after, after + window]`. For every pair of schedules `(i, j)` with
/// `i < j`, each instant that appears in **both** schedules' occurrence sets is
/// reported as an [`OccurrenceConflict`]. The returned vector is sorted by
/// instant, then by `(first_index, second_index)`.
///
/// A `window_seconds` of zero or less yields no conflicts. Fewer than two
/// schedules likewise yields no conflicts.
///
/// # Errors
///
/// Propagates the first error from enumerating any schedule (for example an
/// invalid cron expression).
pub fn detect_schedule_conflicts(
    schedules: &[Schedule],
    after: DateTime<Utc>,
    window_seconds: i64,
) -> Result<Vec<OccurrenceConflict>, ScheduleError> {
    detect_conflicts_inner(schedules, &[], after, window_seconds)
}

/// Like [`detect_schedule_conflicts`] but accepts a parallel slice of names so
/// the resulting conflicts carry human-readable identifiers.
///
/// `names` must be the same length as `schedules`; otherwise a
/// [`ScheduleError::Invalid`] is returned.
pub fn detect_named_schedule_conflicts(
    schedules: &[Schedule],
    names: &[String],
    after: DateTime<Utc>,
    window_seconds: i64,
) -> Result<Vec<OccurrenceConflict>, ScheduleError> {
    if names.len() != schedules.len() {
        return Err(ScheduleError::Invalid(format!(
            "names length ({}) must match schedules length ({})",
            names.len(),
            schedules.len()
        )));
    }
    detect_conflicts_inner(schedules, names, after, window_seconds)
}

fn detect_conflicts_inner(
    schedules: &[Schedule],
    names: &[String],
    after: DateTime<Utc>,
    window_seconds: i64,
) -> Result<Vec<OccurrenceConflict>, ScheduleError> {
    use std::collections::BTreeSet;

    if schedules.len() < 2 || window_seconds <= 0 {
        return Ok(Vec::new());
    }

    // Enumerate each schedule's occurrences within the window once. Use a
    // BTreeSet per schedule so membership tests are fast and duplicate-free.
    let mut occurrence_sets: Vec<BTreeSet<DateTime<Utc>>> = Vec::with_capacity(schedules.len());
    for schedule in schedules {
        let occ = enumerate_occurrences_in_window(schedule, after, window_seconds)?;
        occurrence_sets.push(occ.into_iter().collect());
    }

    let mut conflicts = Vec::new();
    for i in 0..schedules.len() {
        for j in (i + 1)..schedules.len() {
            // Intersect the two occurrence sets.
            for instant in occurrence_sets[i].intersection(&occurrence_sets[j]) {
                let mut conflict = OccurrenceConflict::new(i, j, *instant);
                if !names.is_empty() {
                    conflict.first_name = Some(names[i].clone());
                    conflict.second_name = Some(names[j].clone());
                }
                conflicts.push(conflict);
            }
        }
    }

    conflicts.sort_by(|a, b| {
        a.instant
            .cmp(&b.instant)
            .then_with(|| a.first_index.cmp(&b.first_index))
            .then_with(|| a.second_index.cmp(&b.second_index))
    });
    Ok(conflicts)
}

/// Returns `true` if any pair of the given schedules collides within the
/// look-ahead window. Short-circuits on the first detected conflict.
pub fn has_schedule_conflicts(
    schedules: &[Schedule],
    after: DateTime<Utc>,
    window_seconds: i64,
) -> Result<bool, ScheduleError> {
    Ok(!detect_schedule_conflicts(schedules, after, window_seconds)?.is_empty())
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
    fn enumerate_interval_basic() {
        let after = ts(2026, 6, 13, 0, 0, 0);
        let sched = Schedule::interval(60);
        let occ = enumerate_next_occurrences(&sched, after, 3).expect("enumerate");
        assert_eq!(occ.len(), 3);
        assert_eq!(occ[0], ts(2026, 6, 13, 0, 1, 0));
        assert_eq!(occ[1], ts(2026, 6, 13, 0, 2, 0));
        assert_eq!(occ[2], ts(2026, 6, 13, 0, 3, 0));
    }

    #[test]
    fn enumerate_zero_count_is_empty() {
        let after = ts(2026, 6, 13, 0, 0, 0);
        let sched = Schedule::interval(60);
        assert!(enumerate_next_occurrences(&sched, after, 0)
            .expect("enumerate")
            .is_empty());
    }

    #[test]
    fn enumerate_onetime_future_yields_one() {
        let after = ts(2026, 6, 13, 0, 0, 0);
        let run_at = ts(2026, 6, 13, 1, 0, 0);
        let sched = Schedule::onetime(run_at);
        let occ = enumerate_next_occurrences(&sched, after, 5).expect("enumerate");
        assert_eq!(occ, vec![run_at]);
    }

    #[test]
    fn enumerate_onetime_past_yields_none() {
        let after = ts(2026, 6, 13, 2, 0, 0);
        let run_at = ts(2026, 6, 13, 1, 0, 0);
        let sched = Schedule::onetime(run_at);
        let occ = enumerate_next_occurrences(&sched, after, 5).expect("enumerate");
        assert!(occ.is_empty());
    }

    #[test]
    fn enumerate_window_interval() {
        let after = ts(2026, 6, 13, 0, 0, 0);
        let sched = Schedule::interval(60);
        // Window of 200s -> occurrences at 60, 120, 180 (240 is past the end).
        let occ = enumerate_occurrences_in_window(&sched, after, 200).expect("window");
        assert_eq!(occ.len(), 3);
        assert_eq!(*occ.last().unwrap(), ts(2026, 6, 13, 0, 3, 0));
    }

    #[test]
    fn conflict_interval_pair() {
        let after = ts(2026, 6, 13, 0, 0, 0);
        // 60s and 90s collide at multiples of 180s.
        let a = Schedule::interval(60);
        let b = Schedule::interval(90);
        let conflicts = detect_schedule_conflicts(&[a, b], after, 600).expect("detect");
        // 180, 360, 540 within 600s window.
        assert_eq!(conflicts.len(), 3);
        assert_eq!(conflicts[0].instant, ts(2026, 6, 13, 0, 3, 0));
        assert_eq!(conflicts[1].instant, ts(2026, 6, 13, 0, 6, 0));
        assert_eq!(conflicts[2].instant, ts(2026, 6, 13, 0, 9, 0));
        assert_eq!(conflicts[0].first_index, 0);
        assert_eq!(conflicts[0].second_index, 1);
    }

    #[test]
    fn no_conflict_when_coprime_misaligned() {
        let after = ts(2026, 6, 13, 0, 0, 0);
        // 60s and 60s offset is not expressible with bare intervals, but two
        // identical intervals collide every period.
        let a = Schedule::interval(60);
        let b = Schedule::interval(60);
        let conflicts = detect_schedule_conflicts(&[a, b], after, 180).expect("detect");
        // collisions at 60, 120, 180.
        assert_eq!(conflicts.len(), 3);
    }

    #[test]
    fn three_schedules_pairwise() {
        let after = ts(2026, 6, 13, 0, 0, 0);
        // 60, 120, 180: many overlaps. Just assert we get a sorted, non-empty
        // set and that indices are ordered.
        let scheds = vec![
            Schedule::interval(60),
            Schedule::interval(120),
            Schedule::interval(180),
        ];
        let conflicts = detect_schedule_conflicts(&scheds, after, 360).expect("detect");
        assert!(!conflicts.is_empty());
        for w in conflicts.windows(2) {
            assert!(w[0].instant <= w[1].instant);
        }
        for c in &conflicts {
            assert!(c.first_index < c.second_index);
        }
    }

    #[test]
    fn named_conflicts_carry_names() {
        let after = ts(2026, 6, 13, 0, 0, 0);
        let scheds = vec![Schedule::interval(60), Schedule::interval(60)];
        let names = vec!["alpha".to_string(), "beta".to_string()];
        let conflicts =
            detect_named_schedule_conflicts(&scheds, &names, after, 60).expect("detect");
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].first_name.as_deref(), Some("alpha"));
        assert_eq!(conflicts[0].second_name.as_deref(), Some("beta"));
        let rendered = conflicts[0].to_string();
        assert!(rendered.contains("alpha"));
        assert!(rendered.contains("beta"));
    }

    #[test]
    fn named_conflicts_length_mismatch_errors() {
        let after = ts(2026, 6, 13, 0, 0, 0);
        let scheds = vec![Schedule::interval(60), Schedule::interval(60)];
        let names = vec!["only-one".to_string()];
        let err = detect_named_schedule_conflicts(&scheds, &names, after, 60).unwrap_err();
        assert!(err.is_invalid());
    }

    #[test]
    fn fewer_than_two_schedules_no_conflict() {
        let after = ts(2026, 6, 13, 0, 0, 0);
        let scheds = vec![Schedule::interval(60)];
        assert!(detect_schedule_conflicts(&scheds, after, 600)
            .expect("detect")
            .is_empty());
    }

    #[test]
    fn zero_window_no_conflict() {
        let after = ts(2026, 6, 13, 0, 0, 0);
        let scheds = vec![Schedule::interval(60), Schedule::interval(60)];
        assert!(detect_schedule_conflicts(&scheds, after, 0)
            .expect("detect")
            .is_empty());
    }

    #[test]
    fn has_conflicts_helper() {
        let after = ts(2026, 6, 13, 0, 0, 0);
        let colliding = vec![Schedule::interval(60), Schedule::interval(60)];
        assert!(has_schedule_conflicts(&colliding, after, 60).expect("has"));

        let onetime_a = vec![
            Schedule::onetime(ts(2026, 6, 13, 0, 5, 0)),
            Schedule::onetime(ts(2026, 6, 13, 0, 6, 0)),
        ];
        assert!(!has_schedule_conflicts(&onetime_a, after, 600).expect("has"));
    }

    #[test]
    fn onetime_conflict_same_instant() {
        let after = ts(2026, 6, 13, 0, 0, 0);
        let instant = ts(2026, 6, 13, 0, 5, 0);
        let scheds = vec![Schedule::onetime(instant), Schedule::onetime(instant)];
        let conflicts = detect_schedule_conflicts(&scheds, after, 600).expect("detect");
        assert_eq!(conflicts.len(), 1);
        assert_eq!(conflicts[0].instant, instant);
    }

    #[cfg(feature = "cron")]
    #[test]
    fn enumerate_crontab_hourly() {
        // Every hour on the hour.
        let sched = Schedule::crontab("0", "*", "*", "*", "*");
        let after = ts(2026, 6, 13, 0, 30, 0);
        let occ = enumerate_next_occurrences(&sched, after, 3).expect("enumerate");
        assert_eq!(occ[0], ts(2026, 6, 13, 1, 0, 0));
        assert_eq!(occ[1], ts(2026, 6, 13, 2, 0, 0));
        assert_eq!(occ[2], ts(2026, 6, 13, 3, 0, 0));
    }

    #[cfg(feature = "cron")]
    #[test]
    fn conflict_crontab_vs_interval() {
        // Crontab "every hour" vs interval every 3600s, aligned at the hour.
        let cron = Schedule::crontab("0", "*", "*", "*", "*");
        let interval = Schedule::interval(3600);
        // Start exactly on the hour so the interval lands on the hour too.
        let after = ts(2026, 6, 13, 0, 0, 0);
        let conflicts =
            detect_schedule_conflicts(&[cron, interval], after, 3 * 3600).expect("detect");
        // Both fire at 01:00, 02:00, 03:00.
        assert_eq!(conflicts.len(), 3);
        assert_eq!(conflicts[0].instant, ts(2026, 6, 13, 1, 0, 0));
    }
}
