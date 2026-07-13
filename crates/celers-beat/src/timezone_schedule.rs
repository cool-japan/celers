//! Timezone-aware schedule evaluation.
//!
//! This module adds an *additive* timezone layer on top of the core
//! [`Schedule`] type. A [`Crontab`](crate::schedule::Schedule::Crontab)
//! schedule can be evaluated against a specific IANA timezone
//! ([`chrono_tz::Tz`]) so that its wall-clock fields (minute / hour / day /
//! month / day-of-week) are interpreted in *local* time and the resulting
//! instant is converted back to UTC. Daylight-saving-time (DST) transitions
//! are honoured: a spring-forward gap skips the non-existent local time, and a
//! fall-back overlap resolves deterministically to the earliest valid instant.
//!
//! The layer is strictly additive: when no timezone is attached, evaluation is
//! byte-for-byte identical to the existing UTC behaviour of
//! [`Schedule::next_run`](crate::schedule::Schedule::next_run).
//!
//! # Example
//!
//! ```
//! # #[cfg(feature = "cron")]
//! # {
//! use celers_beat::Schedule;
//! use chrono_tz::America::New_York;
//!
//! // 09:00 New-York wall-clock, every weekday.
//! let schedule = Schedule::crontab("0", "9", "1-5", "*", "*").with_timezone(New_York);
//! assert!(schedule.timezone_tz().is_some());
//! # }
//! ```

#![cfg(feature = "cron")]

use crate::config::ScheduleError;
use crate::schedule::Schedule;
use chrono::{DateTime, Utc};
use chrono_tz::Tz;

impl Schedule {
    /// Attach an IANA timezone to this schedule.
    ///
    /// The schedule's wall-clock fields are subsequently interpreted in the
    /// supplied timezone. This is **additive**: only [`Schedule::Crontab`]
    /// schedules depend on wall-clock fields, so the timezone is recorded on
    /// the crontab variant. For [`Schedule::Interval`] and
    /// [`Schedule::OneTime`] schedules (which are defined in absolute terms and
    /// therefore timezone-independent) the schedule is returned unchanged, so
    /// calling `with_timezone` on them is a harmless no-op.
    ///
    /// Passing [`chrono_tz::UTC`] is equivalent to leaving the schedule in its
    /// default (UTC) interpretation, but it is still recorded so that
    /// [`Schedule::timezone_tz`] round-trips.
    ///
    /// # Example
    /// ```
    /// # #[cfg(feature = "cron")]
    /// # {
    /// use celers_beat::Schedule;
    /// use chrono_tz::Europe::London;
    ///
    /// let schedule = Schedule::crontab("30", "14", "*", "*", "*").with_timezone(London);
    /// assert_eq!(schedule.timezone_tz(), Some(London));
    /// # }
    /// ```
    #[must_use]
    pub fn with_timezone(self, tz: Tz) -> Self {
        match self {
            Schedule::Crontab {
                minute,
                hour,
                day_of_week,
                day_of_month,
                month_of_year,
                ..
            } => Schedule::Crontab {
                minute,
                hour,
                day_of_week,
                day_of_month,
                month_of_year,
                timezone: Some(tz.name().to_string()),
            },
            // Interval / OneTime / Solar are absolute-time schedules: a
            // timezone does not change when they fire, so return them as-is.
            other => other,
        }
    }

    /// Return the timezone attached to this schedule, if any.
    ///
    /// Returns `None` when the schedule has no timezone (and therefore uses the
    /// default UTC interpretation), or when the schedule type does not carry a
    /// timezone. Returns `None` as well if the stored timezone string cannot be
    /// parsed back into a [`chrono_tz::Tz`] (which cannot happen for schedules
    /// built through the public API, but guards against hand-edited state).
    pub fn timezone_tz(&self) -> Option<Tz> {
        match self {
            Schedule::Crontab {
                timezone: Some(name),
                ..
            } => name.parse::<Tz>().ok(),
            _ => None,
        }
    }

    /// Return the IANA timezone name attached to this schedule, if any.
    pub fn timezone_name(&self) -> Option<&str> {
        match self {
            Schedule::Crontab {
                timezone: Some(name),
                ..
            } => Some(name.as_str()),
            _ => None,
        }
    }

    /// Whether this schedule carries an explicit timezone.
    pub fn has_timezone(&self) -> bool {
        self.timezone_name().is_some()
    }

    /// Compute the next run honouring the supplied timezone explicitly.
    ///
    /// This is equivalent to attaching `tz` via [`Schedule::with_timezone`] and
    /// calling [`Schedule::next_run`], but it does not allocate a new schedule.
    /// The `last_run` instant is still expressed in UTC; it is converted into
    /// `tz`, the next local occurrence is found, and the result is converted
    /// back to UTC.
    ///
    /// For non-crontab schedules the timezone is irrelevant and this simply
    /// delegates to [`Schedule::next_run`].
    ///
    /// # DST handling
    /// Wall-clock times that do not exist in `tz` (spring-forward gaps) are
    /// skipped, and ambiguous times (fall-back overlaps) resolve to the first
    /// matching instant, courtesy of the underlying `cron` evaluation over a
    /// DST-aware [`chrono_tz::Tz`].
    pub fn next_run_in_tz(
        &self,
        last_run: Option<DateTime<Utc>>,
        tz: Tz,
    ) -> Result<DateTime<Utc>, ScheduleError> {
        match self {
            Schedule::Crontab { .. } => self.clone().with_timezone(tz).next_run(last_run),
            // Absolute schedules ignore the timezone entirely.
            _ => self.next_run(last_run),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, TimeZone, Timelike, Weekday};
    use chrono_tz::America::New_York;
    use chrono_tz::Asia::Tokyo;
    use chrono_tz::Europe::London;
    use chrono_tz::UTC as TZ_UTC;

    #[test]
    fn with_timezone_records_iana_name_on_crontab() {
        let schedule = Schedule::crontab("0", "9", "1-5", "*", "*").with_timezone(New_York);
        assert_eq!(schedule.timezone_name(), Some("America/New_York"));
        assert_eq!(schedule.timezone_tz(), Some(New_York));
        assert!(schedule.has_timezone());
    }

    #[test]
    fn with_timezone_is_noop_for_interval() {
        let schedule = Schedule::interval(60).with_timezone(New_York);
        // Interval is absolute: no timezone is recorded and behaviour is
        // unchanged.
        assert!(!schedule.has_timezone());
        assert_eq!(schedule.timezone_tz(), None);
        let base = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let next = schedule.next_run(Some(base)).unwrap();
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 1, 1, 0, 1, 0).unwrap());
    }

    #[test]
    fn with_timezone_is_noop_for_onetime() {
        let run_at = Utc.with_ymd_and_hms(2026, 5, 1, 12, 0, 0).unwrap();
        let schedule = Schedule::onetime(run_at).with_timezone(Tokyo);
        assert!(!schedule.has_timezone());
        assert_eq!(schedule.next_run(None).unwrap(), run_at);
    }

    #[test]
    fn utc_default_unchanged_when_no_timezone() {
        // A crontab with no timezone must behave identically whether evaluated
        // through next_run (UTC) or next_run_in_tz(UTC).
        let schedule = Schedule::crontab("0", "9", "*", "*", "*");
        let start = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let via_utc = schedule.next_run(Some(start)).unwrap();
        assert_eq!(via_utc, Utc.with_ymd_and_hms(2026, 6, 1, 9, 0, 0).unwrap());
        let via_tz = schedule.next_run_in_tz(Some(start), TZ_UTC).unwrap();
        assert_eq!(via_tz, via_utc);
    }

    #[test]
    fn tokyo_nine_am_converts_to_utc_midnight() {
        // 09:00 in Tokyo (UTC+9, no DST) is 00:00 UTC.
        let schedule = Schedule::crontab("0", "9", "*", "*", "*").with_timezone(Tokyo);
        let start = Utc.with_ymd_and_hms(2026, 6, 1, 0, 0, 0).unwrap();
        let next = schedule.next_run(Some(start)).unwrap();
        // Local 2026-06-02 09:00 JST -> 2026-06-02 00:00 UTC (since start is
        // already at/after 2026-06-01 09:00 JST which is 2026-06-01 00:00 UTC).
        let local = next.with_timezone(&Tokyo);
        assert_eq!(local.hour(), 9);
        assert_eq!(local.minute(), 0);
    }

    #[test]
    fn new_york_nine_am_summer_is_utc_offset_minus_four() {
        // In summer New York observes EDT (UTC-4): 09:00 local -> 13:00 UTC.
        let schedule = Schedule::crontab("0", "9", "*", "*", "*").with_timezone(New_York);
        let start = Utc.with_ymd_and_hms(2026, 7, 1, 0, 0, 0).unwrap();
        let next = schedule.next_run(Some(start)).unwrap();
        let local = next.with_timezone(&New_York);
        assert_eq!(local.hour(), 9);
        assert_eq!(next.hour(), 13, "EDT is UTC-4, so 09:00 local is 13:00 UTC");
    }

    #[test]
    fn new_york_nine_am_winter_is_utc_offset_minus_five() {
        // In winter New York observes EST (UTC-5): 09:00 local -> 14:00 UTC.
        let schedule = Schedule::crontab("0", "9", "*", "*", "*").with_timezone(New_York);
        let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let next = schedule.next_run(Some(start)).unwrap();
        let local = next.with_timezone(&New_York);
        assert_eq!(local.hour(), 9);
        assert_eq!(next.hour(), 14, "EST is UTC-5, so 09:00 local is 14:00 UTC");
    }

    #[test]
    fn spring_forward_skips_nonexistent_local_time() {
        // 2026-03-08 is the US spring-forward Sunday: 02:00 -> 03:00, so the
        // wall-clock instant 02:30 does NOT exist that day. A "daily at 02:30"
        // schedule must skip 03-08 and fire next on 03-09 at 02:30 EDT.
        let schedule = Schedule::crontab("30", "2", "*", "*", "*").with_timezone(New_York);

        // Start at 2026-03-08 01:00 local (= 06:00 UTC, EST/UTC-5).
        let start = New_York
            .with_ymd_and_hms(2026, 3, 8, 1, 0, 0)
            .unwrap()
            .with_timezone(&Utc);

        let next = schedule.next_run(Some(start)).unwrap();
        let local = next.with_timezone(&New_York);

        // Must NOT be on the spring-forward day (02:30 doesn't exist then).
        assert_ne!(
            local.day(),
            8,
            "02:30 on the spring-forward day does not exist and must be skipped"
        );
        assert_eq!(local.day(), 9, "should fire the following day");
        assert_eq!(local.hour(), 2);
        assert_eq!(local.minute(), 30);
        // 02:30 EDT (UTC-4) on 2026-03-09 == 06:30 UTC.
        assert_eq!(next, Utc.with_ymd_and_hms(2026, 3, 9, 6, 30, 0).unwrap());
    }

    #[test]
    fn fall_back_overlap_resolves_deterministically() {
        // 2026-11-01 is the US fall-back Sunday: 02:00 -> 01:00, so 01:30
        // occurs twice. A "daily at 01:30" schedule must still produce a single
        // unambiguous UTC instant. We only require that it lands on 01:30 local
        // and that re-evaluation strictly advances (no infinite loop on the
        // duplicated hour).
        let schedule = Schedule::crontab("30", "1", "*", "*", "*").with_timezone(New_York);

        let start = New_York
            .with_ymd_and_hms(2026, 10, 31, 12, 0, 0)
            .unwrap()
            .with_timezone(&Utc);

        let first = schedule.next_run(Some(start)).unwrap();
        let first_local = first.with_timezone(&New_York);
        assert_eq!(first_local.hour(), 1);
        assert_eq!(first_local.minute(), 30);

        // The next occurrence after `first` must be strictly later.
        let second = schedule.next_run(Some(first)).unwrap();
        assert!(
            second > first,
            "evaluation must always advance past last_run"
        );
    }

    #[test]
    fn weekday_filter_respects_local_calendar() {
        // 09:00 New-York on weekdays. Starting on a Friday evening UTC, the
        // next local weekday occurrence must be a Monday-Friday in New York.
        let schedule = Schedule::crontab("0", "9", "1-5", "*", "*").with_timezone(New_York);
        let mut at = Utc.with_ymd_and_hms(2026, 3, 6, 23, 0, 0).unwrap(); // Fri 18:00 EST
        for _ in 0..5 {
            let next = schedule.next_run(Some(at)).unwrap();
            let wd = next.with_timezone(&New_York).weekday();
            assert!(
                matches!(
                    wd,
                    Weekday::Mon | Weekday::Tue | Weekday::Wed | Weekday::Thu | Weekday::Fri
                ),
                "timezone-aware weekday cron fired on local {wd:?}"
            );
            at = next;
        }
    }

    #[test]
    fn next_run_in_tz_matches_with_timezone() {
        let schedule = Schedule::crontab("0", "9", "*", "*", "*");
        let start = Utc.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap();
        let via_method = schedule.next_run_in_tz(Some(start), London).unwrap();
        let via_builder = schedule
            .clone()
            .with_timezone(London)
            .next_run(Some(start))
            .unwrap();
        assert_eq!(via_method, via_builder);
    }

    #[test]
    fn london_and_new_york_differ() {
        // The same wall-clock crontab in two zones yields different UTC instants.
        let london = Schedule::crontab("0", "12", "*", "*", "*").with_timezone(London);
        let ny = Schedule::crontab("0", "12", "*", "*", "*").with_timezone(New_York);
        let start = Utc.with_ymd_and_hms(2026, 1, 5, 0, 0, 0).unwrap();
        let next_london = london.next_run(Some(start)).unwrap();
        let next_ny = ny.next_run(Some(start)).unwrap();
        assert_ne!(next_london, next_ny);
    }
}
