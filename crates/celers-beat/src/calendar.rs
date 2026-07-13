//! Working-calendar utilities: recurring holidays and business-day arithmetic.
//!
//! This module builds on the [`HolidayCalendar`](crate::schedule::HolidayCalendar)
//! and [`DayOfWeek`] types from
//! [`crate::schedule`] and adds two capabilities required by the scheduling
//! roadmap:
//!
//! 1. **Recurring (annual) holidays** — a [`WorkingCalendar`] that knows about
//!    both fixed-date holidays (a single calendar date) and recurring holidays
//!    that fall on the same month/day every year, plus a configurable set of
//!    weekend days.
//! 2. **Business-day calculations** — [`WorkingCalendar::is_business_day`],
//!    [`WorkingCalendar::next_business_day`],
//!    [`WorkingCalendar::previous_business_day`],
//!    [`WorkingCalendar::add_business_days`] (the count may be negative) and
//!    [`WorkingCalendar::business_days_between`], all of which honour both the
//!    weekend definition and the holiday set.
//!
//! Everything is implemented with [`chrono`] and pure Rust — there are no
//! external calendar dependencies and no shortcuts.
//!
//! # Example
//!
//! ```
//! use celers_beat::calendar::WorkingCalendar;
//! use chrono::NaiveDate;
//!
//! let mut cal = WorkingCalendar::new();
//! // New Year's Day every year.
//! cal.add_recurring_holiday("New Year's Day", 1, 1);
//! // A one-off company holiday in 2026 only.
//! cal.add_fixed_holiday("Company Offsite", 2026, 3, 16);
//!
//! // 2026-01-01 is a Thursday but it is a recurring holiday.
//! let new_year = NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
//! assert!(cal.is_holiday(new_year));
//! assert!(!cal.is_business_day(new_year));
//!
//! // The first business day strictly after New Year's Day 2026 (a Thursday).
//! assert_eq!(
//!     cal.next_business_day(new_year),
//!     NaiveDate::from_ymd_opt(2026, 1, 2)
//! );
//! ```

use crate::schedule::DayOfWeek;
use chrono::{DateTime, Datelike, NaiveDate, Utc};
use serde::{Deserialize, Serialize};

/// Maximum number of days the business-day search routines will scan before
/// giving up. This guards against pathological calendars (e.g. every day a
/// holiday) so the routines always terminate. Ten years of calendar days is
/// far larger than any realistic run of consecutive non-business days.
const MAX_SCAN_DAYS: i64 = 3660;

/// A named holiday that may be either a fixed calendar date or a recurring
/// annual (month/day) holiday.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum CalendarHoliday {
    /// A holiday that occurs once on a specific calendar date.
    Fixed {
        /// Human-readable name of the holiday.
        name: String,
        /// Year of the holiday.
        year: i32,
        /// Month of the holiday (1-12).
        month: u32,
        /// Day of the holiday (1-31).
        day: u32,
    },
    /// A holiday that recurs on the same month/day every year.
    Recurring {
        /// Human-readable name of the holiday.
        name: String,
        /// Month of the holiday (1-12).
        month: u32,
        /// Day of the holiday (1-31).
        day: u32,
    },
}

impl CalendarHoliday {
    /// The display name of the holiday.
    pub fn name(&self) -> &str {
        match self {
            CalendarHoliday::Fixed { name, .. } => name,
            CalendarHoliday::Recurring { name, .. } => name,
        }
    }

    /// Returns `true` when this holiday falls on `date`.
    pub fn matches(&self, date: NaiveDate) -> bool {
        match self {
            CalendarHoliday::Fixed {
                year, month, day, ..
            } => date.year() == *year && date.month() == *month && date.day() == *day,
            CalendarHoliday::Recurring { month, day, .. } => {
                date.month() == *month && date.day() == *day
            }
        }
    }

    /// Returns `true` for recurring (annual) holidays.
    pub fn is_recurring(&self) -> bool {
        matches!(self, CalendarHoliday::Recurring { .. })
    }
}

/// A working calendar that combines a weekend definition with fixed and
/// recurring holidays, and exposes business-day arithmetic.
///
/// The weekend defaults to Saturday and Sunday but may be customised (for
/// example to model a Friday/Saturday weekend, or a six-day working week).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkingCalendar {
    /// All holidays known to the calendar.
    holidays: Vec<CalendarHoliday>,
    /// The set of weekdays treated as the weekend (non-business days).
    weekend: Vec<DayOfWeek>,
}

impl Default for WorkingCalendar {
    fn default() -> Self {
        Self::new()
    }
}

impl WorkingCalendar {
    /// Create a new calendar with no holidays and the default Saturday/Sunday
    /// weekend.
    pub fn new() -> Self {
        Self {
            holidays: Vec::new(),
            weekend: vec![DayOfWeek::Saturday, DayOfWeek::Sunday],
        }
    }

    /// Create a calendar with an explicit weekend definition and no holidays.
    ///
    /// Duplicate entries in `weekend` are removed so membership tests stay
    /// well-defined.
    pub fn with_weekend(weekend: impl IntoIterator<Item = DayOfWeek>) -> Self {
        let mut cal = Self {
            holidays: Vec::new(),
            weekend: Vec::new(),
        };
        cal.set_weekend(weekend);
        cal
    }

    /// Replace the weekend definition. Duplicate days are removed.
    pub fn set_weekend(&mut self, weekend: impl IntoIterator<Item = DayOfWeek>) {
        let mut days: Vec<DayOfWeek> = Vec::new();
        for day in weekend {
            if !days.contains(&day) {
                days.push(day);
            }
        }
        self.weekend = days;
    }

    /// The current weekend definition.
    pub fn weekend(&self) -> &[DayOfWeek] {
        &self.weekend
    }

    /// Returns `true` when `day` is part of the configured weekend.
    pub fn is_weekend_day(&self, day: DayOfWeek) -> bool {
        self.weekend.contains(&day)
    }

    /// Add a fixed-date (one-off) holiday.
    pub fn add_fixed_holiday(&mut self, name: impl Into<String>, year: i32, month: u32, day: u32) {
        self.holidays.push(CalendarHoliday::Fixed {
            name: name.into(),
            year,
            month,
            day,
        });
    }

    /// Add a recurring (annual) holiday that falls on the same month/day every
    /// year.
    pub fn add_recurring_holiday(&mut self, name: impl Into<String>, month: u32, day: u32) {
        self.holidays.push(CalendarHoliday::Recurring {
            name: name.into(),
            month,
            day,
        });
    }

    /// Add an already-constructed [`CalendarHoliday`].
    pub fn add_holiday(&mut self, holiday: CalendarHoliday) {
        self.holidays.push(holiday);
    }

    /// Remove every holiday whose name equals `name` (case-sensitive).
    ///
    /// Returns the number of holiday entries removed.
    pub fn remove_holiday_by_name(&mut self, name: &str) -> usize {
        let before = self.holidays.len();
        self.holidays.retain(|h| h.name() != name);
        before - self.holidays.len()
    }

    /// Remove every holiday that falls on `date` (fixed or recurring).
    ///
    /// Returns the number of holiday entries removed.
    pub fn remove_holiday_on(&mut self, date: NaiveDate) -> usize {
        let before = self.holidays.len();
        self.holidays.retain(|h| !h.matches(date));
        before - self.holidays.len()
    }

    /// The holidays currently held by the calendar.
    pub fn holidays(&self) -> &[CalendarHoliday] {
        &self.holidays
    }

    /// Number of holiday entries.
    pub fn len(&self) -> usize {
        self.holidays.len()
    }

    /// Returns `true` when no holidays are configured.
    pub fn is_empty(&self) -> bool {
        self.holidays.is_empty()
    }

    /// Returns `true` when `date` is a holiday (fixed or recurring).
    pub fn is_holiday(&self, date: NaiveDate) -> bool {
        self.holidays.iter().any(|h| h.matches(date))
    }

    /// Returns the name of the holiday falling on `date`, if any. When several
    /// holidays coincide the first one added wins.
    pub fn holiday_name(&self, date: NaiveDate) -> Option<&str> {
        self.holidays
            .iter()
            .find(|h| h.matches(date))
            .map(|h| h.name())
    }

    /// All holidays observed in `year`, returned as `(date, name)` pairs sorted
    /// by date. Recurring holidays are materialised for the requested year.
    pub fn holidays_in_year(&self, year: i32) -> Vec<(NaiveDate, String)> {
        let mut result: Vec<(NaiveDate, String)> = Vec::new();
        for holiday in &self.holidays {
            let candidate = match holiday {
                CalendarHoliday::Fixed {
                    year: hy,
                    month,
                    day,
                    name,
                } => {
                    if *hy == year {
                        NaiveDate::from_ymd_opt(year, *month, *day).map(|d| (d, name.clone()))
                    } else {
                        None
                    }
                }
                CalendarHoliday::Recurring { month, day, name } => {
                    NaiveDate::from_ymd_opt(year, *month, *day).map(|d| (d, name.clone()))
                }
            };
            if let Some(entry) = candidate {
                result.push(entry);
            }
        }
        result.sort_by(|a, b| a.0.cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        result
    }

    /// Returns `true` when `date` is a business day: neither a weekend day nor
    /// a holiday.
    pub fn is_business_day(&self, date: NaiveDate) -> bool {
        let day = DayOfWeek::from_chrono(date.weekday());
        !self.is_weekend_day(day) && !self.is_holiday(date)
    }

    /// The first business day strictly **after** `date`.
    ///
    /// Returns `None` only if no business day can be found within
    /// `MAX_SCAN_DAYS` (an effectively impossible calendar).
    pub fn next_business_day(&self, date: NaiveDate) -> Option<NaiveDate> {
        let mut current = date;
        for _ in 0..MAX_SCAN_DAYS {
            current = current.succ_opt()?;
            if self.is_business_day(current) {
                return Some(current);
            }
        }
        None
    }

    /// The first business day strictly **before** `date`.
    ///
    /// Returns `None` only if no business day can be found within
    /// `MAX_SCAN_DAYS`.
    pub fn previous_business_day(&self, date: NaiveDate) -> Option<NaiveDate> {
        let mut current = date;
        for _ in 0..MAX_SCAN_DAYS {
            current = current.pred_opt()?;
            if self.is_business_day(current) {
                return Some(current);
            }
        }
        None
    }

    /// The first business day on or **after** `date` (returns `date` itself
    /// when it is already a business day).
    pub fn business_day_on_or_after(&self, date: NaiveDate) -> Option<NaiveDate> {
        if self.is_business_day(date) {
            return Some(date);
        }
        self.next_business_day(date)
    }

    /// The first business day on or **before** `date` (returns `date` itself
    /// when it is already a business day).
    pub fn business_day_on_or_before(&self, date: NaiveDate) -> Option<NaiveDate> {
        if self.is_business_day(date) {
            return Some(date);
        }
        self.previous_business_day(date)
    }

    /// Add `n` business days to `date`.
    ///
    /// * `n == 0` returns `date` unchanged (even when `date` is a weekend or
    ///   holiday — no normalisation is performed for a zero offset).
    /// * `n > 0` steps forward `n` business days.
    /// * `n < 0` steps backward `|n|` business days.
    ///
    /// Returns `None` if the search exceeds `MAX_SCAN_DAYS` or runs off the
    /// representable date range.
    pub fn add_business_days(&self, date: NaiveDate, n: i64) -> Option<NaiveDate> {
        if n == 0 {
            return Some(date);
        }
        let mut remaining = n.unsigned_abs();
        let mut current = date;
        let forward = n > 0;
        for _ in 0..MAX_SCAN_DAYS {
            current = if forward {
                current.succ_opt()?
            } else {
                current.pred_opt()?
            };
            if self.is_business_day(current) {
                remaining -= 1;
                if remaining == 0 {
                    return Some(current);
                }
            }
        }
        None
    }

    /// Count the number of business days in the half-open interval
    /// `(start, end]` — i.e. business days strictly after `start` up to and
    /// including `end`.
    ///
    /// The result is **signed**: when `end` precedes `start` the count is
    /// negative and counts business days in `(end, start]` with a flipped sign.
    /// When `start == end` the result is `0`. This convention makes
    /// `add_business_days(start, business_days_between(start, end))` land on the
    /// next business day on/before `end` and round-trips cleanly for
    /// business-day endpoints.
    ///
    /// Returns `None` if the span exceeds `MAX_SCAN_DAYS`.
    pub fn business_days_between(&self, start: NaiveDate, end: NaiveDate) -> Option<i64> {
        use std::cmp::Ordering;
        match start.cmp(&end) {
            Ordering::Equal => Some(0),
            Ordering::Less => self.count_business_days_exclusive_inclusive(start, end),
            Ordering::Greater => self
                .count_business_days_exclusive_inclusive(end, start)
                .map(|c| -c),
        }
    }

    /// Count business days in `(lo, hi]` where `lo < hi`.
    fn count_business_days_exclusive_inclusive(&self, lo: NaiveDate, hi: NaiveDate) -> Option<i64> {
        let mut count = 0i64;
        let mut current = lo;
        for _ in 0..MAX_SCAN_DAYS {
            current = current.succ_opt()?;
            if current > hi {
                break;
            }
            if self.is_business_day(current) {
                count += 1;
            }
        }
        if current < hi {
            // We exhausted the scan budget before reaching `hi`.
            return None;
        }
        Some(count)
    }
}

/// Convenience: extract the `NaiveDate` (in UTC) of a timestamp so callers that
/// work with [`DateTime<Utc>`] can feed the date-based calendar API directly.
pub fn date_of(timestamp: DateTime<Utc>) -> NaiveDate {
    timestamp.date_naive()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schedule::DayOfWeek;

    fn d(y: i32, m: u32, day: u32) -> NaiveDate {
        NaiveDate::from_ymd_opt(y, m, day).expect("valid test date")
    }

    #[test]
    fn weekend_defaults_to_sat_sun() {
        let cal = WorkingCalendar::new();
        // 2026-06-13 is a Saturday, 2026-06-14 is a Sunday, 2026-06-12 a Friday.
        assert!(cal.is_weekend_day(DayOfWeek::Saturday));
        assert!(cal.is_weekend_day(DayOfWeek::Sunday));
        assert!(!cal.is_weekend_day(DayOfWeek::Monday));
        assert!(!cal.is_business_day(d(2026, 6, 13)));
        assert!(!cal.is_business_day(d(2026, 6, 14)));
        assert!(cal.is_business_day(d(2026, 6, 12)));
    }

    #[test]
    fn custom_weekend_friday_saturday() {
        let cal = WorkingCalendar::with_weekend([DayOfWeek::Friday, DayOfWeek::Saturday]);
        // 2026-06-12 Friday, 2026-06-13 Saturday, 2026-06-14 Sunday.
        assert!(!cal.is_business_day(d(2026, 6, 12))); // Friday weekend
        assert!(!cal.is_business_day(d(2026, 6, 13))); // Saturday weekend
        assert!(cal.is_business_day(d(2026, 6, 14))); // Sunday is a work day
    }

    #[test]
    fn set_weekend_dedups() {
        let mut cal = WorkingCalendar::new();
        cal.set_weekend([DayOfWeek::Sunday, DayOfWeek::Sunday, DayOfWeek::Saturday]);
        assert_eq!(cal.weekend().len(), 2);
    }

    #[test]
    fn fixed_and_recurring_holidays() {
        let mut cal = WorkingCalendar::new();
        cal.add_recurring_holiday("New Year's Day", 1, 1);
        cal.add_fixed_holiday("Company Offsite", 2026, 3, 16);

        // Recurring matches every year.
        assert!(cal.is_holiday(d(2025, 1, 1)));
        assert!(cal.is_holiday(d(2026, 1, 1)));
        assert!(cal.is_holiday(d(2030, 1, 1)));
        assert_eq!(cal.holiday_name(d(2026, 1, 1)), Some("New Year's Day"));

        // Fixed matches only that year.
        assert!(cal.is_holiday(d(2026, 3, 16)));
        assert!(!cal.is_holiday(d(2027, 3, 16)));
    }

    #[test]
    fn remove_holiday_by_name_and_date() {
        let mut cal = WorkingCalendar::new();
        cal.add_recurring_holiday("New Year's Day", 1, 1);
        cal.add_fixed_holiday("Offsite", 2026, 3, 16);
        cal.add_fixed_holiday("Offsite", 2027, 3, 16);

        assert_eq!(cal.remove_holiday_by_name("Offsite"), 2);
        assert!(cal.is_holiday(d(2026, 1, 1)));
        assert!(!cal.is_holiday(d(2026, 3, 16)));

        assert_eq!(cal.remove_holiday_on(d(2026, 1, 1)), 1);
        assert!(!cal.is_holiday(d(2026, 1, 1)));
        assert!(cal.is_empty());
    }

    #[test]
    fn holidays_in_year_materialises_recurring() {
        let mut cal = WorkingCalendar::new();
        cal.add_recurring_holiday("New Year's Day", 1, 1);
        cal.add_recurring_holiday("Christmas", 12, 25);
        cal.add_fixed_holiday("One-off 2026", 2026, 6, 10);
        cal.add_fixed_holiday("One-off 2025", 2025, 6, 10);

        let in_2026 = cal.holidays_in_year(2026);
        assert_eq!(in_2026.len(), 3);
        // Sorted by date: Jan 1, Jun 10, Dec 25.
        assert_eq!(in_2026[0].0, d(2026, 1, 1));
        assert_eq!(in_2026[1].0, d(2026, 6, 10));
        assert_eq!(in_2026[1].1, "One-off 2026");
        assert_eq!(in_2026[2].0, d(2026, 12, 25));
    }

    #[test]
    fn business_day_skips_weekend() {
        let cal = WorkingCalendar::new();
        // Friday 2026-06-12 -> next business day is Monday 2026-06-15.
        assert_eq!(cal.next_business_day(d(2026, 6, 12)), Some(d(2026, 6, 15)));
        // Monday 2026-06-15 -> previous business day is Friday 2026-06-12.
        assert_eq!(
            cal.previous_business_day(d(2026, 6, 15)),
            Some(d(2026, 6, 12))
        );
    }

    #[test]
    fn business_day_skips_holiday() {
        let mut cal = WorkingCalendar::new();
        // 2026-07-03 (Friday) declared a holiday; 2026-07-04 Sat, 07-05 Sun.
        cal.add_fixed_holiday("Independence Day (observed)", 2026, 7, 3);
        // Thursday 2026-07-02 -> next business day skips holiday Fri + weekend
        // -> Monday 2026-07-06.
        assert_eq!(cal.next_business_day(d(2026, 7, 2)), Some(d(2026, 7, 6)));
    }

    #[test]
    fn on_or_after_and_before() {
        let cal = WorkingCalendar::new();
        // Saturday 2026-06-13.
        assert_eq!(
            cal.business_day_on_or_after(d(2026, 6, 13)),
            Some(d(2026, 6, 15))
        );
        assert_eq!(
            cal.business_day_on_or_before(d(2026, 6, 13)),
            Some(d(2026, 6, 12))
        );
        // A weekday returns itself.
        assert_eq!(
            cal.business_day_on_or_after(d(2026, 6, 12)),
            Some(d(2026, 6, 12))
        );
    }

    #[test]
    fn add_business_days_positive() {
        let cal = WorkingCalendar::new();
        // Monday 2026-06-08 + 5 business days = Monday 2026-06-15.
        assert_eq!(
            cal.add_business_days(d(2026, 6, 8), 5),
            Some(d(2026, 6, 15))
        );
        // Friday 2026-06-12 + 1 business day skips the weekend -> Mon 06-15.
        assert_eq!(
            cal.add_business_days(d(2026, 6, 12), 1),
            Some(d(2026, 6, 15))
        );
    }

    #[test]
    fn add_business_days_negative() {
        let cal = WorkingCalendar::new();
        // Monday 2026-06-15 - 1 business day -> Friday 2026-06-12.
        assert_eq!(
            cal.add_business_days(d(2026, 6, 15), -1),
            Some(d(2026, 6, 12))
        );
        // Monday 2026-06-15 - 5 business days -> Monday 2026-06-08.
        assert_eq!(
            cal.add_business_days(d(2026, 6, 15), -5),
            Some(d(2026, 6, 8))
        );
    }

    #[test]
    fn add_business_days_zero_is_identity() {
        let cal = WorkingCalendar::new();
        // Even on a Saturday, n == 0 returns the same date untouched.
        assert_eq!(
            cal.add_business_days(d(2026, 6, 13), 0),
            Some(d(2026, 6, 13))
        );
    }

    #[test]
    fn add_business_days_with_holiday() {
        let mut cal = WorkingCalendar::new();
        cal.add_fixed_holiday("Holiday", 2026, 6, 10); // Wednesday
                                                       // Monday 2026-06-08 + 3 business days: Tue 09, [Wed 10 holiday skipped],
                                                       // Thu 11, Fri 12 -> 2026-06-12.
        assert_eq!(
            cal.add_business_days(d(2026, 6, 8), 3),
            Some(d(2026, 6, 12))
        );
    }

    #[test]
    fn business_days_between_basic() {
        let cal = WorkingCalendar::new();
        // (Mon 06-08, Fri 06-12] -> Tue,Wed,Thu,Fri = 4 business days.
        assert_eq!(
            cal.business_days_between(d(2026, 6, 8), d(2026, 6, 12)),
            Some(4)
        );
        // Across a weekend: (Fri 06-12, Mon 06-15] -> only Mon = 1.
        assert_eq!(
            cal.business_days_between(d(2026, 6, 12), d(2026, 6, 15)),
            Some(1)
        );
        // Same day -> 0.
        assert_eq!(
            cal.business_days_between(d(2026, 6, 12), d(2026, 6, 12)),
            Some(0)
        );
    }

    #[test]
    fn business_days_between_is_signed() {
        let cal = WorkingCalendar::new();
        let forward = cal
            .business_days_between(d(2026, 6, 8), d(2026, 6, 12))
            .expect("forward count");
        let backward = cal
            .business_days_between(d(2026, 6, 12), d(2026, 6, 8))
            .expect("backward count");
        assert_eq!(forward, 4);
        assert_eq!(backward, -4);
    }

    #[test]
    fn business_days_between_excludes_holidays() {
        let mut cal = WorkingCalendar::new();
        cal.add_fixed_holiday("Holiday", 2026, 6, 10); // Wednesday
                                                       // (Mon 06-08, Fri 06-12] minus the Wed holiday = Tue,Thu,Fri = 3.
        assert_eq!(
            cal.business_days_between(d(2026, 6, 8), d(2026, 6, 12)),
            Some(3)
        );
    }

    #[test]
    fn add_then_between_round_trips_for_business_days() {
        let cal = WorkingCalendar::new();
        let start = d(2026, 6, 8); // Monday, a business day.
        for n in 1..=10i64 {
            let target = cal.add_business_days(start, n).expect("add");
            assert_eq!(
                cal.business_days_between(start, target),
                Some(n),
                "round-trip failed for n={n}"
            );
        }
    }

    #[test]
    fn date_of_extracts_utc_date() {
        use chrono::TimeZone;
        let ts = Utc.with_ymd_and_hms(2026, 6, 13, 23, 59, 0).unwrap();
        assert_eq!(date_of(ts), d(2026, 6, 13));
    }
}
