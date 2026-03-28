//! Schedule types and calendar support
//!
//! This module contains the core `Schedule` enum and related calendar types
//! including business day calendars, holiday calendars, and day-of-week types.

use crate::config::ScheduleError;
use chrono::Datelike;
use chrono::{DateTime, Duration, Timelike, Utc};
#[cfg(feature = "cron")]
use chrono_tz::Tz;
use serde::{Deserialize, Serialize};

/// Schedule type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Schedule {
    /// Interval schedule (every N seconds)
    Interval {
        /// Interval in seconds
        every: u64,
    },

    /// Crontab schedule
    #[cfg(feature = "cron")]
    Crontab {
        /// Minute (0-59)
        minute: String,
        /// Hour (0-23)
        hour: String,
        /// Day of week (0-6, 0=Sunday)
        day_of_week: String,
        /// Day of month (1-31)
        day_of_month: String,
        /// Month (1-12)
        month_of_year: String,
        /// Timezone (IANA timezone name, e.g., "America/New_York")
        /// If None, uses UTC
        #[serde(default)]
        timezone: Option<String>,
    },

    /// Solar schedule (sunrise, sunset)
    #[cfg(feature = "solar")]
    Solar {
        /// Event type ("sunrise", "sunset")
        event: String,
        /// Latitude
        latitude: f64,
        /// Longitude
        longitude: f64,
    },

    /// One-time schedule (run once at specific time)
    OneTime {
        /// Exact run time (UTC)
        run_at: DateTime<Utc>,
    },
}

impl Schedule {
    /// Create interval schedule
    pub fn interval(seconds: u64) -> Self {
        Self::Interval { every: seconds }
    }

    /// Create crontab schedule (UTC)
    #[cfg(feature = "cron")]
    pub fn crontab(
        minute: &str,
        hour: &str,
        day_of_week: &str,
        day_of_month: &str,
        month_of_year: &str,
    ) -> Self {
        Self::Crontab {
            minute: minute.to_string(),
            hour: hour.to_string(),
            day_of_week: day_of_week.to_string(),
            day_of_month: day_of_month.to_string(),
            month_of_year: month_of_year.to_string(),
            timezone: None,
        }
    }

    /// Create crontab schedule with timezone
    ///
    /// # Arguments
    /// * `minute` - Minute field (0-59, *, */N)
    /// * `hour` - Hour field (0-23, *, */N)
    /// * `day_of_week` - Day of week field (0-6, *, */N)
    /// * `day_of_month` - Day of month field (1-31, *, */N)
    /// * `month_of_year` - Month field (1-12, *, */N)
    /// * `timezone` - IANA timezone name (e.g., "America/New_York", "Europe/London")
    ///
    /// # Examples
    /// ```
    /// use celers_beat::Schedule;
    ///
    /// // Run at 9:00 AM New York time every weekday
    /// let schedule = Schedule::crontab_tz(
    ///     "0",
    ///     "9",
    ///     "1-5",
    ///     "*",
    ///     "*",
    ///     "America/New_York"
    /// );
    /// ```
    #[cfg(feature = "cron")]
    pub fn crontab_tz(
        minute: &str,
        hour: &str,
        day_of_week: &str,
        day_of_month: &str,
        month_of_year: &str,
        timezone: &str,
    ) -> Self {
        Self::Crontab {
            minute: minute.to_string(),
            hour: hour.to_string(),
            day_of_week: day_of_week.to_string(),
            day_of_month: day_of_month.to_string(),
            month_of_year: month_of_year.to_string(),
            timezone: Some(timezone.to_string()),
        }
    }

    /// Create solar schedule
    #[cfg(feature = "solar")]
    pub fn solar(event: &str, latitude: f64, longitude: f64) -> Self {
        Self::Solar {
            event: event.to_string(),
            latitude,
            longitude,
        }
    }

    /// Create one-time schedule
    pub fn onetime(run_at: DateTime<Utc>) -> Self {
        Self::OneTime { run_at }
    }

    /// Calculate next run time
    pub fn next_run(
        &self,
        last_run: Option<DateTime<Utc>>,
    ) -> Result<DateTime<Utc>, ScheduleError> {
        match self {
            Schedule::Interval { every } => {
                let base = last_run.unwrap_or_else(Utc::now);
                Ok(base + Duration::seconds(*every as i64))
            }
            #[cfg(feature = "cron")]
            Schedule::Crontab {
                minute,
                hour,
                day_of_week,
                day_of_month,
                month_of_year,
                timezone,
            } => {
                use cron::Schedule as CronSchedule;
                use std::str::FromStr;

                // Build cron expression from fields
                // Cron format: sec min hour day month day_of_week year
                // We use "0" for seconds and "*" for year
                let cron_expr = format!(
                    "0 {} {} {} {} {} *",
                    minute, hour, day_of_month, month_of_year, day_of_week
                );

                let cron_schedule = CronSchedule::from_str(&cron_expr)
                    .map_err(|e| ScheduleError::Parse(format!("Invalid cron expression: {}", e)))?;

                // If timezone is specified, convert to/from that timezone
                if let Some(tz_str) = timezone {
                    let tz: Tz = tz_str.parse().map_err(|_| {
                        ScheduleError::Parse(format!("Invalid timezone: {}", tz_str))
                    })?;

                    // Convert current UTC time to target timezone
                    let after_utc = last_run.unwrap_or_else(Utc::now);
                    let after_tz = after_utc.with_timezone(&tz);

                    // Find next occurrence in target timezone
                    let next_tz = cron_schedule.after(&after_tz).next().ok_or_else(|| {
                        ScheduleError::Invalid("No future execution time".to_string())
                    })?;

                    // Convert back to UTC
                    Ok(next_tz.with_timezone(&Utc))
                } else {
                    // No timezone specified, use UTC
                    let after = last_run.unwrap_or_else(Utc::now);
                    let next = cron_schedule.after(&after).next().ok_or_else(|| {
                        ScheduleError::Invalid("No future execution time".to_string())
                    })?;

                    Ok(next)
                }
            }
            #[cfg(feature = "solar")]
            Schedule::Solar {
                event,
                latitude,
                longitude,
            } => {
                #[allow(deprecated)]
                use sunrise::sunrise_sunset;

                // Start from last_run or now
                let start_time = last_run.unwrap_or_else(Utc::now);
                let mut current_date = start_time.date_naive();

                // Search for next occurrence (up to 365 days ahead)
                for _ in 0..365 {
                    #[allow(deprecated)]
                    let (sunrise_time, sunset_time) = sunrise_sunset(
                        *latitude,
                        *longitude,
                        current_date.year(),
                        current_date.month(),
                        current_date.day(),
                    );

                    let event_time = match event.to_lowercase().as_str() {
                        "sunrise" => {
                            // sunrise_time is minutes since midnight
                            let hours = (sunrise_time / 60) as u32;
                            let minutes = (sunrise_time % 60) as u32;
                            current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunrise time: {} minutes",
                                        sunrise_time
                                    ))
                                })?
                                .and_utc()
                        }
                        "sunset" => {
                            // sunset_time is minutes since midnight
                            let hours = (sunset_time / 60) as u32;
                            let minutes = (sunset_time % 60) as u32;
                            current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunset time: {} minutes",
                                        sunset_time
                                    ))
                                })?
                                .and_utc()
                        }
                        // Civil twilight (sun 6° below horizon) - approximate 30 min before/after sunrise/sunset
                        "civil_twilight_begin" | "dawn" => {
                            let hours = (sunrise_time / 60) as u32;
                            let minutes = (sunrise_time % 60) as u32;
                            let sunrise = current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunrise time: {} minutes",
                                        sunrise_time
                                    ))
                                })?
                                .and_utc();
                            sunrise - Duration::minutes(30)
                        }
                        "civil_twilight_end" | "dusk" => {
                            let hours = (sunset_time / 60) as u32;
                            let minutes = (sunset_time % 60) as u32;
                            let sunset = current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunset time: {} minutes",
                                        sunset_time
                                    ))
                                })?
                                .and_utc();
                            sunset + Duration::minutes(30)
                        }
                        // Nautical twilight (sun 12° below horizon) - approximate 60 min before/after sunrise/sunset
                        "nautical_twilight_begin" => {
                            let hours = (sunrise_time / 60) as u32;
                            let minutes = (sunrise_time % 60) as u32;
                            let sunrise = current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunrise time: {} minutes",
                                        sunrise_time
                                    ))
                                })?
                                .and_utc();
                            sunrise - Duration::minutes(60)
                        }
                        "nautical_twilight_end" => {
                            let hours = (sunset_time / 60) as u32;
                            let minutes = (sunset_time % 60) as u32;
                            let sunset = current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunset time: {} minutes",
                                        sunset_time
                                    ))
                                })?
                                .and_utc();
                            sunset + Duration::minutes(60)
                        }
                        // Astronomical twilight (sun 18° below horizon) - approximate 90 min before/after sunrise/sunset
                        "astronomical_twilight_begin" => {
                            let hours = (sunrise_time / 60) as u32;
                            let minutes = (sunrise_time % 60) as u32;
                            let sunrise = current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunrise time: {} minutes",
                                        sunrise_time
                                    ))
                                })?
                                .and_utc();
                            sunrise - Duration::minutes(90)
                        }
                        "astronomical_twilight_end" => {
                            let hours = (sunset_time / 60) as u32;
                            let minutes = (sunset_time % 60) as u32;
                            let sunset = current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunset time: {} minutes",
                                        sunset_time
                                    ))
                                })?
                                .and_utc();
                            sunset + Duration::minutes(90)
                        }
                        // Golden hour (sun 0-6° above horizon) - approximate 30 min after sunrise / before sunset
                        "golden_hour_begin" => {
                            let hours = (sunrise_time / 60) as u32;
                            let minutes = (sunrise_time % 60) as u32;
                            current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunrise time: {} minutes",
                                        sunrise_time
                                    ))
                                })?
                                .and_utc()
                            // Golden hour starts at sunrise
                        }
                        "golden_hour_end" => {
                            let hours = (sunset_time / 60) as u32;
                            let minutes = (sunset_time % 60) as u32;
                            let sunset = current_date
                                .and_hms_opt(hours, minutes, 0)
                                .ok_or_else(|| {
                                    ScheduleError::Invalid(format!(
                                        "Invalid sunset time: {} minutes",
                                        sunset_time
                                    ))
                                })?
                                .and_utc();
                            // Golden hour ends ~30 min before sunset
                            sunset - Duration::minutes(30)
                        }
                        _ => {
                            return Err(ScheduleError::Invalid(format!(
                                "Unknown solar event: {}. Supported events: sunrise, sunset, civil_twilight_begin/end, nautical_twilight_begin/end, astronomical_twilight_begin/end, golden_hour_begin/end, dawn, dusk",
                                event
                            )))
                        }
                    };

                    // If this event time is in the future, return it
                    if event_time > start_time {
                        return Ok(event_time);
                    }

                    // Move to next day
                    current_date = current_date
                        .checked_add_days(chrono::Days::new(1))
                        .ok_or_else(|| ScheduleError::Invalid("Date overflow".to_string()))?;
                }

                Err(ScheduleError::Invalid(
                    "Could not find solar event in next 365 days".to_string(),
                ))
            }
            Schedule::OneTime { run_at } => {
                // If never run before, return the scheduled time
                // If already run, return error (one-time schedules don't repeat)
                if last_run.is_some() {
                    Err(ScheduleError::Invalid(
                        "One-time schedule has already been executed".to_string(),
                    ))
                } else {
                    Ok(*run_at)
                }
            }
        }
    }

    /// Check if this is an interval schedule
    pub fn is_interval(&self) -> bool {
        matches!(self, Schedule::Interval { .. })
    }

    /// Check if this is a crontab schedule
    #[cfg(feature = "cron")]
    pub fn is_crontab(&self) -> bool {
        matches!(self, Schedule::Crontab { .. })
    }

    /// Check if this is a solar schedule
    #[cfg(feature = "solar")]
    pub fn is_solar(&self) -> bool {
        matches!(self, Schedule::Solar { .. })
    }

    /// Check if this is a one-time schedule
    pub fn is_onetime(&self) -> bool {
        matches!(self, Schedule::OneTime { .. })
    }
}

impl std::fmt::Display for Schedule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Schedule::Interval { every } => write!(f, "Interval[every {}s]", every),
            #[cfg(feature = "cron")]
            Schedule::Crontab {
                minute,
                hour,
                day_of_week,
                day_of_month,
                month_of_year,
                timezone,
            } => {
                if let Some(tz) = timezone {
                    write!(
                        f,
                        "Crontab[{} {} {} {} {} ({})]",
                        minute, hour, day_of_month, day_of_week, month_of_year, tz
                    )
                } else {
                    write!(
                        f,
                        "Crontab[{} {} {} {} {} (UTC)]",
                        minute, hour, day_of_month, day_of_week, month_of_year
                    )
                }
            }
            #[cfg(feature = "solar")]
            Schedule::Solar {
                event,
                latitude,
                longitude,
            } => write!(f, "Solar[{} at ({:.4}, {:.4})]", event, latitude, longitude),
            Schedule::OneTime { run_at } => {
                write!(f, "OneTime[at {}]", run_at.format("%Y-%m-%d %H:%M:%S UTC"))
            }
        }
    }
}

// ============================================================================
// Business Day Calendar
// ============================================================================

/// Day of week
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DayOfWeek {
    Monday,
    Tuesday,
    Wednesday,
    Thursday,
    Friday,
    Saturday,
    Sunday,
}

impl DayOfWeek {
    /// Check if this is a weekend day (Saturday or Sunday)
    pub fn is_weekend(&self) -> bool {
        matches!(self, DayOfWeek::Saturday | DayOfWeek::Sunday)
    }

    /// Check if this is a weekday (Monday-Friday)
    pub fn is_weekday(&self) -> bool {
        !self.is_weekend()
    }

    /// Convert from chrono Weekday
    pub fn from_chrono(weekday: chrono::Weekday) -> Self {
        match weekday {
            chrono::Weekday::Mon => DayOfWeek::Monday,
            chrono::Weekday::Tue => DayOfWeek::Tuesday,
            chrono::Weekday::Wed => DayOfWeek::Wednesday,
            chrono::Weekday::Thu => DayOfWeek::Thursday,
            chrono::Weekday::Fri => DayOfWeek::Friday,
            chrono::Weekday::Sat => DayOfWeek::Saturday,
            chrono::Weekday::Sun => DayOfWeek::Sunday,
        }
    }
}

/// Business hours configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusinessHours {
    /// Start hour (0-23)
    pub start_hour: u32,
    /// End hour (0-23)
    pub end_hour: u32,
}

impl BusinessHours {
    /// Create a new business hours configuration
    ///
    /// # Arguments
    /// * `start_hour` - Start hour (0-23)
    /// * `end_hour` - End hour (0-23)
    pub fn new(start_hour: u32, end_hour: u32) -> Self {
        Self {
            start_hour,
            end_hour,
        }
    }

    /// Standard business hours (9 AM - 5 PM)
    pub fn standard() -> Self {
        Self {
            start_hour: 9,
            end_hour: 17,
        }
    }

    /// Check if a given hour is within business hours
    pub fn is_business_hour(&self, hour: u32) -> bool {
        hour >= self.start_hour && hour < self.end_hour
    }

    /// Check if a given time is within business hours
    pub fn is_within(&self, time: &DateTime<Utc>) -> bool {
        self.is_business_hour(time.hour())
    }
}

/// Business day calendar configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BusinessCalendar {
    /// Business hours configuration
    pub business_hours: BusinessHours,
    /// Working days (defaults to Monday-Friday)
    #[serde(default = "default_working_days")]
    pub working_days: Vec<DayOfWeek>,
}

#[allow(dead_code)]
fn default_working_days() -> Vec<DayOfWeek> {
    vec![
        DayOfWeek::Monday,
        DayOfWeek::Tuesday,
        DayOfWeek::Wednesday,
        DayOfWeek::Thursday,
        DayOfWeek::Friday,
    ]
}

impl BusinessCalendar {
    /// Create a new business calendar with standard hours (9 AM - 5 PM, Mon-Fri)
    pub fn standard() -> Self {
        Self {
            business_hours: BusinessHours::standard(),
            working_days: default_working_days(),
        }
    }

    /// Create a custom business calendar
    pub fn new(business_hours: BusinessHours, working_days: Vec<DayOfWeek>) -> Self {
        Self {
            business_hours,
            working_days,
        }
    }

    /// Check if a given day of week is a working day
    pub fn is_working_day(&self, day: DayOfWeek) -> bool {
        self.working_days.contains(&day)
    }

    /// Check if a given date/time is within business hours
    pub fn is_business_time(&self, time: &DateTime<Utc>) -> bool {
        let day = DayOfWeek::from_chrono(time.weekday());
        self.is_working_day(day) && self.business_hours.is_within(time)
    }

    /// Find the next business time after the given time
    ///
    /// This will advance to the next business day/hour if necessary.
    pub fn next_business_time(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        let mut current = time;

        // Try up to 14 days (2 weeks) to find next business time
        for _ in 0..14 {
            let day = DayOfWeek::from_chrono(current.weekday());

            if self.is_working_day(day) {
                // Check if we're in business hours
                let hour = current.hour();
                if hour < self.business_hours.start_hour {
                    // Before business hours - move to start of business hours today
                    current = current
                        .with_hour(self.business_hours.start_hour)
                        .unwrap()
                        .with_minute(0)
                        .unwrap()
                        .with_second(0)
                        .unwrap();
                    return current;
                } else if hour < self.business_hours.end_hour {
                    // Within business hours - this is valid
                    return current;
                }
                // After business hours - fall through to next day
            }

            // Move to start of next day
            current = (current + Duration::days(1))
                .with_hour(self.business_hours.start_hour)
                .unwrap()
                .with_minute(0)
                .unwrap()
                .with_second(0)
                .unwrap();
        }

        current
    }
}

// ============================================================================
// Holiday Calendar
// ============================================================================

/// Holiday definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Holiday {
    /// Holiday name
    pub name: String,
    /// Date (year, month, day)
    pub date: (i32, u32, u32),
}

impl Holiday {
    /// Create a new holiday
    pub fn new(name: impl Into<String>, year: i32, month: u32, day: u32) -> Self {
        Self {
            name: name.into(),
            date: (year, month, day),
        }
    }

    /// Check if this holiday matches the given date
    pub fn matches(&self, date: &DateTime<Utc>) -> bool {
        let (year, month, day) = self.date;
        date.year() == year && date.month() == month && date.day() == day
    }
}

/// Holiday calendar
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct HolidayCalendar {
    /// List of holidays
    holidays: Vec<Holiday>,
}

impl HolidayCalendar {
    /// Create a new empty holiday calendar
    pub fn new() -> Self {
        Self {
            holidays: Vec::new(),
        }
    }

    /// Add a holiday to the calendar
    pub fn add_holiday(&mut self, holiday: Holiday) {
        self.holidays.push(holiday);
    }

    /// Add a holiday by date components
    pub fn add(&mut self, name: impl Into<String>, year: i32, month: u32, day: u32) {
        self.holidays.push(Holiday::new(name, year, month, day));
    }

    /// Check if a given date is a holiday
    pub fn is_holiday(&self, date: &DateTime<Utc>) -> bool {
        self.holidays.iter().any(|h| h.matches(date))
    }

    /// Get the holiday for a given date, if any
    pub fn get_holiday(&self, date: &DateTime<Utc>) -> Option<&Holiday> {
        self.holidays.iter().find(|h| h.matches(date))
    }

    /// Get the number of holidays in this calendar
    pub fn len(&self) -> usize {
        self.holidays.len()
    }

    /// Check if the calendar is empty
    pub fn is_empty(&self) -> bool {
        self.holidays.is_empty()
    }

    /// Find the next non-holiday date after the given time
    pub fn next_non_holiday(&self, time: DateTime<Utc>) -> DateTime<Utc> {
        let mut current = time;

        // Try up to 365 days to find a non-holiday
        for _ in 0..365 {
            if !self.is_holiday(&current) {
                return current;
            }
            current += Duration::days(1);
        }

        current
    }

    /// Create a US federal holidays calendar for a given year
    ///
    /// Includes all 11 US federal holidays:
    /// - New Year's Day (January 1)
    /// - Martin Luther King Jr Day (3rd Monday in January)
    /// - Presidents Day (3rd Monday in February)
    /// - Memorial Day (Last Monday in May)
    /// - Juneteenth (June 19)
    /// - Independence Day (July 4)
    /// - Labor Day (1st Monday in September)
    /// - Columbus Day (2nd Monday in October)
    /// - Veterans Day (November 11)
    /// - Thanksgiving Day (4th Thursday in November)
    /// - Christmas Day (December 25)
    pub fn us_federal(year: i32) -> Self {
        let mut calendar = Self::new();

        // New Year's Day (January 1)
        calendar.add("New Year's Day", year, 1, 1);

        // Martin Luther King Jr Day (3rd Monday in January)
        if let Some((_, day)) = Self::nth_weekday(year, 1, 1, 3) {
            calendar.add("Martin Luther King Jr Day", year, 1, day);
        }

        // Presidents Day (3rd Monday in February)
        if let Some((_, day)) = Self::nth_weekday(year, 2, 1, 3) {
            calendar.add("Presidents Day", year, 2, day);
        }

        // Memorial Day (Last Monday in May)
        if let Some((_, day)) = Self::last_weekday(year, 5, 1) {
            calendar.add("Memorial Day", year, 5, day);
        }

        // Juneteenth (June 19)
        calendar.add("Juneteenth", year, 6, 19);

        // Independence Day (July 4)
        calendar.add("Independence Day", year, 7, 4);

        // Labor Day (1st Monday in September)
        if let Some((_, day)) = Self::nth_weekday(year, 9, 1, 1) {
            calendar.add("Labor Day", year, 9, day);
        }

        // Columbus Day (2nd Monday in October)
        if let Some((_, day)) = Self::nth_weekday(year, 10, 1, 2) {
            calendar.add("Columbus Day", year, 10, day);
        }

        // Veterans Day (November 11)
        calendar.add("Veterans Day", year, 11, 11);

        // Thanksgiving Day (4th Thursday in November)
        if let Some((_, day)) = Self::nth_weekday(year, 11, 4, 4) {
            calendar.add("Thanksgiving Day", year, 11, day);
        }

        // Christmas Day (December 25)
        calendar.add("Christmas Day", year, 12, 25);

        calendar
    }

    /// Calculate the Nth occurrence of a weekday in a given month
    ///
    /// # Arguments
    /// * `year` - Year
    /// * `month` - Month (1-12)
    /// * `weekday` - Day of week (0=Sunday, 1=Monday, ..., 6=Saturday)
    /// * `nth` - Which occurrence (1=first, 2=second, etc.)
    ///
    /// # Returns
    /// `Some((month, day))` if found, `None` otherwise
    fn nth_weekday(year: i32, month: u32, weekday: u32, nth: u32) -> Option<(u32, u32)> {
        use chrono::NaiveDate;

        let first_day = NaiveDate::from_ymd_opt(year, month, 1)?;
        let first_weekday = first_day.weekday().num_days_from_sunday();

        // Calculate days until first occurrence of target weekday
        let days_until_first = if weekday >= first_weekday {
            weekday - first_weekday
        } else {
            7 - (first_weekday - weekday)
        };

        // Calculate the date of the nth occurrence
        let target_day = 1 + days_until_first + (nth - 1) * 7;

        // Validate the day exists in the month
        if NaiveDate::from_ymd_opt(year, month, target_day).is_some() {
            Some((month, target_day))
        } else {
            None
        }
    }

    /// Find the last occurrence of a weekday in a given month
    ///
    /// # Arguments
    /// * `year` - Year
    /// * `month` - Month (1-12)
    /// * `weekday` - Day of week (0=Sunday, 1=Monday, ..., 6=Saturday)
    ///
    /// # Returns
    /// `Some((month, day))` if found, `None` otherwise
    fn last_weekday(year: i32, month: u32, weekday: u32) -> Option<(u32, u32)> {
        use chrono::NaiveDate;

        // Start from the last day of the month and work backwards
        let next_month = if month == 12 { 1 } else { month + 1 };
        let next_year = if month == 12 { year + 1 } else { year };
        let last_day = NaiveDate::from_ymd_opt(next_year, next_month, 1)?.pred_opt()?;

        // Search backwards for the target weekday
        for days_back in 0..7 {
            if let Some(date) = last_day.checked_sub_signed(Duration::days(days_back)) {
                if date.weekday().num_days_from_sunday() == weekday {
                    return Some((month, date.day()));
                }
            }
        }

        None
    }

    /// Create a Japan national holidays calendar for a given year
    ///
    /// Includes all 16 Japanese national holidays:
    /// - New Year's Day (January 1)
    /// - Coming of Age Day (2nd Monday in January)
    /// - National Foundation Day (February 11)
    /// - Emperor's Birthday (February 23)
    /// - Vernal Equinox Day (March 20 - approximate)
    /// - Showa Day (April 29)
    /// - Constitution Memorial Day (May 3)
    /// - Greenery Day (May 4)
    /// - Children's Day (May 5)
    /// - Marine Day (3rd Monday in July)
    /// - Mountain Day (August 11)
    /// - Respect for the Aged Day (3rd Monday in September)
    /// - Autumnal Equinox Day (September 23 - approximate)
    /// - Sports Day (2nd Monday in October)
    /// - Culture Day (November 3)
    /// - Labor Thanksgiving Day (November 23)
    pub fn japan(year: i32) -> Self {
        let mut calendar = Self::new();

        // New Year's Day (January 1)
        calendar.add("New Year's Day", year, 1, 1);

        // Coming of Age Day (2nd Monday in January)
        if let Some((_, day)) = Self::nth_weekday(year, 1, 1, 2) {
            calendar.add("Coming of Age Day", year, 1, day);
        }

        // National Foundation Day (February 11)
        calendar.add("National Foundation Day", year, 2, 11);

        // Emperor's Birthday (February 23)
        calendar.add("Emperor's Birthday", year, 2, 23);

        // Vernal Equinox Day (around March 20-21, using March 20 as approximation)
        calendar.add("Vernal Equinox Day", year, 3, 20);

        // Showa Day (April 29)
        calendar.add("Showa Day", year, 4, 29);

        // Constitution Memorial Day (May 3)
        calendar.add("Constitution Memorial Day", year, 5, 3);

        // Greenery Day (May 4)
        calendar.add("Greenery Day", year, 5, 4);

        // Children's Day (May 5)
        calendar.add("Children's Day", year, 5, 5);

        // Marine Day (3rd Monday in July)
        if let Some((_, day)) = Self::nth_weekday(year, 7, 1, 3) {
            calendar.add("Marine Day", year, 7, day);
        }

        // Mountain Day (August 11)
        calendar.add("Mountain Day", year, 8, 11);

        // Respect for the Aged Day (3rd Monday in September)
        if let Some((_, day)) = Self::nth_weekday(year, 9, 1, 3) {
            calendar.add("Respect for the Aged Day", year, 9, day);
        }

        // Autumnal Equinox Day (around September 22-23, using September 23 as approximation)
        calendar.add("Autumnal Equinox Day", year, 9, 23);

        // Sports Day (2nd Monday in October)
        if let Some((_, day)) = Self::nth_weekday(year, 10, 1, 2) {
            calendar.add("Sports Day", year, 10, day);
        }

        // Culture Day (November 3)
        calendar.add("Culture Day", year, 11, 3);

        // Labor Thanksgiving Day (November 23)
        calendar.add("Labor Thanksgiving Day", year, 11, 23);

        calendar
    }

    /// Create a UK public holidays calendar for a given year (England and Wales)
    ///
    /// Includes the standard UK bank holidays:
    /// - New Year's Day (January 1)
    /// - Good Friday (calculated based on Easter)
    /// - Easter Monday (calculated based on Easter)
    /// - Early May Bank Holiday (1st Monday in May)
    /// - Spring Bank Holiday (Last Monday in May)
    /// - Summer Bank Holiday (Last Monday in August)
    /// - Christmas Day (December 25)
    /// - Boxing Day (December 26)
    ///
    /// Note: Easter-dependent holidays use a simplified approximation.
    /// For exact dates, use a proper Easter calculation algorithm.
    pub fn uk(year: i32) -> Self {
        let mut calendar = Self::new();

        // New Year's Day (January 1)
        calendar.add("New Year's Day", year, 1, 1);

        // Easter calculation (simplified - using a fixed date approximation)
        // In a production system, you would use a proper Easter algorithm
        // For now, we'll use common approximate dates

        // Good Friday (approximate - varies between March 20 and April 23)
        // Using April 15 as a typical date
        calendar.add("Good Friday", year, 4, 15);

        // Easter Monday (Good Friday + 3 days)
        calendar.add("Easter Monday", year, 4, 18);

        // Early May Bank Holiday (1st Monday in May)
        if let Some((_, day)) = Self::nth_weekday(year, 5, 1, 1) {
            calendar.add("Early May Bank Holiday", year, 5, day);
        }

        // Spring Bank Holiday (Last Monday in May)
        if let Some((_, day)) = Self::last_weekday(year, 5, 1) {
            calendar.add("Spring Bank Holiday", year, 5, day);
        }

        // Summer Bank Holiday (Last Monday in August)
        if let Some((_, day)) = Self::last_weekday(year, 8, 1) {
            calendar.add("Summer Bank Holiday", year, 8, day);
        }

        // Christmas Day (December 25)
        calendar.add("Christmas Day", year, 12, 25);

        // Boxing Day (December 26)
        calendar.add("Boxing Day", year, 12, 26);

        calendar
    }

    /// Create a Canada statutory holidays calendar for a given year
    ///
    /// Includes federal statutory holidays observed across Canada:
    /// - New Year's Day (January 1)
    /// - Good Friday (calculated based on Easter)
    /// - Victoria Day (Monday before May 25)
    /// - Canada Day (July 1)
    /// - Labour Day (1st Monday in September)
    /// - Thanksgiving (2nd Monday in October)
    /// - Remembrance Day (November 11) - observed federally
    /// - Christmas Day (December 25)
    /// - Boxing Day (December 26)
    ///
    /// Note: Provincial holidays may vary and are not included.
    pub fn canada(year: i32) -> Self {
        let mut calendar = Self::new();

        // New Year's Day (January 1)
        calendar.add("New Year's Day", year, 1, 1);

        // Good Friday (approximate - using April 15 as typical date)
        calendar.add("Good Friday", year, 4, 15);

        // Victoria Day (Monday before May 25)
        // This is the last Monday on or before May 24
        if let Some((_, day)) = Self::monday_on_or_before(year, 5, 24) {
            calendar.add("Victoria Day", year, 5, day);
        }

        // Canada Day (July 1)
        calendar.add("Canada Day", year, 7, 1);

        // Labour Day (1st Monday in September)
        if let Some((_, day)) = Self::nth_weekday(year, 9, 1, 1) {
            calendar.add("Labour Day", year, 9, day);
        }

        // Thanksgiving (2nd Monday in October)
        if let Some((_, day)) = Self::nth_weekday(year, 10, 1, 2) {
            calendar.add("Thanksgiving", year, 10, day);
        }

        // Remembrance Day (November 11)
        calendar.add("Remembrance Day", year, 11, 11);

        // Christmas Day (December 25)
        calendar.add("Christmas Day", year, 12, 25);

        // Boxing Day (December 26)
        calendar.add("Boxing Day", year, 12, 26);

        calendar
    }

    /// Find the Monday on or before a specific date
    ///
    /// # Arguments
    /// * `year` - Year
    /// * `month` - Month (1-12)
    /// * `day` - Day of month
    ///
    /// # Returns
    /// `Some((month, day))` if found, `None` otherwise
    fn monday_on_or_before(year: i32, month: u32, day: u32) -> Option<(u32, u32)> {
        use chrono::NaiveDate;

        let date = NaiveDate::from_ymd_opt(year, month, day)?;
        let weekday = date.weekday().num_days_from_sunday();

        // If it's already Monday (1), return it
        // Otherwise, go back to the previous Monday
        let days_back = if weekday >= 1 {
            weekday - 1
        } else {
            6 // Sunday, so go back 6 days to Monday
        };

        let target = date.checked_sub_signed(Duration::days(days_back as i64))?;
        Some((target.month(), target.day()))
    }
}

// ============================================================================
// Calendar-Aware Schedule Extensions
// ============================================================================
