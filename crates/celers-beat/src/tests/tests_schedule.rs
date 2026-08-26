#![cfg(test)]

//! `Schedule` type tests: interval, one-time, crontab, and solar schedules.
//!
//! The scheduler-side half of this suite (scheduled tasks, `BeatScheduler`,
//! persistence, jitter, catch-up, groups, retry) lives in
//! `tests_scheduler.rs`; this file crossed the 2000-line limit and was split
//! at that section boundary.

use crate::*;
#[cfg(feature = "cron")]
use chrono::Timelike;
// Only the solar-schedule tests below build an absolute date.
#[cfg(feature = "solar")]
use chrono::TimeZone;
use chrono::{Duration, Utc};

// ===== Interval Schedule Tests =====

#[test]
fn test_interval_schedule_basic() {
    let schedule = Schedule::interval(60);
    assert!(schedule.is_interval());
}

#[test]
fn test_interval_schedule_next_run_no_last_run() {
    let schedule = Schedule::interval(60);
    let before = Utc::now();
    let next_run = schedule.next_run(None).unwrap();
    let after = Utc::now();

    // Next run should be ~60 seconds from now
    assert!(next_run > before + Duration::seconds(59));
    assert!(next_run < after + Duration::seconds(61));
}

#[test]
fn test_interval_schedule_next_run_with_last_run() {
    let schedule = Schedule::interval(60);
    let last_run = Utc::now();
    let next_run = schedule.next_run(Some(last_run)).unwrap();

    // Next run should be exactly 60 seconds after last run
    let expected = last_run + Duration::seconds(60);
    assert_eq!(next_run, expected);
}

#[test]
fn test_interval_schedule_multiple_intervals() {
    for interval in [1, 5, 10, 30, 60, 120, 300, 3600] {
        let schedule = Schedule::interval(interval);
        let last_run = Utc::now();
        let next_run = schedule.next_run(Some(last_run)).unwrap();

        assert_eq!(
            next_run,
            last_run + Duration::seconds(interval as i64),
            "Failed for interval {}",
            interval
        );
    }
}

#[test]
fn test_interval_schedule_display() {
    let schedule = Schedule::interval(60);
    let display = format!("{}", schedule);
    assert_eq!(display, "Interval[every 60s]");
}

// ===== OneTime Schedule Tests =====

#[test]
fn test_onetime_schedule_basic() {
    let run_at = Utc::now() + Duration::hours(1);
    let schedule = Schedule::onetime(run_at);
    assert!(schedule.is_onetime());
}

#[test]
fn test_onetime_schedule_next_run_no_last_run() {
    let run_at = Utc::now() + Duration::hours(1);
    let schedule = Schedule::onetime(run_at);
    let next_run = schedule.next_run(None).unwrap();
    assert_eq!(next_run, run_at);
}

#[test]
fn test_onetime_schedule_next_run_with_last_run() {
    let run_at = Utc::now() + Duration::hours(1);
    let schedule = Schedule::onetime(run_at);
    let last_run = Utc::now();
    let result = schedule.next_run(Some(last_run));

    // Should return error because one-time schedules can't run twice
    assert!(result.is_err());
    if let Err(ScheduleError::Invalid(msg)) = result {
        assert_eq!(msg, "One-time schedule has already been executed");
    }
}

#[test]
fn test_onetime_schedule_display() {
    let run_at = Utc::now() + Duration::hours(1);
    let schedule = Schedule::onetime(run_at);
    let display = format!("{}", schedule);
    assert!(display.starts_with("OneTime[at "));
    assert!(display.ends_with(" UTC]"));
}

#[test]
fn test_onetime_schedule_in_future() {
    let run_at = Utc::now() + Duration::days(7);
    let schedule = Schedule::onetime(run_at);
    let next_run = schedule.next_run(None).unwrap();
    assert_eq!(next_run, run_at);
}

#[test]
fn test_onetime_schedule_in_past() {
    // OneTime schedules can be set in the past (scheduler will run immediately if due)
    let run_at = Utc::now() - Duration::hours(1);
    let schedule = Schedule::onetime(run_at);
    let next_run = schedule.next_run(None).unwrap();
    assert_eq!(next_run, run_at);
}

#[test]
fn test_onetime_task_auto_cleanup() {
    let mut scheduler = BeatScheduler::new();
    let run_at = Utc::now() - Duration::hours(1); // Past time, so it's immediately due
    let task = ScheduledTask::new("test_onetime".to_string(), Schedule::onetime(run_at));

    scheduler.add_task(task).unwrap();
    assert_eq!(scheduler.tasks.len(), 1);

    // Mark as successful - should auto-remove
    scheduler.mark_task_success("test_onetime").unwrap();
    assert_eq!(scheduler.tasks.len(), 0);
}

#[test]
fn test_onetime_task_auto_cleanup_with_start_time() {
    let mut scheduler = BeatScheduler::new();
    let run_at = Utc::now() - Duration::hours(1);
    let task = ScheduledTask::new("test_onetime".to_string(), Schedule::onetime(run_at));

    scheduler.add_task(task).unwrap();
    assert_eq!(scheduler.tasks.len(), 1);

    // Mark as successful with start time - should auto-remove
    let started_at = Utc::now() - Duration::seconds(5);
    scheduler
        .mark_task_success_with_start("test_onetime", started_at)
        .unwrap();
    assert_eq!(scheduler.tasks.len(), 0);
}

#[test]
fn test_onetime_task_not_removed_on_failure() {
    let mut scheduler = BeatScheduler::new();
    let run_at = Utc::now() - Duration::hours(1);
    let task = ScheduledTask::new("test_onetime".to_string(), Schedule::onetime(run_at));

    scheduler.add_task(task).unwrap();
    assert_eq!(scheduler.tasks.len(), 1);

    // Mark as failed - should NOT auto-remove (user might want to retry manually)
    scheduler.mark_task_failure("test_onetime").unwrap();
    assert_eq!(scheduler.tasks.len(), 1);
}

#[test]
fn test_onetime_serialization() {
    let run_at = Utc::now() + Duration::hours(2);
    let schedule = Schedule::onetime(run_at);

    // Serialize
    let json = serde_json::to_string(&schedule).unwrap();

    // Deserialize
    let deserialized: Schedule = serde_json::from_str(&json).unwrap();

    // Verify
    assert!(deserialized.is_onetime());
    let next_run = deserialized.next_run(None).unwrap();
    assert_eq!(next_run, run_at);
}

// ===== Crontab Schedule Tests =====

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_basic() {
    let schedule = Schedule::crontab("0", "0", "*", "*", "*");
    assert!(schedule.is_crontab());
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_every_minute() {
    let schedule = Schedule::crontab("*", "*", "*", "*", "*");
    let now = Utc::now();
    let next_run = schedule.next_run(Some(now)).unwrap();

    // Should be within next 2 minutes
    assert!(next_run > now);
    assert!(next_run < now + Duration::minutes(2));
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_specific_time() {
    // Every day at 10:30
    let schedule = Schedule::crontab("30", "10", "*", "*", "*");
    let now = Utc::now();
    let next_run = schedule.next_run(Some(now)).unwrap();

    assert!(next_run > now);
    assert_eq!(next_run.hour(), 10);
    assert_eq!(next_run.minute(), 30);
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_invalid() {
    let schedule = Schedule::crontab("invalid", "0", "*", "*", "*");
    let result = schedule.next_run(None);
    assert!(result.is_err());
    assert!(result.unwrap_err().is_parse());
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_display() {
    let schedule = Schedule::crontab("0", "12", "*", "*", "1");
    let display = format!("{}", schedule);
    assert_eq!(display, "Crontab[0 12 * * 1 (UTC)]");
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_with_timezone() {
    let schedule = Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York");
    assert!(schedule.is_crontab());

    // Display should show timezone
    let display = format!("{}", schedule);
    assert!(display.contains("America/New_York"));
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_timezone_next_run() {
    // Schedule for 9:00 AM New York time on weekdays
    let schedule = Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York");

    // Get next run time
    let next_run = schedule.next_run(None).unwrap();

    // Verify the time is valid (should be in the future)
    assert!(next_run > Utc::now());
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_invalid_timezone() {
    let schedule = Schedule::crontab_tz("0", "9", "*", "*", "*", "Invalid/Timezone");
    let result = schedule.next_run(None);
    assert!(result.is_err());
    assert!(result.unwrap_err().is_parse());
}

#[cfg(feature = "cron")]
#[test]
fn test_crontab_schedule_timezone_serialization() {
    let schedule = Schedule::crontab_tz("30", "14", "*", "*", "*", "Europe/London");
    let json = serde_json::to_string(&schedule).unwrap();
    let deserialized: Schedule = serde_json::from_str(&json).unwrap();

    // Verify timezone is preserved
    let display = format!("{}", deserialized);
    assert!(display.contains("Europe/London"));
}

// ===== Solar Schedule Tests =====

#[cfg(feature = "solar")]
#[test]
fn test_solar_schedule_basic() {
    let schedule = Schedule::solar("sunrise", 35.6762, 139.6503); // Tokyo
    assert!(schedule.is_solar());
}

// `Schedule::Solar` resolves real solar instants via `sunrise` 3.0.0's
// `SolarDay::event_time`, which returns an absolute `DateTime<Utc>`.
//
// HISTORY -- the bug these tests now guard against: the branch used to call the
// deprecated `sunrise::sunrise_sunset(lat, lon, y, m, d)`, whose `(i64, i64)`
// return is a pair of Unix **seconds** timestamps, and treated each value as
// "minutes since midnight" (`let hours = (sunrise_time / 60) as u32`). For any
// real date that divides a ~1.7-billion-second timestamp into an hour count in
// the tens of millions, `NaiveDate::and_hms_opt` rejected it (hour must be
// < 24), and `next_run` returned `Err(ScheduleError::Invalid(..))` on the very
// first loop iteration. Solar schedules never fired at all.
//
// The instants asserted below are independently checkable against published
// almanac data, which is the point of pinning them rather than only asserting
// `is_ok()`: Tokyo's 2026 summer-solstice sunrise is 04:25 JST (= 19:25Z the
// previous UTC day) and its sunset 19:00 JST (= 10:00Z).

/// The UTC instant `sunrise` computes for a given event, date and place, with a
/// tolerance that absorbs algorithmic refinements without letting a unit or
/// sign error through: a minute of slack cannot hide an error measured in hours
/// or days.
#[cfg(feature = "solar")]
fn assert_near(actual: chrono::DateTime<Utc>, expected: &str, context: &str) {
    let expected: chrono::DateTime<Utc> = expected
        .parse()
        .expect("the expected instant literal must be a valid RFC 3339 timestamp");
    let skew = (actual - expected).num_seconds().abs();
    assert!(
        skew <= 60,
        "{context}: expected {expected}, got {actual} ({skew}s apart)"
    );
}

/// Tokyo, eastern hemisphere. The sunrise belonging to local date 2026-06-22
/// falls on 2026-06-21 in UTC, which is exactly the case that a scan anchored
/// at `start_time.date_naive()` would skip.
#[cfg(feature = "solar")]
#[test]
fn test_solar_schedule_sunrise() {
    let schedule = Schedule::solar("sunrise", 35.6762, 139.6503); // Tokyo
    let pinned = Utc
        .with_ymd_and_hms(2026, 6, 21, 0, 0, 0)
        .single()
        .expect("2026-06-21T00:00:00Z is an unambiguous UTC instant");

    let next_run = schedule
        .next_run(Some(pinned))
        .expect("Tokyo has a sunrise on the summer solstice");

    assert!(next_run > pinned, "next_run must be in the future");
    // 19:25:59Z == 04:25:59 JST on 2026-06-22 -- the published Tokyo solstice
    // sunrise. Under the old minutes/seconds confusion this was an `Err`.
    assert_near(next_run, "2026-06-21T19:25:59Z", "Tokyo solstice sunrise");
}

#[cfg(feature = "solar")]
#[test]
fn test_solar_schedule_sunset() {
    let schedule = Schedule::solar("sunset", 35.6762, 139.6503); // Tokyo
    let pinned = Utc
        .with_ymd_and_hms(2026, 6, 21, 0, 0, 0)
        .single()
        .expect("2026-06-21T00:00:00Z is an unambiguous UTC instant");

    let next_run = schedule
        .next_run(Some(pinned))
        .expect("Tokyo has a sunset on the summer solstice");

    assert!(next_run > pinned, "next_run must be in the future");
    // 10:00:18Z == 19:00:18 JST on 2026-06-21.
    assert_near(next_run, "2026-06-21T10:00:18Z", "Tokyo solstice sunset");
}

/// Same day, western hemisphere. Tokyo alone cannot catch a sign error in the
/// longitude handling or in the day the scan starts from: San Francisco's
/// sunset for local 2026-06-20 lands at 03:34Z on 2026-06-21, i.e. the scan
/// must look at the day *before* the start date to find it.
#[cfg(feature = "solar")]
#[test]
fn solar_next_run_handles_negative_longitude() {
    let pinned = Utc
        .with_ymd_and_hms(2026, 6, 21, 0, 0, 0)
        .single()
        .expect("2026-06-21T00:00:00Z is an unambiguous UTC instant");

    let sunset = Schedule::solar("sunset", 37.7749, -122.4194)
        .next_run(Some(pinned))
        .expect("San Francisco has a sunset on the summer solstice");
    // 03:34:41Z == 20:34:41 PDT on 2026-06-20.
    assert_near(
        sunset,
        "2026-06-21T03:34:41Z",
        "San Francisco solstice sunset",
    );

    let sunrise = Schedule::solar("sunrise", 37.7749, -122.4194)
        .next_run(Some(pinned))
        .expect("San Francisco has a sunrise on the summer solstice");
    // 12:48:02Z == 05:48:02 PDT on 2026-06-21.
    assert_near(
        sunrise,
        "2026-06-21T12:48:02Z",
        "San Francisco solstice sunrise",
    );
}

/// Civil/nautical/astronomical twilight resolve through the `sunrise` crate's
/// own `Dawn`/`Dusk` events (6°/12°/18° below the horizon) rather than the
/// fixed ±30/60/90-minute offsets from sunrise they used to approximate. The
/// ordering asserted here is a property of the definitions -- the darker the
/// threshold, the earlier the morning event -- so it holds independently of the
/// exact almanac values.
#[cfg(feature = "solar")]
#[test]
fn solar_twilight_events_are_ordered_by_depth() {
    let pinned = Utc
        .with_ymd_and_hms(2026, 6, 21, 0, 0, 0)
        .single()
        .expect("2026-06-21T00:00:00Z is an unambiguous UTC instant");
    let next = |event: &str| {
        Schedule::solar(event, 35.6762, 139.6503)
            .next_run(Some(pinned))
            .unwrap_or_else(|e| panic!("Tokyo {event} on the solstice must resolve: {e:?}"))
    };

    let astronomical = next("astronomical_twilight_begin");
    let nautical = next("nautical_twilight_begin");
    let civil = next("dawn");
    let sunrise = next("sunrise");

    assert!(
        astronomical < nautical && nautical < civil && civil < sunrise,
        "morning twilight must deepen in order: astronomical {astronomical} < nautical \
         {nautical} < civil {civil} < sunrise {sunrise}"
    );
    // A real 18°-below-horizon solve, not sunrise minus a flat 90 minutes: at
    // Tokyo's latitude on the solstice the true gap is nearly two hours.
    let gap = (sunrise - astronomical).num_minutes();
    assert!(
        gap > 100,
        "astronomical dawn should precede sunrise by well over the old flat 90-minute \
         approximation at this latitude, got {gap} minutes"
    );
}

/// Golden hour: the sun between 0° (the horizon) and 6° of elevation, now
/// resolved through `SolarEvent::Elevation` rather than the fixed
/// 0/-30-minute offsets from sunrise/sunset it used to approximate (see the
/// comment on the `"golden_hour_begin"`/`"golden_hour_end"` match arms in
/// `Schedule::next_run`'s solar branch for the sign-convention writeup).
///
/// Reference values were computed independently of this crate: a standalone
/// program against the `sunrise = "3.0.0"` crate directly, re-implementing
/// only the "scan from the day before, keep the first result after `start`"
/// search `next_run` performs, for `SolarEvent::Elevation { elevation: 0.0,
/// morning: true }` and `SolarEvent::Elevation { elevation:
/// -6f64.to_radians(), morning: false }`. That probe reproduced this file's
/// existing pinned sunrise/sunset values exactly before its golden-hour
/// output was trusted.
#[cfg(feature = "solar")]
#[test]
fn test_solar_schedule_golden_hour() {
    let pinned = Utc
        .with_ymd_and_hms(2026, 6, 21, 0, 0, 0)
        .single()
        .expect("2026-06-21T00:00:00Z is an unambiguous UTC instant");

    let begin = Schedule::solar("golden_hour_begin", 35.6762, 139.6503)
        .next_run(Some(pinned))
        .expect("Tokyo has a golden hour on the summer solstice");
    // A few minutes after Tokyo's 19:25:59Z sunrise (`test_solar_schedule_sunrise`):
    // the sun's centre keeps climbing from Sunrise's 5/6°-below-horizon depression
    // up to the true 0° crossing.
    assert_near(begin, "2026-06-21T19:30:42Z", "Tokyo golden_hour_begin");

    let end = Schedule::solar("golden_hour_end", 35.6762, 139.6503)
        .next_run(Some(pinned))
        .expect("Tokyo has a golden hour on the summer solstice");
    // Well before Tokyo's 10:00:18Z sunset (`test_solar_schedule_sunset`): golden
    // light fades as the sun descends through 6° elevation, ahead of sunset itself.
    assert_near(end, "2026-06-21T09:22:25Z", "Tokyo golden_hour_end");

    // San Francisco, western hemisphere: the same negative-longitude coverage
    // `solar_next_run_handles_negative_longitude` established for sunrise/sunset,
    // extended to golden hour.
    let begin_sf = Schedule::solar("golden_hour_begin", 37.7749, -122.4194)
        .next_run(Some(pinned))
        .expect("San Francisco has a golden hour on the summer solstice");
    assert_near(
        begin_sf,
        "2026-06-21T12:52:56Z",
        "San Francisco golden_hour_begin",
    );

    let end_sf = Schedule::solar("golden_hour_end", 37.7749, -122.4194)
        .next_run(Some(pinned))
        .expect("San Francisco has a golden hour on the summer solstice");
    assert_near(
        end_sf,
        "2026-06-21T02:55:30Z",
        "San Francisco golden_hour_end",
    );
}

/// `golden_hour_begin` sits strictly after sunrise and `golden_hour_end`
/// strictly before sunset -- properties of the 0°/6° definitions themselves
/// (see `test_solar_schedule_golden_hour`'s doc comment for the sign
/// convention), so, like `solar_twilight_events_are_ordered_by_depth` above,
/// this holds independently of the exact almanac values.
#[cfg(feature = "solar")]
#[test]
fn solar_golden_hour_brackets_sunrise_and_sunset() {
    let pinned = Utc
        .with_ymd_and_hms(2026, 6, 21, 0, 0, 0)
        .single()
        .expect("2026-06-21T00:00:00Z is an unambiguous UTC instant");
    let next = |event: &str| {
        Schedule::solar(event, 35.6762, 139.6503)
            .next_run(Some(pinned))
            .unwrap_or_else(|e| panic!("Tokyo {event} on the solstice must resolve: {e:?}"))
    };

    let sunrise = next("sunrise");
    let golden_begin = next("golden_hour_begin");
    assert!(
        golden_begin > sunrise,
        "golden_hour_begin {golden_begin} must follow sunrise {sunrise}: the true 0° crossing \
         comes after the refraction-adjusted Sunrise event"
    );

    let golden_end = next("golden_hour_end");
    let sunset = next("sunset");
    assert!(
        golden_end < sunset,
        "golden_hour_end {golden_end} must precede sunset {sunset}: the sun is still 6° up \
         when golden hour ends"
    );

    // The evening gap (sun descending the 6.83° from Sunset's depression up to
    // the 6° threshold) should be several times the morning gap (sun climbing
    // only the 0.83° between Sunrise's depression and the true horizon): both
    // traverse comparable apparent angular speed near the horizon, so their
    // ratio tracks the 0.83°/6.83° distance ratio (~8.2x), not the date or
    // place chosen.
    let morning_gap = (golden_begin - sunrise).num_seconds();
    let evening_gap = (sunset - golden_end).num_seconds();
    assert!(
        morning_gap > 0 && evening_gap > 0,
        "both gaps must be strictly positive: morning {morning_gap}s, evening {evening_gap}s"
    );
    let ratio = evening_gap as f64 / morning_gap as f64;
    assert!(
        (5.0..=12.0).contains(&ratio),
        "evening gap should be roughly 8x the morning gap (6.83/0.83 degrees), got ratio \
         {ratio:.2} ({evening_gap}s / {morning_gap}s)"
    );
}

/// Polar day is not an error. Above the Arctic Circle the sun does not rise or
/// set for weeks, so `SolarDay::event_time` returns `None` for every date in
/// that stretch; the scan must walk past them rather than fail on day one.
#[cfg(feature = "solar")]
#[test]
fn solar_next_run_skips_polar_day_instead_of_failing() {
    let schedule = Schedule::solar("sunrise", 78.0, 15.0); // Svalbard
    let midsummer = Utc
        .with_ymd_and_hms(2026, 6, 21, 0, 0, 0)
        .single()
        .expect("2026-06-21T00:00:00Z is an unambiguous UTC instant");

    let next_run = schedule
        .next_run(Some(midsummer))
        .expect("the first sunrise after Svalbard's midnight sun must be found, not errored");

    assert!(next_run > midsummer);
    // Svalbard's midnight sun runs into late August: the first sunrise is
    // roughly two months out, far beyond the next-day answer a naive scan
    // would give.
    assert!(
        next_run > midsummer + Duration::days(50),
        "expected the first post-polar-day sunrise (late August), got {next_run}"
    );
    assert!(
        next_run < midsummer + Duration::days(75),
        "expected the first post-polar-day sunrise (late August), got {next_run}"
    );
}

/// Out-of-range coordinates are a configuration error, not a panic. The
/// deprecated `sunrise_sunset` helper the branch used to call resolved them
/// with `.expect("invalid coordinates")` inside the crate, which would abort
/// the beat process.
#[cfg(feature = "solar")]
#[test]
fn solar_next_run_rejects_out_of_range_coordinates() {
    let err = Schedule::solar("sunrise", 91.0, 0.0)
        .next_run(None)
        .expect_err("latitude 91 is not a valid coordinate");
    assert!(
        err.is_invalid(),
        "expected ScheduleError::Invalid, got {err:?}"
    );

    let err = Schedule::solar("sunrise", 0.0, 181.0)
        .next_run(None)
        .expect_err("longitude 181 is not a valid coordinate");
    assert!(
        err.is_invalid(),
        "expected ScheduleError::Invalid, got {err:?}"
    );
}

#[cfg(feature = "solar")]
#[test]
fn test_solar_schedule_invalid_event() {
    // Tokyo's coordinates are valid, so the only thing that can fail here is
    // the event-name lookup. Asserting the message keeps this test tied to that
    // arm: coordinate validation also yields `ScheduleError::Invalid`, so
    // `is_invalid()` alone would still pass if the unknown-event arm were
    // deleted.
    let schedule = Schedule::solar("invalid", 35.6762, 139.6503);
    let err = schedule
        .next_run(None)
        .expect_err("'invalid' is not a solar event name");
    assert!(
        err.is_invalid(),
        "expected ScheduleError::Invalid, got {err:?}"
    );
    let message = err.to_string();
    assert!(
        message.contains("Unknown solar event"),
        "the error must name the unknown event as the cause, got: {message}"
    );
}

#[cfg(feature = "solar")]
#[test]
fn test_solar_schedule_display() {
    let schedule = Schedule::solar("sunrise", 35.6762, 139.6503);
    let display = format!("{}", schedule);
    assert!(display.contains("Solar[sunrise"));
    assert!(display.contains("35.6762"));
    assert!(display.contains("139.6503"));
}
