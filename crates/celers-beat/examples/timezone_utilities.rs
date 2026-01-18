//! Timezone Utilities Example
//!
//! Demonstrates comprehensive timezone conversion, detection, and DST utilities
//! for managing schedules across different timezones.
//!
//! Run with:
//! ```bash
//! cargo run --example timezone_utilities --features cron
//! ```

use celers_beat::TimezoneUtils;
use chrono::Utc;

fn main() {
    println!("=== Celers Beat Timezone Utilities ===\n");

    // 1. Automatic Timezone Detection
    println!("1. System Timezone Detection");
    println!("   ----------------------------");
    let system_tz = TimezoneUtils::detect_system_timezone();
    println!("   Detected system timezone: {}", system_tz);
    println!(
        "   Valid: {}\n",
        TimezoneUtils::is_valid_timezone(&system_tz)
    );

    // 2. Timezone Validation
    println!("2. Timezone Validation");
    println!("   -------------------");
    let valid_timezones = vec!["America/New_York", "Europe/London", "Asia/Tokyo", "UTC"];
    let invalid_timezones = vec!["Invalid/Timezone", "EST", "America/NotReal"];

    println!("   Valid timezones:");
    for tz in &valid_timezones {
        println!("     {} -> {}", tz, TimezoneUtils::is_valid_timezone(tz));
    }

    println!("   Invalid timezones:");
    for tz in &invalid_timezones {
        println!("     {} -> {}", tz, TimezoneUtils::is_valid_timezone(tz));
    }
    println!();

    // 3. Timezone Listing and Search
    println!("3. Timezone Listing and Search");
    println!("   ---------------------------");
    let all_timezones = TimezoneUtils::list_all_timezones();
    println!("   Total available timezones: {}", all_timezones.len());

    let us_timezones = TimezoneUtils::search_timezones("america");
    println!("   US/America timezones found: {}", us_timezones.len());
    println!("   Sample US timezones:");
    for tz in us_timezones.iter().take(5) {
        println!("     - {}", tz);
    }

    let japan_timezones = TimezoneUtils::search_timezones("tokyo");
    println!("   Tokyo timezones: {:?}", japan_timezones);
    println!();

    // 4. Current Time in Multiple Timezones
    println!("4. Current Time in Multiple Timezones");
    println!("   -----------------------------------");
    let zones = vec![
        "America/New_York",
        "America/Los_Angeles",
        "Europe/London",
        "Europe/Paris",
        "Asia/Tokyo",
        "Asia/Singapore",
        "Australia/Sydney",
    ];

    let times = TimezoneUtils::current_time_in_zones(&zones);
    for (tz, time) in times {
        println!("   {}: {}", tz, time);
    }
    println!();

    // 5. Detailed Timezone Information
    println!("5. Detailed Timezone Information");
    println!("   -----------------------------");
    let interesting_zones = vec![
        "America/New_York",
        "Asia/Tokyo",
        "Europe/London",
        "Australia/Sydney",
    ];

    for tz in interesting_zones {
        if let Ok(info) = TimezoneUtils::get_timezone_info(tz, None) {
            println!("   {}", info);
        }
    }
    println!();

    // 6. DST Status Checking
    println!("6. Daylight Saving Time (DST) Status");
    println!("   ----------------------------------");
    let dst_zones = vec![
        "America/New_York",
        "America/Phoenix", // No DST
        "Europe/London",
        "Asia/Tokyo", // No DST
    ];

    for tz in dst_zones {
        match TimezoneUtils::is_dst_active(tz, None) {
            Ok(is_dst) => {
                println!(
                    "   {} - DST: {}",
                    tz,
                    if is_dst { "Active" } else { "Not Active" }
                );
            }
            Err(e) => println!("   {} - Error: {}", tz, e),
        }
    }
    println!();

    // 7. UTC Offset Information
    println!("7. UTC Offset Calculation");
    println!("   ----------------------");
    let offset_zones = vec![
        "America/New_York",    // UTC-5 or UTC-4
        "America/Los_Angeles", // UTC-8 or UTC-7
        "Europe/London",       // UTC+0 or UTC+1
        "Asia/Tokyo",          // UTC+9
        "Australia/Sydney",    // UTC+10 or UTC+11
    ];

    for tz in offset_zones {
        match TimezoneUtils::get_utc_offset(tz, None) {
            Ok(offset_seconds) => {
                let offset_hours = offset_seconds as f32 / 3600.0;
                println!(
                    "   {} - UTC{:+.1}h ({} seconds)",
                    tz, offset_hours, offset_seconds
                );
            }
            Err(e) => println!("   {} - Error: {}", tz, e),
        }
    }
    println!();

    // 8. Time Conversion Between Timezones
    println!("8. Time Conversion Between Timezones");
    println!("   ---------------------------------");
    let utc_now = Utc::now();
    println!("   UTC Time: {}", utc_now);

    let conversions = vec![
        ("America/New_York", "Asia/Tokyo"),
        ("Europe/London", "America/Los_Angeles"),
        ("Asia/Singapore", "Australia/Sydney"),
    ];

    for (from_tz, to_tz) in conversions {
        match TimezoneUtils::convert_between_timezones(utc_now, from_tz, to_tz) {
            Ok(converted) => {
                println!("   {} -> {}: {}", from_tz, to_tz, converted);
            }
            Err(e) => println!("   Conversion error: {}", e),
        }
    }
    println!();

    // 9. Common Timezone Abbreviations
    println!("9. Common Timezone Abbreviations");
    println!("   -----------------------------");
    let abbrevs = TimezoneUtils::get_common_timezone_abbreviations();
    println!("   Total abbreviations: {}", abbrevs.len());
    println!("   Sample mappings:");

    let common_abbrevs = vec!["EST", "PST", "JST", "GMT", "CET", "AEST"];
    for abbrev in common_abbrevs {
        if let Some(tz) = abbrevs.get(abbrev) {
            println!("     {} -> {}", abbrev, tz);
        }
    }
    println!();

    // 10. Practical Scheduling Example
    println!("10. Practical Scheduling Example");
    println!("    ----------------------------");
    println!("    Scenario: Schedule a task for 9 AM in different timezones\n");

    let target_hour = 9;
    let target_minute = 0;
    let target_zones = vec!["America/New_York", "Europe/London", "Asia/Tokyo"];

    for tz in target_zones {
        match TimezoneUtils::time_until_next_occurrence(target_hour, target_minute, tz) {
            Ok(duration) => {
                let hours = duration.num_hours();
                let minutes = duration.num_minutes() % 60;
                println!(
                    "    {} @ {}:{:02} -> in {}h {}m",
                    tz, target_hour, target_minute, hours, minutes
                );
            }
            Err(e) => println!("    {} - Error: {}", tz, e),
        }
    }
    println!();

    // 11. Timezone Comparison
    println!("11. Timezone Comparison");
    println!("    -------------------");
    println!("    Comparing timezones at the same moment:\n");

    let comparison_zones = vec!["America/New_York", "Europe/London", "Asia/Tokyo"];
    let comparison_times = TimezoneUtils::current_time_in_zones(&comparison_zones);

    println!("    When it's:");
    for (tz, time) in comparison_times {
        println!("      {} in {}", time, tz);
    }
    println!();

    // 12. Working Hours Across Timezones
    println!("12. Working Hours Across Timezones");
    println!("    -------------------------------");
    println!("    Finding overlap for 9 AM - 5 PM working hours:\n");

    // This is a practical example showing how to think about
    // scheduling meetings across timezones
    println!("    New York: 9 AM - 5 PM EST/EDT");
    if let Ok(ny_offset) = TimezoneUtils::get_utc_offset("America/New_York", None) {
        let london_9am_utc = 9 * 3600 - ny_offset;
        let london_5pm_utc = 17 * 3600 - ny_offset;

        if let Ok(london_offset) = TimezoneUtils::get_utc_offset("Europe/London", None) {
            let london_9am = london_9am_utc + london_offset;
            let london_5pm = london_5pm_utc + london_offset;

            println!(
                "    London: {} - {} GMT/BST (equivalent)",
                london_9am / 3600,
                london_5pm / 3600
            );
        }
    }
    println!();

    println!("=== Example Complete ===");
    println!("\nThese utilities are useful for:");
    println!("  - Scheduling tasks across multiple timezones");
    println!("  - Converting cron schedules between timezones");
    println!("  - Handling DST transitions automatically");
    println!("  - Building timezone-aware applications");
    println!("  - Debugging timezone-related issues");
}
