//! Schedule Builders and Templates Example
//!
//! Demonstrates the fluent Schedule Builder API and pre-built schedule templates
//! for common scheduling patterns.
//!
//! Run with:
//! ```bash
//! cargo run --example schedule_builders --features cron
//! ```

use celers_beat::{ScheduleBuilder, ScheduleTemplates, ScheduledTask};

fn main() {
    println!("=== Celers Beat Schedule Builders & Templates ===\n");

    // ========================================================================
    // Part 1: Schedule Builder - Fluent API
    // ========================================================================
    println!("1. Schedule Builder - Fluent API");
    println!("   -------------------------------\n");

    // Simple interval schedules
    println!("   Simple Intervals:");
    let every_30_sec = ScheduleBuilder::new().every_n_seconds(30).build();
    println!("     Every 30 seconds: {:?}", every_30_sec);

    let every_15_min = ScheduleBuilder::new().every_n_minutes(15).build();
    println!("     Every 15 minutes: {:?}", every_15_min);

    let every_2_hours = ScheduleBuilder::new().every_n_hours(2).build();
    println!("     Every 2 hours: {:?}", every_2_hours);

    let daily = ScheduleBuilder::new().every_n_days(1).build();
    println!("     Daily: {:?}\n", daily);

    // Business hours only (requires cron feature)
    println!("   Business Hours (Mon-Fri, 9 AM - 5 PM):");
    let business_hours_30min = ScheduleBuilder::new()
        .every_n_minutes(30)
        .business_hours_only()
        .build();
    println!(
        "     Every 30 min during business hours: {:?}",
        business_hours_30min
    );

    let business_hours_hourly = ScheduleBuilder::new()
        .every_n_hours(1)
        .business_hours_only()
        .build();
    println!(
        "     Hourly during business hours: {:?}\n",
        business_hours_hourly
    );

    // Weekdays only
    println!("   Weekdays Only (Mon-Fri):");
    let weekday_schedule = ScheduleBuilder::new()
        .every_n_hours(2)
        .weekdays_only()
        .build();
    println!("     Every 2 hours on weekdays: {:?}\n", weekday_schedule);

    // Weekends only
    println!("   Weekends Only (Sat-Sun):");
    let weekend_schedule = ScheduleBuilder::new()
        .every_n_hours(1)
        .weekends_only()
        .build();
    println!("     Hourly on weekends: {:?}\n", weekend_schedule);

    // With timezone
    println!("   With Timezone:");
    let ny_schedule = ScheduleBuilder::new()
        .every_n_hours(1)
        .in_timezone("America/New_York")
        .build();
    println!("     Hourly in New York time: {:?}", ny_schedule);

    let tokyo_business = ScheduleBuilder::new()
        .every_n_minutes(30)
        .business_hours_only()
        .in_timezone("Asia/Tokyo")
        .build();
    println!("     Business hours in Tokyo: {:?}\n", tokyo_business);

    // ========================================================================
    // Part 2: Schedule Templates - Pre-built Patterns
    // ========================================================================
    println!("2. Schedule Templates - Common Patterns");
    println!("   ------------------------------------\n");

    // Common intervals
    println!("   Common Intervals:");
    println!("     Every minute: {:?}", ScheduleTemplates::every_minute());
    println!(
        "     Every 5 minutes: {:?}",
        ScheduleTemplates::every_5_minutes()
    );
    println!(
        "     Every 15 minutes: {:?}",
        ScheduleTemplates::every_15_minutes()
    );
    println!(
        "     Every 30 minutes: {:?}",
        ScheduleTemplates::every_30_minutes()
    );
    println!("     Hourly: {:?}", ScheduleTemplates::hourly());
    println!(
        "     Every 2 hours: {:?}",
        ScheduleTemplates::every_2_hours()
    );
    println!(
        "     Every 6 hours: {:?}",
        ScheduleTemplates::every_6_hours()
    );
    println!(
        "     Every 12 hours: {:?}\n",
        ScheduleTemplates::every_12_hours()
    );

    // Daily schedules
    println!("   Daily Schedules:");
    println!(
        "     Daily at midnight: {:?}",
        ScheduleTemplates::daily_at_midnight()
    );
    println!(
        "     Daily at 3 AM: {:?}",
        ScheduleTemplates::daily_at_hour(3)
    );
    println!(
        "     Daily at 9 AM: {:?}\n",
        ScheduleTemplates::daily_at_hour(9)
    );

    // Weekly schedules
    println!("   Weekly Schedules:");
    println!(
        "     Weekdays at 9:00 AM: {:?}",
        ScheduleTemplates::weekdays_at(9, 0)
    );
    println!(
        "     Monday at 9:00 AM: {:?}",
        ScheduleTemplates::weekly_on_monday(9, 0)
    );
    println!(
        "     Weekend mornings (8 AM): {:?}\n",
        ScheduleTemplates::weekend_mornings()
    );

    // Monthly schedules
    println!("   Monthly Schedules:");
    println!(
        "     First day of month: {:?}",
        ScheduleTemplates::monthly_first_day()
    );
    println!(
        "     Last day of month: {:?}\n",
        ScheduleTemplates::monthly_last_day()
    );

    // Business hours
    println!("   Business Hours:");
    println!(
        "     Hourly (9 AM - 5 PM, Mon-Fri): {:?}",
        ScheduleTemplates::business_hours_hourly()
    );
    println!(
        "     Every 15 min (business hours): {:?}\n",
        ScheduleTemplates::business_hours_every_15_minutes()
    );

    // Quarterly
    println!("   Quarterly:");
    println!(
        "     Quarterly (Jan/Apr/Jul/Oct 1st): {:?}\n",
        ScheduleTemplates::quarterly()
    );

    // ========================================================================
    // Part 3: Practical Use Cases
    // ========================================================================
    println!("3. Practical Use Cases");
    println!("   -------------------\n");

    // Use case 1: Health check monitoring
    println!("   Use Case 1: Health Check Monitoring");
    let health_check = ScheduledTask::new(
        "health_check".to_string(),
        ScheduleTemplates::every_minute(),
    );
    println!("     Task: Check system health every minute");
    println!("     Schedule: {:?}\n", health_check.schedule);

    // Use case 2: Report generation
    println!("   Use Case 2: Daily Report Generation");
    let daily_report = ScheduledTask::new(
        "generate_daily_report".to_string(),
        ScheduleTemplates::daily_at_hour(3),
    );
    println!("     Task: Generate reports at 3 AM daily");
    println!("     Schedule: {:?}\n", daily_report.schedule);

    // Use case 3: Business hours API sync
    println!("   Use Case 3: Business Hours API Sync");
    let api_sync = ScheduledTask::new(
        "sync_with_external_api".to_string(),
        ScheduleTemplates::business_hours_every_15_minutes(),
    );
    println!("     Task: Sync with external API every 15 min during business hours");
    println!("     Schedule: {:?}\n", api_sync.schedule);

    // Use case 4: Weekend batch processing
    println!("   Use Case 4: Weekend Batch Processing");
    let weekend_batch = ScheduledTask::new(
        "process_weekend_batch".to_string(),
        ScheduleBuilder::new()
            .every_n_hours(2)
            .weekends_only()
            .build(),
    );
    println!("     Task: Process batches every 2 hours on weekends");
    println!("     Schedule: {:?}\n", weekend_batch.schedule);

    // Use case 5: Monthly billing
    println!("   Use Case 5: Monthly Billing");
    let monthly_billing = ScheduledTask::new(
        "process_monthly_billing".to_string(),
        ScheduleTemplates::monthly_first_day(),
    );
    println!("     Task: Process billing on 1st of each month");
    println!("     Schedule: {:?}\n", monthly_billing.schedule);

    // Use case 6: Quarterly reports
    println!("   Use Case 6: Quarterly Reports");
    let quarterly_reports = ScheduledTask::new(
        "generate_quarterly_reports".to_string(),
        ScheduleTemplates::quarterly(),
    );
    println!("     Task: Generate quarterly reports");
    println!("     Schedule: {:?}\n", quarterly_reports.schedule);

    // Use case 7: Timezone-aware notifications
    println!("   Use Case 7: Timezone-Aware Notifications");
    let ny_notifications = ScheduledTask::new(
        "send_ny_notifications".to_string(),
        ScheduleBuilder::new()
            .every_n_hours(1)
            .business_hours_only()
            .in_timezone("America/New_York")
            .build(),
    );
    println!("     Task: Send notifications during NY business hours");
    println!("     Schedule: {:?}\n", ny_notifications.schedule);

    // Use case 8: Log rotation
    println!("   Use Case 8: Log Rotation");
    let log_rotation = ScheduledTask::new(
        "rotate_logs".to_string(),
        ScheduleTemplates::daily_at_midnight(),
    );
    println!("     Task: Rotate logs at midnight");
    println!("     Schedule: {:?}\n", log_rotation.schedule);

    // ========================================================================
    // Part 4: Comparison
    // ========================================================================
    println!("4. Builder vs Template Comparison");
    println!("   -------------------------------\n");

    println!("   Same schedule, different approaches:\n");

    // Approach 1: Using builder
    let builder_schedule = ScheduleBuilder::new()
        .every_n_minutes(15)
        .business_hours_only()
        .build();
    println!("     Builder API:");
    println!("       {:?}\n", builder_schedule);

    // Approach 2: Using template
    let template_schedule = ScheduleTemplates::business_hours_every_15_minutes();
    println!("     Template:");
    println!("       {:?}\n", template_schedule);

    println!("   Both produce equivalent schedules!");

    // ========================================================================
    // Summary
    // ========================================================================
    println!("\n=== Summary ===");
    println!("\nSchedule Builder Benefits:");
    println!("  - Fluent, chainable API");
    println!("  - Flexible and customizable");
    println!("  - Good for unique scheduling needs");
    println!("  - Type-safe and validated");

    println!("\nSchedule Template Benefits:");
    println!("  - Pre-built common patterns");
    println!("  - One-line schedule creation");
    println!("  - Best practices built-in");
    println!("  - Good for standard use cases");

    println!("\nWhen to use each:");
    println!("  Builder  -> Custom requirements, timezone-specific, unique patterns");
    println!("  Template -> Standard patterns, quick setup, common intervals");
}
