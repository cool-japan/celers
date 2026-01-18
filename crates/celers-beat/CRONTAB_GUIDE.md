# Crontab Syntax Guide

Complete guide to crontab scheduling in celers-beat.

## Table of Contents

- [Basic Syntax](#basic-syntax)
- [Field Descriptions](#field-descriptions)
- [Special Characters](#special-characters)
- [Common Patterns](#common-patterns)
- [Timezone Support](#timezone-support)
- [Best Practices](#best-practices)
- [Examples](#examples)

## Basic Syntax

Crontab schedules use five fields to specify when tasks should run:

```
┌───────────── minute (0-59)
│ ┌───────────── hour (0-23)
│ │ ┌───────────── day of week (0-6, 0=Sunday)
│ │ │ ┌───────────── day of month (1-31)
│ │ │ │ ┌───────────── month (1-12)
│ │ │ │ │
* * * * *
```

### Creating a Crontab Schedule (UTC)

```rust
use celers_beat::Schedule;

let schedule = Schedule::crontab(
    "0",    // minute
    "9",    // hour
    "*",    // day of week
    "*",    // day of month
    "*"     // month
);
// Runs every day at 9:00 AM UTC
```

### Creating a Crontab Schedule with Timezone

```rust
use celers_beat::Schedule;

let schedule = Schedule::crontab_tz(
    "0",                 // minute
    "9",                 // hour
    "1-5",               // day of week (Mon-Fri)
    "*",                 // day of month
    "*",                 // month
    "America/New_York"   // timezone
);
// Runs weekdays at 9:00 AM Eastern Time (handles DST automatically)
```

## Field Descriptions

### Minute (0-59)

- `*` - Every minute
- `0` - At minute 0 (top of the hour)
- `15` - At minute 15
- `*/5` - Every 5 minutes
- `0,30` - At minutes 0 and 30
- `15-45` - Minutes 15 through 45

### Hour (0-23)

- `*` - Every hour
- `0` - Midnight
- `12` - Noon
- `*/2` - Every 2 hours
- `9-17` - 9 AM through 5 PM
- `8,12,16` - At 8 AM, noon, and 4 PM

### Day of Week (0-6, 0=Sunday)

- `*` - Every day
- `0` - Sunday
- `1` - Monday
- `5` - Friday
- `1-5` - Monday through Friday (weekdays)
- `0,6` - Saturday and Sunday (weekends)

**Note:** Some cron implementations use 7 for Sunday; celers-beat uses 0.

### Day of Month (1-31)

- `*` - Every day
- `1` - First day of month
- `15` - 15th day of month
- `*/2` - Every other day
- `1,15` - 1st and 15th
- `L` - Last day of month (not supported yet)

### Month (1-12)

- `*` - Every month
- `1` - January
- `6` - June
- `*/3` - Every 3 months (quarterly)
- `1,4,7,10` - Quarterly (Jan, Apr, Jul, Oct)
- `6-8` - June through August (summer)

## Special Characters

### Asterisk (`*`)

Matches any value for that field.

```rust
// Every minute of every hour
Schedule::crontab("*", "*", "*", "*", "*")
```

### Step Values (`*/N`)

Executes every N units.

```rust
// Every 15 minutes
Schedule::crontab("*/15", "*", "*", "*", "*")

// Every 2 hours
Schedule::crontab("0", "*/2", "*", "*", "*")
```

### Comma (`,`)

Specifies a list of values.

```rust
// At 8:00, 12:00, and 16:00
Schedule::crontab("0", "8,12,16", "*", "*", "*")

// Monday, Wednesday, Friday
Schedule::crontab("0", "9", "1,3,5", "*", "*")
```

### Range (`-`)

Specifies a range of values.

```rust
// Monday through Friday at 9 AM
Schedule::crontab("0", "9", "1-5", "*", "*")

// Business hours (9 AM - 5 PM)
Schedule::crontab("0", "9-17", "*", "*", "*")
```

### Combining Operators

You can combine operators in a single field:

```rust
// Every 30 minutes during business hours on weekdays
Schedule::crontab("0,30", "9-17", "1-5", "*", "*")

// First and last Monday of each month at 9 AM
// (requires more complex logic - use custom schedule)
```

## Common Patterns

### Every Minute

```rust
Schedule::crontab("*", "*", "*", "*", "*")
```

### Every Hour

```rust
Schedule::crontab("0", "*", "*", "*", "*")
```

### Every Day at Midnight

```rust
Schedule::crontab("0", "0", "*", "*", "*")
```

### Every Day at Specific Time

```rust
// 9:00 AM
Schedule::crontab("0", "9", "*", "*", "*")

// 2:30 PM
Schedule::crontab("30", "14", "*", "*", "*")
```

### Every Weekday

```rust
// Monday-Friday at 9 AM
Schedule::crontab("0", "9", "1-5", "*", "*")
```

### Every Weekend

```rust
// Saturday and Sunday at 10 AM
Schedule::crontab("0", "10", "0,6", "*", "*")
```

### Weekly (Specific Day)

```rust
// Every Monday at 9 AM
Schedule::crontab("0", "9", "1", "*", "*")

// Every Friday at 5 PM
Schedule::crontab("0", "17", "5", "*", "*")
```

### Monthly (Specific Day)

```rust
// First of every month at midnight
Schedule::crontab("0", "0", "*", "1", "*")

// 15th of every month at noon
Schedule::crontab("0", "12", "*", "15", "*")
```

### Quarterly

```rust
// First day of Q1, Q2, Q3, Q4 at midnight
Schedule::crontab("0", "0", "*", "1", "1,4,7,10")
```

### Yearly

```rust
// January 1st at midnight
Schedule::crontab("0", "0", "*", "1", "1")

// July 4th at noon
Schedule::crontab("0", "12", "*", "4", "7")
```

### Every N Minutes

```rust
// Every 5 minutes
Schedule::crontab("*/5", "*", "*", "*", "*")

// Every 15 minutes
Schedule::crontab("*/15", "*", "*", "*", "*")

// Every 30 minutes
Schedule::crontab("*/30", "*", "*", "*", "*")
```

### Business Hours

```rust
// Every hour during business hours (9 AM - 5 PM) on weekdays
Schedule::crontab("0", "9-17", "1-5", "*", "*")

// Every 30 minutes during business hours
Schedule::crontab("0,30", "9-17", "1-5", "*", "*")
```

### Off-Hours

```rust
// Every hour outside business hours (6 PM - 8 AM)
// Morning: 0-8
Schedule::crontab("0", "0-8", "*", "*", "*")
// Evening: 18-23
Schedule::crontab("0", "18-23", "*", "*", "*")
```

## Timezone Support

### IANA Timezone Names

Use standard IANA timezone identifiers:

```rust
// Common US timezones
"America/New_York"      // Eastern
"America/Chicago"       // Central
"America/Denver"        // Mountain
"America/Los_Angeles"   // Pacific
"America/Anchorage"     // Alaska
"Pacific/Honolulu"      // Hawaii

// European timezones
"Europe/London"         // GMT/BST
"Europe/Paris"          // CET/CEST
"Europe/Berlin"         // CET/CEST
"Europe/Moscow"         // MSK

// Asian timezones
"Asia/Tokyo"            // JST
"Asia/Shanghai"         // CST
"Asia/Singapore"        // SGT
"Asia/Dubai"            // GST

// Australian timezones
"Australia/Sydney"      // AEDT/AEST
"Australia/Melbourne"   // AEDT/AEST
"Australia/Perth"       // AWST
```

### Automatic DST Handling

Timezone-aware schedules automatically handle Daylight Saving Time transitions:

```rust
// Runs at 9 AM local time year-round, adjusting for DST
let schedule = Schedule::crontab_tz(
    "0", "9", "*", "*", "*",
    "America/New_York"
);
```

### UTC vs Local Time

```rust
// UTC (always same absolute time)
let utc = Schedule::crontab("0", "14", "*", "*", "*");
// Always 2:00 PM UTC

// Local time (varies with DST)
let local = Schedule::crontab_tz(
    "0", "14", "*", "*", "*",
    "America/New_York"
);
// Always 2:00 PM Eastern (EDT or EST)
```

## Best Practices

### 1. Use Timezone-Aware Schedules for User-Facing Times

```rust
// Good: Users expect 9 AM in their timezone
Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York")

// Less ideal: 9 AM UTC might be middle of night for users
Schedule::crontab("0", "9", "1-5", "*", "*")
```

### 2. Avoid Exact Hour Boundaries for Heavy Tasks

```rust
// Good: Offset by a few minutes to avoid thundering herd
Schedule::crontab("5", "0", "*", "*", "*")  // 12:05 AM

// Less ideal: Everyone runs at midnight
Schedule::crontab("0", "0", "*", "*", "*")  // 12:00 AM
```

### 3. Use Intervals for Sub-Hourly Tasks

```rust
// Good: Simple and clear
Schedule::interval(300)  // Every 5 minutes

// Works but more complex
Schedule::crontab("*/5", "*", "*", "*", "*")
```

### 4. Document Business Logic

```rust
// Good: Clear intent
// Monday morning reports for previous week
let monday_reports = ScheduledTask::new(
    "weekly_report".to_string(),
    Schedule::crontab("0", "9", "1", "*", "*")
);

// Add comment explaining the schedule
```

### 5. Test with Multiple Iterations

```rust
use celers_beat::BeatScheduler;

let mut scheduler = BeatScheduler::new();
scheduler.add_task(task)?;

// Preview next 10 executions to verify schedule
let preview = scheduler.preview_upcoming_executions(Some(10), None);
```

### 6. Consider Holidays and Blackouts

```rust
use celers_beat::{ScheduledTask, BusinessCalendar, HolidayCalendar};

// Skip execution on US holidays
let task = ScheduledTask::new(
    "business_task".to_string(),
    Schedule::crontab("0", "9", "1-5", "*", "*")
);

// Then check calendar before execution (application logic)
```

## Examples

### Complete Working Examples

#### Daily Report at 9 AM Eastern Time

```rust
use celers_beat::{BeatScheduler, Schedule, ScheduledTask};

let mut scheduler = BeatScheduler::new();

let daily_report = ScheduledTask::new(
    "daily_report".to_string(),
    Schedule::crontab_tz("0", "9", "1-5", "*", "*", "America/New_York")
);

scheduler.add_task(daily_report)?;
```

#### Weekly Summary Every Monday

```rust
let weekly_summary = ScheduledTask::new(
    "weekly_summary".to_string(),
    Schedule::crontab("0", "8", "1", "*", "*")  // Monday 8 AM UTC
);
```

#### Monthly Billing on First Day

```rust
let monthly_billing = ScheduledTask::new(
    "monthly_billing".to_string(),
    Schedule::crontab("0", "0", "*", "1", "*")  // 1st of month, midnight
);
```

#### Quarterly Reports

```rust
let quarterly_report = ScheduledTask::new(
    "quarterly_report".to_string(),
    Schedule::crontab("0", "9", "*", "1", "1,4,7,10")  // Q1,Q2,Q3,Q4
);
```

#### Hourly Cleanup During Off-Hours

```rust
let night_cleanup = ScheduledTask::new(
    "night_cleanup".to_string(),
    Schedule::crontab("0", "0-6,22-23", "*", "*", "*")  // Midnight-6AM, 10PM-11PM
);
```

## Troubleshooting

### Schedule Not Running

1. **Check the schedule syntax**:
   ```rust
   let health = task.check_health();
   println!("{:?}", health);
   ```

2. **Verify timezone**:
   ```rust
   let next_run = schedule.next_run(None)?;
   println!("Next run: {} UTC", next_run);
   ```

3. **Preview upcoming executions**:
   ```rust
   let preview = scheduler.preview_upcoming_executions(Some(5), Some("task_name"));
   ```

### Unexpected Run Times

1. **DST transition**: Timezone-aware schedules adjust for DST
2. **Wrong timezone**: Verify IANA timezone name
3. **UTC confusion**: Ensure you're comparing times in the same timezone

### Testing Crontab Schedules

```rust
use chrono::Utc;

let schedule = Schedule::crontab("0", "9", "1-5", "*", "*");

// Test next 10 runs
let mut last_run = Utc::now();
for i in 0..10 {
    let next = schedule.next_run(Some(last_run))?;
    println!("Run {}: {}", i + 1, next);
    last_run = next;
}
```

## See Also

- [README.md](README.md) - Main documentation
- [PRODUCTION_GUIDE.md](PRODUCTION_GUIDE.md) - Production deployment
- [CELERY_MIGRATION.md](CELERY_MIGRATION.md) - Migration from Celery Beat
- [Cron Wikipedia](https://en.wikipedia.org/wiki/Cron) - Cron history and format
- [IANA Time Zone Database](https://www.iana.org/time-zones) - Timezone reference
