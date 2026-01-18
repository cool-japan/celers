/// Advanced Features Example
///
/// This example demonstrates:
/// - Country-specific holiday calendars (US, Japan, UK, Canada)
/// - Priority-based task execution
/// - Automatic conflict resolution
/// - Missed schedule detection
/// - Comprehensive alerting and monitoring
use celers_beat::{BeatScheduler, HolidayCalendar, Schedule, ScheduledTask};

fn main() {
    println!("=== Advanced Features Demo ===\n");

    // ========================================================================
    // 1. Country-Specific Holiday Calendars
    // ========================================================================
    println!("1. Holiday Calendars");
    println!("------------------------------------------------------------");

    // US Federal Holidays (11 holidays)
    let us_holidays = HolidayCalendar::us_federal(2025);
    println!("US Federal Holidays 2025: {} holidays", us_holidays.len());

    // Japan National Holidays (16 holidays)
    let japan_holidays = HolidayCalendar::japan(2025);
    println!(
        "Japan National Holidays 2025: {} holidays",
        japan_holidays.len()
    );

    // UK Public Holidays (8 bank holidays)
    let uk_holidays = HolidayCalendar::uk(2025);
    println!("UK Public Holidays 2025: {} holidays", uk_holidays.len());

    // Canada Statutory Holidays (9 federal holidays)
    let canada_holidays = HolidayCalendar::canada(2025);
    println!(
        "Canada Statutory Holidays 2025: {} holidays\n",
        canada_holidays.len()
    );

    // ========================================================================
    // 2. Priority-Based Task Execution
    // ========================================================================
    println!("2. Priority-Based Task Execution");
    println!("------------------------------------------------------------");

    let mut scheduler = BeatScheduler::new();

    // Critical priority task (priority 9)
    let mut critical_task =
        ScheduledTask::new("critical_backup".to_string(), Schedule::interval(300));
    critical_task.options.priority = Some(9);
    scheduler.add_task(critical_task).unwrap();
    println!("Added critical_backup (priority: 9)");

    // High priority task (priority 7)
    let mut high_task = ScheduledTask::new("log_rotation".to_string(), Schedule::interval(300));
    high_task.options.priority = Some(7);
    scheduler.add_task(high_task).unwrap();
    println!("Added log_rotation (priority: 7)");

    // Normal priority task (priority 5 - default)
    let normal_task = ScheduledTask::new("send_report".to_string(), Schedule::interval(300));
    scheduler.add_task(normal_task).unwrap();
    println!("Added send_report (priority: 5 - default)");

    // Low priority task (priority 2)
    let mut low_task = ScheduledTask::new("cleanup_temp".to_string(), Schedule::interval(300));
    low_task.options.priority = Some(2);
    scheduler.add_task(low_task).unwrap();
    println!("Added cleanup_temp (priority: 2)");

    // Get tasks ordered by priority
    let ordered_tasks = scheduler.get_tasks_by_priority();
    println!("\nTask execution order (by priority):");
    for task in ordered_tasks {
        let priority = task.options.priority.unwrap_or(5);
        println!("  - {} (priority: {})", task.name, priority);
    }
    println!();

    // ========================================================================
    // 3. Schedule Conflict Detection and Resolution
    // ========================================================================
    println!("3. Schedule Conflict Detection");
    println!("------------------------------------------------------------");

    // Detect conflicts (check 1 hour window, assuming 60s task duration)
    let conflicts = scheduler.detect_conflicts(3600, 60);
    if conflicts.is_empty() {
        println!("No schedule conflicts detected");
    } else {
        println!("Found {} conflicts:", conflicts.len());
        for conflict in &conflicts {
            println!(
                "  - {} <-> {} (overlap: {}s, severity: {:?})",
                conflict.task1, conflict.task2, conflict.overlap_seconds, conflict.severity
            );
        }
    }

    // Preview automatic conflict resolutions
    let resolutions = scheduler.preview_conflict_resolutions(3600, 60, Some(30));
    if !resolutions.is_empty() {
        println!("\nProposed conflict resolutions:");
        for (task, resolution) in &resolutions {
            println!("  - {}: {}", task, resolution);
        }

        // Apply automatic resolutions
        println!("\nApplying automatic resolutions...");
        let applied = scheduler.auto_resolve_conflicts(3600, 60, Some(30));
        println!("Applied {} resolutions", applied.len());
    }
    println!();

    // ========================================================================
    // 4. Missed Schedule Detection
    // ========================================================================
    println!("4. Missed Schedule Detection");
    println!("------------------------------------------------------------");

    // Detect missed schedules (grace period: 60 seconds)
    let missed = scheduler.detect_missed_schedules(60);
    if missed.is_empty() {
        println!("No missed schedules detected");
    } else {
        println!("Found {} missed schedules:", missed.len());
        for (task_name, missed_by) in &missed {
            println!(
                "  - {} missed by {} seconds",
                task_name,
                missed_by.num_seconds()
            );
        }
    }

    // Get detailed statistics
    let stats = scheduler.get_missed_schedule_stats(Some(60));
    if !stats.is_empty() {
        println!("\nMissed Schedule Statistics:");
        for (task, seconds, schedule_type) in stats {
            println!("  - {} ({}) missed by {}s", task, schedule_type, seconds);
        }
    }
    println!();

    // ========================================================================
    // 5. Comprehensive Alerting
    // ========================================================================
    println!("5. Alerting System");
    println!("------------------------------------------------------------");

    // Check for missed schedules and trigger alerts
    let alert_count = scheduler.check_missed_schedules(Some(60));
    println!("Triggered {} alerts for missed schedules", alert_count);

    // Get all alerts
    let all_alerts = scheduler.get_alerts();
    println!("Total alerts: {}", all_alerts.len());

    // Get critical alerts only
    let critical_alerts = scheduler.get_critical_alerts();
    if !critical_alerts.is_empty() {
        println!("\nCritical Alerts:");
        for alert in critical_alerts {
            println!(
                "  - [{}] {}: {}",
                alert.level, alert.task_name, alert.message
            );
        }
    }
    println!();

    // ========================================================================
    // 6. Task Health Monitoring
    // ========================================================================
    println!("6. Health Monitoring");
    println!("------------------------------------------------------------");

    let health_results = scheduler.check_all_tasks_health();
    println!("Checked health of {} tasks", health_results.len());

    let unhealthy = scheduler.get_unhealthy_tasks();
    if unhealthy.is_empty() {
        println!("All tasks are healthy");
    } else {
        println!("Found {} unhealthy tasks:", unhealthy.len());
        for result in unhealthy {
            println!("  - {} ({:?})", result.task_name, result.health);
        }
    }
    println!();

    // ========================================================================
    // 7. Advanced Task Queries
    // ========================================================================
    println!("7. Advanced Task Queries");
    println!("------------------------------------------------------------");

    // Get due tasks ordered by priority
    let due_by_priority = scheduler.get_due_tasks_by_priority();
    println!("Due tasks (ordered by priority): {}", due_by_priority.len());
    for task in due_by_priority {
        let priority = task.options.priority.unwrap_or(5);
        println!("  - {} (priority: {})", task.name, priority);
    }
    println!();

    println!("=== Demo Complete ===");
}
