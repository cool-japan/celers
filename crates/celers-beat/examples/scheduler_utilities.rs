#[cfg(feature = "cron")]
use celers_beat::TimezoneUtils;
/// Scheduler Utilities Example
///
/// This example demonstrates the advanced scheduler utilities:
/// - Starvation prevention for low-priority tasks
/// - Schedule preview (upcoming executions)
/// - Dry-run simulation mode
/// - Comprehensive scheduler statistics
/// - Timezone conversion utilities
use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
use chrono::Utc;

fn main() {
    println!("=== Scheduler Utilities Demo ===\n");

    let mut scheduler = BeatScheduler::new();

    // ========================================================================
    // 1. Add tasks with different priorities
    // ========================================================================
    println!("1. Setting up tasks with different priorities");
    println!("------------------------------------------------------------");

    // High priority task
    let mut high_priority =
        ScheduledTask::new("critical_backup".to_string(), Schedule::interval(300));
    high_priority.options.priority = Some(9);
    scheduler.add_task(high_priority).unwrap();
    println!("Added critical_backup (priority 9)");

    // Medium priority task
    let mut medium_priority =
        ScheduledTask::new("send_report".to_string(), Schedule::interval(300));
    medium_priority.options.priority = Some(5);
    scheduler.add_task(medium_priority).unwrap();
    println!("Added send_report (priority 5)");

    // Low priority task
    let mut low_priority = ScheduledTask::new("cleanup_temp".to_string(), Schedule::interval(300));
    low_priority.options.priority = Some(2);
    scheduler.add_task(low_priority).unwrap();
    println!("Added cleanup_temp (priority 2)\n");

    // ========================================================================
    // 2. Starvation Prevention
    // ========================================================================
    println!("2. Starvation Prevention");
    println!("------------------------------------------------------------");

    // Check for task starvation (60 minute threshold)
    let waiting_info = scheduler.get_tasks_with_starvation_prevention(Some(60), Some(2));
    println!("Checking for starved tasks (threshold: 60 minutes):");
    for info in &waiting_info {
        if info.priority_boosted {
            println!(
                "  ⚠ {} priority BOOSTED: {} -> {} ({})",
                info.task_name,
                info.original_priority.unwrap_or(5),
                info.effective_priority,
                info.boost_reason.as_ref().unwrap()
            );
        } else {
            println!(
                "  ✓ {} priority: {} (no boost needed)",
                info.task_name, info.effective_priority
            );
        }
    }

    // Get due tasks with starvation prevention
    let due_with_prevention = scheduler.get_due_tasks_with_starvation_prevention(Some(60));
    println!(
        "\nDue tasks with starvation prevention: {}",
        due_with_prevention.len()
    );
    for task in &due_with_prevention {
        println!(
            "  - {} (priority: {})",
            task.name,
            task.options.priority.unwrap_or(5)
        );
    }
    println!();

    // ========================================================================
    // 3. Schedule Preview
    // ========================================================================
    println!("3. Schedule Preview - Upcoming Executions");
    println!("------------------------------------------------------------");

    // Preview next 5 executions for all tasks
    let preview = scheduler.preview_upcoming_executions(Some(5), None);
    println!("Preview of next 5 executions:");
    for (task_name, times) in &preview {
        println!("\n  {}:", task_name);
        for (i, time) in times.iter().enumerate() {
            println!(
                "    {}. {} (in {} seconds)",
                i + 1,
                time.format("%Y-%m-%d %H:%M:%S UTC"),
                (time.signed_duration_since(Utc::now())).num_seconds()
            );
        }
    }

    // Preview specific task
    println!("\n\nPreview for 'send_report' task only:");
    let single_preview = scheduler.preview_upcoming_executions(Some(3), Some("send_report"));
    if let Some(times) = single_preview.get("send_report") {
        for (i, time) in times.iter().enumerate() {
            println!("  {}. {}", i + 1, time.format("%Y-%m-%d %H:%M:%S UTC"));
        }
    }
    println!();

    // ========================================================================
    // 4. Dry-Run Simulation
    // ========================================================================
    println!("4. Dry-Run Simulation");
    println!("------------------------------------------------------------");

    // Simulate 10 minutes of execution
    let simulation = scheduler.dry_run(600, Some(1));
    println!("Simulating 10 minutes of execution:");
    println!("  Total executions: {}", simulation.len());

    if !simulation.is_empty() {
        println!("\n  First 10 executions:");
        for (time, task_name) in simulation.iter().take(10) {
            println!("    {} at {}", task_name, time.format("%H:%M:%S"));
        }

        // Count executions per task
        let mut task_counts: std::collections::HashMap<String, usize> =
            std::collections::HashMap::new();
        for (_, task_name) in &simulation {
            *task_counts.entry(task_name.clone()).or_insert(0) += 1;
        }

        println!("\n  Executions per task:");
        for (task_name, count) in &task_counts {
            println!("    {}: {} executions", task_name, count);
        }
    }
    println!();

    // ========================================================================
    // 5. Comprehensive Statistics
    // ========================================================================
    println!("5. Comprehensive Scheduler Statistics");
    println!("------------------------------------------------------------");

    // Simulate some task runs to generate statistics
    for task_name in ["critical_backup", "send_report", "cleanup_temp"] {
        scheduler.mark_task_run(task_name).ok();
    }

    let stats = scheduler.get_comprehensive_stats();
    println!("{}", stats);

    // ========================================================================
    // 6. Timezone Utilities (requires cron feature)
    // ========================================================================
    #[cfg(feature = "cron")]
    {
        println!("6. Timezone Conversion Utilities");
        println!("------------------------------------------------------------");

        // Format time in different timezones
        println!("Current time in different timezones:");
        let zones = vec![
            "America/New_York",
            "Europe/London",
            "Asia/Tokyo",
            "Australia/Sydney",
        ];

        let times = TimezoneUtils::current_time_in_zones(&zones);
        for (tz, time) in &times {
            println!("  {}: {}", tz, time);
        }

        // Calculate time until next 9 AM New York time
        match TimezoneUtils::time_until_next_occurrence(9, 0, "America/New_York") {
            Ok(duration) => {
                println!("\nTime until next 9:00 AM New York:");
                println!(
                    "  {} hours {} minutes",
                    duration.num_hours(),
                    duration.num_minutes() % 60
                );
            }
            Err(e) => println!("Error calculating time: {}", e),
        }
        println!();
    }

    #[cfg(not(feature = "cron"))]
    {
        println!("6. Timezone Utilities");
        println!("------------------------------------------------------------");
        println!("(Requires 'cron' feature to be enabled)");
        println!("Run with: cargo run --example scheduler_utilities --features cron\n");
    }

    // ========================================================================
    // 7. Task Waiting Analysis
    // ========================================================================
    println!("7. Task Waiting Time Analysis");
    println!("------------------------------------------------------------");

    println!("Task waiting times:");
    for info in &waiting_info {
        let waiting_seconds = info.waiting_duration.num_seconds();
        let waiting_display = if waiting_seconds > 86400 {
            format!("{} days", waiting_seconds / 86400)
        } else if waiting_seconds > 3600 {
            format!("{} hours", waiting_seconds / 3600)
        } else if waiting_seconds > 60 {
            format!("{} minutes", waiting_seconds / 60)
        } else {
            format!("{} seconds", waiting_seconds)
        };

        println!("  {}: waiting {}", info.task_name, waiting_display);
    }
    println!();

    println!("=== Demo Complete ===");
    println!("\nKey Features Demonstrated:");
    println!("  ✓ Starvation prevention with automatic priority boosting");
    println!("  ✓ Schedule preview for capacity planning");
    println!("  ✓ Dry-run simulation for testing");
    println!("  ✓ Comprehensive scheduler statistics");
    println!("  ✓ Timezone conversion utilities");
}
