/// CLI Utilities Example
///
/// This example demonstrates how to build command-line tools for scheduler management:
/// - List all tasks with status
/// - Inspect task details
/// - Enable/disable tasks
/// - Export scheduler state
/// - Validate schedule configuration
/// - Generate schedule reports
///
/// Run with: cargo run --example cli_utilities --features cron
///
/// In production, you would use clap or another CLI framework to parse arguments:
/// Example commands this could support:
///   scheduler list
///   scheduler inspect <task-name>
///   scheduler enable <task-name>
///   scheduler disable <task-name>
///   scheduler export [--format json|yaml]
///   scheduler validate
///   scheduler report [--hours 24]
use celers_beat::{BeatScheduler, ScheduledTask};
use chrono::Utc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== CLI Utilities Demo ===\n");

    // Load or create scheduler
    let state_file = std::env::temp_dir()
        .join("cli_scheduler.json")
        .to_string_lossy()
        .to_string();
    let scheduler = match BeatScheduler::load_from_file(&state_file) {
        Ok(s) => {
            println!("✓ Loaded scheduler from {}\n", state_file);
            s
        }
        Err(_) => {
            println!("✓ Creating new scheduler\n");
            let mut s = BeatScheduler::with_persistence(state_file);
            setup_example_tasks(&mut s)?;
            s
        }
    };

    // Demonstrate CLI commands
    println!("=== Command: list ===");
    cmd_list(&scheduler);

    println!("\n=== Command: inspect send_report ===");
    cmd_inspect(&scheduler, "send_report");

    println!("\n=== Command: validate ===");
    cmd_validate(&scheduler);

    println!("\n=== Command: report --hours 24 ===");
    cmd_report(&scheduler, 24);

    println!("\n=== Command: health ===");
    cmd_health(&scheduler);

    println!("\n=== Command: metrics ===");
    cmd_metrics(&scheduler);

    println!("\n=== Command: export --format json ===");
    cmd_export(&scheduler, "json")?;

    println!("\n=== Command: conflicts ===");
    cmd_conflicts(&scheduler);

    println!("\n=== Command: dependencies ===");
    cmd_dependencies(&scheduler)?;

    Ok(())
}

/// Setup example tasks for demonstration
fn setup_example_tasks(scheduler: &mut BeatScheduler) -> Result<(), Box<dyn std::error::Error>> {
    use celers_beat::Schedule;

    // Add various tasks
    let tasks = vec![
        ("database_backup", Schedule::interval(86400), Some(9)),
        ("send_report", Schedule::interval(3600), Some(5)),
        ("cleanup_temp", Schedule::interval(7200), Some(2)),
        ("log_rotation", Schedule::interval(3600), Some(7)),
    ];

    for (name, schedule, priority) in tasks {
        let mut task = ScheduledTask::new(name.to_string(), schedule);
        if let Some(p) = priority {
            task.options.priority = Some(p);
        }
        scheduler.add_task(task)?;
    }

    // Simulate some executions
    scheduler.mark_task_run("send_report")?;
    scheduler.mark_task_run("cleanup_temp")?;

    scheduler.save_state()?;
    Ok(())
}

/// List all tasks with status
fn cmd_list(scheduler: &BeatScheduler) {
    let tasks = scheduler.list_tasks();

    if tasks.is_empty() {
        println!("No tasks scheduled");
        return;
    }

    println!("┌{0:─<50}┬{0:─<12}┬{0:─<10}┬{0:─<8}┐", "");
    println!(
        "│ {:<48} │ {:<10} │ {:<8} │ {:<6} │",
        "Task Name", "Status", "Priority", "Runs"
    );
    println!("├{0:─<50}┼{0:─<12}┼{0:─<10}┼{0:─<8}┤", "");

    for (name, task) in tasks {
        let status = if task.enabled { "enabled" } else { "disabled" };
        let priority = task.options.priority.unwrap_or(5);
        let runs = task.total_run_count;

        println!(
            "│ {:<48} │ {:<10} │ {:>8} │ {:>6} │",
            truncate(name, 48),
            status,
            priority,
            runs
        );
    }

    println!("└{0:─<50}┴{0:─<12}┴{0:─<10}┴{0:─<8}┘", "");
}

/// Inspect detailed task information
fn cmd_inspect(scheduler: &BeatScheduler, task_name: &str) {
    match scheduler.get_task(task_name) {
        Some(task) => {
            println!("Task: {}", task.name);
            println!("├─ Schedule: {}", task.schedule);
            println!("├─ Enabled: {}", task.enabled);
            println!("├─ Priority: {}", task.options.priority.unwrap_or(5));

            if let Some(queue) = &task.options.queue {
                println!("├─ Queue: {}", queue);
            }

            if let Some(expires) = task.options.expires {
                println!("├─ Expires: {}s", expires);
            }

            println!("├─ Total Runs: {}", task.total_run_count);

            if let Some(last_run) = task.last_run_at {
                println!("├─ Last Run: {}", last_run.format("%Y-%m-%d %H:%M:%S UTC"));
            } else {
                println!("├─ Last Run: Never");
            }

            // Show next run time
            if let Ok(next_run) = task.next_run_time() {
                let duration = next_run.signed_duration_since(Utc::now());
                let seconds = duration.num_seconds();

                let time_str = if seconds < 60 {
                    format!("{}s", seconds)
                } else if seconds < 3600 {
                    format!("{}m {}s", seconds / 60, seconds % 60)
                } else if seconds < 86400 {
                    format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60)
                } else {
                    format!("{}d {}h", seconds / 86400, (seconds % 86400) / 3600)
                };

                println!(
                    "├─ Next Run: {} (in {})",
                    next_run.format("%Y-%m-%d %H:%M:%S UTC"),
                    time_str
                );
            }

            // Show execution history summary
            let history = &task.execution_history;
            if !history.is_empty() {
                println!("├─ Recent Executions:");
                let recent = history.iter().rev().take(10);
                let count = recent.clone().count();
                for (i, record) in recent.enumerate() {
                    let prefix = if i == count - 1 { "└" } else { "│" };
                    println!(
                        "│  {} {} - {:?} ({}ms)",
                        prefix,
                        record.started_at.format("%H:%M:%S"),
                        record.result,
                        record.duration_ms.unwrap_or(0)
                    );
                }
            }
        }
        None => {
            println!("Error: Task '{}' not found", task_name);
        }
    }
}

/// Validate all schedules
fn cmd_validate(scheduler: &BeatScheduler) {
    let health_results = scheduler.check_all_tasks_health();

    let mut healthy = 0;
    let mut warnings = 0;
    let mut unhealthy = 0;

    for result in &health_results {
        if result.health.is_healthy() {
            healthy += 1;
        } else if result.health.has_warnings() {
            warnings += 1;
        } else if result.health.is_unhealthy() {
            unhealthy += 1;
        }
    }

    println!("Schedule Validation Results:");
    println!("  ✓ Healthy: {}", healthy);

    if warnings > 0 {
        println!("  ⚠ Warnings: {}", warnings);
        for result in health_results.iter().filter(|r| r.health.has_warnings()) {
            println!(
                "    - {}: {:?}",
                result.task_name,
                result.health.get_issues()
            );
        }
    }

    if unhealthy > 0 {
        println!("  ✗ Unhealthy: {}", unhealthy);
        for result in health_results.iter().filter(|r| r.health.is_unhealthy()) {
            println!(
                "    - {}: {:?}",
                result.task_name,
                result.health.get_issues()
            );
        }
    }

    if unhealthy == 0 && warnings == 0 {
        println!("\n✓ All schedules valid");
    }
}

/// Generate schedule report
fn cmd_report(scheduler: &BeatScheduler, hours: u32) {
    println!("Schedule Report (next {} hours)", hours);
    println!("────────────────────────────────");

    // Get upcoming executions for all tasks
    let preview = scheduler.preview_upcoming_executions(Some(10), None);

    let mut timeline: Vec<(chrono::DateTime<Utc>, String)> = Vec::new();
    let cutoff = Utc::now() + chrono::Duration::hours(hours as i64);

    for (task_name, times) in preview {
        for time in times {
            if time <= cutoff {
                timeline.push((time, task_name.clone()));
            }
        }
    }

    timeline.sort_by_key(|(time, _)| *time);

    if timeline.is_empty() {
        println!("No scheduled executions in the next {} hours", hours);
        return;
    }

    println!("\nUpcoming Executions:");
    for (time, task_name) in timeline.iter().take(20) {
        let duration = time.signed_duration_since(Utc::now());
        let in_str = format_duration(duration.num_seconds());

        println!(
            "  {} - {} (in {})",
            time.format("%m/%d %H:%M"),
            task_name,
            in_str
        );
    }

    if timeline.len() > 20 {
        println!("  ... and {} more", timeline.len() - 20);
    }
}

/// Show health status
fn cmd_health(scheduler: &BeatScheduler) {
    let stats = scheduler.get_comprehensive_stats();

    println!("Scheduler Health:");
    println!("  Total Tasks: {}", stats.total_tasks);
    println!("  Enabled: {}", stats.enabled_tasks);
    println!("  Disabled: {}", stats.total_tasks - stats.enabled_tasks);
    println!();
    println!("  Healthy: {}", stats.healthy_tasks);
    println!("  Warnings: {}", stats.warning_tasks);
    println!("  Unhealthy: {}", stats.unhealthy_tasks);

    let overall_health = if stats.unhealthy_tasks > 0 {
        "UNHEALTHY ✗"
    } else if stats.warning_tasks > 0 {
        "WARNING ⚠"
    } else {
        "HEALTHY ✓"
    };

    println!();
    println!("  Overall: {}", overall_health);
}

/// Show scheduler metrics
fn cmd_metrics(scheduler: &BeatScheduler) {
    let stats = scheduler.get_comprehensive_stats();

    println!("Scheduler Metrics:");
    println!("  Executions:");
    println!("    Total: {}", stats.total_executions);
    println!("    Failures: {}", stats.total_failures);
    println!("    Timeouts: {}", stats.total_timeouts);
    println!("    Success Rate: {:.2}%", stats.success_rate * 100.0);

    if let Some(avg_duration) = stats.avg_duration_ms {
        println!();
        println!("  Performance:");
        println!("    Avg Duration: {}ms", avg_duration);
    }

    if stats.total_executions > 0 {
        if let (Some(oldest), Some(newest)) = (stats.oldest_execution, stats.newest_execution) {
            let uptime = newest.signed_duration_since(oldest);
            println!();
            println!("  Uptime: {}", format_duration(uptime.num_seconds()));
        }
    }
}

/// Export scheduler state
fn cmd_export(scheduler: &BeatScheduler, format: &str) -> Result<(), Box<dyn std::error::Error>> {
    match format {
        "json" => {
            let json = scheduler.export_state()?;
            println!("Exported state (JSON):");
            println!("{}", json);
        }
        _ => {
            println!("Error: Unsupported format '{}'. Supported: json", format);
        }
    }

    Ok(())
}

/// Show schedule conflicts
fn cmd_conflicts(scheduler: &BeatScheduler) {
    let conflicts = scheduler.detect_conflicts(3600, 60);

    if conflicts.is_empty() {
        println!("✓ No schedule conflicts detected");
        return;
    }

    println!("Schedule Conflicts:");
    for conflict in &conflicts {
        println!(
            "  {:?}: {} ↔ {} (overlap: {}s)",
            conflict.severity, conflict.task1, conflict.task2, conflict.overlap_seconds
        );
    }

    println!("\nTotal conflicts: {}", conflicts.len());
}

/// Show task dependencies
fn cmd_dependencies(scheduler: &BeatScheduler) -> Result<(), Box<dyn std::error::Error>> {
    let tasks = scheduler.list_tasks();
    let mut has_dependencies = false;

    for (name, task) in tasks {
        if task.has_dependencies() {
            has_dependencies = true;
            println!("  {} depends on:", name);
            for dep in &task.dependencies {
                println!("    - {}", dep);
            }
        }
    }

    if !has_dependencies {
        println!("No task dependencies defined");
    }

    Ok(())
}

/// Helper: Truncate string to max length
fn truncate(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len - 3])
    }
}

/// Helper: Format duration in human-readable form
fn format_duration(seconds: i64) -> String {
    if seconds < 60 {
        format!("{}s", seconds)
    } else if seconds < 3600 {
        format!("{}m {}s", seconds / 60, seconds % 60)
    } else if seconds < 86400 {
        format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60)
    } else {
        format!("{}d {}h", seconds / 86400, (seconds % 86400) / 3600)
    }
}
