/// Weighted Fair Queuing (WFQ) Example
///
/// Demonstrates how to use WFQ for fair task scheduling based on task weights.
/// Higher-weight tasks get proportionally more execution time while preventing
/// starvation of lower-weight tasks.
///
/// Run with: cargo run --example weighted_fair_queuing
use celers_beat::{BeatScheduler, Schedule, ScheduledTask};

fn main() {
    println!("=== Weighted Fair Queuing Demo ===\n");

    let mut scheduler = BeatScheduler::new();

    // ========================================================================
    // 1. Create tasks with different weights
    // ========================================================================
    println!("1. Creating tasks with different weights");
    println!("------------------------------------------------------------");

    // Critical task (weight 5.0) - gets high priority
    let critical_task = ScheduledTask::new("critical_backup".to_string(), Schedule::interval(60))
        .with_wfq_weight(5.0)
        .unwrap();
    println!("✓ Critical task (weight=5.0): critical_backup");

    // Important task (weight 3.0) - medium-high priority
    let important_task = ScheduledTask::new("important_report".to_string(), Schedule::interval(60))
        .with_wfq_weight(3.0)
        .unwrap();
    println!("✓ Important task (weight=3.0): important_report");

    // Normal task (weight 1.0) - standard priority
    let normal_task = ScheduledTask::new("normal_cleanup".to_string(), Schedule::interval(60))
        .with_wfq_weight(1.0)
        .unwrap();
    println!("✓ Normal task (weight=1.0): normal_cleanup");

    // Background task (weight 0.5) - low priority
    let background_task = ScheduledTask::new(
        "background_optimization".to_string(),
        Schedule::interval(60),
    )
    .with_wfq_weight(0.5)
    .unwrap();
    println!("✓ Background task (weight=0.5): background_optimization\n");

    // Add all tasks to scheduler
    scheduler.add_task(critical_task).unwrap();
    scheduler.add_task(important_task).unwrap();
    scheduler.add_task(normal_task).unwrap();
    scheduler.add_task(background_task).unwrap();

    // ========================================================================
    // 2. Display WFQ Statistics
    // ========================================================================
    println!("2. WFQ Statistics");
    println!("------------------------------------------------------------");
    let stats = scheduler.get_wfq_stats();
    println!("{}", stats);
    println!("  Total weight: {:.2}", stats.total_weight);
    println!("  Average weight: {:.2}", stats.average_weight);
    println!(
        "  Tasks with WFQ: {}/{}\n",
        stats.tasks_with_wfq_config, stats.total_tasks
    );

    // ========================================================================
    // 3. Simulate task executions with WFQ
    // ========================================================================
    println!("3. Simulating task executions");
    println!("------------------------------------------------------------");

    // Wait for tasks to become due
    std::thread::sleep(std::time::Duration::from_secs(1));

    // Get due tasks using WFQ
    let due_tasks = scheduler.get_due_tasks_wfq();
    println!("Due tasks (ordered by virtual finish time):");
    for (idx, task_info) in due_tasks.iter().enumerate() {
        println!(
            "  {}. {} (weight={:.1}, vft={:.2})",
            idx + 1,
            task_info.name,
            task_info.weight,
            task_info.virtual_finish_time
        );
    }
    println!();

    // ========================================================================
    // 4. Simulate executions and update virtual times
    // ========================================================================
    println!("4. Executing tasks and updating virtual times");
    println!("------------------------------------------------------------");

    // Simulate execution of each task
    let execution_times = vec![
        ("critical_backup", 20.0),
        ("important_report", 15.0),
        ("normal_cleanup", 10.0),
        ("background_optimization", 5.0),
    ];

    for (task_name, duration) in &execution_times {
        scheduler
            .update_wfq_after_execution(task_name, *duration)
            .unwrap();
        println!("✓ Executed '{}' (duration={:.1}s)", task_name, duration);
    }
    println!();

    // ========================================================================
    // 5. Show updated virtual times
    // ========================================================================
    println!("5. Virtual times after execution");
    println!("------------------------------------------------------------");

    for task_name in [
        "critical_backup",
        "important_report",
        "normal_cleanup",
        "background_optimization",
    ] {
        if let Some(task) = scheduler.list_tasks().get(task_name) {
            let vft = task.wfq_finish_time();
            let weight = task.wfq_weight();
            println!(
                "  {}: weight={:.1}, virtual_finish_time={:.2}",
                task_name, weight, vft
            );
        }
    }
    println!();

    // ========================================================================
    // 6. Demonstrate fairness over multiple executions
    // ========================================================================
    println!("6. Fairness demonstration (multiple executions)");
    println!("------------------------------------------------------------");

    // Simulate 5 rounds of executions
    for round in 1..=5 {
        // Wait for tasks to become due again
        std::thread::sleep(std::time::Duration::from_millis(100));

        let due_tasks = scheduler.get_due_tasks_wfq();
        if !due_tasks.is_empty() {
            println!("\nRound {}:", round);
            for task_info in due_tasks.iter().take(2) {
                println!(
                    "  Scheduling: {} (vft={:.2})",
                    task_info.name, task_info.virtual_finish_time
                );

                // Simulate execution
                let duration = match task_info.name.as_str() {
                    "critical_backup" => 20.0,
                    "important_report" => 15.0,
                    "normal_cleanup" => 10.0,
                    "background_optimization" => 5.0,
                    _ => 10.0,
                };

                scheduler
                    .update_wfq_after_execution(&task_info.name, duration)
                    .unwrap();
            }
        }
    }
    println!();

    // ========================================================================
    // 7. Final statistics
    // ========================================================================
    println!("7. Final WFQ Statistics");
    println!("------------------------------------------------------------");

    let final_stats = scheduler.get_wfq_stats();
    println!("{}", final_stats);
    println!(
        "\nGlobal virtual time: {:.2}",
        final_stats.global_virtual_time
    );

    println!("\nTask execution summary:");
    for task_name in [
        "critical_backup",
        "important_report",
        "normal_cleanup",
        "background_optimization",
    ] {
        if let Some(task) = scheduler.list_tasks().get(task_name) {
            if let Some(wfq_state) = &task.wfq_state {
                println!("  {}:", task_name);
                println!("    Weight: {:.1}", wfq_state.weight.value());
                println!(
                    "    Total execution time: {:.1}s",
                    wfq_state.total_execution_time
                );
                println!(
                    "    Virtual finish time: {:.2}",
                    wfq_state.virtual_finish_time
                );
                println!(
                    "    Fairness ratio: {:.2}",
                    wfq_state.total_execution_time / wfq_state.weight.value()
                );
            }
        }
    }

    // ========================================================================
    // 8. Key takeaways
    // ========================================================================
    println!("\n=== Key Takeaways ===");
    println!("------------------------------------------------------------");
    println!("• Tasks with higher weights get more execution time");
    println!("• Virtual time ensures fairness over the long run");
    println!("• Lower-weight tasks are never starved");
    println!("• WFQ balances priority with fair resource allocation");
    println!("• Suitable for production workloads with mixed priorities");
    println!();
}
