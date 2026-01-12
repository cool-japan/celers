//! Simple scheduler example showing basic usage
//!
//! Run with: cargo run --example simple_scheduler

use celers_beat::{BeatScheduler, Schedule, ScheduledTask};
use chrono::Utc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Simple Scheduler Example ===\n");

    let state_file = "/tmp/scheduler_simple.json";
    let mut scheduler = BeatScheduler::with_persistence(state_file.to_string());

    // Add interval tasks
    let task1 = ScheduledTask::new("cleanup".to_string(), Schedule::interval(60));
    scheduler.add_task(task1)?;
    println!("✓ Added task 'cleanup' (every 60 seconds)");

    let task2 = ScheduledTask::new("report".to_string(), Schedule::interval(300));
    scheduler.add_task(task2)?;
    println!("✓ Added task 'report' (every 300 seconds)");

    // Add one-time task
    let run_at = Utc::now() + chrono::Duration::hours(2);
    let task3 = ScheduledTask::new("migration".to_string(), Schedule::onetime(run_at));
    scheduler.add_task(task3)?;
    println!("✓ Added one-time task 'migration' (in 2 hours)");

    // Save state (automatic with persistence)
    scheduler.save_state()?;
    println!("\n✓ Saved scheduler state to {}", state_file);

    // Load state
    let _loaded = BeatScheduler::load_from_file(state_file)?;
    println!("✓ Loaded tasks from file successfully");

    println!("\n=== Success! ===");

    Ok(())
}
