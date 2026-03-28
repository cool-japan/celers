//! Schedule management command implementations.

use colored::Colorize;
use tabled::{settings::Style, Table, Tabled};

/// List all scheduled tasks
pub async fn list_schedules(broker_url: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!("{}", "=== Scheduled Tasks ===".bold().cyan());
    println!();

    // Schedules are stored with keys like: celers:schedule:<name>
    let schedule_pattern = "celers:schedule:*";
    let mut cursor = 0;
    let mut schedule_keys: Vec<String> = Vec::new();

    loop {
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(schedule_pattern)
            .arg("COUNT")
            .arg(100)
            .query_async(&mut conn)
            .await?;

        schedule_keys.extend(keys);
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    if schedule_keys.is_empty() {
        println!("{}", "No scheduled tasks found".yellow());
        println!();
        println!("Add a schedule with: celers schedule add <name> --task <task> --cron <expr>");
        return Ok(());
    }

    #[derive(Tabled)]
    struct ScheduleInfo {
        #[tabled(rename = "Name")]
        name: String,
        #[tabled(rename = "Task")]
        task: String,
        #[tabled(rename = "Cron")]
        cron: String,
        #[tabled(rename = "Status")]
        status: String,
        #[tabled(rename = "Last Run")]
        last_run: String,
    }

    let mut schedules = Vec::new();

    for key in &schedule_keys {
        // Extract schedule name from key
        let name = key.strip_prefix("celers:schedule:").unwrap_or(key);

        // Get schedule data
        let schedule_data: Option<String> =
            redis::cmd("GET").arg(key).query_async(&mut conn).await?;

        if let Some(data) = schedule_data {
            if let Ok(schedule) = serde_json::from_str::<serde_json::Value>(&data) {
                let task = schedule
                    .get("task")
                    .and_then(|v| v.as_str())
                    .unwrap_or("N/A")
                    .to_string();
                let cron = schedule
                    .get("cron")
                    .and_then(|v| v.as_str())
                    .unwrap_or("N/A")
                    .to_string();

                // Check if paused
                let pause_key = format!("celers:schedule:{name}:paused");
                let paused: Option<String> = redis::cmd("GET")
                    .arg(&pause_key)
                    .query_async(&mut conn)
                    .await?;

                let status = if paused.is_some() {
                    "Paused".to_string()
                } else {
                    "Active".to_string()
                };

                let last_run = schedule
                    .get("last_run")
                    .and_then(|v| v.as_str())
                    .unwrap_or("Never")
                    .to_string();

                schedules.push(ScheduleInfo {
                    name: name.to_string(),
                    task,
                    cron,
                    status,
                    last_run,
                });
            }
        }
    }

    let table = Table::new(schedules).with(Style::rounded()).to_string();
    println!("{table}");
    println!();
    println!(
        "{}",
        format!("Total scheduled tasks: {}", schedule_keys.len())
            .cyan()
            .bold()
    );

    Ok(())
}

/// Add a new scheduled task
#[allow(clippy::too_many_arguments)]
pub async fn add_schedule(
    broker_url: &str,
    name: &str,
    task: &str,
    cron: &str,
    queue: &str,
    args: Option<&str>,
) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!("{}", "=== Add Scheduled Task ===".bold().cyan());
    println!();

    // Validate cron expression (basic validation)
    let cron_parts: Vec<&str> = cron.split_whitespace().collect();
    if cron_parts.len() != 5 {
        println!(
            "{}",
            "✗ Invalid cron expression. Expected format: 'min hour day month weekday'"
                .red()
                .bold()
        );
        println!();
        println!("Examples:");
        println!("  0 0 * * *       - Daily at midnight");
        println!("  0 */2 * * *     - Every 2 hours");
        println!("  */15 * * * *    - Every 15 minutes");
        println!("  0 9 * * 1-5     - Weekdays at 9 AM");
        return Ok(());
    }

    // Check if schedule already exists
    let schedule_key = format!("celers:schedule:{name}");
    let exists: bool = redis::cmd("EXISTS")
        .arg(&schedule_key)
        .query_async(&mut conn)
        .await?;

    if exists {
        println!(
            "{}",
            format!("✗ Schedule '{name}' already exists").red().bold()
        );
        println!();
        println!("Use a different name or remove the existing schedule first:");
        println!("  celers schedule remove {name} --confirm");
        return Ok(());
    }

    // Parse args if provided
    let args_value = if let Some(args_str) = args {
        match serde_json::from_str::<serde_json::Value>(args_str) {
            Ok(v) => v,
            Err(e) => {
                println!("{}", "✗ Invalid JSON arguments".red().bold());
                println!("  Error: {e}");
                return Ok(());
            }
        }
    } else {
        serde_json::json!({})
    };

    // Create schedule data
    let schedule_data = serde_json::json!({
        "name": name,
        "task": task,
        "cron": cron,
        "queue": queue,
        "args": args_value,
        "created_at": chrono::Utc::now().to_rfc3339(),
        "last_run": null,
    });

    // Save schedule
    let schedule_json = serde_json::to_string(&schedule_data)?;
    let _: () = redis::cmd("SET")
        .arg(&schedule_key)
        .arg(&schedule_json)
        .query_async(&mut conn)
        .await?;

    println!("{}", "✓ Schedule added successfully".green().bold());
    println!();
    println!("  {} {}", "Name:".cyan(), name);
    println!("  {} {}", "Task:".cyan(), task);
    println!("  {} {}", "Cron:".cyan(), cron);
    println!("  {} {}", "Queue:".cyan(), queue);
    if let Some(args) = args {
        println!("  {} {}", "Args:".cyan(), args);
    }
    println!();
    println!("{}", "Note:".yellow().bold());
    println!("  • Ensure a beat scheduler is running to execute this schedule");
    println!("  • The schedule is active and will run at the specified times");

    Ok(())
}

/// Remove a scheduled task
pub async fn remove_schedule(broker_url: &str, name: &str, confirm: bool) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let schedule_key = format!("celers:schedule:{name}");

    // Check if schedule exists
    let exists: bool = redis::cmd("EXISTS")
        .arg(&schedule_key)
        .query_async(&mut conn)
        .await?;

    if !exists {
        println!("{}", format!("✗ Schedule '{name}' not found").red());
        return Ok(());
    }

    if !confirm {
        println!(
            "{}",
            format!("⚠ Warning: This will delete schedule '{name}'")
                .yellow()
                .bold()
        );
        println!("   Add --confirm to proceed");
        return Ok(());
    }

    // Remove schedule and its pause flag
    let pause_key = format!("celers:schedule:{name}:paused");
    let _: () = redis::cmd("DEL")
        .arg(&schedule_key)
        .arg(&pause_key)
        .query_async(&mut conn)
        .await?;

    println!(
        "{}",
        format!("✓ Schedule '{name}' removed successfully")
            .green()
            .bold()
    );

    Ok(())
}

/// Pause a schedule
pub async fn pause_schedule(broker_url: &str, name: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let schedule_key = format!("celers:schedule:{name}");

    // Check if schedule exists
    let exists: bool = redis::cmd("EXISTS")
        .arg(&schedule_key)
        .query_async(&mut conn)
        .await?;

    if !exists {
        println!("{}", format!("✗ Schedule '{name}' not found").red());
        return Ok(());
    }

    // Set pause flag
    let pause_key = format!("celers:schedule:{name}:paused");
    let timestamp = chrono::Utc::now().to_rfc3339();
    let _: () = redis::cmd("SET")
        .arg(&pause_key)
        .arg(&timestamp)
        .query_async(&mut conn)
        .await?;

    println!(
        "{}",
        format!("✓ Schedule '{name}' has been paused")
            .green()
            .bold()
    );
    println!();
    println!("{}", "Note:".yellow().bold());
    println!("  • Schedule will not execute until resumed");
    println!("  • Use 'celers schedule resume {name}' to resume");
    println!();
    println!("  Paused at: {}", timestamp.cyan());

    Ok(())
}

/// Resume a paused schedule
pub async fn resume_schedule(broker_url: &str, name: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let schedule_key = format!("celers:schedule:{name}");

    // Check if schedule exists
    let exists: bool = redis::cmd("EXISTS")
        .arg(&schedule_key)
        .query_async(&mut conn)
        .await?;

    if !exists {
        println!("{}", format!("✗ Schedule '{name}' not found").red());
        return Ok(());
    }

    // Check if paused
    let pause_key = format!("celers:schedule:{name}:paused");
    let paused: Option<String> = redis::cmd("GET")
        .arg(&pause_key)
        .query_async(&mut conn)
        .await?;

    if paused.is_none() {
        println!("{}", format!("✓ Schedule '{name}' is not paused").yellow());
        return Ok(());
    }

    // Remove pause flag
    let _: () = redis::cmd("DEL")
        .arg(&pause_key)
        .query_async(&mut conn)
        .await?;

    println!(
        "{}",
        format!("✓ Schedule '{name}' has been resumed")
            .green()
            .bold()
    );
    println!();
    println!("{}", "Note:".yellow().bold());
    println!("  • Schedule will now execute at the specified times");
    if let Some(paused_at) = paused {
        println!("  • Was paused at: {}", paused_at.dimmed());
    }

    Ok(())
}

/// Manually trigger a scheduled task
pub async fn trigger_schedule(broker_url: &str, name: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let schedule_key = format!("celers:schedule:{name}");

    println!(
        "{}",
        format!("=== Trigger Schedule: {name} ===").bold().cyan()
    );
    println!();

    // Get schedule data
    let schedule_data: Option<String> = redis::cmd("GET")
        .arg(&schedule_key)
        .query_async(&mut conn)
        .await?;

    if let Some(data) = schedule_data {
        if let Ok(schedule) = serde_json::from_str::<serde_json::Value>(&data) {
            let task = schedule
                .get("task")
                .and_then(|v| v.as_str())
                .unwrap_or("N/A");
            let queue = schedule
                .get("queue")
                .and_then(|v| v.as_str())
                .unwrap_or("celers");

            // Publish trigger command via Redis Pub/Sub
            let trigger_channel = format!("celers:schedule:{name}:trigger");
            let subscribers: usize = redis::cmd("PUBLISH")
                .arg(&trigger_channel)
                .arg("TRIGGER")
                .query_async(&mut conn)
                .await?;

            if subscribers > 0 {
                println!(
                    "{}",
                    format!("✓ Trigger signal sent for schedule '{name}'")
                        .green()
                        .bold()
                );
                println!();
                println!("  {} {}", "Task:".cyan(), task);
                println!("  {} {}", "Queue:".cyan(), queue);
                println!();
                println!("The task will be executed immediately by the beat scheduler.");
            } else {
                println!(
                    "{}",
                    "⚠ No beat scheduler subscribed to trigger channel"
                        .yellow()
                        .bold()
                );
                println!();
                println!("The trigger command was sent but no beat scheduler is listening.");
                println!("Ensure a beat scheduler is running to execute scheduled tasks.");
            }
        } else {
            println!("{}", "✗ Invalid schedule data".red());
        }
    } else {
        println!("{}", format!("✗ Schedule '{name}' not found").red());
    }

    Ok(())
}

/// Show execution history for a schedule
pub async fn schedule_history(broker_url: &str, name: &str, limit: usize) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!(
        "{}",
        format!("=== Schedule History: {name} ===").bold().cyan()
    );
    println!();

    // Get history from Redis sorted set
    let history_key = format!("celers:schedule:{name}:history");
    let entries: Vec<String> = redis::cmd("ZREVRANGE")
        .arg(&history_key)
        .arg(0)
        .arg(limit as isize - 1)
        .query_async(&mut conn)
        .await?;

    if entries.is_empty() {
        println!("{}", "No execution history found".yellow());
        println!();
        println!("History is recorded when tasks are triggered by the beat scheduler.");
        return Ok(());
    }

    #[derive(Tabled)]
    struct HistoryEntry {
        #[tabled(rename = "#")]
        index: String,
        #[tabled(rename = "Timestamp")]
        timestamp: String,
        #[tabled(rename = "Status")]
        status: String,
        #[tabled(rename = "Task ID")]
        task_id: String,
    }

    let mut history_entries = Vec::new();
    for (idx, entry) in entries.iter().enumerate() {
        if let Ok(data) = serde_json::from_str::<serde_json::Value>(entry) {
            let timestamp = data
                .get("timestamp")
                .and_then(|v| v.as_str())
                .unwrap_or("N/A")
                .to_string();
            let status = data
                .get("status")
                .and_then(|v| v.as_str())
                .unwrap_or("unknown")
                .to_string();
            let task_id = data
                .get("task_id")
                .and_then(|v| v.as_str())
                .unwrap_or("N/A")
                .to_string();

            history_entries.push(HistoryEntry {
                index: (idx + 1).to_string(),
                timestamp,
                status,
                task_id,
            });
        }
    }

    let entry_count = history_entries.len();
    let table = Table::new(history_entries)
        .with(Style::rounded())
        .to_string();
    println!("{table}");
    println!();
    println!(
        "Showing {} of {} entries",
        entry_count.to_string().yellow(),
        entries.len()
    );

    Ok(())
}
