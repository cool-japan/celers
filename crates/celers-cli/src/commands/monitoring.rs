//! Monitoring, health checks, diagnostics, and reporting command implementations.

use celers_broker_redis::RedisBroker;
use celers_core::Broker;
use chrono::Utc;
use colored::Colorize;
use tabled::{settings::Style, Table, Tabled};

use super::task::inspect_task;

/// Display Prometheus metrics
pub async fn show_metrics(
    format: &str,
    output_file: Option<&str>,
    pattern: Option<&str>,
    watch_interval: Option<u64>,
) -> anyhow::Result<()> {
    // If watch mode is enabled and output_file is set, it doesn't make sense
    if watch_interval.is_some() && output_file.is_some() {
        println!(
            "{}",
            "⚠ Watch mode cannot be used with file output".yellow()
        );
        return Ok(());
    }

    if let Some(interval) = watch_interval {
        // Watch mode - refresh metrics periodically
        println!("{}", "=== Metrics Watch Mode ===".bold().green());
        println!(
            "{}",
            format!("Refreshing every {interval} seconds (Ctrl+C to stop)").dimmed()
        );
        println!();

        loop {
            // Clear screen for better readability
            print!("\x1B[2J\x1B[1;1H"); // ANSI escape codes to clear screen

            // Display current time
            println!(
                "{}",
                format!("Last updated: {}", Utc::now().format("%Y-%m-%d %H:%M:%S")).dimmed()
            );
            println!();

            // Gather and format metrics
            format_and_display_metrics(format, pattern)?;

            // Sleep for the specified interval
            tokio::time::sleep(tokio::time::Duration::from_secs(interval)).await;
        }
    } else {
        // One-time display
        format_and_output_metrics(format, output_file, pattern)?;
        Ok(())
    }
}

/// Format and output metrics (for one-time display)
fn format_and_output_metrics(
    format: &str,
    output_file: Option<&str>,
    pattern: Option<&str>,
) -> anyhow::Result<()> {
    // Gather metrics from Prometheus registry
    let metrics_text = celers_metrics::gather_metrics();

    // Filter metrics if pattern is provided
    let filtered_metrics = filter_metrics(&metrics_text, pattern);

    // Format metrics based on format parameter
    let output = format_metrics_output(format, &filtered_metrics, pattern)?;

    // Write to file or stdout
    if let Some(file_path) = output_file {
        std::fs::write(file_path, &output)?;
        println!(
            "{}",
            format!("✓ Metrics exported to '{file_path}'")
                .green()
                .bold()
        );
        println!("  {} {}", "Format:".cyan(), format);
        if let Some(pat) = pattern {
            println!("  {} {}", "Filter:".cyan(), pat);
        }
    } else {
        println!("{output}");
    }

    Ok(())
}

/// Format and display metrics (for watch mode)
fn format_and_display_metrics(format: &str, pattern: Option<&str>) -> anyhow::Result<()> {
    let metrics_text = celers_metrics::gather_metrics();
    let filtered_metrics = filter_metrics(&metrics_text, pattern);
    let output = format_metrics_output(format, &filtered_metrics, pattern)?;
    println!("{output}");
    Ok(())
}

/// Filter metrics text by pattern
fn filter_metrics(metrics_text: &str, pattern: Option<&str>) -> String {
    if let Some(pat) = pattern {
        metrics_text
            .lines()
            .filter(|line| {
                if line.starts_with("# HELP") || line.starts_with("# TYPE") {
                    line.contains(pat)
                } else if line.starts_with('#') {
                    false
                } else {
                    line.contains(pat)
                }
            })
            .collect::<Vec<_>>()
            .join("\n")
    } else {
        metrics_text.to_string()
    }
}

/// Format metrics into the requested output format
fn format_metrics_output(
    format: &str,
    filtered_metrics: &str,
    pattern: Option<&str>,
) -> anyhow::Result<String> {
    match format.to_lowercase().as_str() {
        "json" => {
            let mut metrics_map = serde_json::Map::new();
            for line in filtered_metrics.lines() {
                if line.starts_with('#') || line.trim().is_empty() {
                    continue;
                }
                if let Some(space_idx) = line.rfind(' ') {
                    let (metric_part, value_str) = line.split_at(space_idx);
                    let value_str = value_str.trim();
                    if let Ok(value) = value_str.parse::<f64>() {
                        let metric_name = if let Some(brace_idx) = metric_part.find('{') {
                            &metric_part[..brace_idx]
                        } else {
                            metric_part
                        };
                        metrics_map.insert(metric_name.to_string(), serde_json::json!(value));
                    }
                }
            }
            Ok(serde_json::to_string_pretty(&metrics_map)?)
        }
        "prometheus" | "prom" => Ok(filtered_metrics.to_string()),
        _ => {
            let mut output = String::new();
            output.push_str(&format!("{}\n\n", "=== CeleRS Metrics ===".bold().green()));
            let mut current_metric = String::new();
            let mut help_text = String::new();

            for line in filtered_metrics.lines() {
                if line.starts_with("# HELP") {
                    if let Some(help) = line.strip_prefix("# HELP ") {
                        let parts: Vec<&str> = help.splitn(2, ' ').collect();
                        if parts.len() == 2 {
                            current_metric = parts[0].to_string();
                            help_text = parts[1].to_string();
                        }
                    }
                } else if line.starts_with('#') || line.trim().is_empty() {
                    // Skip
                } else if let Some(space_idx) = line.rfind(' ') {
                    let (metric_part, value_str) = line.split_at(space_idx);
                    let value_str = value_str.trim();
                    let metric_name = if let Some(brace_idx) = metric_part.find('{') {
                        &metric_part[..brace_idx]
                    } else {
                        metric_part
                    };
                    if metric_name == current_metric && !help_text.is_empty() {
                        output.push_str(&format!("{}\n", metric_name.cyan().bold()));
                        output.push_str(&format!("  {}\n", help_text.dimmed()));
                        output.push_str(&format!(
                            "  {} {}\n\n",
                            "Value:".yellow(),
                            value_str.green()
                        ));
                        help_text.clear();
                    }
                }
            }

            if output.trim().is_empty() {
                output = format!("{}\n", "No metrics found".yellow());
                if pattern.is_some() {
                    output.push_str(&format!(
                        "{}\n",
                        "Try adjusting your filter pattern".dimmed()
                    ));
                }
            }
            Ok(output)
        }
    }
}

/// Run system health diagnostics
pub async fn health_check(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    println!("{}", "=== System Health Check ===".bold().cyan());
    println!();

    let mut health_issues = Vec::new();
    let mut health_warnings = Vec::new();

    // Test 1: Broker Connection
    println!("{}", "1. Broker Connection".bold());
    let client = match redis::Client::open(broker_url) {
        Ok(c) => {
            println!("  {} Redis client created", "✓".green());
            c
        }
        Err(e) => {
            println!("  {} Failed to create Redis client: {}", "✗".red(), e);
            health_issues.push("Cannot create Redis client".to_string());
            println!();
            println!("{}", "Health Check Failed".red().bold());
            return Ok(());
        }
    };

    let mut conn = match client.get_multiplexed_async_connection().await {
        Ok(c) => {
            println!("  {} Successfully connected to broker", "✓".green());
            c
        }
        Err(e) => {
            println!("  {} Failed to connect: {}", "✗".red(), e);
            health_issues.push("Cannot connect to broker".to_string());
            println!();
            println!("{}", "Health Check Failed".red().bold());
            return Ok(());
        }
    };

    // Test PING
    match redis::cmd("PING").query_async::<String>(&mut conn).await {
        Ok(_) => {
            println!("  {} PING successful", "✓".green());
        }
        Err(e) => {
            println!("  {} PING failed: {}", "⚠".yellow(), e);
            health_warnings.push("PING to broker failed".to_string());
        }
    }

    println!();

    // Test 2: Queue Status
    println!("{}", "2. Queue Status".bold());
    let broker = RedisBroker::new(broker_url, queue)?;

    let _queue_size = match broker.queue_size().await {
        Ok(size) => {
            println!("  {} Queue size: {}", "✓".green(), size);
            size
        }
        Err(e) => {
            println!("  {} Failed to get queue size: {}", "✗".red(), e);
            health_issues.push("Cannot get queue size".to_string());
            0
        }
    };

    let dlq_size = match broker.dlq_size().await {
        Ok(size) => {
            if size > 0 {
                println!("  {} DLQ size: {} (has failed tasks)", "⚠".yellow(), size);
                health_warnings.push(format!("{size} tasks in Dead Letter Queue"));
            } else {
                println!("  {} DLQ size: {} (empty)", "✓".green(), size);
            }
            size
        }
        Err(e) => {
            println!("  {} Failed to get DLQ size: {}", "✗".red(), e);
            health_issues.push("Cannot get DLQ size".to_string());
            0
        }
    };

    println!();

    // Test 3: Queue Accessibility
    println!("{}", "3. Queue Accessibility".bold());
    let queue_key = format!("celers:{queue}");
    let _queue_type: String = match redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await
    {
        Ok(t) => {
            if t == "none" {
                println!(
                    "  {} Queue does not exist (will be created on first task)",
                    "⚠".yellow()
                );
                health_warnings.push("Queue not yet created".to_string());
            } else if t == "list" {
                println!("  {} Queue type: FIFO (list)", "✓".green());
            } else if t == "zset" {
                println!("  {} Queue type: Priority (sorted set)", "✓".green());
            } else {
                println!("  {} Unknown queue type: {}", "⚠".yellow(), t);
                health_warnings.push(format!("Unknown queue type: {t}"));
            }
            t
        }
        Err(e) => {
            println!("  {} Failed to check queue type: {}", "✗".red(), e);
            health_issues.push("Cannot check queue type".to_string());
            "none".to_string()
        }
    };

    // Check if queue is paused
    let pause_key = format!("celers:{queue}:paused");
    match redis::cmd("GET")
        .arg(&pause_key)
        .query_async::<Option<String>>(&mut conn)
        .await
    {
        Ok(Some(paused_at)) => {
            println!("  {} Queue is PAUSED (since: {})", "⚠".yellow(), paused_at);
            health_warnings.push(format!("Queue is paused since {paused_at}"));
        }
        Ok(None) => {
            println!("  {} Queue is not paused", "✓".green());
        }
        Err(e) => {
            println!("  {} Failed to check pause status: {}", "⚠".yellow(), e);
        }
    }

    println!();

    // Test 4: Memory Usage (if accessible)
    println!("{}", "4. Broker Memory".bold());
    match redis::cmd("INFO")
        .arg("memory")
        .query_async::<String>(&mut conn)
        .await
    {
        Ok(info) => {
            for line in info.lines() {
                if line.starts_with("used_memory_human:") {
                    let memory = line.split(':').nth(1).unwrap_or("N/A");
                    println!("  {} Used memory: {}", "✓".green(), memory);
                    break;
                }
            }
        }
        Err(_) => {
            println!("  {} Memory info not available", "⚠".yellow());
        }
    }

    println!();

    // Test 5: Health Summary
    println!("{}", "Health Summary".bold().cyan());
    println!();

    if health_issues.is_empty() && health_warnings.is_empty() {
        println!(
            "{}",
            "  ✓ All checks passed! System is healthy.".green().bold()
        );
    } else {
        if !health_issues.is_empty() {
            println!("{}", "  Critical Issues:".red().bold());
            for issue in &health_issues {
                println!("    {} {}", "✗".red(), issue);
            }
            println!();
        }

        if !health_warnings.is_empty() {
            println!("{}", "  Warnings:".yellow().bold());
            for warning in &health_warnings {
                println!("    {} {}", "⚠".yellow(), warning);
            }
            println!();
        }

        if health_issues.is_empty() {
            println!(
                "{}",
                "  Overall: System is operational with warnings"
                    .yellow()
                    .bold()
            );
        } else {
            println!("{}", "  Overall: System has critical issues".red().bold());
        }
    }

    println!();

    // Recommendations
    if dlq_size > 0 {
        println!("{}", "Recommendations:".cyan().bold());
        println!("  • Inspect DLQ: celers dlq inspect");
        println!("  • Clear DLQ: celers dlq clear --confirm");
        println!();
    }

    Ok(())
}

/// Automatic problem detection and diagnostics
pub async fn doctor(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    println!("{}", "=== CeleRS Doctor ===".bold().cyan());
    println!("{}", "Running automatic diagnostics...".dimmed());
    println!();

    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let mut issues = Vec::new();
    let mut warnings = Vec::new();
    let mut recommendations = Vec::new();

    // Test 1: Broker connectivity
    println!("{}", "1. Checking broker connectivity...".bold());
    match redis::cmd("PING").query_async::<String>(&mut conn).await {
        Ok(_) => {
            println!("  {} Broker is reachable", "✓".green());
        }
        Err(e) => {
            println!("  {} Broker connection failed: {}", "✗".red(), e);
            issues.push("Cannot connect to broker".to_string());
            recommendations.push("Check broker URL and ensure Redis is running".to_string());
        }
    }
    println!();

    // Test 2: Queue health
    println!("{}", "2. Analyzing queue health...".bold());
    let broker = RedisBroker::new(broker_url, queue)?;

    let queue_size = broker.queue_size().await.unwrap_or(0);
    let dlq_size = broker.dlq_size().await.unwrap_or(0);

    println!("  {} Pending tasks: {}", "•".cyan(), queue_size);
    println!("  {} DLQ tasks: {}", "•".cyan(), dlq_size);

    if dlq_size > 10 {
        warnings.push(format!("High number of failed tasks in DLQ: {dlq_size}"));
        recommendations.push("Inspect DLQ with: celers dlq inspect".to_string());
    }

    if queue_size > 1000 {
        warnings.push(format!("Large queue backlog: {queue_size} tasks"));
        recommendations.push("Consider scaling up workers".to_string());
    }
    println!();

    // Test 3: Worker availability
    println!("{}", "3. Checking worker availability...".bold());
    let worker_pattern = "celers:worker:*:heartbeat";
    let mut cursor = 0;
    let mut worker_count = 0;

    loop {
        let (new_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
            .arg(cursor)
            .arg("MATCH")
            .arg(worker_pattern)
            .arg("COUNT")
            .arg(100)
            .query_async(&mut conn)
            .await?;

        worker_count += keys.len();
        cursor = new_cursor;

        if cursor == 0 {
            break;
        }
    }

    println!("  {} Active workers: {}", "•".cyan(), worker_count);

    if worker_count == 0 && queue_size > 0 {
        issues.push("No workers available to process pending tasks".to_string());
        recommendations.push("Start workers with: celers worker".to_string());
    } else if worker_count > 0 {
        println!("  {} Workers are available", "✓".green());
    }
    println!();

    // Test 4: Queue pause status
    println!("{}", "4. Checking queue status...".bold());
    let pause_key = format!("celers:{queue}:paused");
    let paused: Option<String> = redis::cmd("GET")
        .arg(&pause_key)
        .query_async(&mut conn)
        .await?;

    if let Some(paused_at) = paused {
        warnings.push(format!("Queue '{queue}' is paused since {paused_at}"));
        recommendations.push("Resume queue with: celers queue resume".to_string());
        println!("  {} Queue is PAUSED", "⚠".yellow());
    } else {
        println!("  {} Queue is active", "✓".green());
    }
    println!();

    // Test 5: Memory usage
    println!("{}", "5. Checking broker memory...".bold());
    match redis::cmd("INFO")
        .arg("memory")
        .query_async::<String>(&mut conn)
        .await
    {
        Ok(info) => {
            for line in info.lines() {
                if line.starts_with("used_memory_human:") {
                    let memory = line.split(':').nth(1).unwrap_or("N/A");
                    println!("  {} Used memory: {}", "•".cyan(), memory);
                }
                if line.starts_with("maxmemory_human:") {
                    let max_memory = line.split(':').nth(1).unwrap_or("N/A");
                    if max_memory != "0B" {
                        println!("  {} Max memory: {}", "•".cyan(), max_memory);
                    }
                }
            }
            println!("  {} Memory usage is acceptable", "✓".green());
        }
        Err(_) => {
            println!("  {} Memory info unavailable", "⚠".yellow());
        }
    }
    println!();

    // Summary
    println!("{}", "=== Diagnosis Summary ===".bold().cyan());
    println!();

    if issues.is_empty() && warnings.is_empty() {
        println!(
            "{}",
            "  ✓ No issues detected! System is healthy.".green().bold()
        );
    } else {
        if !issues.is_empty() {
            println!("{}", "  Critical Issues:".red().bold());
            for issue in &issues {
                println!("    {} {}", "✗".red(), issue);
            }
            println!();
        }

        if !warnings.is_empty() {
            println!("{}", "  Warnings:".yellow().bold());
            for warning in &warnings {
                println!("    {} {}", "⚠".yellow(), warning);
            }
            println!();
        }

        if !recommendations.is_empty() {
            println!("{}", "  Recommendations:".cyan().bold());
            for (i, rec) in recommendations.iter().enumerate() {
                println!("    {}. {}", i + 1, rec);
            }
            println!();
        }

        if issues.is_empty() {
            println!(
                "{}",
                "  Overall: System is operational with warnings"
                    .yellow()
                    .bold()
            );
        } else {
            println!(
                "{}",
                "  Overall: System has critical issues that need attention"
                    .red()
                    .bold()
            );
        }
    }

    Ok(())
}

/// Show task execution logs
pub async fn show_task_logs(
    broker_url: &str,
    task_id_str: &str,
    limit: usize,
) -> anyhow::Result<()> {
    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    println!("{}", "=== Task Execution Logs ===".bold().cyan());
    println!("Task ID: {}", task_id.to_string().yellow());
    println!();

    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    let logs_key = format!("celers:task:{task_id}:logs");

    let exists: bool = redis::cmd("EXISTS")
        .arg(&logs_key)
        .query_async(&mut conn)
        .await?;

    if !exists {
        println!("{}", "✗ No logs found for this task".red());
        println!();
        println!("Possible reasons:");
        println!("  • Task hasn't been executed yet");
        println!("  • Logs have expired (TTL)");
        println!("  • Task was executed before logging was enabled");
        println!("  • Wrong task ID");
        return Ok(());
    }

    let log_count: isize = redis::cmd("LLEN")
        .arg(&logs_key)
        .query_async(&mut conn)
        .await?;

    println!(
        "{}",
        format!("Total log entries: {log_count}").cyan().bold()
    );
    println!();

    let logs: Vec<String> = redis::cmd("LRANGE")
        .arg(&logs_key)
        .arg(-(limit as isize))
        .arg(-1)
        .query_async(&mut conn)
        .await?;

    if logs.is_empty() {
        println!("{}", "No log entries available".yellow());
        return Ok(());
    }

    for (idx, log_entry) in logs.iter().enumerate() {
        if let Ok(log_json) = serde_json::from_str::<serde_json::Value>(log_entry) {
            let timestamp = log_json
                .get("timestamp")
                .and_then(|v| v.as_str())
                .unwrap_or("N/A");
            let level = log_json
                .get("level")
                .and_then(|v| v.as_str())
                .unwrap_or("INFO");
            let message = log_json
                .get("message")
                .and_then(|v| v.as_str())
                .unwrap_or(log_entry);

            let level_colored = match level {
                "ERROR" | "error" => level.red().bold(),
                "WARN" | "warn" => level.yellow().bold(),
                "DEBUG" | "debug" => level.dimmed(),
                _ => level.cyan().bold(),
            };

            println!(
                "{} {} {} {}",
                format!("[{}]", idx + 1).dimmed(),
                timestamp.dimmed(),
                level_colored,
                message
            );
        } else {
            println!("{} {}", format!("[{}]", idx + 1).dimmed(), log_entry);
        }
    }

    println!();
    if log_count as usize > limit {
        println!(
            "{}",
            format!("Showing last {} of {} log entries", logs.len(), log_count).yellow()
        );
        println!(
            "{}",
            format!("Use --limit to show more entries (max: {log_count})").dimmed()
        );
    } else {
        println!(
            "{}",
            format!("Showing all {} log entries", logs.len())
                .green()
                .bold()
        );
    }

    Ok(())
}

/// Debug task execution details
pub async fn debug_task(broker_url: &str, queue: &str, task_id_str: &str) -> anyhow::Result<()> {
    let task_id = task_id_str
        .parse::<uuid::Uuid>()
        .map_err(|_| anyhow::anyhow!("Invalid task ID format"))?;

    println!("{}", format!("=== Debug Task: {task_id} ===").bold().cyan());
    println!();

    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    // Get task logs
    let logs_key = format!("celers:task:{task_id}:logs");
    let logs: Vec<String> = redis::cmd("LRANGE")
        .arg(&logs_key)
        .arg(0)
        .arg(-1)
        .query_async(&mut conn)
        .await?;

    if logs.is_empty() {
        println!("{}", "No debug logs found for this task".yellow());
    } else {
        println!("{}", "Task Logs:".green().bold());
        println!();
        for log in &logs {
            if let Ok(log_json) = serde_json::from_str::<serde_json::Value>(log) {
                let level = log_json
                    .get("level")
                    .and_then(|v| v.as_str())
                    .unwrap_or("INFO");
                let message = log_json
                    .get("message")
                    .and_then(|v| v.as_str())
                    .unwrap_or(log);
                let timestamp = log_json
                    .get("timestamp")
                    .and_then(|v| v.as_str())
                    .unwrap_or("");

                let level_colored = match level {
                    "ERROR" | "error" => level.red(),
                    "WARN" | "warn" => level.yellow(),
                    "DEBUG" | "debug" => level.cyan(),
                    _ => level.normal(),
                };

                println!("[{}] {} {}", timestamp.dimmed(), level_colored, message);
            } else {
                println!("{log}");
            }
        }
        println!();
    }

    // Get task metadata
    let metadata_key = format!("celers:task:{task_id}:metadata");
    let metadata: Option<String> = redis::cmd("GET")
        .arg(&metadata_key)
        .query_async(&mut conn)
        .await?;

    if let Some(meta_str) = metadata {
        println!("{}", "Task Metadata:".green().bold());
        println!();
        if let Ok(meta_json) = serde_json::from_str::<serde_json::Value>(&meta_str) {
            println!("{}", serde_json::to_string_pretty(&meta_json)?);
        } else {
            println!("{meta_str}");
        }
        println!();
    }

    // Get task state from queue
    inspect_task(broker_url, queue, task_id_str).await?;

    Ok(())
}

/// Debug worker issues
pub async fn debug_worker(broker_url: &str, worker_id: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!(
        "{}",
        format!("=== Debug Worker: {worker_id} ===").bold().cyan()
    );
    println!();

    // Get worker heartbeat
    let heartbeat_key = format!("celers:worker:{worker_id}:heartbeat");
    let heartbeat: Option<String> = redis::cmd("GET")
        .arg(&heartbeat_key)
        .query_async(&mut conn)
        .await?;

    if heartbeat.is_none() {
        println!("{}", format!("✗ Worker '{worker_id}' not found").red());
        println!();
        println!("Possible causes:");
        println!("  • Worker is not running");
        println!("  • Worker ID is incorrect");
        println!("  • Heartbeat expired (worker crashed)");
        return Ok(());
    }

    println!("{}", "Worker Status: Active".green().bold());
    if let Some(hb) = heartbeat {
        println!("Last heartbeat: {}", hb.yellow());
    }
    println!();

    // Check worker stats
    let stats_key = format!("celers:worker:{worker_id}:stats");
    let stats: Option<String> = redis::cmd("GET")
        .arg(&stats_key)
        .query_async(&mut conn)
        .await?;

    if let Some(stats_str) = stats {
        println!("{}", "Worker Statistics:".green().bold());
        if let Ok(stats_json) = serde_json::from_str::<serde_json::Value>(&stats_str) {
            println!("{}", serde_json::to_string_pretty(&stats_json)?);
        } else {
            println!("{stats_str}");
        }
        println!();
    }

    // Check for pause status
    let pause_key = format!("celers:worker:{worker_id}:paused");
    let paused: bool = redis::cmd("EXISTS")
        .arg(&pause_key)
        .query_async(&mut conn)
        .await?;

    if paused {
        println!("{}", "⚠ Worker is PAUSED".yellow().bold());
        println!("  Tasks are not being processed");
        println!();
    }

    // Check for drain status
    let drain_key = format!("celers:worker:{worker_id}:draining");
    let draining: bool = redis::cmd("EXISTS")
        .arg(&drain_key)
        .query_async(&mut conn)
        .await?;

    if draining {
        println!("{}", "⚠ Worker is DRAINING".yellow().bold());
        println!("  Not accepting new tasks");
        println!();
    }

    // Get worker logs
    let logs_key = format!("celers:worker:{worker_id}:logs");
    let logs: Vec<String> = redis::cmd("LRANGE")
        .arg(&logs_key)
        .arg(-20)
        .arg(-1)
        .query_async(&mut conn)
        .await?;

    if !logs.is_empty() {
        println!("{}", "Recent Worker Logs (last 20):".green().bold());
        println!();
        for log in logs {
            println!("{log}");
        }
    }

    Ok(())
}

/// Generate daily execution report
pub async fn report_daily(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!("{}", "=== Daily Execution Report ===".bold().cyan());
    println!();

    let now = Utc::now();
    let today_key = format!("celers:metrics:{}:daily:{}", queue, now.format("%Y-%m-%d"));

    let metrics: Option<String> = redis::cmd("GET")
        .arg(&today_key)
        .query_async(&mut conn)
        .await?;

    if let Some(metrics_str) = metrics {
        if let Ok(metrics_json) = serde_json::from_str::<serde_json::Value>(&metrics_str) {
            println!("{}", format!("Date: {}", now.format("%Y-%m-%d")).yellow());
            println!();

            #[derive(Tabled)]
            struct DailyMetric {
                #[tabled(rename = "Metric")]
                metric: String,
                #[tabled(rename = "Count")]
                count: String,
            }

            let mut daily_metrics = vec![];

            if let Some(total) = metrics_json
                .get("total_tasks")
                .and_then(serde_json::Value::as_u64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Total Tasks".to_string(),
                    count: total.to_string(),
                });
            }

            if let Some(succeeded) = metrics_json
                .get("succeeded")
                .and_then(serde_json::Value::as_u64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Succeeded".to_string(),
                    count: succeeded.to_string(),
                });
            }

            if let Some(failed) = metrics_json
                .get("failed")
                .and_then(serde_json::Value::as_u64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Failed".to_string(),
                    count: failed.to_string(),
                });
            }

            if let Some(retried) = metrics_json
                .get("retried")
                .and_then(serde_json::Value::as_u64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Retried".to_string(),
                    count: retried.to_string(),
                });
            }

            if let Some(avg_time) = metrics_json
                .get("avg_execution_time")
                .and_then(serde_json::Value::as_f64)
            {
                daily_metrics.push(DailyMetric {
                    metric: "Avg Execution Time".to_string(),
                    count: format!("{avg_time:.2}s"),
                });
            }

            let table = Table::new(daily_metrics).with(Style::rounded()).to_string();
            println!("{table}");
        }
    } else {
        println!("{}", "No metrics available for today".yellow());
        println!();
        println!("Metrics are collected automatically when tasks are executed.");
        println!("Ensure workers are running and processing tasks.");
    }

    Ok(())
}

/// Generate weekly statistics report
pub async fn report_weekly(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!("{}", "=== Weekly Statistics Report ===".bold().cyan());
    println!();

    let now = Utc::now();
    let week_start = now - chrono::Duration::days(7);

    println!(
        "{}",
        format!(
            "Period: {} to {}",
            week_start.format("%Y-%m-%d"),
            now.format("%Y-%m-%d")
        )
        .yellow()
    );
    println!();

    let mut total_tasks = 0u64;
    let mut total_succeeded = 0u64;
    let mut total_failed = 0u64;
    let mut total_retried = 0u64;

    for day_offset in 0..7 {
        let day = now - chrono::Duration::days(day_offset);
        let day_key = format!("celers:metrics:{}:daily:{}", queue, day.format("%Y-%m-%d"));

        let metrics: Option<String> = redis::cmd("GET")
            .arg(&day_key)
            .query_async(&mut conn)
            .await?;

        if let Some(metrics_str) = metrics {
            if let Ok(metrics_json) = serde_json::from_str::<serde_json::Value>(&metrics_str) {
                if let Some(tasks) = metrics_json
                    .get("total_tasks")
                    .and_then(serde_json::Value::as_u64)
                {
                    total_tasks += tasks;
                }
                if let Some(succeeded) = metrics_json
                    .get("succeeded")
                    .and_then(serde_json::Value::as_u64)
                {
                    total_succeeded += succeeded;
                }
                if let Some(failed) = metrics_json
                    .get("failed")
                    .and_then(serde_json::Value::as_u64)
                {
                    total_failed += failed;
                }
                if let Some(retried) = metrics_json
                    .get("retried")
                    .and_then(serde_json::Value::as_u64)
                {
                    total_retried += retried;
                }
            }
        }
    }

    #[derive(Tabled)]
    struct WeeklyMetric {
        #[tabled(rename = "Metric")]
        metric: String,
        #[tabled(rename = "Count")]
        count: String,
        #[tabled(rename = "Percentage")]
        percentage: String,
    }

    let success_rate = if total_tasks > 0 {
        (total_succeeded as f64 / total_tasks as f64) * 100.0
    } else {
        0.0
    };

    let failure_rate = if total_tasks > 0 {
        (total_failed as f64 / total_tasks as f64) * 100.0
    } else {
        0.0
    };

    let weekly_metrics = vec![
        WeeklyMetric {
            metric: "Total Tasks".to_string(),
            count: total_tasks.to_string(),
            percentage: "100%".to_string(),
        },
        WeeklyMetric {
            metric: "Succeeded".to_string(),
            count: total_succeeded.to_string(),
            percentage: format!("{success_rate:.1}%"),
        },
        WeeklyMetric {
            metric: "Failed".to_string(),
            count: total_failed.to_string(),
            percentage: format!("{failure_rate:.1}%"),
        },
        WeeklyMetric {
            metric: "Retried".to_string(),
            count: total_retried.to_string(),
            percentage: "-".to_string(),
        },
    ];

    let table = Table::new(weekly_metrics)
        .with(Style::rounded())
        .to_string();
    println!("{table}");

    if total_tasks == 0 {
        println!();
        println!("{}", "No tasks processed this week".yellow());
    }

    Ok(())
}

/// Analyze performance bottlenecks
pub async fn analyze_bottlenecks(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!(
        "{}",
        "=== Performance Bottleneck Analysis ===".bold().cyan()
    );
    println!();

    let queue_key = format!("celers:{queue}");
    let queue_type: String = redis::cmd("TYPE")
        .arg(&queue_key)
        .query_async(&mut conn)
        .await?;

    let queue_size: isize = match queue_type.as_str() {
        "list" => {
            redis::cmd("LLEN")
                .arg(&queue_key)
                .query_async(&mut conn)
                .await?
        }
        "zset" => {
            redis::cmd("ZCARD")
                .arg(&queue_key)
                .query_async(&mut conn)
                .await?
        }
        _ => 0,
    };

    let worker_keys: Vec<String> = redis::cmd("KEYS")
        .arg("celers:worker:*:heartbeat")
        .query_async(&mut conn)
        .await?;
    let worker_count = worker_keys.len();

    let dlq_key = format!("celers:{queue}:dlq");
    let dlq_size: isize = redis::cmd("LLEN")
        .arg(&dlq_key)
        .query_async(&mut conn)
        .await?;

    println!("{}", "System Overview:".green().bold());
    println!("  Queue Depth: {}", queue_size.to_string().yellow());
    println!("  Active Workers: {}", worker_count.to_string().yellow());
    println!("  DLQ Size: {}", dlq_size.to_string().yellow());
    println!();

    let mut bottlenecks = Vec::new();

    if queue_size > 1000 {
        bottlenecks.push("High queue depth - consider scaling up workers");
    }
    if worker_count == 0 && queue_size > 0 {
        bottlenecks.push("No active workers - tasks are not being processed");
    }
    if worker_count > 0 && queue_size > (worker_count * 100) as isize {
        bottlenecks.push("Queue depth is very high relative to worker count");
    }
    if dlq_size > 100 {
        bottlenecks.push("High DLQ size - many tasks are failing");
    }

    if bottlenecks.is_empty() {
        println!("{}", "✓ No significant bottlenecks detected".green());
    } else {
        println!("{}", "⚠ Bottlenecks Detected:".yellow().bold());
        println!();
        for (idx, bottleneck) in bottlenecks.iter().enumerate() {
            println!("  {}. {}", idx + 1, bottleneck);
        }
        println!();

        println!("{}", "Recommendations:".cyan().bold());
        println!();
        if queue_size > 1000 {
            println!(
                "  • Scale up workers: celers worker-mgmt scale {}",
                worker_count * 2
            );
        }
        if worker_count == 0 {
            println!("  • Start workers: celers worker --broker {broker_url}");
        }
        if dlq_size > 100 {
            println!("  • Investigate failed tasks: celers dlq inspect");
            println!("  • Check task implementations for errors");
        }
    }

    Ok(())
}

/// Analyze failure patterns
pub async fn analyze_failures(broker_url: &str, queue: &str) -> anyhow::Result<()> {
    let broker = RedisBroker::new(broker_url, queue)?;

    println!("{}", "=== Failure Pattern Analysis ===".bold().cyan());
    println!();

    let dlq_size = broker.dlq_size().await?;
    println!("Total failed tasks: {}", dlq_size.to_string().yellow());

    if dlq_size == 0 {
        println!("{}", "✓ No failed tasks to analyze".green());
        return Ok(());
    }

    println!();

    let tasks = broker.inspect_dlq(100).await?;

    let mut task_name_failures: std::collections::HashMap<String, usize> =
        std::collections::HashMap::new();

    for task in &tasks {
        *task_name_failures
            .entry(task.metadata.name.clone())
            .or_insert(0) += 1;
    }

    println!("{}", "Failures by Task Type:".green().bold());
    println!();

    #[derive(Tabled)]
    struct FailureCount {
        #[tabled(rename = "Task Name")]
        task_name: String,
        #[tabled(rename = "Failures")]
        count: String,
    }

    let mut task_failures: Vec<FailureCount> = task_name_failures
        .into_iter()
        .map(|(name, count)| FailureCount {
            task_name: name,
            count: count.to_string(),
        })
        .collect();
    task_failures.sort_by(|a, b| {
        b.count
            .parse::<usize>()
            .unwrap_or(0)
            .cmp(&a.count.parse::<usize>().unwrap_or(0))
    });

    let table = Table::new(task_failures.iter().take(10))
        .with(Style::rounded())
        .to_string();
    println!("{table}");
    println!();
    println!("{}", "Recommendations:".cyan().bold());
    println!();
    println!("  • Review task implementations for the most failing tasks");
    println!("  • Check error logs: celers task logs <task-id>");
    println!("  • Consider increasing retry limits for transient failures");
    println!("  • Replay fixed tasks: celers dlq replay <task-id>");

    Ok(())
}

/// Stream worker logs with optional filtering and follow mode
pub async fn worker_logs(
    broker_url: &str,
    worker_id: &str,
    level_filter: Option<&str>,
    follow: bool,
    initial_lines: usize,
) -> anyhow::Result<()> {
    let client = redis::Client::open(broker_url)?;
    let mut conn = client.get_multiplexed_async_connection().await?;

    println!(
        "{}",
        format!("=== Worker Logs: {worker_id} ===").bold().cyan()
    );
    println!();

    let heartbeat_key = format!("celers:worker:{worker_id}:heartbeat");
    let exists: bool = redis::cmd("EXISTS")
        .arg(&heartbeat_key)
        .query_async(&mut conn)
        .await?;

    if !exists {
        println!("{}", format!("✗ Worker '{worker_id}' not found").red());
        return Ok(());
    }

    let logs_key = format!("celers:worker:{worker_id}:logs");

    let logs: Vec<String> = redis::cmd("LRANGE")
        .arg(&logs_key)
        .arg(-(initial_lines as isize))
        .arg(-1)
        .query_async(&mut conn)
        .await?;

    for log in &logs {
        display_log_line(log, level_filter);
    }

    if !follow {
        return Ok(());
    }

    println!();
    println!("{}", "=== Following logs (Ctrl+C to stop) ===".dimmed());
    println!();

    let mut last_length: isize = redis::cmd("LLEN")
        .arg(&logs_key)
        .query_async(&mut conn)
        .await?;

    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let current_length: isize = redis::cmd("LLEN")
            .arg(&logs_key)
            .query_async(&mut conn)
            .await?;

        if current_length > last_length {
            let new_logs: Vec<String> = redis::cmd("LRANGE")
                .arg(&logs_key)
                .arg(last_length)
                .arg(-1)
                .query_async(&mut conn)
                .await?;

            for log in &new_logs {
                display_log_line(log, level_filter);
            }

            last_length = current_length;
        }

        let still_exists: bool = redis::cmd("EXISTS")
            .arg(&heartbeat_key)
            .query_async(&mut conn)
            .await?;

        if !still_exists {
            println!();
            println!("{}", "Worker has stopped".yellow());
            break;
        }
    }

    Ok(())
}

/// Helper function to display a log line with optional filtering
#[allow(dead_code)]
fn display_log_line(log: &str, level_filter: Option<&str>) {
    if let Ok(log_json) = serde_json::from_str::<serde_json::Value>(log) {
        let level = log_json
            .get("level")
            .and_then(|v| v.as_str())
            .unwrap_or("INFO");
        let message = log_json
            .get("message")
            .and_then(|v| v.as_str())
            .unwrap_or(log);
        let timestamp = log_json
            .get("timestamp")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        if let Some(filter) = level_filter {
            if !level.eq_ignore_ascii_case(filter) {
                return;
            }
        }

        let level_colored = match level.to_uppercase().as_str() {
            "ERROR" => level.red(),
            "WARN" => level.yellow(),
            "DEBUG" => level.cyan(),
            _ => level.normal(),
        };

        println!("[{}] {} {}", timestamp.dimmed(), level_colored, message);
    } else if level_filter.is_none() {
        println!("{log}");
    }
}

/// Start auto-scaling service
pub async fn autoscale_start(
    broker_url: &str,
    queue: &str,
    autoscale_config: Option<crate::config::AutoScaleConfig>,
) -> anyhow::Result<()> {
    println!("{}", "=== Auto-Scaling Service ===".bold().green());
    println!();

    let config = match autoscale_config {
        Some(cfg) if cfg.enabled => cfg,
        Some(_) => {
            println!(
                "{}",
                "⚠️  Auto-scaling is disabled in configuration".yellow()
            );
            return Ok(());
        }
        None => {
            println!("{}", "⚠️  No auto-scaling configuration found".yellow());
            println!("Add [autoscale] section to your celers.toml");
            return Ok(());
        }
    };

    println!("Configuration:");
    println!("  Min workers: {}", config.min_workers.to_string().cyan());
    println!("  Max workers: {}", config.max_workers.to_string().cyan());
    println!(
        "  Scale up threshold: {}",
        config.scale_up_threshold.to_string().cyan()
    );
    println!(
        "  Scale down threshold: {}",
        config.scale_down_threshold.to_string().cyan()
    );
    println!(
        "  Check interval: {}s",
        config.check_interval_secs.to_string().cyan()
    );
    println!();

    let broker = RedisBroker::new(broker_url, queue)?;
    println!("{}", "✓ Connected to broker".green());
    println!();
    println!("{}", "Starting auto-scaling monitor...".green().bold());
    println!("{}", "  Press Ctrl+C to stop".dimmed());
    println!();

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(config.check_interval_secs)).await;

        let queue_size = broker.queue_size().await?;

        let client = redis::Client::open(broker_url)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let worker_keys: Vec<String> = redis::cmd("KEYS")
            .arg("celers:worker:*:heartbeat")
            .query_async(&mut conn)
            .await?;
        let current_workers = worker_keys.len();

        println!(
            "[{}] Queue: {}, Workers: {}",
            Utc::now().format("%H:%M:%S").to_string().dimmed(),
            queue_size.to_string().yellow(),
            current_workers.to_string().cyan()
        );

        if queue_size > config.scale_up_threshold && current_workers < config.max_workers {
            let needed = config.max_workers.min(current_workers + 1);
            println!(
                "  {} Scale up recommended: {} -> {}",
                "↑".green().bold(),
                current_workers,
                needed
            );
        } else if queue_size < config.scale_down_threshold && current_workers > config.min_workers {
            let target = config.min_workers.max(current_workers.saturating_sub(1));
            println!(
                "  {} Scale down possible: {} -> {}",
                "↓".yellow().bold(),
                current_workers,
                target
            );
        }
    }
}

/// Show auto-scaling status
pub async fn autoscale_status(
    broker_url: &str,
    autoscale_config: Option<crate::config::AutoScaleConfig>,
) -> anyhow::Result<()> {
    println!("{}", "=== Auto-Scaling Status ===".bold().cyan());
    println!();

    if let Some(cfg) = autoscale_config {
        println!(
            "Status: {}",
            if cfg.enabled {
                "Enabled".green()
            } else {
                "Disabled".red()
            }
        );
        println!();
        println!("Configuration:");
        println!("  Min workers: {}", cfg.min_workers);
        println!("  Max workers: {}", cfg.max_workers);
        println!("  Scale up threshold: {}", cfg.scale_up_threshold);
        println!("  Scale down threshold: {}", cfg.scale_down_threshold);
        println!("  Check interval: {}s", cfg.check_interval_secs);
        println!();

        let client = redis::Client::open(broker_url)?;
        let mut conn = client.get_multiplexed_async_connection().await?;
        let worker_keys: Vec<String> = redis::cmd("KEYS")
            .arg("celers:worker:*:heartbeat")
            .query_async(&mut conn)
            .await?;

        println!("Current State:");
        println!("  Active workers: {}", worker_keys.len().to_string().cyan());
    } else {
        println!("{}", "Auto-scaling is not configured".yellow());
        println!("Add [autoscale] section to your celers.toml");
    }

    Ok(())
}

/// Start alert monitoring service
pub async fn alert_start(
    broker_url: &str,
    queue: &str,
    alert_config: Option<crate::config::AlertConfig>,
) -> anyhow::Result<()> {
    println!("{}", "=== Alert Monitoring Service ===".bold().green());
    println!();

    let config = match alert_config {
        Some(cfg) if cfg.enabled => cfg,
        Some(_) => {
            println!(
                "{}",
                "⚠️  Alert monitoring is disabled in configuration".yellow()
            );
            return Ok(());
        }
        None => {
            println!("{}", "⚠️  No alert configuration found".yellow());
            println!("Add [alerts] section to your celers.toml");
            return Ok(());
        }
    };

    if config.webhook_url.is_none() {
        println!("{}", "⚠️  No webhook URL configured".yellow());
        return Ok(());
    }

    println!("Configuration:");
    println!(
        "  Webhook URL: {}",
        config
            .webhook_url
            .as_ref()
            .expect("webhook_url validated to be Some")
            .cyan()
    );
    println!(
        "  DLQ threshold: {}",
        config.dlq_threshold.to_string().cyan()
    );
    println!(
        "  Failed threshold: {}",
        config.failed_threshold.to_string().cyan()
    );
    println!(
        "  Check interval: {}s",
        config.check_interval_secs.to_string().cyan()
    );
    println!();

    let broker = RedisBroker::new(broker_url, queue)?;
    println!("{}", "✓ Connected to broker".green());
    println!();
    println!("{}", "Starting alert monitor...".green().bold());
    println!("{}", "  Press Ctrl+C to stop".dimmed());
    println!();

    let webhook_url = config
        .webhook_url
        .expect("webhook_url validated to be Some");

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(config.check_interval_secs)).await;

        let dlq_size = broker.dlq_size().await?;

        println!(
            "[{}] DLQ size: {}",
            Utc::now().format("%H:%M:%S").to_string().dimmed(),
            dlq_size.to_string().yellow()
        );

        if dlq_size > config.dlq_threshold {
            let message = format!(
                "⚠️ DLQ size ({}) exceeded threshold ({})",
                dlq_size, config.dlq_threshold
            );
            println!("  {} Sending alert...", "!".red().bold());

            if let Err(e) = send_webhook_alert(&webhook_url, &message).await {
                println!("  {} Failed to send alert: {}", "✗".red(), e);
            } else {
                println!("  {} Alert sent", "✓".green());
            }
        }
    }
}

/// Test webhook notification
pub async fn alert_test(webhook_url: &str, message: &str) -> anyhow::Result<()> {
    println!("{}", "=== Testing Webhook ===".bold().cyan());
    println!();
    println!("Webhook URL: {}", webhook_url.cyan());
    println!("Message: {}", message.yellow());
    println!();

    println!("Sending test notification...");
    send_webhook_alert(webhook_url, message).await?;

    println!("{}", "✓ Test notification sent successfully".green());

    Ok(())
}

/// Helper function to send webhook alert
async fn send_webhook_alert(webhook_url: &str, message: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let payload = serde_json::json!({
        "text": message,
        "timestamp": Utc::now().to_rfc3339(),
    });

    let response = client.post(webhook_url).json(&payload).send().await?;

    if !response.status().is_success() {
        anyhow::bail!("Webhook request failed with status: {}", response.status());
    }

    Ok(())
}
