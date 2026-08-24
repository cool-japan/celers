//! Health checks, automatic diagnostics, task/worker debugging, and bottleneck/failure analysis.
//!
//! 🤖 Generated with [SplitRS](https://github.com/cool-japan/splitrs)

use super::super::task::inspect_task;
use super::report::scan_worker_heartbeat_keys;
use celers_broker_redis::RedisBroker;
use celers_core::Broker;
use colored::Colorize;
use tabled::{settings::Style, Table, Tabled};

/// Broker memory usage (as a percentage of `maxmemory`) at or above which
/// `doctor` reports a critical issue rather than a mere warning.
const DOCTOR_MEMORY_CRITICAL_PCT: f64 = 90.0;

/// Broker memory usage (as a percentage of `maxmemory`) at or above which
/// `doctor` reports a warning.
const DOCTOR_MEMORY_WARNING_PCT: f64 = 75.0;

/// Pure: classify a `used_memory` / `maxmemory` (bytes) pair into a
/// doctor verdict.
///
/// Extracted from `doctor`'s memory check so the threshold arithmetic is
/// unit-testable without a broker. `maxmemory == 0` is Redis's convention
/// for "no configured limit" -- there is nothing to verify usage against,
/// which is a distinct outcome from both "healthy" and "critical" and must
/// not be silently reported as either.
#[derive(Debug, Clone, Copy, PartialEq)]
enum MemoryVerdict {
    /// No `maxmemory` ceiling is configured; usage cannot be evaluated
    /// against a limit.
    Unbounded,
    /// Below the warning threshold.
    Acceptable { pct_of_max: f64 },
    /// At or above the warning threshold but below critical.
    Warning { pct_of_max: f64 },
    /// At or above the critical threshold.
    Critical { pct_of_max: f64 },
}

#[must_use]
fn classify_memory_usage(used_memory_bytes: u64, max_memory_bytes: u64) -> MemoryVerdict {
    if max_memory_bytes == 0 {
        return MemoryVerdict::Unbounded;
    }
    let pct_of_max = (used_memory_bytes as f64 / max_memory_bytes as f64) * 100.0;
    if pct_of_max >= DOCTOR_MEMORY_CRITICAL_PCT {
        MemoryVerdict::Critical { pct_of_max }
    } else if pct_of_max >= DOCTOR_MEMORY_WARNING_PCT {
        MemoryVerdict::Warning { pct_of_max }
    } else {
        MemoryVerdict::Acceptable { pct_of_max }
    }
}

/// Pure: parse the `used_memory:`/`maxmemory:` byte counters out of a Redis
/// `INFO memory` payload. Returns `None` for a field that is missing or
/// unparseable rather than guessing.
#[must_use]
fn parse_memory_bytes(info: &str, field: &str) -> Option<u64> {
    let prefix = format!("{field}:");
    info.lines()
        .find_map(|line| line.strip_prefix(prefix.as_str()))
        .and_then(|v| v.trim().parse::<u64>().ok())
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
            return Err(anyhow::anyhow!("health check failed"));
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
            return Err(anyhow::anyhow!("health check failed"));
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

    if !health_issues.is_empty() {
        return Err(anyhow::anyhow!("health check failed"));
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

            // Compute the verdict for real instead of printing a canned
            // "acceptable" after any successful INFO call.
            match (
                parse_memory_bytes(&info, "used_memory"),
                parse_memory_bytes(&info, "maxmemory"),
            ) {
                (Some(used), Some(max)) => match classify_memory_usage(used, max) {
                    MemoryVerdict::Unbounded => {
                        println!(
                            "  {} No maxmemory configured; usage cannot be verified against a limit",
                            "ℹ".cyan()
                        );
                    }
                    MemoryVerdict::Acceptable { pct_of_max } => {
                        println!(
                            "  {} Memory usage is acceptable ({pct_of_max:.1}% of maxmemory)",
                            "✓".green()
                        );
                    }
                    MemoryVerdict::Warning { pct_of_max } => {
                        warnings.push(format!(
                            "Broker memory usage is high: {pct_of_max:.1}% of maxmemory"
                        ));
                        println!(
                            "  {} Memory usage is high ({pct_of_max:.1}% of maxmemory)",
                            "⚠".yellow()
                        );
                    }
                    MemoryVerdict::Critical { pct_of_max } => {
                        issues.push(format!(
                            "Broker memory usage is critical: {pct_of_max:.1}% of maxmemory"
                        ));
                        recommendations
                            .push("Increase maxmemory or evict/trim old data".to_string());
                        println!(
                            "  {} Memory usage is critical ({pct_of_max:.1}% of maxmemory)",
                            "✗".red()
                        );
                    }
                },
                _ => {
                    println!(
                        "  {} Could not parse used_memory/maxmemory from broker INFO output",
                        "⚠".yellow()
                    );
                }
            }
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

    // `doctor` must agree with its sibling `health_check` (which already
    // returns `Err` on critical issues, see above): a command meant to gate
    // CI/monitoring is useless if it always exits 0 regardless of what it
    // found. Warnings alone still exit 0 (matching `health_check`).
    if !issues.is_empty() {
        anyhow::bail!(
            "doctor detected {} critical issue(s): {}",
            issues.len(),
            issues.join("; ")
        );
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

    let worker_keys = scan_worker_heartbeat_keys(&mut conn).await?;
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

    // Content-based tail tracking rather than an absolute `LLEN` index:
    // `celers:worker:{id}:logs` is trimmed in practice (bounded log lists),
    // and once a trim shrinks the list, `current_length` can drop below a
    // remembered `last_length` -- the old index-based check
    // (`current_length > last_length`) then stays false forever (or, after
    // the list grows back past the old length, resumes reading from a
    // now-stale/wrong index). Re-fetching a bounded tail window each poll
    // and diffing it by content (`LogTailState::advance`) is robust to
    // trims in either direction and never requires a worker-side change.
    let mut tail_state = LogTailState::default();
    let seed_window: Vec<String> = redis::cmd("LRANGE")
        .arg(&logs_key)
        .arg(-(FOLLOW_TAIL_WINDOW as isize))
        .arg(-1)
        .query_async(&mut conn)
        .await?;
    // Seed silently: `seed_window` overlaps what the initial dump above
    // already printed, so it must not be re-emitted as "new".
    tail_state.advance(seed_window);

    loop {
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let fresh_window: Vec<String> = redis::cmd("LRANGE")
            .arg(&logs_key)
            .arg(-(FOLLOW_TAIL_WINDOW as isize))
            .arg(-1)
            .query_async(&mut conn)
            .await?;

        for log in tail_state.advance(fresh_window) {
            display_log_line(&log, level_filter);
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

/// Tail window size used to poll `celers:worker:{id}:logs` in follow mode
/// (see [`LogTailState`]). Large enough to give
/// [`LogTailState::advance`] a comfortable overlap margin against a
/// moderate trim between two 500ms polls, small enough to keep each poll
/// cheap.
const FOLLOW_TAIL_WINDOW: usize = 200;

/// Sliding-window tail state for `worker_logs --follow`.
///
/// Rather than trusting a remembered `LLEN` as an absolute read offset
/// (broken by list trims -- see [`worker_logs`]'s doc comment above), each
/// poll re-fetches the list's last `N` entries and this pure diff decides
/// which of them are new since the previous poll, by content rather than
/// by index.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct LogTailState {
    /// The most recently fetched tail window, oldest first (as returned by
    /// `LRANGE logs_key -N -1`).
    last_window: Vec<String>,
}

impl LogTailState {
    /// Given a freshly fetched tail window (oldest first), return the
    /// entries that are new since the previous call, in order, and update
    /// the internal state to `fresh_window`.
    ///
    /// Because both the previous and fresh windows are suffixes of the same
    /// append-mostly list, any entry present in both keeps its relative
    /// order: the longest suffix of the previous window that reappears as a
    /// prefix of the fresh window marks how much "carries over"; everything
    /// in the fresh window after that point is new. If no overlap is found
    /// at all (a burst larger than the window, or the list was cleared),
    /// the entire fresh window is treated as new -- over-printing a few
    /// entries again is far less harmful than the original bug (going
    /// silent forever after a trim).
    fn advance(&mut self, fresh_window: Vec<String>) -> Vec<String> {
        let new_entries = match find_overlap_start(&self.last_window, &fresh_window) {
            Some(overlap_start) => fresh_window[overlap_start..].to_vec(),
            None => fresh_window.clone(),
        };
        self.last_window = fresh_window;
        new_entries
    }
}

/// Pure: find the index in `fresh` at which `previous`'s tail stops
/// overlapping, i.e. the length of the longest suffix of `previous` that
/// equals a prefix of `fresh`. Returns `None` when there is no overlap at
/// all (including when `previous` is empty).
#[must_use]
fn find_overlap_start(previous: &[String], fresh: &[String]) -> Option<usize> {
    let max_overlap = previous.len().min(fresh.len());
    for overlap in (1..=max_overlap).rev() {
        if previous[previous.len() - overlap..] == fresh[..overlap] {
            return Some(overlap);
        }
    }
    None
}

/// Helper function to display a log line with optional filtering
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

#[cfg(test)]
mod diagnostics_tests {
    use super::*;

    // ---- parse_memory_bytes ------------------------------------------------

    #[test]
    fn parse_memory_bytes_extracts_known_fields() {
        let info = "\
# Memory
used_memory:1048576
used_memory_human:1.00M
maxmemory:10485760
maxmemory_human:10.00M
";
        assert_eq!(parse_memory_bytes(info, "used_memory"), Some(1_048_576));
        assert_eq!(parse_memory_bytes(info, "maxmemory"), Some(10_485_760));
    }

    #[test]
    fn parse_memory_bytes_missing_field_is_none() {
        let info = "used_memory:1024\n";
        assert_eq!(parse_memory_bytes(info, "maxmemory"), None);
    }

    #[test]
    fn parse_memory_bytes_does_not_match_human_variant() {
        // `used_memory_human:` must not be mistaken for `used_memory:`.
        let info = "used_memory_human:1.00M\n";
        assert_eq!(parse_memory_bytes(info, "used_memory"), None);
    }

    #[test]
    fn parse_memory_bytes_rejects_unparseable_value() {
        let info = "used_memory:not-a-number\n";
        assert_eq!(parse_memory_bytes(info, "used_memory"), None);
    }

    // ---- classify_memory_usage ----------------------------------------------

    #[test]
    fn classify_memory_zero_maxmemory_is_unbounded() {
        assert_eq!(
            classify_memory_usage(999_999_999, 0),
            MemoryVerdict::Unbounded
        );
    }

    #[test]
    fn classify_memory_low_usage_is_acceptable() {
        let verdict = classify_memory_usage(10, 1000); // 1%
        assert!(matches!(verdict, MemoryVerdict::Acceptable { .. }));
    }

    #[test]
    fn classify_memory_at_warning_threshold() {
        let verdict = classify_memory_usage(750, 1000); // 75%
        assert!(matches!(verdict, MemoryVerdict::Warning { .. }));
    }

    #[test]
    fn classify_memory_just_below_warning_is_acceptable() {
        let verdict = classify_memory_usage(749, 1000); // 74.9%
        assert!(matches!(verdict, MemoryVerdict::Acceptable { .. }));
    }

    #[test]
    fn classify_memory_at_critical_threshold() {
        let verdict = classify_memory_usage(900, 1000); // 90%
        assert!(matches!(verdict, MemoryVerdict::Critical { .. }));
    }

    #[test]
    fn classify_memory_over_capacity_is_critical() {
        let verdict = classify_memory_usage(2000, 1000); // 200%
        match verdict {
            MemoryVerdict::Critical { pct_of_max } => assert!((pct_of_max - 200.0).abs() < 1e-9),
            other => panic!("expected Critical, got {other:?}"),
        }
    }

    // ---- LogTailState / find_overlap_start -----------------------------------

    fn s(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| (*s).to_string()).collect()
    }

    #[test]
    fn overlap_start_finds_full_previous_as_prefix_of_fresh() {
        let previous = s(&["a", "b", "c"]);
        let fresh = s(&["a", "b", "c", "d", "e"]);
        assert_eq!(find_overlap_start(&previous, &fresh), Some(3));
    }

    #[test]
    fn overlap_start_finds_partial_suffix_after_shrink() {
        // Only "b", "c" of `previous` survived (e.g. "a" aged/trimmed out),
        // and one new entry "d" was appended.
        let previous = s(&["a", "b", "c"]);
        let fresh = s(&["b", "c", "d"]);
        assert_eq!(find_overlap_start(&previous, &fresh), Some(2));
    }

    #[test]
    fn overlap_start_none_when_previous_empty() {
        let previous: Vec<String> = Vec::new();
        let fresh = s(&["a", "b"]);
        assert_eq!(find_overlap_start(&previous, &fresh), None);
    }

    #[test]
    fn overlap_start_none_when_disjoint() {
        let previous = s(&["a", "b", "c"]);
        let fresh = s(&["x", "y", "z"]);
        assert_eq!(find_overlap_start(&previous, &fresh), None);
    }

    #[test]
    fn log_tail_state_first_advance_yields_nothing_when_used_as_seed() {
        // The real call site discards the first `advance`'s return value
        // (it duplicates what the pre-follow dump already printed); confirm
        // that after seeding, a no-op poll reports no new entries.
        let mut state = LogTailState::default();
        let seed = s(&["l1", "l2", "l3"]);
        let _ = state.advance(seed.clone());

        let unchanged = state.advance(seed);
        assert!(unchanged.is_empty());
    }

    #[test]
    fn log_tail_state_reports_only_appended_entries() {
        let mut state = LogTailState::default();
        let _ = state.advance(s(&["l1", "l2", "l3"]));

        let new_entries = state.advance(s(&["l1", "l2", "l3", "l4", "l5"]));
        assert_eq!(new_entries, s(&["l4", "l5"]));
    }

    #[test]
    fn log_tail_state_survives_a_trim_that_shrinks_the_window() {
        // This is the exact scenario that broke the old `LLEN`-index-based
        // tracker: the list is trimmed (shrinks) between polls, so an
        // absolute length comparison would see `current_length < last_length`
        // and stop emitting new lines -- potentially forever.
        let mut state = LogTailState::default();
        let _ = state.advance(s(&["l1", "l2", "l3", "l4", "l5"]));

        // Trimmed to the last 2 entries, then one new entry appended.
        let new_entries = state.advance(s(&["l4", "l5", "l6"]));
        assert_eq!(new_entries, s(&["l6"]));
    }

    #[test]
    fn log_tail_state_treats_total_replacement_as_all_new() {
        // A burst bigger than the tracked window (or the list being
        // cleared and refilled): no overlap is found, so the whole fresh
        // window is reported. Over-printing a few lines is far less
        // harmful than the original "goes silent forever" bug.
        let mut state = LogTailState::default();
        let _ = state.advance(s(&["l1", "l2", "l3"]));

        let new_entries = state.advance(s(&["l100", "l101", "l102"]));
        assert_eq!(new_entries, s(&["l100", "l101", "l102"]));
    }

    #[test]
    fn log_tail_state_no_change_reports_nothing() {
        let mut state = LogTailState::default();
        let window = s(&["l1", "l2"]);
        let _ = state.advance(window.clone());

        assert!(state.advance(window).is_empty());
    }
}
