//! Utility functions shared across CLI commands.
//!
//! This module provides common helper functions used by various CLI commands,
//! including formatting, validation, and retry logic.

use colored::Colorize;
use tabled::{settings::Style, Table, Tabled};

/// Mask password in connection URLs for safe display.
///
/// Replaces the password portion of database/broker URLs with asterisks
/// to prevent sensitive information from being displayed.
///
/// # Examples
///
/// ```
/// # use celers_cli::command_utils::mask_password;
/// let url = "postgres://user:secret@localhost/db";
/// let masked = mask_password(url);
/// assert!(masked.contains("****"));
/// assert!(!masked.contains("secret"));
/// ```
pub fn mask_password(url: &str) -> String {
    if let Some(at_pos) = url.find('@') {
        if let Some(colon_pos) = url[..at_pos].rfind(':') {
            let before = &url[..=colon_pos];
            let after = &url[at_pos..];
            return format!("{before}****{after}");
        }
    }
    url.to_string()
}

/// Format byte size into human-readable string.
///
/// Converts raw byte counts into human-readable format using
/// appropriate units (B, KB, MB, GB, TB).
///
/// # Examples
///
/// ```
/// # use celers_cli::command_utils::format_bytes;
/// assert_eq!(format_bytes(1024), "1.00 KB");
/// assert_eq!(format_bytes(1048576), "1.00 MB");
/// ```
pub fn format_bytes(bytes: usize) -> String {
    const UNITS: &[&str] = &["B", "KB", "MB", "GB", "TB"];
    let mut size = bytes as f64;
    let mut unit_idx = 0;

    while size >= 1024.0 && unit_idx < UNITS.len() - 1 {
        size /= 1024.0;
        unit_idx += 1;
    }

    if unit_idx == 0 {
        format!("{} {}", size as usize, UNITS[unit_idx])
    } else {
        format!("{:.2} {}", size, UNITS[unit_idx])
    }
}

/// Format duration into human-readable string.
///
/// Converts seconds into a human-readable format (e.g., "1h 30m", "2d 5h").
///
/// # Examples
///
/// ```
/// # use celers_cli::command_utils::format_duration;
/// assert_eq!(format_duration(90), "1m 30s");
/// assert_eq!(format_duration(3660), "1h 1m");
/// ```
pub fn format_duration(seconds: u64) -> String {
    if seconds < 60 {
        format!("{seconds}s")
    } else if seconds < 3600 {
        let minutes = seconds / 60;
        let secs = seconds % 60;
        if secs == 0 {
            format!("{minutes}m")
        } else {
            format!("{minutes}m {secs}s")
        }
    } else if seconds < 86400 {
        let hours = seconds / 3600;
        let minutes = (seconds % 3600) / 60;
        if minutes == 0 {
            format!("{hours}h")
        } else {
            format!("{hours}h {minutes}m")
        }
    } else {
        let days = seconds / 86400;
        let hours = (seconds % 86400) / 3600;
        if hours == 0 {
            format!("{days}d")
        } else {
            format!("{days}d {hours}h")
        }
    }
}

/// Validate task ID format (UUID).
///
/// Ensures the provided string is a valid UUID format.
///
/// # Errors
///
/// Returns an error if the task ID is not a valid UUID.
///
/// # Examples
///
/// ```
/// # use celers_cli::command_utils::validate_task_id;
/// let valid_id = "550e8400-e29b-41d4-a716-446655440000";
/// assert!(validate_task_id(valid_id).is_ok());
///
/// let invalid_id = "not-a-uuid";
/// assert!(validate_task_id(invalid_id).is_err());
/// ```
pub fn validate_task_id(task_id: &str) -> anyhow::Result<uuid::Uuid> {
    uuid::Uuid::parse_str(task_id)
        .map_err(|e| anyhow::anyhow!("Invalid task ID format: {e}. Expected UUID format."))
}

/// Validate queue name.
///
/// Ensures the queue name is not empty, doesn't contain whitespace,
/// and is not too long.
///
/// # Errors
///
/// Returns an error if the queue name is invalid.
///
/// # Examples
///
/// ```
/// # use celers_cli::command_utils::validate_queue_name;
/// assert!(validate_queue_name("my_queue").is_ok());
/// assert!(validate_queue_name("").is_err());
/// assert!(validate_queue_name("has spaces").is_err());
/// ```
pub fn validate_queue_name(queue: &str) -> anyhow::Result<()> {
    if queue.is_empty() {
        anyhow::bail!("Queue name cannot be empty");
    }
    if queue.contains(char::is_whitespace) {
        anyhow::bail!("Queue name cannot contain whitespace");
    }
    if queue.len() > 255 {
        anyhow::bail!("Queue name too long (max 255 characters)");
    }
    Ok(())
}

/// Calculate percentage safely (handles division by zero).
///
/// # Examples
///
/// ```
/// # use celers_cli::command_utils::calculate_percentage;
/// assert_eq!(calculate_percentage(50, 100), 50.0);
/// assert_eq!(calculate_percentage(10, 0), 0.0); // Division by zero handled
/// ```
pub fn calculate_percentage(part: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64 / total as f64) * 100.0
    }
}

/// Print task details in a formatted table.
///
/// Displays task metadata including ID, name, queue, priority, and arguments.
pub fn print_task_details(task: &celers_core::SerializedTask, location: &str) {
    println!("{}", format!("Location: {location}").green().bold());
    println!();

    #[derive(Tabled)]
    struct TaskDetail {
        #[tabled(rename = "Field")]
        field: String,
        #[tabled(rename = "Value")]
        value: String,
    }

    let details = vec![
        TaskDetail {
            field: "ID".to_string(),
            value: task.metadata.id.to_string(),
        },
        TaskDetail {
            field: "Name".to_string(),
            value: task.metadata.name.clone(),
        },
        TaskDetail {
            field: "State".to_string(),
            value: format!("{:?}", task.metadata.state),
        },
        TaskDetail {
            field: "Priority".to_string(),
            value: task.metadata.priority.to_string(),
        },
        TaskDetail {
            field: "Max Retries".to_string(),
            value: task.metadata.max_retries.to_string(),
        },
        TaskDetail {
            field: "Timeout".to_string(),
            value: task
                .metadata
                .timeout_secs
                .map_or_else(|| "default".to_string(), |s| format!("{s}s")),
        },
        TaskDetail {
            field: "Created At".to_string(),
            value: task.metadata.created_at.to_string(),
        },
        TaskDetail {
            field: "Payload Size".to_string(),
            value: format!("{} bytes", task.payload.len()),
        },
    ];

    let table = Table::new(details).with(Style::rounded()).to_string();
    println!("{table}");
}

/// Display a log line with optional level filtering.
///
/// Parses JSON log format and displays with color coding based on level.
/// Filters out logs that don't match the specified level (if provided).
pub fn display_log_line(log: &str, level_filter: Option<&str>) {
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

        // Filter by level if specified
        if let Some(filter) = level_filter {
            if !level.eq_ignore_ascii_case(filter) {
                return;
            }
        }

        // Color code by level
        let colored_level = match level {
            "ERROR" | "error" => level.red().bold(),
            "WARN" | "warn" => level.yellow().bold(),
            "DEBUG" | "debug" => level.cyan(),
            _ => level.green(),
        };

        let colored_msg = match level {
            "ERROR" | "error" => message.red(),
            "WARN" | "warn" => message.yellow(),
            _ => message.normal(),
        };

        println!("[{timestamp}] {colored_level}: {colored_msg}");
    } else {
        // Plain text log
        println!("{log}");
    }
}

/// Send alert via webhook.
///
/// Posts a JSON payload to the specified webhook URL with the alert message.
///
/// # Errors
///
/// Returns an error if the HTTP request fails or returns a non-success status.
pub async fn send_webhook_alert(webhook_url: &str, message: &str) -> anyhow::Result<()> {
    let client = reqwest::Client::new();
    let payload = serde_json::json!({
        "text": message,
        "timestamp": chrono::Utc::now().to_rfc3339(),
    });

    let response = client.post(webhook_url).json(&payload).send().await?;

    if !response.status().is_success() {
        anyhow::bail!("Webhook request failed with status: {}", response.status());
    }

    Ok(())
}

/// Retry an async operation with exponential backoff.
///
/// Attempts the operation up to `max_retries` times with exponential backoff
/// between attempts.
///
/// # Type Parameters
///
/// * `F` - Future-producing function
/// * `T` - Success result type
/// * `E` - Error type (must implement Display)
///
/// # Errors
///
/// Returns the last error if all retry attempts fail.
pub async fn retry_with_backoff<F, T, E>(
    operation: F,
    max_retries: u32,
    operation_name: &str,
) -> Result<T, E>
where
    F: Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<T, E>> + Send>>,
    E: std::fmt::Display,
{
    let mut retries = 0;
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(err) => {
                retries += 1;
                if retries >= max_retries {
                    eprintln!(
                        "{}",
                        format!("✗ {operation_name} failed after {max_retries} attempts: {err}")
                            .red()
                    );
                    return Err(err);
                }

                let backoff_secs = 2_u64.pow(retries);
                eprintln!(
                    "{}",
                    format!(
                        "⚠ {operation_name} attempt {retries} failed: {err}. Retrying in {backoff_secs}s..."
                    )
                    .yellow()
                );
                tokio::time::sleep(tokio::time::Duration::from_secs(backoff_secs)).await;
            }
        }
    }
}

/// Truncate a string to a maximum length with ellipsis.
///
/// If the string exceeds `max_len`, it will be truncated and "..." will be appended.
///
/// # Examples
///
/// ```
/// # use celers_cli::command_utils::truncate_string;
/// assert_eq!(truncate_string("Hello, World!", 10), "Hello, ...");
/// assert_eq!(truncate_string("Short", 10), "Short");
/// ```
pub fn truncate_string(s: &str, max_len: usize) -> String {
    if s.len() <= max_len {
        s.to_string()
    } else {
        format!("{}...", &s[..max_len.saturating_sub(3)])
    }
}

/// Format a timestamp into a human-readable relative time.
///
/// Converts a timestamp into a relative time string like "2 hours ago" or "just now".
///
/// # Examples
///
/// ```
/// # use celers_cli::command_utils::format_relative_time;
/// # use chrono::{Utc, Duration};
/// let now = Utc::now();
/// let past = now - Duration::minutes(5);
/// assert!(format_relative_time(&past).contains("ago"));
/// ```
pub fn format_relative_time(timestamp: &chrono::DateTime<chrono::Utc>) -> String {
    let now = chrono::Utc::now();
    let duration = now.signed_duration_since(*timestamp);

    if duration.num_seconds() < 60 {
        "just now".to_string()
    } else if duration.num_minutes() < 60 {
        let mins = duration.num_minutes();
        if mins == 1 {
            "1 minute ago".to_string()
        } else {
            format!("{mins} minutes ago")
        }
    } else if duration.num_hours() < 24 {
        let hours = duration.num_hours();
        if hours == 1 {
            "1 hour ago".to_string()
        } else {
            format!("{hours} hours ago")
        }
    } else {
        let days = duration.num_days();
        if days == 1 {
            "1 day ago".to_string()
        } else {
            format!("{days} days ago")
        }
    }
}

/// Validate and normalize broker URL.
///
/// Ensures the broker URL has a valid scheme and normalizes the format.
///
/// # Errors
///
/// Returns an error if the URL is invalid or has an unsupported scheme.
///
/// # Examples
///
/// ```
/// # use celers_cli::command_utils::validate_broker_url;
/// assert!(validate_broker_url("redis://localhost:6379").is_ok());
/// assert!(validate_broker_url("postgresql://localhost/db").is_ok());
/// assert!(validate_broker_url("invalid").is_err());
/// ```
pub fn validate_broker_url(url: &str) -> anyhow::Result<String> {
    if url.is_empty() {
        anyhow::bail!("Broker URL cannot be empty");
    }

    // Check for valid scheme
    let valid_schemes = [
        "redis",
        "rediss",
        "postgres",
        "postgresql",
        "mysql",
        "amqp",
        "amqps",
    ];
    let has_valid_scheme = valid_schemes.iter().any(|&scheme| {
        url.starts_with(&format!("{scheme}://")) || url.starts_with(&format!("{scheme}:"))
    });

    if !has_valid_scheme {
        anyhow::bail!(
            "Invalid broker URL scheme. Supported: {}",
            valid_schemes.join(", ")
        );
    }

    Ok(url.to_string())
}

/// Format a count with thousands separators.
///
/// Adds commas as thousands separators for better readability.
///
/// # Examples
///
/// ```
/// # use celers_cli::command_utils::format_count;
/// assert_eq!(format_count(1000), "1,000");
/// assert_eq!(format_count(1234567), "1,234,567");
/// ```
pub fn format_count(count: usize) -> String {
    let count_str = count.to_string();
    let mut result = String::new();

    for (i, ch) in count_str.chars().rev().enumerate() {
        if i > 0 && i % 3 == 0 {
            result.insert(0, ',');
        }
        result.insert(0, ch);
    }

    result
}

/// Display a simple progress indicator.
///
/// Prints a progress bar with percentage and optional message.
pub fn print_progress(current: usize, total: usize, message: &str) {
    if total == 0 {
        return;
    }

    let percentage = (current as f64 / total as f64 * 100.0) as usize;
    let bar_width = 40;
    let filled = (bar_width * current) / total;
    let empty = bar_width - filled;

    let bar = format!(
        "[{}{}] {}/{}",
        "=".repeat(filled),
        " ".repeat(empty),
        current,
        total
    );

    eprint!("\r{} {}% {}", bar, percentage, message);
    if current >= total {
        eprintln!(); // New line when complete
    }
}

/// Write data to CSV file
///
/// # Arguments
///
/// * `file_path` - Output file path
/// * `headers` - CSV column headers
/// * `rows` - Data rows
///
/// # Examples
///
/// ```no_run
/// use celers_cli::command_utils::write_csv;
///
/// # fn example() -> anyhow::Result<()> {
/// let headers = vec!["Name", "Count"];
/// let rows = vec![
///     vec!["Tasks".to_string(), "100".to_string()],
///     vec!["Workers".to_string(), "5".to_string()],
/// ];
/// write_csv("report.csv", &headers, &rows)?;
/// # Ok(())
/// # }
/// ```
pub fn write_csv(file_path: &str, headers: &[&str], rows: &[Vec<String>]) -> anyhow::Result<()> {
    use std::fs::File;
    let file = File::create(file_path)?;
    let mut writer = csv::Writer::from_writer(file);

    // Write headers
    writer.write_record(headers)?;

    // Write rows
    for row in rows {
        writer.write_record(row)?;
    }

    writer.flush()?;
    Ok(())
}

/// Format task statistics as CSV rows
///
/// # Arguments
///
/// * `stats` - Map of task statistics (task_type -> count)
///
/// # Examples
///
/// ```
/// use std::collections::HashMap;
/// use celers_cli::command_utils::format_task_stats_csv;
///
/// let mut stats = HashMap::new();
/// stats.insert("process_data".to_string(), 100);
/// stats.insert("send_email".to_string(), 50);
///
/// let rows = format_task_stats_csv(&stats);
/// assert_eq!(rows.len(), 2);
/// ```
pub fn format_task_stats_csv(stats: &std::collections::HashMap<String, usize>) -> Vec<Vec<String>> {
    let mut rows: Vec<Vec<String>> = stats
        .iter()
        .map(|(task, count)| vec![task.clone(), count.to_string()])
        .collect();

    // Sort by count descending
    rows.sort_by(|a, b| {
        let count_a: usize = a[1].parse().unwrap_or(0);
        let count_b: usize = b[1].parse().unwrap_or(0);
        count_b.cmp(&count_a)
    });

    rows
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mask_password() {
        // Test PostgreSQL URL
        let pg_url = "postgres://user:password123@localhost:5432/dbname";
        let masked = mask_password(pg_url);
        assert!(masked.contains("postgres://user:****@localhost"));
        assert!(!masked.contains("password123"));

        // Test MySQL URL
        let mysql_url = "mysql://admin:secret@127.0.0.1:3306/db";
        let masked = mask_password(mysql_url);
        assert!(masked.contains("mysql://admin:****@127.0.0.1"));
        assert!(!masked.contains("secret"));

        // Test URL without password
        let no_pass_url = "redis://localhost:6379";
        let masked = mask_password(no_pass_url);
        assert_eq!(masked, no_pass_url);
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(500), "500 B");
        assert_eq!(format_bytes(1024), "1.00 KB");
        assert_eq!(format_bytes(1536), "1.50 KB");
        assert_eq!(format_bytes(1048576), "1.00 MB");
        assert_eq!(format_bytes(1073741824), "1.00 GB");
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(0), "0s");
        assert_eq!(format_duration(30), "30s");
        assert_eq!(format_duration(60), "1m");
        assert_eq!(format_duration(90), "1m 30s");
        assert_eq!(format_duration(3600), "1h");
        assert_eq!(format_duration(3660), "1h 1m");
        assert_eq!(format_duration(86400), "1d");
        assert_eq!(format_duration(90000), "1d 1h");
    }

    #[test]
    fn test_validate_task_id() {
        // Valid UUID
        let valid = "550e8400-e29b-41d4-a716-446655440000";
        assert!(validate_task_id(valid).is_ok());

        // Invalid UUIDs
        assert!(validate_task_id("not-a-uuid").is_err());
        assert!(validate_task_id("").is_err());
        assert!(validate_task_id("12345").is_err());
    }

    #[test]
    fn test_validate_queue_name() {
        // Valid queue names
        assert!(validate_queue_name("default").is_ok());
        assert!(validate_queue_name("high-priority").is_ok());
        assert!(validate_queue_name("queue_1").is_ok());

        // Invalid queue names
        assert!(validate_queue_name("").is_err()); // Empty
        assert!(validate_queue_name("queue name").is_err()); // Whitespace
        assert!(validate_queue_name(&"x".repeat(256)).is_err()); // Too long
    }

    #[test]
    fn test_calculate_percentage() {
        assert_eq!(calculate_percentage(0, 100), 0.0);
        assert_eq!(calculate_percentage(50, 100), 50.0);
        assert_eq!(calculate_percentage(100, 100), 100.0);
        assert_eq!(calculate_percentage(25, 100), 25.0);

        // Edge case: division by zero
        assert_eq!(calculate_percentage(10, 0), 0.0);
    }

    #[test]
    fn test_truncate_string() {
        // Short strings should not be truncated
        assert_eq!(truncate_string("Hello", 10), "Hello");
        assert_eq!(truncate_string("", 10), "");

        // Long strings should be truncated with ellipsis
        assert_eq!(truncate_string("Hello, World!", 10), "Hello, ...");
        assert_eq!(
            truncate_string("This is a very long string", 15),
            "This is a ve..."
        );

        // Edge case: max_len <= 3
        assert_eq!(truncate_string("Hello", 3), "...");
        assert_eq!(truncate_string("Hi", 2), "Hi");
    }

    #[test]
    fn test_format_relative_time() {
        use chrono::{Duration, Utc};

        let now = Utc::now();

        // Just now
        let recent = now - Duration::seconds(30);
        assert_eq!(format_relative_time(&recent), "just now");

        // Minutes ago
        let mins_ago = now - Duration::minutes(5);
        assert_eq!(format_relative_time(&mins_ago), "5 minutes ago");

        let one_min_ago = now - Duration::minutes(1);
        assert_eq!(format_relative_time(&one_min_ago), "1 minute ago");

        // Hours ago
        let hours_ago = now - Duration::hours(3);
        assert_eq!(format_relative_time(&hours_ago), "3 hours ago");

        let one_hour_ago = now - Duration::hours(1);
        assert_eq!(format_relative_time(&one_hour_ago), "1 hour ago");

        // Days ago
        let days_ago = now - Duration::days(2);
        assert_eq!(format_relative_time(&days_ago), "2 days ago");

        let one_day_ago = now - Duration::days(1);
        assert_eq!(format_relative_time(&one_day_ago), "1 day ago");
    }

    #[test]
    fn test_validate_broker_url() {
        // Valid URLs
        assert!(validate_broker_url("redis://localhost:6379").is_ok());
        assert!(validate_broker_url("rediss://localhost:6379").is_ok());
        assert!(validate_broker_url("postgres://localhost/db").is_ok());
        assert!(validate_broker_url("postgresql://user:pass@localhost/db").is_ok());
        assert!(validate_broker_url("mysql://localhost:3306/db").is_ok());
        assert!(validate_broker_url("amqp://localhost:5672").is_ok());
        assert!(validate_broker_url("amqps://localhost:5671").is_ok());

        // Invalid URLs
        assert!(validate_broker_url("").is_err()); // Empty
        assert!(validate_broker_url("http://localhost").is_err()); // Wrong scheme
        assert!(validate_broker_url("ftp://localhost").is_err()); // Wrong scheme
        assert!(validate_broker_url("invalid").is_err()); // No scheme
        assert!(validate_broker_url("localhost:6379").is_err()); // Missing scheme
    }

    #[test]
    fn test_format_count() {
        assert_eq!(format_count(0), "0");
        assert_eq!(format_count(100), "100");
        assert_eq!(format_count(999), "999");
        assert_eq!(format_count(1000), "1,000");
        assert_eq!(format_count(1234), "1,234");
        assert_eq!(format_count(12345), "12,345");
        assert_eq!(format_count(123456), "123,456");
        assert_eq!(format_count(1234567), "1,234,567");
        assert_eq!(format_count(12345678), "12,345,678");
    }
}
