//! Shared utility functions for CLI commands.

/// Format bytes into human-readable size
#[allow(dead_code)]
pub(crate) fn format_bytes(bytes: usize) -> String {
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

/// Format duration into human-readable string
#[allow(dead_code)]
pub(crate) fn format_duration(seconds: u64) -> String {
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

/// Validate task ID format
#[allow(dead_code)]
pub(crate) fn validate_task_id(task_id: &str) -> anyhow::Result<uuid::Uuid> {
    uuid::Uuid::parse_str(task_id)
        .map_err(|e| anyhow::anyhow!("Invalid task ID format: {e}. Expected UUID format."))
}

/// Validate queue name
#[allow(dead_code)]
pub(crate) fn validate_queue_name(queue: &str) -> anyhow::Result<()> {
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

/// Calculate percentage safely
#[allow(dead_code)]
pub(crate) fn calculate_percentage(part: usize, total: usize) -> f64 {
    if total == 0 {
        0.0
    } else {
        (part as f64 / total as f64) * 100.0
    }
}

/// Mask password in database URL for display
pub(crate) fn mask_password(url: &str) -> String {
    // Find the pattern user:password@ and mask the password
    if let Some(at_pos) = url.find('@') {
        if let Some(colon_pos) = url[..at_pos].rfind(':') {
            // Check if there's a scheme separator before this
            let scheme_end = url.find("://").map(|p| p + 3).unwrap_or(0);
            if colon_pos > scheme_end {
                return format!("{}****{}", &url[..colon_pos + 1], &url[at_pos..]);
            }
        }
    }
    url.to_string()
}
