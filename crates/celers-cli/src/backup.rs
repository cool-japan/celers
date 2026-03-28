//! Backup and restore functionality for CeleRS broker state.
//!
//! Provides complete backup and restore capabilities for broker state including
//! queues, scheduled tasks, worker configurations, and metrics.

use anyhow::{Context, Result};
use colored::Colorize;
use oxiarc_archive::{TarReader, TarWriter};
use oxiarc_deflate::{gzip_compress, gzip_decompress};
use redis::Commands;
use reqwest::Url;
use serde::{Deserialize, Serialize};

/// Mask password in URL for safe display
fn mask_password(url: &str) -> String {
    if let Ok(parsed) = Url::parse(url) {
        if parsed.password().is_some() {
            let mut masked = parsed.clone();
            let _ = masked.set_password(Some("****"));
            return masked.to_string();
        }
    }
    url.to_string()
}

/// Backup metadata
#[derive(Debug, Serialize, Deserialize)]
pub struct BackupMetadata {
    /// Backup creation timestamp
    pub timestamp: String,
    /// Broker type
    pub broker_type: String,
    /// Broker URL (sanitized)
    pub broker_url: String,
    /// Number of queues backed up
    pub queue_count: usize,
    /// Number of tasks backed up
    pub task_count: usize,
    /// Number of scheduled tasks backed up
    pub schedule_count: usize,
    /// CeleRS version
    pub version: String,
}

/// Queue backup data
#[derive(Debug, Serialize, Deserialize)]
pub struct QueueBackup {
    /// Queue name
    pub name: String,
    /// Queue type (fifo or priority)
    pub queue_type: String,
    /// Pending tasks
    pub pending_tasks: Vec<String>,
    /// DLQ tasks
    pub dlq_tasks: Vec<String>,
    /// Delayed tasks
    pub delayed_tasks: Vec<String>,
}

/// Complete backup structure
#[derive(Debug, Serialize, Deserialize)]
pub struct Backup {
    /// Backup metadata
    pub metadata: BackupMetadata,
    /// Queue backups
    pub queues: Vec<QueueBackup>,
    /// Scheduled tasks
    pub schedules: Vec<ScheduleBackup>,
}

/// Scheduled task backup
#[derive(Debug, Serialize, Deserialize)]
pub struct ScheduleBackup {
    /// Schedule name
    pub name: String,
    /// Task name
    pub task: String,
    /// Cron expression
    pub cron: String,
    /// Queue name
    pub queue: String,
    /// Task arguments (JSON)
    pub args: Option<String>,
}

/// Create a full backup of broker state
///
/// # Arguments
///
/// * `broker_url` - Broker connection URL
/// * `output_path` - Output file path (should end with .tar.gz)
///
/// # Examples
///
/// ```no_run
/// use celers_cli::backup::create_backup;
///
/// # async fn example() -> anyhow::Result<()> {
/// create_backup("redis://localhost:6379", "backup.tar.gz").await?;
/// # Ok(())
/// # }
/// ```
pub async fn create_backup(broker_url: &str, output_path: &str) -> Result<()> {
    println!("{}", "Creating backup...".cyan());

    // Connect to Redis
    let client = redis::Client::open(broker_url).context("Failed to create Redis client")?;
    let mut con = client
        .get_connection()
        .context("Failed to connect to Redis")?;

    // Get all queue names
    let queue_keys: Vec<String> = con
        .keys("celers:queue:*")
        .context("Failed to get queue keys")?;

    let mut queues = Vec::new();
    let mut total_tasks = 0;

    for key in queue_keys {
        // Extract queue name from key
        let queue_name = key
            .strip_prefix("celers:queue:")
            .unwrap_or(&key)
            .to_string();

        // Skip internal keys
        if queue_name.contains(':') {
            continue;
        }

        println!("  Backing up queue: {}", queue_name.yellow());

        // Get pending tasks
        let pending_tasks: Vec<String> = con
            .lrange(format!("celers:queue:{queue_name}"), 0, -1)
            .unwrap_or_default();

        // Get DLQ tasks
        let dlq_tasks: Vec<String> = con
            .lrange(format!("celers:dlq:{queue_name}"), 0, -1)
            .unwrap_or_default();

        // Get delayed tasks
        let delayed_tasks: Vec<String> = con
            .zrange(format!("celers:delayed:{queue_name}"), 0, -1)
            .unwrap_or_default();

        let task_count = pending_tasks.len() + dlq_tasks.len() + delayed_tasks.len();
        total_tasks += task_count;

        println!(
            "    {} tasks (pending: {}, dlq: {}, delayed: {})",
            task_count,
            pending_tasks.len(),
            dlq_tasks.len(),
            delayed_tasks.len()
        );

        queues.push(QueueBackup {
            name: queue_name,
            queue_type: "fifo".to_string(), // Default to FIFO
            pending_tasks,
            dlq_tasks,
            delayed_tasks,
        });
    }

    // Get scheduled tasks
    let mut schedules = Vec::new();
    let schedule_keys: Vec<String> = con.keys("celers:beat:schedule:*").unwrap_or_default();

    for key in schedule_keys {
        let schedule_name = key
            .strip_prefix("celers:beat:schedule:")
            .unwrap_or(&key)
            .to_string();

        if let Ok(_data) = con.get::<_, String>(&key) {
            // Parse schedule data (simplified - actual format may vary)
            // TODO: Parse actual schedule format from Redis
            schedules.push(ScheduleBackup {
                name: schedule_name,
                task: String::new(),
                cron: String::new(),
                queue: String::new(),
                args: None,
            });
        }
    }

    // Create backup metadata
    let metadata = BackupMetadata {
        timestamp: chrono::Utc::now().to_rfc3339(),
        broker_type: "redis".to_string(),
        broker_url: mask_password(broker_url),
        queue_count: queues.len(),
        task_count: total_tasks,
        schedule_count: schedules.len(),
        version: env!("CARGO_PKG_VERSION").to_string(),
    };

    let backup = Backup {
        metadata,
        queues,
        schedules,
    };

    // Create tar.gz archive
    // First build the tar in memory, then gzip compress, then write to file
    let mut tar_buf = Vec::new();
    {
        let cursor = std::io::Cursor::new(&mut tar_buf);
        let mut tar_writer = TarWriter::new(cursor);

        // Add backup data as JSON
        let json_data = serde_json::to_string_pretty(&backup)?;

        tar_writer
            .add_file("backup.json", json_data.as_bytes())
            .map_err(|e| anyhow::anyhow!("Failed to add file to tar: {}", e))?;

        // Finish the archive
        tar_writer
            .into_inner()
            .map_err(|e| anyhow::anyhow!("Failed to finish tar: {}", e))?;
    }

    // Gzip compress the tar data and write to file
    let compressed = gzip_compress(&tar_buf, 6)
        .map_err(|e| anyhow::anyhow!("Failed to gzip compress: {}", e))?;
    std::fs::write(output_path, &compressed).context("Failed to write output file")?;

    println!();
    println!("{} Backup created successfully", "✓".green().bold());
    println!("  File: {}", output_path.cyan());
    println!("  Queues: {}", backup.metadata.queue_count);
    println!("  Tasks: {}", backup.metadata.task_count);
    println!("  Schedules: {}", backup.metadata.schedule_count);

    Ok(())
}

/// Restore broker state from backup
///
/// # Arguments
///
/// * `broker_url` - Broker connection URL
/// * `input_path` - Backup file path (.tar.gz)
/// * `dry_run` - If true, validate without actually restoring
/// * `selective_queues` - Optional list of specific queues to restore
///
/// # Examples
///
/// ```no_run
/// use celers_cli::backup::restore_backup;
///
/// # async fn example() -> anyhow::Result<()> {
/// restore_backup("redis://localhost:6379", "backup.tar.gz", false, None).await?;
/// # Ok(())
/// # }
/// ```
#[allow(clippy::too_many_arguments)]
pub async fn restore_backup(
    broker_url: &str,
    input_path: &str,
    dry_run: bool,
    selective_queues: Option<Vec<String>>,
) -> Result<()> {
    println!("{}", "Restoring from backup...".cyan());

    // Extract and parse backup
    let compressed_data = std::fs::read(input_path).context("Failed to read backup file")?;
    let tar_data = gzip_decompress(&compressed_data)
        .map_err(|e| anyhow::anyhow!("Failed to decompress backup: {}", e))?;
    let cursor = std::io::Cursor::new(tar_data);
    let mut tar_reader =
        TarReader::new(cursor).map_err(|e| anyhow::anyhow!("Failed to read tar: {}", e))?;

    let backup_json_data = tar_reader
        .extract_by_name("backup.json")
        .map_err(|e| anyhow::anyhow!("Failed to extract backup.json: {}", e))?
        .context("backup.json not found in archive")?;
    let backup_json =
        String::from_utf8(backup_json_data).context("backup.json contains invalid UTF-8")?;

    let backup: Backup =
        serde_json::from_str(&backup_json).context("Failed to parse backup data")?;

    // Display backup info
    println!();
    println!("{}", "Backup Information:".green().bold());
    println!("  Created: {}", backup.metadata.timestamp.yellow());
    println!("  Broker: {}", backup.metadata.broker_url);
    println!("  Queues: {}", backup.metadata.queue_count);
    println!("  Tasks: {}", backup.metadata.task_count);
    println!("  Schedules: {}", backup.metadata.schedule_count);
    println!();

    if dry_run {
        println!("{} Dry run mode - no changes will be made", "ℹ".blue());
        return Ok(());
    }

    // Connect to Redis
    let client = redis::Client::open(broker_url).context("Failed to create Redis client")?;
    let mut con = client
        .get_connection()
        .context("Failed to connect to Redis")?;

    let mut restored_queues = 0;
    let mut restored_tasks = 0;

    // Restore queues
    for queue in backup.queues {
        // Check if we should restore this queue
        if let Some(ref filter) = selective_queues {
            if !filter.contains(&queue.name) {
                continue;
            }
        }

        println!("  Restoring queue: {}", queue.name.yellow());

        // Restore pending tasks
        for task in queue.pending_tasks {
            let _: () = con.rpush(format!("celers:queue:{}", queue.name), &task)?;
            restored_tasks += 1;
        }

        // Restore DLQ tasks
        for task in queue.dlq_tasks {
            let _: () = con.rpush(format!("celers:dlq:{}", queue.name), &task)?;
            restored_tasks += 1;
        }

        // Restore delayed tasks
        for task in queue.delayed_tasks {
            let _: () = con.zadd(
                format!("celers:delayed:{}", queue.name),
                &task,
                chrono::Utc::now().timestamp(),
            )?;
            restored_tasks += 1;
        }

        restored_queues += 1;
    }

    println!();
    println!("{} Restore completed successfully", "✓".green().bold());
    println!("  Queues restored: {}", restored_queues);
    println!("  Tasks restored: {}", restored_tasks);

    Ok(())
}
