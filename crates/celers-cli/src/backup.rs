//! Backup and restore functionality for CeleRS broker state.
//!
//! Provides complete backup and restore capabilities for broker state including
//! queues, scheduled tasks, worker configurations, and metrics.
//!
//! Full snapshots are captured via [`create_backup_incremental`] (called with no baseline)
//! and restored via [`restore_backup_with_policy`]. This module additionally supports:
//!
//! - **Incremental backups** ([`create_backup_incremental`]): capture only the
//!   queue/task/schedule entries that are new or changed relative to a prior backup
//!   archive (or, best-effort, relative to an explicit `--since` timestamp).
//! - **Conflict resolution on restore** ([`restore_backup_with_policy`] /
//!   [`ConflictPolicy`]): control what happens when a queue being restored already has
//!   live content at the target broker (skip it, overwrite it, or merge the two).

use anyhow::{Context, Result};
use celers_beat::task::ScheduledTask;
use chrono::{DateTime, Utc};
use colored::Colorize;
use oxiarc_archive::{TarReader, TarWriter};
use oxiarc_deflate::{gzip_compress, gzip_decompress};
use redis::Commands;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use tracing::warn;
use url::Url;

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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
    /// Whether this backup is an incremental delta rather than a full snapshot
    #[serde(default)]
    pub incremental: bool,
    /// For incremental backups, the timestamp of the baseline they were computed against
    /// (either the previous backup archive's own timestamp, or an explicit `--since`
    /// cutoff). Always `None` for full snapshots.
    #[serde(default)]
    pub since_timestamp: Option<String>,
}

/// Queue backup data
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Backup {
    /// Backup metadata
    pub metadata: BackupMetadata,
    /// Queue backups
    pub queues: Vec<QueueBackup>,
    /// Scheduled tasks
    pub schedules: Vec<ScheduleBackup>,
}

/// Scheduled task backup
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
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

/// How to resolve a queue/task that already has live content at the restore target.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, clap::ValueEnum)]
pub enum ConflictPolicy {
    /// Leave the existing queue untouched; the backup's content for that queue is not
    /// restored.
    #[default]
    Skip,
    /// Replace the existing queue's content entirely with the backup's content.
    Overwrite,
    /// Union the backup's content into the existing queue, de-duplicating tasks by id
    /// (the existing, already-live task wins on an id collision).
    Merge,
}

/// Create an incremental backup of broker state.
///
/// Unlike a plain full-snapshot call (pass `previous_backup_path: None, since: None`), this
/// diffs a freshly
/// captured broker state against a baseline and only writes out the queue/task/schedule
/// entries that are new or have changed since that baseline.
///
/// The baseline is resolved in this order:
///
/// 1. `previous_backup_path`, if given: the prior backup archive is read and the two
///    snapshots are content-diffed via [`diff_backup`] (queue task entries and schedules
///    are compared verbatim; anything present in the current scan but absent, or
///    different, in the previous archive is retained). This is the recommended,
///    deterministic mode, since it does not depend on wall-clock timestamps embedded in
///    task payloads.
/// 2. `since`, if given and `previous_backup_path` is `None`: an RFC 3339 timestamp,
///    applied via [`filter_backup_since`]. Task entries are kept only if their embedded
///    `metadata.updated_at` (falling back to `metadata.created_at`) is at or after
///    `since`; entries without a parseable timestamp are conservatively kept. This is a
///    best-effort mode: `ScheduleBackup` carries no timestamp, so schedules are always
///    retained when filtering by `since` alone.
/// 3. If neither is given, the full current snapshot is captured and written out as-is.
///
/// # Arguments
///
/// * `broker_url` - Broker connection URL
/// * `output_path` - Output file path (should end with .tar.gz)
/// * `previous_backup_path` - Optional path to a prior backup archive to diff against
/// * `since` - Optional RFC 3339 timestamp, used only when `previous_backup_path` is `None`
///
/// # Examples
///
/// ```no_run
/// use celers_cli::backup::create_backup_incremental;
///
/// # async fn example() -> anyhow::Result<()> {
/// // Diff against a prior archive (recommended: deterministic, no wall-clock dependence).
/// create_backup_incremental(
///     "redis://localhost:6379",
///     "backup-incremental.tar.gz",
///     Some("backup-full.tar.gz"),
///     None,
/// )
/// .await?;
/// # Ok(())
/// # }
/// ```
pub async fn create_backup_incremental(
    broker_url: &str,
    output_path: &str,
    previous_backup_path: Option<&str>,
    since: Option<&str>,
) -> Result<()> {
    let is_incremental = previous_backup_path.is_some() || since.is_some();

    if is_incremental {
        println!("{}", "Creating incremental backup...".cyan());
    } else {
        println!("{}", "Creating backup...".cyan());
    }

    let current = capture_broker_state(broker_url)?;

    let backup = if let Some(prev_path) = previous_backup_path {
        if since.is_some() {
            println!(
                "  {} both a previous backup and --since were given; --since is ignored",
                "Note:".yellow()
            );
        }

        let previous = read_backup_archive(prev_path)
            .with_context(|| format!("Failed to read previous backup '{prev_path}'"))?;
        println!(
            "  Diffing against previous backup created at {}",
            previous.metadata.timestamp.yellow()
        );
        diff_backup(&previous, current)
    } else if let Some(since_str) = since {
        let since_ts = DateTime::parse_from_rfc3339(since_str)
            .map(|dt| dt.with_timezone(&Utc))
            .with_context(|| {
                format!("Invalid --since timestamp '{since_str}', expected RFC 3339")
            })?;
        println!(
            "  Filtering tasks changed since {} (best-effort; schedules always included)",
            since_ts.to_rfc3339().yellow()
        );
        filter_backup_since(current, since_ts)
    } else {
        current
    };

    write_backup_archive(output_path, &backup)?;

    println!();
    if backup.metadata.incremental {
        println!(
            "{} Incremental backup created successfully",
            "✓".green().bold()
        );
    } else {
        println!("{} Backup created successfully", "✓".green().bold());
    }
    println!("  File: {}", output_path.cyan());
    println!("  Queues: {}", backup.metadata.queue_count);
    println!("  Tasks: {}", backup.metadata.task_count);
    println!("  Schedules: {}", backup.metadata.schedule_count);
    if let Some(since_ts) = &backup.metadata.since_timestamp {
        println!("  Baseline: {since_ts}");
    }

    Ok(())
}

/// Connect to the broker and capture its complete current state as a [`Backup`].
///
/// This performs the live scan (queues, pending/DLQ/delayed tasks, scheduled tasks) that
/// backs every [`create_backup_incremental`] call (full snapshot or incremental alike; it
/// diffs or filters the captured snapshot before it is written out).
fn capture_broker_state(broker_url: &str) -> Result<Backup> {
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

        if let Ok(data) = con.get::<_, String>(&key) {
            match serde_json::from_str::<ScheduledTask>(&data) {
                Ok(task) => {
                    let args = if task.args.is_empty() {
                        None
                    } else {
                        serde_json::to_string(&task.args).ok()
                    };
                    schedules.push(ScheduleBackup {
                        name: schedule_name,
                        task: task.name,
                        cron: task.schedule.to_string(),
                        queue: task.options.queue.unwrap_or_default(),
                        args,
                    });
                }
                Err(e) => {
                    warn!("Failed to parse schedule {schedule_name}: {e}");
                }
            }
        }
    }

    let metadata = BackupMetadata {
        timestamp: chrono::Utc::now().to_rfc3339(),
        broker_type: "redis".to_string(),
        broker_url: mask_password(broker_url),
        queue_count: queues.len(),
        task_count: total_tasks,
        schedule_count: schedules.len(),
        version: env!("CARGO_PKG_VERSION").to_string(),
        incremental: false,
        since_timestamp: None,
    };

    Ok(Backup {
        metadata,
        queues,
        schedules,
    })
}

/// Serialize `backup` as `backup.json` inside a tar archive, gzip-compress it, and write
/// the result to `output_path`.
fn write_backup_archive(output_path: &str, backup: &Backup) -> Result<()> {
    // Create tar.gz archive.
    // First build the tar in memory, then gzip compress, then write to file.
    let mut tar_buf = Vec::new();
    {
        let cursor = std::io::Cursor::new(&mut tar_buf);
        let mut tar_writer = TarWriter::new(cursor);

        // Add backup data as JSON
        let json_data = serde_json::to_string_pretty(backup)?;

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

    Ok(())
}

/// Read and parse a [`Backup`] from a `.tar.gz` archive at `path`.
fn read_backup_archive(path: &str) -> Result<Backup> {
    let compressed_data = std::fs::read(path).context("Failed to read backup file")?;
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

    serde_json::from_str(&backup_json).context("Failed to parse backup data")
}

/// Compute the incremental delta of `current` relative to `previous`.
///
/// A queue's pending/DLQ/delayed task entry is retained only if it does not appear
/// verbatim in the same bucket of the matching queue (by name) in `previous`; this
/// naturally captures both brand-new tasks and tasks whose content changed (for example, a
/// task's `updated_at` timestamp advancing changes its serialized bytes). A queue that ends
/// up with no retained tasks in any bucket is dropped entirely from the result. A schedule
/// is retained only if `previous` has no schedule that is identical in every field (`name`,
/// `task`, `cron`, `queue`, `args`).
///
/// The returned [`Backup`]'s metadata has `incremental` set to `true` and
/// `since_timestamp` set to `previous`'s own timestamp.
#[must_use]
pub fn diff_backup(previous: &Backup, current: Backup) -> Backup {
    let previous_queues: HashMap<&str, &QueueBackup> = previous
        .queues
        .iter()
        .map(|q| (q.name.as_str(), q))
        .collect();

    let queues: Vec<QueueBackup> = current
        .queues
        .into_iter()
        .filter_map(|queue| {
            let baseline = previous_queues.get(queue.name.as_str()).copied();
            let pending_tasks = diff_task_list(
                baseline.map(|q| q.pending_tasks.as_slice()),
                queue.pending_tasks,
            );
            let dlq_tasks =
                diff_task_list(baseline.map(|q| q.dlq_tasks.as_slice()), queue.dlq_tasks);
            let delayed_tasks = diff_task_list(
                baseline.map(|q| q.delayed_tasks.as_slice()),
                queue.delayed_tasks,
            );

            if pending_tasks.is_empty() && dlq_tasks.is_empty() && delayed_tasks.is_empty() {
                None
            } else {
                Some(QueueBackup {
                    name: queue.name,
                    queue_type: queue.queue_type,
                    pending_tasks,
                    dlq_tasks,
                    delayed_tasks,
                })
            }
        })
        .collect();

    let previous_schedules: HashSet<&ScheduleBackup> = previous.schedules.iter().collect();
    let schedules: Vec<ScheduleBackup> = current
        .schedules
        .into_iter()
        .filter(|s| !previous_schedules.contains(s))
        .collect();

    let task_count: usize = queues
        .iter()
        .map(|q| q.pending_tasks.len() + q.dlq_tasks.len() + q.delayed_tasks.len())
        .sum();

    let metadata = BackupMetadata {
        timestamp: current.metadata.timestamp,
        broker_type: current.metadata.broker_type,
        broker_url: current.metadata.broker_url,
        queue_count: queues.len(),
        task_count,
        schedule_count: schedules.len(),
        version: current.metadata.version,
        incremental: true,
        since_timestamp: Some(previous.metadata.timestamp.clone()),
    };

    Backup {
        metadata,
        queues,
        schedules,
    }
}

/// Retain only the entries of `incoming` that are not byte-identical to some entry in
/// `baseline`. When `baseline` is `None` (the queue did not exist at all in the previous
/// backup) every entry is retained.
fn diff_task_list(baseline: Option<&[String]>, incoming: Vec<String>) -> Vec<String> {
    match baseline {
        None => incoming,
        Some(baseline) => {
            let baseline_set: HashSet<&str> = baseline.iter().map(String::as_str).collect();
            incoming
                .into_iter()
                .filter(|t| !baseline_set.contains(t.as_str()))
                .collect()
        }
    }
}

/// Best-effort filter used when no previous backup archive is available: retains only task
/// entries whose embedded `metadata.updated_at` (falling back to `metadata.created_at`) is
/// at or after `since`. Task entries whose timestamp cannot be determined are conservatively
/// kept, since we cannot prove they are unchanged. `ScheduleBackup` carries no timestamp at
/// all, so schedules are always retained by this filter; prefer [`diff_backup`] against a
/// previous archive when accurate schedule filtering matters.
///
/// The returned [`Backup`]'s metadata has `incremental` set to `true` and
/// `since_timestamp` set to `since` (formatted as RFC 3339).
#[must_use]
pub fn filter_backup_since(current: Backup, since: DateTime<Utc>) -> Backup {
    let queues: Vec<QueueBackup> = current
        .queues
        .into_iter()
        .filter_map(|queue| {
            let pending_tasks = retain_since(queue.pending_tasks, since);
            let dlq_tasks = retain_since(queue.dlq_tasks, since);
            let delayed_tasks = retain_since(queue.delayed_tasks, since);

            if pending_tasks.is_empty() && dlq_tasks.is_empty() && delayed_tasks.is_empty() {
                None
            } else {
                Some(QueueBackup {
                    name: queue.name,
                    queue_type: queue.queue_type,
                    pending_tasks,
                    dlq_tasks,
                    delayed_tasks,
                })
            }
        })
        .collect();

    let task_count: usize = queues
        .iter()
        .map(|q| q.pending_tasks.len() + q.dlq_tasks.len() + q.delayed_tasks.len())
        .sum();
    let schedule_count = current.schedules.len();

    let metadata = BackupMetadata {
        timestamp: current.metadata.timestamp,
        broker_type: current.metadata.broker_type,
        broker_url: current.metadata.broker_url,
        queue_count: queues.len(),
        task_count,
        schedule_count,
        version: current.metadata.version,
        incremental: true,
        since_timestamp: Some(since.to_rfc3339()),
    };

    Backup {
        metadata,
        queues,
        schedules: current.schedules,
    }
}

/// Keep only entries whose embedded task timestamp is at or after `since` (entries whose
/// timestamp cannot be determined are conservatively kept).
fn retain_since(tasks: Vec<String>, since: DateTime<Utc>) -> Vec<String> {
    tasks
        .into_iter()
        .filter(|raw| match task_timestamp(raw) {
            Some(ts) => ts >= since,
            None => true,
        })
        .collect()
}

/// Best-effort extraction of a task's `updated_at` (falling back to `created_at`) from its
/// serialized JSON representation, as produced by `celers-broker-redis`'s `SerializedTask`.
/// Returns `None` when `raw` is not JSON, or has no recognizable metadata timestamp.
fn task_timestamp(raw: &str) -> Option<DateTime<Utc>> {
    let value: serde_json::Value = serde_json::from_str(raw).ok()?;
    let metadata = value.get("metadata")?;
    let ts = metadata
        .get("updated_at")
        .or_else(|| metadata.get("created_at"))?
        .as_str()?;
    DateTime::parse_from_rfc3339(ts)
        .ok()
        .map(|dt| dt.with_timezone(&Utc))
}

/// Restore broker state from a backup archive, resolving conflicts with pre-existing
/// queues according to `conflict_policy`.
///
/// # Arguments
///
/// * `broker_url` - Broker connection URL
/// * `input_path` - Backup file path (.tar.gz)
/// * `dry_run` - If true, validate without actually restoring
/// * `selective_queues` - Optional list of specific queues to restore
/// * `conflict_policy` - How to resolve a queue that already has live content at the
///   restore target; see [`ConflictPolicy`]
///
/// # Examples
///
/// ```no_run
/// use celers_cli::backup::{restore_backup_with_policy, ConflictPolicy};
///
/// # async fn example() -> anyhow::Result<()> {
/// restore_backup_with_policy(
///     "redis://localhost:6379",
///     "backup.tar.gz",
///     false,
///     None,
///     ConflictPolicy::Skip,
/// )
/// .await?;
/// # Ok(())
/// # }
/// ```
pub async fn restore_backup_with_policy(
    broker_url: &str,
    input_path: &str,
    dry_run: bool,
    selective_queues: Option<Vec<String>>,
    conflict_policy: ConflictPolicy,
) -> Result<()> {
    println!("{}", "Restoring from backup...".cyan());

    let backup = read_backup_archive(input_path)?;

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
    let mut skipped_queues = 0;

    // Restore queues
    for queue in backup.queues {
        // Check if we should restore this queue
        if let Some(ref filter) = selective_queues {
            if !filter.contains(&queue.name) {
                continue;
            }
        }

        let existing = read_existing_queue(&mut con, &queue.name);
        let had_conflict = existing.is_some();
        let queue_name = queue.name.clone();

        let resolved = match resolve_queue_conflict(existing.as_ref(), queue, conflict_policy) {
            Some(resolved) => resolved,
            None => {
                println!(
                    "  {} queue (already exists, policy=skip): {}",
                    "Skipping".yellow(),
                    queue_name
                );
                skipped_queues += 1;
                continue;
            }
        };

        if had_conflict {
            // Clear existing keys before writing the resolved (overwrite or merged)
            // content, since Redis list/sorted-set writes are append-only.
            let _: () = con.del(format!("celers:queue:{}", resolved.name))?;
            let _: () = con.del(format!("celers:dlq:{}", resolved.name))?;
            let _: () = con.del(format!("celers:delayed:{}", resolved.name))?;
        }

        println!("  Restoring queue: {}", resolved.name.yellow());

        // Restore pending tasks
        for task in &resolved.pending_tasks {
            let _: () = con.rpush(format!("celers:queue:{}", resolved.name), task)?;
            restored_tasks += 1;
        }

        // Restore DLQ tasks
        for task in &resolved.dlq_tasks {
            let _: () = con.rpush(format!("celers:dlq:{}", resolved.name), task)?;
            restored_tasks += 1;
        }

        // Restore delayed tasks
        for task in &resolved.delayed_tasks {
            let _: () = con.zadd(
                format!("celers:delayed:{}", resolved.name),
                task,
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
    if skipped_queues > 0 {
        println!(
            "  Queues skipped (already existed, policy=skip): {}",
            skipped_queues
        );
    }

    Ok(())
}

/// Read the currently-live pending/DLQ/delayed tasks for `name` from the broker, if any
/// exist. Returns `None` when the queue has no tasks in any of the three buckets, which is
/// treated as "does not exist yet" for conflict-resolution purposes.
fn read_existing_queue(con: &mut redis::Connection, name: &str) -> Option<QueueBackup> {
    let pending_tasks: Vec<String> = con
        .lrange(format!("celers:queue:{name}"), 0, -1)
        .unwrap_or_default();
    let dlq_tasks: Vec<String> = con
        .lrange(format!("celers:dlq:{name}"), 0, -1)
        .unwrap_or_default();
    let delayed_tasks: Vec<String> = con
        .zrange(format!("celers:delayed:{name}"), 0, -1)
        .unwrap_or_default();

    if pending_tasks.is_empty() && dlq_tasks.is_empty() && delayed_tasks.is_empty() {
        None
    } else {
        Some(QueueBackup {
            name: name.to_string(),
            queue_type: "fifo".to_string(),
            pending_tasks,
            dlq_tasks,
            delayed_tasks,
        })
    }
}

/// Resolve a conflict between an existing queue already present at the restore target and
/// the incoming queue from a backup archive, according to `policy`.
///
/// Returns `None` when nothing should be written to the target (`policy` is
/// [`ConflictPolicy::Skip`] and `existing` is `Some`). Returns `Some(QueueBackup)` with the
/// full content that should be written to the target otherwise. When `existing` is `None`
/// there is no conflict at all, so `incoming` is always returned unchanged regardless of
/// `policy`.
#[must_use]
pub fn resolve_queue_conflict(
    existing: Option<&QueueBackup>,
    incoming: QueueBackup,
    policy: ConflictPolicy,
) -> Option<QueueBackup> {
    match existing {
        None => Some(incoming),
        Some(existing) => match policy {
            ConflictPolicy::Skip => None,
            ConflictPolicy::Overwrite => Some(incoming),
            ConflictPolicy::Merge => Some(merge_queue(existing, incoming)),
        },
    }
}

/// Merge `incoming` into `existing`: the existing queue's tasks are kept as-is, and any
/// incoming task whose id is not already present is appended. On an id collision the
/// existing (already-live) task wins and the incoming duplicate is dropped.
fn merge_queue(existing: &QueueBackup, incoming: QueueBackup) -> QueueBackup {
    QueueBackup {
        name: existing.name.clone(),
        queue_type: existing.queue_type.clone(),
        pending_tasks: merge_task_lists(&existing.pending_tasks, incoming.pending_tasks),
        dlq_tasks: merge_task_lists(&existing.dlq_tasks, incoming.dlq_tasks),
        delayed_tasks: merge_task_lists(&existing.delayed_tasks, incoming.delayed_tasks),
    }
}

/// Union `incoming` into `existing`, de-duplicating by [`task_identity`]. `existing`
/// entries always come first in the result and always win ties.
fn merge_task_lists(existing: &[String], incoming: Vec<String>) -> Vec<String> {
    let mut seen: HashSet<String> = existing.iter().map(|t| task_identity(t)).collect();
    let mut merged = existing.to_vec();

    for task in incoming {
        let id = task_identity(&task);
        if seen.insert(id) {
            merged.push(task);
        }
    }

    merged
}

/// Best-effort task identity for de-duplication: prefer the embedded `metadata.id` field
/// (present on tasks serialized by `celers-broker-redis` as `SerializedTask` JSON); fall
/// back to the raw serialized string itself when no id can be extracted.
fn task_identity(raw: &str) -> String {
    serde_json::from_str::<serde_json::Value>(raw)
        .ok()
        .and_then(|v| v.get("metadata").and_then(|m| m.get("id")).cloned())
        .and_then(|v| v.as_str().map(str::to_string))
        .unwrap_or_else(|| raw.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task_json(id: &str, timestamp: &str) -> String {
        format!(
            r#"{{"metadata":{{"id":"{id}","name":"demo","state":"Pending","created_at":"{timestamp}","updated_at":"{timestamp}","max_retries":3,"priority":0}},"payload":[]}}"#
        )
    }

    fn sample_metadata(timestamp: &str) -> BackupMetadata {
        BackupMetadata {
            timestamp: timestamp.to_string(),
            broker_type: "redis".to_string(),
            broker_url: "redis://localhost:6379".to_string(),
            queue_count: 0,
            task_count: 0,
            schedule_count: 0,
            version: "0.0.0-test".to_string(),
            incremental: false,
            since_timestamp: None,
        }
    }

    fn sample_schedule(name: &str, cron: &str) -> ScheduleBackup {
        ScheduleBackup {
            name: name.to_string(),
            task: "do_thing".to_string(),
            cron: cron.to_string(),
            queue: "q1".to_string(),
            args: None,
        }
    }

    // ---- diff_backup ----

    #[test]
    fn diff_backup_includes_only_new_and_changed_entries() {
        let previous = Backup {
            metadata: sample_metadata("2026-07-01T00:00:00Z"),
            queues: vec![QueueBackup {
                name: "q1".to_string(),
                queue_type: "fifo".to_string(),
                pending_tasks: vec![
                    task_json("t1", "2026-07-01T00:00:00Z"),
                    task_json("t2", "2026-07-01T00:00:00Z"),
                ],
                dlq_tasks: vec![],
                delayed_tasks: vec![],
            }],
            schedules: vec![sample_schedule("s1", "* * * * *")],
        };

        let current = Backup {
            metadata: sample_metadata("2026-07-12T00:00:00Z"),
            queues: vec![
                QueueBackup {
                    name: "q1".to_string(),
                    queue_type: "fifo".to_string(),
                    pending_tasks: vec![
                        task_json("t1", "2026-07-01T00:00:00Z"), // unchanged
                        task_json("t2", "2026-07-12T00:00:00Z"), // changed (updated_at bumped)
                        task_json("t3", "2026-07-12T00:00:00Z"), // brand new
                    ],
                    dlq_tasks: vec![],
                    delayed_tasks: vec![],
                },
                QueueBackup {
                    name: "q2".to_string(), // brand new queue
                    queue_type: "fifo".to_string(),
                    pending_tasks: vec![task_json("t4", "2026-07-12T00:00:00Z")],
                    dlq_tasks: vec![],
                    delayed_tasks: vec![],
                },
            ],
            schedules: vec![
                sample_schedule("s1", "* * * * *"),   // unchanged
                sample_schedule("s1", "*/5 * * * *"), // same name, cron changed => "changed"
                sample_schedule("s2", "0 * * * *"),   // brand new
            ],
        };

        let delta = diff_backup(&previous, current);

        assert!(delta.metadata.incremental);
        assert_eq!(
            delta.metadata.since_timestamp.as_deref(),
            Some("2026-07-01T00:00:00Z")
        );

        assert_eq!(delta.queues.len(), 2);

        let q1 = delta
            .queues
            .iter()
            .find(|q| q.name == "q1")
            .expect("q1 retained");
        assert_eq!(q1.pending_tasks.len(), 2);
        assert!(q1
            .pending_tasks
            .contains(&task_json("t2", "2026-07-12T00:00:00Z")));
        assert!(q1
            .pending_tasks
            .contains(&task_json("t3", "2026-07-12T00:00:00Z")));
        assert!(!q1
            .pending_tasks
            .contains(&task_json("t1", "2026-07-01T00:00:00Z")));

        let q2 = delta
            .queues
            .iter()
            .find(|q| q.name == "q2")
            .expect("q2 retained");
        assert_eq!(q2.pending_tasks.len(), 1);

        assert_eq!(delta.metadata.queue_count, 2);
        assert_eq!(delta.metadata.task_count, 3);

        assert_eq!(delta.schedules.len(), 2);
        assert!(delta
            .schedules
            .iter()
            .any(|s| s.name == "s1" && s.cron == "*/5 * * * *"));
        assert!(delta.schedules.iter().any(|s| s.name == "s2"));
        assert_eq!(delta.metadata.schedule_count, 2);
    }

    #[test]
    fn diff_backup_drops_queues_with_no_changes() {
        let previous = Backup {
            metadata: sample_metadata("2026-07-01T00:00:00Z"),
            queues: vec![QueueBackup {
                name: "q1".to_string(),
                queue_type: "fifo".to_string(),
                pending_tasks: vec![task_json("t1", "2026-07-01T00:00:00Z")],
                dlq_tasks: vec![],
                delayed_tasks: vec![],
            }],
            schedules: vec![],
        };
        let current = Backup {
            metadata: sample_metadata("2026-07-12T00:00:00Z"),
            queues: vec![QueueBackup {
                name: "q1".to_string(),
                queue_type: "fifo".to_string(),
                pending_tasks: vec![task_json("t1", "2026-07-01T00:00:00Z")], // identical
                dlq_tasks: vec![],
                delayed_tasks: vec![],
            }],
            schedules: vec![],
        };

        let delta = diff_backup(&previous, current);
        assert!(delta.queues.is_empty());
        assert_eq!(delta.metadata.queue_count, 0);
        assert_eq!(delta.metadata.task_count, 0);
    }

    #[test]
    fn diff_backup_keeps_everything_when_no_baseline_queue_exists() {
        let previous = Backup {
            metadata: sample_metadata("2026-07-01T00:00:00Z"),
            queues: vec![],
            schedules: vec![],
        };
        let current = Backup {
            metadata: sample_metadata("2026-07-12T00:00:00Z"),
            queues: vec![QueueBackup {
                name: "q1".to_string(),
                queue_type: "fifo".to_string(),
                pending_tasks: vec![task_json("t1", "2026-07-12T00:00:00Z")],
                dlq_tasks: vec![],
                delayed_tasks: vec![],
            }],
            schedules: vec![],
        };

        let delta = diff_backup(&previous, current);
        assert_eq!(delta.queues.len(), 1);
        assert_eq!(delta.queues[0].pending_tasks.len(), 1);
    }

    // ---- filter_backup_since ----

    #[test]
    fn filter_backup_since_keeps_only_tasks_at_or_after_cutoff() -> Result<()> {
        let since = DateTime::parse_from_rfc3339("2026-07-10T00:00:00Z")?.with_timezone(&Utc);

        let current = Backup {
            metadata: sample_metadata("2026-07-12T00:00:00Z"),
            queues: vec![QueueBackup {
                name: "q1".to_string(),
                queue_type: "fifo".to_string(),
                pending_tasks: vec![
                    task_json("old", "2026-07-01T00:00:00Z"), // before cutoff -> dropped
                    task_json("new", "2026-07-11T00:00:00Z"), // after cutoff -> kept
                    "not-json-garbage".to_string(),           // unparseable -> kept (conservative)
                ],
                dlq_tasks: vec![],
                delayed_tasks: vec![],
            }],
            schedules: vec![sample_schedule("s1", "* * * * *")],
        };

        let filtered = filter_backup_since(current, since);

        assert_eq!(filtered.queues.len(), 1);
        let q1 = &filtered.queues[0];
        assert_eq!(q1.pending_tasks.len(), 2);
        assert!(q1
            .pending_tasks
            .contains(&task_json("new", "2026-07-11T00:00:00Z")));
        assert!(q1.pending_tasks.contains(&"not-json-garbage".to_string()));
        assert!(!q1
            .pending_tasks
            .contains(&task_json("old", "2026-07-01T00:00:00Z")));

        // Schedules always retained by this best-effort filter.
        assert_eq!(filtered.schedules.len(), 1);
        assert!(filtered.metadata.incremental);
        assert_eq!(
            filtered.metadata.since_timestamp.as_deref(),
            Some("2026-07-10T00:00:00+00:00")
        );

        Ok(())
    }

    #[test]
    fn filter_backup_since_drops_queue_left_with_no_tasks() -> Result<()> {
        let since = DateTime::parse_from_rfc3339("2026-07-10T00:00:00Z")?.with_timezone(&Utc);
        let current = Backup {
            metadata: sample_metadata("2026-07-12T00:00:00Z"),
            queues: vec![QueueBackup {
                name: "q1".to_string(),
                queue_type: "fifo".to_string(),
                pending_tasks: vec![task_json("old", "2026-07-01T00:00:00Z")],
                dlq_tasks: vec![],
                delayed_tasks: vec![],
            }],
            schedules: vec![],
        };

        let filtered = filter_backup_since(current, since);
        assert!(filtered.queues.is_empty());

        Ok(())
    }

    // ---- task_identity / task_timestamp ----

    #[test]
    fn task_identity_prefers_metadata_id_and_falls_back_to_raw_string() {
        assert_eq!(
            task_identity(&task_json("abc", "2026-07-01T00:00:00Z")),
            "abc"
        );
        assert_eq!(task_identity("not json"), "not json");
        assert_eq!(
            task_identity(r#"{"no":"metadata field"}"#),
            r#"{"no":"metadata field"}"#
        );
    }

    #[test]
    fn task_timestamp_prefers_updated_at_and_handles_garbage() {
        let expected = DateTime::parse_from_rfc3339("2026-07-05T12:00:00Z")
            .map(|dt| dt.with_timezone(&Utc))
            .ok();
        assert_eq!(
            task_timestamp(&task_json("x", "2026-07-05T12:00:00Z")),
            expected
        );
        assert_eq!(task_timestamp("garbage"), None);
        assert_eq!(task_timestamp(r#"{"metadata":{}}"#), None);
    }

    // ---- resolve_queue_conflict / ConflictPolicy ----

    fn conflicting_queues() -> (QueueBackup, QueueBackup) {
        let existing = QueueBackup {
            name: "q1".to_string(),
            queue_type: "fifo".to_string(),
            pending_tasks: vec![
                task_json("1", "2026-07-01T00:00:00Z"),
                task_json("2", "2026-07-01T00:00:00Z"),
            ],
            dlq_tasks: vec![],
            delayed_tasks: vec![],
        };
        let incoming = QueueBackup {
            name: "q1".to_string(),
            queue_type: "fifo".to_string(),
            pending_tasks: vec![
                task_json("2", "2026-07-12T00:00:00Z"), // overlapping id, different content
                task_json("3", "2026-07-12T00:00:00Z"), // new id
            ],
            dlq_tasks: vec![],
            delayed_tasks: vec![],
        };
        (existing, incoming)
    }

    #[test]
    fn resolve_no_conflict_when_queue_does_not_exist_yet() {
        let (_, incoming) = conflicting_queues();
        for policy in [
            ConflictPolicy::Skip,
            ConflictPolicy::Overwrite,
            ConflictPolicy::Merge,
        ] {
            let resolved = resolve_queue_conflict(None, incoming.clone(), policy);
            assert_eq!(resolved, Some(incoming.clone()));
        }
    }

    #[test]
    fn resolve_skip_leaves_existing_untouched() {
        let (existing, incoming) = conflicting_queues();
        let resolved = resolve_queue_conflict(Some(&existing), incoming, ConflictPolicy::Skip);
        assert_eq!(resolved, None);
    }

    #[test]
    fn resolve_overwrite_replaces_existing_entirely() {
        let (existing, incoming) = conflicting_queues();
        let resolved =
            resolve_queue_conflict(Some(&existing), incoming.clone(), ConflictPolicy::Overwrite);
        assert_eq!(resolved, Some(incoming));
    }

    #[test]
    fn resolve_merge_unions_and_dedupes_by_task_id_with_existing_winning_ties() {
        let (existing, incoming) = conflicting_queues();

        match resolve_queue_conflict(Some(&existing), incoming, ConflictPolicy::Merge) {
            Some(resolved) => {
                assert_eq!(resolved.pending_tasks.len(), 3); // ids 1, 2, 3

                // id "2" keeps the EXISTING version (existing wins ties), not incoming's.
                assert!(resolved
                    .pending_tasks
                    .contains(&task_json("2", "2026-07-01T00:00:00Z")));
                assert!(!resolved
                    .pending_tasks
                    .contains(&task_json("2", "2026-07-12T00:00:00Z")));

                // id "1" (existing-only) and id "3" (incoming-only) are both present.
                assert!(resolved
                    .pending_tasks
                    .contains(&task_json("1", "2026-07-01T00:00:00Z")));
                assert!(resolved
                    .pending_tasks
                    .contains(&task_json("3", "2026-07-12T00:00:00Z")));
            }
            None => panic!("merge policy must always produce content to write"),
        }
    }

    #[test]
    fn merge_falls_back_to_raw_string_identity_for_unparseable_tasks() {
        let existing = QueueBackup {
            name: "q1".to_string(),
            queue_type: "fifo".to_string(),
            pending_tasks: vec!["opaque-a".to_string()],
            dlq_tasks: vec![],
            delayed_tasks: vec![],
        };
        let incoming = QueueBackup {
            name: "q1".to_string(),
            queue_type: "fifo".to_string(),
            pending_tasks: vec!["opaque-a".to_string(), "opaque-b".to_string()],
            dlq_tasks: vec![],
            delayed_tasks: vec![],
        };

        match resolve_queue_conflict(Some(&existing), incoming, ConflictPolicy::Merge) {
            Some(resolved) => {
                assert_eq!(resolved.pending_tasks.len(), 2);
                assert!(resolved.pending_tasks.contains(&"opaque-a".to_string()));
                assert!(resolved.pending_tasks.contains(&"opaque-b".to_string()));
            }
            None => panic!("merge policy must always produce content to write"),
        }
    }

    #[test]
    fn conflict_policy_default_is_skip() {
        assert_eq!(ConflictPolicy::default(), ConflictPolicy::Skip);
    }

    // ---- archive round trip (no broker needed) ----

    #[test]
    fn write_then_read_backup_archive_round_trips() -> Result<()> {
        let dir = tempfile::tempdir().context("create temp dir")?;
        let path = dir.path().join("backup.tar.gz");
        let path_str = path.to_str().context("temp path is valid UTF-8")?;

        let backup = Backup {
            metadata: sample_metadata("2026-07-12T00:00:00Z"),
            queues: vec![QueueBackup {
                name: "q1".to_string(),
                queue_type: "fifo".to_string(),
                pending_tasks: vec![task_json("1", "2026-07-01T00:00:00Z")],
                dlq_tasks: vec![],
                delayed_tasks: vec![],
            }],
            schedules: vec![sample_schedule("s1", "* * * * *")],
        };

        write_backup_archive(path_str, &backup)?;
        let round_tripped = read_backup_archive(path_str)?;

        assert_eq!(round_tripped, backup);
        Ok(())
    }
}
