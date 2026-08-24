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
    /// Delayed tasks, each paired with its captured `execute_at` score.
    pub delayed_tasks: Vec<DelayedTaskEntry>,
}

/// One delayed-queue entry captured by [`capture_broker_state`]: the raw
/// task payload exactly as stored in Redis, plus the `execute_at`
/// unix-timestamp `ZADD` score it had in the source broker.
///
/// `capture_broker_state` used to `ZRANGE` the delayed ZSET without
/// `WITHSCORES`, discarding the score entirely, so `restore_backup_with_policy`
/// had nothing to restore but the payload and substituted `Utc::now()` as
/// every restored delayed task's score -- making it immediately eligible
/// for dequeue rather than resuming its original schedule. Carrying the
/// score alongside the payload (mirroring the `ExportedEntry` design
/// `commands::queue::export_queue`/`import_queue` already use for the same
/// "don't discard the score" problem) lets restore reproduce it exactly.
///
/// `execute_at` is an `i64` (not `f64`, unlike `ExportedEntry::score`) since
/// every score `RedisBroker` ever `ZADD`s into a delayed queue is an integer
/// unix timestamp cast to `f64` only for the Redis wire protocol (see
/// `RedisBroker::enqueue_at`) -- rounding a `ZRANGE ... WITHSCORES` reply
/// back to `i64` is exact for any realistic timestamp, and keeps this type
/// (and therefore `QueueBackup`) `Eq`, unlike `f64`.
///
/// Deserialization (see the hand-written [`Deserialize`] impl below) also
/// accepts the pre-fix on-disk shape -- a bare JSON string, matching the old
/// `delayed_tasks: Vec<String>` -- so an archive written before this type
/// existed still loads via `celers backup restore` rather than hard-erroring.
/// Serialization always emits the current `{payload, execute_at}` shape.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct DelayedTaskEntry {
    /// The task exactly as stored in Redis.
    pub payload: String,
    /// This entry's `ZADD` score (unix timestamp) at capture time.
    pub execute_at: i64,
}

impl<'de> Deserialize<'de> for DelayedTaskEntry {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Repr {
            /// The shape every `delayed_tasks` entry had before this type
            /// existed: the raw payload only, no captured score.
            Legacy(String),
            /// The current shape.
            Current { payload: String, execute_at: i64 },
        }

        Ok(match Repr::deserialize(deserializer)? {
            // `execute_at: 0` (the Unix epoch) is an honest sentinel for
            // "never captured", not a fabricated value -- a promoter
            // comparing `score <= now()` treats it as immediately eligible,
            // which is the exact same *observable* restore behavior a
            // legacy archive had before this fix (it substituted
            // `Utc::now()` at restore time; either way the task is
            // immediately eligible), without pretending "now, at restore
            // time" was ever the real original schedule.
            Repr::Legacy(payload) => DelayedTaskEntry {
                payload,
                execute_at: 0,
            },
            Repr::Current {
                payload,
                execute_at,
            } => DelayedTaskEntry {
                payload,
                execute_at,
            },
        })
    }
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
/// * `allow_empty` - When `false` (the default from the CLI), a capture that finds zero
///   queues and zero schedules fails loudly instead of writing a silently-empty archive
///   (see below); pass `true` (`--allow-empty`) to confirm an empty backup is expected
///   and write it anyway.
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
///     false,
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
    allow_empty: bool,
) -> Result<()> {
    let is_incremental = previous_backup_path.is_some() || since.is_some();

    if is_incremental {
        println!("{}", "Creating incremental backup...".cyan());
    } else {
        println!("{}", "Creating backup...".cyan());
    }

    let current = capture_broker_state(broker_url)?;

    if is_totally_empty(&current) {
        // A disaster-recovery command that always "succeeds" while
        // capturing nothing is worse than one that fails loudly (idx 314).
        println!(
            "{}",
            "⚠ This backup captured ZERO queues and ZERO schedules."
                .yellow()
                .bold()
        );
        println!(
            "  {}",
            "Queues are enumerated from this deployment's configured `queues` list \
             (celers.toml), not by scanning Redis -- if the broker is not actually \
             empty, check that list includes every queue you expect backed up."
                .dimmed()
        );
        if !allow_empty {
            println!(
                "  {}",
                "Pass --allow-empty to confirm this is expected and write the (empty) \
                 archive anyway."
                    .dimmed()
            );
            anyhow::bail!(
                "refusing to write a backup archive with zero queues and zero schedules \
                 (pass --allow-empty to confirm this is expected)"
            );
        }
    }

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

/// Pure: `true` when `backup` captured nothing at all (no queues, no
/// schedules) -- the condition [`create_backup_incremental`] treats as
/// suspicious enough to fail on by default (see its `allow_empty`
/// parameter / `--allow-empty`).
#[must_use]
fn is_totally_empty(backup: &Backup) -> bool {
    backup.queues.is_empty() && backup.schedules.is_empty()
}

/// Connect to the broker and capture its complete current state as a [`Backup`].
///
/// This performs the live scan (queues, pending/DLQ/delayed tasks, scheduled tasks) that
/// backs every [`create_backup_incremental`] call (full snapshot or incremental alike; it
/// diffs or filters the captured snapshot before it is written out).
///
/// Queues are enumerated from the resolved configuration's `queues` list
/// (the same list `celers.toml`'s `[queues]` array / `Config::default_config`
/// already treat as authoritative for "which queues does this deployment
/// have") rather than by pattern-matching Redis keys: `RedisBroker` names
/// every queue-family key directly off the bare queue name (see
/// `crate::keys`) with no shared prefix at all, so there is no `celers:*`-
/// style pattern a `KEYS`/`SCAN` could reliably discover queues by -- the
/// previous `KEYS celers:queue:*` implementation matched nothing any
/// producer or worker ever wrote (idx 314), silently producing an
/// always-empty backup.
///
/// A configured queue is included when *any* of its three buckets
/// (pending/DLQ/delayed) is non-empty -- not only when its main queue key
/// exists. A queue whose only activity so far is a delayed task (via
/// `RedisBroker::enqueue_at`, with nothing yet pushed to the main queue or
/// DLQ) has no main key at all, so gating on the main key alone used to
/// silently drop that queue -- and its delayed task -- from every backup.
fn capture_broker_state(broker_url: &str) -> Result<Backup> {
    // Connect to Redis
    let client = redis::Client::open(broker_url).context("Failed to create Redis client")?;
    let mut con = client
        .get_connection()
        .context("Failed to connect to Redis")?;

    let cfg = crate::config_layer::resolve_config(&crate::config_layer::CliConfigArgs::default())
        .unwrap_or_else(|_| crate::config::Config::default_config());
    let mut queue_names = cfg.queues;
    queue_names.sort();
    queue_names.dedup();

    let mut queues = Vec::new();
    let mut total_tasks = 0;

    for queue_name in queue_names {
        let main_key = crate::keys::main(&queue_name);
        let main_type: String = redis::cmd("TYPE")
            .arg(&main_key)
            .query(&mut con)
            .unwrap_or_else(|_| "none".to_string());

        let pending_tasks: Vec<String> = match main_type.as_str() {
            "list" => con.lrange(&main_key, 0, -1).unwrap_or_default(),
            "zset" => con.zrange(&main_key, 0, -1).unwrap_or_default(),
            _ => Vec::new(),
        };

        // Get DLQ tasks
        let dlq_tasks: Vec<String> = con
            .lrange(crate::keys::dlq(&queue_name), 0, -1)
            .unwrap_or_default();

        // Get delayed tasks, `WITHSCORES` so the original `execute_at` is
        // captured alongside the payload rather than discarded (see
        // `DelayedTaskEntry`'s docs).
        let delayed_scored: Vec<(String, f64)> = con
            .zrange_withscores(crate::keys::delayed(&queue_name), 0, -1)
            .unwrap_or_default();
        let delayed_tasks: Vec<DelayedTaskEntry> = delayed_scored
            .into_iter()
            .map(|(payload, score)| DelayedTaskEntry {
                payload,
                execute_at: score as i64,
            })
            .collect();

        // A configured name nothing has ever enqueued to yet -- skip it
        // rather than recording an empty queue in every backup. Checked
        // against all three buckets, not just the main queue's `TYPE`: a
        // queue whose only activity so far is a delayed task (via
        // `RedisBroker::enqueue_at`, with nothing ever pushed to the main
        // queue or DLQ yet) has `main_type == "none"` even though it very
        // much has real, capture-worthy data in its `:delayed` ZSET -- the
        // old `main_type == "none"` check alone silently dropped that queue
        // (and its delayed task) from every backup.
        if pending_tasks.is_empty() && dlq_tasks.is_empty() && delayed_tasks.is_empty() {
            continue;
        }

        println!("  Backing up queue: {}", queue_name.yellow());

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
            queue_type: if main_type == "zset" {
                "priority".to_string()
            } else {
                "fifo".to_string()
            },
            pending_tasks,
            dlq_tasks,
            delayed_tasks,
        });
    }

    // Get scheduled tasks. `SCAN` (bounded per-call cost) rather than
    // `KEYS` (a single unbounded O(N) command against the whole keyspace),
    // and the key pattern `commands::schedule::add_schedule` actually
    // writes (`celers:schedule:{name}`, see `crate::keys::schedule`) rather
    // than the old `celers:beat:schedule:*`, which nothing wrote to.
    let mut schedules = Vec::new();
    let schedule_keys: Vec<String> = con
        .scan_match(crate::keys::SCHEDULE_SCAN_PATTERN)
        .map(|iter| iter.filter_map(std::result::Result::ok).collect())
        .unwrap_or_default();

    for key in schedule_keys {
        let schedule_name = key
            .strip_prefix("celers:schedule:")
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
            let delayed_tasks = diff_delayed_task_list(
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

/// [`DelayedTaskEntry`] counterpart of [`diff_task_list`]: retains entries whose `payload` is
/// not byte-identical to some entry in `baseline` (comparing only `payload`, matching
/// `diff_task_list`'s "changed content" semantics for the other two buckets -- a delayed
/// task's `execute_at` changing without its payload changing is not a case this backup format
/// currently distinguishes).
fn diff_delayed_task_list(
    baseline: Option<&[DelayedTaskEntry]>,
    incoming: Vec<DelayedTaskEntry>,
) -> Vec<DelayedTaskEntry> {
    match baseline {
        None => incoming,
        Some(baseline) => {
            let baseline_set: HashSet<&str> = baseline.iter().map(|e| e.payload.as_str()).collect();
            incoming
                .into_iter()
                .filter(|e| !baseline_set.contains(e.payload.as_str()))
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
            let delayed_tasks = retain_delayed_since(queue.delayed_tasks, since);

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

/// [`DelayedTaskEntry`] counterpart of [`retain_since`]: filters by the same embedded
/// `metadata.updated_at`/`created_at` timestamp, read from each entry's `payload`.
fn retain_delayed_since(
    tasks: Vec<DelayedTaskEntry>,
    since: DateTime<Utc>,
) -> Vec<DelayedTaskEntry> {
    tasks
        .into_iter()
        .filter(|entry| match task_timestamp(&entry.payload) {
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

        // Keys matching `crate::keys` -- the real, bare-named layout
        // `RedisBroker` reads/writes -- rather than the old `celers:queue:`/
        // `celers:dlq:`/`celers:delayed:` namespace nothing ever wrote to
        // (idx 314). Restoring to the wrong keys was the write-side twin of
        // `capture_broker_state`'s read-side bug: even a backup that had
        // captured real data would have restored it somewhere the broker
        // could never see.
        let main_key = crate::keys::main(&resolved.name);
        let dlq_key = crate::keys::dlq(&resolved.name);
        let delayed_key = crate::keys::delayed(&resolved.name);

        if had_conflict {
            // Clear existing keys before writing the resolved (overwrite or merged)
            // content, since Redis list/sorted-set writes are append-only.
            let _: () = con.del(&main_key)?;
            let _: () = con.del(&dlq_key)?;
            let _: () = con.del(&delayed_key)?;
        }

        println!("  Restoring queue: {}", resolved.name.yellow());

        // Restore pending tasks. `RPUSH` matches `RedisBroker::enqueue`'s
        // own push direction for list-mode (FIFO) queues.
        for task in &resolved.pending_tasks {
            let _: () = con.rpush(&main_key, task)?;
            restored_tasks += 1;
        }

        // Restore DLQ tasks
        for task in &resolved.dlq_tasks {
            let _: () = con.rpush(&dlq_key, task)?;
            restored_tasks += 1;
        }

        // Restore delayed tasks at their captured `execute_at` score (see
        // `DelayedTaskEntry`'s docs), not "now" -- a restored delayed task
        // resumes its original schedule instead of becoming immediately
        // eligible for dequeue.
        for entry in &resolved.delayed_tasks {
            let _: () = con.zadd(&delayed_key, &entry.payload, entry.execute_at)?;
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
///
/// Keys come from `crate::keys` (idx 314) rather than the `celers:queue:`/
/// `celers:dlq:`/`celers:delayed:` namespace nothing ever wrote to. The main
/// key's actual Redis type is also checked before reading it: an
/// unconditional `LRANGE` against a Priority-mode (ZSET) main queue would
/// fail with `WRONGTYPE`, and the `unwrap_or_default()` below would then
/// silently report "no pending tasks" for a queue that is, in fact, full --
/// making `resolve_queue_conflict` treat a real conflict as "does not exist
/// yet" and overwrite it outright regardless of `conflict_policy`.
fn read_existing_queue(con: &mut redis::Connection, name: &str) -> Option<QueueBackup> {
    let main_key = crate::keys::main(name);
    let main_type: String = redis::cmd("TYPE")
        .arg(&main_key)
        .query(con)
        .unwrap_or_else(|_| "none".to_string());

    let pending_tasks: Vec<String> = match main_type.as_str() {
        "list" => con.lrange(&main_key, 0, -1).unwrap_or_default(),
        "zset" => con.zrange(&main_key, 0, -1).unwrap_or_default(),
        _ => Vec::new(),
    };
    let dlq_tasks: Vec<String> = con
        .lrange(crate::keys::dlq(name), 0, -1)
        .unwrap_or_default();
    let delayed_scored: Vec<(String, f64)> = con
        .zrange_withscores(crate::keys::delayed(name), 0, -1)
        .unwrap_or_default();
    let delayed_tasks: Vec<DelayedTaskEntry> = delayed_scored
        .into_iter()
        .map(|(payload, score)| DelayedTaskEntry {
            payload,
            execute_at: score as i64,
        })
        .collect();

    if pending_tasks.is_empty() && dlq_tasks.is_empty() && delayed_tasks.is_empty() {
        None
    } else {
        Some(QueueBackup {
            name: name.to_string(),
            queue_type: if main_type == "zset" {
                "priority".to_string()
            } else {
                "fifo".to_string()
            },
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
        delayed_tasks: merge_delayed_task_lists(&existing.delayed_tasks, incoming.delayed_tasks),
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

/// [`DelayedTaskEntry`] counterpart of [`merge_task_lists`]: de-duplicates by
/// [`task_identity`] of each entry's `payload`. `existing` entries (including their captured
/// `execute_at`) always come first and always win ties, matching `merge_task_lists`'s
/// "already-live data wins" contract.
fn merge_delayed_task_lists(
    existing: &[DelayedTaskEntry],
    incoming: Vec<DelayedTaskEntry>,
) -> Vec<DelayedTaskEntry> {
    let mut seen: HashSet<String> = existing.iter().map(|e| task_identity(&e.payload)).collect();
    let mut merged = existing.to_vec();

    for entry in incoming {
        let id = task_identity(&entry.payload);
        if seen.insert(id) {
            merged.push(entry);
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
    use celers_core::Broker;

    /// Regression test for idx 314: `capture_broker_state` used to scan
    /// `celers:queue:*`/`celers:dlq:*`/`celers:delayed:*`, a namespace
    /// nothing wrote to, so every backup always captured zero queues and
    /// zero tasks regardless of what was actually enqueued. This proves a
    /// backup taken right after enqueuing a real task via `RedisBroker`
    /// actually captures it -- using whichever queue name the resolved
    /// configuration's `queues` list gives (the exact source
    /// `capture_broker_state` itself consults), rather than a hardcoded
    /// name, so this test agrees with the function under test regardless of
    /// this repository's `celers.toml` contents.
    #[tokio::test]
    async fn backup_captures_real_data_enqueued_via_redis_broker() {
        let broker_url = "redis://127.0.0.1:6379";

        let cfg =
            crate::config_layer::resolve_config(&crate::config_layer::CliConfigArgs::default())
                .unwrap_or_else(|_| crate::config::Config::default_config());
        let queue_name =
            cfg.queues.first().cloned().expect(
                "resolved config always has at least one queue (default_config's fallback)",
            );

        let broker =
            celers_broker_redis::RedisBroker::new(broker_url, &queue_name).expect("broker");

        // A marker task, so this test finds *its own* data even if the
        // shared queue already has unrelated content from other
        // concurrently-running processes against the same Redis instance.
        let marker_name = format!("backup-marker-{}", uuid::Uuid::new_v4());
        broker
            .enqueue(celers_core::SerializedTask::new(
                marker_name.clone(),
                Vec::new(),
            ))
            .await
            .expect("enqueue marker task");

        let output_path = std::env::temp_dir().join(format!(
            "celers_cli_backup_test_{}.tar.gz",
            uuid::Uuid::new_v4()
        ));
        let output_path_str = output_path.to_str().expect("utf8 temp path");

        create_backup_incremental(broker_url, output_path_str, None, None, false)
            .await
            .expect("backup");

        let backup = read_backup_archive(output_path_str).expect("read back the archive");

        assert!(
            backup.metadata.task_count > 0,
            "a backup taken right after enqueuing a real task via RedisBroker must not report \
             zero tasks"
        );

        let queue_backup = backup
            .queues
            .iter()
            .find(|q| q.name == queue_name)
            .unwrap_or_else(|| {
                panic!(
                    "queue '{queue_name}' (from the resolved config) must be present in the backup"
                )
            });
        let marker_entry = queue_backup
            .pending_tasks
            .iter()
            .find(|raw| raw.contains(&marker_name))
            .cloned();
        assert!(
            marker_entry.is_some(),
            "the specific task just enqueued via RedisBroker must be present in the backup"
        );

        // Targeted cleanup: LREM removes only a byte-exact match, so this
        // cannot disturb unrelated content another concurrently-running
        // test/process might have in the same shared queue.
        if let Some(raw) = marker_entry {
            let client = redis::Client::open(broker_url).expect("client");
            let mut conn = client
                .get_multiplexed_async_connection_with_config(
                    &crate::pool::async_connection_config(),
                )
                .await
                .expect("conn");
            let _: usize = redis::cmd("LREM")
                .arg(crate::keys::main(&queue_name))
                .arg(1)
                .arg(&raw)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
        }
        let _ = std::fs::remove_file(&output_path);
    }

    /// Regression test for two delayed-task bugs found alongside idx 314:
    /// (1) `capture_broker_state` used to skip a queue entirely unless its
    /// *main* key existed, so a queue whose only activity is a delayed task
    /// (nothing yet pushed to the main queue) was silently dropped from
    /// every backup regardless of its `:delayed` ZSET; (2) even when
    /// captured, delayed tasks were `ZRANGE`d without `WITHSCORES`,
    /// discarding the real `execute_at` entirely. This proves a delayed
    /// task enqueued via `RedisBroker::enqueue_at` -- with nothing else ever
    /// pushed to that queue -- is captured at all, and with its actual
    /// score, not just its payload.
    #[tokio::test]
    async fn backup_captures_delayed_task_execute_at_score() {
        let broker_url = "redis://127.0.0.1:6379";

        let cfg =
            crate::config_layer::resolve_config(&crate::config_layer::CliConfigArgs::default())
                .unwrap_or_else(|_| crate::config::Config::default_config());
        let queue_name =
            cfg.queues.first().cloned().expect(
                "resolved config always has at least one queue (default_config's fallback)",
            );

        let broker =
            celers_broker_redis::RedisBroker::new(broker_url, &queue_name).expect("broker");

        let marker_name = format!("backup-delayed-marker-{}", uuid::Uuid::new_v4());
        // Far enough in the future that it can never be confused with "now".
        let execute_at = chrono::Utc::now().timestamp() + 100_000;
        broker
            .enqueue_at(
                celers_core::SerializedTask::new(marker_name.clone(), Vec::new()),
                execute_at,
            )
            .await
            .expect("enqueue_at marker delayed task");

        let output_path = std::env::temp_dir().join(format!(
            "celers_cli_backup_delayed_test_{}.tar.gz",
            uuid::Uuid::new_v4()
        ));
        let output_path_str = output_path.to_str().expect("utf8 temp path");

        create_backup_incremental(broker_url, output_path_str, None, None, false)
            .await
            .expect("backup");

        let backup = read_backup_archive(output_path_str).expect("read back the archive");

        let queue_backup = backup
            .queues
            .iter()
            .find(|q| q.name == queue_name)
            .unwrap_or_else(|| {
                panic!(
                    "queue '{queue_name}' (from the resolved config) must be present in the backup"
                )
            });
        let marker_entry = queue_backup
            .delayed_tasks
            .iter()
            .find(|e| e.payload.contains(&marker_name))
            .cloned();

        match &marker_entry {
            Some(entry) => assert_eq!(
                entry.execute_at, execute_at,
                "capture_broker_state must preserve the delayed task's real execute_at score, \
                 not discard it"
            ),
            None => panic!(
                "the specific delayed task just enqueued via RedisBroker::enqueue_at must be \
                 present in the backup"
            ),
        }

        // Targeted cleanup: ZREM removes only a byte-exact match, so this
        // cannot disturb unrelated content another concurrently-running
        // test/process might have in the same shared queue.
        if let Some(entry) = marker_entry {
            let client = redis::Client::open(broker_url).expect("client");
            let mut conn = client
                .get_multiplexed_async_connection_with_config(
                    &crate::pool::async_connection_config(),
                )
                .await
                .expect("conn");
            let _: usize = redis::cmd("ZREM")
                .arg(crate::keys::delayed(&queue_name))
                .arg(&entry.payload)
                .query_async(&mut conn)
                .await
                .unwrap_or(0);
        }
        let _ = std::fs::remove_file(&output_path);
    }

    /// Regression test: `restore_backup_with_policy` used to substitute
    /// `Utc::now()` as every restored delayed task's `ZADD` score, making a
    /// restored delayed task immediately eligible instead of resuming its
    /// original schedule. This proves a `DelayedTaskEntry`'s captured
    /// `execute_at` actually reaches the ZSET score on restore, using a
    /// UUID-scoped queue name (restore writes to whatever name the backup
    /// archive specifies, unlike capture, so this does not depend on the
    /// resolved config's queue list).
    #[tokio::test]
    async fn restore_preserves_delayed_task_execute_at_score() {
        let broker_url = "redis://127.0.0.1:6379";
        let queue_name = format!("test-restore-delayed-{}", uuid::Uuid::new_v4());
        let payload = task_json("restore-delayed", "2026-08-01T00:00:00Z");
        // Far enough in the future that it can never be confused with "now".
        let execute_at = chrono::Utc::now().timestamp() + 100_000;

        let backup = Backup {
            metadata: sample_metadata("2026-08-01T00:00:00Z"),
            queues: vec![QueueBackup {
                name: queue_name.clone(),
                queue_type: "fifo".to_string(),
                pending_tasks: vec![],
                dlq_tasks: vec![],
                delayed_tasks: vec![DelayedTaskEntry {
                    payload: payload.clone(),
                    execute_at,
                }],
            }],
            schedules: vec![],
        };

        let archive_path = std::env::temp_dir().join(format!(
            "celers_cli_restore_delayed_test_{}.tar.gz",
            uuid::Uuid::new_v4()
        ));
        let archive_path_str = archive_path.to_str().expect("utf8 temp path");
        write_backup_archive(archive_path_str, &backup).expect("write archive");

        restore_backup_with_policy(
            broker_url,
            archive_path_str,
            false,
            None,
            ConflictPolicy::Overwrite,
        )
        .await
        .expect("restore");

        let client = redis::Client::open(broker_url).expect("client");
        let mut conn = client
            .get_multiplexed_async_connection_with_config(&crate::pool::async_connection_config())
            .await
            .expect("conn");
        let score: Option<f64> = redis::cmd("ZSCORE")
            .arg(crate::keys::delayed(&queue_name))
            .arg(&payload)
            .query_async(&mut conn)
            .await
            .expect("zscore");

        assert_eq!(
            score,
            Some(execute_at as f64),
            "restore must write the captured execute_at as the ZADD score, not `now()`"
        );

        let _: () = redis::cmd("DEL")
            .arg(crate::keys::delayed(&queue_name))
            .query_async(&mut conn)
            .await
            .unwrap_or(());
        let _ = std::fs::remove_file(&archive_path);
    }

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

    // ---- is_totally_empty / --allow-empty ----

    /// Regression test for idx 314's suggested `--allow-empty` follow-up:
    /// `is_totally_empty` -- the condition `create_backup_incremental` fails
    /// on by default -- must be `true` only when *both* buckets are empty,
    /// not either one alone.
    #[test]
    fn is_totally_empty_true_only_when_no_queues_and_no_schedules() {
        let empty = Backup {
            metadata: sample_metadata("2026-08-01T00:00:00Z"),
            queues: vec![],
            schedules: vec![],
        };
        assert!(is_totally_empty(&empty));

        let with_queue = Backup {
            queues: vec![QueueBackup {
                name: "q".to_string(),
                queue_type: "fifo".to_string(),
                pending_tasks: vec![task_json("1", "2026-07-01T00:00:00Z")],
                dlq_tasks: vec![],
                delayed_tasks: vec![],
            }],
            ..empty.clone()
        };
        assert!(!is_totally_empty(&with_queue));

        let with_schedule = Backup {
            schedules: vec![sample_schedule("s1", "* * * * *")],
            ..empty
        };
        assert!(!is_totally_empty(&with_schedule));
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

    // ---- DelayedTaskEntry helpers (diff / retain / merge) ----

    #[test]
    fn diff_delayed_task_list_with_no_baseline_retains_everything() {
        let incoming = vec![DelayedTaskEntry {
            payload: task_json("t1", "2026-07-01T00:00:00Z"),
            execute_at: 1_000,
        }];
        assert_eq!(diff_delayed_task_list(None, incoming.clone()), incoming);
    }

    #[test]
    fn diff_delayed_task_list_drops_unchanged_payload_keeps_new() {
        let baseline = vec![DelayedTaskEntry {
            payload: task_json("t1", "2026-07-01T00:00:00Z"),
            execute_at: 1_000,
        }];
        let incoming = vec![
            DelayedTaskEntry {
                payload: task_json("t1", "2026-07-01T00:00:00Z"), // byte-identical -> dropped
                execute_at: 1_000,
            },
            DelayedTaskEntry {
                payload: task_json("t2", "2026-07-12T00:00:00Z"), // new -> kept
                execute_at: 2_000,
            },
        ];

        let diffed = diff_delayed_task_list(Some(&baseline), incoming);
        assert_eq!(diffed.len(), 1);
        assert_eq!(diffed[0].execute_at, 2_000);
    }

    #[test]
    fn retain_delayed_since_filters_by_embedded_payload_timestamp() {
        let since = DateTime::parse_from_rfc3339("2026-07-05T00:00:00Z")
            .expect("valid rfc3339")
            .with_timezone(&Utc);
        let tasks = vec![
            DelayedTaskEntry {
                payload: task_json("old", "2026-07-01T00:00:00Z"), // before cutoff -> dropped
                execute_at: 1,
            },
            DelayedTaskEntry {
                payload: task_json("new", "2026-07-11T00:00:00Z"), // after cutoff -> kept
                execute_at: 2,
            },
        ];

        let retained = retain_delayed_since(tasks, since);
        assert_eq!(retained.len(), 1);
        assert_eq!(retained[0].execute_at, 2);
    }

    #[test]
    fn merge_delayed_task_lists_dedupes_by_identity_and_existing_wins() {
        let existing = vec![DelayedTaskEntry {
            payload: task_json("1", "2026-07-01T00:00:00Z"),
            execute_at: 10,
        }];
        let incoming = vec![
            DelayedTaskEntry {
                payload: task_json("1", "2026-07-12T00:00:00Z"), // overlapping id
                execute_at: 20,
            },
            DelayedTaskEntry {
                payload: task_json("2", "2026-07-12T00:00:00Z"), // new id
                execute_at: 30,
            },
        ];

        let merged = merge_delayed_task_lists(&existing, incoming);
        assert_eq!(merged.len(), 2);
        assert_eq!(
            merged[0].execute_at, 10,
            "existing entry (and its captured execute_at) must win on id collision"
        );
        assert_eq!(merged[1].execute_at, 30);
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

    // ---- DelayedTaskEntry backward-compatible deserialization ----

    /// `DelayedTaskEntry` must deserialize both its current shape and the
    /// pre-fix shape (a bare JSON string, matching the old
    /// `delayed_tasks: Vec<String>`) -- otherwise every backup archive ever
    /// written before this type existed would hard-fail to load.
    #[test]
    fn delayed_task_entry_deserializes_current_and_legacy_shapes() {
        let current: DelayedTaskEntry =
            serde_json::from_str(r#"{"payload":"raw json","execute_at":1234}"#)
                .expect("current shape must deserialize");
        assert_eq!(current.payload, "raw json");
        assert_eq!(current.execute_at, 1234);

        let legacy: DelayedTaskEntry = serde_json::from_str(r#""raw json""#)
            .expect("legacy bare-string shape must deserialize");
        assert_eq!(legacy.payload, "raw json");
        assert_eq!(
            legacy.execute_at, 0,
            "a legacy entry's never-captured execute_at must deserialize to the honest \
             sentinel 0"
        );
    }

    /// Serialization always emits the current shape, even for an entry
    /// constructed with `execute_at: 0` -- round-tripping never regresses a
    /// freshly captured entry back down to the legacy bare-string shape.
    #[test]
    fn delayed_task_entry_always_serializes_current_shape() {
        let entry = DelayedTaskEntry {
            payload: "raw json".to_string(),
            execute_at: 0,
        };
        let json = serde_json::to_string(&entry).expect("serialize");
        assert!(json.contains("\"payload\""));
        assert!(json.contains("\"execute_at\""));
    }

    /// Build a `.tar.gz` backup archive from hand-crafted `backup.json`
    /// content, bypassing `write_backup_archive` (which -- now that
    /// `DelayedTaskEntry` exists -- always emits the current shape). This is
    /// the only way to reproduce a genuinely pre-fix archive for
    /// [`read_backup_archive_loads_a_legacy_archive_with_bare_string_delayed_tasks`].
    fn write_raw_json_archive(output_path: &str, json: &str) {
        let mut tar_buf = Vec::new();
        {
            let cursor = std::io::Cursor::new(&mut tar_buf);
            let mut tar_writer = TarWriter::new(cursor);
            tar_writer
                .add_file("backup.json", json.as_bytes())
                .expect("add file to tar");
            tar_writer.into_inner().expect("finish tar");
        }
        let compressed = gzip_compress(&tar_buf, 6).expect("gzip compress");
        std::fs::write(output_path, &compressed).expect("write archive file");
    }

    /// Regression test (found during advisor review of the delayed-task
    /// score fix): a real backup archive captured before `DelayedTaskEntry`
    /// existed has `delayed_tasks` as a bare `Vec<String>` on disk.
    /// `read_backup_archive` must still load it -- "the fix for a data-loss
    /// bug makes last week's backup unrestorable" would be a strictly worse
    /// outcome than the bug it fixed.
    #[test]
    fn read_backup_archive_loads_a_legacy_archive_with_bare_string_delayed_tasks() {
        let legacy_payload = task_json("legacy-1", "2026-07-01T00:00:00Z");
        let json_value = serde_json::json!({
            "metadata": {
                "timestamp": "2026-07-01T00:00:00Z",
                "broker_type": "redis",
                "broker_url": "redis://localhost:6379",
                "queue_count": 1,
                "task_count": 1,
                "schedule_count": 0,
                "version": "0.0.0-legacy",
                "incremental": false,
                "since_timestamp": null,
            },
            "queues": [{
                "name": "legacy-queue",
                "queue_type": "fifo",
                "pending_tasks": [],
                "dlq_tasks": [],
                // The pre-fix shape: a bare array of raw payload strings,
                // not `{"payload": ..., "execute_at": ...}` objects.
                "delayed_tasks": [legacy_payload],
            }],
            "schedules": [],
        });
        let json = serde_json::to_string_pretty(&json_value).expect("serialize legacy json");

        let archive_path = std::env::temp_dir().join(format!(
            "celers_cli_legacy_backup_test_{}.tar.gz",
            uuid::Uuid::new_v4()
        ));
        let archive_path_str = archive_path.to_str().expect("utf8 temp path");
        write_raw_json_archive(archive_path_str, &json);

        let backup = read_backup_archive(archive_path_str).unwrap_or_else(|e| {
            panic!(
                "a pre-fix archive with delayed_tasks as a bare Vec<String> must still load: {e}"
            )
        });

        assert_eq!(backup.queues.len(), 1);
        let queue = &backup.queues[0];
        assert_eq!(queue.delayed_tasks.len(), 1);
        assert_eq!(queue.delayed_tasks[0].payload, legacy_payload);
        assert_eq!(
            queue.delayed_tasks[0].execute_at, 0,
            "a legacy entry's execute_at (never captured by the old archive format) must \
             deserialize to the honest sentinel 0, not fail or fabricate a value"
        );

        let _ = std::fs::remove_file(&archive_path);
    }
}
