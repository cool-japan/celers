//! Auto-generated module
//!
//! 🤖 Generated with [SplitRS](https://github.com/cool-japan/splitrs)

use crate::config_layer::CliConfigArgs;
use clap::{Args, Parser, Subcommand};
use clap_complete::Shell;
use std::path::PathBuf;

/// Connection and output options shared by every `inspect` / `control`
/// subcommand.
#[derive(Args, Clone, Debug)]
pub(super) struct ControlArgs {
    /// Broker URL carrying the control channel (e.g. <redis://localhost:6379>)
    #[arg(short, long)]
    pub(super) broker: Option<String>,
    /// Control channel name (defaults to `celers.control`)
    ///
    /// Scope a control plane to one deployment when several share a Redis
    /// instance: a worker only answers commands published to the channel it
    /// subscribes to.
    #[arg(long)]
    pub(super) channel: Option<String>,
    /// Seconds to gather replies before giving up
    #[arg(long, default_value_t = crate::commands::control::DEFAULT_CONTROL_TIMEOUT_SECS)]
    pub(super) timeout: f64,
    /// Only address the worker with this hostname (repeatable)
    #[arg(long = "destination", short = 'd')]
    pub(super) destination: Vec<String>,
    /// Print the raw replies as JSON
    #[arg(long)]
    pub(super) json: bool,
    /// Configuration file path
    #[arg(long)]
    pub(super) config: Option<PathBuf>,
}

/// Read-only inspection of running workers.
#[derive(Subcommand)]
pub(super) enum InspectCommands {
    /// Check which workers are alive
    Ping {
        #[command(flatten)]
        common: ControlArgs,
    },
    /// List the tasks each worker is executing right now
    Active {
        #[command(flatten)]
        common: ControlArgs,
    },
    /// List scheduled (ETA/countdown) tasks held by each worker
    ///
    /// Always empty for CeleRS: ETA tasks wait in the broker's delayed queue,
    /// not in the worker.
    Scheduled {
        #[command(flatten)]
        common: ControlArgs,
    },
    /// List reserved (prefetched) tasks held by each worker
    ///
    /// Always empty for CeleRS: a worker dispatches every message it dequeues
    /// instead of holding a prefetch reserve.
    Reserved {
        #[command(flatten)]
        common: ControlArgs,
    },
    /// List the task ids each worker has been told to revoke
    Revoked {
        #[command(flatten)]
        common: ControlArgs,
    },
    /// List the task types registered in each worker
    Registered {
        #[command(flatten)]
        common: ControlArgs,
    },
    /// Show live counters, pool and broker state for each worker
    Stats {
        #[command(flatten)]
        common: ControlArgs,
    },
    /// Show the depth of the queue each worker consumes
    Queues {
        #[command(flatten)]
        common: ControlArgs,
    },
    /// Show a comprehensive status report for each worker
    Report {
        #[command(flatten)]
        common: ControlArgs,
    },
    /// Show each worker's effective configuration
    Conf {
        #[command(flatten)]
        common: ControlArgs,
    },
    /// Show the circuit-breaker state per task type
    #[command(name = "circuit-breakers")]
    CircuitBreakers {
        #[command(flatten)]
        common: ControlArgs,
    },
}

/// Commands that change what a running worker is doing.
#[derive(Subcommand)]
pub(super) enum ControlCommands {
    /// Check which workers are alive
    Ping {
        #[command(flatten)]
        common: ControlArgs,
    },
    /// Ask workers to stop consuming and drain their in-flight tasks
    Shutdown {
        /// Seconds to let in-flight tasks finish before their messages are
        /// requeued (overrides the worker's configured drain deadline)
        #[arg(long)]
        grace: Option<u64>,
        #[command(flatten)]
        common: ControlArgs,
    },
    /// Revoke one or more task ids
    ///
    /// The revocation is recorded in the queue's durable revoked-id set before
    /// it is broadcast, so it also applies to tasks still sitting in the queue
    /// and to workers that start later — unlike every other control command,
    /// which only reaches workers listening right now.
    Revoke {
        /// Task ids (UUIDs) to revoke
        #[arg(required = true)]
        task_ids: Vec<String>,
        /// Also abort the task if a worker is already running it
        #[arg(long)]
        terminate: bool,
        /// Queue holding the task (defaults to the configured queue)
        ///
        /// The revoked-id set is per queue, so revoking on the wrong one
        /// records a revocation nothing will ever consult.
        #[arg(long)]
        queue: Option<String>,
        #[command(flatten)]
        common: ControlArgs,
    },
    /// Revoke every task whose *name* matches a glob pattern
    ///
    /// Worker-local by construction: a broker queue is keyed by task id, so
    /// nothing broker-side can match a name pattern. Messages already queued
    /// stay queued until a worker sees and drops them.
    #[command(name = "revoke-pattern")]
    RevokePattern {
        /// Glob pattern matched against task names (e.g. `report.*`)
        pattern: String,
        /// Also abort matching tasks that are already running
        #[arg(long)]
        terminate: bool,
        #[command(flatten)]
        common: ControlArgs,
    },
    /// Set or clear a worker-local rate limit for a task type
    #[command(name = "rate-limit")]
    RateLimit {
        /// Task name to limit
        task_name: String,
        /// Maximum tasks per second; omit (or pass --clear) to remove the limit
        #[arg(long)]
        rate: Option<f64>,
        /// Remove the limit instead of setting one
        #[arg(long, conflicts_with = "rate")]
        clear: bool,
        #[command(flatten)]
        common: ControlArgs,
    },
    /// Set or clear the soft/hard time limits for a task type
    #[command(name = "time-limit")]
    TimeLimit {
        /// Task name to limit
        task_name: String,
        /// Soft limit in seconds (a cooperative warning inside the task)
        #[arg(long)]
        soft: Option<u64>,
        /// Hard limit in seconds (the task is killed)
        #[arg(long)]
        hard: Option<u64>,
        #[command(flatten)]
        common: ControlArgs,
    },
    /// Resume consumption of a queue
    #[command(name = "add-consumer")]
    AddConsumer {
        /// Queue name (must be the queue the worker was started on)
        queue: String,
        #[command(flatten)]
        common: ControlArgs,
    },
    /// Stop consuming a queue without stopping the worker
    #[command(name = "cancel-consumer")]
    CancelConsumer {
        /// Queue name (must be the queue the worker was started on)
        queue: String,
        #[command(flatten)]
        common: ControlArgs,
    },
    /// Ask each worker for the depth of its queue
    #[command(name = "queue-length")]
    QueueLength {
        /// Queue name
        queue: String,
        #[command(flatten)]
        common: ControlArgs,
    },
    /// Close a tripped circuit breaker
    #[command(name = "reset-circuit-breaker")]
    ResetCircuitBreaker {
        /// Task name whose breaker to close; omit to close every breaker
        task_name: Option<String>,
        #[command(flatten)]
        common: ControlArgs,
    },
}

#[derive(Subcommand)]
pub(super) enum WorkerMgmtCommands {
    /// List all running workers
    List {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Show detailed statistics for a worker
    Stats {
        /// Worker ID
        worker_id: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Stop a specific worker
    Stop {
        /// Worker ID
        worker_id: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Graceful shutdown
        #[arg(short, long)]
        graceful: bool,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Pause task processing for a worker
    Pause {
        /// Worker ID
        worker_id: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Resume task processing for a worker
    Resume {
        /// Worker ID
        #[arg(short, long)]
        worker_id: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Scale workers to N instances
    Scale {
        /// Target number of workers
        count: usize,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Drain worker (stop accepting new tasks)
    Drain {
        /// Worker ID
        worker_id: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Stream worker logs
    Logs {
        /// Worker ID
        worker_id: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Filter by log level (error, warn, info, debug)
        #[arg(short, long)]
        level: Option<String>,
        /// Follow mode (like tail -f)
        #[arg(short, long)]
        follow: bool,
        /// Number of log lines to show initially
        #[arg(short = 'n', long, default_value_t = 50)]
        lines: usize,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
}
#[derive(Subcommand)]
pub(super) enum QueueCommands {
    /// List all queues (Redis only)
    List {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Purge all tasks from a queue
    Purge {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Confirm deletion
        #[arg(long)]
        confirm: bool,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Show detailed queue statistics
    Stats {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Move all tasks from one queue to another
    Move {
        /// Source queue name
        #[arg(short = 's', long)]
        from: String,
        /// Destination queue name
        #[arg(short = 'd', long)]
        to: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Confirm operation
        #[arg(long)]
        confirm: bool,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Export queue tasks to a JSON file
    Export {
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Output file path
        #[arg(short, long)]
        output: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Import queue tasks from a JSON file
    Import {
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Input file path
        #[arg(short, long)]
        input: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Confirm operation
        #[arg(long)]
        confirm: bool,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Pause queue processing
    Pause {
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Resume queue processing
    Resume {
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
}
#[derive(Subcommand)]
pub(super) enum ScheduleCommands {
    /// List all scheduled tasks
    List {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Add a new scheduled task
    Add {
        /// Schedule name
        name: String,
        /// Task name to execute
        #[arg(short, long)]
        task: String,
        /// Cron expression (e.g., "0 0 * * *" for daily at midnight)
        #[arg(short, long)]
        cron: String,
        /// Queue to send task to
        #[arg(short, long)]
        queue: Option<String>,
        /// Task arguments as JSON
        #[arg(short, long)]
        args: Option<String>,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Remove a scheduled task
    Remove {
        /// Schedule name
        name: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Confirm deletion
        #[arg(long)]
        confirm: bool,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Pause a schedule
    Pause {
        /// Schedule name
        name: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Resume a paused schedule
    Resume {
        /// Schedule name
        name: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Manually trigger a scheduled task
    Trigger {
        /// Schedule name
        name: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Show execution history for a schedule
    History {
        /// Schedule name
        name: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Limit number of history entries
        #[arg(short, long, default_value_t = 20)]
        limit: usize,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
}
#[derive(Subcommand)]
pub(super) enum DlqCommands {
    /// Inspect failed tasks in DLQ
    Inspect {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Maximum number of tasks to show
        #[arg(short, long, default_value_t = 10)]
        limit: usize,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Clear all tasks from DLQ
    Clear {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Confirm deletion
        #[arg(long)]
        confirm: bool,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Replay a specific task from DLQ
    Replay {
        /// Task ID to replay
        task_id: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
}
#[derive(Subcommand)]
pub(super) enum ConfigCommands {
    /// Show the effective configuration after applying every layer
    /// (CLI args > environment > config file > defaults).
    Show {
        /// Configuration override arguments.
        #[command(flatten)]
        args: CliConfigArgs,
        /// Output format for the resolved configuration (toml or yaml).
        #[arg(long, default_value = "toml")]
        format: String,
    },
    /// Reload the configuration from its source file and report what changed.
    Reload {
        /// Configuration override arguments.
        #[command(flatten)]
        args: CliConfigArgs,
    },
}
#[derive(Subcommand)]
pub(super) enum AnalyzeCommands {
    /// Analyze performance bottlenecks
    Bottlenecks {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Analyze failure patterns
    Failures {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Performance profiling (task trend, worker comparison, resource usage)
    #[command(subcommand)]
    Profile(ProfileCommands),
}
#[derive(Subcommand)]
pub(super) enum ProfileCommands {
    /// Profile task execution-time trend for a queue over a rolling window of days
    Task {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Number of days of history to include
        #[arg(short, long, default_value_t = 7)]
        days: u32,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
        /// Output format (table, csv, or html)
        #[arg(short, long, default_value = "table")]
        format: String,
        /// Write output to a file instead of stdout
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Analyze per-worker performance (single worker, or a ranked comparison across all workers)
    Worker {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Worker ID (all active workers are compared when omitted)
        #[arg(short = 'i', long)]
        worker_id: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
        /// Output format (table, csv, or html)
        #[arg(short, long, default_value = "table")]
        format: String,
        /// Write output to a file instead of stdout
        #[arg(short, long)]
        output: Option<String>,
    },
    /// Track resource usage (queue depth, worker count, DLQ size) and load trend for a queue
    Resources {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Number of days of history to include
        #[arg(short, long, default_value_t = 7)]
        days: u32,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
        /// Output format (table, csv, or html)
        #[arg(short, long, default_value = "table")]
        format: String,
        /// Write output to a file instead of stdout
        #[arg(short, long)]
        output: Option<String>,
    },
}
#[derive(Subcommand)]
pub(super) enum ReportCommands {
    /// Generate daily execution report
    Daily {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
        /// Output format (table, csv, or html)
        #[arg(short, long, default_value = "table")]
        format: String,
        /// Write output to a file instead of stdout
        #[arg(short, long)]
        output: Option<String>,
        /// Custom template string for rendering the report
        #[arg(short, long)]
        template: Option<String>,
    },
    /// Generate weekly statistics report
    Weekly {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
        /// Output format (table, csv, or html)
        #[arg(short, long, default_value = "table")]
        format: String,
        /// Write output to a file instead of stdout
        #[arg(short, long)]
        output: Option<String>,
        /// Custom template string for rendering the report
        #[arg(short, long)]
        template: Option<String>,
    },
    /// Show task execution history over a rolling window of days
    History {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Number of days of history to include
        #[arg(short, long, default_value_t = 7)]
        days: u32,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
        /// Output format (table, csv, or html)
        #[arg(short, long, default_value = "table")]
        format: String,
        /// Write output to a file instead of stdout
        #[arg(short, long)]
        output: Option<String>,
        /// Custom template string for rendering the report
        #[arg(short, long)]
        template: Option<String>,
    },
    /// Export statistics for every currently-known worker
    Workers {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
        /// Output format (table, csv, or html)
        #[arg(short, long, default_value = "table")]
        format: String,
        /// Write output to a file instead of stdout
        #[arg(short, long)]
        output: Option<String>,
        /// Custom template string for rendering the report
        #[arg(short, long)]
        template: Option<String>,
    },
    /// Export per-queue depth/health metrics for every discovered queue
    Queues {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
        /// Output format (table, csv, or html)
        #[arg(short, long, default_value = "table")]
        format: String,
        /// Write output to a file instead of stdout
        #[arg(short, long)]
        output: Option<String>,
        /// Custom template string for rendering the report
        #[arg(short, long)]
        template: Option<String>,
    },
}
#[derive(Subcommand)]
pub(super) enum AliasCommands {
    /// List all user-defined aliases.
    List,
    /// Define a new alias (or replace an existing one).
    Add {
        /// Alias name (must not collide with a real command name).
        name: String,
        /// Expansion: the full command line this alias expands to.
        expansion: String,
    },
    /// Remove a user-defined alias.
    Remove {
        /// Alias name to remove.
        name: String,
    },
}
#[derive(Parser)]
#[command(name = "celers")]
#[command(
    version,
    about = "CeleRS - Distributed task queue management CLI",
    long_about = None
)]
pub(crate) struct Cli {
    /// Log level for the CLI (error, warn, info, debug, trace).
    ///
    /// Takes precedence over the `RUST_LOG` environment variable.
    #[arg(long = "log-level", value_name = "LEVEL", global = true)]
    pub(crate) log_level: Option<String>,
    /// Log output format: `text` (human-readable) or `json` (newline-delimited JSON).
    #[arg(long = "log-format", value_enum, default_value_t = crate::logging::LogFormat::Text, global = true)]
    pub(crate) log_format: crate::logging::LogFormat,
    /// Log destination: `stdout` (default), `file:<path>`, or `tcp:<host:port>`.
    #[arg(
        long = "log-sink",
        value_name = "SINK",
        default_value = "stdout",
        global = true
    )]
    pub(crate) log_sink: crate::logging::LogSink,
    #[command(subcommand)]
    pub(super) command: Commands,
}
#[derive(Subcommand)]
pub(super) enum Commands {
    /// Start a worker to process tasks
    #[command(visible_alias = "w")]
    Worker {
        /// Broker URL (e.g., <redis://localhost:6379>)
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Queue mode (fifo or priority)
        #[arg(short, long, default_value = "fifo")]
        mode: String,
        /// Number of concurrent tasks
        #[arg(short, long, default_value_t = 4)]
        concurrency: usize,
        /// Maximum retry attempts
        #[arg(short = 'r', long, default_value_t = 3)]
        max_retries: u32,
        /// Task timeout in seconds
        #[arg(short, long, default_value_t = 300)]
        timeout: u64,
        /// Graceful-shutdown grace period in seconds: how long to wait for
        /// in-flight tasks to finish after Ctrl+C before giving up and
        /// exiting anyway. Overrides `CELERS_WORKER_SHUTDOWN_TIMEOUT_SECS`;
        /// falls back to a 30s default when neither is set.
        #[arg(long)]
        shutdown_timeout: Option<u64>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Check queue status
    Status {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Dead Letter Queue management
    #[command(subcommand)]
    Dlq(DlqCommands),
    /// Replay (re-execute) failed tasks from the Dead Letter Queue
    ///
    /// Re-enqueues failed/DLQ tasks back onto their target queue. Select tasks
    /// by a specific id, a name glob pattern, or all entries; use `--limit` to
    /// cap how many are replayed and `--dry-run` to preview the plan.
    Replay {
        /// Replay a single task by its exact id (UUID).
        #[arg(long, conflicts_with_all = ["pattern", "all"])]
        id: Option<String>,
        /// Replay every task whose name matches this glob (`*`/`?` supported).
        #[arg(long, conflicts_with_all = ["id", "all"])]
        pattern: Option<String>,
        /// Replay all tasks currently in the DLQ.
        #[arg(long, conflicts_with_all = ["id", "pattern"])]
        all: bool,
        /// Maximum number of tasks to replay.
        #[arg(short, long)]
        limit: Option<usize>,
        /// Print the replay plan without enqueuing anything.
        #[arg(long)]
        dry_run: bool,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Simulate synthetic task load against a queue (load testing)
    ///
    /// Generates a deterministic, ordered schedule of synthetic tasks and
    /// enqueues them at a target rate over an optional duration. Choose an
    /// arrival pattern (constant, jittered, or poisson); jittered/poisson
    /// spacing is derived from `--seed` so a given configuration always yields
    /// the same plan. Use `--dry-run` to print the plan without enqueuing.
    #[command(visible_alias = "simulate")]
    Loadtest {
        /// Total number of synthetic tasks to generate.
        #[arg(
            short = 'n',
            long,
            default_value_t = crate::commands::loadtest_cmds::DEFAULT_TOTAL
        )]
        total: usize,
        /// Target arrival rate in tasks per second.
        #[arg(
            short = 'r',
            long,
            default_value_t = crate::commands::loadtest_cmds::DEFAULT_RATE_PER_SEC
        )]
        rate: f64,
        /// Hard cap on the test window in seconds (caps the realised count).
        #[arg(short = 'd', long)]
        duration: Option<u64>,
        /// Task name stamped on every synthetic task.
        #[arg(
            short = 't',
            long,
            default_value = crate::commands::loadtest_cmds::DEFAULT_TASK_NAME
        )]
        task: String,
        /// Synthetic payload size in bytes (1..=1048576).
        #[arg(
            short = 's',
            long,
            default_value_t = crate::commands::loadtest_cmds::DEFAULT_PAYLOAD_BYTES
        )]
        payload_size: usize,
        /// Arrival pattern: constant, jittered, or poisson.
        #[arg(short = 'P', long, default_value = "constant")]
        pattern: String,
        /// Seed for the deterministic jittered/poisson spacing generator.
        #[arg(long, default_value_t = 0)]
        seed: u64,
        /// Maximum jitter as a fraction of one step (jittered pattern only).
        #[arg(long, default_value_t = 0.5)]
        jitter: f64,
        /// Print the load plan without enqueuing anything.
        #[arg(long)]
        dry_run: bool,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Queue management operations
    #[command(subcommand, visible_alias = "q")]
    Queue(QueueCommands),
    /// Task operations
    #[command(subcommand, visible_alias = "t")]
    Task(TaskCommands),
    /// Initialize a configuration file
    Init {
        /// Output path for config file
        #[arg(short, long, default_value = "celers.toml")]
        output: String,
        /// Launch the interactive configuration wizard instead of writing static defaults.
        #[arg(long)]
        wizard: bool,
    },
    /// Inspect and reload the resolved configuration
    #[command(subcommand)]
    Config(ConfigCommands),
    /// Display Prometheus metrics
    Metrics {
        /// Output format (text, json, prometheus)
        #[arg(short, long, default_value = "text")]
        format: String,
        /// Export metrics to file
        #[arg(short, long)]
        output: Option<String>,
        /// Filter metrics by name pattern
        #[arg(short = 'p', long)]
        pattern: Option<String>,
        /// Watch mode - refresh metrics every N seconds
        #[arg(short, long)]
        watch: Option<u64>,
        /// Scrape a remote Prometheus exposition endpoint and render the parsed
        /// metrics as a formatted table (e.g. <http://localhost:9090/metrics>).
        ///
        /// When set, metrics are fetched over HTTP and parsed natively instead
        /// of read from the in-process registry; `--format`/`--output` are
        /// ignored and `--pattern`/`--watch` continue to apply.
        #[arg(short = 'e', long)]
        endpoint: Option<String>,
    },
    /// Live (top-like) monitor of a Prometheus metrics endpoint
    Monitor {
        /// Prometheus exposition endpoint to scrape
        /// (e.g. <http://localhost:9090/metrics>).
        #[arg(
            short = 'e',
            long,
            default_value = crate::commands::metrics_cmds::DEFAULT_METRICS_ENDPOINT
        )]
        endpoint: String,
        /// Refresh interval in seconds
        #[arg(
            short,
            long,
            default_value_t = crate::commands::metrics_cmds::DEFAULT_MONITOR_INTERVAL_SECS
        )]
        interval: u64,
        /// Comma-separated base metric names to surface first
        #[arg(short = 'f', long)]
        focus: Option<String>,
    },
    /// Validate configuration file
    Validate {
        /// Configuration file path
        #[arg(short, long, default_value = "celers.toml")]
        config: String,
        /// Test broker connection
        #[arg(short = 't', long)]
        test_connection: bool,
    },
    /// Generate shell completion scripts
    Completions {
        /// Shell type (bash, zsh, fish, powershell, elvish)
        #[arg(value_enum)]
        shell: Shell,
    },
    /// Generate man pages
    Manpages {
        /// Output directory for man pages
        #[arg(short, long, default_value = "./man")]
        output: String,
    },
    /// Run system health diagnostics
    Health {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Worker management operations
    #[command(subcommand, visible_alias = "wm")]
    WorkerMgmt(WorkerMgmtCommands),
    /// Inspect running workers over the remote control channel
    ///
    /// Broadcasts an inspection request on the CeleRS control channel and
    /// prints every reply that arrives before the timeout. Only workers
    /// started with a control transport answer; "no replies" means nothing was
    /// listening, not that the command failed.
    ///
    /// This is a CeleRS-native protocol: `celery -A app inspect` cannot read a
    /// CeleRS worker, and these subcommands cannot read a Python Celery worker.
    #[command(subcommand)]
    Inspect(InspectCommands),
    /// Send control commands to running workers
    ///
    /// Broadcasts a control request on the CeleRS control channel. See
    /// `celers inspect` for the read-only half; the same caveats about
    /// delivery and Celery interoperability apply.
    #[command(subcommand)]
    Control(ControlCommands),
    /// Automatic problem detection and diagnostics
    Doctor {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Treat warnings as failures too (exit non-zero on any detected
        /// issue, not just critical ones). Useful for gating CI/monitoring
        /// on a fully clean result.
        #[arg(long)]
        strict: bool,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Scheduled task management
    #[command(subcommand)]
    Schedule(ScheduleCommands),
    /// Debug commands for troubleshooting
    #[command(subcommand)]
    Debug(DebugCommands),
    /// Generate execution reports
    #[command(subcommand)]
    Report(ReportCommands),
    /// Analyze system performance and failures
    #[command(subcommand)]
    Analyze(AnalyzeCommands),
    /// Auto-scaling operations
    #[command(subcommand)]
    Autoscale(AutoscaleCommands),
    /// Alert monitoring operations
    #[command(subcommand)]
    Alert(AlertCommands),
    /// Database operations
    #[command(subcommand)]
    Db(DbCommands),
    /// Live dashboard for real-time monitoring
    #[command(visible_alias = "dash")]
    Dashboard {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Refresh interval in seconds
        #[arg(short, long, default_value_t = 1)]
        refresh: u64,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Interactive REPL mode for running multiple commands
    #[command(visible_alias = "i")]
    Interactive {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Create a backup of broker state
    Backup {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Output file path (should end with .tar.gz)
        #[arg(short, long, default_value = "celers-backup.tar.gz")]
        output: String,
        /// Path to a prior backup archive; only entries changed since it was created are included.
        #[arg(long)]
        previous: Option<String>,
        /// RFC 3339 timestamp; only entries changed since this time are included (ignored if --previous is set).
        #[arg(long)]
        since: Option<String>,
        /// Confirm that a backup capturing zero queues and zero schedules is
        /// expected, and write the (empty) archive anyway. Without this,
        /// such a capture fails loudly instead of silently producing a
        /// useless archive (almost always a misconfigured `queues` list).
        #[arg(long)]
        allow_empty: bool,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Restore broker state from backup
    Restore {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Backup file path (.tar.gz)
        #[arg(short, long)]
        input: String,
        /// Dry run mode (validate without restoring)
        #[arg(short, long)]
        dry_run: bool,
        /// Only restore specific queues (comma-separated)
        #[arg(short = 'q', long)]
        queues: Option<String>,
        /// How to resolve a queue that already has live content at the restore target.
        #[arg(long, value_enum, default_value = "skip")]
        conflict_policy: crate::backup::ConflictPolicy,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Visualize task dependencies as an ASCII tree or GraphViz DOT graph.
    Deps {
        /// Path to a queue-export JSON file (see `celers queue export`) or a bare JSON task array.
        #[arg(short, long)]
        from: PathBuf,
        /// Output format: `ascii` (tree) or `dot` (GraphViz).
        #[arg(long, default_value = "ascii")]
        format: String,
        /// Launch an interactive dependency-exploration session.
        #[arg(short, long)]
        interactive: bool,
        /// Task id (or name) to start the interactive session from.
        #[arg(short, long)]
        start: Option<String>,
    },
    /// User-defined command alias management.
    #[command(subcommand, visible_alias = "a")]
    Alias(AliasCommands),
    /// Print the reference table of structured CLI error codes and their suggested fixes.
    ErrorCodes,
    /// Print configured capacity/TTL for the Redis connection pool and
    /// `queue`/`worker` read-path caches (not live hit/reuse ratios).
    ///
    /// A one-shot CLI process runs a single command and exits before
    /// hit/reuse counters on these process-wide caches could accumulate
    /// anything meaningful, so this reports what's configured instead: pool
    /// capacity, per-cache TTL, and current in-process entry counts
    /// (honestly near-zero on a fresh process). For live hit/reuse ratios
    /// accumulated across many commands sharing one process, run `celers
    /// interactive` and use its `stats` command.
    CacheStats,
}
#[derive(Subcommand)]
pub(super) enum AutoscaleCommands {
    /// Start auto-scaling service
    Start {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Show auto-scaling status
    Status {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
}
#[derive(Subcommand)]
pub(super) enum DebugCommands {
    /// Debug task execution details
    Task {
        /// Task ID (UUID)
        task_id: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Debug worker issues
    Worker {
        /// Worker ID
        worker_id: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
}
#[derive(Subcommand)]
pub(super) enum AlertCommands {
    /// Start alert monitoring service
    Start {
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Test webhook notification
    Test {
        /// Webhook URL
        #[arg(short, long)]
        webhook_url: String,
        /// Test message
        #[arg(short, long, default_value = "Test alert from CeleRS CLI")]
        message: String,
    },
}
#[derive(Subcommand)]
pub(super) enum DbCommands {
    /// Test database connection
    TestConnection {
        /// Database URL (`PostgreSQL`, `MySQL`, etc.)
        #[arg(short, long)]
        url: String,
        /// Run latency benchmark
        #[arg(short, long)]
        benchmark: bool,
    },
    /// Check database health
    Health {
        /// Database URL
        #[arg(short, long)]
        url: String,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Show connection pool statistics
    PoolStats {
        /// Database URL
        #[arg(short, long)]
        url: String,
    },
    /// Apply database migrations
    Migrate {
        /// Database URL
        #[arg(short, long)]
        url: String,
        /// Migration action (apply, rollback, status)
        #[arg(short, long, default_value = "apply")]
        action: String,
        /// Number of migrations to rollback (for rollback action)
        #[arg(short, long, default_value_t = 1)]
        steps: usize,
    },
}
#[derive(Subcommand)]
pub(super) enum TaskCommands {
    /// Inspect a specific task by ID
    Inspect {
        /// Task ID (UUID)
        task_id: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Cancel a running or pending task
    Cancel {
        /// Task ID (UUID)
        task_id: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Retry a failed task (from any queue)
    Retry {
        /// Task ID (UUID)
        task_id: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Queue name
        #[arg(short, long)]
        queue: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Show task result from backend
    Result {
        /// Task ID (UUID)
        task_id: String,
        /// Redis backend URL (e.g., <redis://localhost:6379>)
        #[arg(short, long)]
        backend: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Move task to a different queue
    Requeue {
        /// Task ID (UUID)
        task_id: String,
        /// Source queue name
        #[arg(short = 's', long)]
        from: String,
        /// Destination queue name
        #[arg(short = 'd', long)]
        to: String,
        /// Broker URL
        #[arg(short, long)]
        broker: Option<String>,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Show task execution logs
    Logs {
        /// Task ID (UUID)
        task_id: String,
        /// Broker URL (for Redis storage)
        #[arg(short, long)]
        broker: Option<String>,
        /// Number of log lines to show
        #[arg(short, long, default_value_t = 50)]
        limit: usize,
        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
}
