//! `CeleRS` CLI - Command-line interface for distributed task queue management.
//!
//! The `CeleRS` CLI provides comprehensive tools for managing workers, queues, tasks,
//! and monitoring distributed task execution. It supports multiple brokers
//! (Redis, `PostgreSQL`) and provides advanced features like auto-scaling,
//! alerting, and real-time dashboards.
//!
//! # Features
//!
//! - **Worker Management**: Start, stop, pause, resume, and scale workers
//! - **Queue Operations**: List, purge, move, export, and import queues
//! - **Task Management**: Inspect, cancel, retry, and monitor tasks
//! - **DLQ Operations**: Manage failed tasks in the Dead Letter Queue
//! - **Scheduling**: Cron-based task scheduling with Beat scheduler
//! - **Monitoring**: Metrics, live dashboard, health checks
//! - **Debugging**: Diagnostic tools and automatic problem detection
//! - **Database**: Connection testing, health checks, migrations
//!
//! # Quick Start
//!
//! ```bash
//! # Initialize configuration
//! celers init
//!
//! # Start a worker
//! celers worker --broker redis://localhost:6379 --queue my_queue
//!
//! # Check queue status
//! celers status --broker redis://localhost:6379 --queue my_queue
//!
//! # Run health diagnostics
//! celers health --broker redis://localhost:6379
//! ```
//!
//! # Configuration
//!
//! The CLI can be configured via:
//! - Command-line arguments
//! - Configuration files (TOML format)
//! - Environment variables
//!
//! Generate a default configuration:
//! ```bash
//! celers init --output celers.toml
//! ```
//!
//! # Documentation
//!
//! For detailed command documentation, use `--help`:
//! ```bash
//! celers --help
//! celers worker --help
//! celers queue --help
//! ```

mod backup;
mod commands;
mod config;
mod interactive;

use clap::{CommandFactory, Parser, Subcommand};
use clap_complete::{generate, Shell};
use colored::Colorize;
use std::io;
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "celers")]
#[command(version, about = "CeleRS - Distributed task queue management CLI", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start a worker to process tasks
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

    /// Queue management operations
    #[command(subcommand)]
    Queue(QueueCommands),

    /// Task operations
    #[command(subcommand)]
    Task(TaskCommands),

    /// Initialize a configuration file
    Init {
        /// Output path for config file
        #[arg(short, long, default_value = "celers.toml")]
        output: String,
    },

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
    #[command(subcommand)]
    WorkerMgmt(WorkerMgmtCommands),

    /// Automatic problem detection and diagnostics
    Doctor {
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

        /// Configuration file path
        #[arg(long)]
        config: Option<PathBuf>,
    },
}

#[derive(Subcommand)]
enum DlqCommands {
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
enum QueueCommands {
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
enum TaskCommands {
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

#[derive(Subcommand)]
enum WorkerMgmtCommands {
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
enum ScheduleCommands {
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
enum DebugCommands {
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
enum ReportCommands {
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
    },
}

#[derive(Subcommand)]
enum AnalyzeCommands {
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
}

#[derive(Subcommand)]
enum AutoscaleCommands {
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
enum AlertCommands {
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
enum DbCommands {
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

/// Load configuration from file or use defaults
fn load_config(config_path: Option<PathBuf>) -> anyhow::Result<config::Config> {
    if let Some(path) = config_path {
        config::Config::from_file(path)
    } else {
        Ok(config::Config::default_config())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_level(false)
        .init();

    let cli = Cli::parse();

    match cli.command {
        Commands::Worker {
            broker,
            queue,
            mode,
            concurrency,
            max_retries,
            timeout,
            config,
        } => {
            let cfg = load_config(config)?;
            let broker_url = broker.unwrap_or(cfg.broker.url);
            let queue_name = queue.unwrap_or(cfg.broker.queue);

            commands::start_worker(
                &broker_url,
                &queue_name,
                &mode,
                concurrency,
                max_retries,
                timeout,
            )
            .await?;
        }

        Commands::Status {
            broker,
            queue,
            config,
        } => {
            let cfg = load_config(config)?;
            let broker_url = broker.unwrap_or(cfg.broker.url);
            let queue_name = queue.unwrap_or(cfg.broker.queue);

            commands::show_status(&broker_url, &queue_name).await?;
        }

        Commands::Dlq(dlq_cmd) => match dlq_cmd {
            DlqCommands::Inspect {
                broker,
                queue,
                limit,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::inspect_dlq(&broker_url, &queue_name, limit).await?;
            }

            DlqCommands::Clear {
                broker,
                queue,
                confirm,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::clear_dlq(&broker_url, &queue_name, confirm).await?;
            }

            DlqCommands::Replay {
                task_id,
                broker,
                queue,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::replay_task(&broker_url, &queue_name, &task_id).await?;
            }
        },

        Commands::Queue(queue_cmd) => match queue_cmd {
            QueueCommands::List { broker, config } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::list_queues(&broker_url).await?;
            }

            QueueCommands::Purge {
                broker,
                queue,
                confirm,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::purge_queue(&broker_url, &queue_name, confirm).await?;
            }

            QueueCommands::Stats {
                broker,
                queue,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::queue_stats(&broker_url, &queue_name).await?;
            }

            QueueCommands::Move {
                from,
                to,
                broker,
                confirm,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::move_queue(&broker_url, &from, &to, confirm).await?;
            }

            QueueCommands::Export {
                queue,
                output,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::export_queue(&broker_url, &queue_name, &output).await?;
            }

            QueueCommands::Import {
                queue,
                input,
                broker,
                confirm,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::import_queue(&broker_url, &queue_name, &input, confirm).await?;
            }

            QueueCommands::Pause {
                queue,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::pause_queue(&broker_url, &queue_name).await?;
            }

            QueueCommands::Resume {
                queue,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::resume_queue(&broker_url, &queue_name).await?;
            }
        },

        Commands::Task(task_cmd) => match task_cmd {
            TaskCommands::Inspect {
                task_id,
                broker,
                queue,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::inspect_task(&broker_url, &queue_name, &task_id).await?;
            }

            TaskCommands::Cancel {
                task_id,
                broker,
                queue,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::cancel_task(&broker_url, &queue_name, &task_id).await?;
            }

            TaskCommands::Retry {
                task_id,
                broker,
                queue,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::retry_task(&broker_url, &queue_name, &task_id).await?;
            }

            TaskCommands::Result {
                task_id,
                backend,
                config,
            } => {
                let cfg = load_config(config)?;
                let backend_url = backend.unwrap_or(cfg.broker.url);

                commands::show_task_result(&backend_url, &task_id).await?;
            }

            TaskCommands::Requeue {
                task_id,
                from,
                to,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::requeue_task(&broker_url, &from, &to, &task_id).await?;
            }

            TaskCommands::Logs {
                task_id,
                broker,
                limit,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::show_task_logs(&broker_url, &task_id, limit).await?;
            }
        },

        Commands::Init { output } => {
            commands::init_config(&output).await?;
        }

        Commands::Metrics {
            format,
            output,
            pattern,
            watch,
        } => {
            commands::show_metrics(&format, output.as_deref(), pattern.as_deref(), watch).await?;
        }

        Commands::Validate {
            config,
            test_connection,
        } => {
            commands::validate_config(&config, test_connection).await?;
        }

        Commands::Completions { shell } => {
            let mut cmd = Cli::command();
            let bin_name = cmd.get_name().to_string();
            generate(shell, &mut cmd, bin_name, &mut io::stdout());
        }

        Commands::Manpages { output } => {
            use clap_mangen::Man;
            use std::fs;

            // Create output directory if it doesn't exist
            fs::create_dir_all(&output)?;

            let cmd = Cli::command();
            let man = Man::new(cmd);
            let mut buffer = Vec::new();
            man.render(&mut buffer)?;

            let man_path = format!("{output}/celers.1");
            fs::write(&man_path, buffer)?;

            println!("{}", "✓ Man page generated successfully".green());
            println!("  Output: {}", man_path.cyan());
            println!();
            println!("To install:");
            println!("  sudo cp {man_path} /usr/share/man/man1/");
            println!("  sudo mandb");
            println!();
            println!("To view:");
            println!("  man {man_path}");
        }

        Commands::Health {
            broker,
            queue,
            config,
        } => {
            let cfg = load_config(config)?;
            let broker_url = broker.unwrap_or(cfg.broker.url);
            let queue_name = queue.unwrap_or(cfg.broker.queue);

            commands::health_check(&broker_url, &queue_name).await?;
        }

        Commands::WorkerMgmt(worker_cmd) => match worker_cmd {
            WorkerMgmtCommands::List { broker, config } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::list_workers(&broker_url).await?;
            }

            WorkerMgmtCommands::Stats {
                worker_id,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::worker_stats(&broker_url, &worker_id).await?;
            }

            WorkerMgmtCommands::Stop {
                worker_id,
                broker,
                graceful,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::stop_worker(&broker_url, &worker_id, graceful).await?;
            }

            WorkerMgmtCommands::Pause {
                worker_id,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::pause_worker(&broker_url, &worker_id).await?;
            }

            WorkerMgmtCommands::Resume {
                worker_id,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::resume_worker(&broker_url, &worker_id).await?;
            }

            WorkerMgmtCommands::Scale {
                count,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::scale_workers(&broker_url, count).await?;
            }

            WorkerMgmtCommands::Drain {
                worker_id,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::drain_worker(&broker_url, &worker_id).await?;
            }

            WorkerMgmtCommands::Logs {
                worker_id,
                broker,
                level,
                follow,
                lines,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::worker_logs(&broker_url, &worker_id, level.as_deref(), follow, lines)
                    .await?;
            }
        },

        Commands::Doctor {
            broker,
            queue,
            config,
        } => {
            let cfg = load_config(config)?;
            let broker_url = broker.unwrap_or(cfg.broker.url);
            let queue_name = queue.unwrap_or(cfg.broker.queue);

            commands::doctor(&broker_url, &queue_name).await?;
        }

        Commands::Schedule(schedule_cmd) => match schedule_cmd {
            ScheduleCommands::List { broker, config } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::list_schedules(&broker_url).await?;
            }

            ScheduleCommands::Add {
                name,
                task,
                cron,
                queue,
                args,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::add_schedule(
                    &broker_url,
                    &name,
                    &task,
                    &cron,
                    &queue_name,
                    args.as_deref(),
                )
                .await?;
            }

            ScheduleCommands::Remove {
                name,
                broker,
                confirm,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::remove_schedule(&broker_url, &name, confirm).await?;
            }

            ScheduleCommands::Pause {
                name,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::pause_schedule(&broker_url, &name).await?;
            }

            ScheduleCommands::Resume {
                name,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::resume_schedule(&broker_url, &name).await?;
            }

            ScheduleCommands::Trigger {
                name,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::trigger_schedule(&broker_url, &name).await?;
            }

            ScheduleCommands::History {
                name,
                broker,
                limit,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::schedule_history(&broker_url, &name, limit).await?;
            }
        },

        Commands::Debug(debug_cmd) => match debug_cmd {
            DebugCommands::Task {
                task_id,
                broker,
                queue,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::debug_task(&broker_url, &queue_name, &task_id).await?;
            }

            DebugCommands::Worker {
                worker_id,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::debug_worker(&broker_url, &worker_id).await?;
            }
        },

        Commands::Report(report_cmd) => match report_cmd {
            ReportCommands::Daily {
                broker,
                queue,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::report_daily(&broker_url, &queue_name).await?;
            }

            ReportCommands::Weekly {
                broker,
                queue,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::report_weekly(&broker_url, &queue_name).await?;
            }
        },

        Commands::Analyze(analyze_cmd) => match analyze_cmd {
            AnalyzeCommands::Bottlenecks {
                broker,
                queue,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::analyze_bottlenecks(&broker_url, &queue_name).await?;
            }

            AnalyzeCommands::Failures {
                broker,
                queue,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::analyze_failures(&broker_url, &queue_name).await?;
            }
        },

        Commands::Autoscale(autoscale_cmd) => match autoscale_cmd {
            AutoscaleCommands::Start {
                broker,
                queue,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::autoscale_start(&broker_url, &queue_name, cfg.autoscale).await?;
            }

            AutoscaleCommands::Status { broker, config } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                commands::autoscale_status(&broker_url, cfg.autoscale).await?;
            }
        },

        Commands::Alert(alert_cmd) => match alert_cmd {
            AlertCommands::Start {
                broker,
                queue,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                commands::alert_start(&broker_url, &queue_name, cfg.alerts).await?;
            }

            AlertCommands::Test {
                webhook_url,
                message,
            } => {
                commands::alert_test(&webhook_url, &message).await?;
            }
        },

        Commands::Db(db_cmd) => match db_cmd {
            DbCommands::TestConnection { url, benchmark } => {
                commands::db_test_connection(&url, benchmark).await?;
            }

            DbCommands::Health { url, config } => {
                let _cfg = load_config(config)?;
                commands::db_health(&url).await?;
            }

            DbCommands::PoolStats { url } => {
                commands::db_pool_stats(&url).await?;
            }

            DbCommands::Migrate { url, action, steps } => {
                commands::db_migrate(&url, &action, steps).await?;
            }
        },

        Commands::Dashboard {
            broker,
            queue,
            refresh,
            config,
        } => {
            let cfg = load_config(config)?;
            let broker_url = broker.unwrap_or(cfg.broker.url);
            let queue_name = queue.unwrap_or(cfg.broker.queue);

            commands::run_dashboard(&broker_url, &queue_name, refresh).await?;
        }

        Commands::Interactive {
            broker,
            queue,
            config,
        } => {
            let mut cfg = load_config(config)?;

            // Override config with command-line args if provided
            if let Some(broker_url) = broker {
                cfg.broker.url = broker_url;
            }
            if let Some(queue_name) = queue {
                cfg.broker.queue = queue_name;
            }

            interactive::start_interactive(cfg).await?;
        }

        Commands::Backup {
            broker,
            output,
            config,
        } => {
            let cfg = load_config(config)?;
            let broker_url = broker.unwrap_or(cfg.broker.url);

            backup::create_backup(&broker_url, &output).await?;
        }

        Commands::Restore {
            broker,
            input,
            dry_run,
            queues,
            config,
        } => {
            let cfg = load_config(config)?;
            let broker_url = broker.unwrap_or(cfg.broker.url);

            let selective_queues =
                queues.map(|q| q.split(',').map(|s| s.trim().to_string()).collect());

            backup::restore_backup(&broker_url, &input, dry_run, selective_queues).await?;
        }
    }

    Ok(())
}
