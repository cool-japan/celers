mod commands;
mod config;

use clap::{Parser, Subcommand};
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
        /// Broker URL (e.g., redis://localhost:6379)
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

        /// Redis backend URL (e.g., redis://localhost:6379)
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
        },

        Commands::Init { output } => {
            commands::init_config(&output).await?;
        }

        Commands::Metrics {
            format,
            output,
            pattern,
        } => {
            commands::show_metrics(&format, output.as_deref(), pattern.as_deref()).await?;
        }

        Commands::Validate {
            config,
            test_connection,
        } => {
            commands::validate_config(&config, test_connection).await?;
        }
    }

    Ok(())
}
