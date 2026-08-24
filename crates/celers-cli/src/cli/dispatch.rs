//! Command dispatch: routes a parsed `Cli` invocation to its command implementation.
//!
//! 🤖 Generated with [SplitRS](https://github.com/cool-japan/splitrs)

use crate::config_layer::{CliConfigArgs, ReloadableConfig};
use clap::CommandFactory;
use clap_complete::generate;
use colored::Colorize;
use std::io;
use std::path::PathBuf;

use super::types::{
    AlertCommands, AliasCommands, AnalyzeCommands, AutoscaleCommands, Cli, Commands,
    ConfigCommands, ControlArgs, ControlCommands, DbCommands, DebugCommands, DlqCommands,
    InspectCommands, ProfileCommands, QueueCommands, ReportCommands, ScheduleCommands,
    TaskCommands, WorkerMgmtCommands,
};
use celers_core::control::{ControlCommand, InspectCommand};

/// Every top-level `celers` subcommand name, in kebab-case (matching clap's
/// default `Subcommand` rename rule). Passed as the `reserved_names` guard to
/// [`crate::aliases::AliasConfig::add`] so a user-defined alias (`celers
/// alias add <name> <expansion>`) can never shadow a real command.
///
/// This intentionally does **not** include the shorter `visible_alias`
/// strings (`w`, `q`, `t`, `wm`, `dash`, `i`, `a`, `simulate`) layered on top
/// of some of these commands -- only the canonical command names themselves
/// are reserved, matching the precedent already set by `Loadtest`'s
/// `"simulate"` alias, which was never added to this kind of list either.
const RESERVED_COMMAND_NAMES: &[&str] = &[
    "worker",
    "status",
    "dlq",
    "replay",
    "loadtest",
    "queue",
    "task",
    "init",
    "config",
    "metrics",
    "monitor",
    "validate",
    "completions",
    "manpages",
    "health",
    "worker-mgmt",
    "inspect",
    "control",
    "doctor",
    "schedule",
    "debug",
    "report",
    "analyze",
    "autoscale",
    "alert",
    "db",
    "dashboard",
    "interactive",
    "backup",
    "restore",
    "deps",
    "alias",
    "error-codes",
    "cache-stats",
];

/// Load configuration applying the standard precedence chain for the
/// per-command path: defaults < config file (TOML/YAML, auto-detected) <
/// environment variables.
///
/// Per-command CLI options (`--broker`, `--queue`, ...) are applied by each
/// command after this call via `unwrap_or`, so the overall precedence remains
/// CLI args > environment > config file > defaults.
fn load_config(config_path: Option<PathBuf>) -> anyhow::Result<crate::config::Config> {
    let args = CliConfigArgs {
        config: config_path,
        ..Default::default()
    };
    crate::config_layer::resolve_config(&args)
}

/// Resolve the shared `inspect`/`control` options through the usual precedence
/// chain (CLI arg > environment > config file > default).
fn control_options(args: ControlArgs) -> anyhow::Result<crate::commands::ControlOptions> {
    let cfg = load_config(args.config)?;
    let mut options = crate::commands::ControlOptions::new(args.broker.unwrap_or(cfg.broker.url));
    options.channel = args.channel;
    options.timeout_secs = args.timeout;
    options.destination = args.destination;
    options.json = args.json;
    Ok(options)
}

/// Parse task ids for a revoke command, naming the offending value.
///
/// Rejecting the whole command on a malformed id is deliberate: revoking a
/// *subset* of what the operator typed, silently, is how the wrong task ends up
/// still running.
fn parse_task_ids(raw: &[String]) -> anyhow::Result<Vec<uuid::Uuid>> {
    raw.iter()
        .map(|value| {
            uuid::Uuid::parse_str(value)
                .map_err(|e| anyhow::anyhow!("'{value}' is not a valid task id (UUID): {e}"))
        })
        .collect()
}

/// Dispatch a parsed [`Cli`] invocation to its command implementation.
///
/// This is the single entry point that routes every top-level and nested
/// subcommand to its concrete implementation in [`crate::commands`], [`crate::backup`],
/// [`crate::interactive`], or [`crate::config_layer`].
pub(crate) async fn dispatch(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Commands::Worker {
            broker,
            queue,
            mode,
            concurrency,
            max_retries,
            timeout,
            shutdown_timeout,
            config,
        } => {
            let cfg = load_config(config)?;
            let broker_url = broker.unwrap_or(cfg.broker.url);
            let queue_name = queue.unwrap_or(cfg.broker.queue);

            crate::commands::start_worker(
                &broker_url,
                &queue_name,
                &mode,
                concurrency,
                max_retries,
                timeout,
                shutdown_timeout,
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

            crate::commands::show_status(&broker_url, &queue_name).await?;
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

                crate::commands::inspect_dlq(&broker_url, &queue_name, limit).await?;
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

                crate::commands::clear_dlq(&broker_url, &queue_name, confirm).await?;
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

                crate::commands::replay_task(&broker_url, &queue_name, &task_id).await?;
            }
        },

        Commands::Replay {
            id,
            pattern,
            all,
            limit,
            dry_run,
            broker,
            queue,
            config,
        } => {
            // Exactly one selector must be supplied. clap's `conflicts_with_all`
            // guarantees mutual exclusivity; here we reject the empty case.
            let filter = match (id, pattern, all) {
                (Some(task_id), None, false) => crate::commands::ReplayFilter::Id(task_id),
                (None, Some(pat), false) => crate::commands::ReplayFilter::Pattern(pat),
                (None, None, true) => crate::commands::ReplayFilter::All,
                (None, None, false) => {
                    anyhow::bail!(
                        "select tasks to replay with exactly one of --id <UUID>, \
                         --pattern <GLOB>, or --all"
                    );
                }
                _ => {
                    // Unreachable in practice thanks to clap conflict rules,
                    // but kept explicit so the selection contract is enforced
                    // even if those attributes change.
                    anyhow::bail!(
                        "--id, --pattern, and --all are mutually exclusive; specify only one"
                    );
                }
            };

            let cfg = load_config(config)?;
            let broker_url = broker.unwrap_or(cfg.broker.url);
            let queue_name = queue.unwrap_or(cfg.broker.queue);

            crate::commands::replay_dlq(&broker_url, &queue_name, &filter, limit, dry_run).await?;
        }

        Commands::Loadtest {
            total,
            rate,
            duration,
            task,
            payload_size,
            pattern,
            seed,
            jitter,
            dry_run,
            broker,
            queue,
            config,
        } => {
            let arrival = crate::commands::ArrivalPattern::parse(&pattern)
                .map_err(|e| anyhow::anyhow!("invalid --pattern: {e}"))?;

            let cfg = load_config(config)?;
            let broker_url = broker.unwrap_or(cfg.broker.url);
            let queue_name = queue.unwrap_or(cfg.broker.queue);

            let load_config = crate::commands::LoadTestConfig {
                total,
                rate_per_sec: rate,
                duration: duration.map(std::time::Duration::from_secs),
                task_name: task,
                payload_size,
                pattern: arrival,
                seed,
                jitter_fraction: jitter,
            };

            crate::commands::run_loadtest(&broker_url, &queue_name, &load_config, dry_run).await?;
        }

        Commands::Queue(queue_cmd) => match queue_cmd {
            QueueCommands::List { broker, config } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::list_queues(&broker_url).await?;
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

                crate::commands::purge_queue(&broker_url, &queue_name, confirm).await?;
            }

            QueueCommands::Stats {
                broker,
                queue,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                crate::commands::queue_stats(&broker_url, &queue_name).await?;
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

                crate::commands::move_queue(&broker_url, &from, &to, confirm).await?;
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

                crate::commands::export_queue(&broker_url, &queue_name, &output).await?;
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

                crate::commands::import_queue(&broker_url, &queue_name, &input, confirm).await?;
            }

            QueueCommands::Pause {
                queue,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                crate::commands::pause_queue(&broker_url, &queue_name).await?;
            }

            QueueCommands::Resume {
                queue,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                crate::commands::resume_queue(&broker_url, &queue_name).await?;
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

                crate::commands::inspect_task(&broker_url, &queue_name, &task_id).await?;
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

                crate::commands::cancel_task(&broker_url, &queue_name, &task_id).await?;
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

                crate::commands::retry_task(&broker_url, &queue_name, &task_id).await?;
            }

            TaskCommands::Result {
                task_id,
                backend,
                config,
            } => {
                let cfg = load_config(config)?;
                let backend_url = backend.unwrap_or(cfg.broker.url);

                crate::commands::show_task_result(&backend_url, &task_id).await?;
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

                crate::commands::requeue_task(&broker_url, &from, &to, &task_id).await?;
            }

            TaskCommands::Logs {
                task_id,
                broker,
                limit,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::show_task_logs(&broker_url, &task_id, limit).await?;
            }
        },

        Commands::Init { output, wizard } => {
            if wizard {
                crate::commands::init_config_wizard(&output).await?;
            } else {
                crate::commands::init_config(&output).await?;
            }
        }

        Commands::Config(config_cmd) => match config_cmd {
            ConfigCommands::Show { args, format } => {
                let resolved = crate::config_layer::resolve_config(&args)?;
                let fmt = match format.to_ascii_lowercase().as_str() {
                    "yaml" | "yml" => crate::config::ConfigFormat::Yaml,
                    _ => crate::config::ConfigFormat::Toml,
                };
                let rendered = resolved.to_string_with_format(fmt)?;
                println!("{}", "Resolved configuration".bold());
                println!(
                    "{}",
                    "(precedence: CLI args > environment > config file > defaults)".dimmed()
                );
                println!();
                println!("{rendered}");
            }

            ConfigCommands::Reload { args } => {
                let mut reloadable = ReloadableConfig::load(args)?;
                match reloadable.source_path() {
                    Some(path) => {
                        let display = path.display().to_string();
                        println!("Reloading configuration from {}", display.cyan());
                    }
                    None => {
                        println!(
                            "{}",
                            "No --config file specified; reloading from environment, \
                             auto-discovered files, and defaults"
                                .yellow()
                        );
                    }
                }
                println!(
                    "Previous broker: {}",
                    reloadable.current().broker.url.dimmed()
                );
                let diff = reloadable.reload()?;
                if diff.is_empty() {
                    println!("{}", "✓ Configuration is up to date (no changes)".green());
                } else {
                    println!(
                        "{}",
                        format!("✓ Reloaded configuration ({} change(s)):", diff.len()).green()
                    );
                    for change in &diff.changes {
                        println!(
                            "  {} {} → {}",
                            format!("{}:", change.field).cyan(),
                            change.old.dimmed(),
                            change.new.yellow()
                        );
                    }
                }

                // Surface the now-active settings so operators can confirm the
                // effective configuration after the reload.
                let active = reloadable.into_inner();
                println!();
                println!("Active broker: {}", active.broker.url.yellow());
                println!("Active queue:  {}", active.broker.queue.yellow());
            }
        },

        Commands::Metrics {
            format,
            output,
            pattern,
            watch,
            endpoint,
        } => {
            if let Some(endpoint_url) = endpoint {
                // Remote scrape: fetch Prometheus text, parse natively, render
                // the parsed model as a formatted table.
                crate::commands::run_metrics(&endpoint_url, pattern.as_deref(), watch).await?;
            } else {
                crate::commands::show_metrics(
                    &format,
                    output.as_deref(),
                    pattern.as_deref(),
                    watch,
                )
                .await?;
            }
        }

        Commands::Monitor {
            endpoint,
            interval,
            focus,
        } => {
            let focus_owned: Vec<String> = focus
                .map(|f| {
                    f.split(',')
                        .map(|s| s.trim().to_string())
                        .filter(|s| !s.is_empty())
                        .collect()
                })
                .unwrap_or_default();
            let focus_refs: Vec<&str> = focus_owned.iter().map(String::as_str).collect();
            crate::commands::run_monitor(&endpoint, interval, &focus_refs).await?;
        }

        Commands::Validate {
            config,
            test_connection,
        } => {
            crate::commands::validate_config(&config, test_connection).await?;
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

            crate::commands::health_check(&broker_url, &queue_name).await?;
        }

        Commands::WorkerMgmt(worker_cmd) => match worker_cmd {
            WorkerMgmtCommands::List { broker, config } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::list_workers(&broker_url).await?;
            }

            WorkerMgmtCommands::Stats {
                worker_id,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::worker_stats(&broker_url, &worker_id).await?;
            }

            WorkerMgmtCommands::Stop {
                worker_id,
                broker,
                graceful,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::stop_worker(&broker_url, &worker_id, graceful).await?;
            }

            WorkerMgmtCommands::Pause {
                worker_id,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::pause_worker(&broker_url, &worker_id).await?;
            }

            WorkerMgmtCommands::Resume {
                worker_id,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::resume_worker(&broker_url, &worker_id).await?;
            }

            WorkerMgmtCommands::Scale {
                count,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::scale_workers(&broker_url, count).await?;
            }

            WorkerMgmtCommands::Drain {
                worker_id,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::drain_worker(&broker_url, &worker_id).await?;
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

                crate::commands::worker_logs(
                    &broker_url,
                    &worker_id,
                    level.as_deref(),
                    follow,
                    lines,
                )
                .await?;
            }
        },

        Commands::Inspect(inspect_cmd) => {
            let (common, command) = match inspect_cmd {
                InspectCommands::Ping { common } => {
                    let options = control_options(common)?;
                    return crate::commands::ping_workers(&options).await;
                }
                InspectCommands::Active { common } => (common, InspectCommand::Active),
                InspectCommands::Scheduled { common } => (common, InspectCommand::Scheduled),
                InspectCommands::Reserved { common } => (common, InspectCommand::Reserved),
                InspectCommands::Revoked { common } => (common, InspectCommand::Revoked),
                InspectCommands::Registered { common } => (common, InspectCommand::Registered),
                InspectCommands::Stats { common } => (common, InspectCommand::Stats),
                InspectCommands::Queues { common } => (common, InspectCommand::QueueInfo),
                InspectCommands::Report { common } => (common, InspectCommand::Report),
                InspectCommands::Conf { common } => (common, InspectCommand::Conf),
                InspectCommands::CircuitBreakers { common } => {
                    (common, InspectCommand::CircuitBreakers)
                }
            };
            let options = control_options(common)?;
            crate::commands::run_inspect(&options, command).await?;
        }

        Commands::Control(control_cmd) => {
            let (common, command) = match control_cmd {
                ControlCommands::Ping { common } => {
                    let options = control_options(common)?;
                    return crate::commands::ping_workers(&options).await;
                }
                ControlCommands::Shutdown { grace, common } => {
                    (common, ControlCommand::shutdown(grace))
                }
                ControlCommands::Revoke {
                    task_ids,
                    terminate,
                    queue,
                    common,
                } => {
                    let parsed = parse_task_ids(&task_ids)?;
                    // Revoke is the one control command with a durable half:
                    // it records the ids in the queue's revoked set before
                    // broadcasting, so it works with no worker running. That
                    // needs the queue, which the shared options do not carry.
                    let cfg = load_config(common.config.clone())?;
                    let queue_name = queue.unwrap_or(cfg.broker.queue);
                    let queue_mode = cfg.broker.mode;
                    let options = control_options(common)?;
                    return crate::commands::revoke_tasks(
                        &options,
                        &queue_name,
                        &queue_mode,
                        &parsed,
                        terminate,
                    )
                    .await;
                }
                ControlCommands::RevokePattern {
                    pattern,
                    terminate,
                    common,
                } => (
                    common,
                    ControlCommand::revoke_by_pattern(pattern, terminate),
                ),
                ControlCommands::RateLimit {
                    task_name,
                    rate,
                    clear,
                    common,
                } => {
                    // `--clear` and a bare `rate-limit <task>` both mean
                    // "remove the limit"; only an explicit `--rate` sets one.
                    let rate = if clear { None } else { rate };
                    (common, ControlCommand::rate_limit(task_name, rate))
                }
                ControlCommands::TimeLimit {
                    task_name,
                    soft,
                    hard,
                    common,
                } => (common, ControlCommand::time_limit(task_name, soft, hard)),
                ControlCommands::AddConsumer { queue, common } => {
                    (common, ControlCommand::add_consumer(queue))
                }
                ControlCommands::CancelConsumer { queue, common } => {
                    (common, ControlCommand::cancel_consumer(queue))
                }
                ControlCommands::QueueLength { queue, common } => {
                    (common, ControlCommand::queue_length(queue))
                }
                ControlCommands::ResetCircuitBreaker { task_name, common } => {
                    (common, ControlCommand::reset_circuit_breaker(task_name))
                }
            };
            let options = control_options(common)?;
            crate::commands::run_control(&options, command).await?;
        }

        Commands::Doctor {
            broker,
            queue,
            strict,
            config,
        } => {
            let cfg = load_config(config)?;
            let broker_url = broker.unwrap_or(cfg.broker.url);
            let queue_name = queue.unwrap_or(cfg.broker.queue);

            crate::commands::doctor(&broker_url, &queue_name, strict).await?;
        }

        Commands::Schedule(schedule_cmd) => match schedule_cmd {
            ScheduleCommands::List { broker, config } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::list_schedules(&broker_url).await?;
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

                crate::commands::add_schedule(
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

                crate::commands::remove_schedule(&broker_url, &name, confirm).await?;
            }

            ScheduleCommands::Pause {
                name,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::pause_schedule(&broker_url, &name).await?;
            }

            ScheduleCommands::Resume {
                name,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::resume_schedule(&broker_url, &name).await?;
            }

            ScheduleCommands::Trigger {
                name,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::trigger_schedule(&broker_url, &name).await?;
            }

            ScheduleCommands::History {
                name,
                broker,
                limit,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::schedule_history(&broker_url, &name, limit).await?;
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

                crate::commands::debug_task(&broker_url, &queue_name, &task_id).await?;
            }

            DebugCommands::Worker {
                worker_id,
                broker,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::debug_worker(&broker_url, &worker_id).await?;
            }
        },

        Commands::Report(report_cmd) => match report_cmd {
            ReportCommands::Daily {
                broker,
                queue,
                config,
                format,
                output,
                template,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                crate::commands::report_daily_formatted(
                    &broker_url,
                    &queue_name,
                    &format,
                    output.as_deref(),
                    template.as_deref(),
                )
                .await?;
            }

            ReportCommands::Weekly {
                broker,
                queue,
                config,
                format,
                output,
                template,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                crate::commands::report_weekly_formatted(
                    &broker_url,
                    &queue_name,
                    &format,
                    output.as_deref(),
                    template.as_deref(),
                )
                .await?;
            }

            ReportCommands::History {
                broker,
                queue,
                days,
                config,
                format,
                output,
                template,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                crate::commands::report_history(
                    &broker_url,
                    &queue_name,
                    days,
                    &format,
                    output.as_deref(),
                    template.as_deref(),
                )
                .await?;
            }

            ReportCommands::Workers {
                broker,
                config,
                format,
                output,
                template,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::report_workers(
                    &broker_url,
                    &format,
                    output.as_deref(),
                    template.as_deref(),
                )
                .await?;
            }

            ReportCommands::Queues {
                broker,
                config,
                format,
                output,
                template,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::report_queues(
                    &broker_url,
                    &format,
                    output.as_deref(),
                    template.as_deref(),
                )
                .await?;
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

                crate::commands::analyze_bottlenecks(&broker_url, &queue_name).await?;
            }

            AnalyzeCommands::Failures {
                broker,
                queue,
                config,
            } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);
                let queue_name = queue.unwrap_or(cfg.broker.queue);

                crate::commands::analyze_failures(&broker_url, &queue_name).await?;
            }

            AnalyzeCommands::Profile(profile_cmd) => match profile_cmd {
                ProfileCommands::Task {
                    broker,
                    queue,
                    days,
                    config,
                    format,
                    output,
                } => {
                    let cfg = load_config(config)?;
                    let broker_url = broker.unwrap_or(cfg.broker.url);
                    let queue_name = queue.unwrap_or(cfg.broker.queue);

                    crate::commands::profile_task(
                        &broker_url,
                        &queue_name,
                        days,
                        &format,
                        output.as_deref(),
                    )
                    .await?;
                }

                ProfileCommands::Worker {
                    broker,
                    worker_id,
                    config,
                    format,
                    output,
                } => {
                    let cfg = load_config(config)?;
                    let broker_url = broker.unwrap_or(cfg.broker.url);

                    crate::commands::profile_worker(
                        &broker_url,
                        worker_id.as_deref(),
                        &format,
                        output.as_deref(),
                    )
                    .await?;
                }

                ProfileCommands::Resources {
                    broker,
                    queue,
                    days,
                    config,
                    format,
                    output,
                } => {
                    let cfg = load_config(config)?;
                    let broker_url = broker.unwrap_or(cfg.broker.url);
                    let queue_name = queue.unwrap_or(cfg.broker.queue);

                    crate::commands::profile_resources(
                        &broker_url,
                        &queue_name,
                        days,
                        &format,
                        output.as_deref(),
                    )
                    .await?;
                }
            },
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

                crate::commands::autoscale_start(&broker_url, &queue_name, cfg.autoscale).await?;
            }

            AutoscaleCommands::Status { broker, config } => {
                let cfg = load_config(config)?;
                let broker_url = broker.unwrap_or(cfg.broker.url);

                crate::commands::autoscale_status(&broker_url, cfg.autoscale).await?;
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

                crate::commands::alert_start(&broker_url, &queue_name, cfg.alerts).await?;
            }

            AlertCommands::Test {
                webhook_url,
                message,
            } => {
                crate::commands::alert_test(&webhook_url, &message).await?;
            }
        },

        Commands::Db(db_cmd) => match db_cmd {
            DbCommands::TestConnection { url, benchmark } => {
                crate::commands::db_test_connection(&url, benchmark).await?;
            }

            DbCommands::Health { url, config } => {
                let _cfg = load_config(config)?;
                crate::commands::db_health(&url).await?;
            }

            DbCommands::PoolStats { url } => {
                crate::commands::db_pool_stats(&url).await?;
            }

            DbCommands::Migrate { url, action, steps } => {
                crate::commands::db_migrate(&url, &action, steps).await?;
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

            crate::commands::run_dashboard(&broker_url, &queue_name, refresh).await?;
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

            crate::interactive::start_interactive(cfg).await?;
        }

        Commands::Backup {
            broker,
            output,
            previous,
            since,
            allow_empty,
            config,
        } => {
            let cfg = load_config(config)?;
            let broker_url = broker.unwrap_or(cfg.broker.url);

            crate::backup::create_backup_incremental(
                &broker_url,
                &output,
                previous.as_deref(),
                since.as_deref(),
                allow_empty,
            )
            .await?;
        }

        Commands::Restore {
            broker,
            input,
            dry_run,
            queues,
            conflict_policy,
            config,
        } => {
            let cfg = load_config(config)?;
            let broker_url = broker.unwrap_or(cfg.broker.url);

            let selective_queues =
                queues.map(|q| q.split(',').map(|s| s.trim().to_string()).collect());

            crate::backup::restore_backup_with_policy(
                &broker_url,
                &input,
                dry_run,
                selective_queues,
                conflict_policy,
            )
            .await?;
        }

        Commands::Deps {
            from,
            format,
            interactive,
            start,
        } => {
            crate::commands::run_deps(&from, &format, interactive, start.as_deref())?;
        }

        Commands::Alias(cmd) => {
            // Mutates only the on-disk `[aliases]` section via
            // `Config::write_aliases_only` rather than re-serializing the
            // fully env/CLI-resolved `load_config(None)` result back to
            // disk: the latter would bake every resolved field (notably
            // `broker.url`, which an environment variable such as a PaaS
            // platform's auto-injected `REDIS_URL` may have supplied only
            // for this one process) permanently into the file (idx 338).
            let cfg = load_config(None)?;
            let mut aliases = cfg.aliases.clone().unwrap_or_default();
            let config_path = crate::config_layer::resolve_config_path(None);
            match cmd {
                AliasCommands::List => {
                    for (name, expansion) in aliases.list() {
                        println!("{name}\t{expansion}");
                    }
                }
                AliasCommands::Add { name, expansion } => {
                    aliases.add(&name, &expansion, RESERVED_COMMAND_NAMES)?;
                    crate::config::Config::write_aliases_only(&config_path, &aliases)?;
                    println!("Added alias '{name}' -> '{expansion}'");
                }
                AliasCommands::Remove { name } => {
                    if aliases.remove(&name) {
                        crate::config::Config::write_aliases_only(&config_path, &aliases)?;
                        println!("Removed alias '{name}'");
                    } else {
                        println!("No such alias: '{name}'");
                    }
                }
            }
        }

        Commands::ErrorCodes => {
            crate::errors::print_error_code_reference();
        }

        Commands::CacheStats => {
            crate::pool::print_cache_pool_snapshot();
        }
    }

    Ok(())
}
