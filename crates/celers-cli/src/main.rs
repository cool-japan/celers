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

mod aliases;
mod backup;
mod cache;
mod cli;
mod command_utils;
mod commands;
mod config;
mod config_layer;
mod config_validation;
mod errors;
mod interactive;
mod keys;
mod logging;
mod pool;
mod row_ext;
mod smart_defaults;
mod tls_mode;

use clap::Parser;
use colored::Colorize;

use cli::Cli;
use config_layer::CliConfigArgs;

/// Apply alias expansion to an already-split process argument vector.
///
/// `raw_args[0]` is treated as the binary name and is always preserved
/// unexpanded; only `raw_args[1..]` is checked against `aliases`. This is a
/// pure function over its inputs (no filesystem or environment access), which
/// makes it directly unit-testable, unlike [`expand_aliases`] which performs
/// config discovery I/O.
fn apply_alias_expansion(raw_args: &[String], aliases: &aliases::AliasConfig) -> Vec<String> {
    let Some((bin, rest)) = raw_args.split_first() else {
        return raw_args.to_vec();
    };
    let mut expanded = vec![bin.clone()];
    expanded.extend(aliases.resolve(rest));
    expanded
}

/// Expand user-defined aliases (`celers alias add|remove|list`) in the raw
/// process arguments, ahead of `clap` parsing.
///
/// Aliases are loaded via the same config *discovery* mechanism
/// `cli::dispatch`'s per-command config loading uses (auto-detected
/// `celers.toml`/`.yaml`/`.yml` in the current directory, falling back to
/// built-in defaults): an explicit `--config <path>` cannot be honored here
/// since it is only a per-subcommand `clap` field, not knowable until after
/// parsing.
///
/// A malformed or unreadable config file must never block ordinary usage
/// (e.g. `celers --help`), so any resolution failure is swallowed here and
/// alias expansion is simply skipped; the real config error still surfaces
/// normally once the selected command loads its configuration.
fn expand_aliases(raw_args: &[String]) -> Vec<String> {
    let aliases = config_layer::resolve_config(&CliConfigArgs::default())
        .ok()
        .and_then(|cfg| cfg.aliases)
        .unwrap_or_default();
    apply_alias_expansion(raw_args, &aliases)
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // First thing, before anything can build a `rustls::ClientConfig`.
    //
    // No rustls crypto-provider *feature* is enabled anywhere in this
    // workspace, so a bare `rustls::ClientConfig::builder()` — which the
    // `redis` crate uses for `rediss://` — panics unless a process-default
    // provider is already installed. This binary opens `redis::Client` values
    // directly in a dozen commands (`queue`, `backup`, `monitor`, ...), not
    // only through `celers-broker-redis`'s constructors, so installing the
    // provider once here is what covers all of them. Whoever installs first
    // wins; calling it again from a broker constructor is a no-op.
    celers_broker_redis::install_pure_tls_provider();

    let raw_args: Vec<String> = std::env::args().collect();
    let expanded_args = expand_aliases(&raw_args);
    let cli = match Cli::try_parse_from(expanded_args) {
        Ok(cli) => cli,
        Err(err) => err.exit(),
    };

    // Resolve the log level with precedence: --log-level > RUST_LOG > "info".
    let log_args = CliConfigArgs {
        log_level: cli.log_level.clone(),
        ..Default::default()
    };
    let log_level = log_args.resolve_log_level("info");
    let log_format = cli.log_format;
    let log_sink = cli.log_sink.clone();
    logging::init_logging(&log_level, log_format, log_sink)?;

    if let Err(err) = cli::dispatch(cli).await {
        let cli_err = errors::classify_anyhow(&err);
        eprintln!(
            "{} {err:#}",
            format!("error[{}]:", cli_err.code()).red().bold()
        );
        eprintln!(
            "{} {}",
            "  suggestion:".yellow().bold(),
            cli_err.suggestion()
        );
        std::process::exit(1);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn alias_config_with(pairs: &[(&str, &str)]) -> aliases::AliasConfig {
        let map: HashMap<String, String> = pairs
            .iter()
            .map(|(name, expansion)| (name.to_string(), expansion.to_string()))
            .collect();
        aliases::AliasConfig::from_map(map)
    }

    fn to_args(items: &[&str]) -> Vec<String> {
        items.iter().map(|s| s.to_string()).collect()
    }

    #[test]
    fn apply_alias_expansion_expands_a_known_alias_and_preserves_trailing_args() {
        let alias_config = alias_config_with(&[("w", "worker start")]);
        let raw = to_args(&["celers", "w", "--queue", "fast"]);

        let expanded = apply_alias_expansion(&raw, &alias_config);

        assert_eq!(
            expanded,
            to_args(&["celers", "worker", "start", "--queue", "fast"])
        );
    }

    #[test]
    fn apply_alias_expansion_leaves_unknown_command_unchanged() {
        let alias_config = alias_config_with(&[("w", "worker start")]);
        let raw = to_args(&["celers", "status", "--broker", "redis://localhost"]);

        let expanded = apply_alias_expansion(&raw, &alias_config);

        assert_eq!(expanded, raw);
    }

    #[test]
    fn apply_alias_expansion_handles_empty_args() {
        let alias_config = aliases::AliasConfig::new();
        let raw: Vec<String> = Vec::new();

        let expanded = apply_alias_expansion(&raw, &alias_config);

        assert!(expanded.is_empty());
    }

    #[test]
    fn apply_alias_expansion_preserves_binary_name_with_no_trailing_args() {
        let alias_config = alias_config_with(&[("w", "worker start")]);
        let raw = to_args(&["celers"]);

        let expanded = apply_alias_expansion(&raw, &alias_config);

        assert_eq!(expanded, raw);
    }
}
