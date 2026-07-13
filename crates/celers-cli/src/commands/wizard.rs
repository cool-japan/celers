//! Interactive configuration wizard for `celers init --wizard`.
//!
//! This module is split into two deliberately separate halves:
//!
//! - A **pure** half ([`WizardAnswers`], [`build_config`],
//!   [`resolve_wizard_output_path`]) that performs no I/O at all and is
//!   fully unit-tested. [`build_config`] is the single source of truth that
//!   translates a filled-in [`WizardAnswers`] into the [`Config`] structure
//!   understood by the rest of the CLI.
//! - A **thin** interactive half ([`run_wizard`] and its private
//!   `prompt_*` helpers) that asks questions via `dialoguer` and never
//!   contains assembly logic of its own -- it only fills in a
//!   [`WizardAnswers`] and hands it to [`build_config`].
//!
//! Interactive prompts cannot be exercised by a unit test (there is no
//! terminal in `cargo test`), so keeping every real decision in the pure
//! half is what makes this module testable at all.
//!
//! # Examples
//!
//! ```no_run
//! # use celers_cli::commands::wizard::run_wizard;
//! # #[tokio::main]
//! # async fn main() -> anyhow::Result<()> {
//! let config = run_wizard().await?;
//! config.to_file("celers.toml")?;
//! # Ok(())
//! # }
//! ```

use super::utils::validate_queue_name;
use crate::config::{AlertConfig, AutoScaleConfig, BrokerConfig, Config, WorkerConfig};
use celers_broker_redis::RedisBroker;
use colored::Colorize;
use dialoguer::theme::ColorfulTheme;
use dialoguer::{Confirm, Input, Select};
use std::path::{Path, PathBuf};

/// Broker types offered by the wizard's `Select` prompt, in display order.
const BROKER_TYPES: &[&str] = &["redis", "postgres", "mysql", "amqp", "sqs"];

/// Queue dispatch modes offered by the wizard's `Select` prompt.
const QUEUE_MODES: &[&str] = &["fifo", "priority"];

/// Deployment profiles offered by the wizard's `Select` prompt.
const PROFILES: &[&str] = &["dev", "staging", "prod"];

/// Section names offered when the user opts to revise settings after seeing
/// validation warnings. Indices here must line up with the `match` in
/// [`run_wizard`].
const REVISABLE_SECTIONS: &[&str] = &[
    "Broker",
    "Queue",
    "Worker",
    "Auto-scaling",
    "Alerts",
    "Profile / output path",
];

/// The profile written directly to the requested `--output` path rather
/// than to a `{stem}.{profile}.{ext}` overlay file, matching the layout the
/// non-interactive `init_config` command already produces. See
/// [`resolve_wizard_output_path`].
const BASE_PROFILE: &str = "dev";

/// Default output path recommended by the wizard, matching the
/// non-interactive `init` command's own `--output` default.
const DEFAULT_OUTPUT_PATH: &str = "celers.toml";

// Recommended auto-scaling / alert defaults. These mirror
// `celers_cli::config`'s private `default_min_workers`,
// `default_max_workers`, `default_scale_up_threshold`,
// `default_scale_down_threshold`, `default_autoscale_check_interval`,
// `default_dlq_threshold`, `default_failed_threshold`, and
// `default_alert_check_interval` helpers, which are not reachable from
// outside `config.rs`, so the same recommended values are restated here.
const RECOMMENDED_AUTOSCALE_MIN_WORKERS: usize = 1;
const RECOMMENDED_AUTOSCALE_MAX_WORKERS: usize = 10;
const RECOMMENDED_AUTOSCALE_SCALE_UP_THRESHOLD: usize = 100;
const RECOMMENDED_AUTOSCALE_SCALE_DOWN_THRESHOLD: usize = 10;
const RECOMMENDED_AUTOSCALE_CHECK_INTERVAL_SECS: u64 = 30;
const RECOMMENDED_ALERTS_DLQ_THRESHOLD: usize = 50;
const RECOMMENDED_ALERTS_FAILED_THRESHOLD: usize = 100;
const RECOMMENDED_ALERTS_CHECK_INTERVAL_SECS: u64 = 60;

/// Every answer collected by the interactive wizard.
///
/// This is a plain data struct with no I/O of its own -- every field here is
/// exactly what [`build_config`] needs to assemble a full [`Config`].
/// Keeping the "ask" step ([`run_wizard`]) and the "assemble" step
/// ([`build_config`]) separate is what makes assembly fully unit-testable
/// without a terminal.
#[derive(Debug, Clone)]
pub struct WizardAnswers {
    /// Selected broker type (e.g. `"redis"`, `"postgres"`).
    pub broker_type: String,
    /// Broker connection URL.
    pub broker_url: String,
    /// Whether the live connection test succeeded. Informational only --
    /// never blocks config assembly, since a broker may legitimately be
    /// unreachable at setup time (e.g. it is provisioned later).
    pub connection_verified: bool,
    /// Default queue name.
    pub queue_name: String,
    /// Queue mode (`"fifo"` or `"priority"`).
    pub queue_mode: String,
    /// Additional queue names beyond the default queue.
    pub extra_queues: Vec<String>,
    /// Worker concurrency (number of concurrent tasks).
    pub concurrency: usize,
    /// Poll interval in milliseconds.
    pub poll_interval_ms: u64,
    /// Maximum retry attempts.
    pub max_retries: u32,
    /// Default task timeout in seconds.
    pub default_timeout_secs: u64,
    /// Whether auto-scaling should be enabled.
    pub autoscale_enabled: bool,
    /// Minimum worker count (meaningful only when `autoscale_enabled`).
    pub autoscale_min_workers: usize,
    /// Maximum worker count (meaningful only when `autoscale_enabled`).
    pub autoscale_max_workers: usize,
    /// Queue-depth threshold that triggers scaling up.
    pub autoscale_scale_up_threshold: usize,
    /// Queue-depth threshold that triggers scaling down.
    pub autoscale_scale_down_threshold: usize,
    /// Auto-scaler check interval, in seconds.
    pub autoscale_check_interval_secs: u64,
    /// Whether alerting should be enabled.
    pub alerts_enabled: bool,
    /// Webhook URL for alert notifications (meaningful only when
    /// `alerts_enabled`).
    pub alerts_webhook_url: Option<String>,
    /// DLQ size threshold that triggers an alert.
    pub alerts_dlq_threshold: usize,
    /// Failed-task count threshold that triggers an alert.
    pub alerts_failed_threshold: usize,
    /// Alert check interval, in seconds.
    pub alerts_check_interval_secs: u64,
    /// Selected profile name (`"dev"`, `"staging"`, or `"prod"`).
    pub profile: String,
    /// Requested output path for the assembled configuration file.
    pub output_path: String,
}

impl Default for WizardAnswers {
    /// Recommended starting values for every prompt.
    ///
    /// Broker and worker defaults are inherited from
    /// [`Config::default_config`] / `WorkerConfig::default` so they never
    /// drift out of sync with the non-interactive `init` command; the
    /// auto-scaling and alert numeric recommendations mirror `config.rs`'s
    /// private `default_*` helpers (see the `RECOMMENDED_*` constants
    /// above). [`run_wizard`] seeds its prompts from this baseline so every
    /// `Input`/`Select` shows a sensible recommended value that the user
    /// may accept by pressing Enter.
    fn default() -> Self {
        let base = Config::default_config();
        Self {
            broker_type: base.broker.broker_type,
            broker_url: base.broker.url,
            connection_verified: false,
            queue_name: base.broker.queue,
            queue_mode: base.broker.mode,
            extra_queues: Vec::new(),
            concurrency: base.worker.concurrency,
            poll_interval_ms: base.worker.poll_interval_ms,
            max_retries: base.worker.max_retries,
            default_timeout_secs: base.worker.default_timeout_secs,
            autoscale_enabled: false,
            autoscale_min_workers: RECOMMENDED_AUTOSCALE_MIN_WORKERS,
            autoscale_max_workers: RECOMMENDED_AUTOSCALE_MAX_WORKERS,
            autoscale_scale_up_threshold: RECOMMENDED_AUTOSCALE_SCALE_UP_THRESHOLD,
            autoscale_scale_down_threshold: RECOMMENDED_AUTOSCALE_SCALE_DOWN_THRESHOLD,
            autoscale_check_interval_secs: RECOMMENDED_AUTOSCALE_CHECK_INTERVAL_SECS,
            alerts_enabled: false,
            alerts_webhook_url: None,
            alerts_dlq_threshold: RECOMMENDED_ALERTS_DLQ_THRESHOLD,
            alerts_failed_threshold: RECOMMENDED_ALERTS_FAILED_THRESHOLD,
            alerts_check_interval_secs: RECOMMENDED_ALERTS_CHECK_INTERVAL_SECS,
            profile: BASE_PROFILE.to_string(),
            output_path: DEFAULT_OUTPUT_PATH.to_string(),
        }
    }
}

/// Assemble a fully-populated [`Config`] from wizard answers.
///
/// This function performs no I/O and never prompts; it is the single
/// source of truth translating [`WizardAnswers`] into the [`Config`]
/// structure understood by the rest of the CLI (`Config::to_file`,
/// `Config::validate`, ...). Keeping it pure makes every branch
/// unit-testable without a terminal.
///
/// Only *structural* problems are rejected here (empty broker type/URL, a
/// malformed queue name, an empty profile) -- anything a real broker could
/// still use, but that deserves a nudge (an unrecognised broker type, a
/// risky concurrency value, ...), is intentionally left to
/// [`Config::validate`], which reports warnings rather than hard failures.
///
/// # Errors
///
/// Returns an error if the broker type, broker URL, or profile is empty, or
/// if the default queue name or any additional queue name fails
/// `validate_queue_name` (empty, contains whitespace, or over 255 bytes).
///
/// # Examples
///
/// ```
/// use celers_cli::commands::wizard::{build_config, WizardAnswers};
///
/// let answers = WizardAnswers {
///     broker_type: "redis".to_string(),
///     broker_url: "redis://localhost:6379".to_string(),
///     profile: "dev".to_string(),
///     ..WizardAnswers::default()
/// };
///
/// let config = build_config(&answers).expect("well-formed answers assemble a config");
/// assert_eq!(config.broker.broker_type, "redis");
/// assert_eq!(config.profile.as_deref(), Some("dev"));
/// ```
pub fn build_config(answers: &WizardAnswers) -> anyhow::Result<Config> {
    let broker_type = answers.broker_type.trim();
    if broker_type.is_empty() {
        anyhow::bail!("broker type must not be empty");
    }

    let broker_url = answers.broker_url.trim();
    if broker_url.is_empty() {
        anyhow::bail!("broker connection URL must not be empty");
    }

    validate_queue_name(&answers.queue_name)
        .map_err(|e| anyhow::anyhow!("invalid default queue name '{}': {e}", answers.queue_name))?;

    let profile = answers.profile.trim();
    if profile.is_empty() {
        anyhow::bail!("profile name must not be empty");
    }

    let mut queues = Vec::with_capacity(1 + answers.extra_queues.len());
    queues.push(answers.queue_name.clone());
    for extra in &answers.extra_queues {
        let trimmed = extra.trim();
        if trimmed.is_empty() {
            continue;
        }
        validate_queue_name(trimmed)
            .map_err(|e| anyhow::anyhow!("invalid queue name '{trimmed}': {e}"))?;
        if !queues.iter().any(|existing: &String| existing == trimmed) {
            queues.push(trimmed.to_string());
        }
    }

    // Failover settings, the CLI-level connection pool, the read-path TTL
    // cache, and user-defined aliases are not (yet) collected by the
    // wizard; inherit the same recommended values `Config::default_config`
    // uses so the emitted file matches the non-interactive `init` command's
    // baseline.
    let inherited_defaults = Config::default_config();

    let autoscale = answers.autoscale_enabled.then_some(AutoScaleConfig {
        enabled: true,
        min_workers: answers.autoscale_min_workers,
        max_workers: answers.autoscale_max_workers,
        scale_up_threshold: answers.autoscale_scale_up_threshold,
        scale_down_threshold: answers.autoscale_scale_down_threshold,
        check_interval_secs: answers.autoscale_check_interval_secs,
    });

    let alerts = answers.alerts_enabled.then(|| AlertConfig {
        enabled: true,
        webhook_url: answers.alerts_webhook_url.clone(),
        dlq_threshold: answers.alerts_dlq_threshold,
        failed_threshold: answers.alerts_failed_threshold,
        check_interval_secs: answers.alerts_check_interval_secs,
    });

    Ok(Config {
        profile: Some(profile.to_string()),
        broker: BrokerConfig {
            broker_type: broker_type.to_string(),
            url: broker_url.to_string(),
            failover_urls: Vec::new(),
            failover_retries: inherited_defaults.broker.failover_retries,
            failover_timeout_secs: inherited_defaults.broker.failover_timeout_secs,
            queue: answers.queue_name.clone(),
            mode: answers.queue_mode.clone(),
        },
        worker: WorkerConfig {
            concurrency: answers.concurrency,
            poll_interval_ms: answers.poll_interval_ms,
            max_retries: answers.max_retries,
            default_timeout_secs: answers.default_timeout_secs,
        },
        queues,
        autoscale,
        alerts,
        pool: inherited_defaults.pool,
        cache: inherited_defaults.cache,
        aliases: inherited_defaults.aliases,
    })
}

/// Compute the on-disk path the wizard should write its assembled
/// configuration to, given the requested `--output` path and the chosen
/// profile.
///
/// The convention mirrors `config_layer::load_profile_overlay` exactly, so
/// a config written for profile `"prod"` at `output_path = "celers.toml"`
/// lands at `celers.prod.toml` -- the same file `resolve_config` looks for
/// when `--profile prod` (or a `profile = "prod"` key inside the base file)
/// is in effect. The baseline profile (`"dev"`) is written directly to
/// `output_path`, matching the non-wizard `init_config` output and giving
/// `resolve_config` a base file to load even before any overlay exists.
///
/// # Examples
///
/// ```
/// use celers_cli::commands::wizard::resolve_wizard_output_path;
/// use std::path::PathBuf;
///
/// assert_eq!(
///     resolve_wizard_output_path("celers.toml", "dev"),
///     PathBuf::from("celers.toml")
/// );
/// assert_eq!(
///     resolve_wizard_output_path("celers.toml", "prod"),
///     PathBuf::from("celers.prod.toml")
/// );
/// ```
#[must_use]
pub fn resolve_wizard_output_path(output_path: &str, profile: &str) -> PathBuf {
    if profile.eq_ignore_ascii_case(BASE_PROFILE) {
        return PathBuf::from(output_path);
    }

    let path = Path::new(output_path);
    let parent = path.parent().filter(|p| !p.as_os_str().is_empty());
    let stem = path
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("celers");
    let ext = path.extension().and_then(|s| s.to_str()).unwrap_or("toml");

    let file_name = format!("{stem}.{profile}.{ext}");
    match parent {
        Some(dir) => dir.join(file_name),
        None => PathBuf::from(file_name),
    }
}

/// Recommended default connection URL for a given broker type, used to
/// pre-fill the connection-URL prompt.
#[must_use]
fn recommended_broker_url(broker_type: &str) -> &'static str {
    match broker_type.to_ascii_lowercase().as_str() {
        "postgres" | "postgresql" => "postgresql://localhost/celers",
        "mysql" => "mysql://localhost/celers",
        "amqp" | "rabbitmq" => "amqp://guest:guest@localhost:5672/%2f",
        "sqs" => "https://sqs.us-east-1.amazonaws.com/000000000000/celers",
        _ => "redis://localhost:6379",
    }
}

/// Whether the wizard can attempt a live connection probe for `broker_type`
/// from the CLI. Mirrors the broker types `config_cmds::validate_config`
/// can already test interactively; AMQP and SQS have no CLI-side prober
/// today.
#[must_use]
fn broker_supports_live_probe(broker_type: &str) -> bool {
    matches!(
        broker_type.to_ascii_lowercase().as_str(),
        "redis" | "postgres" | "postgresql" | "mysql"
    )
}

/// Attempt a live connection probe against `url` for `broker_type`,
/// returning whether it succeeded.
///
/// PostgreSQL/MySQL probing delegates to
/// [`super::database::db_test_connection`] (which connects via
/// `oxisql_postgres`/`oxisql_mysql` with the URL's own TLS mode -- see
/// [`crate::tls_mode`] -- and runs a version-query round trip), so that
/// probing logic lives in exactly one place: the same implementation
/// already reachable from `celers db test-connection`. Redis uses
/// [`RedisBroker::ping`], the broker crate's own connectivity probe --
/// `db_test_connection` does not cover Redis. Broker types with no
/// CLI-side live probe (see [`broker_supports_live_probe`]) are not
/// dispatched here; callers should check that first.
///
/// Note: this is `commands::database::db_test_connection`, not the
/// lower-level top-level `crate::database` module of the same name (which
/// is not part of the `celers` binary's own private module tree and has no
/// caller there today).
async fn probe_broker_connection(broker_type: &str, url: &str) -> bool {
    match broker_type.to_ascii_lowercase().as_str() {
        "redis" => match RedisBroker::new(url, "celers") {
            Ok(broker) => broker.ping().await.is_ok(),
            Err(_) => false,
        },
        "postgres" | "postgresql" | "mysql" => super::database::db_test_connection(url, false)
            .await
            .is_ok(),
        _ => false,
    }
}

/// Step 1+2: broker selection and live connection testing.
///
/// Loops on the connection URL prompt: on a failed probe, the user may
/// enter a different URL and try again, or move on with
/// `connection_verified = false`.
async fn prompt_broker(theme: &ColorfulTheme, answers: &mut WizardAnswers) -> anyhow::Result<()> {
    println!("{}", "Step 1/6: Broker selection".bold().cyan());

    let default_idx = BROKER_TYPES
        .iter()
        .position(|candidate| *candidate == answers.broker_type)
        .unwrap_or(0);
    let broker_idx = Select::with_theme(theme)
        .with_prompt("Select broker type")
        .items(BROKER_TYPES)
        .default(default_idx)
        .interact()?;
    answers.broker_type = BROKER_TYPES[broker_idx].to_string();

    println!("\n{}", "Step 2/6: Connection test".bold().cyan());
    loop {
        let recommended = recommended_broker_url(&answers.broker_type);
        let url: String = Input::with_theme(theme)
            .with_prompt("Broker connection URL")
            .default(recommended.to_string())
            .validate_with(|input: &String| -> Result<(), String> {
                if input.trim().is_empty() {
                    Err("connection URL must not be empty".to_string())
                } else {
                    Ok(())
                }
            })
            .interact_text()?;
        answers.broker_url = url;

        if !broker_supports_live_probe(&answers.broker_type) {
            println!(
                "{}",
                format!(
                    "(live connection testing from this wizard is not available for '{}'; skipping)",
                    answers.broker_type
                )
                .yellow()
            );
            answers.connection_verified = false;
            break;
        }

        println!("Testing connection...");
        answers.connection_verified =
            probe_broker_connection(&answers.broker_type, &answers.broker_url).await;

        if answers.connection_verified {
            println!("{}", "\u{2713} Connection succeeded".green().bold());
            break;
        }

        println!("{}", "\u{2717} Connection failed".red().bold());
        let retry = Confirm::with_theme(theme)
            .with_prompt("Try a different URL?")
            .default(true)
            .interact()?;
        if !retry {
            break;
        }
    }

    Ok(())
}

/// Step 3: queue configuration, with `dialoguer`-level validation
/// (re-prompts automatically on invalid input via [`validate_queue_name`]).
fn prompt_queue(theme: &ColorfulTheme, answers: &mut WizardAnswers) -> anyhow::Result<()> {
    println!("\n{}", "Step 3/6: Queue configuration".bold().cyan());

    answers.queue_name = Input::with_theme(theme)
        .with_prompt("Default queue name")
        .default(answers.queue_name.clone())
        .validate_with(|input: &String| -> Result<(), String> {
            validate_queue_name(input).map_err(|e| e.to_string())
        })
        .interact_text()?;

    let mode_default = if answers.queue_mode == "priority" {
        1
    } else {
        0
    };
    let mode_idx = Select::with_theme(theme)
        .with_prompt("Queue mode")
        .items(QUEUE_MODES)
        .default(mode_default)
        .interact()?;
    answers.queue_mode = QUEUE_MODES[mode_idx].to_string();

    let extra_raw: String = Input::with_theme(theme)
        .with_prompt("Additional queue names (comma-separated, optional)")
        .allow_empty(true)
        .default(answers.extra_queues.join(", "))
        .interact_text()?;

    let mut extra_queues = Vec::new();
    for candidate in extra_raw.split(',') {
        let trimmed = candidate.trim();
        if trimmed.is_empty() {
            continue;
        }
        validate_queue_name(trimmed)
            .map_err(|e| anyhow::anyhow!("invalid additional queue name '{trimmed}': {e}"))?;
        extra_queues.push(trimmed.to_string());
    }
    answers.extra_queues = extra_queues;

    Ok(())
}

/// Step 4: worker settings, pre-filled with recommended defaults.
fn prompt_worker(theme: &ColorfulTheme, answers: &mut WizardAnswers) -> anyhow::Result<()> {
    println!(
        "\n{}",
        "Step 4/6: Worker settings (press Enter to accept the recommendation)"
            .bold()
            .cyan()
    );

    answers.concurrency = Input::with_theme(theme)
        .with_prompt("Worker concurrency (recommended: 4)")
        .default(answers.concurrency)
        .interact_text()?;
    answers.poll_interval_ms = Input::with_theme(theme)
        .with_prompt("Poll interval in milliseconds (recommended: 1000)")
        .default(answers.poll_interval_ms)
        .interact_text()?;
    answers.max_retries = Input::with_theme(theme)
        .with_prompt("Maximum retry attempts (recommended: 3)")
        .default(answers.max_retries)
        .interact_text()?;
    answers.default_timeout_secs = Input::with_theme(theme)
        .with_prompt("Default task timeout in seconds (recommended: 300)")
        .default(answers.default_timeout_secs)
        .interact_text()?;

    Ok(())
}

/// Step 5: auto-scaling setup.
fn prompt_autoscale(theme: &ColorfulTheme, answers: &mut WizardAnswers) -> anyhow::Result<()> {
    println!("\n{}", "Step 5/6: Auto-scaling".bold().cyan());

    answers.autoscale_enabled = Confirm::with_theme(theme)
        .with_prompt("Enable auto-scaling?")
        .default(answers.autoscale_enabled)
        .interact()?;

    if !answers.autoscale_enabled {
        return Ok(());
    }

    answers.autoscale_min_workers = Input::with_theme(theme)
        .with_prompt("Minimum workers (recommended: 1)")
        .default(answers.autoscale_min_workers)
        .interact_text()?;
    answers.autoscale_max_workers = Input::with_theme(theme)
        .with_prompt("Maximum workers (recommended: 10)")
        .default(answers.autoscale_max_workers)
        .interact_text()?;
    answers.autoscale_scale_up_threshold = Input::with_theme(theme)
        .with_prompt("Scale-up queue-depth threshold (recommended: 100)")
        .default(answers.autoscale_scale_up_threshold)
        .interact_text()?;
    answers.autoscale_scale_down_threshold = Input::with_theme(theme)
        .with_prompt("Scale-down queue-depth threshold (recommended: 10)")
        .default(answers.autoscale_scale_down_threshold)
        .interact_text()?;
    answers.autoscale_check_interval_secs = Input::with_theme(theme)
        .with_prompt("Check interval in seconds (recommended: 30)")
        .default(answers.autoscale_check_interval_secs)
        .interact_text()?;

    Ok(())
}

/// Step 6: alert setup.
fn prompt_alerts(theme: &ColorfulTheme, answers: &mut WizardAnswers) -> anyhow::Result<()> {
    println!("\n{}", "Step 6/6: Alerts".bold().cyan());

    answers.alerts_enabled = Confirm::with_theme(theme)
        .with_prompt("Enable alerts?")
        .default(answers.alerts_enabled)
        .interact()?;

    if !answers.alerts_enabled {
        return Ok(());
    }

    let webhook: String = Input::with_theme(theme)
        .with_prompt("Alert webhook URL (leave blank for none)")
        .allow_empty(true)
        .default(answers.alerts_webhook_url.clone().unwrap_or_default())
        .interact_text()?;
    answers.alerts_webhook_url = if webhook.trim().is_empty() {
        None
    } else {
        Some(webhook)
    };

    answers.alerts_dlq_threshold = Input::with_theme(theme)
        .with_prompt("DLQ size threshold (recommended: 50)")
        .default(answers.alerts_dlq_threshold)
        .interact_text()?;
    answers.alerts_failed_threshold = Input::with_theme(theme)
        .with_prompt("Failed-task threshold (recommended: 100)")
        .default(answers.alerts_failed_threshold)
        .interact_text()?;
    answers.alerts_check_interval_secs = Input::with_theme(theme)
        .with_prompt("Check interval in seconds (recommended: 60)")
        .default(answers.alerts_check_interval_secs)
        .interact_text()?;

    Ok(())
}

/// Profile selection and output path.
fn prompt_profile_and_output(
    theme: &ColorfulTheme,
    answers: &mut WizardAnswers,
) -> anyhow::Result<()> {
    println!("\n{}", "Profile".bold().cyan());

    let default_idx = PROFILES
        .iter()
        .position(|candidate| *candidate == answers.profile)
        .unwrap_or(0);
    let profile_idx = Select::with_theme(theme)
        .with_prompt("Select profile")
        .items(PROFILES)
        .default(default_idx)
        .interact()?;
    answers.profile = PROFILES[profile_idx].to_string();

    answers.output_path = Input::with_theme(theme)
        .with_prompt("Config output path")
        .default(answers.output_path.clone())
        .validate_with(|input: &String| -> Result<(), String> {
            if input.trim().is_empty() {
                Err("output path must not be empty".to_string())
            } else {
                Ok(())
            }
        })
        .interact_text()?;

    Ok(())
}

/// Interactively assemble a validated [`Config`] via `dialoguer` prompts.
///
/// Walks the user through broker selection, a live connection test, queue
/// configuration, worker settings (with recommended defaults),
/// auto-scaling and alert setup, and profile selection -- then loops
/// [`Config::validate`] until either there are no warnings or the user
/// explicitly chooses to proceed anyway (warnings are informational, never
/// fatal). All assembly logic lives in [`build_config`]; this function only
/// collects answers and reports progress.
///
/// # Errors
///
/// Returns an error if a prompt's underlying terminal I/O fails, or if
/// [`build_config`] rejects the collected answers (see its docs for what
/// counts as structurally invalid).
///
/// # Examples
///
/// ```no_run
/// # use celers_cli::commands::wizard::run_wizard;
/// # #[tokio::main]
/// # async fn main() -> anyhow::Result<()> {
/// let config = run_wizard().await?;
/// config.to_file("celers.toml")?;
/// # Ok(())
/// # }
/// ```
pub async fn run_wizard() -> anyhow::Result<Config> {
    let theme = ColorfulTheme::default();
    let mut answers = WizardAnswers::default();

    println!("{}", "=== CeleRS Configuration Wizard ===".bold().cyan());
    println!("Press Enter at any prompt to accept the recommended value.\n");

    prompt_broker(&theme, &mut answers).await?;
    prompt_queue(&theme, &mut answers)?;
    prompt_worker(&theme, &mut answers)?;
    prompt_autoscale(&theme, &mut answers)?;
    prompt_alerts(&theme, &mut answers)?;
    prompt_profile_and_output(&theme, &mut answers)?;

    let mut config = build_config(&answers)?;

    loop {
        let warnings = config.validate()?;
        if warnings.is_empty() {
            println!(
                "\n{}",
                "\u{2713} Configuration is valid, no warnings"
                    .green()
                    .bold()
            );
            break;
        }

        println!("\n{}", "Configuration warnings:".yellow().bold());
        for warning in &warnings {
            println!("  {} {warning}", "\u{26a0}".yellow());
        }

        let proceed = Confirm::with_theme(&theme)
            .with_prompt("Continue with these settings anyway?")
            .default(true)
            .interact()?;
        if proceed {
            break;
        }

        let section_idx = Select::with_theme(&theme)
            .with_prompt("Which section would you like to revise?")
            .items(REVISABLE_SECTIONS)
            .default(2)
            .interact()?;
        match section_idx {
            0 => prompt_broker(&theme, &mut answers).await?,
            1 => prompt_queue(&theme, &mut answers)?,
            3 => prompt_autoscale(&theme, &mut answers)?,
            4 => prompt_alerts(&theme, &mut answers)?,
            5 => prompt_profile_and_output(&theme, &mut answers)?,
            _ => prompt_worker(&theme, &mut answers)?,
        }

        config = build_config(&answers)?;
    }

    Ok(config)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A representative, fully-populated set of answers, as if a user had
    /// clicked through the wizard picking a non-default broker URL, a
    /// couple of extra queues, and the `staging` profile.
    fn sample_answers() -> WizardAnswers {
        WizardAnswers {
            broker_type: "redis".to_string(),
            broker_url: "redis://localhost:6379".to_string(),
            connection_verified: true,
            queue_name: "celers".to_string(),
            queue_mode: "fifo".to_string(),
            extra_queues: vec!["high_priority".to_string(), "low_priority".to_string()],
            profile: "staging".to_string(),
            output_path: "celers.toml".to_string(),
            ..WizardAnswers::default()
        }
    }

    #[test]
    fn wizard_answers_default_matches_recommended_baseline() {
        let answers = WizardAnswers::default();
        assert_eq!(answers.broker_type, "redis");
        assert_eq!(answers.broker_url, "redis://localhost:6379");
        assert_eq!(answers.queue_name, "celers");
        assert_eq!(answers.queue_mode, "fifo");
        assert_eq!(answers.concurrency, 4);
        assert_eq!(answers.poll_interval_ms, 1000);
        assert_eq!(answers.max_retries, 3);
        assert_eq!(answers.default_timeout_secs, 300);
        assert!(!answers.autoscale_enabled);
        assert!(!answers.alerts_enabled);
        assert_eq!(answers.profile, "dev");
        assert_eq!(answers.output_path, "celers.toml");
    }

    #[test]
    fn build_config_assembles_expected_config_from_representative_answers() {
        let answers = sample_answers();
        let config = build_config(&answers).expect("representative answers must build a config");

        assert_eq!(config.broker.broker_type, "redis");
        assert_eq!(config.broker.url, "redis://localhost:6379");
        assert_eq!(config.broker.queue, "celers");
        assert_eq!(config.broker.mode, "fifo");
        assert_eq!(
            config.broker.failover_retries,
            Config::default_config().broker.failover_retries
        );
        assert_eq!(
            config.queues,
            vec!["celers", "high_priority", "low_priority"]
        );
        assert_eq!(config.profile, Some("staging".to_string()));
        assert!(config.autoscale.is_none());
        assert!(config.alerts.is_none());
    }

    #[test]
    fn build_config_deduplicates_extra_queues_and_drops_blanks() {
        let mut answers = sample_answers();
        answers.extra_queues = vec![
            "celers".to_string(), // duplicate of the default queue, dropped
            "high_priority".to_string(),
            "high_priority".to_string(), // duplicate, dropped
            "  ".to_string(),            // blank, dropped
            "low_priority".to_string(),
        ];

        let config = build_config(&answers).expect("valid answers");
        assert_eq!(
            config.queues,
            vec!["celers", "high_priority", "low_priority"]
        );
    }

    #[test]
    fn build_config_populates_autoscale_when_enabled() {
        let mut answers = sample_answers();
        answers.autoscale_enabled = true;
        answers.autoscale_min_workers = 2;
        answers.autoscale_max_workers = 20;
        answers.autoscale_scale_up_threshold = 200;
        answers.autoscale_scale_down_threshold = 20;
        answers.autoscale_check_interval_secs = 45;

        let config = build_config(&answers).expect("valid answers");
        let autoscale = config.autoscale.expect("autoscale must be populated");
        assert!(autoscale.enabled);
        assert_eq!(autoscale.min_workers, 2);
        assert_eq!(autoscale.max_workers, 20);
        assert_eq!(autoscale.scale_up_threshold, 200);
        assert_eq!(autoscale.scale_down_threshold, 20);
        assert_eq!(autoscale.check_interval_secs, 45);
    }

    #[test]
    fn build_config_leaves_autoscale_none_when_disabled() {
        let answers = sample_answers();
        assert!(!answers.autoscale_enabled);
        let config = build_config(&answers).expect("valid answers");
        assert!(config.autoscale.is_none());
    }

    #[test]
    fn build_config_populates_alerts_when_enabled() {
        let mut answers = sample_answers();
        answers.alerts_enabled = true;
        answers.alerts_webhook_url = Some("https://hooks.example.com/x".to_string());
        answers.alerts_dlq_threshold = 77;
        answers.alerts_failed_threshold = 88;
        answers.alerts_check_interval_secs = 99;

        let config = build_config(&answers).expect("valid answers");
        let alerts = config.alerts.expect("alerts must be populated");
        assert!(alerts.enabled);
        assert_eq!(
            alerts.webhook_url,
            Some("https://hooks.example.com/x".to_string())
        );
        assert_eq!(alerts.dlq_threshold, 77);
        assert_eq!(alerts.failed_threshold, 88);
        assert_eq!(alerts.check_interval_secs, 99);
    }

    #[test]
    fn build_config_rejects_empty_broker_type() {
        let mut answers = sample_answers();
        answers.broker_type = "   ".to_string();
        let err = build_config(&answers).expect_err("empty broker type must be rejected");
        assert!(err.to_string().contains("broker type"));
    }

    #[test]
    fn build_config_rejects_empty_broker_url() {
        let mut answers = sample_answers();
        answers.broker_url = String::new();
        let err = build_config(&answers).expect_err("empty broker URL must be rejected");
        assert!(err.to_string().contains("connection URL"));
    }

    #[test]
    fn build_config_rejects_invalid_queue_name() {
        let mut answers = sample_answers();
        answers.queue_name = "bad queue name".to_string();
        let err = build_config(&answers).expect_err("whitespace queue name must be rejected");
        assert!(err.to_string().contains("default queue name"));
    }

    #[test]
    fn build_config_rejects_invalid_extra_queue_name() {
        let mut answers = sample_answers();
        answers.extra_queues = vec!["bad name".to_string()];
        let err = build_config(&answers).expect_err("whitespace extra queue name must be rejected");
        assert!(err.to_string().contains("bad name"));
    }

    #[test]
    fn build_config_rejects_empty_profile() {
        let mut answers = sample_answers();
        answers.profile = "  ".to_string();
        let err = build_config(&answers).expect_err("empty profile must be rejected");
        assert!(err.to_string().contains("profile"));
    }

    #[test]
    fn build_config_accepts_unknown_broker_type_deferring_to_config_validate() {
        // build_config only enforces structural validity; recognising the
        // broker type is Config::validate()'s job, which emits a warning
        // rather than an error (see test_config_validation_invalid_broker_type
        // in config.rs).
        let mut answers = sample_answers();
        answers.broker_type = "totally-unknown".to_string();
        let config = build_config(&answers).expect("unknown broker type is structurally valid");
        let warnings = config.validate().expect("validate never errors today");
        assert!(warnings.iter().any(|w| w.contains("Unknown broker type")));
    }

    #[test]
    fn resolve_wizard_output_path_writes_dev_profile_to_base_path() {
        assert_eq!(
            resolve_wizard_output_path("celers.toml", "dev"),
            PathBuf::from("celers.toml")
        );
        // Case-insensitive: "Dev"/"DEV" are still treated as the baseline.
        assert_eq!(
            resolve_wizard_output_path("celers.toml", "Dev"),
            PathBuf::from("celers.toml")
        );
    }

    #[test]
    fn resolve_wizard_output_path_writes_overlay_for_non_base_profile() {
        assert_eq!(
            resolve_wizard_output_path("celers.toml", "prod"),
            PathBuf::from("celers.prod.toml")
        );
        assert_eq!(
            resolve_wizard_output_path("celers.yaml", "staging"),
            PathBuf::from("celers.staging.yaml")
        );
    }

    #[test]
    fn resolve_wizard_output_path_preserves_parent_directory() {
        assert_eq!(
            resolve_wizard_output_path("config/celers.toml", "prod"),
            PathBuf::from("config/celers.prod.toml")
        );
    }

    #[test]
    fn resolve_wizard_output_path_handles_missing_extension() {
        assert_eq!(
            resolve_wizard_output_path("celers", "prod"),
            PathBuf::from("celers.prod.toml")
        );
    }

    #[test]
    fn recommended_broker_url_covers_every_offered_broker_type() {
        assert_eq!(recommended_broker_url("redis"), "redis://localhost:6379");
        assert_eq!(
            recommended_broker_url("postgres"),
            "postgresql://localhost/celers"
        );
        assert_eq!(
            recommended_broker_url("postgresql"),
            "postgresql://localhost/celers"
        );
        assert_eq!(recommended_broker_url("mysql"), "mysql://localhost/celers");
        assert!(recommended_broker_url("amqp").starts_with("amqp://"));
        assert!(recommended_broker_url("sqs").starts_with("https://"));
    }

    #[test]
    fn broker_supports_live_probe_matches_database_rs_coverage() {
        assert!(broker_supports_live_probe("redis"));
        assert!(broker_supports_live_probe("postgres"));
        assert!(broker_supports_live_probe("postgresql"));
        assert!(broker_supports_live_probe("mysql"));
        assert!(broker_supports_live_probe("REDIS"));
        assert!(!broker_supports_live_probe("amqp"));
        assert!(!broker_supports_live_probe("rabbitmq"));
        assert!(!broker_supports_live_probe("sqs"));
    }

    #[test]
    fn broker_types_and_queue_modes_and_profiles_are_non_empty() {
        // Guards against an accidental empty const, which would make the
        // corresponding Select prompt panic at runtime (`items` would be
        // empty).
        assert!(!BROKER_TYPES.is_empty());
        assert!(!QUEUE_MODES.is_empty());
        assert!(!PROFILES.is_empty());
        assert!(!REVISABLE_SECTIONS.is_empty());
        assert!(PROFILES.contains(&BASE_PROFILE));
    }
}
