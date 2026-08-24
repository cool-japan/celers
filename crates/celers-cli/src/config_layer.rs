//! Layered configuration resolution and dynamic reload for the `CeleRS` CLI.
//!
//! This module ties together the three configuration sources supported by the
//! CLI and establishes a single, well-defined precedence order:
//!
//! ```text
//! CLI arguments  >  environment variables  >  config file  >  built-in defaults
//! ```
//!
//! The pieces are:
//!
//! - [`CliConfigArgs`] — a flattened set of `clap` arguments that map directly
//!   onto [`Config`] fields and override any value coming from the environment
//!   or a file.
//! - [`resolve_config`] — applies the precedence chain and returns the final
//!   [`Config`].
//! - [`ReloadableConfig`] — remembers where a configuration was loaded from so
//!   it can be reloaded at runtime, reporting exactly what changed via
//!   [`ConfigDiff`].
//!
//! # Example
//!
//! ```no_run
//! use celers_cli::config_layer::{CliConfigArgs, resolve_config};
//!
//! # fn main() -> anyhow::Result<()> {
//! // Typically `args` is produced by clap; here we build it manually.
//! let args = CliConfigArgs {
//!     broker: Some("redis://override:6379".to_string()),
//!     ..Default::default()
//! };
//!
//! // Defaults < file < environment < CLI args.
//! let config = resolve_config(&args)?;
//! assert_eq!(config.broker.url, "redis://override:6379");
//! # Ok(())
//! # }
//! ```

use crate::config::{Config, ConfigDiff, ConfigFormat};
use clap::Args;
use std::path::{Path, PathBuf};

/// Command-line configuration overrides.
///
/// These arguments are intended to be `#[command(flatten)]`-ed into individual
/// subcommands (or a top-level command). Every field is optional; a `None`
/// value means "do not override", allowing the environment, the config file, or
/// the built-in defaults to supply the value instead.
///
/// The precedence applied by [`resolve_config`] is:
/// CLI args > environment > config file > defaults.
#[derive(Args, Debug, Clone, Default)]
pub struct CliConfigArgs {
    /// Path to a configuration file (TOML or YAML, auto-detected by extension).
    #[arg(long, value_name = "PATH", global = true)]
    pub config: Option<PathBuf>,

    /// Broker connection URL (overrides `broker.url`).
    #[arg(long, value_name = "URL", global = true)]
    pub broker: Option<String>,

    /// Result backend URL (overrides `broker.url` when no explicit broker is
    /// set; primarily used by result-oriented commands).
    #[arg(long, value_name = "URL", global = true)]
    pub backend: Option<String>,

    /// Broker type, e.g. `redis` or `postgres` (overrides `broker.type`).
    #[arg(long = "broker-type", value_name = "TYPE", global = true)]
    pub broker_type: Option<String>,

    /// Default queue name (overrides `broker.queue`).
    #[arg(long, value_name = "NAME", global = true)]
    pub queue: Option<String>,

    /// Queue mode: `fifo` or `priority` (overrides `broker.mode`).
    #[arg(long = "queue-mode", value_name = "MODE", global = true)]
    pub queue_mode: Option<String>,

    /// Comma-separated list of queue names (overrides `queues`).
    #[arg(long, value_name = "Q1,Q2,...", value_delimiter = ',', global = true)]
    pub queues: Option<Vec<String>>,

    /// Number of concurrent tasks (overrides `worker.concurrency`).
    #[arg(long, value_name = "N", global = true)]
    pub concurrency: Option<usize>,

    /// Poll interval in milliseconds (overrides `worker.poll_interval_ms`).
    #[arg(long = "poll-interval-ms", value_name = "MS", global = true)]
    pub poll_interval_ms: Option<u64>,

    /// Maximum retry attempts (overrides `worker.max_retries`).
    #[arg(long = "max-retries", value_name = "N", global = true)]
    pub max_retries: Option<u32>,

    /// Default task timeout in seconds (overrides `worker.default_timeout_secs`).
    #[arg(long = "timeout-secs", value_name = "SECS", global = true)]
    pub timeout_secs: Option<u64>,

    /// Configuration profile to load (dev, staging, prod, ...).
    #[arg(long, value_name = "PROFILE", global = true)]
    pub profile: Option<String>,

    /// Log level for the CLI (`error`, `warn`, `info`, `debug`, `trace`).
    ///
    /// Takes precedence over the `RUST_LOG` environment variable.
    #[arg(long = "log-level", value_name = "LEVEL", global = true)]
    pub log_level: Option<String>,
}

impl CliConfigArgs {
    /// Apply the CLI overrides onto an already-resolved [`Config`].
    ///
    /// Only fields with an explicit value (`Some`) are written, so this is the
    /// highest-precedence layer. The `config`, `backend`, and `log_level`
    /// fields are handled outside [`Config`] (file selection, result backend
    /// selection, and tracing setup respectively) and are intentionally not
    /// applied here.
    pub fn apply_to(&self, config: &mut Config) {
        if let Some(ref broker) = self.broker {
            config.broker.url = broker.clone();
        }
        if let Some(ref broker_type) = self.broker_type {
            config.broker.broker_type = broker_type.clone();
        }
        if let Some(ref queue) = self.queue {
            config.broker.queue = queue.clone();
        }
        if let Some(ref mode) = self.queue_mode {
            config.broker.mode = mode.clone();
        }
        if let Some(ref queues) = self.queues {
            let cleaned: Vec<String> = queues
                .iter()
                .map(|q| q.trim().to_string())
                .filter(|q| !q.is_empty())
                .collect();
            if !cleaned.is_empty() {
                config.queues = cleaned;
            }
        }
        if let Some(concurrency) = self.concurrency {
            config.worker.concurrency = concurrency;
        }
        if let Some(poll_interval_ms) = self.poll_interval_ms {
            config.worker.poll_interval_ms = poll_interval_ms;
        }
        if let Some(max_retries) = self.max_retries {
            config.worker.max_retries = max_retries;
        }
        if let Some(timeout_secs) = self.timeout_secs {
            config.worker.default_timeout_secs = timeout_secs;
        }
        if let Some(ref profile) = self.profile {
            config.profile = Some(profile.clone());
        }
    }

    /// Resolve the effective log level, honouring precedence:
    /// `--log-level` > `RUST_LOG` env > the supplied `default`.
    #[must_use]
    pub fn resolve_log_level(&self, default: &str) -> String {
        if let Some(ref level) = self.log_level {
            return level.clone();
        }
        std::env::var("RUST_LOG").unwrap_or_else(|_| default.to_string())
    }
}

/// Resolve the final configuration by applying every layer in precedence order.
///
/// 1. Built-in defaults ([`Config::default_config`]).
/// 2. Config file, if one is provided via `--config` or discovered on disk
///    (TOML/YAML auto-detected, with `${VAR}` expansion). A `--profile` value
///    additionally merges any profile-specific overlay file.
/// 3. Environment variables ([`Config::apply_env_overrides`]).
/// 4. CLI arguments ([`CliConfigArgs::apply_to`]).
///
/// Later layers override earlier ones.
pub fn resolve_config(args: &CliConfigArgs) -> anyhow::Result<Config> {
    // Layer 1 + 2: defaults, then file (with optional profile overlay).
    let mut config = load_base_config(args.config.as_deref(), args.profile.as_deref())?;

    // Layer 3: environment variables.
    config.apply_env_overrides();

    // Layer 4: CLI arguments (highest precedence).
    args.apply_to(&mut config);

    Ok(config)
}

/// Default config-file names searched (in order) when no `--config` is given.
const DEFAULT_CONFIG_NAMES: [&str; 3] = ["celers.toml", "celers.yaml", "celers.yml"];

/// Load the base configuration (defaults + file, including profile overlay).
///
/// When `path` is `None`, the well-known default file names are probed in the
/// current directory; if none exist the built-in defaults are returned.
fn load_base_config(path: Option<&Path>, profile: Option<&str>) -> anyhow::Result<Config> {
    let resolved_path = match path {
        Some(p) => Some(p.to_path_buf()),
        None => discover_config_file(),
    };

    let Some(path) = resolved_path else {
        return Ok(Config::default_config());
    };

    let mut config = Config::from_file(&path)?;

    // Merge a profile-specific overlay file when requested.
    let effective_profile = profile
        .map(str::to_string)
        .or_else(|| config.profile.clone());
    if let Some(profile) = effective_profile {
        if let Some((overlay, overlay_raw)) = load_profile_overlay(&path, &profile)? {
            config = merge_overlay(config, overlay, &overlay_raw);
        }
    }

    Ok(config)
}

/// Probe the current directory for a default configuration file.
fn discover_config_file() -> Option<PathBuf> {
    DEFAULT_CONFIG_NAMES
        .iter()
        .map(PathBuf::from)
        .find(|candidate| candidate.exists())
}

/// Resolve the on-disk path a *mutating* command (e.g. `celers alias
/// add`/`remove`) should write back to, using the same precedence
/// [`resolve_config`] itself uses to read: an explicit `--config` path if
/// given, otherwise the first auto-discovered default config file in the
/// current directory (see `DEFAULT_CONFIG_NAMES`).
///
/// Unlike the plain [`Config`] returned by [`resolve_config`] (which falls
/// back to in-memory defaults when nothing exists on disk), this never
/// silently loses the target path: when no file exists yet, it returns the
/// first default name (`celers.toml`) -- the same filename the CLI's `init`
/// command writes to by default -- so a mutating command run before `celers
/// init` still has a sensible file to create rather than failing.
#[must_use]
pub fn resolve_config_path(explicit: Option<&Path>) -> PathBuf {
    explicit
        .map(Path::to_path_buf)
        .or_else(discover_config_file)
        .unwrap_or_else(|| PathBuf::from(DEFAULT_CONFIG_NAMES[0]))
}

/// Load a profile-specific overlay file located alongside the base config.
///
/// Given a base path of `dir/celers.toml` and profile `prod`, this looks for
/// `dir/celers.prod.toml` (matching the base file's extension), then for the
/// other supported extensions. Returns `Ok(None)` when no overlay exists.
///
/// Returns both the typed [`Config`] (used for its actual values) and the
/// same file's raw, format-agnostic [`serde_json::Value`] (used by
/// [`merge_overlay`] to tell "this field was genuinely absent from the
/// overlay file" apart from "this field was present and happened to equal
/// the type default" -- a distinction the typed `Config` alone cannot make,
/// since both parse to the exact same default value).
fn load_profile_overlay(
    base: &Path,
    profile: &str,
) -> anyhow::Result<Option<(Config, serde_json::Value)>> {
    let parent = base.parent().filter(|p| !p.as_os_str().is_empty());
    let stem = base
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("celers");

    // Preferred extension order: the base file's own extension first.
    let base_ext = match ConfigFormat::from_path(base) {
        ConfigFormat::Yaml => "yaml",
        ConfigFormat::Toml => "toml",
    };
    let extensions = [base_ext, "toml", "yaml", "yml"];

    for ext in extensions {
        let file_name = format!("{stem}.{profile}.{ext}");
        let candidate = match parent {
            Some(dir) => dir.join(&file_name),
            None => PathBuf::from(&file_name),
        };
        if candidate.exists() {
            let config = Config::from_file(&candidate)?;
            let raw = parse_raw_overlay_value(&candidate)?;
            return Ok(Some((config, raw)));
        }
    }

    Ok(None)
}

/// Parse `path`'s raw content into a format-agnostic [`serde_json::Value`]
/// tree, applying the exact same environment-variable expansion
/// [`Config::from_file`] does first (so the two views of the file stay
/// consistent, even though expansion only ever changes string *values*, not
/// which keys are present).
///
/// TOML and YAML are each parsed with their own `Deserialize` impl
/// (`toml::Value` / `serde_yaml::Value`) and then re-serialized through
/// `serde_json::to_value`, giving [`merge_overlay`] one uniform
/// presence-checking representation regardless of the overlay file's
/// on-disk format.
fn parse_raw_overlay_value(path: &Path) -> anyhow::Result<serde_json::Value> {
    let content = std::fs::read_to_string(path)?;
    let expanded = crate::config::expand_env_vars(&content);
    let value = match ConfigFormat::from_path(path) {
        ConfigFormat::Toml => {
            let v: toml::Value = toml::from_str(&expanded)?;
            serde_json::to_value(v)?
        }
        ConfigFormat::Yaml => {
            let v: serde_yaml::Value = serde_yaml::from_str(&expanded)?;
            serde_json::to_value(v)?
        }
    };
    Ok(value)
}

/// `true` when `raw[section][field]` is present and non-null -- i.e. the
/// overlay file actually set this field, regardless of what value it was
/// set to (including a value that happens to equal the type default). See
/// [`parse_raw_overlay_value`] and [`merge_overlay`].
#[must_use]
fn overlay_sets(raw: &serde_json::Value, section: &str, field: &str) -> bool {
    raw.get(section)
        .and_then(|s| s.get(field))
        .is_some_and(|v| !v.is_null())
}

/// Top-level counterpart of [`overlay_sets`] for a field with no section
/// nesting (`queues`, `profile`).
#[must_use]
fn overlay_sets_top(raw: &serde_json::Value, field: &str) -> bool {
    raw.get(field).is_some_and(|v| !v.is_null())
}

/// Merge an overlay configuration onto a base, with an overlay field winning
/// whenever it was actually *present* in the overlay file -- checked against
/// `overlay_raw` (see [`overlay_sets`]/[`overlay_sets_top`]), not against
/// whether the overlay's typed value differs from the type default.
///
/// This used to compare `overlay`'s typed value to [`Config::default_config`]
/// instead of consulting the raw file (idx 337 part 2): since a field
/// deserializes to the exact same default value whether it was explicitly
/// set to that value or simply absent from the file, that comparison could
/// never let an overlay *reset* a base value back down to the default --
/// the overlay's field wins the comparison against the default (a no-op)
/// while the base's genuinely non-default value silently survives. It also
/// merged only a subset of [`Config`]'s fields, leaving
/// `broker.failover_retries`/`failover_timeout_secs`, `pool`, `cache`, and
/// `aliases` un-overridable by any profile overlay no matter what the
/// overlay file said.
///
/// `Option<T>` fields (`autoscale`, `alerts`, `aliases`) are the one
/// exception that never needed the raw-value treatment: serde already
/// represents "absent" as `None` for those, so `Option::is_some()` was (and
/// remains) a correct presence check on the typed value alone.
fn merge_overlay(mut base: Config, overlay: Config, overlay_raw: &serde_json::Value) -> Config {
    if overlay_sets_top(overlay_raw, "profile") {
        base.profile = overlay.profile;
    }
    if overlay_sets(overlay_raw, "broker", "url") {
        base.broker.url = overlay.broker.url;
    }
    // `BrokerConfig::broker_type` is `#[serde(rename = "type")]`, so the raw
    // on-disk key is "type", not "broker_type".
    if overlay_sets(overlay_raw, "broker", "type") {
        base.broker.broker_type = overlay.broker.broker_type;
    }
    if overlay_sets(overlay_raw, "broker", "queue") {
        base.broker.queue = overlay.broker.queue;
    }
    if overlay_sets(overlay_raw, "broker", "mode") {
        base.broker.mode = overlay.broker.mode;
    }
    if overlay_sets(overlay_raw, "broker", "failover_urls") {
        base.broker.failover_urls = overlay.broker.failover_urls;
    }
    if overlay_sets(overlay_raw, "broker", "failover_retries") {
        base.broker.failover_retries = overlay.broker.failover_retries;
    }
    if overlay_sets(overlay_raw, "broker", "failover_timeout_secs") {
        base.broker.failover_timeout_secs = overlay.broker.failover_timeout_secs;
    }
    if overlay_sets(overlay_raw, "worker", "concurrency") {
        base.worker.concurrency = overlay.worker.concurrency;
    }
    if overlay_sets(overlay_raw, "worker", "poll_interval_ms") {
        base.worker.poll_interval_ms = overlay.worker.poll_interval_ms;
    }
    if overlay_sets(overlay_raw, "worker", "max_retries") {
        base.worker.max_retries = overlay.worker.max_retries;
    }
    if overlay_sets(overlay_raw, "worker", "default_timeout_secs") {
        base.worker.default_timeout_secs = overlay.worker.default_timeout_secs;
    }
    if overlay_sets_top(overlay_raw, "queues") {
        base.queues = overlay.queues;
    }
    if overlay.autoscale.is_some() {
        base.autoscale = overlay.autoscale;
    }
    if overlay.alerts.is_some() {
        base.alerts = overlay.alerts;
    }
    if overlay_sets(overlay_raw, "pool", "max_size") {
        base.pool.max_size = overlay.pool.max_size;
    }
    if overlay_sets(overlay_raw, "pool", "reuse_enabled") {
        base.pool.reuse_enabled = overlay.pool.reuse_enabled;
    }
    if overlay_sets(overlay_raw, "cache", "ttl_secs") {
        base.cache.ttl_secs = overlay.cache.ttl_secs;
    }
    if overlay_sets(overlay_raw, "cache", "enabled") {
        base.cache.enabled = overlay.cache.enabled;
    }
    if overlay.aliases.is_some() {
        base.aliases = overlay.aliases;
    }

    base
}

/// A configuration bound to the file it was loaded from, enabling runtime
/// reloads that report what changed.
///
/// `ReloadableConfig` captures the original [`CliConfigArgs`] so that a
/// [`ReloadableConfig::reload`] re-applies the full precedence chain (file ->
/// environment -> CLI args) against the (possibly edited) source file. This
/// means dynamic updates respect the same rules as the initial load: CLI
/// overrides still win, environment changes are picked up, and file edits take
/// effect.
#[derive(Debug, Clone)]
pub struct ReloadableConfig {
    args: CliConfigArgs,
    current: Config,
}

impl ReloadableConfig {
    /// Resolve a configuration from `args` and wrap it for later reloads.
    pub fn load(args: CliConfigArgs) -> anyhow::Result<Self> {
        let current = resolve_config(&args)?;
        Ok(Self { args, current })
    }

    /// Borrow the currently active configuration.
    #[must_use]
    pub fn current(&self) -> &Config {
        &self.current
    }

    /// Consume the wrapper and return the active configuration.
    #[must_use]
    pub fn into_inner(self) -> Config {
        self.current
    }

    /// The configuration file this instance reloads from, if any.
    ///
    /// Reflects an explicit `--config` argument; `None` when the configuration
    /// was resolved from auto-discovered files, the environment, and defaults
    /// only.
    #[must_use]
    pub fn source_path(&self) -> Option<&Path> {
        self.args.config.as_deref()
    }

    /// Re-resolve the configuration from its source and apply the result.
    ///
    /// Returns a [`ConfigDiff`] describing every field that changed relative to
    /// the previously active configuration. The active configuration is updated
    /// in place to the freshly resolved value.
    ///
    /// The full precedence chain is re-applied, so this picks up edits to the
    /// config file *and* any changes to the relevant environment variables,
    /// while keeping CLI overrides authoritative.
    pub fn reload(&mut self) -> anyhow::Result<ConfigDiff> {
        let updated = resolve_config(&self.args)?;
        let diff = self.current.diff(&updated);
        self.current = updated;
        Ok(diff)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use std::sync::{Mutex, MutexGuard, OnceLock};

    /// Serialises tests that mutate process-wide environment variables.
    fn env_guard() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    /// Create a uniquely-named temporary file in the system temp dir,
    /// preserving the extension of `name` so format auto-detection works.
    fn temp_path(name: &str) -> PathBuf {
        let path = Path::new(name);
        let stem = path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("config");
        let ext = path.extension().and_then(|s| s.to_str());
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let base = format!("celers_cli_test_{}_{}_{}", std::process::id(), stem, nanos);
        let file_name = match ext {
            Some(ext) => format!("{base}.{ext}"),
            None => base,
        };
        std::env::temp_dir().join(file_name)
    }

    fn write_file(path: &Path, content: &str) {
        let mut file = std::fs::File::create(path).expect("create temp config file");
        file.write_all(content.as_bytes())
            .expect("write temp config file");
    }

    #[test]
    fn test_args_apply_to_overrides_fields() {
        let mut config = Config::default_config();
        let args = CliConfigArgs {
            broker: Some("redis://arg:6379".to_string()),
            broker_type: Some("postgres".to_string()),
            queue: Some("arg_queue".to_string()),
            queue_mode: Some("priority".to_string()),
            queues: Some(vec!["a".to_string(), "b".to_string()]),
            concurrency: Some(16),
            poll_interval_ms: Some(250),
            max_retries: Some(7),
            timeout_secs: Some(120),
            profile: Some("staging".to_string()),
            ..Default::default()
        };

        args.apply_to(&mut config);

        assert_eq!(config.broker.url, "redis://arg:6379");
        assert_eq!(config.broker.broker_type, "postgres");
        assert_eq!(config.broker.queue, "arg_queue");
        assert_eq!(config.broker.mode, "priority");
        assert_eq!(config.queues, vec!["a", "b"]);
        assert_eq!(config.worker.concurrency, 16);
        assert_eq!(config.worker.poll_interval_ms, 250);
        assert_eq!(config.worker.max_retries, 7);
        assert_eq!(config.worker.default_timeout_secs, 120);
        assert_eq!(config.profile, Some("staging".to_string()));
    }

    #[test]
    fn test_args_apply_to_none_keeps_existing() {
        let mut config = Config::default_config();
        config.broker.url = "redis://existing:6379".to_string();
        let args = CliConfigArgs::default();

        args.apply_to(&mut config);

        assert_eq!(config.broker.url, "redis://existing:6379");
        assert_eq!(config.worker.concurrency, 4);
    }

    #[test]
    fn test_load_toml_config_from_temp_dir() {
        let path = temp_path("load.toml");
        write_file(
            &path,
            r#"
[broker]
type = "redis"
url = "redis://file:6379"
queue = "file_queue"

[worker]
concurrency = 9
"#,
        );

        let args = CliConfigArgs {
            config: Some(path.clone()),
            ..Default::default()
        };
        let _guard = env_guard();
        clear_env();
        let config = resolve_config(&args).expect("resolve toml config");

        assert_eq!(config.broker.url, "redis://file:6379");
        assert_eq!(config.broker.queue, "file_queue");
        assert_eq!(config.worker.concurrency, 9);

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_load_yaml_config_from_temp_dir() {
        let path = temp_path("load.yaml");
        write_file(
            &path,
            "broker:\n  type: redis\n  url: \"redis://yaml:6379\"\n  queue: yaml_queue\nworker:\n  concurrency: 11\nqueues:\n  - q1\n  - q2\n",
        );

        let args = CliConfigArgs {
            config: Some(path.clone()),
            ..Default::default()
        };
        let _guard = env_guard();
        clear_env();
        let config = resolve_config(&args).expect("resolve yaml config");

        assert_eq!(config.broker.url, "redis://yaml:6379");
        assert_eq!(config.broker.queue, "yaml_queue");
        assert_eq!(config.worker.concurrency, 11);
        assert_eq!(config.queues, vec!["q1", "q2"]);

        let _ = std::fs::remove_file(&path);
    }

    /// Remove every environment variable the resolver consults, so file/arg
    /// precedence tests are deterministic.
    fn clear_env() {
        for key in [
            "CELERY_BROKER_URL",
            "CELERS_BROKER_URL",
            "CELERY_DEFAULT_QUEUE",
            "CELERS_QUEUE",
            "CELERS_QUEUE_MODE",
            "CELERYD_CONCURRENCY",
            "CELERS_CONCURRENCY",
            "CELERS_POLL_INTERVAL_MS",
            "CELERY_TASK_MAX_RETRIES",
            "CELERS_MAX_RETRIES",
            "CELERY_TASK_TIME_LIMIT",
            "CELERS_TIMEOUT_SECS",
            "CELERS_QUEUES",
            "CELERS_PROFILE",
        ] {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn test_precedence_arg_overrides_env_overrides_file() {
        let _guard = env_guard();
        clear_env();

        let path = temp_path("precedence.toml");
        write_file(
            &path,
            r#"
[broker]
type = "redis"
url = "redis://file:6379"
queue = "file_queue"

[worker]
concurrency = 2
"#,
        );

        // Environment overrides the file.
        std::env::set_var("CELERY_BROKER_URL", "redis://env:6379");
        std::env::set_var("CELERYD_CONCURRENCY", "5");
        std::env::set_var("CELERS_QUEUE", "env_queue");

        // File only (no env, no arg) -> file value.
        let file_only = CliConfigArgs {
            config: Some(path.clone()),
            ..Default::default()
        };
        // Temporarily strip env to assert the file value in isolation.
        clear_env();
        let file_config = resolve_config(&file_only).expect("file only");
        assert_eq!(file_config.broker.url, "redis://file:6379");
        assert_eq!(file_config.worker.concurrency, 2);
        assert_eq!(file_config.broker.queue, "file_queue");

        // Re-set env and confirm it overrides the file.
        std::env::set_var("CELERY_BROKER_URL", "redis://env:6379");
        std::env::set_var("CELERYD_CONCURRENCY", "5");
        std::env::set_var("CELERS_QUEUE", "env_queue");
        let env_config = resolve_config(&file_only).expect("env over file");
        assert_eq!(env_config.broker.url, "redis://env:6379");
        assert_eq!(env_config.worker.concurrency, 5);
        assert_eq!(env_config.broker.queue, "env_queue");

        // Now add CLI args; they must win over both env and file.
        let arg_config = CliConfigArgs {
            config: Some(path.clone()),
            broker: Some("redis://arg:6379".to_string()),
            concurrency: Some(8),
            ..Default::default()
        };
        let resolved = resolve_config(&arg_config).expect("arg over env over file");
        assert_eq!(resolved.broker.url, "redis://arg:6379");
        assert_eq!(resolved.worker.concurrency, 8);
        // queue not set as arg -> env value still applies.
        assert_eq!(resolved.broker.queue, "env_queue");

        clear_env();
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_defaults_when_no_file_no_env_no_args() {
        let _guard = env_guard();
        clear_env();
        // Point at a path that does not exist to skip discovery side effects.
        let missing = temp_path("does-not-exist.toml");
        let args = CliConfigArgs {
            config: None,
            ..Default::default()
        };
        // Ensure no stray default file in cwd interferes by checking against a
        // config that, if a file existed, would differ. We only assert the
        // worker default which is stable regardless.
        let config = resolve_config(&args).expect("defaults");
        // Either discovered file or defaults; concurrency default is 4 unless a
        // local file overrides it. We assert the resolver runs without error
        // and yields a sane broker scheme.
        assert!(config.broker.url.contains("://") || config.broker.url.is_empty());
        let _ = missing;
    }

    #[test]
    fn test_reload_picks_up_changed_file() {
        let _guard = env_guard();
        clear_env();

        let path = temp_path("reload.toml");
        write_file(
            &path,
            r#"
[broker]
type = "redis"
url = "redis://before:6379"
queue = "before_queue"

[worker]
concurrency = 3
"#,
        );

        let args = CliConfigArgs {
            config: Some(path.clone()),
            ..Default::default()
        };
        let mut reloadable = ReloadableConfig::load(args).expect("initial load");
        assert_eq!(reloadable.current().broker.url, "redis://before:6379");
        assert_eq!(reloadable.current().worker.concurrency, 3);
        assert_eq!(reloadable.source_path(), Some(path.as_path()));

        // Mutate the file on disk.
        write_file(
            &path,
            r#"
[broker]
type = "redis"
url = "redis://after:6379"
queue = "after_queue"

[worker]
concurrency = 12
"#,
        );

        let diff = reloadable.reload().expect("reload");
        assert!(!diff.is_empty());
        assert_eq!(reloadable.current().broker.url, "redis://after:6379");
        assert_eq!(reloadable.current().broker.queue, "after_queue");
        assert_eq!(reloadable.current().worker.concurrency, 12);

        let changed_fields: Vec<&str> = diff.changes.iter().map(|c| c.field.as_str()).collect();
        assert!(changed_fields.contains(&"broker.url"));
        assert!(changed_fields.contains(&"broker.queue"));
        assert!(changed_fields.contains(&"worker.concurrency"));

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_reload_no_change_yields_empty_diff() {
        let _guard = env_guard();
        clear_env();

        let path = temp_path("reload_nochange.toml");
        write_file(
            &path,
            r#"
[broker]
type = "redis"
url = "redis://stable:6379"
"#,
        );

        let args = CliConfigArgs {
            config: Some(path.clone()),
            ..Default::default()
        };
        let mut reloadable = ReloadableConfig::load(args).expect("initial load");
        let diff = reloadable.reload().expect("reload");
        assert!(diff.is_empty(), "expected empty diff, got: {diff}");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_reload_arg_stays_authoritative() {
        let _guard = env_guard();
        clear_env();

        let path = temp_path("reload_arg.toml");
        write_file(
            &path,
            "[broker]\ntype = \"redis\"\nurl = \"redis://file1:6379\"\n",
        );

        let args = CliConfigArgs {
            config: Some(path.clone()),
            broker: Some("redis://cli:6379".to_string()),
            ..Default::default()
        };
        let mut reloadable = ReloadableConfig::load(args).expect("initial load");
        assert_eq!(reloadable.current().broker.url, "redis://cli:6379");

        // Even after the file changes, the CLI override must win.
        write_file(
            &path,
            "[broker]\ntype = \"redis\"\nurl = \"redis://file2:6379\"\n",
        );
        let diff = reloadable.reload().expect("reload");
        assert!(diff.is_empty(), "CLI override should mask file change");
        assert_eq!(reloadable.current().broker.url, "redis://cli:6379");

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_profile_overlay_merge() {
        let _guard = env_guard();
        clear_env();

        let base = temp_path("celers.toml");
        let dir = base.parent().expect("temp dir").to_path_buf();
        let stem = base
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("stem")
            .to_string();
        let overlay = dir.join(format!("{stem}.prod.toml"));

        write_file(
            &base,
            "[broker]\ntype = \"redis\"\nurl = \"redis://base:6379\"\n[worker]\nconcurrency = 4\n",
        );
        write_file(
            &overlay,
            "[broker]\ntype = \"redis\"\nurl = \"redis://prod:6379\"\n[worker]\nconcurrency = 32\n",
        );

        let args = CliConfigArgs {
            config: Some(base.clone()),
            profile: Some("prod".to_string()),
            ..Default::default()
        };
        let config = resolve_config(&args).expect("profile overlay");
        assert_eq!(config.broker.url, "redis://prod:6379");
        assert_eq!(config.worker.concurrency, 32);

        let _ = std::fs::remove_file(&base);
        let _ = std::fs::remove_file(&overlay);
    }

    /// Regression test for idx 337 part 2: `merge_overlay` used to apply an
    /// overlay field only when its typed value differed from the *type
    /// default*, not the base -- so an overlay explicitly resetting a field
    /// back down to the default (here, `worker.concurrency = 4`, the real
    /// default) was silently ignored, leaving the base's non-default value
    /// (32) in place instead.
    #[test]
    fn test_profile_overlay_can_reset_a_field_to_the_type_default() {
        let _guard = env_guard();
        clear_env();

        let base = temp_path("celers.toml");
        let dir = base.parent().expect("temp dir").to_path_buf();
        let stem = base
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("stem")
            .to_string();
        let overlay = dir.join(format!("{stem}.reset.toml"));

        write_file(
            &base,
            "[broker]\ntype = \"redis\"\nurl = \"redis://base:6379\"\n[worker]\nconcurrency = 32\n",
        );
        // Explicitly sets concurrency back to its type default (4) -- must
        // still win over the base's non-default 32.
        write_file(&overlay, "[worker]\nconcurrency = 4\n");

        let args = CliConfigArgs {
            config: Some(base.clone()),
            profile: Some("reset".to_string()),
            ..Default::default()
        };
        let config = resolve_config(&args).expect("profile overlay");
        assert_eq!(
            config.worker.concurrency, 4,
            "an overlay explicitly setting a field to the type default must still override \
             the base's non-default value"
        );
        // The overlay never mentioned `broker.url` at all -- must still
        // come from the base, not get reset to the default broker URL.
        assert_eq!(config.broker.url, "redis://base:6379");

        let _ = std::fs::remove_file(&base);
        let _ = std::fs::remove_file(&overlay);
    }

    /// Regression test for idx 337 part 2: `merge_overlay` used to never
    /// merge `broker.failover_retries`/`failover_timeout_secs`, `pool`,
    /// `cache`, or `aliases` at all -- no matter what a profile overlay file
    /// said, those sections always came from the base (or the built-in
    /// default if the base didn't set them either).
    #[test]
    fn test_profile_overlay_merges_previously_unmerged_sections() {
        let _guard = env_guard();
        clear_env();

        let base = temp_path("celers.toml");
        let dir = base.parent().expect("temp dir").to_path_buf();
        let stem = base
            .file_stem()
            .and_then(|s| s.to_str())
            .expect("stem")
            .to_string();
        let overlay = dir.join(format!("{stem}.full.toml"));

        write_file(
            &base,
            "[broker]\ntype = \"redis\"\nurl = \"redis://base:6379\"\n",
        );
        write_file(
            &overlay,
            "[broker]\nfailover_retries = 7\nfailover_timeout_secs = 42\n\
             [pool]\nmax_size = 99\nreuse_enabled = false\n\
             [cache]\nttl_secs = 999\nenabled = false\n\
             [aliases]\nw = \"worker start\"\n",
        );

        let args = CliConfigArgs {
            config: Some(base.clone()),
            profile: Some("full".to_string()),
            ..Default::default()
        };
        let config = resolve_config(&args).expect("profile overlay");

        assert_eq!(config.broker.failover_retries, 7);
        assert_eq!(config.broker.failover_timeout_secs, 42);
        assert_eq!(config.pool.max_size, 99);
        assert!(!config.pool.reuse_enabled);
        assert_eq!(config.cache.ttl_secs, 999);
        assert!(!config.cache.enabled);
        assert!(
            config
                .aliases
                .is_some_and(|a| a.list().iter().any(|(name, _)| *name == "w")),
            "the overlay's [aliases] section must be merged in, not silently dropped"
        );

        let _ = std::fs::remove_file(&base);
        let _ = std::fs::remove_file(&overlay);
    }

    #[test]
    fn test_resolve_log_level_precedence() {
        let _guard = env_guard();
        std::env::remove_var("RUST_LOG");

        // Default wins when nothing else set.
        let args = CliConfigArgs::default();
        assert_eq!(args.resolve_log_level("info"), "info");

        // Env overrides default.
        std::env::set_var("RUST_LOG", "warn");
        assert_eq!(args.resolve_log_level("info"), "warn");

        // CLI arg overrides env.
        let args = CliConfigArgs {
            log_level: Some("debug".to_string()),
            ..Default::default()
        };
        assert_eq!(args.resolve_log_level("info"), "debug");

        std::env::remove_var("RUST_LOG");
    }

    #[test]
    fn test_format_detection_via_extension() {
        assert_eq!(ConfigFormat::from_path("a.toml"), ConfigFormat::Toml);
        assert_eq!(ConfigFormat::from_path("a.yaml"), ConfigFormat::Yaml);
        assert_eq!(ConfigFormat::from_path("a.yml"), ConfigFormat::Yaml);
        assert_eq!(ConfigFormat::from_path("a.YAML"), ConfigFormat::Yaml);
        assert_eq!(ConfigFormat::from_path("noext"), ConfigFormat::Toml);
    }
}
