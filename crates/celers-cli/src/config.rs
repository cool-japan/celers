//! Configuration file support for `CeleRS` CLI.
//!
//! This module provides configuration management for the `CeleRS` CLI tool, including:
//! - TOML file parsing and validation
//! - Environment variable expansion
//! - Configuration profiles (dev, staging, prod)
//! - Broker and worker settings
//! - Auto-scaling and alerting configuration
//!
//! # Configuration File Format
//!
//! The configuration file uses TOML format and supports the following sections:
//! - `[broker]`: Broker connection settings (Redis, `PostgreSQL`, etc.)
//! - `[worker]`: Worker runtime settings (concurrency, retries, timeouts)
//! - `[autoscale]`: Auto-scaling configuration
//! - `[alerts]`: Alert and notification settings
//! - `[pool]`: CLI-level connection pool settings (max size, reuse tracking)
//! - `[cache]`: Read-path TTL cache settings (queue stats, worker lists, ...)
//!
//! # Environment Variables
//!
//! Configuration values can reference environment variables using the syntax:
//! - `${VAR_NAME}` - Required environment variable
//! - `${VAR_NAME:default}` - Optional with default value
//!
//! # Examples
//!
//! ```toml
//! [broker]
//! type = "redis"
//! url = "${REDIS_URL:redis://localhost:6379}"
//! queue = "my_queue"
//!
//! [worker]
//! concurrency = 4
//! max_retries = 3
//! ```
//!
//! # Usage
//!
//! ```no_run
//! use celers_cli::config::Config;
//! use std::path::PathBuf;
//!
//! # fn main() -> anyhow::Result<()> {
//! // Load from file
//! let config = Config::from_file(PathBuf::from("celers.toml"))?;
//!
//! // Get default configuration
//! let default_config = Config::default_config();
//!
//! // Save to file
//! default_config.to_file("celers.toml")?;
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::env;
use std::path::Path;
use std::sync::{OnceLock, RwLock};
use tracing::warn;

/// Serialization format used for a configuration file.
///
/// The format is normally auto-detected from a file's extension via
/// [`ConfigFormat::from_path`], falling back to TOML for unknown or missing
/// extensions.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConfigFormat {
    /// TOML format (`.toml`).
    Toml,
    /// YAML format (`.yaml` or `.yml`).
    Yaml,
}

impl ConfigFormat {
    /// Detect the configuration format from a file path's extension.
    ///
    /// Recognises `.yaml`/`.yml` as [`ConfigFormat::Yaml`] and everything else
    /// (including `.toml` and paths without an extension) as
    /// [`ConfigFormat::Toml`]. Matching is case-insensitive.
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_cli::config::ConfigFormat;
    /// use std::path::Path;
    ///
    /// assert_eq!(ConfigFormat::from_path(Path::new("c.yaml")), ConfigFormat::Yaml);
    /// assert_eq!(ConfigFormat::from_path(Path::new("c.YML")), ConfigFormat::Yaml);
    /// assert_eq!(ConfigFormat::from_path(Path::new("c.toml")), ConfigFormat::Toml);
    /// assert_eq!(ConfigFormat::from_path(Path::new("celers")), ConfigFormat::Toml);
    /// ```
    #[must_use]
    pub fn from_path<P: AsRef<Path>>(path: P) -> Self {
        match path
            .as_ref()
            .extension()
            .and_then(|ext| ext.to_str())
            .map(str::to_ascii_lowercase)
            .as_deref()
        {
            Some("yaml" | "yml") => ConfigFormat::Yaml,
            _ => ConfigFormat::Toml,
        }
    }
}

/// Main CLI configuration structure.
///
/// Contains all settings for broker connection, worker configuration,
/// auto-scaling, and alerting. Can be loaded from TOML files with
/// environment variable support.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Configuration profile name (dev, staging, prod, etc.)
    #[serde(default)]
    pub profile: Option<String>,

    /// Broker configuration
    pub broker: BrokerConfig,

    /// Worker configuration
    #[serde(default)]
    pub worker: WorkerConfig,

    /// Queue names
    #[serde(default)]
    pub queues: Vec<String>,

    /// Auto-scaling configuration
    #[serde(default)]
    pub autoscale: Option<AutoScaleConfig>,

    /// Alert configuration
    #[serde(default)]
    pub alerts: Option<AlertConfig>,

    /// CLI-level connection pool configuration
    #[serde(default)]
    pub pool: PoolConfig,

    /// Read-path TTL cache configuration
    #[serde(default)]
    pub cache: CacheConfig,

    /// User-defined command aliases (`celers alias add|remove|list`), e.g. `w` -> `worker start`.
    #[serde(default)]
    pub aliases: Option<crate::aliases::AliasConfig>,
}

/// Auto-scaling configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AutoScaleConfig {
    /// Enable auto-scaling
    #[serde(default)]
    pub enabled: bool,

    /// Minimum number of workers
    #[serde(default = "default_min_workers")]
    pub min_workers: usize,

    /// Maximum number of workers
    #[serde(default = "default_max_workers")]
    pub max_workers: usize,

    /// Queue depth threshold for scaling up
    #[serde(default = "default_scale_up_threshold")]
    pub scale_up_threshold: usize,

    /// Queue depth threshold for scaling down
    #[serde(default = "default_scale_down_threshold")]
    pub scale_down_threshold: usize,

    /// Check interval in seconds
    #[serde(default = "default_autoscale_check_interval")]
    pub check_interval_secs: u64,
}

/// Alert configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AlertConfig {
    /// Enable alerts
    #[serde(default)]
    pub enabled: bool,

    /// Webhook URL for notifications
    pub webhook_url: Option<String>,

    /// DLQ size threshold for alerts
    #[serde(default = "default_dlq_threshold")]
    pub dlq_threshold: usize,

    /// Failed task threshold for alerts
    #[serde(default = "default_failed_threshold")]
    pub failed_threshold: usize,

    /// Alert check interval in seconds
    #[serde(default = "default_alert_check_interval")]
    pub check_interval_secs: u64,
}

/// Broker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    /// Broker type (redis or postgres)
    #[serde(rename = "type")]
    pub broker_type: String,

    /// Connection URL
    pub url: String,

    /// Failover broker URLs (optional)
    #[serde(default)]
    pub failover_urls: Vec<String>,

    /// Failover retry attempts
    #[serde(default = "default_failover_retries")]
    pub failover_retries: u32,

    /// Failover timeout in seconds
    #[serde(default = "default_failover_timeout")]
    pub failover_timeout_secs: u64,

    /// Default queue name
    #[serde(default = "default_queue_name")]
    pub queue: String,

    /// Queue mode (fifo or priority)
    #[serde(default = "default_queue_mode")]
    pub mode: String,
}

/// Worker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerConfig {
    /// Number of concurrent tasks
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,

    /// Poll interval in milliseconds
    #[serde(default = "default_poll_interval")]
    pub poll_interval_ms: u64,

    /// Maximum number of retries
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,

    /// Default task timeout in seconds
    #[serde(default = "default_timeout")]
    pub default_timeout_secs: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            concurrency: default_concurrency(),
            poll_interval_ms: default_poll_interval(),
            max_retries: default_max_retries(),
            default_timeout_secs: default_timeout(),
        }
    }
}

/// CLI-level connection pool configuration.
///
/// Governs [`crate::pool::ClientPool`], the process-local pool of reusable
/// broker connections used by the `queue`/`worker`/`task` read paths (see
/// `crate::pool::redis_connection_pool`). This is distinct from the
/// lower-level `PostgresBroker`/`sqlx`-style database connection pool
/// reported by [`crate::commands::db_pool_stats`]: that pool manages actual
/// database connections for `celers db` commands, while this one manages
/// short-lived CLI-process broker handles.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PoolConfig {
    /// Maximum number of distinct pooled connections (keyed by broker URL)
    /// to keep alive at once.
    #[serde(default = "default_pool_max_size")]
    pub max_size: usize,

    /// Whether connection reuse is enabled. When `false`, every read-path
    /// call reconnects instead of reusing a pooled handle (reuse tracking
    /// still runs, but will always report zero reuse).
    #[serde(default = "default_pool_reuse_enabled")]
    pub reuse_enabled: bool,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_size: default_pool_max_size(),
            reuse_enabled: default_pool_reuse_enabled(),
        }
    }
}

/// Process-wide snapshot of the `[pool]`/`[cache]` sections of the most
/// recently loaded configuration *file* (before any environment-variable
/// layer is applied to them), populated by [`Config::apply_env_overrides`].
///
/// [`PoolConfig::from_env_or_default`]/[`CacheConfig::from_env_or_default`]
/// are called directly by `commands::queue`/`commands::worker`/
/// `commands::task`'s read paths, which are reached from CLI dispatch
/// without a loaded [`Config`] in scope (see their doc comments) -- so
/// without this, a `celers.toml` `[pool]`/`[cache]` section was silently
/// never consulted by anything: every one of those call sites only ever saw
/// environment variables layered on top of this struct's *hardcoded* type
/// defaults, never the file's values (idx 337).
///
/// This stores only the *file* layer (not the fully env-resolved values) so
/// that `from_env_or_default`'s own env-var checks stay live and
/// independently authoritative -- it only changes what they fall back to
/// when no environment variable is set, from a hardcoded default to
/// whatever the file specified (or the hardcoded default, if no file was
/// loaded either). A `RwLock` (rather than a write-once `OnceLock<T>`)
/// because a long-lived process (`celers interactive`, or library use of
/// this crate) may resolve configuration more than once over its lifetime.
fn file_pool_cache_floor() -> &'static RwLock<Option<(PoolConfig, CacheConfig)>> {
    static FLOOR: OnceLock<RwLock<Option<(PoolConfig, CacheConfig)>>> = OnceLock::new();
    FLOOR.get_or_init(|| RwLock::new(None))
}

/// Read the current file-provided pool/cache floor, or `PoolConfig`/
/// `CacheConfig`'s hardcoded type defaults if no configuration has been
/// resolved yet in this process (e.g. a unit test constructing these types
/// directly, or a library caller that never called `apply_env_overrides`).
fn file_pool_cache_floor_or_default() -> (PoolConfig, CacheConfig) {
    file_pool_cache_floor()
        .read()
        .ok()
        .and_then(|guard| guard.clone())
        .unwrap_or_else(|| (PoolConfig::default(), CacheConfig::default()))
}

impl PoolConfig {
    /// Effective pool configuration, honoring `CELERS_POOL_MAX_SIZE` /
    /// `CELERS_POOL_REUSE_ENABLED` environment overrides (mirroring
    /// [`Config::apply_env_overrides`]'s mechanism for broker/worker
    /// settings) and otherwise falling back to whichever configuration file
    /// was most recently resolved in this process (via
    /// [`Config::apply_env_overrides`]), or [`PoolConfig::default`] if none
    /// was (idx 337).
    ///
    /// Command read paths in `commands::queue`/`commands::worker`/
    /// `commands::task` are reached directly from CLI dispatch without a
    /// loaded [`Config`] in scope, so they use this as their source of truth
    /// for pool sizing, keeping it configurable without threading a `Config`
    /// through every call site.
    #[must_use]
    pub fn from_env_or_default() -> Self {
        let (defaults, _) = file_pool_cache_floor_or_default();
        Self {
            max_size: first_env_parsed(&["CELERS_POOL_MAX_SIZE"]).unwrap_or(defaults.max_size),
            reuse_enabled: first_env_parsed(&["CELERS_POOL_REUSE_ENABLED"])
                .unwrap_or(defaults.reuse_enabled),
        }
    }
}

/// Read-path TTL cache configuration.
///
/// Governs [`crate::cache::TtlCache`] instances used by the `queue`/`worker`
/// read paths (`list_queues`, `queue_stats`, `list_workers`, `worker_stats`)
/// to avoid redundant broker round trips for frequently-read, slowly-changing
/// data.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CacheConfig {
    /// How long a cached entry remains valid, in seconds.
    #[serde(default = "default_cache_ttl_secs")]
    pub ttl_secs: u64,

    /// Whether read-path caching is enabled at all. When `false`, callers
    /// should treat every lookup as a miss and always fetch fresh data.
    #[serde(default = "default_cache_enabled")]
    pub enabled: bool,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            ttl_secs: default_cache_ttl_secs(),
            enabled: default_cache_enabled(),
        }
    }
}

impl CacheConfig {
    /// Effective cache configuration, honoring `CELERS_CACHE_TTL_SECS` /
    /// `CELERS_CACHE_ENABLED` environment overrides and otherwise falling
    /// back to whichever configuration file was most recently resolved in
    /// this process, or [`CacheConfig::default`] if none was (idx 337). See
    /// [`PoolConfig::from_env_or_default`] for why the read paths use this
    /// instead of a threaded-through [`Config`].
    #[must_use]
    pub fn from_env_or_default() -> Self {
        let (_, defaults) = file_pool_cache_floor_or_default();
        Self {
            ttl_secs: first_env_parsed(&["CELERS_CACHE_TTL_SECS"]).unwrap_or(defaults.ttl_secs),
            enabled: first_env_parsed(&["CELERS_CACHE_ENABLED"]).unwrap_or(defaults.enabled),
        }
    }

    /// The configured time-to-live as a [`std::time::Duration`], for direct
    /// use with [`crate::cache::TtlCache::new`].
    #[must_use]
    pub fn ttl(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.ttl_secs)
    }
}

/// The hardcoded fallback broker URL used by [`Config::default_config`] when
/// no configuration file is found at all. Also consulted by
/// [`Config::apply_env_overrides`] as the "no file supplied a `broker.url`"
/// signal for its `REDIS_URL`/`AMQP_URL` fallback precedence (idx 338) -- see
/// that method's doc comment for the caveat this implies.
const DEFAULT_BROKER_URL: &str = "redis://localhost:6379";

fn default_pool_max_size() -> usize {
    16
}

fn default_pool_reuse_enabled() -> bool {
    true
}

fn default_cache_ttl_secs() -> u64 {
    30
}

fn default_cache_enabled() -> bool {
    true
}

fn default_queue_name() -> String {
    "celers".to_string()
}

fn default_queue_mode() -> String {
    "fifo".to_string()
}

fn default_concurrency() -> usize {
    4
}

fn default_poll_interval() -> u64 {
    1000
}

fn default_max_retries() -> u32 {
    3
}

fn default_timeout() -> u64 {
    300
}

fn default_failover_retries() -> u32 {
    3
}

fn default_failover_timeout() -> u64 {
    5
}

fn default_min_workers() -> usize {
    1
}

fn default_max_workers() -> usize {
    10
}

fn default_scale_up_threshold() -> usize {
    100
}

fn default_scale_down_threshold() -> usize {
    10
}

fn default_autoscale_check_interval() -> u64 {
    30
}

fn default_dlq_threshold() -> usize {
    50
}

fn default_failed_threshold() -> usize {
    100
}

fn default_alert_check_interval() -> u64 {
    60
}

/// Expand environment variables in a string
/// Supports ${VAR} and ${`VAR:default_value`} syntax
fn expand_env_vars(s: &str) -> String {
    let mut result = s.to_string();
    let mut start_idx = 0;

    while let Some(start) = result[start_idx..].find("${") {
        let start = start_idx + start;
        if let Some(end) = result[start..].find('}') {
            let end = start + end;
            let var_expr = &result[start + 2..end];

            // Parse variable name and default value
            let (var_name, default_value) = if let Some(colon_idx) = var_expr.find(':') {
                let var_name = &var_expr[..colon_idx];
                let default = &var_expr[colon_idx + 1..];
                (var_name, Some(default))
            } else {
                (var_expr, None)
            };

            // Get environment variable value or use default
            let value = env::var(var_name)
                .ok()
                .or_else(|| default_value.map(String::from));

            if let Some(value) = value {
                result.replace_range(start..=end, &value);
                start_idx = start + value.len();
            } else {
                // No value found and no default, keep original and move forward
                start_idx = end + 1;
            }
        } else {
            break;
        }
    }

    result
}

impl Config {
    /// Load configuration from a file, auto-detecting the format (TOML or YAML)
    /// from the file extension and expanding environment variables.
    ///
    /// `.yaml`/`.yml` files are parsed as YAML; everything else is parsed as
    /// TOML. Environment variables in the file content are expanded using the
    /// `${VAR}` / `${VAR:default}` syntax before parsing.
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let format = ConfigFormat::from_path(&path);
        let content = std::fs::read_to_string(&path)?;
        let expanded_content = expand_env_vars(&content);
        Self::from_str_with_format(&expanded_content, format)
    }

    /// Parse configuration from a string using an explicit format.
    ///
    /// Unlike [`Config::from_file`] this does **not** perform environment
    /// variable expansion; callers that need it should expand the content
    /// first.
    pub fn from_str_with_format(content: &str, format: ConfigFormat) -> anyhow::Result<Self> {
        let config = match format {
            ConfigFormat::Toml => toml::from_str(content)?,
            ConfigFormat::Yaml => serde_yaml::from_str(content)?,
        };
        Ok(config)
    }

    /// Serialize this configuration to a string in the requested format.
    pub fn to_string_with_format(&self, format: ConfigFormat) -> anyhow::Result<String> {
        let content = match format {
            ConfigFormat::Toml => toml::to_string_pretty(self)?,
            ConfigFormat::Yaml => serde_yaml::to_string(self)?,
        };
        Ok(content)
    }

    /// Save configuration to a file, auto-detecting the format from the
    /// file extension.
    ///
    /// `.yaml`/`.yml` paths are written as YAML; everything else is written as
    /// TOML.
    pub fn to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let format = ConfigFormat::from_path(&path);
        let content = self.to_string_with_format(format)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Update just the `[aliases]` section of the on-disk configuration file
    /// at `path`, re-reading and re-writing every other section exactly as
    /// it already exists on disk.
    ///
    /// This exists so a mutating command like `celers alias add`/`remove`
    /// can persist without going through the fully env/CLI-arg-resolved
    /// in-memory [`Config`] (as returned by
    /// [`crate::config_layer::resolve_config`]) and calling
    /// [`Config::to_file`] on *that* -- doing so bakes every resolved field
    /// back into the file, including a `broker.url` an environment variable
    /// (e.g. a PaaS platform's auto-injected `REDIS_URL`) may have supplied
    /// only for this one process, permanently overwriting whatever the file
    /// actually said (idx 338). This reads the file fresh (falling back to
    /// [`Config::default_config`] when `path` does not exist yet, mirroring
    /// [`crate::config_layer::resolve_config_path`]'s "first default name"
    /// contract for a mutating command run before `celers init`), mutates
    /// only `aliases`, and writes that back -- every other section is
    /// preserved byte-for-byte as it was on disk.
    ///
    /// NOTE: as of this fix, nothing calls this yet -- `cli::dispatch`'s
    /// `Commands::Alias` handler (outside this module) still does
    /// `load_config(None)?.to_file(...)` on the fully resolved config. See
    /// the crate-level followups. `#[allow(dead_code)]` (matching this
    /// file's existing `from_file_with_profile`/`merge_with`) since this is
    /// public, tested API ready for that handler to adopt, not unreachable
    /// code left over from a removed feature.
    #[allow(dead_code)]
    pub fn write_aliases_only<P: AsRef<Path>>(
        path: P,
        aliases: &crate::aliases::AliasConfig,
    ) -> anyhow::Result<()> {
        let path = path.as_ref();
        let mut on_disk = if path.exists() {
            Self::from_file(path)?
        } else {
            Self::default_config()
        };
        on_disk.aliases = Some(aliases.clone());
        on_disk.to_file(path)
    }

    /// Create a default configuration file
    pub fn default_config() -> Self {
        Self {
            profile: None,
            broker: BrokerConfig {
                broker_type: "redis".to_string(),
                url: DEFAULT_BROKER_URL.to_string(),
                failover_urls: vec![],
                failover_retries: default_failover_retries(),
                failover_timeout_secs: default_failover_timeout(),
                queue: "celers".to_string(),
                mode: "fifo".to_string(),
            },
            worker: WorkerConfig::default(),
            queues: vec!["celers".to_string()],
            autoscale: None,
            alerts: None,
            pool: PoolConfig::default(),
            cache: CacheConfig::default(),
            aliases: None,
        }
    }

    /// Load configuration for a specific profile
    #[allow(dead_code)]
    pub fn from_file_with_profile<P: AsRef<Path>>(path: P, profile: &str) -> anyhow::Result<Self> {
        let base_config = Self::from_file(&path)?;

        // Try to load profile-specific configuration
        let profile_path = path
            .as_ref()
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(format!("celers.{profile}.toml"));

        if profile_path.exists() {
            let profile_config = Self::from_file(profile_path)?;
            Ok(base_config.merge_with(profile_config))
        } else {
            Ok(base_config)
        }
    }

    /// Merge this configuration with another, with the other taking precedence
    #[allow(dead_code)]
    fn merge_with(mut self, other: Self) -> Self {
        // Merge broker config
        if !other.broker.url.is_empty() {
            self.broker.url = other.broker.url;
        }
        if !other.broker.broker_type.is_empty() {
            self.broker.broker_type = other.broker.broker_type;
        }
        if !other.broker.failover_urls.is_empty() {
            self.broker.failover_urls = other.broker.failover_urls;
        }

        // Merge worker config
        if other.worker.concurrency > 0 {
            self.worker.concurrency = other.worker.concurrency;
        }

        // Merge queues
        if !other.queues.is_empty() {
            self.queues = other.queues;
        }

        // Merge autoscale
        if other.autoscale.is_some() {
            self.autoscale = other.autoscale;
        }

        // Merge alerts
        if other.alerts.is_some() {
            self.alerts = other.alerts;
        }

        self
    }

    /// Validate configuration settings
    pub fn validate(&self) -> anyhow::Result<Vec<String>> {
        let mut warnings = Vec::new();

        // Validate broker type
        let valid_broker_types = [
            "redis",
            "postgres",
            "postgresql",
            "mysql",
            "amqp",
            "rabbitmq",
            "sqs",
        ];
        if !valid_broker_types.contains(&self.broker.broker_type.to_lowercase().as_str()) {
            warnings.push(format!(
                "Unknown broker type '{}'. Supported types: {}",
                self.broker.broker_type,
                valid_broker_types.join(", ")
            ));
        }

        // Validate queue mode
        if self.broker.mode != "fifo" && self.broker.mode != "priority" {
            warnings.push(format!(
                "Unknown queue mode '{}'. Expected 'fifo' or 'priority'",
                self.broker.mode
            ));
        }

        // Validate worker configuration
        if self.worker.concurrency == 0 {
            warnings.push("Concurrency is 0 - worker will not process any tasks".to_string());
        } else if self.worker.concurrency > 100 {
            warnings.push(format!(
                "High concurrency ({}) may cause resource exhaustion",
                self.worker.concurrency
            ));
        }

        if self.worker.poll_interval_ms < 100 {
            warnings.push("Very low poll interval may cause excessive CPU usage".to_string());
        }

        // Validate autoscale configuration
        if let Some(ref autoscale) = self.autoscale {
            if autoscale.enabled {
                if autoscale.min_workers == 0 {
                    warnings.push("Autoscale min_workers is 0".to_string());
                }
                if autoscale.max_workers < autoscale.min_workers {
                    warnings.push(format!(
                        "Autoscale max_workers ({}) is less than min_workers ({})",
                        autoscale.max_workers, autoscale.min_workers
                    ));
                }
                if autoscale.scale_down_threshold >= autoscale.scale_up_threshold {
                    warnings.push(
                        "Autoscale scale_down_threshold should be less than scale_up_threshold"
                            .to_string(),
                    );
                }
            }
        }

        // Validate alert configuration
        if let Some(ref alerts) = self.alerts {
            if alerts.enabled && alerts.webhook_url.is_none() {
                warnings.push("Alerts enabled but no webhook_url configured".to_string());
            }
        }

        Ok(warnings)
    }

    /// Apply environment-variable overrides to this configuration.
    ///
    /// Recognised variables (in addition to the `${VAR}` expansion performed
    /// when loading files) follow the upstream Celery `CELERY_*` / `CELERYD_*`
    /// conventions, with `CELERS_*` aliases accepted for convenience:
    ///
    /// - `CELERY_BROKER_URL` / `CELERS_BROKER_URL` -> `broker.url`
    /// - `CELERY_DEFAULT_QUEUE` / `CELERS_QUEUE` -> `broker.queue`
    /// - `CELERS_QUEUE_MODE` -> `broker.mode`
    /// - `CELERYD_CONCURRENCY` / `CELERS_CONCURRENCY` -> `worker.concurrency`
    /// - `CELERS_POLL_INTERVAL_MS` -> `worker.poll_interval_ms`
    /// - `CELERY_TASK_MAX_RETRIES` / `CELERS_MAX_RETRIES` -> `worker.max_retries`
    /// - `CELERY_TASK_TIME_LIMIT` / `CELERS_TIMEOUT_SECS` -> `worker.default_timeout_secs`
    /// - `CELERS_QUEUES` -> `queues` (comma-separated)
    /// - `CELERS_PROFILE` -> `profile`
    /// - `CELERS_POOL_MAX_SIZE` / `CELERS_POOL_REUSE_ENABLED` -> `pool.*`
    /// - `CELERS_CACHE_TTL_SECS` / `CELERS_CACHE_ENABLED` -> `cache.*`
    ///
    /// Variables that are unset are ignored, leaving the existing value
    /// (typically whatever the config file set, or a hardcoded default if
    /// no file was loaded) untouched. Variables that are *set* but fail to
    /// parse are also ignored the same way, but logged via [`tracing::warn`]
    /// (idx 337) so a typo like `CELERS_CONCURRENCY=abc` or
    /// `CELERS_MAX_RETRIES=-1` does not silently vanish -- see
    /// [`first_env_parsed`].
    ///
    /// When neither `CELERY_BROKER_URL` nor `CELERS_BROKER_URL` is set, and
    /// `broker.url` is still exactly [`DEFAULT_BROKER_URL`] (i.e. no config
    /// file supplied one -- `BrokerConfig::url` has no `#[serde(default)]`,
    /// so parsing *any* file that omits it fails outright, meaning this
    /// field can only still hold the hardcoded default here if
    /// [`Config::default_config`] was used because no file was found at
    /// all), `broker.url` falls back to
    /// [`crate::smart_defaults::detect_broker_from_env`], which additionally
    /// recognizes the wider `REDIS_URL` / `AMQP_URL` hosting-provider
    /// conventions. This keeps the overall precedence as CLI arg (applied by
    /// the caller, not here) > explicit `CELERY_BROKER_URL` /
    /// `CELERS_BROKER_URL` > an explicit `broker.url` from a config file >
    /// `REDIS_URL` / `AMQP_URL` fallback > hardcoded default -- previously,
    /// a generic `REDIS_URL` a PaaS platform injects automatically (Heroku,
    /// Railway, Render, docker-compose, ...) silently overrode an explicit
    /// `broker.url = "amqp://..."` from `celers.toml` with no warning (idx
    /// 338). NOTE: this is a value-based heuristic, not true provenance
    /// tracking -- a config file that explicitly sets `broker.url` to
    /// exactly [`DEFAULT_BROKER_URL`] is indistinguishable here from "no
    /// file was loaded at all", so it would still be overridden by
    /// `REDIS_URL`/`AMQP_URL`. Precisely tracking "was this explicitly set"
    /// would need `BrokerConfig::url` to become `Option<String>`, which
    /// cascades into `config_layer::merge_overlay` and
    /// `CliConfigArgs::apply_to` (outside this module) -- see the
    /// crate-level followups.
    pub fn apply_env_overrides(&mut self) {
        // Snapshot the file/default-provided `[pool]`/`[cache]` sections
        // *before* the env layer below (if any) mutates them, so
        // `PoolConfig`/`CacheConfig::from_env_or_default` (called from
        // read-path command implementations with no `Config` in scope --
        // see their doc comments) can fall back to the file's values
        // instead of always falling back to a hardcoded type default (idx
        // 337).
        if let Ok(mut floor) = file_pool_cache_floor().write() {
            *floor = Some((self.pool.clone(), self.cache.clone()));
        }

        if let Some(url) = first_env(&["CELERY_BROKER_URL", "CELERS_BROKER_URL"]) {
            self.broker.url = url;
        } else if self.broker.url == DEFAULT_BROKER_URL {
            if let Some(url) = crate::smart_defaults::detect_broker_from_env() {
                self.broker.url = url;
            }
        }
        if let Some(queue) = first_env(&["CELERY_DEFAULT_QUEUE", "CELERS_QUEUE"]) {
            self.broker.queue = queue;
        }
        if let Some(mode) = first_env(&["CELERS_QUEUE_MODE"]) {
            self.broker.mode = mode;
        }
        if let Some(concurrency) = first_env_parsed(&["CELERYD_CONCURRENCY", "CELERS_CONCURRENCY"])
        {
            self.worker.concurrency = concurrency;
        }
        if let Some(poll) = first_env_parsed(&["CELERS_POLL_INTERVAL_MS"]) {
            self.worker.poll_interval_ms = poll;
        }
        if let Some(retries) = first_env_parsed(&["CELERY_TASK_MAX_RETRIES", "CELERS_MAX_RETRIES"])
        {
            self.worker.max_retries = retries;
        }
        if let Some(timeout) = first_env_parsed(&["CELERY_TASK_TIME_LIMIT", "CELERS_TIMEOUT_SECS"])
        {
            self.worker.default_timeout_secs = timeout;
        }
        if let Some(queues) = first_env(&["CELERS_QUEUES"]) {
            let parsed: Vec<String> = queues
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect();
            if !parsed.is_empty() {
                self.queues = parsed;
            }
        }
        if let Some(profile) = first_env(&["CELERS_PROFILE"]) {
            self.profile = Some(profile);
        }
        if let Some(max_size) = first_env_parsed(&["CELERS_POOL_MAX_SIZE"]) {
            self.pool.max_size = max_size;
        }
        if let Some(reuse_enabled) = first_env_parsed(&["CELERS_POOL_REUSE_ENABLED"]) {
            self.pool.reuse_enabled = reuse_enabled;
        }
        if let Some(ttl_secs) = first_env_parsed(&["CELERS_CACHE_TTL_SECS"]) {
            self.cache.ttl_secs = ttl_secs;
        }
        if let Some(enabled) = first_env_parsed(&["CELERS_CACHE_ENABLED"]) {
            self.cache.enabled = enabled;
        }
    }

    /// Compute a human-readable diff between this configuration (the previous
    /// state) and an updated configuration.
    ///
    /// Used by the dynamic-reload machinery to report what changed when a
    /// configuration source is reloaded. Only the operationally significant
    /// fields are compared.
    #[must_use]
    pub fn diff(&self, updated: &Config) -> ConfigDiff {
        let mut changes = Vec::new();

        push_change_opt(&mut changes, "profile", &self.profile, &updated.profile);
        push_change(
            &mut changes,
            "broker.type",
            &self.broker.broker_type,
            &updated.broker.broker_type,
        );
        push_change(
            &mut changes,
            "broker.url",
            &self.broker.url,
            &updated.broker.url,
        );
        push_change(
            &mut changes,
            "broker.queue",
            &self.broker.queue,
            &updated.broker.queue,
        );
        push_change(
            &mut changes,
            "broker.mode",
            &self.broker.mode,
            &updated.broker.mode,
        );
        push_change(
            &mut changes,
            "broker.failover_urls",
            &format!("{:?}", self.broker.failover_urls),
            &format!("{:?}", updated.broker.failover_urls),
        );
        push_change(
            &mut changes,
            "worker.concurrency",
            &self.worker.concurrency,
            &updated.worker.concurrency,
        );
        push_change(
            &mut changes,
            "worker.poll_interval_ms",
            &self.worker.poll_interval_ms,
            &updated.worker.poll_interval_ms,
        );
        push_change(
            &mut changes,
            "worker.max_retries",
            &self.worker.max_retries,
            &updated.worker.max_retries,
        );
        push_change(
            &mut changes,
            "worker.default_timeout_secs",
            &self.worker.default_timeout_secs,
            &updated.worker.default_timeout_secs,
        );
        push_change(
            &mut changes,
            "queues",
            &format!("{:?}", self.queues),
            &format!("{:?}", updated.queues),
        );
        push_change(
            &mut changes,
            "autoscale",
            &self.autoscale.is_some(),
            &updated.autoscale.is_some(),
        );
        push_change(
            &mut changes,
            "alerts",
            &self.alerts.is_some(),
            &updated.alerts.is_some(),
        );
        push_change(
            &mut changes,
            "pool.max_size",
            &self.pool.max_size,
            &updated.pool.max_size,
        );
        push_change(
            &mut changes,
            "pool.reuse_enabled",
            &self.pool.reuse_enabled,
            &updated.pool.reuse_enabled,
        );
        push_change(
            &mut changes,
            "cache.ttl_secs",
            &self.cache.ttl_secs,
            &updated.cache.ttl_secs,
        );
        push_change(
            &mut changes,
            "cache.enabled",
            &self.cache.enabled,
            &updated.cache.enabled,
        );

        ConfigDiff { changes }
    }
}

/// Return the value of the first set environment variable from `keys`.
fn first_env(keys: &[&str]) -> Option<String> {
    keys.iter()
        .find_map(|key| env::var(key).ok().filter(|v| !v.is_empty()))
}

/// Return the parsed value of the first set, successfully-parsed environment
/// variable from `keys`.
///
/// A variable that is set but fails to parse (e.g. `CELERS_CONCURRENCY=abc`)
/// is skipped in favor of the next candidate exactly as an unset variable
/// would be -- but unlike an unset variable, this logs a warning first (idx
/// 337), since a malformed override silently reducing to "as if it were
/// never set" is easy for an operator to miss entirely otherwise.
fn first_env_parsed<T: std::str::FromStr>(keys: &[&str]) -> Option<T> {
    keys.iter().find_map(|key| {
        let raw = env::var(key).ok()?;
        match raw.parse::<T>() {
            Ok(value) => Some(value),
            Err(_) => {
                warn!(
                    "Environment variable {key}={raw:?} could not be parsed as the expected \
                     type; ignoring it (falling back to the next source in the precedence chain)"
                );
                None
            }
        }
    })
}

/// Record a single field change between two displayable values if they differ.
fn push_change<T: PartialEq + std::fmt::Display>(
    changes: &mut Vec<FieldChange>,
    field: &str,
    old: &T,
    new: &T,
) {
    if old != new {
        changes.push(FieldChange {
            field: field.to_string(),
            old: old.to_string(),
            new: new.to_string(),
        });
    }
}

/// Record a change between two optional string values if they differ.
fn push_change_opt(
    changes: &mut Vec<FieldChange>,
    field: &str,
    old: &Option<String>,
    new: &Option<String>,
) {
    if old != new {
        changes.push(FieldChange {
            field: field.to_string(),
            old: old.clone().unwrap_or_else(|| "<none>".to_string()),
            new: new.clone().unwrap_or_else(|| "<none>".to_string()),
        });
    }
}

/// A single changed configuration field, as reported by [`Config::diff`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FieldChange {
    /// Dotted field path (e.g. `broker.url`).
    pub field: String,
    /// Previous value rendered as a string.
    pub old: String,
    /// New value rendered as a string.
    pub new: String,
}

/// The set of configuration fields that changed between two configurations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ConfigDiff {
    /// The individual field changes.
    pub changes: Vec<FieldChange>,
}

impl ConfigDiff {
    /// Returns `true` when no fields changed.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.changes.is_empty()
    }

    /// Number of fields that changed.
    #[must_use]
    pub fn len(&self) -> usize {
        self.changes.len()
    }
}

impl std::fmt::Display for ConfigDiff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.changes.is_empty() {
            return write!(f, "no changes");
        }
        for (idx, change) in self.changes.iter().enumerate() {
            if idx > 0 {
                writeln!(f)?;
            }
            write!(f, "{}: {} -> {}", change.field, change.old, change.new)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default_config();
        assert_eq!(config.profile, None);
        assert_eq!(config.broker.broker_type, "redis");
        assert_eq!(config.broker.url, "redis://localhost:6379");
        assert_eq!(config.broker.queue, "celers");
        assert_eq!(config.broker.mode, "fifo");
        assert_eq!(config.broker.failover_urls, Vec::<String>::new());
        assert_eq!(config.broker.failover_retries, 3);
        assert_eq!(config.broker.failover_timeout_secs, 5);
        assert_eq!(config.worker.concurrency, 4);
        assert_eq!(config.worker.poll_interval_ms, 1000);
        assert_eq!(config.worker.max_retries, 3);
        assert_eq!(config.worker.default_timeout_secs, 300);
        assert_eq!(config.queues, vec!["celers"]);
        assert!(config.autoscale.is_none());
        assert!(config.alerts.is_none());
        assert_eq!(config.pool.max_size, 16);
        assert!(config.pool.reuse_enabled);
        assert_eq!(config.cache.ttl_secs, 30);
        assert!(config.cache.enabled);
    }

    #[test]
    fn test_config_serialization() {
        let config = Config::default_config();
        let toml_str = toml::to_string(&config).unwrap();
        assert!(toml_str.contains("type = \"redis\""));
        assert!(toml_str.contains("url = \"redis://localhost:6379\""));
        assert!(toml_str.contains("concurrency = 4"));
    }

    #[test]
    fn test_config_deserialization() {
        let toml_str = r#"
queues = ["queue1", "queue2"]

[broker]
type = "redis"
url = "redis://127.0.0.1:6379"
queue = "test_queue"
mode = "priority"

[worker]
concurrency = 8
poll_interval_ms = 500
max_retries = 5
default_timeout_secs = 600
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.broker.broker_type, "redis");
        assert_eq!(config.broker.url, "redis://127.0.0.1:6379");
        assert_eq!(config.broker.queue, "test_queue");
        assert_eq!(config.broker.mode, "priority");
        assert_eq!(config.worker.concurrency, 8);
        assert_eq!(config.worker.poll_interval_ms, 500);
        assert_eq!(config.worker.max_retries, 5);
        assert_eq!(config.worker.default_timeout_secs, 600);
        assert_eq!(config.queues, vec!["queue1", "queue2"]);
    }

    #[test]
    fn test_config_defaults() {
        let toml_str = r#"
            [broker]
            type = "redis"
            url = "redis://localhost:6379"
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.broker.queue, "celers");
        assert_eq!(config.broker.mode, "fifo");
        assert_eq!(config.worker.concurrency, 4);
        assert_eq!(config.worker.poll_interval_ms, 1000);
        // A config file predating the [pool]/[cache] sections must still
        // parse, falling back to their defaults (backward compatibility).
        assert_eq!(config.pool.max_size, 16);
        assert!(config.pool.reuse_enabled);
        assert_eq!(config.cache.ttl_secs, 30);
        assert!(config.cache.enabled);
    }

    #[test]
    fn test_config_validation_valid() {
        let config = Config::default_config();
        let warnings = config.validate().unwrap();
        assert!(warnings.is_empty());
    }

    #[test]
    fn test_config_validation_invalid_broker_type() {
        let mut config = Config::default_config();
        config.broker.broker_type = "invalid".to_string();
        let warnings = config.validate().unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("Unknown broker type"));
    }

    #[test]
    fn test_config_validation_invalid_queue_mode() {
        let mut config = Config::default_config();
        config.broker.mode = "invalid".to_string();
        let warnings = config.validate().unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("Unknown queue mode"));
    }

    #[test]
    fn test_config_validation_zero_concurrency() {
        let mut config = Config::default_config();
        config.worker.concurrency = 0;
        let warnings = config.validate().unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("Concurrency is 0"));
    }

    #[test]
    fn test_config_validation_high_concurrency() {
        let mut config = Config::default_config();
        config.worker.concurrency = 150;
        let warnings = config.validate().unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("High concurrency"));
    }

    #[test]
    fn test_config_validation_low_poll_interval() {
        let mut config = Config::default_config();
        config.worker.poll_interval_ms = 50;
        let warnings = config.validate().unwrap();
        assert_eq!(warnings.len(), 1);
        assert!(warnings[0].contains("Very low poll interval"));
    }

    #[test]
    fn test_config_validation_multiple_issues() {
        let mut config = Config::default_config();
        config.broker.broker_type = "unknown".to_string();
        config.broker.mode = "invalid_mode".to_string();
        config.worker.concurrency = 0;
        config.worker.poll_interval_ms = 50;

        let warnings = config.validate().unwrap();
        assert_eq!(warnings.len(), 4);
    }

    #[test]
    fn test_config_file_roundtrip() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();

        let config = Config::default_config();
        config.to_file(temp_path).unwrap();

        let loaded_config = Config::from_file(temp_path).unwrap();
        assert_eq!(config.broker.broker_type, loaded_config.broker.broker_type);
        assert_eq!(config.broker.url, loaded_config.broker.url);
        assert_eq!(config.worker.concurrency, loaded_config.worker.concurrency);
    }

    /// Regression test for the `write_aliases_only` half of idx 338:
    /// updating aliases must never overwrite any other on-disk section
    /// (most importantly `broker.url`) with whatever an in-memory, possibly
    /// env-resolved `Config` happens to hold.
    #[test]
    fn write_aliases_only_preserves_every_other_section_on_disk() {
        let temp_file = tempfile::NamedTempFile::new().unwrap();
        let temp_path = temp_file.path();

        let mut on_disk = Config::default_config();
        on_disk.broker.url = "amqp://explicit-from-file:5672".to_string();
        on_disk.worker.concurrency = 42;
        on_disk.to_file(temp_path).unwrap();

        let mut aliases = crate::aliases::AliasConfig::new();
        aliases
            .add("w", "worker start", &["worker", "queue", "task"])
            .expect("valid alias");

        Config::write_aliases_only(temp_path, &aliases).expect("write_aliases_only");

        let reloaded = Config::from_file(temp_path).expect("reload");
        assert_eq!(
            reloaded.broker.url, "amqp://explicit-from-file:5672",
            "write_aliases_only must not clobber broker.url with anything from an in-memory, \
             possibly env-resolved Config"
        );
        assert_eq!(reloaded.worker.concurrency, 42);
        assert_eq!(reloaded.aliases, Some(aliases));
    }

    /// `write_aliases_only` against a path that does not exist yet must
    /// still succeed, producing a sensible file (mirroring
    /// `config_layer::resolve_config_path`'s "first default name" contract
    /// for a mutating command run before `celers init`).
    #[test]
    fn write_aliases_only_creates_a_sensible_file_when_none_exists_yet() {
        let path = std::env::temp_dir().join(format!(
            "celers_cli_alias_only_test_{}_{}.toml",
            std::process::id(),
            uuid::Uuid::new_v4()
        ));
        let _ = std::fs::remove_file(&path);

        let mut aliases = crate::aliases::AliasConfig::new();
        aliases
            .add("w", "worker start", &["worker", "queue", "task"])
            .expect("valid alias");

        Config::write_aliases_only(&path, &aliases).expect("write_aliases_only on a missing file");

        let reloaded = Config::from_file(&path).expect("reload");
        assert_eq!(reloaded.aliases, Some(aliases));
        assert_eq!(
            reloaded.broker.broker_type, "redis",
            "falls back to Config::default_config for every other section"
        );

        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn test_worker_config_default() {
        let worker_config = WorkerConfig::default();
        assert_eq!(worker_config.concurrency, 4);
        assert_eq!(worker_config.poll_interval_ms, 1000);
        assert_eq!(worker_config.max_retries, 3);
        assert_eq!(worker_config.default_timeout_secs, 300);
    }

    #[test]
    fn test_expand_env_vars_simple() {
        env::set_var("TEST_VAR", "test_value");
        let result = super::expand_env_vars("prefix ${TEST_VAR} suffix");
        assert_eq!(result, "prefix test_value suffix");
        env::remove_var("TEST_VAR");
    }

    #[test]
    fn test_expand_env_vars_with_default() {
        env::remove_var("MISSING_VAR");
        let result = super::expand_env_vars("value is ${MISSING_VAR:default_value}");
        assert_eq!(result, "value is default_value");
    }

    #[test]
    fn test_expand_env_vars_without_default() {
        env::remove_var("MISSING_VAR");
        let result = super::expand_env_vars("value is ${MISSING_VAR}");
        assert_eq!(result, "value is ${MISSING_VAR}");
    }

    #[test]
    fn test_expand_env_vars_multiple() {
        env::set_var("VAR1", "value1");
        env::set_var("VAR2", "value2");
        let result = super::expand_env_vars("${VAR1} and ${VAR2}");
        assert_eq!(result, "value1 and value2");
        env::remove_var("VAR1");
        env::remove_var("VAR2");
    }

    #[test]
    fn test_expand_env_vars_mixed() {
        env::set_var("EXISTING_VAR", "exists");
        env::remove_var("MISSING_VAR");
        let result = super::expand_env_vars("${EXISTING_VAR} and ${MISSING_VAR:default}");
        assert_eq!(result, "exists and default");
        env::remove_var("EXISTING_VAR");
    }

    #[test]
    fn test_config_with_env_vars() {
        env::set_var("TEST_REDIS_HOST", "localhost");
        env::set_var("TEST_REDIS_PORT", "6379");

        let toml_str = r#"
[broker]
type = "redis"
url = "redis://${TEST_REDIS_HOST}:${TEST_REDIS_PORT}"
queue = "celers"
mode = "fifo"
        "#;

        let expanded = super::expand_env_vars(toml_str);
        assert!(
            expanded.contains("redis://localhost:6379"),
            "Expanded content: {}",
            expanded
        );

        let config: Config = toml::from_str(&expanded).unwrap();
        assert_eq!(config.broker.url, "redis://localhost:6379");

        env::remove_var("TEST_REDIS_HOST");
        env::remove_var("TEST_REDIS_PORT");
    }

    #[test]
    fn test_config_with_env_vars_and_defaults() {
        env::remove_var("REDIS_HOST");

        let toml_str = r#"
[broker]
type = "redis"
url = "redis://${REDIS_HOST:localhost}:${REDIS_PORT:6379}"
queue = "celers"
mode = "fifo"
        "#;

        let expanded = super::expand_env_vars(toml_str);
        assert!(expanded.contains("redis://localhost:6379"));

        let config: Config = toml::from_str(&expanded).unwrap();
        assert_eq!(config.broker.url, "redis://localhost:6379");
    }

    #[test]
    fn test_broker_failover_config() {
        let toml_str = r#"
[broker]
type = "redis"
url = "redis://primary:6379"
failover_urls = ["redis://backup1:6379", "redis://backup2:6379"]
failover_retries = 5
failover_timeout_secs = 10
queue = "celers"
mode = "fifo"
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.broker.failover_urls.len(), 2);
        assert_eq!(config.broker.failover_urls[0], "redis://backup1:6379");
        assert_eq!(config.broker.failover_urls[1], "redis://backup2:6379");
        assert_eq!(config.broker.failover_retries, 5);
        assert_eq!(config.broker.failover_timeout_secs, 10);
    }

    #[test]
    fn test_autoscale_config() {
        let toml_str = r#"
[broker]
type = "redis"
url = "redis://localhost:6379"

[autoscale]
enabled = true
min_workers = 2
max_workers = 20
scale_up_threshold = 200
scale_down_threshold = 20
check_interval_secs = 60
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.autoscale.is_some());
        let autoscale = config.autoscale.unwrap();
        assert!(autoscale.enabled);
        assert_eq!(autoscale.min_workers, 2);
        assert_eq!(autoscale.max_workers, 20);
        assert_eq!(autoscale.scale_up_threshold, 200);
        assert_eq!(autoscale.scale_down_threshold, 20);
        assert_eq!(autoscale.check_interval_secs, 60);
    }

    #[test]
    fn test_alert_config() {
        let toml_str = r#"
[broker]
type = "redis"
url = "redis://localhost:6379"

[alerts]
enabled = true
webhook_url = "https://hooks.slack.com/services/xxx"
dlq_threshold = 100
failed_threshold = 200
check_interval_secs = 120
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(config.alerts.is_some());
        let alerts = config.alerts.unwrap();
        assert!(alerts.enabled);
        assert_eq!(
            alerts.webhook_url,
            Some("https://hooks.slack.com/services/xxx".to_string())
        );
        assert_eq!(alerts.dlq_threshold, 100);
        assert_eq!(alerts.failed_threshold, 200);
        assert_eq!(alerts.check_interval_secs, 120);
    }

    #[test]
    fn test_config_validation_autoscale_invalid() {
        let mut config = Config::default_config();
        config.autoscale = Some(AutoScaleConfig {
            enabled: true,
            min_workers: 0,
            max_workers: 5,
            scale_up_threshold: 100,
            scale_down_threshold: 10,
            check_interval_secs: 30,
        });

        let warnings = config.validate().unwrap();
        assert!(warnings.iter().any(|w| w.contains("min_workers is 0")));
    }

    #[test]
    fn test_config_validation_autoscale_max_less_than_min() {
        let mut config = Config::default_config();
        config.autoscale = Some(AutoScaleConfig {
            enabled: true,
            min_workers: 10,
            max_workers: 5,
            scale_up_threshold: 100,
            scale_down_threshold: 10,
            check_interval_secs: 30,
        });

        let warnings = config.validate().unwrap();
        assert!(warnings
            .iter()
            .any(|w| w.contains("max_workers") && w.contains("min_workers")));
    }

    #[test]
    fn test_config_validation_autoscale_threshold_invalid() {
        let mut config = Config::default_config();
        config.autoscale = Some(AutoScaleConfig {
            enabled: true,
            min_workers: 1,
            max_workers: 10,
            scale_up_threshold: 50,
            scale_down_threshold: 100,
            check_interval_secs: 30,
        });

        let warnings = config.validate().unwrap();
        assert!(warnings.iter().any(|w| w.contains("scale_down_threshold")));
    }

    #[test]
    fn test_config_validation_alert_no_webhook() {
        let mut config = Config::default_config();
        config.alerts = Some(AlertConfig {
            enabled: true,
            webhook_url: None,
            dlq_threshold: 50,
            failed_threshold: 100,
            check_interval_secs: 60,
        });

        let warnings = config.validate().unwrap();
        assert!(warnings.iter().any(|w| w.contains("webhook_url")));
    }

    #[test]
    fn test_profile_config() {
        let toml_str = r#"
profile = "production"

[broker]
type = "redis"
url = "redis://localhost:6379"
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.profile, Some("production".to_string()));
    }

    #[test]
    fn test_pool_config_toml_roundtrip() {
        let toml_str = r#"
[broker]
type = "redis"
url = "redis://localhost:6379"

[pool]
max_size = 32
reuse_enabled = false
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.pool.max_size, 32);
        assert!(!config.pool.reuse_enabled);

        let rendered = toml::to_string(&config).unwrap();
        assert!(rendered.contains("max_size = 32"));
        assert!(rendered.contains("reuse_enabled = false"));

        let reparsed: Config = toml::from_str(&rendered).unwrap();
        assert_eq!(reparsed.pool, config.pool);
    }

    #[test]
    fn test_cache_config_toml_roundtrip() {
        let toml_str = r#"
[broker]
type = "redis"
url = "redis://localhost:6379"

[cache]
ttl_secs = 120
enabled = false
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.cache.ttl_secs, 120);
        assert!(!config.cache.enabled);
        assert_eq!(config.cache.ttl(), std::time::Duration::from_secs(120));

        let rendered = toml::to_string(&config).unwrap();
        assert!(rendered.contains("ttl_secs = 120"));
        assert!(rendered.contains("enabled = false"));

        let reparsed: Config = toml::from_str(&rendered).unwrap();
        assert_eq!(reparsed.cache, config.cache);
    }

    #[test]
    fn test_pool_config_partial_toml_uses_defaults_for_missing_fields() {
        let toml_str = r#"
[broker]
type = "redis"
url = "redis://localhost:6379"

[pool]
max_size = 4
        "#;

        let config: Config = toml::from_str(toml_str).unwrap();
        assert_eq!(config.pool.max_size, 4);
        assert!(
            config.pool.reuse_enabled,
            "omitted field falls back to default"
        );
    }

    // NOTE: like `CELERS_POOL_*` / `CELERS_CACHE_*` below, `REDIS_URL` /
    // `AMQP_URL` / `CELERY_BROKER_URL` are all exercised sequentially within
    // the single test function below rather than split across multiple test
    // functions, to avoid two functions racing on the same process-wide env
    // vars under `cargo test`'s default same-process, multi-threaded runner
    // (nextest, this crate's primary runner, isolates each test in its own
    // process and would not have this issue).
    #[test]
    fn test_apply_env_overrides_broker_url_falls_back_to_detect_broker_from_env() {
        env::remove_var("CELERY_BROKER_URL");
        env::remove_var("CELERS_BROKER_URL");
        env::remove_var("REDIS_URL");
        env::remove_var("AMQP_URL");

        // No explicit CELERY_BROKER_URL/CELERS_BROKER_URL, but REDIS_URL is
        // set: broker.url should pick it up via detect_broker_from_env().
        env::set_var("REDIS_URL", "redis://from-redis-url:6379");
        let mut config = Config::default_config();
        config.apply_env_overrides();
        assert_eq!(config.broker.url, "redis://from-redis-url:6379");

        // Explicit CELERY_BROKER_URL alongside REDIS_URL: the explicit
        // override still wins over the detect_broker_from_env() fallback.
        env::set_var("CELERY_BROKER_URL", "redis://from-celery-broker-url:6379");
        let mut config = Config::default_config();
        config.apply_env_overrides();
        assert_eq!(config.broker.url, "redis://from-celery-broker-url:6379");

        // Regression test for idx 338: a `broker.url` that already differs
        // from the hardcoded default (standing in for "a config file
        // explicitly set this") must NOT be silently replaced by a generic
        // REDIS_URL/AMQP_URL -- only CELERY_BROKER_URL/CELERS_BROKER_URL
        // (an explicit, CeleRS-specific override) may still win.
        env::remove_var("CELERY_BROKER_URL");
        env::remove_var("CELERS_BROKER_URL");
        env::set_var("REDIS_URL", "redis://from-redis-url-again:6379");
        let mut config = Config::default_config();
        config.broker.url = "amqp://explicit-from-file:5672".to_string();
        config.apply_env_overrides();
        assert_eq!(
            config.broker.url, "amqp://explicit-from-file:5672",
            "an explicit (non-default) broker.url must survive apply_env_overrides even when a \
             generic REDIS_URL is set -- only CELERY_BROKER_URL/CELERS_BROKER_URL may override it"
        );

        env::remove_var("CELERY_BROKER_URL");
        env::remove_var("CELERS_BROKER_URL");
        env::remove_var("REDIS_URL");
        env::remove_var("AMQP_URL");
    }

    // NOTE: `CELERS_POOL_*` / `CELERS_CACHE_*` are each exercised by exactly
    // one test function below (unset/valid/invalid checked sequentially
    // within that single function), rather than split across multiple test
    // functions. `cargo test`'s default same-process, multi-threaded
    // execution would otherwise let two functions race on the same env var
    // (nextest, this crate's primary runner, isolates each test in its own
    // process and would not have this issue, but the fallback runner would).

    #[test]
    fn test_pool_config_from_env_or_default() {
        env::remove_var("CELERS_POOL_MAX_SIZE");
        env::remove_var("CELERS_POOL_REUSE_ENABLED");
        let defaults = PoolConfig::from_env_or_default();
        assert_eq!(defaults.max_size, 16);
        assert!(defaults.reuse_enabled);

        env::set_var("CELERS_POOL_MAX_SIZE", "64");
        env::set_var("CELERS_POOL_REUSE_ENABLED", "false");
        let overridden = PoolConfig::from_env_or_default();
        assert_eq!(overridden.max_size, 64);
        assert!(!overridden.reuse_enabled);

        env::set_var("CELERS_POOL_MAX_SIZE", "not-a-number");
        let invalid = PoolConfig::from_env_or_default();
        assert_eq!(
            invalid.max_size, 16,
            "unparseable override falls back to default"
        );

        env::remove_var("CELERS_POOL_MAX_SIZE");
        env::remove_var("CELERS_POOL_REUSE_ENABLED");

        // Regression test for idx 337: a `[pool]` section from a resolved
        // configuration file must be honored by `from_env_or_default` when
        // no environment variable overrides it -- previously, every
        // consumer of `from_env_or_default` only ever saw env vars layered
        // on top of the hardcoded type default, never a file's values.
        let mut cfg = Config::default_config();
        cfg.pool.max_size = 77;
        cfg.pool.reuse_enabled = false;
        cfg.apply_env_overrides();
        assert_eq!(
            PoolConfig::from_env_or_default().max_size,
            77,
            "a file-provided [pool] section must be honored, not just env vars / hardcoded defaults"
        );
        assert!(!PoolConfig::from_env_or_default().reuse_enabled);

        // An explicit env var still wins over the file-provided floor.
        env::set_var("CELERS_POOL_MAX_SIZE", "5");
        assert_eq!(PoolConfig::from_env_or_default().max_size, 5);
        env::remove_var("CELERS_POOL_MAX_SIZE");

        // Restore the process-wide floor to the type defaults so any later
        // test in this binary that assumes "no file loaded yet" continues
        // to see PoolConfig::default() (only observable under the plain
        // `cargo test` fallback runner's shared process; nextest, this
        // crate's primary runner, isolates each test in its own process).
        let mut restore = Config::default_config();
        restore.apply_env_overrides();
    }

    #[test]
    fn test_cache_config_from_env_or_default() {
        env::remove_var("CELERS_CACHE_TTL_SECS");
        env::remove_var("CELERS_CACHE_ENABLED");
        let defaults = CacheConfig::from_env_or_default();
        assert_eq!(defaults.ttl_secs, 30);
        assert!(defaults.enabled);

        env::set_var("CELERS_CACHE_TTL_SECS", "5");
        env::set_var("CELERS_CACHE_ENABLED", "false");
        let overridden = CacheConfig::from_env_or_default();
        assert_eq!(overridden.ttl_secs, 5);
        assert!(!overridden.enabled);
        assert_eq!(overridden.ttl(), std::time::Duration::from_secs(5));

        env::set_var("CELERS_CACHE_TTL_SECS", "not-a-number");
        let invalid = CacheConfig::from_env_or_default();
        assert_eq!(
            invalid.ttl_secs, 30,
            "unparseable override falls back to default"
        );

        env::remove_var("CELERS_CACHE_TTL_SECS");
        env::remove_var("CELERS_CACHE_ENABLED");

        // Regression test for idx 337: same as PoolConfig above, a `[cache]`
        // section from a resolved configuration file must be honored.
        let mut cfg = Config::default_config();
        cfg.cache.ttl_secs = 111;
        cfg.cache.enabled = false;
        cfg.apply_env_overrides();
        assert_eq!(
            CacheConfig::from_env_or_default().ttl_secs,
            111,
            "a file-provided [cache] section must be honored, not just env vars / hardcoded defaults"
        );
        assert!(!CacheConfig::from_env_or_default().enabled);

        env::set_var("CELERS_CACHE_TTL_SECS", "7");
        assert_eq!(CacheConfig::from_env_or_default().ttl_secs, 7);
        env::remove_var("CELERS_CACHE_TTL_SECS");

        // Restore the process-wide floor to the type defaults; see the
        // matching comment in test_pool_config_from_env_or_default.
        let mut restore = Config::default_config();
        restore.apply_env_overrides();
    }

    #[test]
    fn test_config_diff_reports_pool_and_cache_changes() {
        let before = Config::default_config();
        let mut after = Config::default_config();
        after.pool.max_size = 99;
        after.cache.enabled = false;

        let diff = before.diff(&after);
        assert!(diff.changes.iter().any(|c| c.field == "pool.max_size"));
        assert!(diff.changes.iter().any(|c| c.field == "cache.enabled"));
        assert!(!diff.changes.iter().any(|c| c.field == "pool.reuse_enabled"));
    }

    #[test]
    fn test_config_diff_no_pool_cache_changes_when_equal() {
        let before = Config::default_config();
        let after = Config::default_config();
        let diff = before.diff(&after);
        assert!(diff.is_empty());
    }
}
