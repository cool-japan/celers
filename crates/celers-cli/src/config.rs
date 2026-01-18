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
    /// Load configuration from a TOML file with environment variable expansion
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let expanded_content = expand_env_vars(&content);
        let config: Config = toml::from_str(&expanded_content)?;
        Ok(config)
    }

    /// Save configuration to a TOML file
    pub fn to_file<P: AsRef<Path>>(&self, path: P) -> anyhow::Result<()> {
        let content = toml::to_string_pretty(self)?;
        std::fs::write(path, content)?;
        Ok(())
    }

    /// Create a default configuration file
    pub fn default_config() -> Self {
        Self {
            profile: None,
            broker: BrokerConfig {
                broker_type: "redis".to_string(),
                url: "redis://localhost:6379".to_string(),
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
}
