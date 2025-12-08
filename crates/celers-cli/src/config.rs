//! Configuration file support for CeleRS CLI

use serde::{Deserialize, Serialize};
use std::env;
use std::path::Path;

/// CLI configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Broker configuration
    pub broker: BrokerConfig,

    /// Worker configuration
    #[serde(default)]
    pub worker: WorkerConfig,

    /// Queue names
    #[serde(default)]
    pub queues: Vec<String>,
}

/// Broker configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerConfig {
    /// Broker type (redis or postgres)
    #[serde(rename = "type")]
    pub broker_type: String,

    /// Connection URL
    pub url: String,

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

/// Expand environment variables in a string
/// Supports ${VAR} and ${VAR:default_value} syntax
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
            broker: BrokerConfig {
                broker_type: "redis".to_string(),
                url: "redis://localhost:6379".to_string(),
                queue: "celers".to_string(),
                mode: "fifo".to_string(),
            },
            worker: WorkerConfig::default(),
            queues: vec!["celers".to_string()],
        }
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

        Ok(warnings)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = Config::default_config();
        assert_eq!(config.broker.broker_type, "redis");
        assert_eq!(config.broker.url, "redis://localhost:6379");
        assert_eq!(config.broker.queue, "celers");
        assert_eq!(config.broker.mode, "fifo");
        assert_eq!(config.worker.concurrency, 4);
        assert_eq!(config.worker.poll_interval_ms, 1000);
        assert_eq!(config.worker.max_retries, 3);
        assert_eq!(config.worker.default_timeout_secs, 300);
        assert_eq!(config.queues, vec!["celers"]);
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
}
