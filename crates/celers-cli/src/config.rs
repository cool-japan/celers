//! Configuration file support for CeleRS CLI

use serde::{Deserialize, Serialize};
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

impl Config {
    /// Load configuration from a TOML file
    pub fn from_file<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let content = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&content)?;
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
}
