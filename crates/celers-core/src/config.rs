//! Celery-compatible configuration for `CeleRS`
//!
//! This module provides configuration structures that are compatible with
//! Python Celery's configuration format, making it easy to migrate from
//! Celery to `CeleRS` or run them side-by-side.
//!
//! # Example
//!
//! ```rust
//! use celers_core::config::{CeleryConfig, TaskConfig, BrokerTransport};
//! use std::time::Duration;
//!
//! let config = CeleryConfig::default()
//!     .with_broker_url("redis://localhost:6379/0")
//!     .with_result_backend("redis://localhost:6379/1")
//!     .with_task_serializer("json")
//!     .with_timezone("UTC")
//!     .with_worker_concurrency(4);
//!
//! assert_eq!(config.broker_url, "redis://localhost:6379/0");
//! assert_eq!(config.worker_concurrency, 4);
//! ```

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;

/// Celery-compatible main configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CeleryConfig {
    /// Broker connection URL (`CELERY_BROKER_URL`)
    pub broker_url: String,

    /// Result backend URL (`CELERY_RESULT_BACKEND`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_backend: Option<String>,

    /// Task serializer format (`CELERY_TASK_SERIALIZER`)
    #[serde(default = "default_serializer")]
    pub task_serializer: String,

    /// Result serializer format (`CELERY_RESULT_SERIALIZER`)
    #[serde(default = "default_serializer")]
    pub result_serializer: String,

    /// Accepted content types (`CELERY_ACCEPT_CONTENT`)
    #[serde(default = "default_accept_content")]
    pub accept_content: Vec<String>,

    /// Timezone for scheduling (`CELERY_TIMEZONE`)
    #[serde(default = "default_timezone")]
    pub timezone: String,

    /// Use UTC timestamps (`CELERY_ENABLE_UTC`)
    #[serde(default = "default_true")]
    pub enable_utc: bool,

    /// Track task started events (`CELERY_TASK_TRACK_STARTED`)
    #[serde(default)]
    pub task_track_started: bool,

    /// Send task sent events (`CELERY_TASK_SEND_SENT_EVENT`)
    #[serde(default)]
    pub task_send_sent_event: bool,

    /// Acknowledge tasks late (`CELERY_TASK_ACKS_LATE`)
    #[serde(default)]
    pub task_acks_late: bool,

    /// Reject on worker lost (`CELERY_TASK_REJECT_ON_WORKER_LOST`)
    #[serde(default)]
    pub task_reject_on_worker_lost: bool,

    /// Worker concurrency (`CELERYD_CONCURRENCY`)
    #[serde(default = "default_concurrency")]
    pub worker_concurrency: usize,

    /// Worker prefetch multiplier (`CELERYD_PREFETCH_MULTIPLIER`)
    #[serde(default = "default_prefetch_multiplier")]
    pub worker_prefetch_multiplier: usize,

    /// Maximum tasks per child before restart (`CELERYD_MAX_TASKS_PER_CHILD`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_max_tasks_per_child: Option<usize>,

    /// Maximum memory per child in KB (`CELERYD_MAX_MEMORY_PER_CHILD`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub worker_max_memory_per_child: Option<usize>,

    /// Worker heartbeat interval in seconds (`CELERY_WORKER_HEARTBEAT`)
    #[serde(default = "default_heartbeat_interval")]
    pub worker_heartbeat: u64,

    /// Task default queue (`CELERY_DEFAULT_QUEUE`)
    #[serde(default = "default_queue_name")]
    pub task_default_queue: String,

    /// Task default exchange (`CELERY_DEFAULT_EXCHANGE`)
    #[serde(default = "default_queue_name")]
    pub task_default_exchange: String,

    /// Task default exchange type (`CELERY_DEFAULT_EXCHANGE_TYPE`)
    #[serde(default = "default_exchange_type")]
    pub task_default_exchange_type: String,

    /// Task default routing key (`CELERY_DEFAULT_ROUTING_KEY`)
    #[serde(default = "default_queue_name")]
    pub task_default_routing_key: String,

    /// Task routes (`CELERY_TASK_ROUTES`)
    #[serde(default)]
    pub task_routes: HashMap<String, TaskRoute>,

    /// Task time limit in seconds (`CELERY_TASK_TIME_LIMIT`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_time_limit: Option<u64>,

    /// Task soft time limit in seconds (`CELERY_TASK_SOFT_TIME_LIMIT`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub task_soft_time_limit: Option<u64>,

    /// Task default retry delay in seconds (`CELERY_TASK_DEFAULT_RETRY_DELAY`)
    #[serde(default = "default_retry_delay")]
    pub task_default_retry_delay: u64,

    /// Task max retries (`CELERY_TASK_MAX_RETRIES`)
    #[serde(default = "default_max_retries")]
    pub task_max_retries: u32,

    /// Result expires in seconds (`CELERY_RESULT_EXPIRES`)
    #[serde(default = "default_result_expires")]
    pub result_expires: u64,

    /// Result compression (`CELERY_RESULT_COMPRESSION`)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_compression: Option<String>,

    /// Result compression threshold in bytes
    #[serde(default = "default_compression_threshold")]
    pub result_compression_threshold: usize,

    /// Task-specific configurations
    #[serde(default)]
    pub task_annotations: HashMap<String, TaskConfig>,

    /// Broker transport options (`CELERY_BROKER_TRANSPORT_OPTIONS`)
    #[serde(default)]
    pub broker_transport_options: BrokerTransport,

    /// Result backend transport options
    #[serde(default)]
    pub result_backend_transport_options: BackendTransport,

    /// Beat schedule configuration (`CELERYBEAT_SCHEDULE`)
    #[serde(default)]
    pub beat_schedule: HashMap<String, BeatSchedule>,

    /// Custom configuration extensions
    #[serde(flatten)]
    pub custom: HashMap<String, serde_json::Value>,
}

/// Task routing configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRoute {
    /// Target queue name
    pub queue: String,

    /// Exchange name
    #[serde(skip_serializing_if = "Option::is_none")]
    pub exchange: Option<String>,

    /// Routing key
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routing_key: Option<String>,

    /// Priority (0-255)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,
}

/// Per-task configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskConfig {
    /// Task time limit in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_limit: Option<u64>,

    /// Task soft time limit in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub soft_time_limit: Option<u64>,

    /// Max retries for this task
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,

    /// Default retry delay
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default_retry_delay: Option<u64>,

    /// Task priority
    #[serde(skip_serializing_if = "Option::is_none")]
    pub priority: Option<u8>,

    /// Target queue
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue: Option<String>,

    /// Acknowledge late
    #[serde(skip_serializing_if = "Option::is_none")]
    pub acks_late: Option<bool>,

    /// Track started
    #[serde(skip_serializing_if = "Option::is_none")]
    pub track_started: Option<bool>,

    /// Rate limit (e.g., "10/s", "100/m", "1000/h")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit: Option<String>,
}

/// Broker transport options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BrokerTransport {
    /// Visibility timeout in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub visibility_timeout: Option<u64>,

    /// Connection pool size
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<usize>,

    /// Connection retry settings
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,

    /// Retry interval in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval_start: Option<u64>,

    /// Retry interval max in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub interval_max: Option<u64>,

    /// Additional transport-specific options
    #[serde(flatten)]
    pub custom: HashMap<String, serde_json::Value>,
}

/// Result backend transport options
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BackendTransport {
    /// Result expiration in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result_expires: Option<u64>,

    /// Connection pool size
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<usize>,

    /// Additional backend-specific options
    #[serde(flatten)]
    pub custom: HashMap<String, serde_json::Value>,
}

/// Beat scheduler configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BeatSchedule {
    /// Task name to execute
    pub task: String,

    /// Schedule definition
    pub schedule: ScheduleDefinition,

    /// Task arguments
    #[serde(skip_serializing_if = "Option::is_none")]
    pub args: Option<Vec<serde_json::Value>>,

    /// Task keyword arguments
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kwargs: Option<HashMap<String, serde_json::Value>>,

    /// Task options
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<TaskConfig>,
}

/// Schedule definition for beat tasks
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ScheduleDefinition {
    /// Crontab schedule (e.g., "0 0 * * *")
    Crontab(String),

    /// Interval in seconds
    Interval(u64),

    /// Complex schedule
    Complex {
        /// Schedule type (crontab, interval, solar)
        #[serde(rename = "type")]
        schedule_type: String,

        /// Schedule value
        value: serde_json::Value,
    },
}

impl Default for CeleryConfig {
    fn default() -> Self {
        Self {
            broker_url: "redis://localhost:6379/0".to_string(),
            result_backend: Some("redis://localhost:6379/1".to_string()),
            task_serializer: default_serializer(),
            result_serializer: default_serializer(),
            accept_content: default_accept_content(),
            timezone: default_timezone(),
            enable_utc: true,
            task_track_started: false,
            task_send_sent_event: false,
            task_acks_late: false,
            task_reject_on_worker_lost: false,
            worker_concurrency: default_concurrency(),
            worker_prefetch_multiplier: default_prefetch_multiplier(),
            worker_max_tasks_per_child: None,
            worker_max_memory_per_child: None,
            worker_heartbeat: default_heartbeat_interval(),
            task_default_queue: default_queue_name(),
            task_default_exchange: default_queue_name(),
            task_default_exchange_type: default_exchange_type(),
            task_default_routing_key: default_queue_name(),
            task_routes: HashMap::new(),
            task_time_limit: None,
            task_soft_time_limit: None,
            task_default_retry_delay: default_retry_delay(),
            task_max_retries: default_max_retries(),
            result_expires: default_result_expires(),
            result_compression: None,
            result_compression_threshold: default_compression_threshold(),
            task_annotations: HashMap::new(),
            broker_transport_options: BrokerTransport::default(),
            result_backend_transport_options: BackendTransport::default(),
            beat_schedule: HashMap::new(),
            custom: HashMap::new(),
        }
    }
}

impl CeleryConfig {
    /// Create a new configuration with broker URL
    #[inline]
    pub fn new(broker_url: impl Into<String>) -> Self {
        Self {
            broker_url: broker_url.into(),
            ..Default::default()
        }
    }

    /// Set broker URL
    #[inline]
    #[must_use]
    pub fn with_broker_url(mut self, url: impl Into<String>) -> Self {
        self.broker_url = url.into();
        self
    }

    /// Set result backend URL
    #[inline]
    #[must_use]
    pub fn with_result_backend(mut self, url: impl Into<String>) -> Self {
        self.result_backend = Some(url.into());
        self
    }

    /// Set task serializer
    #[inline]
    #[must_use]
    pub fn with_task_serializer(mut self, serializer: impl Into<String>) -> Self {
        self.task_serializer = serializer.into();
        self
    }

    /// Set result serializer
    #[inline]
    #[must_use]
    pub fn with_result_serializer(mut self, serializer: impl Into<String>) -> Self {
        self.result_serializer = serializer.into();
        self
    }

    /// Set accepted content types
    #[inline]
    #[must_use]
    pub fn with_accept_content(mut self, content: Vec<String>) -> Self {
        self.accept_content = content;
        self
    }

    /// Set timezone
    #[inline]
    #[must_use]
    pub fn with_timezone(mut self, tz: impl Into<String>) -> Self {
        self.timezone = tz.into();
        self
    }

    /// Enable/disable UTC
    #[must_use]
    pub const fn with_enable_utc(mut self, enabled: bool) -> Self {
        self.enable_utc = enabled;
        self
    }

    /// Set worker concurrency
    #[must_use]
    pub const fn with_worker_concurrency(mut self, concurrency: usize) -> Self {
        self.worker_concurrency = concurrency;
        self
    }

    /// Set worker prefetch multiplier
    #[must_use]
    pub const fn with_prefetch_multiplier(mut self, multiplier: usize) -> Self {
        self.worker_prefetch_multiplier = multiplier;
        self
    }

    /// Set default queue name
    #[inline]
    #[must_use]
    pub fn with_default_queue(mut self, queue: impl Into<String>) -> Self {
        self.task_default_queue = queue.into();
        self
    }

    /// Add task route
    #[inline]
    #[must_use]
    pub fn with_task_route(mut self, task: impl Into<String>, route: TaskRoute) -> Self {
        self.task_routes.insert(task.into(), route);
        self
    }

    /// Add task annotation
    #[inline]
    #[must_use]
    pub fn with_task_annotation(mut self, task: impl Into<String>, config: TaskConfig) -> Self {
        self.task_annotations.insert(task.into(), config);
        self
    }

    /// Set result expiration
    #[must_use]
    pub const fn with_result_expires(mut self, expires: u64) -> Self {
        self.result_expires = expires;
        self
    }

    /// Enable result compression
    #[inline]
    #[must_use]
    pub fn with_result_compression(mut self, algorithm: impl Into<String>) -> Self {
        self.result_compression = Some(algorithm.into());
        self
    }

    /// Set compression threshold
    #[must_use]
    pub const fn with_compression_threshold(mut self, threshold: usize) -> Self {
        self.result_compression_threshold = threshold;
        self
    }

    /// Add beat schedule
    #[inline]
    #[must_use]
    pub fn with_beat_schedule(mut self, name: impl Into<String>, schedule: BeatSchedule) -> Self {
        self.beat_schedule.insert(name.into(), schedule);
        self
    }

    /// Get task configuration for a specific task
    #[inline]
    #[must_use]
    pub fn get_task_config(&self, task_name: &str) -> Option<&TaskConfig> {
        self.task_annotations.get(task_name)
    }

    /// Get task route for a specific task
    #[inline]
    #[must_use]
    pub fn get_task_route(&self, task_name: &str) -> Option<&TaskRoute> {
        self.task_routes.get(task_name)
    }

    /// Get result expiration duration
    #[inline]
    #[must_use]
    pub const fn result_expires_duration(&self) -> Duration {
        Duration::from_secs(self.result_expires)
    }

    /// Get task time limit duration
    #[inline]
    #[must_use]
    pub fn task_time_limit_duration(&self) -> Option<Duration> {
        self.task_time_limit.map(Duration::from_secs)
    }

    /// Get task soft time limit duration
    #[inline]
    #[must_use]
    pub fn task_soft_time_limit_duration(&self) -> Option<Duration> {
        self.task_soft_time_limit.map(Duration::from_secs)
    }

    /// Load configuration from environment variables
    #[must_use]
    pub fn from_env() -> Self {
        let mut config = Self::default();

        if let Ok(url) = std::env::var("CELERY_BROKER_URL") {
            config.broker_url = url;
        }
        if let Ok(backend) = std::env::var("CELERY_RESULT_BACKEND") {
            config.result_backend = Some(backend);
        }
        if let Ok(serializer) = std::env::var("CELERY_TASK_SERIALIZER") {
            config.task_serializer = serializer;
        }
        if let Ok(tz) = std::env::var("CELERY_TIMEZONE") {
            config.timezone = tz;
        }
        if let Ok(concurrency) = std::env::var("CELERYD_CONCURRENCY") {
            if let Ok(val) = concurrency.parse() {
                config.worker_concurrency = val;
            }
        }

        config
    }

    /// Validate configuration
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid (e.g., empty broker URL, invalid concurrency, unsupported serializer).
    pub fn validate(&self) -> Result<(), String> {
        if self.broker_url.is_empty() {
            return Err("broker_url is required".to_string());
        }

        if self.worker_concurrency == 0 {
            return Err("worker_concurrency must be greater than 0".to_string());
        }

        if !["json", "msgpack", "yaml", "pickle"].contains(&self.task_serializer.as_str()) {
            return Err(format!(
                "Unsupported task_serializer: {}",
                self.task_serializer
            ));
        }

        Ok(())
    }
}

// Default value functions
fn default_serializer() -> String {
    "json".to_string()
}

fn default_accept_content() -> Vec<String> {
    vec!["json".to_string(), "msgpack".to_string()]
}

fn default_timezone() -> String {
    "UTC".to_string()
}

fn default_true() -> bool {
    true
}

fn default_concurrency() -> usize {
    num_cpus::get()
}

fn default_prefetch_multiplier() -> usize {
    4
}

fn default_heartbeat_interval() -> u64 {
    10
}

fn default_queue_name() -> String {
    "celery".to_string()
}

fn default_exchange_type() -> String {
    "direct".to_string()
}

fn default_retry_delay() -> u64 {
    180 // 3 minutes
}

fn default_max_retries() -> u32 {
    3
}

fn default_result_expires() -> u64 {
    86400 // 24 hours
}

fn default_compression_threshold() -> usize {
    1024 * 1024 // 1MB
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = CeleryConfig::default();
        assert_eq!(config.broker_url, "redis://localhost:6379/0");
        assert_eq!(config.task_serializer, "json");
        assert_eq!(config.timezone, "UTC");
        assert!(config.enable_utc);
    }

    #[test]
    fn test_config_builder() {
        let config = CeleryConfig::new("redis://localhost:6379/0")
            .with_result_backend("redis://localhost:6379/1")
            .with_worker_concurrency(8)
            .with_default_queue("my_queue");

        assert_eq!(config.worker_concurrency, 8);
        assert_eq!(config.task_default_queue, "my_queue");
    }

    #[test]
    fn test_config_validation() {
        let config = CeleryConfig::default();
        assert!(config.validate().is_ok());

        let invalid = CeleryConfig {
            broker_url: String::new(),
            ..Default::default()
        };
        assert!(invalid.validate().is_err());
    }

    #[test]
    fn test_task_route() {
        let route = TaskRoute {
            queue: "high_priority".to_string(),
            exchange: Some("tasks".to_string()),
            routing_key: Some("task.high".to_string()),
            priority: Some(9),
        };

        let config = CeleryConfig::default().with_task_route("important_task", route);

        assert!(config.get_task_route("important_task").is_some());
    }

    #[test]
    fn test_duration_conversions() {
        let config = CeleryConfig::default();
        assert_eq!(config.result_expires_duration(), Duration::from_secs(86400));
    }
}
