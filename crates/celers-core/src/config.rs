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
    ///
    /// Supports all standard `CELERY_*` and `CELERYD_*` environment variables.
    /// Boolean values accept: `true`/`false`, `1`/`0`, `yes`/`no`, `on`/`off`.
    #[must_use]
    pub fn from_env() -> Self {
        let mut config = Self::default();

        // String env vars
        if let Ok(url) = std::env::var("CELERY_BROKER_URL") {
            config.broker_url = url;
        }
        if let Ok(backend) = std::env::var("CELERY_RESULT_BACKEND") {
            config.result_backend = Some(backend);
        }
        if let Ok(serializer) = std::env::var("CELERY_TASK_SERIALIZER") {
            config.task_serializer = serializer;
        }
        if let Ok(serializer) = std::env::var("CELERY_RESULT_SERIALIZER") {
            config.result_serializer = serializer;
        }
        if let Ok(tz) = std::env::var("CELERY_TIMEZONE") {
            config.timezone = tz;
        }
        if let Ok(queue) = std::env::var("CELERY_DEFAULT_QUEUE") {
            config.task_default_queue = queue;
        }
        if let Ok(exchange) = std::env::var("CELERY_DEFAULT_EXCHANGE") {
            config.task_default_exchange = exchange;
        }
        if let Ok(exchange_type) = std::env::var("CELERY_DEFAULT_EXCHANGE_TYPE") {
            config.task_default_exchange_type = exchange_type;
        }
        if let Ok(routing_key) = std::env::var("CELERY_DEFAULT_ROUTING_KEY") {
            config.task_default_routing_key = routing_key;
        }

        // Boolean env vars
        if let Some(val) = parse_env_bool("CELERY_ENABLE_UTC") {
            config.enable_utc = val;
        }
        if let Some(val) = parse_env_bool("CELERY_TASK_TRACK_STARTED") {
            config.task_track_started = val;
        }
        if let Some(val) = parse_env_bool("CELERY_TASK_SEND_SENT_EVENT") {
            config.task_send_sent_event = val;
        }
        if let Some(val) = parse_env_bool("CELERY_TASK_ACKS_LATE") {
            config.task_acks_late = val;
        }
        if let Some(val) = parse_env_bool("CELERY_TASK_REJECT_ON_WORKER_LOST") {
            config.task_reject_on_worker_lost = val;
        }

        // Numeric env vars
        if let Ok(concurrency) = std::env::var("CELERYD_CONCURRENCY") {
            if let Ok(val) = concurrency.parse() {
                config.worker_concurrency = val;
            }
        }
        if let Some(val) = parse_env_usize("CELERYD_PREFETCH_MULTIPLIER") {
            config.worker_prefetch_multiplier = val;
        }
        if let Some(val) = parse_env_usize("CELERYD_MAX_TASKS_PER_CHILD") {
            config.worker_max_tasks_per_child = Some(val);
        }
        if let Some(val) = parse_env_usize("CELERYD_MAX_MEMORY_PER_CHILD") {
            config.worker_max_memory_per_child = Some(val);
        }

        // Duration / numeric env vars (seconds)
        if let Some(val) = parse_env_u64("CELERY_TASK_TIME_LIMIT") {
            config.task_time_limit = Some(val);
        }
        if let Some(val) = parse_env_u64("CELERY_TASK_SOFT_TIME_LIMIT") {
            config.task_soft_time_limit = Some(val);
        }
        if let Some(val) = parse_env_u64("CELERY_TASK_DEFAULT_RETRY_DELAY") {
            config.task_default_retry_delay = val;
        }
        if let Some(val) = parse_env_u32("CELERY_TASK_MAX_RETRIES") {
            config.task_max_retries = val;
        }
        if let Some(val) = parse_env_u64("CELERY_RESULT_EXPIRES") {
            config.result_expires = val;
        }

        config
    }

    /// Perform detailed configuration validation, returning structured errors and warnings
    pub fn validate_detailed(&self) -> ConfigValidation {
        let mut validation = ConfigValidation::new();

        // Validate broker URL
        if self.broker_url.is_empty() {
            validation.add_error(
                "broker_url",
                "broker URL is required",
                Some("set CELERY_BROKER_URL environment variable".to_string()),
            );
        } else if !self.broker_url.starts_with("redis://")
            && !self.broker_url.starts_with("rediss://")
            && !self.broker_url.starts_with("amqp://")
            && !self.broker_url.starts_with("amqps://")
            && !self.broker_url.starts_with("sqs://")
            && !self.broker_url.starts_with("postgres://")
            && !self.broker_url.starts_with("postgresql://")
            && !self.broker_url.starts_with("mysql://")
        {
            validation.add_error(
                "broker_url",
                format!("unrecognized broker URL scheme: {}", self.broker_url),
                Some("use redis://, amqp://, sqs://, postgres://, or mysql://".to_string()),
            );
        }

        // Validate result backend URL
        if let Some(ref url) = self.result_backend {
            if !url.starts_with("redis://")
                && !url.starts_with("rediss://")
                && !url.starts_with("postgres://")
                && !url.starts_with("postgresql://")
                && !url.starts_with("mysql://")
                && !url.starts_with("grpc://")
            {
                validation.add_error(
                    "result_backend",
                    format!("unrecognized result backend URL scheme: {}", url),
                    Some("use redis://, postgres://, mysql://, or grpc://".to_string()),
                );
            }
        }

        // Validate serializers
        let valid_serializers = ["json", "msgpack", "yaml", "pickle", "bson", "protobuf"];
        if !valid_serializers.contains(&self.task_serializer.as_str()) {
            validation.add_error(
                "task_serializer",
                format!("unknown serializer: {}", self.task_serializer),
                Some(format!("use one of: {}", valid_serializers.join(", "))),
            );
        }
        if !valid_serializers.contains(&self.result_serializer.as_str()) {
            validation.add_error(
                "result_serializer",
                format!("unknown serializer: {}", self.result_serializer),
                Some(format!("use one of: {}", valid_serializers.join(", "))),
            );
        }

        // Validate concurrency
        if self.worker_concurrency == 0 {
            validation.add_error(
                "worker_concurrency",
                "concurrency must be at least 1",
                Some("set CELERYD_CONCURRENCY to a positive integer".to_string()),
            );
        }
        if self.worker_concurrency > 1024 {
            validation.add_warning(
                "worker_concurrency",
                format!(
                    "high concurrency value ({}), may cause resource exhaustion",
                    self.worker_concurrency
                ),
            );
        }

        // Validate time limits
        if let (Some(hard), Some(soft)) = (self.task_time_limit, self.task_soft_time_limit) {
            if soft >= hard {
                validation.add_warning(
                    "task_soft_time_limit",
                    "soft time limit should be less than hard time limit",
                );
            }
        }

        // Validate prefetch
        if self.worker_prefetch_multiplier == 0 {
            validation.add_warning(
                "worker_prefetch_multiplier",
                "prefetch multiplier of 0 disables prefetching, consider setting to 1",
            );
        }

        // Validate retry settings
        if self.task_max_retries > 100 {
            validation.add_warning(
                "task_max_retries",
                format!(
                    "high max retries ({}), may cause infinite retry loops",
                    self.task_max_retries
                ),
            );
        }

        validation
    }

    /// Export configuration as environment variable key-value pairs
    pub fn to_env_vars(&self) -> Vec<(String, String)> {
        let mut vars = Vec::new();

        vars.push(("CELERY_BROKER_URL".to_string(), self.broker_url.clone()));
        if let Some(ref backend) = self.result_backend {
            vars.push(("CELERY_RESULT_BACKEND".to_string(), backend.clone()));
        }
        vars.push((
            "CELERY_TASK_SERIALIZER".to_string(),
            self.task_serializer.clone(),
        ));
        vars.push((
            "CELERY_RESULT_SERIALIZER".to_string(),
            self.result_serializer.clone(),
        ));
        vars.push(("CELERY_TIMEZONE".to_string(), self.timezone.clone()));
        vars.push(("CELERY_ENABLE_UTC".to_string(), self.enable_utc.to_string()));
        vars.push((
            "CELERY_TASK_TRACK_STARTED".to_string(),
            self.task_track_started.to_string(),
        ));
        vars.push((
            "CELERY_TASK_SEND_SENT_EVENT".to_string(),
            self.task_send_sent_event.to_string(),
        ));
        vars.push((
            "CELERY_TASK_ACKS_LATE".to_string(),
            self.task_acks_late.to_string(),
        ));
        vars.push((
            "CELERY_TASK_REJECT_ON_WORKER_LOST".to_string(),
            self.task_reject_on_worker_lost.to_string(),
        ));
        vars.push((
            "CELERYD_CONCURRENCY".to_string(),
            self.worker_concurrency.to_string(),
        ));
        vars.push((
            "CELERYD_PREFETCH_MULTIPLIER".to_string(),
            self.worker_prefetch_multiplier.to_string(),
        ));
        if let Some(val) = self.worker_max_tasks_per_child {
            vars.push(("CELERYD_MAX_TASKS_PER_CHILD".to_string(), val.to_string()));
        }
        if let Some(val) = self.worker_max_memory_per_child {
            vars.push(("CELERYD_MAX_MEMORY_PER_CHILD".to_string(), val.to_string()));
        }
        vars.push((
            "CELERY_DEFAULT_QUEUE".to_string(),
            self.task_default_queue.clone(),
        ));
        vars.push((
            "CELERY_DEFAULT_EXCHANGE".to_string(),
            self.task_default_exchange.clone(),
        ));
        vars.push((
            "CELERY_DEFAULT_EXCHANGE_TYPE".to_string(),
            self.task_default_exchange_type.clone(),
        ));
        vars.push((
            "CELERY_DEFAULT_ROUTING_KEY".to_string(),
            self.task_default_routing_key.clone(),
        ));
        if let Some(val) = self.task_time_limit {
            vars.push(("CELERY_TASK_TIME_LIMIT".to_string(), val.to_string()));
        }
        if let Some(val) = self.task_soft_time_limit {
            vars.push(("CELERY_TASK_SOFT_TIME_LIMIT".to_string(), val.to_string()));
        }
        vars.push((
            "CELERY_TASK_DEFAULT_RETRY_DELAY".to_string(),
            self.task_default_retry_delay.to_string(),
        ));
        vars.push((
            "CELERY_TASK_MAX_RETRIES".to_string(),
            self.task_max_retries.to_string(),
        ));
        vars.push((
            "CELERY_RESULT_EXPIRES".to_string(),
            self.result_expires.to_string(),
        ));

        vars
    }

    /// Dump configuration as a formatted debug string
    pub fn dump(&self) -> String {
        let mut output = String::from("CeleRS Configuration:\n");
        output.push_str(&format!("  broker_url: {}\n", self.broker_url));
        output.push_str(&format!("  result_backend: {:?}\n", self.result_backend));
        output.push_str(&format!("  task_serializer: {}\n", self.task_serializer));
        output.push_str(&format!(
            "  result_serializer: {}\n",
            self.result_serializer
        ));
        output.push_str(&format!("  timezone: {}\n", self.timezone));
        output.push_str(&format!("  enable_utc: {}\n", self.enable_utc));
        output.push_str(&format!(
            "  task_track_started: {}\n",
            self.task_track_started
        ));
        output.push_str(&format!(
            "  task_send_sent_event: {}\n",
            self.task_send_sent_event
        ));
        output.push_str(&format!("  task_acks_late: {}\n", self.task_acks_late));
        output.push_str(&format!(
            "  task_reject_on_worker_lost: {}\n",
            self.task_reject_on_worker_lost
        ));
        output.push_str(&format!(
            "  worker_concurrency: {}\n",
            self.worker_concurrency
        ));
        output.push_str(&format!(
            "  worker_prefetch_multiplier: {}\n",
            self.worker_prefetch_multiplier
        ));
        output.push_str(&format!(
            "  worker_max_tasks_per_child: {:?}\n",
            self.worker_max_tasks_per_child
        ));
        output.push_str(&format!(
            "  worker_max_memory_per_child: {:?}\n",
            self.worker_max_memory_per_child
        ));
        output.push_str(&format!("  worker_heartbeat: {}s\n", self.worker_heartbeat));
        output.push_str(&format!(
            "  task_default_queue: {}\n",
            self.task_default_queue
        ));
        output.push_str(&format!(
            "  task_default_exchange: {}\n",
            self.task_default_exchange
        ));
        output.push_str(&format!(
            "  task_default_exchange_type: {}\n",
            self.task_default_exchange_type
        ));
        output.push_str(&format!(
            "  task_default_routing_key: {}\n",
            self.task_default_routing_key
        ));
        output.push_str(&format!("  task_time_limit: {:?}\n", self.task_time_limit));
        output.push_str(&format!(
            "  task_soft_time_limit: {:?}\n",
            self.task_soft_time_limit
        ));
        output.push_str(&format!(
            "  task_default_retry_delay: {}s\n",
            self.task_default_retry_delay
        ));
        output.push_str(&format!("  task_max_retries: {}\n", self.task_max_retries));
        output.push_str(&format!("  result_expires: {}s\n", self.result_expires));
        output.push_str(&format!(
            "  result_compression: {:?}\n",
            self.result_compression
        ));
        output.push_str(&format!(
            "  result_compression_threshold: {} bytes\n",
            self.result_compression_threshold
        ));
        output.push_str(&format!(
            "  task_routes: {} route(s)\n",
            self.task_routes.len()
        ));
        output.push_str(&format!(
            "  task_annotations: {} annotation(s)\n",
            self.task_annotations.len()
        ));
        output.push_str(&format!(
            "  beat_schedule: {} schedule(s)\n",
            self.beat_schedule.len()
        ));
        output
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

// --- Environment variable parsing helpers ---

/// Parse an environment variable as a boolean.
///
/// Accepts: `true`/`false`, `1`/`0`, `yes`/`no`, `on`/`off` (case-insensitive).
fn parse_env_bool(var: &str) -> Option<bool> {
    std::env::var(var)
        .ok()
        .and_then(|v| match v.to_lowercase().as_str() {
            "true" | "1" | "yes" | "on" => Some(true),
            "false" | "0" | "no" | "off" => Some(false),
            _ => None,
        })
}

fn parse_env_u64(var: &str) -> Option<u64> {
    std::env::var(var).ok().and_then(|v| v.parse().ok())
}

fn parse_env_u32(var: &str) -> Option<u32> {
    std::env::var(var).ok().and_then(|v| v.parse().ok())
}

fn parse_env_usize(var: &str) -> Option<usize> {
    std::env::var(var).ok().and_then(|v| v.parse().ok())
}

// --- Detailed configuration validation types ---

/// Detailed configuration validation result containing structured errors and warnings
#[derive(Debug, Clone, Default)]
pub struct ConfigValidation {
    /// Configuration errors that must be fixed
    pub errors: Vec<ConfigError>,
    /// Configuration warnings that may indicate suboptimal settings
    pub warnings: Vec<ConfigWarning>,
}

impl ConfigValidation {
    /// Create an empty validation result
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if there are no errors
    pub fn is_valid(&self) -> bool {
        self.errors.is_empty()
    }

    /// Returns `true` if there are any warnings
    pub fn has_warnings(&self) -> bool {
        !self.warnings.is_empty()
    }

    /// Number of errors
    pub fn error_count(&self) -> usize {
        self.errors.len()
    }

    /// Number of warnings
    pub fn warning_count(&self) -> usize {
        self.warnings.len()
    }

    fn add_error(
        &mut self,
        field: impl Into<String>,
        message: impl Into<String>,
        suggestion: Option<String>,
    ) {
        self.errors.push(ConfigError {
            field: field.into(),
            message: message.into(),
            suggestion,
        });
    }

    fn add_warning(&mut self, field: impl Into<String>, message: impl Into<String>) {
        self.warnings.push(ConfigWarning {
            field: field.into(),
            message: message.into(),
        });
    }
}

/// A configuration error with optional suggestion for fixing it
#[derive(Debug, Clone)]
pub struct ConfigError {
    /// The configuration field name
    pub field: String,
    /// Human-readable error message
    pub message: String,
    /// Optional suggestion for fixing the error
    pub suggestion: Option<String>,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.field, self.message)?;
        if let Some(ref suggestion) = self.suggestion {
            write!(f, " (suggestion: {})", suggestion)?;
        }
        Ok(())
    }
}

/// A configuration warning indicating a potentially suboptimal setting
#[derive(Debug, Clone)]
pub struct ConfigWarning {
    /// The configuration field name
    pub field: String,
    /// Human-readable warning message
    pub message: String,
}

impl std::fmt::Display for ConfigWarning {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "[{}] {}", self.field, self.message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Mutex to serialize env-var-mutating tests (env vars are process-global)
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    /// Helper to set env vars safely in tests.
    /// SAFETY: These are test-only calls. We hold ENV_LOCK to prevent races.
    fn set_env(key: &str, val: &str) {
        unsafe { std::env::set_var(key, val) };
    }

    /// Helper to remove env vars safely in tests.
    fn remove_env(key: &str) {
        unsafe { std::env::remove_var(key) };
    }

    /// List of all CELERY_* env var keys used by from_env(), for cleanup.
    const ALL_ENV_KEYS: &[&str] = &[
        "CELERY_BROKER_URL",
        "CELERY_RESULT_BACKEND",
        "CELERY_TASK_SERIALIZER",
        "CELERY_RESULT_SERIALIZER",
        "CELERY_TIMEZONE",
        "CELERY_DEFAULT_QUEUE",
        "CELERY_DEFAULT_EXCHANGE",
        "CELERY_DEFAULT_EXCHANGE_TYPE",
        "CELERY_DEFAULT_ROUTING_KEY",
        "CELERY_ENABLE_UTC",
        "CELERY_TASK_TRACK_STARTED",
        "CELERY_TASK_SEND_SENT_EVENT",
        "CELERY_TASK_ACKS_LATE",
        "CELERY_TASK_REJECT_ON_WORKER_LOST",
        "CELERYD_CONCURRENCY",
        "CELERYD_PREFETCH_MULTIPLIER",
        "CELERYD_MAX_TASKS_PER_CHILD",
        "CELERYD_MAX_MEMORY_PER_CHILD",
        "CELERY_TASK_TIME_LIMIT",
        "CELERY_TASK_SOFT_TIME_LIMIT",
        "CELERY_TASK_DEFAULT_RETRY_DELAY",
        "CELERY_TASK_MAX_RETRIES",
        "CELERY_RESULT_EXPIRES",
    ];

    fn cleanup_env() {
        for key in ALL_ENV_KEYS {
            remove_env(key);
        }
    }

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

    #[test]
    fn test_from_env_boolean_vars() {
        let _guard = ENV_LOCK.lock();
        cleanup_env();

        set_env("CELERY_ENABLE_UTC", "true");
        set_env("CELERY_TASK_TRACK_STARTED", "1");
        set_env("CELERY_TASK_SEND_SENT_EVENT", "yes");
        set_env("CELERY_TASK_ACKS_LATE", "on");
        set_env("CELERY_TASK_REJECT_ON_WORKER_LOST", "false");

        let config = CeleryConfig::from_env();
        assert!(config.enable_utc);
        assert!(config.task_track_started);
        assert!(config.task_send_sent_event);
        assert!(config.task_acks_late);
        assert!(!config.task_reject_on_worker_lost);

        cleanup_env();
    }

    #[test]
    fn test_from_env_numeric_vars() {
        let _guard = ENV_LOCK.lock();
        cleanup_env();

        set_env("CELERYD_PREFETCH_MULTIPLIER", "8");
        set_env("CELERYD_CONCURRENCY", "16");
        set_env("CELERYD_MAX_TASKS_PER_CHILD", "1000");
        set_env("CELERYD_MAX_MEMORY_PER_CHILD", "524288");

        let config = CeleryConfig::from_env();
        assert_eq!(config.worker_prefetch_multiplier, 8);
        assert_eq!(config.worker_concurrency, 16);
        assert_eq!(config.worker_max_tasks_per_child, Some(1000));
        assert_eq!(config.worker_max_memory_per_child, Some(524288));

        cleanup_env();
    }

    #[test]
    fn test_from_env_string_vars() {
        let _guard = ENV_LOCK.lock();
        cleanup_env();

        set_env("CELERY_DEFAULT_QUEUE", "myqueue");
        set_env("CELERY_DEFAULT_EXCHANGE", "myexchange");
        set_env("CELERY_DEFAULT_EXCHANGE_TYPE", "topic");
        set_env("CELERY_DEFAULT_ROUTING_KEY", "task.default");
        set_env("CELERY_RESULT_SERIALIZER", "msgpack");

        let config = CeleryConfig::from_env();
        assert_eq!(config.task_default_queue, "myqueue");
        assert_eq!(config.task_default_exchange, "myexchange");
        assert_eq!(config.task_default_exchange_type, "topic");
        assert_eq!(config.task_default_routing_key, "task.default");
        assert_eq!(config.result_serializer, "msgpack");

        cleanup_env();
    }

    #[test]
    fn test_from_env_duration_vars() {
        let _guard = ENV_LOCK.lock();
        cleanup_env();

        set_env("CELERY_TASK_TIME_LIMIT", "300");
        set_env("CELERY_TASK_SOFT_TIME_LIMIT", "240");
        set_env("CELERY_TASK_DEFAULT_RETRY_DELAY", "60");
        set_env("CELERY_TASK_MAX_RETRIES", "5");
        set_env("CELERY_RESULT_EXPIRES", "3600");

        let config = CeleryConfig::from_env();
        assert_eq!(config.task_time_limit, Some(300));
        assert_eq!(config.task_soft_time_limit, Some(240));
        assert_eq!(config.task_default_retry_delay, 60);
        assert_eq!(config.task_max_retries, 5);
        assert_eq!(config.result_expires, 3600);

        cleanup_env();
    }

    #[test]
    fn test_parse_env_bool_variants() {
        let _guard = ENV_LOCK.lock();
        cleanup_env();

        // Truthy values
        for val in &["true", "TRUE", "True", "1", "yes", "YES", "on", "ON"] {
            set_env("CELERY_ENABLE_UTC", val);
            assert_eq!(
                parse_env_bool("CELERY_ENABLE_UTC"),
                Some(true),
                "failed for {}",
                val
            );
        }

        // Falsy values
        for val in &["false", "FALSE", "False", "0", "no", "NO", "off", "OFF"] {
            set_env("CELERY_ENABLE_UTC", val);
            assert_eq!(
                parse_env_bool("CELERY_ENABLE_UTC"),
                Some(false),
                "failed for {}",
                val
            );
        }

        // Invalid values return None
        set_env("CELERY_ENABLE_UTC", "maybe");
        assert_eq!(parse_env_bool("CELERY_ENABLE_UTC"), None);

        // Missing var returns None
        remove_env("CELERY_ENABLE_UTC");
        assert_eq!(parse_env_bool("CELERY_ENABLE_UTC"), None);

        cleanup_env();
    }

    #[test]
    fn test_validate_detailed_valid_config() {
        let config = CeleryConfig::default();
        let validation = config.validate_detailed();
        assert!(validation.is_valid());
        assert_eq!(validation.error_count(), 0);
    }

    #[test]
    fn test_validate_detailed_invalid_broker_url() {
        let config = CeleryConfig {
            broker_url: "ftp://bad-scheme".to_string(),
            ..Default::default()
        };
        let validation = config.validate_detailed();
        assert!(!validation.is_valid());
        assert!(validation.errors.iter().any(|e| e.field == "broker_url"));
    }

    #[test]
    fn test_validate_detailed_invalid_serializer() {
        let config = CeleryConfig {
            task_serializer: "xml".to_string(),
            ..Default::default()
        };
        let validation = config.validate_detailed();
        assert!(!validation.is_valid());
        assert!(validation
            .errors
            .iter()
            .any(|e| e.field == "task_serializer"));
    }

    #[test]
    fn test_validate_detailed_zero_concurrency() {
        let config = CeleryConfig {
            worker_concurrency: 0,
            ..Default::default()
        };
        let validation = config.validate_detailed();
        assert!(!validation.is_valid());
        assert!(validation
            .errors
            .iter()
            .any(|e| e.field == "worker_concurrency"));
    }

    #[test]
    fn test_validate_detailed_time_limit_warning() {
        let config = CeleryConfig {
            task_time_limit: Some(60),
            task_soft_time_limit: Some(120), // soft >= hard
            ..Default::default()
        };
        let validation = config.validate_detailed();
        assert!(validation.has_warnings());
        assert!(validation
            .warnings
            .iter()
            .any(|w| w.field == "task_soft_time_limit"));
    }

    #[test]
    fn test_to_env_vars_roundtrip() {
        let config = CeleryConfig::new("amqp://localhost:5672")
            .with_result_backend("redis://localhost:6379/1")
            .with_task_serializer("msgpack")
            .with_result_serializer("json")
            .with_timezone("US/Eastern")
            .with_enable_utc(false)
            .with_worker_concurrency(12)
            .with_prefetch_multiplier(2)
            .with_default_queue("tasks");

        let vars = config.to_env_vars();

        // Check that key env vars are present with correct values
        let find_var = |key: &str| -> Option<String> {
            vars.iter().find(|(k, _)| k == key).map(|(_, v)| v.clone())
        };

        assert_eq!(
            find_var("CELERY_BROKER_URL").as_deref(),
            Some("amqp://localhost:5672")
        );
        assert_eq!(
            find_var("CELERY_RESULT_BACKEND").as_deref(),
            Some("redis://localhost:6379/1")
        );
        assert_eq!(
            find_var("CELERY_TASK_SERIALIZER").as_deref(),
            Some("msgpack")
        );
        assert_eq!(
            find_var("CELERY_RESULT_SERIALIZER").as_deref(),
            Some("json")
        );
        assert_eq!(find_var("CELERY_TIMEZONE").as_deref(), Some("US/Eastern"));
        assert_eq!(find_var("CELERY_ENABLE_UTC").as_deref(), Some("false"));
        assert_eq!(find_var("CELERYD_CONCURRENCY").as_deref(), Some("12"));
        assert_eq!(
            find_var("CELERYD_PREFETCH_MULTIPLIER").as_deref(),
            Some("2")
        );
        assert_eq!(find_var("CELERY_DEFAULT_QUEUE").as_deref(), Some("tasks"));
    }

    #[test]
    fn test_dump_output() {
        let config = CeleryConfig::default();
        let output = config.dump();

        assert!(output.starts_with("CeleRS Configuration:\n"));
        assert!(output.contains("broker_url:"));
        assert!(output.contains("task_serializer:"));
        assert!(output.contains("worker_concurrency:"));
        assert!(output.contains("result_expires:"));
        assert!(output.contains("task_routes:"));
        assert!(output.contains("beat_schedule:"));
    }

    #[test]
    fn test_config_validation_display() {
        let error = ConfigError {
            field: "broker_url".to_string(),
            message: "invalid URL".to_string(),
            suggestion: Some("use redis://".to_string()),
        };
        let display = format!("{}", error);
        assert!(display.contains("[broker_url]"));
        assert!(display.contains("invalid URL"));
        assert!(display.contains("suggestion: use redis://"));

        let error_no_suggestion = ConfigError {
            field: "concurrency".to_string(),
            message: "must be positive".to_string(),
            suggestion: None,
        };
        let display2 = format!("{}", error_no_suggestion);
        assert!(display2.contains("[concurrency]"));
        assert!(display2.contains("must be positive"));
        assert!(!display2.contains("suggestion"));

        let warning = ConfigWarning {
            field: "prefetch".to_string(),
            message: "value too high".to_string(),
        };
        let display3 = format!("{}", warning);
        assert!(display3.contains("[prefetch]"));
        assert!(display3.contains("value too high"));
    }

    #[test]
    fn test_validate_detailed_high_concurrency_warning() {
        let config = CeleryConfig {
            worker_concurrency: 2048,
            ..Default::default()
        };
        let validation = config.validate_detailed();
        assert!(validation.has_warnings());
        assert!(validation
            .warnings
            .iter()
            .any(|w| w.field == "worker_concurrency"));
    }

    #[test]
    fn test_validate_detailed_prefetch_zero_warning() {
        let config = CeleryConfig {
            worker_prefetch_multiplier: 0,
            ..Default::default()
        };
        let validation = config.validate_detailed();
        assert!(validation.has_warnings());
        assert!(validation
            .warnings
            .iter()
            .any(|w| w.field == "worker_prefetch_multiplier"));
    }

    #[test]
    fn test_validate_detailed_high_retries_warning() {
        let config = CeleryConfig {
            task_max_retries: 200,
            ..Default::default()
        };
        let validation = config.validate_detailed();
        assert!(validation.has_warnings());
        assert!(validation
            .warnings
            .iter()
            .any(|w| w.field == "task_max_retries"));
    }

    #[test]
    fn test_validate_detailed_invalid_result_backend() {
        let config = CeleryConfig {
            result_backend: Some("ftp://invalid".to_string()),
            ..Default::default()
        };
        let validation = config.validate_detailed();
        assert!(!validation.is_valid());
        assert!(validation
            .errors
            .iter()
            .any(|e| e.field == "result_backend"));
    }

    #[test]
    fn test_validate_detailed_invalid_result_serializer() {
        let config = CeleryConfig {
            result_serializer: "xml".to_string(),
            ..Default::default()
        };
        let validation = config.validate_detailed();
        assert!(!validation.is_valid());
        assert!(validation
            .errors
            .iter()
            .any(|e| e.field == "result_serializer"));
    }

    #[test]
    fn test_config_validation_counts() {
        let mut validation = ConfigValidation::new();
        assert!(validation.is_valid());
        assert!(!validation.has_warnings());
        assert_eq!(validation.error_count(), 0);
        assert_eq!(validation.warning_count(), 0);

        validation.add_error("f1", "e1", None);
        validation.add_warning("f2", "w1");
        assert!(!validation.is_valid());
        assert!(validation.has_warnings());
        assert_eq!(validation.error_count(), 1);
        assert_eq!(validation.warning_count(), 1);
    }
}
