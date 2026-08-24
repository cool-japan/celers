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
    ///
    /// A value that cannot be parsed (`CELERYD_CONCURRENCY=eight`) is logged at
    /// `warn` level and the default is kept. Use
    /// [`CeleryConfig::from_env_checked`] to turn such a value into an error
    /// instead of starting on a silently different configuration.
    #[must_use]
    pub fn from_env() -> Self {
        let (config, errors) = Self::load_env();
        for error in errors {
            tracing::warn!(
                field = %error.field,
                message = %error.message,
                "Ignoring unparseable environment variable; using the default instead"
            );
        }
        config
    }

    /// Load configuration from environment variables, failing on any value that
    /// cannot be parsed.
    ///
    /// # Errors
    ///
    /// Returns every malformed environment variable, so a typo cannot silently
    /// downgrade the process to a default it was never configured with.
    pub fn from_env_checked() -> Result<Self, Vec<ConfigError>> {
        let (config, errors) = Self::load_env();
        if errors.is_empty() {
            Ok(config)
        } else {
            Err(errors)
        }
    }

    /// Shared environment loading: returns the config plus any parse failures.
    fn load_env() -> (Self, Vec<ConfigError>) {
        let mut errors: Vec<ConfigError> = Vec::new();
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

        if let Ok(content) = std::env::var("CELERY_ACCEPT_CONTENT") {
            let entries: Vec<String> = content
                .split(',')
                .map(str::trim)
                .filter(|entry| !entry.is_empty())
                .map(str::to_string)
                .collect();
            if entries.is_empty() {
                errors.push(env_error(
                    "CELERY_ACCEPT_CONTENT",
                    &content,
                    "a comma-separated list of content types",
                ));
            } else {
                config.accept_content = entries;
            }
        }

        // Boolean env vars
        {
            let mut load_bool = |var: &str, slot: &mut bool| match parse_env_bool_checked(var) {
                Ok(Some(val)) => *slot = val,
                Ok(None) => {}
                Err(raw) => errors.push(env_error(var, &raw, "true/false, 1/0, yes/no, on/off")),
            };
            load_bool("CELERY_ENABLE_UTC", &mut config.enable_utc);
            load_bool("CELERY_TASK_TRACK_STARTED", &mut config.task_track_started);
            load_bool(
                "CELERY_TASK_SEND_SENT_EVENT",
                &mut config.task_send_sent_event,
            );
            load_bool("CELERY_TASK_ACKS_LATE", &mut config.task_acks_late);
            load_bool(
                "CELERY_TASK_REJECT_ON_WORKER_LOST",
                &mut config.task_reject_on_worker_lost,
            );
        }

        // Numeric env vars
        match parse_env_number::<usize>("CELERYD_CONCURRENCY") {
            Ok(Some(val)) => config.worker_concurrency = val,
            Ok(None) => {}
            Err(raw) => errors.push(env_error("CELERYD_CONCURRENCY", &raw, "a positive integer")),
        }
        match parse_env_number::<usize>("CELERYD_PREFETCH_MULTIPLIER") {
            Ok(Some(val)) => config.worker_prefetch_multiplier = val,
            Ok(None) => {}
            Err(raw) => errors.push(env_error("CELERYD_PREFETCH_MULTIPLIER", &raw, "an integer")),
        }
        match parse_env_number::<usize>("CELERYD_MAX_TASKS_PER_CHILD") {
            Ok(Some(val)) => config.worker_max_tasks_per_child = Some(val),
            Ok(None) => {}
            Err(raw) => errors.push(env_error("CELERYD_MAX_TASKS_PER_CHILD", &raw, "an integer")),
        }
        match parse_env_number::<usize>("CELERYD_MAX_MEMORY_PER_CHILD") {
            Ok(Some(val)) => config.worker_max_memory_per_child = Some(val),
            Ok(None) => {}
            Err(raw) => errors.push(env_error(
                "CELERYD_MAX_MEMORY_PER_CHILD",
                &raw,
                "an integer (KiB)",
            )),
        }

        // Duration / numeric env vars (seconds)
        match parse_env_number::<u64>("CELERY_TASK_TIME_LIMIT") {
            Ok(Some(val)) => config.task_time_limit = Some(val),
            Ok(None) => {}
            Err(raw) => errors.push(env_error("CELERY_TASK_TIME_LIMIT", &raw, "seconds")),
        }
        match parse_env_number::<u64>("CELERY_TASK_SOFT_TIME_LIMIT") {
            Ok(Some(val)) => config.task_soft_time_limit = Some(val),
            Ok(None) => {}
            Err(raw) => errors.push(env_error("CELERY_TASK_SOFT_TIME_LIMIT", &raw, "seconds")),
        }
        match parse_env_number::<u64>("CELERY_TASK_DEFAULT_RETRY_DELAY") {
            Ok(Some(val)) => config.task_default_retry_delay = val,
            Ok(None) => {}
            Err(raw) => errors.push(env_error(
                "CELERY_TASK_DEFAULT_RETRY_DELAY",
                &raw,
                "seconds",
            )),
        }
        match parse_env_number::<u32>("CELERY_TASK_MAX_RETRIES") {
            Ok(Some(val)) => config.task_max_retries = val,
            Ok(None) => {}
            Err(raw) => errors.push(env_error("CELERY_TASK_MAX_RETRIES", &raw, "an integer")),
        }
        match parse_env_number::<u64>("CELERY_RESULT_EXPIRES") {
            Ok(Some(val)) => config.result_expires = val,
            Ok(None) => {}
            Err(raw) => errors.push(env_error("CELERY_RESULT_EXPIRES", &raw, "seconds")),
        }
        match parse_env_number::<u64>("CELERY_WORKER_HEARTBEAT") {
            Ok(Some(val)) => config.worker_heartbeat = val,
            Ok(None) => {}
            Err(raw) => errors.push(env_error("CELERY_WORKER_HEARTBEAT", &raw, "seconds")),
        }
        if let Ok(compression) = std::env::var("CELERY_RESULT_COMPRESSION") {
            config.result_compression = Some(compression);
        }
        match parse_env_number::<usize>("CELERY_RESULT_COMPRESSION_THRESHOLD") {
            Ok(Some(val)) => config.result_compression_threshold = val,
            Ok(None) => {}
            Err(raw) => errors.push(env_error(
                "CELERY_RESULT_COMPRESSION_THRESHOLD",
                &raw,
                "a byte count",
            )),
        }

        (config, errors)
    }

    /// Returns `true` if `content_type` is allowed by `accept_content`.
    ///
    /// This is the enforcement point for Celery's content-type allowlist: a
    /// message whose declared content type is not accepted must be rejected
    /// **before** its body is deserialized. Both the short serializer name
    /// (`json`) and the wire MIME type (`application/json`) are recognised, MIME
    /// parameters (`; charset=utf-8`) are ignored, and matching is
    /// case-insensitive.
    #[must_use]
    pub fn is_content_type_accepted(&self, content_type: &str) -> bool {
        let Some(name) = normalize_content_type(content_type) else {
            return false;
        };
        self.accept_content
            .iter()
            .filter_map(|accepted| normalize_content_type(accepted))
            .any(|accepted| accepted == name)
    }

    /// The accepted content types, normalized to short serializer names.
    #[must_use]
    pub fn accepted_serializer_names(&self) -> Vec<String> {
        self.accept_content
            .iter()
            .filter_map(|accepted| normalize_content_type(accepted))
            .map(str::to_string)
            .collect()
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

        // Validate serializers. `pickle` is deliberately absent: this workspace
        // does not provide a pickle serializer (see
        // `celers-protocol/src/serializer.rs`), so accepting it here would give
        // operators a false sense that it works — and it is precisely the
        // serializer whose exclusion matters most for safety.
        for (field, serializer) in [
            ("task_serializer", &self.task_serializer),
            ("result_serializer", &self.result_serializer),
        ] {
            if serializer.eq_ignore_ascii_case("pickle") {
                validation.add_error(
                    field,
                    "pickle is not supported: no pickle serializer is provided (arbitrary code \
                     execution risk)",
                    Some(format!("use one of: {}", SUPPORTED_SERIALIZERS.join(", "))),
                );
            } else if normalize_content_type(serializer).is_none() {
                validation.add_error(
                    field,
                    format!("unknown serializer: {serializer}"),
                    Some(format!("use one of: {}", SUPPORTED_SERIALIZERS.join(", "))),
                );
            }
        }

        // Validate the content-type allowlist itself.
        if self.accept_content.is_empty() {
            validation.add_error(
                "accept_content",
                "accept_content is empty: no message content type would be accepted",
                Some("list at least the serializer you use, e.g. [\"json\"]".to_string()),
            );
        }
        for entry in &self.accept_content {
            if entry.eq_ignore_ascii_case("pickle") {
                validation.add_error(
                    "accept_content",
                    "pickle is not supported and must not be accepted (arbitrary code execution \
                     risk)",
                    Some("remove \"pickle\" from accept_content".to_string()),
                );
            } else if normalize_content_type(entry).is_none() {
                validation.add_error(
                    "accept_content",
                    format!("unknown content type: {entry}"),
                    Some(format!(
                        "use serializer names or MIME types, one of: {}",
                        SUPPORTED_SERIALIZERS.join(", ")
                    )),
                );
            }
        }
        // A serializer the worker is configured to *produce* must also be
        // accepted, otherwise the messages it writes are rejected on receipt.
        for (field, serializer) in [
            ("task_serializer", &self.task_serializer),
            ("result_serializer", &self.result_serializer),
        ] {
            if !self.accept_content.is_empty()
                && normalize_content_type(serializer).is_some()
                && !self.is_content_type_accepted(serializer)
            {
                validation.add_error(
                    field,
                    format!("{serializer} is not listed in accept_content"),
                    Some(format!("add \"{serializer}\" to accept_content")),
                );
            }
        }

        // Surface unrecognised keys: `#[serde(flatten)] custom` silently absorbs
        // typos such as `worker_concurency`, which then never take effect.
        for key in self.custom.keys() {
            let suggestion = closest_known_field(key)
                .map(|field| format!("did you mean \"{field}\"?"))
                .unwrap_or_else(|| {
                    "remove it or move it under an explicitly namespaced key".to_string()
                });
            validation.add_warning(
                "custom",
                format!("unrecognised configuration key \"{key}\": {suggestion}"),
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
    ///
    /// Every scalar setting that [`CeleryConfig::from_env`] understands is
    /// exported, so `from_env` after applying these variables reproduces the
    /// scalar configuration. Structured settings (`task_routes`,
    /// `task_annotations`, `beat_schedule`, `task_configs`) have no environment
    /// representation and are intentionally omitted — persist those as a config
    /// file instead.
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
        vars.push((
            "CELERY_WORKER_HEARTBEAT".to_string(),
            self.worker_heartbeat.to_string(),
        ));
        vars.push((
            "CELERY_ACCEPT_CONTENT".to_string(),
            self.accept_content.join(","),
        ));
        if let Some(ref compression) = self.result_compression {
            vars.push(("CELERY_RESULT_COMPRESSION".to_string(), compression.clone()));
        }
        vars.push((
            "CELERY_RESULT_COMPRESSION_THRESHOLD".to_string(),
            self.result_compression_threshold.to_string(),
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
    /// Thin wrapper over [`CeleryConfig::validate_detailed`] so there is a
    /// single source of truth for what "valid" means; it returns the first
    /// error as a string.
    ///
    /// # Errors
    ///
    /// Returns an error if the configuration is invalid (e.g., empty broker URL, invalid concurrency, unsupported serializer).
    pub fn validate(&self) -> Result<(), String> {
        let validation = self.validate_detailed();
        match validation.errors.first() {
            Some(error) => Err(error.to_string()),
            None => Ok(()),
        }
    }
}

/// Serializer names this build recognises (`pickle` is intentionally absent).
pub const SUPPORTED_SERIALIZERS: [&str; 5] = ["json", "msgpack", "yaml", "bson", "protobuf"];

/// Normalize a content type or serializer name to its short serializer name.
///
/// Accepts short names (`json`) and wire MIME types (`application/json`,
/// `application/x-msgpack`, …), ignores MIME parameters and case. Returns
/// `None` for anything this build does not support — including `pickle`.
fn normalize_content_type(value: &str) -> Option<&'static str> {
    let value = value.split(';').next().unwrap_or(value).trim();
    let lower = value.to_ascii_lowercase();
    match lower.as_str() {
        "json" | "application/json" | "text/json" => Some("json"),
        "msgpack" | "messagepack" | "application/x-msgpack" | "application/msgpack" => {
            Some("msgpack")
        }
        "yaml" | "application/x-yaml" | "application/yaml" | "text/yaml" => Some("yaml"),
        "bson" | "application/bson" | "application/x-bson" => Some("bson"),
        "protobuf" | "application/protobuf" | "application/x-protobuf" => Some("protobuf"),
        _ => None,
    }
}

/// Known top-level configuration keys, used to suggest fixes for typos that
/// `#[serde(flatten)] custom` would otherwise swallow.
const KNOWN_CONFIG_FIELDS: [&str; 26] = [
    "broker_url",
    "result_backend",
    "task_serializer",
    "result_serializer",
    "accept_content",
    "timezone",
    "enable_utc",
    "task_track_started",
    "task_send_sent_event",
    "task_acks_late",
    "task_reject_on_worker_lost",
    "worker_concurrency",
    "worker_prefetch_multiplier",
    "worker_max_tasks_per_child",
    "worker_max_memory_per_child",
    "worker_heartbeat",
    "task_default_queue",
    "task_default_exchange",
    "task_default_exchange_type",
    "task_default_routing_key",
    "task_time_limit",
    "task_soft_time_limit",
    "task_default_retry_delay",
    "task_max_retries",
    "result_expires",
    "beat_schedule",
];

/// Return the known configuration field closest to `key`, if any is close
/// enough to be a plausible typo.
fn closest_known_field(key: &str) -> Option<&'static str> {
    let key_lower = key.to_ascii_lowercase();
    let mut best: Option<(usize, &'static str)> = None;
    for field in KNOWN_CONFIG_FIELDS {
        let distance = edit_distance(&key_lower, field);
        if best.is_none_or(|(best_distance, _)| distance < best_distance) {
            best = Some((distance, field));
        }
    }
    // Only suggest when the strings really are close (a third of the length,
    // capped at 3 edits).
    best.and_then(|(distance, field)| {
        let threshold = (key_lower.len() / 3).clamp(1, 3);
        (distance <= threshold).then_some(field)
    })
}

/// Levenshtein edit distance between two ASCII strings.
fn edit_distance(a: &str, b: &str) -> usize {
    let a: Vec<u8> = a.bytes().collect();
    let b: Vec<u8> = b.bytes().collect();
    if a.is_empty() {
        return b.len();
    }
    if b.is_empty() {
        return a.len();
    }
    let mut previous: Vec<usize> = (0..=b.len()).collect();
    let mut current = vec![0usize; b.len() + 1];
    for (i, &ca) in a.iter().enumerate() {
        current[0] = i + 1;
        for (j, &cb) in b.iter().enumerate() {
            let cost = usize::from(ca != cb);
            current[j + 1] = (previous[j] + cost)
                .min(previous[j + 1] + 1)
                .min(current[j] + 1);
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[b.len()]
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

/// Parse an environment variable as a boolean, distinguishing "unset"
/// (`Ok(None)`) from "set to something unparseable" (`Err(raw_value)`).
fn parse_env_bool_checked(var: &str) -> Result<Option<bool>, String> {
    let Ok(raw) = std::env::var(var) else {
        return Ok(None);
    };
    match raw.trim().to_lowercase().as_str() {
        "true" | "1" | "yes" | "on" => Ok(Some(true)),
        "false" | "0" | "no" | "off" => Ok(Some(false)),
        _ => Err(raw),
    }
}

/// Parse an environment variable as a number, distinguishing "unset" from "set
/// to something unparseable".
fn parse_env_number<T: std::str::FromStr>(var: &str) -> Result<Option<T>, String> {
    let Ok(raw) = std::env::var(var) else {
        return Ok(None);
    };
    match raw.trim().parse::<T>() {
        Ok(value) => Ok(Some(value)),
        Err(_) => Err(raw),
    }
}

/// Build a [`ConfigError`] describing a malformed environment variable.
fn env_error(var: &str, raw: &str, expected: &str) -> ConfigError {
    ConfigError {
        field: var.to_string(),
        message: format!("invalid value {raw:?}"),
        suggestion: Some(format!("expected {expected}")),
    }
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
mod tests;
