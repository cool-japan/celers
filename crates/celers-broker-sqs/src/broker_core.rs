//! Core SQS broker struct and fundamental methods
//!
//! Contains the `SqsBroker` struct definition, constructors, builder methods,
//! and essential internal operations (client management, queue URL resolution,
//! CloudWatch integration, compression, and retry logic).

use aws_config::BehaviorVersion;
use aws_sdk_cloudwatch::{
    types::{ComparisonOperator, Dimension, MetricDatum, StandardUnit, Statistic},
    Client as CloudWatchClient,
};
use aws_sdk_sqs::types::MessageAttributeValue;
use aws_sdk_sqs::Client;
use celers_kombu::{BrokerError, Envelope, QueueMode, Result};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info, warn};

use crate::celery_compat;
use crate::delivery::ReceiptMetadata;
use crate::retry_policy::{
    backoff_delay_ms, is_retryable_error, wall_clock_jitter_permille, MAX_RETRY_ATTEMPTS,
};
use crate::types::{
    AdaptivePollingConfig, AlarmConfig, CloudWatchConfig, DlqConfig, FifoConfig, PollingStrategy,
    SseConfig,
};
use crate::visibility::VisibilityHeartbeat;

/// Maximum number of receipt-metadata entries kept in memory.
///
/// Entries are dropped on `ack`/`reject`; this bound keeps a consumer that
/// neither acknowledges nor rejects from growing the map without limit.
const MAX_TRACKED_RECEIPTS: usize = 10_000;

/// Load the AWS configuration every client in this crate is built from.
///
/// One place, so the SQS client, the CloudWatch client and the credential
/// providers underneath them all get the same behaviour version and the same
/// HTTP transport.
///
/// With the default `pure-http` feature the transport is
/// [`crate::pure_http::pure_http_client`] — a Pure-Rust `HttpClient` over
/// `oxihttp-client`. It has to be installed explicitly because the workspace
/// does not enable `aws-config/default-https-client`: that feature hard-selects
/// `aws-smithy-http-client/rustls-aws-lc`, i.e. the FFI `aws-lc-sys` stack that
/// `deny.toml` bans. Without `pure-http`, HTTP client selection is left to the
/// application (see the feature's documentation in `Cargo.toml`).
///
/// `BehaviorVersion::latest()` is passed explicitly rather than through the
/// `behavior-version-latest` Cargo feature, so the choice is visible in source.
pub(crate) async fn load_aws_config() -> aws_config::SdkConfig {
    let loader = aws_config::defaults(BehaviorVersion::latest());

    #[cfg(feature = "pure-http")]
    let loader = loader.http_client(crate::pure_http::pure_http_client());

    loader.load().await
}

/// AWS SQS broker implementation
pub struct SqsBroker {
    pub(crate) client: Option<Client>,
    pub(crate) cloudwatch_client: Option<CloudWatchClient>,
    pub(crate) queue_name: String,
    /// Cache of queue URLs for multiple queues (queue_name -> queue_url)
    pub(crate) queue_url_cache: HashMap<String, String>,
    /// Automatically create queue if it doesn't exist
    pub(crate) auto_create_queue: bool,
    /// Visibility timeout in seconds (default 30)
    pub(crate) visibility_timeout: i32,
    /// Long polling wait time in seconds (default 20, max 20)
    pub(crate) wait_time_seconds: i32,
    /// Maximum messages to receive per poll (default 1, max 10)
    pub(crate) max_messages: i32,
    /// Dead Letter Queue configuration
    pub(crate) dlq_config: Option<DlqConfig>,
    /// FIFO queue configuration
    pub(crate) fifo_config: Option<FifoConfig>,
    /// Server-side encryption configuration
    pub(crate) sse_config: Option<SseConfig>,
    /// Message retention period in seconds (60-1209600, default 345600 = 4 days)
    pub(crate) message_retention_seconds: i32,
    /// Delay seconds for messages (0-900, default 0)
    pub(crate) delay_seconds: i32,
    /// CloudWatch metrics configuration
    pub(crate) cloudwatch_config: Option<CloudWatchConfig>,
    /// Adaptive polling configuration
    pub(crate) adaptive_polling: Option<AdaptivePollingConfig>,
    /// Enable message compression for messages larger than threshold (in bytes)
    pub(crate) compression_threshold: Option<usize>,
    /// Maximum retry attempts for AWS API calls (default 3)
    pub(crate) max_retries: u32,
    /// Base delay for retry backoff in milliseconds (default 100)
    pub(crate) retry_base_delay_ms: u64,
    /// Celery compatibility configuration for Python interop
    pub(crate) celery_config: Option<celery_compat::CelerySqsConfig>,
    /// Celery attribute mapper for message serialization
    pub(crate) celery_mapper: Option<celery_compat::CeleryAttributeMapper>,
    /// Messages received but not yet returned by `consume`, keyed by the
    /// *physical* queue they were received from.
    pub(crate) prefetch: HashMap<String, VecDeque<Envelope>>,
    /// System-attribute metadata for in-flight messages, keyed by delivery tag.
    pub(crate) receipt_metadata: HashMap<String, ReceiptMetadata>,
    /// Insertion order of `receipt_metadata`, used for bounded eviction.
    pub(crate) receipt_metadata_order: VecDeque<String>,
    /// Whether to keep in-flight messages invisible with a background heartbeat
    pub(crate) visibility_heartbeat_enabled: bool,
    /// Total seconds a single heartbeat may keep a message invisible
    pub(crate) heartbeat_max_extension_secs: i64,
    /// Running heartbeats, keyed by delivery tag
    pub(crate) heartbeats: HashMap<String, VisibilityHeartbeat>,
    /// Last observed transport/credential health, updated by every SDK call
    pub(crate) connection_healthy: Arc<AtomicBool>,
}

impl SqsBroker {
    /// Create a new SQS broker
    ///
    /// # Arguments
    /// * `queue_name` - SQS queue name (will be created if it doesn't exist)
    ///
    /// Note: For FIFO queues, the queue name must end with ".fifo"
    pub async fn new(queue_name: &str) -> Result<Self> {
        Ok(Self {
            client: None,
            cloudwatch_client: None,
            queue_name: queue_name.to_string(),
            queue_url_cache: HashMap::new(),
            auto_create_queue: false,
            visibility_timeout: 30,
            wait_time_seconds: 20, // Long polling
            max_messages: 1,
            dlq_config: None,
            fifo_config: None,
            sse_config: None,
            message_retention_seconds: 345600, // 4 days
            delay_seconds: 0,
            cloudwatch_config: None,
            adaptive_polling: None,
            compression_threshold: None,
            max_retries: 3,
            retry_base_delay_ms: 100,
            celery_config: None,
            celery_mapper: None,
            prefetch: HashMap::new(),
            receipt_metadata: HashMap::new(),
            receipt_metadata_order: VecDeque::new(),
            visibility_heartbeat_enabled: false,
            heartbeat_max_extension_secs: i64::from(crate::visibility::MAX_VISIBILITY_TIMEOUT_SECS),
            heartbeats: HashMap::new(),
            connection_healthy: Arc::new(AtomicBool::new(true)),
        })
    }

    /// Set visibility timeout (default 30 seconds, max 43200 = 12 hours)
    pub fn with_visibility_timeout(mut self, seconds: i32) -> Self {
        self.visibility_timeout = seconds.clamp(0, 43200);
        self
    }

    /// Set the long polling wait time (default 20 seconds, max 20).
    ///
    /// This is an upper bound applied to every `ReceiveMessage` request: the
    /// effective wait is `min(caller timeout, this value, 20)`. It is also
    /// written to the queue's `ReceiveMessageWaitTimeSeconds` attribute by
    /// [`create_queue`](celers_kombu::Broker::create_queue).
    pub fn with_wait_time(mut self, seconds: i32) -> Self {
        self.wait_time_seconds = seconds.clamp(0, 20);
        self
    }

    /// Set the number of messages fetched per `ReceiveMessage` (default 1, max 10).
    ///
    /// Values above 1 turn on *prefetching*: [`consume`](celers_kombu::Consumer::consume)
    /// fetches up to this many messages in one request, returns the first and
    /// buffers the rest per queue, so subsequent calls are served without an
    /// API round trip. That is where the 10x cost reduction comes from.
    ///
    /// The buffered messages are already in flight, so their visibility
    /// timeout is running: pair a batch size above 1 with a visibility timeout
    /// comfortably larger than `max_messages * handler duration`, or enable
    /// [`with_visibility_heartbeat`](Self::with_visibility_heartbeat).
    pub fn with_max_messages(mut self, max: i32) -> Self {
        self.max_messages = max.clamp(1, 10);
        self
    }

    /// Keep in-flight messages invisible with a background heartbeat.
    ///
    /// When enabled, every message returned by
    /// [`consume`](celers_kombu::Consumer::consume) or
    /// [`consume_batch`](Self::consume_batch) gets a background task that calls
    /// `ChangeMessageVisibility` every `visibility_timeout / 3` seconds until
    /// the message is acknowledged, rejected, or the extension budget set by
    /// [`with_heartbeat_max_extension`](Self::with_heartbeat_max_extension) is
    /// exhausted.
    ///
    /// Without it, a handler that outruns the visibility timeout has its
    /// message redelivered to a second worker while it is still executing.
    pub fn with_visibility_heartbeat(mut self, enabled: bool) -> Self {
        self.visibility_heartbeat_enabled = enabled;
        self
    }

    /// Total seconds a single heartbeat may keep one message invisible.
    ///
    /// Defaults to SQS's own 12-hour ceiling. Lower it so that a wedged handler
    /// eventually releases its message instead of holding it forever.
    pub fn with_heartbeat_max_extension(mut self, seconds: i64) -> Self {
        self.heartbeat_max_extension_secs =
            seconds.clamp(1, i64::from(crate::visibility::MAX_VISIBILITY_TIMEOUT_SECS));
        self
    }

    /// Configure Dead Letter Queue (DLQ)
    ///
    /// Messages that fail processing more than `max_receive_count` times
    /// will be moved to the DLQ.
    pub fn with_dlq(mut self, config: DlqConfig) -> Self {
        self.dlq_config = Some(config);
        self
    }

    /// Configure FIFO queue settings
    ///
    /// Note: Queue name must end with ".fifo" for FIFO queues
    pub fn with_fifo(mut self, config: FifoConfig) -> Self {
        self.fifo_config = Some(config);
        self
    }

    /// Configure server-side encryption
    pub fn with_sse(mut self, config: SseConfig) -> Self {
        self.sse_config = Some(config);
        self
    }

    /// Set message retention period (default 4 days)
    ///
    /// Valid range: 60 seconds to 1209600 seconds (14 days)
    pub fn with_message_retention(mut self, seconds: i32) -> Self {
        self.message_retention_seconds = seconds.clamp(60, 1209600);
        self
    }

    /// Set default delay for messages (default 0)
    ///
    /// Valid range: 0 to 900 seconds (15 minutes)
    pub fn with_delay_seconds(mut self, seconds: i32) -> Self {
        self.delay_seconds = seconds.clamp(0, 900);
        self
    }

    /// Configure CloudWatch metrics publishing
    ///
    /// When enabled, the broker will automatically publish queue metrics to CloudWatch.
    pub fn with_cloudwatch(mut self, config: CloudWatchConfig) -> Self {
        self.cloudwatch_config = Some(config);
        self
    }

    /// Configure adaptive polling strategy
    ///
    /// This allows the broker to adjust its polling behavior based on queue activity,
    /// potentially reducing costs and improving efficiency.
    pub fn with_adaptive_polling(mut self, config: AdaptivePollingConfig) -> Self {
        self.adaptive_polling = Some(config);
        self
    }

    /// Enable automatic queue creation if queue doesn't exist
    ///
    /// When enabled, the broker will automatically create the queue with configured
    /// settings when it doesn't exist, instead of returning an error.
    ///
    /// # Arguments
    /// * `enabled` - Whether to enable auto-creation (default: false)
    ///
    /// # Example
    /// ```ignore
    /// let broker = SqsBroker::new("my-queue")
    ///     .await?
    ///     .with_auto_create_queue(true);
    /// ```
    pub fn with_auto_create_queue(mut self, enabled: bool) -> Self {
        self.auto_create_queue = enabled;
        self
    }

    /// Enable message compression for messages larger than threshold
    ///
    /// Automatically compresses messages using gzip when they exceed the specified
    /// size threshold. This helps handle larger payloads while staying under SQS
    /// message size limits (256 KB).
    ///
    /// # Arguments
    /// * `threshold_bytes` - Minimum message size (in bytes) to trigger compression
    ///
    /// # Example
    /// ```ignore
    /// let broker = SqsBroker::new("my-queue")
    ///     .await?
    ///     .with_compression(10240); // Compress messages > 10KB
    /// ```
    pub fn with_compression(mut self, threshold_bytes: usize) -> Self {
        self.compression_threshold = Some(threshold_bytes);
        self
    }

    /// Configure retry behavior for AWS API calls
    ///
    /// Sets the maximum number of retry attempts and base delay for exponential
    /// backoff when AWS API calls fail.
    ///
    /// # Arguments
    /// * `max_retries` - Maximum total attempts, clamped to `1..=20`
    /// * `base_delay_ms` - Base delay in milliseconds for exponential backoff (default: 100)
    ///
    /// The delay is `base_delay_ms * 2^attempt`, capped at 60 seconds and
    /// jittered; see [`crate::retry_policy`]. Only transient failures are
    /// retried — a client-side validation error fails immediately instead of
    /// burning the whole budget.
    ///
    /// # Example
    /// ```ignore
    /// let broker = SqsBroker::new("my-queue")
    ///     .await?
    ///     .with_retry_config(5, 200); // 5 attempts, 200ms base delay
    /// ```
    pub fn with_retry_config(mut self, max_retries: u32, base_delay_ms: u64) -> Self {
        self.max_retries = max_retries.clamp(1, MAX_RETRY_ATTEMPTS);
        self.retry_base_delay_ms = base_delay_ms;
        self
    }

    /// Enable Celery compatibility mode for Python interoperability
    ///
    /// This enables compatibility with Python Celery workers via the Kombu SQS
    /// transport:
    ///
    /// - Every Celery header is mapped to an SQS MessageAttribute on *all*
    ///   publish paths (single, batch, FIFO, delayed), and read back on both
    ///   consume paths — the round trip is symmetric.
    /// - Every public queue-name argument is a *logical* name, translated with
    ///   the configured
    ///   [`QueueNamingStrategy`](crate::celery_compat::QueueNamingStrategy)
    ///   exactly once, on every path — publish, consume, `queue_size`, `purge`,
    ///   `create_queue`, `delete_queue`, tagging, redrive policies and stats.
    ///   A `.fifo` suffix is preserved across the translation. The only
    ///   exception is [`list_queues`](celers_kombu::Broker::list_queues), which
    ///   reports the names AWS itself holds; use
    ///   [`physical_queue_name`](Self::physical_queue_name) to compare against
    ///   it.
    /// - When `enable_priority_queues` is set, publishes are routed to a
    ///   per-priority queue and `consume` polls those queues highest-priority
    ///   first. [`priority_queues`](Self::priority_queues) lists them.
    ///
    /// # Arguments
    /// * `config` - Celery SQS configuration
    ///
    /// # Example
    /// ```ignore
    /// use celers_broker_sqs::{SqsBroker, celery_compat::CelerySqsConfig};
    ///
    /// let broker = SqsBroker::new("celery")
    ///     .await?
    ///     .with_celery_compat(CelerySqsConfig::kombu_compatible("celery"));
    /// ```
    pub fn with_celery_compat(mut self, config: celery_compat::CelerySqsConfig) -> Self {
        self.celery_mapper = Some(celery_compat::CeleryAttributeMapper::new());
        self.celery_config = Some(config);
        self
    }

    /// Enable default Celery compatibility mode
    ///
    /// Uses Kombu-compatible settings with "celery" as the queue prefix.
    ///
    /// # Example
    /// ```ignore
    /// let broker = SqsBroker::new("tasks")
    ///     .await?
    ///     .with_celery_defaults();
    /// ```
    pub fn with_celery_defaults(self) -> Self {
        self.with_celery_compat(celery_compat::CelerySqsConfig::kombu_compatible("celery"))
    }

    /// Check if Celery compatibility mode is enabled
    #[inline]
    pub fn is_celery_compatible(&self) -> bool {
        self.celery_config.is_some()
    }

    /// Get the Celery configuration if enabled
    #[inline]
    pub fn celery_config(&self) -> Option<&celery_compat::CelerySqsConfig> {
        self.celery_config.as_ref()
    }

    /// Create a broker with production-optimized settings
    ///
    /// - Long polling up to 20s (the caller's poll timeout still applies)
    /// - Prefetching 10 messages per `ReceiveMessage` request
    /// - Visibility timeout: 5 minutes, refreshed by a background heartbeat
    /// - Message retention: 14 days
    ///
    /// # Example
    /// ```ignore
    /// let broker = SqsBroker::production("my-queue").await?;
    /// ```
    pub async fn production(queue_name: &str) -> Result<Self> {
        Self::new(queue_name).await.map(|b| {
            b.with_wait_time(20)
                .with_max_messages(10)
                .with_visibility_timeout(300)
                .with_message_retention(1209600)
                .with_visibility_heartbeat(true)
        })
    }

    /// Create a broker with development-optimized settings
    ///
    /// - Short polling for quick feedback (5s)
    /// - Single message processing
    /// - Short visibility timeout (30s)
    /// - Short retention (1 hour)
    ///
    /// # Example
    /// ```ignore
    /// let broker = SqsBroker::development("my-queue").await?;
    /// ```
    pub async fn development(queue_name: &str) -> Result<Self> {
        Self::new(queue_name).await.map(|b| {
            b.with_wait_time(5)
                .with_max_messages(1)
                .with_visibility_timeout(30)
                .with_message_retention(3600)
        })
    }

    /// Create a broker with cost-optimized settings
    ///
    /// - Maximum long polling (20s)
    /// - Maximum batch size (10 messages)
    /// - Adaptive polling with exponential backoff
    /// - Standard retention (4 days)
    ///
    /// Can reduce costs by up to 90% for high-volume workloads.
    ///
    /// # Example
    /// ```ignore
    /// let broker = SqsBroker::cost_optimized("my-queue").await?;
    /// ```
    pub async fn cost_optimized(queue_name: &str) -> Result<Self> {
        let adaptive_config = AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff)
            .with_min_wait_time(1)
            .with_max_wait_time(20)
            .with_backoff_multiplier(2.0);

        Self::new(queue_name).await.map(|b| {
            b.with_wait_time(20)
                .with_max_messages(10)
                .with_adaptive_polling(adaptive_config)
        })
    }

    /// Validate message size against SQS limit (256 KB)
    ///
    /// Returns the size in bytes if valid, otherwise returns an error.
    ///
    /// # Arguments
    /// * `message` - Message to validate
    ///
    /// # Example
    /// ```ignore
    /// let size = broker.validate_message_size(&message)?;
    /// println!("Message size: {} bytes", size);
    /// ```
    pub fn validate_message_size(&self, message: &celers_protocol::Message) -> Result<usize> {
        let json = serde_json::to_string(message)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;
        let size = json.len();
        const MAX_SIZE: usize = 262_144; // 256 KB in bytes

        if size > MAX_SIZE {
            return Err(BrokerError::OperationFailed(format!(
                "Message size {} bytes exceeds SQS limit of {} bytes (256 KB)",
                size, MAX_SIZE
            )));
        }

        Ok(size)
    }

    /// Calculate the total size of a batch of messages
    ///
    /// Returns the total size in bytes.
    ///
    /// # Arguments
    /// * `messages` - Messages to calculate size for
    pub fn calculate_batch_size(&self, messages: &[celers_protocol::Message]) -> Result<usize> {
        let mut total_size = 0;
        for message in messages {
            total_size += self.validate_message_size(message)?;
        }
        Ok(total_size)
    }

    /// Check if this broker is configured for FIFO queue
    pub fn is_fifo(&self) -> bool {
        self.queue_name.ends_with(".fifo") || self.fifo_config.is_some()
    }

    /// Get or create SQS client (cloned for borrow checker compatibility)
    pub(crate) async fn get_client(&mut self) -> Result<Client> {
        if self.client.is_none() {
            let config = load_aws_config().await;
            self.client = Some(Client::new(&config));
        }

        self.client
            .clone()
            .ok_or_else(|| BrokerError::Connection("SQS client not initialized".to_string()))
    }

    /// Look up a queue URL, distinguishing "missing" from "broken".
    ///
    /// Returns `Ok(None)` **only** when SQS answered `QueueDoesNotExist`. Every
    /// other failure — throttling, expired or missing credentials, an IAM
    /// denial on `sqs:GetQueueUrl`, DNS/TLS trouble, a misconfigured region —
    /// is surfaced as [`BrokerError::Connection`] carrying the real error.
    ///
    /// The previous implementation swallowed the SDK error entirely and told
    /// the operator the queue did not exist, which sent them to fix a
    /// non-problem while the real cause stayed invisible (and, with
    /// `auto_create_queue`, turned every transient throttle into a
    /// `CreateQueue` call).
    pub(crate) async fn try_get_queue_url(&mut self, queue: &str) -> Result<Option<String>> {
        if let Some(url) = self.queue_url_cache.get(queue) {
            return Ok(Some(url.clone()));
        }

        let client = self.get_client().await?;

        match client.get_queue_url().queue_name(queue).send().await {
            Ok(output) => {
                let url = output
                    .queue_url()
                    .ok_or_else(|| {
                        BrokerError::OperationFailed("No queue URL returned".to_string())
                    })?
                    .to_string();

                self.queue_url_cache.insert(queue.to_string(), url.clone());
                self.mark_healthy();
                Ok(Some(url))
            }
            Err(sdk_error) => {
                let missing = sdk_error
                    .as_service_error()
                    .is_some_and(|service| service.is_queue_does_not_exist());

                if missing {
                    // A definitive answer from SQS: the connection is fine.
                    self.mark_healthy();
                    return Ok(None);
                }

                self.mark_unhealthy();
                Err(BrokerError::Connection(format!(
                    "GetQueueUrl for '{}' failed: {}",
                    queue,
                    crate::retry_policy::describe_error(&sdk_error)
                )))
            }
        }
    }

    /// Get or create queue URL with enhanced caching
    ///
    /// This method caches queue URLs for multiple queues and supports
    /// automatic queue creation when enabled.
    ///
    /// # Errors
    ///
    /// * [`BrokerError::QueueNotFound`] when the queue genuinely does not exist
    ///   and auto-creation is disabled.
    /// * [`BrokerError::Connection`] for credential, permission or transport
    ///   failures — these are never mistaken for a missing queue.
    pub(crate) async fn get_queue_url(&mut self, queue: &str) -> Result<String> {
        if let Some(url) = self.try_get_queue_url(queue).await? {
            return Ok(url);
        }

        if !self.auto_create_queue {
            return Err(BrokerError::QueueNotFound(format!(
                "{queue} (call create_queue() first or enable auto_create_queue)"
            )));
        }

        info!("Auto-creating queue: {}", queue);
        let mode = if queue.ends_with(".fifo") {
            QueueMode::Fifo
        } else {
            QueueMode::Priority
        };
        self.create_queue_internal(queue, mode).await?;

        match self.try_get_queue_url(queue).await? {
            Some(url) => Ok(url),
            None => Err(BrokerError::OperationFailed(format!(
                "Queue '{queue}' still does not exist after CreateQueue succeeded"
            ))),
        }
    }

    /// Internal create_queue used by `get_queue_url` for auto-creation.
    ///
    /// Takes a *physical* queue name: `get_queue_url` has already applied the
    /// naming strategy, and applying it twice would produce
    /// `celery_celery_tasks`.
    pub(crate) async fn create_queue_internal(
        &mut self,
        physical_queue: &str,
        mode: QueueMode,
    ) -> Result<()> {
        self.create_queue_physical(physical_queue, mode).await
    }

    /// Get or create CloudWatch client
    pub(crate) async fn get_cloudwatch_client(&mut self) -> Result<CloudWatchClient> {
        if self.cloudwatch_client.is_none() {
            let config = load_aws_config().await;
            self.cloudwatch_client = Some(CloudWatchClient::new(&config));
        }

        self.cloudwatch_client
            .clone()
            .ok_or_else(|| BrokerError::Connection("CloudWatch client not initialized".to_string()))
    }

    /// Publish queue metrics to CloudWatch
    ///
    /// This publishes the current queue statistics to CloudWatch for monitoring.
    ///
    /// # Metrics Published
    /// - `ApproximateNumberOfMessages` - Messages available in the queue
    /// - `ApproximateNumberOfMessagesNotVisible` - Messages being processed
    /// - `ApproximateNumberOfMessagesDelayed` - Delayed messages
    /// - `ApproximateAgeOfOldestMessage` - Age of oldest message in seconds (if queue not empty)
    ///
    /// # Arguments
    /// * `queue` - Queue name to publish metrics for
    pub async fn publish_metrics(&mut self, queue: &str) -> Result<()> {
        let config = match &self.cloudwatch_config {
            Some(c) if c.enabled => c.clone(),
            _ => return Ok(()), // CloudWatch not enabled
        };

        let stats = self.get_queue_stats(queue).await?;
        let cw_client = self.get_cloudwatch_client().await?;

        let mut dimensions = vec![Dimension::builder().name("QueueName").value(queue).build()];

        // Add custom dimensions
        for (name, value) in &config.dimensions {
            dimensions.push(Dimension::builder().name(name).value(value).build());
        }

        let mut metrics = vec![
            MetricDatum::builder()
                .metric_name("ApproximateNumberOfMessages")
                .value(stats.approximate_message_count as f64)
                .unit(StandardUnit::Count)
                .set_dimensions(Some(dimensions.clone()))
                .build(),
            MetricDatum::builder()
                .metric_name("ApproximateNumberOfMessagesNotVisible")
                .value(stats.approximate_not_visible_count as f64)
                .unit(StandardUnit::Count)
                .set_dimensions(Some(dimensions.clone()))
                .build(),
            MetricDatum::builder()
                .metric_name("ApproximateNumberOfMessagesDelayed")
                .value(stats.approximate_delayed_count as f64)
                .unit(StandardUnit::Count)
                .set_dimensions(Some(dimensions.clone()))
                .build(),
        ];

        // Add age of oldest message if available (queue not empty)
        if let Some(age) = stats.approximate_age_of_oldest_message {
            metrics.push(
                MetricDatum::builder()
                    .metric_name("ApproximateAgeOfOldestMessage")
                    .value(age as f64)
                    .unit(StandardUnit::Seconds)
                    .set_dimensions(Some(dimensions.clone()))
                    .build(),
            );
        }

        cw_client
            .put_metric_data()
            .namespace(&config.namespace)
            .set_metric_data(Some(metrics))
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to publish metrics: {}", e))
            })?;

        debug!(
            "Published CloudWatch metrics for queue {} to namespace {}",
            queue, config.namespace
        );
        Ok(())
    }

    /// Create a CloudWatch Alarm for queue monitoring
    ///
    /// This creates an alarm that monitors a specific metric and triggers when
    /// the threshold is breached. Useful for alerting on queue depth, message age,
    /// or other metrics.
    ///
    /// # Arguments
    /// * `config` - Alarm configuration
    ///
    /// # Example
    /// ```ignore
    /// let alarm = AlarmConfig::queue_depth_alarm("HighQueueDepth", "my-queue", 1000.0)
    ///     .with_alarm_action("arn:aws:sns:us-east-1:123456789:alerts");
    /// broker.create_alarm(alarm).await?;
    /// ```
    pub async fn create_alarm(&mut self, config: AlarmConfig) -> Result<()> {
        let cw_client = self.get_cloudwatch_client().await?;

        // Convert dimensions to CloudWatch format
        let mut dimensions = Vec::new();
        for (name, value) in &config.dimensions {
            dimensions.push(Dimension::builder().name(name).value(value).build());
        }

        // Parse comparison operator
        let comparison = match config.comparison_operator.as_str() {
            "GreaterThanThreshold" => ComparisonOperator::GreaterThanThreshold,
            "GreaterThanOrEqualToThreshold" => ComparisonOperator::GreaterThanOrEqualToThreshold,
            "LessThanThreshold" => ComparisonOperator::LessThanThreshold,
            "LessThanOrEqualToThreshold" => ComparisonOperator::LessThanOrEqualToThreshold,
            _ => ComparisonOperator::GreaterThanThreshold,
        };

        // Parse statistic
        let statistic = match config.statistic.as_str() {
            "Average" => Statistic::Average,
            "Sum" => Statistic::Sum,
            "Minimum" => Statistic::Minimum,
            "Maximum" => Statistic::Maximum,
            "SampleCount" => Statistic::SampleCount,
            _ => Statistic::Average,
        };

        let mut alarm_builder = cw_client
            .put_metric_alarm()
            .alarm_name(&config.alarm_name)
            .metric_name(&config.metric_name)
            .namespace(&config.namespace)
            .threshold(config.threshold)
            .comparison_operator(comparison)
            .evaluation_periods(config.evaluation_periods)
            .period(config.period)
            .statistic(statistic)
            .treat_missing_data(&config.treat_missing_data);

        if let Some(desc) = &config.description {
            alarm_builder = alarm_builder.alarm_description(desc);
        }

        if !dimensions.is_empty() {
            alarm_builder = alarm_builder.set_dimensions(Some(dimensions));
        }

        if !config.alarm_actions.is_empty() {
            alarm_builder = alarm_builder.set_alarm_actions(Some(config.alarm_actions.clone()));
        }

        alarm_builder
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to create alarm: {}", e)))?;

        info!("Created CloudWatch alarm: {}", config.alarm_name);
        Ok(())
    }

    /// Delete a CloudWatch Alarm
    ///
    /// # Arguments
    /// * `alarm_name` - Name of the alarm to delete
    pub async fn delete_alarm(&mut self, alarm_name: &str) -> Result<()> {
        let cw_client = self.get_cloudwatch_client().await?;

        cw_client
            .delete_alarms()
            .alarm_names(alarm_name)
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to delete alarm: {}", e)))?;

        info!("Deleted CloudWatch alarm: {}", alarm_name);
        Ok(())
    }

    /// List all CloudWatch Alarms for this queue
    ///
    /// # Arguments
    /// * `queue` - Queue name to list alarms for
    ///
    /// # Returns
    /// Vector of alarm names
    pub async fn list_alarms(&mut self, queue: &str) -> Result<Vec<String>> {
        let cw_client = self.get_cloudwatch_client().await?;

        let result =
            cw_client.describe_alarms().send().await.map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to list alarms: {}", e))
            })?;

        let alarm_names = result
            .metric_alarms()
            .iter()
            .filter_map(|alarm| {
                // Filter alarms that have our queue name in dimensions
                let has_queue = alarm
                    .dimensions()
                    .iter()
                    .any(|d| d.name() == Some("QueueName") && d.value() == Some(queue));
                if has_queue {
                    alarm.alarm_name().map(|s| s.to_string())
                } else {
                    None
                }
            })
            .collect();

        Ok(alarm_names)
    }

    /// Compress message body using gzip
    ///
    /// Returns the compressed data as a base64-encoded string with a compression marker.
    pub(crate) fn compress_message(&self, data: &str) -> Result<String> {
        let compressed = oxiarc_deflate::gzip_compress(data.as_bytes(), 6)
            .map_err(|e| BrokerError::Serialization(format!("Compression failed: {}", e)))?;

        // Add marker prefix to indicate compressed data
        Ok(format!(
            "__GZIP__:{}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &compressed)
        ))
    }

    /// Decompress message body from gzip
    ///
    /// Expects a base64-encoded compressed string with the compression marker.
    pub(crate) fn decompress_message(&self, data: &str) -> Result<String> {
        // Check for compression marker
        if !data.starts_with("__GZIP__:") {
            return Ok(data.to_string()); // Not compressed
        }

        let encoded = &data[9..]; // Skip marker
        let compressed =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
                .map_err(|e| BrokerError::Serialization(format!("Base64 decode failed: {}", e)))?;

        let decompressed_bytes = oxiarc_deflate::gzip_decompress(&compressed)
            .map_err(|e| BrokerError::Serialization(format!("Decompression failed: {}", e)))?;
        let decompressed = String::from_utf8(decompressed_bytes)
            .map_err(|e| BrokerError::Serialization(format!("UTF-8 decode failed: {}", e)))?;

        Ok(decompressed)
    }

    /// Retry an async operation with exponential backoff.
    ///
    /// Only *transient* failures are retried (see
    /// [`is_retryable_error`]); a client-side validation error such as the FIFO
    /// `MissingParameter` returns immediately instead of sleeping through the
    /// whole retry budget.
    ///
    /// Note that `SendMessage` is not idempotent on a standard queue: a request
    /// that times out after AWS accepted it produces a duplicate when retried.
    /// CeleRS therefore documents at-least-once delivery. FIFO publishes carry
    /// a stable `MessageDeduplicationId` derived from the task id, which makes
    /// them idempotent within SQS's 5-minute deduplication interval.
    pub(crate) async fn retry_with_backoff<F, Fut, T>(&self, operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        self.retry_with_backoff_if(operation, is_retryable_error)
            .await
    }

    /// Retry an async operation, deciding retryability with `should_retry`.
    pub(crate) async fn retry_with_backoff_if<F, Fut, T, P>(
        &self,
        mut operation: F,
        should_retry: P,
    ) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
        P: Fn(&BrokerError) -> bool,
    {
        let mut attempt = 0u32;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    attempt += 1;

                    if attempt >= self.max_retries {
                        return Err(error);
                    }

                    if !should_retry(&error) {
                        debug!("Not retrying non-transient failure: {}", error);
                        return Err(error);
                    }

                    let delay_ms = backoff_delay_ms(
                        self.retry_base_delay_ms,
                        attempt,
                        wall_clock_jitter_permille(),
                    );
                    debug!(
                        "Retry attempt {}/{}, waiting {}ms before retry",
                        attempt, self.max_retries, delay_ms
                    );

                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }

    /// Clear the queue URL cache
    ///
    /// This forces the broker to fetch queue URLs from AWS on the next operation.
    /// Useful when queue URLs might have changed or when troubleshooting.
    pub fn clear_queue_url_cache(&mut self) {
        self.queue_url_cache.clear();
        debug!("Cleared queue URL cache");
    }

    // --- Queue name resolution -------------------------------------------

    /// Translate a logical queue name into the physical SQS queue name.
    ///
    /// With Celery compatibility enabled this applies the configured
    /// [`QueueNamingStrategy`](crate::celery_compat::QueueNamingStrategy)
    /// (Kombu prefixes, `.` -> `-`), preserving any `.fifo` suffix; otherwise
    /// the name is returned unchanged.
    ///
    /// Every broker operation takes the *logical* name and performs this
    /// translation itself, so you rarely need this. It is useful for matching
    /// against [`list_queues`](celers_kombu::Broker::list_queues), which
    /// reports the names AWS holds, and for building CloudWatch dimensions.
    ///
    /// The translation is deliberately **not** idempotent (`celery_tasks`
    /// resolves to `celery_celery_tasks`), which is why it is applied at
    /// exactly one boundary per operation — never pass a value returned here
    /// back into another broker method in Celery mode.
    pub fn physical_queue_name(&self, queue: &str) -> String {
        self.resolve_queue_name(queue)
    }

    pub(crate) fn resolve_queue_name(&self, queue: &str) -> String {
        let Some(ref config) = self.celery_config else {
            return queue.to_string();
        };

        // A `.fifo` suffix is load bearing: SQS refuses to treat a queue as
        // FIFO without it, and the Kombu strategy rewrites `.` to `-`. Strip
        // the suffix before applying the strategy and put it back afterwards.
        match queue.strip_suffix(".fifo") {
            Some(stem) => format!("{}.fifo", config.naming_strategy.apply(stem)),
            None => config.naming_strategy.apply(queue),
        }
    }

    /// Physical queue names for a logical queue, highest priority first.
    ///
    /// Empty unless Celery compatibility is enabled with
    /// `enable_priority_queues`. SQS has no message-level priority, so CeleRS
    /// follows Kombu and spreads priorities across sibling queues; `consume`
    /// polls them in the order returned here.
    pub fn priority_queues(&self, queue: &str) -> Vec<String> {
        let Some(ref config) = self.celery_config else {
            return Vec::new();
        };
        if !config.enable_priority_queues {
            return Vec::new();
        }

        if queue.ends_with(".fifo") {
            return Vec::new();
        }

        celery_compat::PriorityQueueManager::new(queue)
            .with_naming_strategy(config.naming_strategy.clone())
            .get_queues_by_priority()
    }

    /// Physical queue a message should be published to.
    pub(crate) fn resolve_publish_queue(
        &self,
        queue: &str,
        message: &celers_protocol::Message,
    ) -> String {
        let Some(ref config) = self.celery_config else {
            return queue.to_string();
        };

        match (config.enable_priority_queues, message.properties.priority) {
            // FIFO queues are never split across priority siblings: the `.fifo`
            // suffix has to stay last and ordering is the point of the queue.
            (true, Some(priority)) if !queue.ends_with(".fifo") => {
                celery_compat::PriorityQueueManager::new(queue)
                    .with_naming_strategy(config.naming_strategy.clone())
                    .get_queue_for_priority(priority)
            }
            _ => self.resolve_queue_name(queue),
        }
    }

    /// Whether a *physical* queue name addresses a FIFO queue.
    ///
    /// SQS requires FIFO queue names to end with `.fifo`, so the suffix is the
    /// authoritative test — a `FifoConfig` on the broker must not turn an
    /// unrelated standard queue into a FIFO publish.
    pub(crate) fn is_fifo_queue(&self, queue: &str) -> bool {
        queue.ends_with(".fifo")
    }

    /// Derive the `MessageGroupId` for a message published to `queue`.
    pub(crate) fn derive_group_id(
        &self,
        queue: &str,
        message: &celers_protocol::Message,
    ) -> String {
        let (source, default) = match self.fifo_config {
            Some(ref config) => (
                config.group_id_source.clone(),
                config.default_message_group_id.clone(),
            ),
            None => (crate::types::FifoGroupIdSource::default(), None),
        };

        crate::fifo::derive_message_group_id(&source, queue, message, default.as_deref())
    }

    // --- Message encoding -------------------------------------------------

    /// Serialize (and optionally compress) a message body.
    pub(crate) fn encode_body(&self, message: &celers_protocol::Message) -> Result<String> {
        let body = serde_json::to_string(message)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;

        if let Some(threshold) = self.compression_threshold {
            if body.len() > threshold {
                let original_size = body.len();
                let compressed = self.compress_message(&body)?;
                debug!(
                    "Compressed message from {} to {} bytes",
                    original_size,
                    compressed.len()
                );
                return Ok(compressed);
            }
        }

        Ok(body)
    }

    /// Build the SQS MessageAttributes for a message.
    ///
    /// This is the single source of truth for every publish path (single,
    /// batch, FIFO, FIFO batch, delayed). Previously only `Producer::publish`
    /// consulted the Celery mapper, so batch and FIFO sends emitted a different
    /// wire format than single sends.
    pub(crate) fn build_attributes(
        &self,
        message: &celers_protocol::Message,
    ) -> Result<HashMap<String, MessageAttributeValue>> {
        if let Some(ref mapper) = self.celery_mapper {
            return mapper
                .serialize_message(message)
                .map_err(|e| BrokerError::OperationFailed(e.to_string()));
        }

        let mut attributes = HashMap::new();

        if let Some(priority) = message.properties.priority {
            attributes.insert(
                "priority".to_string(),
                MessageAttributeValue::builder()
                    .data_type("Number")
                    .string_value(priority.to_string())
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            );
        }

        if let Some(ref correlation_id) = message.properties.correlation_id {
            attributes.insert(
                "correlation_id".to_string(),
                MessageAttributeValue::builder()
                    .data_type("String")
                    .string_value(correlation_id)
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            );
        }

        Ok(attributes)
    }

    // --- Connection health ------------------------------------------------

    pub(crate) fn mark_healthy(&self) {
        self.connection_healthy.store(true, Ordering::Release);
    }

    pub(crate) fn mark_unhealthy(&self) {
        self.connection_healthy.store(false, Ordering::Release);
    }

    /// Record the outcome of an SDK call for [`Transport::is_connected`](celers_kombu::Transport::is_connected).
    pub(crate) fn record_call_result<T>(&self, result: &Result<T>) {
        match result {
            Ok(_) => self.mark_healthy(),
            Err(BrokerError::Connection(_)) => self.mark_unhealthy(),
            Err(_) => {}
        }
    }

    /// Last known transport/credential health.
    ///
    /// Updated by every SDK call the broker makes. `Transport::is_connected`
    /// is synchronous and cannot probe AWS, so it reports this flag AND-ed with
    /// the local client state; use [`health_check`](Self::health_check) for a
    /// real readiness probe.
    pub fn is_connection_healthy(&self) -> bool {
        self.connection_healthy.load(Ordering::Acquire)
    }

    // --- Receipt metadata -------------------------------------------------

    /// Remember the system attributes of an in-flight message.
    pub(crate) fn remember_receipt_metadata(
        &mut self,
        delivery_tag: &str,
        metadata: ReceiptMetadata,
    ) {
        if self.receipt_metadata.len() >= MAX_TRACKED_RECEIPTS {
            while self.receipt_metadata.len() >= MAX_TRACKED_RECEIPTS {
                match self.receipt_metadata_order.pop_front() {
                    Some(oldest) => {
                        self.receipt_metadata.remove(&oldest);
                    }
                    None => break,
                }
            }
        }

        if self
            .receipt_metadata
            .insert(delivery_tag.to_string(), metadata)
            .is_none()
        {
            self.receipt_metadata_order
                .push_back(delivery_tag.to_string());
        }
    }

    /// Drop the metadata for a message that is no longer in flight.
    pub(crate) fn forget_receipt_metadata(&mut self, delivery_tag: &str) {
        self.receipt_metadata.remove(delivery_tag);
    }

    /// System attributes recorded for an in-flight message.
    ///
    /// Populated by `consume`/`consume_batch` from the SQS message system
    /// attributes, and dropped again on `ack`/`reject`.
    pub fn receipt_metadata(&self, delivery_tag: &str) -> Option<&ReceiptMetadata> {
        self.receipt_metadata.get(delivery_tag)
    }

    /// `ApproximateReceiveCount` of an in-flight message (1 on first delivery).
    ///
    /// This is the raw value behind [`Envelope::redelivered`], which poison
    /// detection and DLQ analytics need in order to act on *how often* a
    /// message failed rather than merely whether it is a redelivery.
    pub fn receive_count(&self, delivery_tag: &str) -> Option<u32> {
        self.receipt_metadata
            .get(delivery_tag)
            .map(|metadata| metadata.receive_count)
    }

    // --- Visibility heartbeats -------------------------------------------

    /// Start a visibility heartbeat for one in-flight message.
    ///
    /// Normally driven automatically by `consume`/`consume_batch` when
    /// [`with_visibility_heartbeat`](Self::with_visibility_heartbeat) is
    /// enabled; call it directly to protect a message handled outside those
    /// paths. The heartbeat is stopped by `ack`, `reject`,
    /// [`stop_visibility_heartbeat`](Self::stop_visibility_heartbeat), or by
    /// dropping the broker.
    pub async fn start_visibility_heartbeat(&mut self, delivery_tag: &str) -> Result<()> {
        let (source, handle) = crate::delivery::decode_delivery_tag(delivery_tag);
        let queue = source
            .map(str::to_string)
            .unwrap_or_else(|| self.queue_name.clone());

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(&queue).await?;

        let heartbeat = VisibilityHeartbeat::spawn(
            client,
            queue_url,
            handle.to_string(),
            self.visibility_timeout,
            self.heartbeat_max_extension_secs,
        );

        if let Some(previous) = self.heartbeats.insert(delivery_tag.to_string(), heartbeat) {
            previous.stop();
        }

        Ok(())
    }

    /// Stop the visibility heartbeat for a message, if one is running.
    pub fn stop_visibility_heartbeat(&mut self, delivery_tag: &str) {
        if let Some(heartbeat) = self.heartbeats.remove(delivery_tag) {
            heartbeat.stop();
        }
    }

    /// Stop every running visibility heartbeat.
    pub fn stop_all_visibility_heartbeats(&mut self) {
        for (_, heartbeat) in self.heartbeats.drain() {
            heartbeat.stop();
        }
    }

    /// Number of visibility heartbeats currently running.
    pub fn active_heartbeat_count(&self) -> usize {
        self.heartbeats.len()
    }

    /// Spawn a heartbeat for a freshly received message, if enabled.
    pub(crate) fn maybe_start_heartbeat(
        &mut self,
        client: &Client,
        queue_url: &str,
        delivery_tag: &str,
        receipt_handle: &str,
    ) {
        if !self.visibility_heartbeat_enabled {
            return;
        }

        let heartbeat = VisibilityHeartbeat::spawn(
            client.clone(),
            queue_url.to_string(),
            receipt_handle.to_string(),
            self.visibility_timeout,
            self.heartbeat_max_extension_secs,
        );

        if let Some(previous) = self.heartbeats.insert(delivery_tag.to_string(), heartbeat) {
            previous.stop();
        }
    }

    // --- Prefetch buffer --------------------------------------------------

    /// Take a buffered message for a physical queue, if one is available.
    pub(crate) fn take_prefetched(&mut self, queue: &str) -> Option<Envelope> {
        let buffer = self.prefetch.get_mut(queue)?;
        let envelope = buffer.pop_front();
        if buffer.is_empty() {
            self.prefetch.remove(queue);
        }
        envelope
    }

    /// Buffer surplus messages fetched by a batched `ReceiveMessage`.
    pub(crate) fn buffer_prefetched(&mut self, queue: &str, envelopes: Vec<Envelope>) {
        if envelopes.is_empty() {
            return;
        }

        self.prefetch
            .entry(queue.to_string())
            .or_default()
            .extend(envelopes);
    }

    /// Number of messages currently buffered by prefetching.
    pub fn prefetched_count(&self) -> usize {
        self.prefetch.values().map(VecDeque::len).sum()
    }

    /// Drop every buffered message without acknowledging it.
    ///
    /// The messages become visible again after their visibility timeout.
    pub fn clear_prefetch(&mut self) {
        let dropped = self.prefetched_count();
        if dropped > 0 {
            warn!(
                "Discarding {} prefetched SQS message(s); they will be redelivered",
                dropped
            );
        }
        self.prefetch.clear();
    }
}
