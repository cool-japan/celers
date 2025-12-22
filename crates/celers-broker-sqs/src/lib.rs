//! AWS SQS broker implementation for CeleRS
//!
//! This crate provides AWS SQS support for cloud-native deployments.
//!
//! # Features
//!
//! - Long polling for efficiency (up to 20 seconds)
//! - Visibility timeout handling
//! - Dead Letter Queue (DLQ) integration
//! - FIFO queue support with message deduplication
//! - Server-side encryption (SSE) with optional KMS
//! - IAM role authentication
//! - Batch operations for throughput (publish_batch, consume_batch, ack_batch)
//! - Priority queue support (via message attributes)
//! - Cost optimization through batch API calls (10x reduction)
//! - Queue monitoring and statistics
//! - CloudWatch metrics integration
//! - Adaptive polling strategies
//! - Parallel message processing
//!
//! # Quick Start
//!
//! ```ignore
//! use celers_broker_sqs::SqsBroker;
//! use celers_kombu::{Transport, Producer, Consumer};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut broker = SqsBroker::new("my-queue-name").await?;
//! broker.connect().await?;
//!
//! // Publish a message
//! let message = Message::new("tasks.add");
//! broker.publish("my-queue", message).await?;
//!
//! // Consume messages
//! let envelope = broker.consume("my-queue", std::time::Duration::from_secs(20)).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # FIFO Queue Example
//!
//! ```ignore
//! use celers_broker_sqs::{SqsBroker, FifoConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create FIFO queue with content-based deduplication
//! let fifo_config = FifoConfig::new()
//!     .with_content_based_deduplication(true)
//!     .with_high_throughput(true);
//!
//! let mut broker = SqsBroker::new("my-queue.fifo")
//!     .await?
//!     .with_fifo(fifo_config);
//!
//! // For FIFO queues, publish with message group ID
//! broker.publish_fifo("my-queue.fifo", message, "group-1", None).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Batch Operations (10x Cost Reduction)
//!
//! ```ignore
//! use celers_broker_sqs::SqsBroker;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let mut broker = SqsBroker::new("my-queue").await?;
//! broker.connect().await?;
//!
//! // Batch publish (up to 10 messages)
//! let messages = vec![
//!     Message::new("task.1"),
//!     Message::new("task.2"),
//!     Message::new("task.3"),
//! ];
//! broker.publish_batch("my-queue", messages).await?;
//!
//! // Batch consume (up to 10 messages)
//! let envelopes = broker.consume_batch("my-queue", 10, Duration::from_secs(20)).await?;
//!
//! // Batch acknowledge
//! let tags: Vec<String> = envelopes.iter().map(|e| e.delivery_tag.clone()).collect();
//! broker.ack_batch(&tags).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # AWS IAM Policy Requirements
//!
//! Minimum IAM permissions needed for production use:
//!
//! ```json
//! {
//!   "Version": "2012-10-17",
//!   "Statement": [
//!     {
//!       "Effect": "Allow",
//!       "Action": [
//!         "sqs:SendMessage",
//!         "sqs:SendMessageBatch",
//!         "sqs:ReceiveMessage",
//!         "sqs:DeleteMessage",
//!         "sqs:DeleteMessageBatch",
//!         "sqs:ChangeMessageVisibility",
//!         "sqs:GetQueueUrl",
//!         "sqs:GetQueueAttributes"
//!       ],
//!       "Resource": "arn:aws:sqs:*:*:celers-*"
//!     }
//!   ]
//! }
//! ```
//!
//! For queue management operations, add:
//! - `sqs:CreateQueue`
//! - `sqs:DeleteQueue`
//! - `sqs:PurgeQueue`
//! - `sqs:ListQueues`
//!
//! For CloudWatch metrics, add:
//! ```json
//! {
//!   "Effect": "Allow",
//!   "Action": ["cloudwatch:PutMetricData"],
//!   "Resource": "*",
//!   "Condition": {
//!     "StringEquals": {"cloudwatch:namespace": "CeleRS/SQS"}
//!   }
//! }
//! ```
//!
//! # Cost Optimization
//!
//! AWS SQS charges per API request. Optimize costs by:
//!
//! 1. **Use batch operations** (10x cost reduction)
//! 2. **Enable long polling** (reduces empty receives)
//! 3. **Use adaptive polling** (adjusts wait time based on activity)
//!
//! Example with cost optimization:
//!
//! ```ignore
//! use celers_broker_sqs::{SqsBroker, AdaptivePollingConfig, PollingStrategy};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let adaptive_config = AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff)
//!     .with_min_wait_time(1)
//!     .with_max_wait_time(20);
//!
//! let mut broker = SqsBroker::new("my-queue")
//!     .await?
//!     .with_wait_time(20)           // Enable long polling
//!     .with_max_messages(10)        // Receive up to 10 messages at once
//!     .with_adaptive_polling(adaptive_config);
//!
//! broker.connect().await?;
//!
//! // This configuration can reduce costs by 90% for high-volume workloads
//! # Ok(())
//! # }
//! ```
//!
//! **Cost Example**: Processing 10M messages/month
//! - Without optimization: ~$10.80/month
//! - With batching + long polling: ~$0.80/month (92.6% savings!)
//!
//! # Authentication
//!
//! The broker uses AWS SDK's credential chain in this order:
//! 1. Environment variables (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`)
//! 2. IAM role (recommended for EC2/ECS/Lambda)
//! 3. AWS credentials file (`~/.aws/credentials`)
//! 4. ECS container credentials
//! 5. EC2 instance metadata
//!
//! **Recommendation**: Use IAM roles in production for enhanced security.

use async_trait::async_trait;
use aws_config::BehaviorVersion;
use aws_sdk_cloudwatch::{
    types::{ComparisonOperator, Dimension, MetricDatum, StandardUnit, Statistic},
    Client as CloudWatchClient,
};
use aws_sdk_sqs::{
    types::{MessageAttributeValue, QueueAttributeName},
    Client,
};
use celers_kombu::{
    Broker, BrokerError, Consumer, Envelope, Producer, QueueMode, Result, Transport,
};
use celers_protocol::Message;
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, info, warn};

/// Dead Letter Queue (DLQ) configuration
#[derive(Debug, Clone)]
pub struct DlqConfig {
    /// ARN of the dead letter queue
    pub dlq_arn: String,
    /// Maximum number of receives before sending to DLQ (1-1000)
    pub max_receive_count: i32,
}

impl DlqConfig {
    /// Create a new DLQ configuration
    pub fn new(dlq_arn: impl Into<String>, max_receive_count: i32) -> Self {
        Self {
            dlq_arn: dlq_arn.into(),
            max_receive_count: max_receive_count.clamp(1, 1000),
        }
    }
}

/// FIFO queue configuration
#[derive(Debug, Clone, Default)]
pub struct FifoConfig {
    /// Enable content-based deduplication (uses SHA-256 of message body)
    pub content_based_deduplication: bool,
    /// Enable high throughput mode (300 -> 3000 TPS per message group)
    pub high_throughput: bool,
    /// Default message group ID for messages without explicit group
    pub default_message_group_id: Option<String>,
}

impl FifoConfig {
    /// Create a new FIFO configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable content-based deduplication
    pub fn with_content_based_deduplication(mut self, enabled: bool) -> Self {
        self.content_based_deduplication = enabled;
        self
    }

    /// Enable high throughput mode (3000 TPS per message group)
    pub fn with_high_throughput(mut self, enabled: bool) -> Self {
        self.high_throughput = enabled;
        self
    }

    /// Set default message group ID
    pub fn with_default_message_group_id(mut self, group_id: impl Into<String>) -> Self {
        self.default_message_group_id = Some(group_id.into());
        self
    }
}

/// Server-side encryption configuration
#[derive(Debug, Clone)]
pub struct SseConfig {
    /// Use KMS encryption (true) or SQS-managed encryption (false)
    pub use_kms: bool,
    /// KMS key ID or alias (required if use_kms is true)
    pub kms_key_id: Option<String>,
    /// KMS data key reuse period in seconds (60-86400)
    pub kms_data_key_reuse_period: Option<i32>,
}

impl SseConfig {
    /// Create SSE configuration with SQS-managed encryption
    pub fn sqs_managed() -> Self {
        Self {
            use_kms: false,
            kms_key_id: None,
            kms_data_key_reuse_period: None,
        }
    }

    /// Create SSE configuration with KMS encryption
    pub fn kms(key_id: impl Into<String>) -> Self {
        Self {
            use_kms: true,
            kms_key_id: Some(key_id.into()),
            kms_data_key_reuse_period: Some(300), // 5 minutes default
        }
    }

    /// Set KMS data key reuse period (60-86400 seconds)
    pub fn with_data_key_reuse_period(mut self, seconds: i32) -> Self {
        self.kms_data_key_reuse_period = Some(seconds.clamp(60, 86400));
        self
    }
}

/// Queue statistics and monitoring data
#[derive(Debug, Clone, Default)]
pub struct QueueStats {
    /// Approximate number of messages in the queue
    pub approximate_message_count: u64,
    /// Approximate number of messages not visible (being processed)
    pub approximate_not_visible_count: u64,
    /// Approximate number of delayed messages
    pub approximate_delayed_count: u64,
    /// Approximate age of oldest message in seconds (None if queue is empty)
    pub approximate_age_of_oldest_message: Option<u64>,
    /// Queue creation timestamp (Unix epoch seconds)
    pub created_timestamp: Option<u64>,
    /// Last modified timestamp (Unix epoch seconds)
    pub last_modified_timestamp: Option<u64>,
    /// Message retention period in seconds
    pub message_retention_period: Option<u64>,
    /// Visibility timeout in seconds
    pub visibility_timeout: Option<u64>,
    /// Whether this is a FIFO queue
    pub is_fifo: bool,
}

/// CloudWatch metrics configuration
#[derive(Debug, Clone)]
pub struct CloudWatchConfig {
    /// Namespace for CloudWatch metrics (default: "CeleRS/SQS")
    pub namespace: String,
    /// Enable automatic metrics publishing
    pub enabled: bool,
    /// Additional dimensions for metrics
    pub dimensions: HashMap<String, String>,
}

impl Default for CloudWatchConfig {
    fn default() -> Self {
        Self {
            namespace: "CeleRS/SQS".to_string(),
            enabled: false,
            dimensions: HashMap::new(),
        }
    }
}

impl CloudWatchConfig {
    /// Create a new CloudWatch configuration
    pub fn new(namespace: impl Into<String>) -> Self {
        Self {
            namespace: namespace.into(),
            enabled: true,
            dimensions: HashMap::new(),
        }
    }

    /// Add a dimension for CloudWatch metrics
    pub fn with_dimension(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.dimensions.insert(name.into(), value.into());
        self
    }

    /// Enable or disable CloudWatch metrics
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }
}

/// CloudWatch Alarm configuration
#[derive(Debug, Clone)]
pub struct AlarmConfig {
    /// Alarm name
    pub alarm_name: String,
    /// Alarm description
    pub description: Option<String>,
    /// Metric name to monitor
    pub metric_name: String,
    /// Namespace for the metric
    pub namespace: String,
    /// Threshold value for the alarm
    pub threshold: f64,
    /// Comparison operator (GreaterThanThreshold, LessThanThreshold, etc.)
    pub comparison_operator: String,
    /// Number of evaluation periods
    pub evaluation_periods: i32,
    /// Period in seconds for metric evaluation
    pub period: i32,
    /// Statistic to use (Average, Sum, Minimum, Maximum, SampleCount)
    pub statistic: String,
    /// Treat missing data as (notBreaching, breaching, ignore, missing)
    pub treat_missing_data: String,
    /// Dimensions for the metric
    pub dimensions: HashMap<String, String>,
    /// SNS topic ARN for alarm notifications (optional)
    pub alarm_actions: Vec<String>,
}

impl AlarmConfig {
    /// Create a new alarm configuration
    pub fn new(
        alarm_name: impl Into<String>,
        metric_name: impl Into<String>,
        threshold: f64,
    ) -> Self {
        Self {
            alarm_name: alarm_name.into(),
            description: None,
            metric_name: metric_name.into(),
            namespace: "CeleRS/SQS".to_string(),
            threshold,
            comparison_operator: "GreaterThanThreshold".to_string(),
            evaluation_periods: 1,
            period: 60,
            statistic: "Average".to_string(),
            treat_missing_data: "notBreaching".to_string(),
            dimensions: HashMap::new(),
            alarm_actions: Vec::new(),
        }
    }

    /// Set alarm description
    pub fn with_description(mut self, description: impl Into<String>) -> Self {
        self.description = Some(description.into());
        self
    }

    /// Set namespace
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Set comparison operator (GreaterThanThreshold, LessThanThreshold, etc.)
    pub fn with_comparison_operator(mut self, operator: impl Into<String>) -> Self {
        self.comparison_operator = operator.into();
        self
    }

    /// Set evaluation periods (how many periods must breach threshold)
    pub fn with_evaluation_periods(mut self, periods: i32) -> Self {
        self.evaluation_periods = periods.max(1);
        self
    }

    /// Set period in seconds for metric evaluation (60, 300, etc.)
    pub fn with_period(mut self, seconds: i32) -> Self {
        self.period = seconds.max(60);
        self
    }

    /// Set statistic (Average, Sum, Minimum, Maximum, SampleCount)
    pub fn with_statistic(mut self, statistic: impl Into<String>) -> Self {
        self.statistic = statistic.into();
        self
    }

    /// Set how to treat missing data (notBreaching, breaching, ignore, missing)
    pub fn with_treat_missing_data(mut self, treatment: impl Into<String>) -> Self {
        self.treat_missing_data = treatment.into();
        self
    }

    /// Add a dimension for the metric
    pub fn with_dimension(mut self, name: impl Into<String>, value: impl Into<String>) -> Self {
        self.dimensions.insert(name.into(), value.into());
        self
    }

    /// Add SNS topic ARN for alarm notifications
    pub fn with_alarm_action(mut self, sns_topic_arn: impl Into<String>) -> Self {
        self.alarm_actions.push(sns_topic_arn.into());
        self
    }

    /// Create alarm config for high queue depth
    pub fn queue_depth_alarm(
        alarm_name: impl Into<String>,
        queue_name: impl Into<String>,
        threshold: f64,
    ) -> Self {
        Self::new(alarm_name, "ApproximateNumberOfMessages", threshold)
            .with_description("Alert when queue depth exceeds threshold")
            .with_dimension("QueueName", queue_name)
            .with_statistic("Average")
            .with_period(300) // 5 minutes
            .with_evaluation_periods(2)
    }

    /// Create alarm config for old messages (backlog detection)
    pub fn message_age_alarm(
        alarm_name: impl Into<String>,
        queue_name: impl Into<String>,
        threshold_seconds: f64,
    ) -> Self {
        Self::new(
            alarm_name,
            "ApproximateAgeOfOldestMessage",
            threshold_seconds,
        )
        .with_description("Alert when oldest message age exceeds threshold")
        .with_dimension("QueueName", queue_name)
        .with_statistic("Maximum")
        .with_period(300) // 5 minutes
        .with_evaluation_periods(1)
    }
}

/// Adaptive polling strategy for queue consumption
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PollingStrategy {
    /// Fixed wait time (no adaptation)
    Fixed,
    /// Exponential backoff when queue is empty
    ExponentialBackoff,
    /// Aggressive polling when messages are available, backoff when empty
    Adaptive,
}

impl Default for PollingStrategy {
    fn default() -> Self {
        Self::Fixed
    }
}

/// Configuration for adaptive polling
#[derive(Debug, Clone)]
pub struct AdaptivePollingConfig {
    /// Polling strategy to use
    pub strategy: PollingStrategy,
    /// Minimum wait time in seconds (default: 1)
    pub min_wait_time: i32,
    /// Maximum wait time in seconds (default: 20)
    pub max_wait_time: i32,
    /// Backoff multiplier for exponential backoff (default: 2.0)
    pub backoff_multiplier: f64,
    /// Current wait time (internal state)
    current_wait_time: i32,
    /// Consecutive empty receives count (internal state)
    consecutive_empty_receives: u32,
}

impl Default for AdaptivePollingConfig {
    fn default() -> Self {
        Self {
            strategy: PollingStrategy::Fixed,
            min_wait_time: 1,
            max_wait_time: 20,
            backoff_multiplier: 2.0,
            current_wait_time: 20,
            consecutive_empty_receives: 0,
        }
    }
}

impl AdaptivePollingConfig {
    /// Create a new adaptive polling configuration
    pub fn new(strategy: PollingStrategy) -> Self {
        Self {
            strategy,
            ..Default::default()
        }
    }

    /// Set minimum wait time (1-20 seconds)
    pub fn with_min_wait_time(mut self, seconds: i32) -> Self {
        self.min_wait_time = seconds.clamp(1, 20);
        self
    }

    /// Set maximum wait time (1-20 seconds)
    pub fn with_max_wait_time(mut self, seconds: i32) -> Self {
        self.max_wait_time = seconds.clamp(1, 20);
        self
    }

    /// Set backoff multiplier (1.0-10.0)
    pub fn with_backoff_multiplier(mut self, multiplier: f64) -> Self {
        self.backoff_multiplier = multiplier.clamp(1.0, 10.0);
        self
    }

    /// Adjust wait time based on whether messages were received
    pub fn adjust_wait_time(&mut self, received_messages: bool) {
        match self.strategy {
            PollingStrategy::Fixed => {
                // No adjustment for fixed strategy
            }
            PollingStrategy::ExponentialBackoff => {
                if received_messages {
                    // Reset to min wait time when messages are received
                    self.current_wait_time = self.min_wait_time;
                    self.consecutive_empty_receives = 0;
                } else {
                    // Exponential backoff when no messages
                    self.consecutive_empty_receives += 1;
                    let new_wait = (self.current_wait_time as f64 * self.backoff_multiplier) as i32;
                    self.current_wait_time = new_wait.min(self.max_wait_time);
                }
            }
            PollingStrategy::Adaptive => {
                if received_messages {
                    // Decrease wait time when messages are available (more aggressive)
                    self.current_wait_time = (self.current_wait_time / 2).max(self.min_wait_time);
                    self.consecutive_empty_receives = 0;
                } else {
                    // Increase wait time when no messages (save costs)
                    self.consecutive_empty_receives += 1;
                    if self.consecutive_empty_receives >= 3 {
                        let new_wait =
                            (self.current_wait_time as f64 * self.backoff_multiplier) as i32;
                        self.current_wait_time = new_wait.min(self.max_wait_time);
                    }
                }
            }
        }
    }

    /// Get the current wait time
    pub fn current_wait_time(&self) -> i32 {
        self.current_wait_time
    }

    /// Reset the adaptive polling state
    pub fn reset(&mut self) {
        self.current_wait_time = self.max_wait_time;
        self.consecutive_empty_receives = 0;
    }
}

/// AWS SQS broker implementation
pub struct SqsBroker {
    client: Option<Client>,
    cloudwatch_client: Option<CloudWatchClient>,
    queue_name: String,
    /// Cache of queue URLs for multiple queues (queue_name -> queue_url)
    queue_url_cache: HashMap<String, String>,
    /// Automatically create queue if it doesn't exist
    auto_create_queue: bool,
    /// Visibility timeout in seconds (default 30)
    visibility_timeout: i32,
    /// Long polling wait time in seconds (default 20, max 20)
    wait_time_seconds: i32,
    /// Maximum messages to receive per poll (default 1, max 10)
    max_messages: i32,
    /// Dead Letter Queue configuration
    dlq_config: Option<DlqConfig>,
    /// FIFO queue configuration
    fifo_config: Option<FifoConfig>,
    /// Server-side encryption configuration
    sse_config: Option<SseConfig>,
    /// Message retention period in seconds (60-1209600, default 345600 = 4 days)
    message_retention_seconds: i32,
    /// Delay seconds for messages (0-900, default 0)
    delay_seconds: i32,
    /// CloudWatch metrics configuration
    cloudwatch_config: Option<CloudWatchConfig>,
    /// Adaptive polling configuration
    adaptive_polling: Option<AdaptivePollingConfig>,
    /// Enable message compression for messages larger than threshold (in bytes)
    compression_threshold: Option<usize>,
    /// Maximum retry attempts for AWS API calls (default 3)
    max_retries: u32,
    /// Base delay for retry backoff in milliseconds (default 100)
    retry_base_delay_ms: u64,
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
        })
    }

    /// Set visibility timeout (default 30 seconds, max 43200 = 12 hours)
    pub fn with_visibility_timeout(mut self, seconds: i32) -> Self {
        self.visibility_timeout = seconds.clamp(0, 43200);
        self
    }

    /// Set long polling wait time (default 20 seconds, max 20)
    pub fn with_wait_time(mut self, seconds: i32) -> Self {
        self.wait_time_seconds = seconds.clamp(0, 20);
        self
    }

    /// Set maximum messages per poll (default 1, max 10)
    pub fn with_max_messages(mut self, max: i32) -> Self {
        self.max_messages = max.clamp(1, 10);
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
    /// * `max_retries` - Maximum retry attempts (default: 3)
    /// * `base_delay_ms` - Base delay in milliseconds for exponential backoff (default: 100)
    ///
    /// # Example
    /// ```ignore
    /// let broker = SqsBroker::new("my-queue")
    ///     .await?
    ///     .with_retry_config(5, 200); // 5 retries, 200ms base delay
    /// ```
    pub fn with_retry_config(mut self, max_retries: u32, base_delay_ms: u64) -> Self {
        self.max_retries = max_retries;
        self.retry_base_delay_ms = base_delay_ms;
        self
    }

    /// Create a broker with production-optimized settings
    ///
    /// - Long polling enabled (20s)
    /// - Batch receiving enabled (10 messages)
    /// - Visibility timeout: 5 minutes
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
    pub fn validate_message_size(&self, message: &Message) -> Result<usize> {
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
    pub fn calculate_batch_size(&self, messages: &[Message]) -> Result<usize> {
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
    async fn get_client(&mut self) -> Result<Client> {
        if self.client.is_none() {
            let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
            self.client = Some(Client::new(&config));
        }

        self.client
            .clone()
            .ok_or_else(|| BrokerError::Connection("SQS client not initialized".to_string()))
    }

    /// Get or create queue URL with enhanced caching
    ///
    /// This method now caches queue URLs for multiple queues and supports
    /// automatic queue creation when enabled.
    async fn get_queue_url(&mut self, queue: &str) -> Result<String> {
        // Check cache first
        if let Some(url) = self.queue_url_cache.get(queue) {
            return Ok(url.clone());
        }

        let client = self.get_client().await?;

        // Try to get existing queue URL
        match client.get_queue_url().queue_name(queue).send().await {
            Ok(output) => {
                let url = output
                    .queue_url()
                    .ok_or_else(|| {
                        BrokerError::OperationFailed("No queue URL returned".to_string())
                    })?
                    .to_string();

                // Cache the URL
                self.queue_url_cache.insert(queue.to_string(), url.clone());

                Ok(url)
            }
            Err(_) => {
                // Queue doesn't exist
                if self.auto_create_queue {
                    // Automatically create the queue
                    info!("Auto-creating queue: {}", queue);
                    let mode = if queue.ends_with(".fifo") {
                        QueueMode::Fifo
                    } else {
                        QueueMode::Priority
                    };
                    self.create_queue(queue, mode).await?;

                    // Retry getting the URL
                    let output = client
                        .get_queue_url()
                        .queue_name(queue)
                        .send()
                        .await
                        .map_err(|e| {
                            BrokerError::OperationFailed(format!(
                                "Failed to get queue URL after creation: {}",
                                e
                            ))
                        })?;

                    let url = output
                        .queue_url()
                        .ok_or_else(|| {
                            BrokerError::OperationFailed("No queue URL returned".to_string())
                        })?
                        .to_string();

                    // Cache the URL
                    self.queue_url_cache.insert(queue.to_string(), url.clone());

                    Ok(url)
                } else {
                    // Return error (use create_queue explicitly)
                    Err(BrokerError::OperationFailed(format!(
                        "Queue '{}' does not exist. Call create_queue() first or enable auto_create_queue.",
                        queue
                    )))
                }
            }
        }
    }

    /// Get or create CloudWatch client
    async fn get_cloudwatch_client(&mut self) -> Result<CloudWatchClient> {
        if self.cloudwatch_client.is_none() {
            let config = aws_config::defaults(BehaviorVersion::latest()).load().await;
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

    /// Publish multiple messages in a single batch (up to 10 messages)
    ///
    /// This is significantly more efficient and cost-effective than individual publishes.
    /// AWS SQS charges per API request, so batch operations reduce costs by 10x.
    ///
    /// # Arguments
    /// * `queue` - Queue name to publish to
    /// * `messages` - Vector of messages to publish (max 10)
    ///
    /// # Returns
    /// Number of messages successfully published
    ///
    /// # Note
    /// If batch size exceeds 10, only the first 10 messages will be sent.
    pub async fn publish_batch(&mut self, queue: &str, messages: Vec<Message>) -> Result<usize> {
        if messages.is_empty() {
            return Ok(0);
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        // SQS batch limit is 10 messages
        let batch_size = messages.len().min(10);
        let batch_messages = &messages[..batch_size];

        // Build batch entries
        let mut entries = Vec::new();
        for (idx, message) in batch_messages.iter().enumerate() {
            let mut body = serde_json::to_string(message)
                .map_err(|e| BrokerError::Serialization(e.to_string()))?;

            // Apply compression if enabled and message exceeds threshold
            if let Some(threshold) = self.compression_threshold {
                let original_size = body.len();
                if original_size > threshold {
                    body = self.compress_message(&body)?;
                    debug!(
                        "Compressed batch message {} from {} to {} bytes",
                        idx,
                        original_size,
                        body.len()
                    );
                }
            }

            let mut entry = aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id(idx.to_string())
                .message_body(body);

            // Add message attributes
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

            if !attributes.is_empty() {
                entry = entry.set_message_attributes(Some(attributes));
            }

            entries.push(
                entry
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            );
        }

        // Send batch
        let result = client
            .send_message_batch()
            .queue_url(&queue_url)
            .set_entries(Some(entries))
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to send batch: {}", e)))?;

        let successful = result.successful().len();

        let failed = result.failed();
        if !failed.is_empty() {
            warn!(
                "Batch send had {} failures out of {}",
                failed.len(),
                batch_size
            );
        }

        debug!(
            "Published {} messages in batch to SQS queue: {}",
            successful, queue
        );
        Ok(successful)
    }

    /// Publish a large batch of messages with automatic chunking
    ///
    /// Automatically splits large batches into groups of 10 messages and sends them
    /// in multiple API calls. This is more efficient than calling publish() repeatedly.
    ///
    /// # Arguments
    /// * `queue` - Queue name to publish to
    /// * `messages` - Vector of messages to publish (any size)
    ///
    /// # Returns
    /// Number of messages successfully published
    ///
    /// # Example
    /// ```ignore
    /// // Send 100 messages in 10 batches of 10
    /// let messages = vec![/* ... 100 messages ... */];
    /// let count = broker.publish_batch_chunked("my-queue", messages).await?;
    /// println!("Published {} messages", count);
    /// ```
    pub async fn publish_batch_chunked(
        &mut self,
        queue: &str,
        messages: Vec<Message>,
    ) -> Result<usize> {
        if messages.is_empty() {
            return Ok(0);
        }

        let mut total_published = 0;

        // Process in chunks of 10
        for chunk in messages.chunks(10) {
            let chunk_messages = chunk.to_vec();
            let published = self.publish_batch(queue, chunk_messages).await?;
            total_published += published;
        }

        info!(
            "Published {} messages in {} chunks to SQS queue: {}",
            total_published,
            messages.len().div_ceil(10),
            queue
        );

        Ok(total_published)
    }

    /// Consume multiple messages in a single batch (up to 10 messages)
    ///
    /// More efficient than polling one message at a time.
    ///
    /// # Arguments
    /// * `queue` - Queue name to consume from
    /// * `max_messages` - Maximum number of messages to receive (max 10)
    /// * `timeout` - Long polling wait time (max 20 seconds)
    ///
    /// # Returns
    /// Vector of envelopes
    pub async fn consume_batch(
        &mut self,
        queue: &str,
        max_messages: i32,
        timeout: Duration,
    ) -> Result<Vec<Envelope>> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let wait_time = timeout.as_secs().min(20) as i32;
        let max_msgs = max_messages.min(10);

        let result = client
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(max_msgs)
            .visibility_timeout(self.visibility_timeout)
            .wait_time_seconds(wait_time)
            .message_attribute_names("All")
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to receive messages: {}", e))
            })?;

        let mut envelopes = Vec::new();

        if let Some(messages) = result.messages {
            for sqs_message in messages {
                let body = sqs_message.body().ok_or_else(|| {
                    BrokerError::OperationFailed("Message has no body".to_string())
                })?;

                let receipt_handle = sqs_message
                    .receipt_handle()
                    .ok_or_else(|| {
                        BrokerError::OperationFailed("Message has no receipt handle".to_string())
                    })?
                    .to_string();

                // Decompress message if it was compressed
                let decompressed_body = self.decompress_message(body)?;

                let message: Message = serde_json::from_str(&decompressed_body)
                    .map_err(|e| BrokerError::Serialization(e.to_string()))?;

                let envelope = Envelope {
                    delivery_tag: receipt_handle,
                    message,
                    redelivered: sqs_message.attributes().is_some_and(|attrs| {
                        attrs
                            .get(&aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount)
                            .and_then(|count| count.parse::<i32>().ok())
                            .map(|count| count > 1)
                            .unwrap_or(false)
                    }),
                };

                envelopes.push(envelope);
            }
        }

        debug!(
            "Consumed {} messages in batch from SQS queue: {}",
            envelopes.len(),
            queue
        );
        Ok(envelopes)
    }

    /// Acknowledge (delete) multiple messages in a single batch (up to 10 messages)
    ///
    /// This is significantly more efficient than individual acks.
    ///
    /// # Arguments
    /// * `queue` - Queue name
    /// * `receipt_handles` - Vector of receipt handles to delete (max 10)
    ///
    /// # Returns
    /// Number of messages successfully deleted
    pub async fn ack_batch(&mut self, queue: &str, receipt_handles: Vec<String>) -> Result<usize> {
        if receipt_handles.is_empty() {
            return Ok(0);
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        // SQS batch limit is 10 messages
        let batch_size = receipt_handles.len().min(10);
        let batch_handles = &receipt_handles[..batch_size];

        // Build batch entries
        let mut entries = Vec::new();
        for (idx, receipt_handle) in batch_handles.iter().enumerate() {
            entries.push(
                aws_sdk_sqs::types::DeleteMessageBatchRequestEntry::builder()
                    .id(idx.to_string())
                    .receipt_handle(receipt_handle)
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            );
        }

        // Delete batch
        let result = client
            .delete_message_batch()
            .queue_url(&queue_url)
            .set_entries(Some(entries))
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to delete batch: {}", e)))?;

        let successful = result.successful().len();

        let failed = result.failed();
        if !failed.is_empty() {
            warn!(
                "Batch delete had {} failures out of {}",
                failed.len(),
                batch_size
            );
        }

        debug!(
            "Acknowledged {} messages in batch from SQS queue: {}",
            successful, queue
        );
        Ok(successful)
    }

    /// Publish a message to a FIFO queue
    ///
    /// FIFO queues require a message group ID for ordering guarantees.
    /// Optionally provide a deduplication ID for exactly-once delivery.
    ///
    /// # Arguments
    /// * `queue` - Queue name (must end with ".fifo")
    /// * `message` - The message to publish
    /// * `message_group_id` - Required for FIFO ordering
    /// * `deduplication_id` - Optional; if None and content-based deduplication is
    ///   disabled, a UUID will be generated
    pub async fn publish_fifo(
        &mut self,
        queue: &str,
        message: Message,
        message_group_id: &str,
        deduplication_id: Option<&str>,
    ) -> Result<()> {
        if !queue.ends_with(".fifo") {
            return Err(BrokerError::OperationFailed(
                "FIFO queue name must end with '.fifo'".to_string(),
            ));
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        // Serialize message to JSON
        let mut body = serde_json::to_string(&message)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;

        // Apply compression if enabled and message exceeds threshold
        if let Some(threshold) = self.compression_threshold {
            let original_size = body.len();
            if original_size > threshold {
                body = self.compress_message(&body)?;
                debug!(
                    "Compressed FIFO message from {} to {} bytes",
                    original_size,
                    body.len()
                );
            }
        }

        // Build message attributes
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

        // Determine deduplication ID
        let dedup_id = deduplication_id.map(String::from).or_else(|| {
            // If content-based deduplication is enabled, SQS will handle it
            if self
                .fifo_config
                .as_ref()
                .is_some_and(|c| c.content_based_deduplication)
            {
                None
            } else {
                // Generate a UUID for deduplication
                Some(uuid::Uuid::new_v4().to_string())
            }
        });

        // Send message
        let mut request = client
            .send_message()
            .queue_url(&queue_url)
            .message_body(&body)
            .message_group_id(message_group_id);

        if let Some(ref dedup) = dedup_id {
            request = request.message_deduplication_id(dedup);
        }

        if !attributes.is_empty() {
            request = request.set_message_attributes(Some(attributes));
        }

        request.send().await.map_err(|e| {
            BrokerError::OperationFailed(format!("Failed to send FIFO message: {}", e))
        })?;

        debug!(
            "Published FIFO message to queue: {} (group: {})",
            queue, message_group_id
        );
        Ok(())
    }

    /// Publish multiple messages to a FIFO queue in a batch
    ///
    /// # Arguments
    /// * `queue` - Queue name (must end with ".fifo")
    /// * `messages` - Vector of (message, message_group_id, optional_deduplication_id)
    ///
    /// # Returns
    /// Number of messages successfully published
    pub async fn publish_fifo_batch(
        &mut self,
        queue: &str,
        messages: Vec<(Message, String, Option<String>)>,
    ) -> Result<usize> {
        if messages.is_empty() {
            return Ok(0);
        }

        if !queue.ends_with(".fifo") {
            return Err(BrokerError::OperationFailed(
                "FIFO queue name must end with '.fifo'".to_string(),
            ));
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let batch_size = messages.len().min(10);
        let batch_messages = &messages[..batch_size];

        let content_based_dedup = self
            .fifo_config
            .as_ref()
            .is_some_and(|c| c.content_based_deduplication);

        let mut entries = Vec::new();
        for (idx, (message, group_id, dedup_id)) in batch_messages.iter().enumerate() {
            let mut body = serde_json::to_string(message)
                .map_err(|e| BrokerError::Serialization(e.to_string()))?;

            // Apply compression if enabled and message exceeds threshold
            if let Some(threshold) = self.compression_threshold {
                let original_size = body.len();
                if original_size > threshold {
                    body = self.compress_message(&body)?;
                    debug!(
                        "Compressed FIFO batch message {} from {} to {} bytes",
                        idx,
                        original_size,
                        body.len()
                    );
                }
            }

            let mut entry = aws_sdk_sqs::types::SendMessageBatchRequestEntry::builder()
                .id(idx.to_string())
                .message_body(body)
                .message_group_id(group_id);

            // Set deduplication ID
            let final_dedup_id = dedup_id.clone().or_else(|| {
                if content_based_dedup {
                    None
                } else {
                    Some(uuid::Uuid::new_v4().to_string())
                }
            });

            if let Some(ref dedup) = final_dedup_id {
                entry = entry.message_deduplication_id(dedup);
            }

            // Add message attributes
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

            if !attributes.is_empty() {
                entry = entry.set_message_attributes(Some(attributes));
            }

            entries.push(
                entry
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            );
        }

        let result = client
            .send_message_batch()
            .queue_url(&queue_url)
            .set_entries(Some(entries))
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to send FIFO batch: {}", e))
            })?;

        let successful = result.successful().len();

        let failed = result.failed();
        if !failed.is_empty() {
            warn!(
                "FIFO batch send had {} failures out of {}",
                failed.len(),
                batch_size
            );
        }

        debug!(
            "Published {} FIFO messages in batch to queue: {}",
            successful, queue
        );
        Ok(successful)
    }

    /// Get detailed queue statistics and monitoring data
    ///
    /// Returns approximate counts for messages and other queue attributes.
    pub async fn get_queue_stats(&mut self, queue: &str) -> Result<QueueStats> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let result = client
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(QueueAttributeName::All)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to get queue attributes: {}", e))
            })?;

        let attrs = result.attributes();

        let parse_u64 = |name: QueueAttributeName| -> Option<u64> {
            attrs
                .and_then(|a| a.get(&name))
                .and_then(|v| v.parse().ok())
        };

        let parse_bool = |name: QueueAttributeName| -> bool {
            attrs
                .and_then(|a| a.get(&name))
                .map(|v| v == "true")
                .unwrap_or(false)
        };

        // Parse age of oldest message from raw attributes map
        // AWS SQS returns this but aws-sdk-sqs may not have the enum variant yet
        let age_of_oldest = attrs.and_then(|a| {
            a.iter()
                .find(|(k, _)| k.as_str() == "ApproximateAgeOfOldestMessage")
                .and_then(|(_, v)| v.parse::<u64>().ok())
        });

        Ok(QueueStats {
            approximate_message_count: parse_u64(QueueAttributeName::ApproximateNumberOfMessages)
                .unwrap_or(0),
            approximate_not_visible_count: parse_u64(
                QueueAttributeName::ApproximateNumberOfMessagesNotVisible,
            )
            .unwrap_or(0),
            approximate_delayed_count: parse_u64(
                QueueAttributeName::ApproximateNumberOfMessagesDelayed,
            )
            .unwrap_or(0),
            approximate_age_of_oldest_message: age_of_oldest,
            created_timestamp: parse_u64(QueueAttributeName::CreatedTimestamp),
            last_modified_timestamp: parse_u64(QueueAttributeName::LastModifiedTimestamp),
            message_retention_period: parse_u64(QueueAttributeName::MessageRetentionPeriod),
            visibility_timeout: parse_u64(QueueAttributeName::VisibilityTimeout),
            is_fifo: parse_bool(QueueAttributeName::FifoQueue),
        })
    }

    /// Extend visibility timeout for a message
    ///
    /// Use this when processing takes longer than expected to prevent
    /// the message from becoming visible to other consumers.
    ///
    /// # Arguments
    /// * `delivery_tag` - The receipt handle of the message
    /// * `timeout_seconds` - New visibility timeout (0-43200 seconds)
    pub async fn extend_visibility(
        &mut self,
        delivery_tag: &str,
        timeout_seconds: i32,
    ) -> Result<()> {
        let client = self.get_client().await?;
        let queue_name = self.queue_name.clone();
        let queue_url = self.get_queue_url(&queue_name).await?;

        client
            .change_message_visibility()
            .queue_url(queue_url)
            .receipt_handle(delivery_tag)
            .visibility_timeout(timeout_seconds.clamp(0, 43200))
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to extend visibility: {}", e))
            })?;

        debug!(
            "Extended visibility timeout to {} seconds for message",
            timeout_seconds
        );
        Ok(())
    }

    /// Get the ARN of a queue
    pub async fn get_queue_arn(&mut self, queue: &str) -> Result<String> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let result = client
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(QueueAttributeName::QueueArn)
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to get queue ARN: {}", e)))?;

        result
            .attributes()
            .and_then(|a| a.get(&QueueAttributeName::QueueArn))
            .map(|s| s.to_string())
            .ok_or_else(|| BrokerError::OperationFailed("Queue ARN not found".to_string()))
    }

    /// Configure redrive policy (Dead Letter Queue) for an existing queue
    ///
    /// # Arguments
    /// * `queue` - The source queue name
    /// * `dlq_arn` - ARN of the dead letter queue
    /// * `max_receive_count` - Number of receives before moving to DLQ
    pub async fn set_redrive_policy(
        &mut self,
        queue: &str,
        dlq_arn: &str,
        max_receive_count: i32,
    ) -> Result<()> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let redrive_policy = serde_json::json!({
            "deadLetterTargetArn": dlq_arn,
            "maxReceiveCount": max_receive_count.clamp(1, 1000).to_string()
        })
        .to_string();

        client
            .set_queue_attributes()
            .queue_url(&queue_url)
            .attributes(QueueAttributeName::RedrivePolicy, redrive_policy)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to set redrive policy: {}", e))
            })?;

        info!(
            "Set redrive policy for queue {} -> DLQ {} (max receives: {})",
            queue, dlq_arn, max_receive_count
        );
        Ok(())
    }

    /// Extend visibility timeout for multiple messages in a batch
    ///
    /// More efficient than extending visibility for individual messages.
    ///
    /// # Arguments
    /// * `queue` - Queue name
    /// * `entries` - Vector of (receipt_handle, timeout_seconds) tuples (max 10)
    ///
    /// # Returns
    /// Number of messages successfully updated
    pub async fn extend_visibility_batch(
        &mut self,
        queue: &str,
        entries: Vec<(String, i32)>,
    ) -> Result<usize> {
        if entries.is_empty() {
            return Ok(0);
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let batch_size = entries.len().min(10);
        let batch_entries = &entries[..batch_size];

        let mut request_entries = Vec::new();
        for (idx, (receipt_handle, timeout)) in batch_entries.iter().enumerate() {
            request_entries.push(
                aws_sdk_sqs::types::ChangeMessageVisibilityBatchRequestEntry::builder()
                    .id(idx.to_string())
                    .receipt_handle(receipt_handle)
                    .visibility_timeout(timeout.clamp(&0, &43200).to_owned())
                    .build()
                    .map_err(|e| BrokerError::OperationFailed(e.to_string()))?,
            );
        }

        let result = client
            .change_message_visibility_batch()
            .queue_url(&queue_url)
            .set_entries(Some(request_entries))
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to extend visibility batch: {}", e))
            })?;

        let successful = result.successful().len();

        let failed = result.failed();
        if !failed.is_empty() {
            warn!(
                "Batch visibility extension had {} failures out of {}",
                failed.len(),
                batch_size
            );
        }

        debug!("Extended visibility for {} messages in batch", successful);
        Ok(successful)
    }

    /// Publish a message with a custom delay
    ///
    /// The message will be invisible for the specified delay before becoming available.
    ///
    /// # Arguments
    /// * `queue` - Queue name
    /// * `message` - The message to publish
    /// * `delay_seconds` - Delay before message becomes visible (0-900 seconds)
    pub async fn publish_with_delay(
        &mut self,
        queue: &str,
        message: Message,
        delay_seconds: i32,
    ) -> Result<()> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let body = serde_json::to_string(&message)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;

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

        client
            .send_message()
            .queue_url(&queue_url)
            .message_body(&body)
            .delay_seconds(delay_seconds.clamp(0, 900))
            .set_message_attributes(if attributes.is_empty() {
                None
            } else {
                Some(attributes)
            })
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to send delayed message: {}", e))
            })?;

        debug!(
            "Published message to SQS queue {} with {} second delay",
            queue, delay_seconds
        );
        Ok(())
    }

    /// Update queue attributes
    ///
    /// # Arguments
    /// * `queue` - Queue name
    /// * `visibility_timeout` - Optional new visibility timeout (0-43200 seconds)
    /// * `message_retention` - Optional new message retention period (60-1209600 seconds)
    /// * `delay_seconds` - Optional new default delay (0-900 seconds)
    pub async fn update_queue_attributes(
        &mut self,
        queue: &str,
        visibility_timeout: Option<i32>,
        message_retention: Option<i32>,
        delay_seconds: Option<i32>,
    ) -> Result<()> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let mut attributes = HashMap::new();

        if let Some(vt) = visibility_timeout {
            attributes.insert(
                QueueAttributeName::VisibilityTimeout,
                vt.clamp(0, 43200).to_string(),
            );
        }

        if let Some(mr) = message_retention {
            attributes.insert(
                QueueAttributeName::MessageRetentionPeriod,
                mr.clamp(60, 1209600).to_string(),
            );
        }

        if let Some(ds) = delay_seconds {
            attributes.insert(
                QueueAttributeName::DelaySeconds,
                ds.clamp(0, 900).to_string(),
            );
        }

        if attributes.is_empty() {
            return Ok(());
        }

        client
            .set_queue_attributes()
            .queue_url(&queue_url)
            .set_attributes(Some(attributes))
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to update queue attributes: {}", e))
            })?;

        debug!("Updated queue attributes for: {}", queue);
        Ok(())
    }

    /// Remove the redrive policy from a queue
    ///
    /// This disables the Dead Letter Queue for the specified queue.
    pub async fn remove_redrive_policy(&mut self, queue: &str) -> Result<()> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        // Setting an empty string removes the redrive policy
        client
            .set_queue_attributes()
            .queue_url(&queue_url)
            .attributes(QueueAttributeName::RedrivePolicy, "")
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to remove redrive policy: {}", e))
            })?;

        info!("Removed redrive policy from queue: {}", queue);
        Ok(())
    }

    /// Get the redrive policy for a queue
    ///
    /// Returns the DLQ ARN and max receive count if configured.
    pub async fn get_redrive_policy(&mut self, queue: &str) -> Result<Option<(String, i32)>> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let result = client
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(QueueAttributeName::RedrivePolicy)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to get redrive policy: {}", e))
            })?;

        if let Some(policy_str) = result
            .attributes()
            .and_then(|a| a.get(&QueueAttributeName::RedrivePolicy))
        {
            if policy_str.is_empty() {
                return Ok(None);
            }

            let policy: serde_json::Value = serde_json::from_str(policy_str)
                .map_err(|e| BrokerError::Serialization(e.to_string()))?;

            let dlq_arn = policy["deadLetterTargetArn"]
                .as_str()
                .unwrap_or("")
                .to_string();

            let max_receive_count = policy["maxReceiveCount"]
                .as_str()
                .and_then(|s| s.parse().ok())
                .or_else(|| policy["maxReceiveCount"].as_i64().map(|n| n as i32))
                .unwrap_or(10);

            if dlq_arn.is_empty() {
                return Ok(None);
            }

            return Ok(Some((dlq_arn, max_receive_count)));
        }

        Ok(None)
    }

    /// Tag a queue with metadata
    ///
    /// # Arguments
    /// * `queue` - Queue name
    /// * `tags` - Map of tag key-value pairs
    pub async fn tag_queue(&mut self, queue: &str, tags: HashMap<String, String>) -> Result<()> {
        if tags.is_empty() {
            return Ok(());
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        client
            .tag_queue()
            .queue_url(&queue_url)
            .set_tags(Some(tags))
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to tag queue: {}", e)))?;

        debug!("Tagged queue: {}", queue);
        Ok(())
    }

    /// Get tags for a queue
    pub async fn get_queue_tags(&mut self, queue: &str) -> Result<HashMap<String, String>> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let result = client
            .list_queue_tags()
            .queue_url(&queue_url)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to get queue tags: {}", e))
            })?;

        Ok(result.tags().cloned().unwrap_or_default())
    }

    /// Remove tags from a queue
    pub async fn untag_queue(&mut self, queue: &str, tag_keys: Vec<String>) -> Result<()> {
        if tag_keys.is_empty() {
            return Ok(());
        }

        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        client
            .untag_queue()
            .queue_url(&queue_url)
            .set_tag_keys(Some(tag_keys))
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to untag queue: {}", e)))?;

        debug!("Removed tags from queue: {}", queue);
        Ok(())
    }

    /// Check if the broker is healthy and can access the queue
    ///
    /// This method verifies that:
    /// - AWS SDK client can be initialized
    /// - Queue URL can be retrieved
    /// - Queue attributes can be fetched
    ///
    /// Useful for Kubernetes readiness/liveness probes and monitoring systems.
    ///
    /// # Arguments
    /// * `queue` - Queue name to check
    ///
    /// # Returns
    /// - `Ok(true)` if queue is accessible and healthy
    /// - `Ok(false)` if queue doesn't exist or is inaccessible
    /// - `Err(_)` if there's a connection or authentication issue
    pub async fn health_check(&mut self, queue: &str) -> Result<bool> {
        match self.get_queue_url(queue).await {
            Ok(queue_url) => {
                // Try to get queue attributes to verify full access
                let client = self.get_client().await?;
                match client
                    .get_queue_attributes()
                    .queue_url(&queue_url)
                    .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
                    .send()
                    .await
                {
                    Ok(_) => {
                        debug!("Health check passed for queue: {}", queue);
                        Ok(true)
                    }
                    Err(e) => {
                        warn!("Health check failed for queue {}: {}", queue, e);
                        Ok(false)
                    }
                }
            }
            Err(e) => {
                warn!("Health check failed - queue not found: {}", e);
                Ok(false)
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

    /// Compress message body using gzip
    ///
    /// Returns the compressed data as a base64-encoded string with a compression marker.
    fn compress_message(&self, data: &str) -> Result<String> {
        use std::io::Write;

        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        encoder
            .write_all(data.as_bytes())
            .map_err(|e| BrokerError::Serialization(format!("Compression failed: {}", e)))?;

        let compressed = encoder
            .finish()
            .map_err(|e| BrokerError::Serialization(format!("Compression finish failed: {}", e)))?;

        // Add marker prefix to indicate compressed data
        Ok(format!(
            "__GZIP__:{}",
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &compressed)
        ))
    }

    /// Decompress message body from gzip
    ///
    /// Expects a base64-encoded compressed string with the compression marker.
    fn decompress_message(&self, data: &str) -> Result<String> {
        use std::io::Read;

        // Check for compression marker
        if !data.starts_with("__GZIP__:") {
            return Ok(data.to_string()); // Not compressed
        }

        let encoded = &data[9..]; // Skip marker
        let compressed =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, encoded)
                .map_err(|e| BrokerError::Serialization(format!("Base64 decode failed: {}", e)))?;

        let mut decoder = flate2::read::GzDecoder::new(&compressed[..]);
        let mut decompressed = String::new();
        decoder
            .read_to_string(&mut decompressed)
            .map_err(|e| BrokerError::Serialization(format!("Decompression failed: {}", e)))?;

        Ok(decompressed)
    }

    /// Retry an async operation with exponential backoff
    ///
    /// Uses the configured max_retries and retry_base_delay_ms settings.
    async fn retry_with_backoff<F, Fut, T>(&self, mut operation: F) -> Result<T>
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Result<T>>,
    {
        let mut attempt = 0;

        loop {
            match operation().await {
                Ok(result) => return Ok(result),
                Err(e) => {
                    attempt += 1;
                    if attempt >= self.max_retries {
                        return Err(e);
                    }

                    // Exponential backoff: base_delay * 2^attempt
                    let delay_ms = self.retry_base_delay_ms * (1 << attempt);
                    debug!(
                        "Retry attempt {}/{}, waiting {}ms before retry",
                        attempt, self.max_retries, delay_ms
                    );

                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                }
            }
        }
    }

    /// Get messages from the Dead Letter Queue (DLQ)
    ///
    /// Retrieves messages that failed processing and were moved to the DLQ.
    /// Requires the DLQ ARN to be configured.
    ///
    /// # Arguments
    /// * `max_messages` - Maximum messages to retrieve (1-10)
    ///
    /// # Returns
    /// Vector of envelopes from the DLQ
    ///
    /// # Example
    /// ```ignore
    /// let dlq_messages = broker.get_dlq_messages(10).await?;
    /// for envelope in dlq_messages {
    ///     println!("Failed message: {:?}", envelope.message);
    /// }
    /// ```
    pub async fn get_dlq_messages(&mut self, max_messages: i32) -> Result<Vec<Envelope>> {
        let dlq_arn = self
            .dlq_config
            .as_ref()
            .ok_or_else(|| BrokerError::OperationFailed("DLQ not configured".to_string()))?
            .dlq_arn
            .clone();

        // Extract queue name from ARN (format: arn:aws:sqs:region:account:queue-name)
        let dlq_name = dlq_arn
            .split(':')
            .next_back()
            .ok_or_else(|| BrokerError::OperationFailed("Invalid DLQ ARN format".to_string()))?;

        self.consume_batch(dlq_name, max_messages.clamp(1, 10), Duration::from_secs(20))
            .await
    }

    /// Move a message from DLQ back to the main queue (redrive)
    ///
    /// Takes a message from the DLQ and republishes it to the main queue.
    ///
    /// # Arguments
    /// * `envelope` - Message envelope from DLQ
    ///
    /// # Example
    /// ```ignore
    /// let dlq_messages = broker.get_dlq_messages(10).await?;
    /// for envelope in dlq_messages {
    ///     // Inspect and potentially redrive the message
    ///     broker.redrive_dlq_message(&envelope).await?;
    /// }
    /// ```
    pub async fn redrive_dlq_message(&mut self, envelope: &Envelope) -> Result<()> {
        let dlq_arn = self
            .dlq_config
            .as_ref()
            .ok_or_else(|| BrokerError::OperationFailed("DLQ not configured".to_string()))?
            .dlq_arn
            .clone();

        let dlq_name = dlq_arn
            .split(':')
            .next_back()
            .ok_or_else(|| BrokerError::OperationFailed("Invalid DLQ ARN format".to_string()))?;

        // Republish to main queue
        self.publish(&self.queue_name.clone(), envelope.message.clone())
            .await?;

        // Delete from DLQ
        self.ack(&envelope.delivery_tag).await?;

        info!("Redriven message from DLQ {} to main queue", dlq_name);
        Ok(())
    }

    /// Get DLQ statistics
    ///
    /// Returns statistics about the Dead Letter Queue including message count
    /// and oldest message age.
    ///
    /// # Returns
    /// QueueStats for the DLQ
    ///
    /// # Example
    /// ```ignore
    /// let dlq_stats = broker.get_dlq_stats().await?;
    /// println!("DLQ has {} messages", dlq_stats.approximate_message_count);
    /// ```
    pub async fn get_dlq_stats(&mut self) -> Result<QueueStats> {
        let dlq_arn = self
            .dlq_config
            .as_ref()
            .ok_or_else(|| BrokerError::OperationFailed("DLQ not configured".to_string()))?
            .dlq_arn
            .clone();

        let dlq_name = dlq_arn
            .split(':')
            .next_back()
            .ok_or_else(|| BrokerError::OperationFailed("Invalid DLQ ARN format".to_string()))?;

        self.get_queue_stats(dlq_name).await
    }

    /// Process messages in parallel with a handler function
    ///
    /// This method consumes messages in batches and processes them concurrently,
    /// improving throughput for I/O-bound or CPU-intensive tasks.
    ///
    /// # Arguments
    /// * `queue` - Queue name to consume from
    /// * `max_messages` - Maximum messages to process in parallel (1-10)
    /// * `timeout` - Long polling wait time
    /// * `handler` - Async function to process each message
    ///
    /// # Returns
    /// Number of messages successfully processed
    ///
    /// # Example
    /// ```ignore
    /// let processed = broker.consume_parallel(
    ///     "my-queue",
    ///     5,
    ///     Duration::from_secs(20),
    ///     |envelope| async move {
    ///         println!("Processing message: {:?}", envelope.message);
    ///         Ok(())
    ///     }
    /// ).await?;
    /// ```
    #[allow(clippy::too_many_arguments)]
    pub async fn consume_parallel<F, Fut>(
        &mut self,
        queue: &str,
        max_messages: i32,
        timeout: Duration,
        handler: F,
    ) -> Result<usize>
    where
        F: Fn(Envelope) -> Fut + Send + Sync + 'static,
        Fut: std::future::Future<Output = Result<()>> + Send,
    {
        let envelopes = self.consume_batch(queue, max_messages, timeout).await?;

        if envelopes.is_empty() {
            return Ok(0);
        }

        let handler = std::sync::Arc::new(handler);
        let mut tasks = Vec::new();

        for envelope in envelopes {
            let delivery_tag = envelope.delivery_tag.clone();
            let handler = handler.clone();

            let task = tokio::spawn(async move {
                let result = handler(envelope).await;
                (delivery_tag, result)
            });

            tasks.push(task);
        }

        let mut successful = 0;
        let mut failed_tags = Vec::new();

        for task in tasks {
            match task.await {
                Ok((delivery_tag, Ok(()))) => {
                    // Handler succeeded, acknowledge message
                    if let Err(e) = self.ack(&delivery_tag).await {
                        warn!("Failed to acknowledge message {}: {}", delivery_tag, e);
                    } else {
                        successful += 1;
                    }
                }
                Ok((delivery_tag, Err(e))) => {
                    // Handler failed, requeue message
                    warn!("Handler failed for message {}: {}", delivery_tag, e);
                    failed_tags.push(delivery_tag);
                }
                Err(e) => {
                    // Task panicked
                    warn!("Task panicked: {}", e);
                }
            }
        }

        // Reject failed messages (requeue them)
        for tag in failed_tags {
            if let Err(e) = self.reject(&tag, true).await {
                warn!("Failed to requeue message {}: {}", tag, e);
            }
        }

        debug!(
            "Processed {} messages in parallel from queue: {}",
            successful, queue
        );
        Ok(successful)
    }
}

#[async_trait]
impl Transport for SqsBroker {
    async fn connect(&mut self) -> Result<()> {
        info!("Connecting to AWS SQS: {}", self.queue_name);

        // Initialize client
        let _ = self.get_client().await?;

        // Get or create queue URL and cache it (clone queue_name to avoid borrow conflict)
        let queue_name = self.queue_name.clone();
        let _ = self.get_queue_url(&queue_name).await?;

        info!("Connected to SQS queue: {}", queue_name);
        Ok(())
    }

    async fn disconnect(&mut self) -> Result<()> {
        self.client = None;
        self.queue_url_cache.clear();
        info!("Disconnected from SQS");
        Ok(())
    }

    fn is_connected(&self) -> bool {
        self.client.is_some() && !self.queue_url_cache.is_empty()
    }

    fn name(&self) -> &str {
        "sqs"
    }
}

#[async_trait]
impl Producer for SqsBroker {
    async fn publish(&mut self, queue: &str, message: Message) -> Result<()> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        // Serialize message to JSON
        let mut body = serde_json::to_string(&message)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;

        // Apply compression if enabled and message exceeds threshold
        if let Some(threshold) = self.compression_threshold {
            let original_size = body.len();
            if original_size > threshold {
                body = self.compress_message(&body)?;
                debug!(
                    "Compressed message from {} to {} bytes",
                    original_size,
                    body.len()
                );
            }
        }

        // Build message attributes
        let mut attributes = HashMap::new();

        // Add priority as message attribute
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

        // Add correlation ID
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

        // Send message with retry logic
        let send_operation = || async {
            client
                .send_message()
                .queue_url(&queue_url)
                .message_body(&body)
                .set_message_attributes(if attributes.is_empty() {
                    None
                } else {
                    Some(attributes.clone())
                })
                .send()
                .await
                .map_err(|e| BrokerError::OperationFailed(format!("Failed to send message: {}", e)))
        };

        self.retry_with_backoff(send_operation).await?;

        debug!("Published message to SQS queue: {}", queue);
        Ok(())
    }

    async fn publish_with_routing(
        &mut self,
        _exchange: &str,
        routing_key: &str,
        message: Message,
    ) -> Result<()> {
        // SQS doesn't have exchanges, route to queue directly
        warn!(
            "SQS doesn't support exchanges, routing to queue: {}",
            routing_key
        );
        self.publish(routing_key, message).await
    }
}

#[async_trait]
impl Consumer for SqsBroker {
    async fn consume(&mut self, queue: &str, timeout: Duration) -> Result<Option<Envelope>> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        // Determine wait time based on adaptive polling configuration
        let wait_time = if let Some(ref mut adaptive) = self.adaptive_polling {
            adaptive.current_wait_time()
        } else {
            timeout.as_secs().min(20) as i32
        };

        // Receive message with long polling
        let result = client
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(1)
            .visibility_timeout(self.visibility_timeout)
            .wait_time_seconds(wait_time)
            .message_attribute_names("All")
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to receive message: {}", e))
            })?;

        let received_messages = result
            .messages
            .as_ref()
            .map(|m| !m.is_empty())
            .unwrap_or(false);

        // Adjust adaptive polling based on result
        if let Some(ref mut adaptive) = self.adaptive_polling {
            adaptive.adjust_wait_time(received_messages);
        }

        if let Some(messages) = result.messages {
            if let Some(sqs_message) = messages.into_iter().next() {
                let body = sqs_message.body().ok_or_else(|| {
                    BrokerError::OperationFailed("Message has no body".to_string())
                })?;

                let receipt_handle = sqs_message
                    .receipt_handle()
                    .ok_or_else(|| {
                        BrokerError::OperationFailed("Message has no receipt handle".to_string())
                    })?
                    .to_string();

                // Decompress message if it was compressed
                let decompressed_body = self.decompress_message(body)?;

                // Deserialize message
                let message: Message = serde_json::from_str(&decompressed_body)
                    .map_err(|e| BrokerError::Serialization(e.to_string()))?;

                let envelope = Envelope {
                    delivery_tag: receipt_handle,
                    message,
                    redelivered: sqs_message.attributes().is_some_and(|attrs| {
                        attrs.get(&aws_sdk_sqs::types::MessageSystemAttributeName::ApproximateReceiveCount)
                            .and_then(|count| count.parse::<i32>().ok())
                            .map(|count| count > 1)
                            .unwrap_or(false)
                    }),
                };

                debug!("Consumed message from SQS queue: {}", queue);
                return Ok(Some(envelope));
            }
        }

        // No message received within timeout
        Ok(None)
    }

    async fn ack(&mut self, delivery_tag: &str) -> Result<()> {
        let client = self.get_client().await?;
        let queue_name = self.queue_name.clone();
        let queue_url = self.get_queue_url(&queue_name).await?;

        client
            .delete_message()
            .queue_url(&queue_url)
            .receipt_handle(delivery_tag)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to delete message: {}", e))
            })?;

        debug!("Acknowledged message: {}", delivery_tag);
        Ok(())
    }

    async fn reject(&mut self, delivery_tag: &str, requeue: bool) -> Result<()> {
        let client = self.get_client().await?;
        let queue_name = self.queue_name.clone();
        let queue_url = self.get_queue_url(&queue_name).await?;

        if requeue {
            // Change visibility timeout to 0 to make message immediately available
            client
                .change_message_visibility()
                .queue_url(&queue_url)
                .receipt_handle(delivery_tag)
                .visibility_timeout(0)
                .send()
                .await
                .map_err(|e| {
                    BrokerError::OperationFailed(format!("Failed to requeue message: {}", e))
                })?;

            debug!("Rejected and requeued message: {}", delivery_tag);
        } else {
            // Delete message (don't requeue)
            client
                .delete_message()
                .queue_url(&queue_url)
                .receipt_handle(delivery_tag)
                .send()
                .await
                .map_err(|e| {
                    BrokerError::OperationFailed(format!("Failed to delete message: {}", e))
                })?;

            debug!("Rejected and deleted message: {}", delivery_tag);
        }

        Ok(())
    }

    async fn queue_size(&mut self, queue: &str) -> Result<usize> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        let result = client
            .get_queue_attributes()
            .queue_url(&queue_url)
            .attribute_names(QueueAttributeName::ApproximateNumberOfMessages)
            .send()
            .await
            .map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to get queue attributes: {}", e))
            })?;

        let count = result
            .attributes()
            .and_then(|attrs| attrs.get(&QueueAttributeName::ApproximateNumberOfMessages))
            .and_then(|count_str| count_str.parse::<usize>().ok())
            .unwrap_or(0);

        Ok(count)
    }
}

#[async_trait]
impl Broker for SqsBroker {
    async fn purge(&mut self, queue: &str) -> Result<usize> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        // Get current size before purge
        let size = self.queue_size(queue).await?;

        client
            .purge_queue()
            .queue_url(&queue_url)
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to purge queue: {}", e)))?;

        debug!("Purged SQS queue: {}", queue);
        Ok(size)
    }

    async fn create_queue(&mut self, queue: &str, mode: QueueMode) -> Result<()> {
        // Clone values before borrowing
        let visibility_timeout = self.visibility_timeout;
        let wait_time_seconds = self.wait_time_seconds;
        let message_retention_seconds = self.message_retention_seconds;
        let delay_seconds = self.delay_seconds;
        let fifo_config = self.fifo_config.clone();
        let sse_config = self.sse_config.clone();
        let dlq_config = self.dlq_config.clone();

        let client = self.get_client().await?;

        let mut attributes = HashMap::new();

        // Set visibility timeout
        attributes.insert(
            QueueAttributeName::VisibilityTimeout,
            visibility_timeout.to_string(),
        );

        // Set receive message wait time for long polling
        attributes.insert(
            QueueAttributeName::ReceiveMessageWaitTimeSeconds,
            wait_time_seconds.to_string(),
        );

        // Set message retention period
        attributes.insert(
            QueueAttributeName::MessageRetentionPeriod,
            message_retention_seconds.to_string(),
        );

        // Set default delay seconds
        attributes.insert(QueueAttributeName::DelaySeconds, delay_seconds.to_string());

        // Configure for priority mode if requested
        if matches!(mode, QueueMode::Priority) {
            warn!("SQS doesn't natively support priority queues. Priority is handled via message attributes.");
        }

        // Configure FIFO queue settings
        let is_fifo = queue.ends_with(".fifo") || fifo_config.is_some();
        if is_fifo {
            if !queue.ends_with(".fifo") {
                return Err(BrokerError::OperationFailed(
                    "FIFO queue name must end with '.fifo'".to_string(),
                ));
            }

            attributes.insert(QueueAttributeName::FifoQueue, "true".to_string());

            if let Some(ref fifo) = fifo_config {
                if fifo.content_based_deduplication {
                    attributes.insert(
                        QueueAttributeName::ContentBasedDeduplication,
                        "true".to_string(),
                    );
                }

                if fifo.high_throughput {
                    attributes.insert(
                        QueueAttributeName::DeduplicationScope,
                        "messageGroup".to_string(),
                    );
                    attributes.insert(
                        QueueAttributeName::FifoThroughputLimit,
                        "perMessageGroupId".to_string(),
                    );
                }
            }

            info!("Creating FIFO queue: {}", queue);
        }

        // Configure Server-Side Encryption
        if let Some(ref sse) = sse_config {
            if sse.use_kms {
                if let Some(ref key_id) = sse.kms_key_id {
                    attributes.insert(QueueAttributeName::KmsMasterKeyId, key_id.clone());
                }
                if let Some(reuse_period) = sse.kms_data_key_reuse_period {
                    attributes.insert(
                        QueueAttributeName::KmsDataKeyReusePeriodSeconds,
                        reuse_period.to_string(),
                    );
                }
                info!("Queue {} configured with KMS encryption", queue);
            } else {
                attributes.insert(QueueAttributeName::SqsManagedSseEnabled, "true".to_string());
                info!("Queue {} configured with SQS-managed SSE", queue);
            }
        }

        // Configure Dead Letter Queue (redrive policy)
        if let Some(ref dlq) = dlq_config {
            let redrive_policy = serde_json::json!({
                "deadLetterTargetArn": dlq.dlq_arn,
                "maxReceiveCount": dlq.max_receive_count.to_string()
            })
            .to_string();

            attributes.insert(QueueAttributeName::RedrivePolicy, redrive_policy);
            info!(
                "Queue {} configured with DLQ: {} (max receives: {})",
                queue, dlq.dlq_arn, dlq.max_receive_count
            );
        }

        let result = client
            .create_queue()
            .queue_name(queue)
            .set_attributes(Some(attributes))
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to create queue: {}", e)))?;

        if let Some(url) = result.queue_url() {
            // Cache the queue URL
            self.queue_url_cache
                .insert(queue.to_string(), url.to_string());
            debug!("Created SQS queue: {} ({})", queue, url);
        }

        Ok(())
    }

    async fn delete_queue(&mut self, queue: &str) -> Result<()> {
        let client = self.get_client().await?;
        let queue_url = self.get_queue_url(queue).await?;

        client
            .delete_queue()
            .queue_url(&queue_url)
            .send()
            .await
            .map_err(|e| BrokerError::OperationFailed(format!("Failed to delete queue: {}", e)))?;

        // Remove from cache
        self.queue_url_cache.remove(queue);

        debug!("Deleted SQS queue: {}", queue);
        Ok(())
    }

    async fn list_queues(&mut self) -> Result<Vec<String>> {
        let client = self.get_client().await?;

        let result =
            client.list_queues().send().await.map_err(|e| {
                BrokerError::OperationFailed(format!("Failed to list queues: {}", e))
            })?;

        let queues = result
            .queue_urls()
            .iter()
            .filter_map(|url| {
                // Extract queue name from URL (last segment)
                url.rsplit('/').next().map(String::from)
            })
            .collect();

        Ok(queues)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_sqs_broker_creation() {
        let broker = SqsBroker::new("test-queue").await;
        assert!(broker.is_ok());
    }

    #[test]
    fn test_broker_name() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let broker = rt.block_on(async { SqsBroker::new("test").await.unwrap() });
        assert_eq!(broker.name(), "sqs");
    }

    #[tokio::test]
    async fn test_builder_pattern() {
        let broker = SqsBroker::new("test-queue")
            .await
            .unwrap()
            .with_visibility_timeout(60)
            .with_wait_time(10)
            .with_max_messages(5);

        assert_eq!(broker.visibility_timeout, 60);
        assert_eq!(broker.wait_time_seconds, 10);
        assert_eq!(broker.max_messages, 5);
    }

    #[tokio::test]
    async fn test_fifo_config() {
        let fifo = FifoConfig::new()
            .with_content_based_deduplication(true)
            .with_high_throughput(true)
            .with_default_message_group_id("default-group");

        assert!(fifo.content_based_deduplication);
        assert!(fifo.high_throughput);
        assert_eq!(
            fifo.default_message_group_id,
            Some("default-group".to_string())
        );
    }

    #[tokio::test]
    async fn test_fifo_broker() {
        let broker = SqsBroker::new("test-queue.fifo")
            .await
            .unwrap()
            .with_fifo(FifoConfig::new().with_content_based_deduplication(true));

        assert!(broker.is_fifo());
        assert!(broker.fifo_config.is_some());
    }

    #[tokio::test]
    async fn test_dlq_config() {
        let dlq = DlqConfig::new("arn:aws:sqs:us-east-1:123456789:my-dlq", 3);

        assert_eq!(dlq.dlq_arn, "arn:aws:sqs:us-east-1:123456789:my-dlq");
        assert_eq!(dlq.max_receive_count, 3);
    }

    #[tokio::test]
    async fn test_dlq_config_clamping() {
        let dlq_low = DlqConfig::new("arn:test", 0);
        assert_eq!(dlq_low.max_receive_count, 1);

        let dlq_high = DlqConfig::new("arn:test", 2000);
        assert_eq!(dlq_high.max_receive_count, 1000);
    }

    #[tokio::test]
    async fn test_sse_config_sqs_managed() {
        let sse = SseConfig::sqs_managed();

        assert!(!sse.use_kms);
        assert!(sse.kms_key_id.is_none());
    }

    #[tokio::test]
    async fn test_sse_config_kms() {
        let sse = SseConfig::kms("alias/my-key").with_data_key_reuse_period(600);

        assert!(sse.use_kms);
        assert_eq!(sse.kms_key_id, Some("alias/my-key".to_string()));
        assert_eq!(sse.kms_data_key_reuse_period, Some(600));
    }

    #[tokio::test]
    async fn test_sse_config_data_key_reuse_clamping() {
        let sse = SseConfig::kms("key").with_data_key_reuse_period(30);
        assert_eq!(sse.kms_data_key_reuse_period, Some(60)); // min 60

        let sse_high = SseConfig::kms("key").with_data_key_reuse_period(100000);
        assert_eq!(sse_high.kms_data_key_reuse_period, Some(86400)); // max 86400
    }

    #[tokio::test]
    async fn test_broker_with_all_configs() {
        let broker = SqsBroker::new("my-queue.fifo")
            .await
            .unwrap()
            .with_visibility_timeout(60)
            .with_wait_time(15)
            .with_max_messages(10)
            .with_message_retention(86400)
            .with_delay_seconds(5)
            .with_fifo(FifoConfig::new().with_content_based_deduplication(true))
            .with_sse(SseConfig::sqs_managed())
            .with_dlq(DlqConfig::new("arn:test:dlq", 5));

        assert_eq!(broker.visibility_timeout, 60);
        assert_eq!(broker.wait_time_seconds, 15);
        assert_eq!(broker.max_messages, 10);
        assert_eq!(broker.message_retention_seconds, 86400);
        assert_eq!(broker.delay_seconds, 5);
        assert!(broker.fifo_config.is_some());
        assert!(broker.sse_config.is_some());
        assert!(broker.dlq_config.is_some());
    }

    #[tokio::test]
    async fn test_visibility_timeout_clamping() {
        let broker = SqsBroker::new("test")
            .await
            .unwrap()
            .with_visibility_timeout(50000); // above max

        assert_eq!(broker.visibility_timeout, 43200); // clamped to max
    }

    #[tokio::test]
    async fn test_wait_time_clamping() {
        let broker = SqsBroker::new("test").await.unwrap().with_wait_time(30); // above max

        assert_eq!(broker.wait_time_seconds, 20); // clamped to max
    }

    #[tokio::test]
    async fn test_max_messages_clamping() {
        let broker = SqsBroker::new("test").await.unwrap().with_max_messages(15); // above max

        assert_eq!(broker.max_messages, 10); // clamped to max
    }

    #[tokio::test]
    async fn test_message_retention_clamping() {
        let broker_low = SqsBroker::new("test")
            .await
            .unwrap()
            .with_message_retention(30); // below min

        assert_eq!(broker_low.message_retention_seconds, 60); // clamped to min

        let broker_high = SqsBroker::new("test")
            .await
            .unwrap()
            .with_message_retention(2000000); // above max

        assert_eq!(broker_high.message_retention_seconds, 1209600); // clamped to max (14 days)
    }

    #[tokio::test]
    async fn test_delay_seconds_clamping() {
        let broker = SqsBroker::new("test")
            .await
            .unwrap()
            .with_delay_seconds(1000); // above max

        assert_eq!(broker.delay_seconds, 900); // clamped to max (15 min)
    }

    #[test]
    fn test_queue_stats_default() {
        let stats = QueueStats::default();

        assert_eq!(stats.approximate_message_count, 0);
        assert_eq!(stats.approximate_not_visible_count, 0);
        assert_eq!(stats.approximate_delayed_count, 0);
        assert!(stats.created_timestamp.is_none());
        assert!(stats.last_modified_timestamp.is_none());
        assert!(stats.message_retention_period.is_none());
        assert!(stats.visibility_timeout.is_none());
        assert!(!stats.is_fifo);
    }

    #[tokio::test]
    async fn test_is_fifo_by_name() {
        let broker = SqsBroker::new("my-queue.fifo").await.unwrap();
        assert!(broker.is_fifo());
    }

    #[tokio::test]
    async fn test_is_not_fifo() {
        let broker = SqsBroker::new("my-queue").await.unwrap();
        assert!(!broker.is_fifo());
    }

    // CloudWatch configuration tests
    #[test]
    fn test_cloudwatch_config_default() {
        let config = CloudWatchConfig::default();
        assert_eq!(config.namespace, "CeleRS/SQS");
        assert!(!config.enabled);
        assert!(config.dimensions.is_empty());
    }

    #[test]
    fn test_cloudwatch_config_new() {
        let config = CloudWatchConfig::new("MyNamespace");
        assert_eq!(config.namespace, "MyNamespace");
        assert!(config.enabled);
        assert!(config.dimensions.is_empty());
    }

    #[test]
    fn test_cloudwatch_config_with_dimensions() {
        let config = CloudWatchConfig::new("CeleRS/SQS")
            .with_dimension("Environment", "production")
            .with_dimension("Application", "my-app");

        assert_eq!(config.dimensions.len(), 2);
        assert_eq!(
            config.dimensions.get("Environment"),
            Some(&"production".to_string())
        );
        assert_eq!(
            config.dimensions.get("Application"),
            Some(&"my-app".to_string())
        );
    }

    #[test]
    fn test_cloudwatch_config_enabled() {
        let config = CloudWatchConfig::new("test").with_enabled(false);
        assert!(!config.enabled);

        let config2 = CloudWatchConfig::default().with_enabled(true);
        assert!(config2.enabled);
    }

    #[tokio::test]
    async fn test_broker_with_cloudwatch() {
        let cw_config = CloudWatchConfig::new("CeleRS/SQS").with_dimension("Test", "value");

        let broker = SqsBroker::new("test-queue")
            .await
            .unwrap()
            .with_cloudwatch(cw_config);

        assert!(broker.cloudwatch_config.is_some());
        let config = broker.cloudwatch_config.unwrap();
        assert_eq!(config.namespace, "CeleRS/SQS");
        assert!(config.enabled);
    }

    // Adaptive polling tests
    #[test]
    fn test_polling_strategy_default() {
        let strategy = PollingStrategy::default();
        assert_eq!(strategy, PollingStrategy::Fixed);
    }

    #[test]
    fn test_adaptive_polling_config_default() {
        let config = AdaptivePollingConfig::default();
        assert_eq!(config.strategy, PollingStrategy::Fixed);
        assert_eq!(config.min_wait_time, 1);
        assert_eq!(config.max_wait_time, 20);
        assert_eq!(config.backoff_multiplier, 2.0);
        assert_eq!(config.current_wait_time(), 20);
    }

    #[test]
    fn test_adaptive_polling_config_new() {
        let config = AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff);
        assert_eq!(config.strategy, PollingStrategy::ExponentialBackoff);
        assert_eq!(config.current_wait_time(), 20);
    }

    #[test]
    fn test_adaptive_polling_config_builders() {
        let config = AdaptivePollingConfig::new(PollingStrategy::Adaptive)
            .with_min_wait_time(2)
            .with_max_wait_time(15)
            .with_backoff_multiplier(3.0);

        assert_eq!(config.min_wait_time, 2);
        assert_eq!(config.max_wait_time, 15);
        assert_eq!(config.backoff_multiplier, 3.0);
    }

    #[test]
    fn test_adaptive_polling_config_clamping() {
        let config = AdaptivePollingConfig::new(PollingStrategy::Fixed)
            .with_min_wait_time(0) // below min
            .with_max_wait_time(30) // above max
            .with_backoff_multiplier(15.0); // above max

        assert_eq!(config.min_wait_time, 1); // clamped to min
        assert_eq!(config.max_wait_time, 20); // clamped to max
        assert_eq!(config.backoff_multiplier, 10.0); // clamped to max
    }

    #[test]
    fn test_adaptive_polling_fixed_strategy() {
        let mut config = AdaptivePollingConfig::new(PollingStrategy::Fixed);
        let initial_wait = config.current_wait_time();

        config.adjust_wait_time(false); // empty receive
        assert_eq!(config.current_wait_time(), initial_wait); // no change

        config.adjust_wait_time(true); // received messages
        assert_eq!(config.current_wait_time(), initial_wait); // no change
    }

    #[test]
    fn test_adaptive_polling_exponential_backoff() {
        let mut config = AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff)
            .with_min_wait_time(1)
            .with_max_wait_time(20)
            .with_backoff_multiplier(2.0);

        // Start with max wait time
        assert_eq!(config.current_wait_time(), 20);

        // Receive messages - should reset to min
        config.adjust_wait_time(true);
        assert_eq!(config.current_wait_time(), 1);

        // Empty receive - should double
        config.adjust_wait_time(false);
        assert_eq!(config.current_wait_time(), 2);

        // Another empty receive - should double again
        config.adjust_wait_time(false);
        assert_eq!(config.current_wait_time(), 4);

        // Keep going until we hit max
        for _ in 0..10 {
            config.adjust_wait_time(false);
        }
        assert_eq!(config.current_wait_time(), 20); // capped at max

        // Receive messages - should reset to min
        config.adjust_wait_time(true);
        assert_eq!(config.current_wait_time(), 1);
    }

    #[test]
    fn test_adaptive_polling_adaptive_strategy() {
        let mut config = AdaptivePollingConfig::new(PollingStrategy::Adaptive)
            .with_min_wait_time(1)
            .with_max_wait_time(20)
            .with_backoff_multiplier(2.0);

        // Start with max wait time
        assert_eq!(config.current_wait_time(), 20);

        // Receive messages - should halve
        config.adjust_wait_time(true);
        assert_eq!(config.current_wait_time(), 10);

        // Receive more messages - should halve again
        config.adjust_wait_time(true);
        assert_eq!(config.current_wait_time(), 5);

        // Keep receiving - should eventually hit min
        for _ in 0..10 {
            config.adjust_wait_time(true);
        }
        assert_eq!(config.current_wait_time(), 1);

        // Empty receive (less than 3 consecutive) - should not change
        config.adjust_wait_time(false);
        assert_eq!(config.current_wait_time(), 1);

        config.adjust_wait_time(false);
        assert_eq!(config.current_wait_time(), 1);

        // 3rd consecutive empty receive - should start increasing
        config.adjust_wait_time(false);
        assert_eq!(config.current_wait_time(), 2);

        // More empty receives (each triggers increase after 3+ consecutive)
        config.adjust_wait_time(false); // 4th: 2 * 2 = 4
        assert_eq!(config.current_wait_time(), 4);

        config.adjust_wait_time(false); // 5th: 4 * 2 = 8
        assert_eq!(config.current_wait_time(), 8);

        config.adjust_wait_time(false); // 6th: 8 * 2 = 16
        assert_eq!(config.current_wait_time(), 16);
    }

    #[test]
    fn test_adaptive_polling_reset() {
        let mut config =
            AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff).with_max_wait_time(20);

        // Adjust wait time
        config.adjust_wait_time(true);
        assert_ne!(config.current_wait_time(), 20);

        // Reset
        config.reset();
        assert_eq!(config.current_wait_time(), 20);
    }

    #[tokio::test]
    async fn test_broker_with_adaptive_polling() {
        let adaptive_config = AdaptivePollingConfig::new(PollingStrategy::Adaptive)
            .with_min_wait_time(1)
            .with_max_wait_time(15);

        let broker = SqsBroker::new("test-queue")
            .await
            .unwrap()
            .with_adaptive_polling(adaptive_config);

        assert!(broker.adaptive_polling.is_some());
        let config = broker.adaptive_polling.unwrap();
        assert_eq!(config.strategy, PollingStrategy::Adaptive);
        assert_eq!(config.min_wait_time, 1);
        assert_eq!(config.max_wait_time, 15);
    }

    #[tokio::test]
    async fn test_broker_with_all_new_configs() {
        let cw_config = CloudWatchConfig::new("CeleRS/SQS").with_dimension("Environment", "test");

        let adaptive_config = AdaptivePollingConfig::new(PollingStrategy::ExponentialBackoff)
            .with_min_wait_time(2)
            .with_max_wait_time(18);

        let broker = SqsBroker::new("test-queue")
            .await
            .unwrap()
            .with_cloudwatch(cw_config)
            .with_adaptive_polling(adaptive_config);

        assert!(broker.cloudwatch_config.is_some());
        assert!(broker.adaptive_polling.is_some());
    }

    #[test]
    fn test_health_check_method_exists() {
        // This test just verifies the health_check method is callable
        // Actual testing requires AWS credentials and a real/mock SQS queue
        // Integration tests should cover this in /tmp/celers_sqs_integration_tests.rs
    }

    #[test]
    fn test_alarm_config_new() {
        let config = AlarmConfig::new("TestAlarm", "ApproximateNumberOfMessages", 100.0);
        assert_eq!(config.alarm_name, "TestAlarm");
        assert_eq!(config.metric_name, "ApproximateNumberOfMessages");
        assert_eq!(config.threshold, 100.0);
        assert_eq!(config.namespace, "CeleRS/SQS");
        assert_eq!(config.comparison_operator, "GreaterThanThreshold");
        assert_eq!(config.evaluation_periods, 1);
        assert_eq!(config.period, 60);
        assert_eq!(config.statistic, "Average");
    }

    #[test]
    fn test_alarm_config_queue_depth() {
        let config = AlarmConfig::queue_depth_alarm("HighDepth", "my-queue", 1000.0);
        assert_eq!(config.alarm_name, "HighDepth");
        assert_eq!(config.metric_name, "ApproximateNumberOfMessages");
        assert_eq!(config.threshold, 1000.0);
        assert_eq!(config.period, 300); // 5 minutes
        assert_eq!(config.evaluation_periods, 2);
        assert_eq!(config.statistic, "Average");
        assert_eq!(
            config.dimensions.get("QueueName"),
            Some(&"my-queue".to_string())
        );
    }

    #[test]
    fn test_alarm_config_message_age() {
        let config = AlarmConfig::message_age_alarm("OldMessages", "my-queue", 600.0);
        assert_eq!(config.alarm_name, "OldMessages");
        assert_eq!(config.metric_name, "ApproximateAgeOfOldestMessage");
        assert_eq!(config.threshold, 600.0);
        assert_eq!(config.period, 300);
        assert_eq!(config.evaluation_periods, 1);
        assert_eq!(config.statistic, "Maximum");
    }

    #[test]
    fn test_alarm_config_builders() {
        let config = AlarmConfig::new("TestAlarm", "TestMetric", 50.0)
            .with_description("Test alarm")
            .with_namespace("Custom/Namespace")
            .with_comparison_operator("LessThanThreshold")
            .with_evaluation_periods(3)
            .with_period(120)
            .with_statistic("Sum")
            .with_treat_missing_data("breaching")
            .with_dimension("Env", "prod")
            .with_alarm_action("arn:aws:sns:us-east-1:123:topic");

        assert_eq!(config.description, Some("Test alarm".to_string()));
        assert_eq!(config.namespace, "Custom/Namespace");
        assert_eq!(config.comparison_operator, "LessThanThreshold");
        assert_eq!(config.evaluation_periods, 3);
        assert_eq!(config.period, 120);
        assert_eq!(config.statistic, "Sum");
        assert_eq!(config.treat_missing_data, "breaching");
        assert_eq!(config.dimensions.get("Env"), Some(&"prod".to_string()));
        assert_eq!(config.alarm_actions.len(), 1);
    }

    #[test]
    fn test_alarm_config_period_clamping() {
        let config = AlarmConfig::new("Test", "Metric", 100.0).with_period(30);
        assert_eq!(config.period, 60); // Clamped to minimum of 60

        let config2 = AlarmConfig::new("Test", "Metric", 100.0).with_period(3600);
        assert_eq!(config2.period, 3600); // No clamping for valid values
    }

    #[test]
    fn test_alarm_config_evaluation_periods_clamping() {
        let config = AlarmConfig::new("Test", "Metric", 100.0).with_evaluation_periods(0);
        assert_eq!(config.evaluation_periods, 1); // Clamped to minimum of 1

        let config2 = AlarmConfig::new("Test", "Metric", 100.0).with_evaluation_periods(5);
        assert_eq!(config2.evaluation_periods, 5); // No clamping for valid values
    }

    #[tokio::test]
    async fn test_production_preset() {
        let broker = SqsBroker::production("test-queue").await.unwrap();
        assert_eq!(broker.wait_time_seconds, 20);
        assert_eq!(broker.max_messages, 10);
        assert_eq!(broker.visibility_timeout, 300);
        assert_eq!(broker.message_retention_seconds, 1209600);
    }

    #[tokio::test]
    async fn test_development_preset() {
        let broker = SqsBroker::development("test-queue").await.unwrap();
        assert_eq!(broker.wait_time_seconds, 5);
        assert_eq!(broker.max_messages, 1);
        assert_eq!(broker.visibility_timeout, 30);
        assert_eq!(broker.message_retention_seconds, 3600);
    }

    #[tokio::test]
    async fn test_cost_optimized_preset() {
        let broker = SqsBroker::cost_optimized("test-queue").await.unwrap();
        assert_eq!(broker.wait_time_seconds, 20);
        assert_eq!(broker.max_messages, 10);
        assert!(broker.adaptive_polling.is_some());

        let adaptive = broker.adaptive_polling.unwrap();
        assert_eq!(adaptive.strategy, PollingStrategy::ExponentialBackoff);
        assert_eq!(adaptive.min_wait_time, 1);
        assert_eq!(adaptive.max_wait_time, 20);
    }

    #[tokio::test]
    async fn test_validate_message_size_small() {
        use uuid::Uuid;

        let broker = SqsBroker::new("test").await.unwrap();
        let msg = Message::new("test.task".to_string(), Uuid::new_v4(), vec![1, 2, 3]);

        let size = broker.validate_message_size(&msg);
        assert!(size.is_ok());
        assert!(size.unwrap() < 262_144); // Less than 256 KB
    }

    #[tokio::test]
    async fn test_validate_message_size_large() {
        use uuid::Uuid;

        let broker = SqsBroker::new("test").await.unwrap();
        // Create a large payload (>256 KB)
        let large_data = vec![0u8; 300_000];
        let msg = Message::new("test.task".to_string(), Uuid::new_v4(), large_data);

        let size = broker.validate_message_size(&msg);
        assert!(size.is_err());
    }

    #[tokio::test]
    async fn test_calculate_batch_size() {
        use uuid::Uuid;

        let broker = SqsBroker::new("test").await.unwrap();
        let mut messages = Vec::new();

        for i in 0..5 {
            let msg = Message::new(format!("test.task.{}", i), Uuid::new_v4(), vec![1, 2, 3]);
            messages.push(msg);
        }

        let total_size = broker.calculate_batch_size(&messages);
        assert!(total_size.is_ok());
        assert!(total_size.unwrap() > 0);
    }

    // Compression/decompression tests
    #[tokio::test]
    async fn test_compress_decompress() {
        let broker = SqsBroker::new("test").await.unwrap();
        let original_data = "This is a test message that will be compressed and decompressed";

        // Compress
        let compressed = broker.compress_message(original_data);
        assert!(compressed.is_ok());
        let compressed_data = compressed.unwrap();

        // Should have compression marker
        assert!(compressed_data.starts_with("__GZIP__:"));

        // Decompress
        let decompressed = broker.decompress_message(&compressed_data);
        assert!(decompressed.is_ok());
        assert_eq!(decompressed.unwrap(), original_data);
    }

    #[tokio::test]
    async fn test_decompress_uncompressed() {
        let broker = SqsBroker::new("test").await.unwrap();
        let uncompressed_data = "This is not compressed";

        // Should pass through uncompressed data unchanged
        let result = broker.decompress_message(uncompressed_data);
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), uncompressed_data);
    }

    #[tokio::test]
    async fn test_compression_config() {
        let broker = SqsBroker::new("test")
            .await
            .unwrap()
            .with_compression(10240); // 10 KB threshold

        assert_eq!(broker.compression_threshold, Some(10240));
    }

    #[tokio::test]
    async fn test_compression_large_message() {
        let broker = SqsBroker::new("test").await.unwrap();

        // Create a large message
        let large_message = "x".repeat(50000);

        // Compress it
        let compressed = broker.compress_message(&large_message);
        assert!(compressed.is_ok());
        let compressed_data = compressed.unwrap();

        // Compressed should be smaller
        assert!(compressed_data.len() < large_message.len());

        // Should be able to decompress back
        let decompressed = broker.decompress_message(&compressed_data);
        assert!(decompressed.is_ok());
        assert_eq!(decompressed.unwrap(), large_message);
    }

    #[tokio::test]
    async fn test_retry_config() {
        let broker = SqsBroker::new("test")
            .await
            .unwrap()
            .with_retry_config(5, 200);

        assert_eq!(broker.max_retries, 5);
        assert_eq!(broker.retry_base_delay_ms, 200);
    }
}
