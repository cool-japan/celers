//! Type definitions for the SQS broker
//!
//! Contains configuration structs, enums, and data types used by the SQS broker.

use std::collections::HashMap;

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
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PollingStrategy {
    /// Fixed wait time (no adaptation)
    #[default]
    Fixed,
    /// Exponential backoff when queue is empty
    ExponentialBackoff,
    /// Aggressive polling when messages are available, backoff when empty
    Adaptive,
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
