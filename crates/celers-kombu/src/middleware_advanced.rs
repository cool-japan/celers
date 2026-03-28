//! Advanced middleware implementations.

use async_trait::async_trait;
use celers_protocol::Message;
use std::collections::HashMap;
use std::time::Duration;

use crate::{BrokerError, MessageMiddleware, Priority, Result};

/// Batching middleware for automatic message batching
///
/// Automatically batches messages based on size or time thresholds.
///
/// # Examples
///
/// ```
/// use celers_kombu::{BatchingMiddleware, MessageMiddleware};
///
/// let middleware = BatchingMiddleware::new(100, 5000);
/// assert_eq!(middleware.name(), "batching");
/// ```
#[derive(Debug, Clone)]
pub struct BatchingMiddleware {
    batch_size: usize,
    batch_timeout_ms: u64,
}

impl BatchingMiddleware {
    /// Create a new batching middleware
    ///
    /// # Arguments
    ///
    /// * `batch_size` - Maximum messages per batch
    /// * `batch_timeout_ms` - Maximum wait time in milliseconds
    pub fn new(batch_size: usize, batch_timeout_ms: u64) -> Self {
        Self {
            batch_size,
            batch_timeout_ms,
        }
    }

    /// Create with default settings (100 messages, 5 second timeout)
    pub fn with_defaults() -> Self {
        Self::new(100, 5000)
    }
}

#[async_trait]
impl MessageMiddleware for BatchingMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Add batching metadata
        message.headers.extra.insert(
            "batch-size-hint".to_string(),
            serde_json::json!(self.batch_size),
        );
        message.headers.extra.insert(
            "batch-timeout-ms".to_string(),
            serde_json::json!(self.batch_timeout_ms),
        );

        // Mark message as batch-enabled
        message
            .headers
            .extra
            .insert("batching-enabled".to_string(), serde_json::json!(true));

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No-op for consume side
        Ok(())
    }

    fn name(&self) -> &str {
        "batching"
    }
}

/// Audit middleware for comprehensive audit logging
///
/// Logs all message operations for audit trails and compliance.
///
/// # Examples
///
/// ```
/// use celers_kombu::{AuditMiddleware, MessageMiddleware};
///
/// let middleware = AuditMiddleware::new(true);
/// assert_eq!(middleware.name(), "audit");
/// ```
#[derive(Debug, Clone)]
pub struct AuditMiddleware {
    log_body: bool,
}

impl AuditMiddleware {
    /// Create a new audit middleware
    ///
    /// # Arguments
    ///
    /// * `log_body` - Whether to include message body in audit logs
    pub fn new(log_body: bool) -> Self {
        Self { log_body }
    }

    /// Create audit middleware with body logging enabled
    pub fn with_body_logging() -> Self {
        Self::new(true)
    }

    /// Create audit middleware without body logging
    pub fn without_body_logging() -> Self {
        Self::new(false)
    }

    fn create_audit_entry(&self, message: &Message, operation: &str) -> String {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let body_info = if self.log_body {
            format!("body_size={}", message.body.len())
        } else {
            "body=<redacted>".to_string()
        };

        format!(
            "[AUDIT] timestamp={} operation={} task_id={} task_name={} {}",
            timestamp,
            operation,
            message.task_id(),
            message.task_name(),
            body_info
        )
    }
}

#[async_trait]
impl MessageMiddleware for AuditMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let audit_entry = self.create_audit_entry(message, "PUBLISH");

        // In production, this would be sent to an audit logging system
        message
            .headers
            .extra
            .insert("audit-publish".to_string(), serde_json::json!(audit_entry));

        // Add audit ID
        let audit_id = uuid::Uuid::new_v4().to_string();
        message
            .headers
            .extra
            .insert("audit-id".to_string(), serde_json::json!(audit_id));

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        let audit_entry = self.create_audit_entry(message, "CONSUME");

        // In production, this would be sent to an audit logging system
        message
            .headers
            .extra
            .insert("audit-consume".to_string(), serde_json::json!(audit_entry));

        Ok(())
    }

    fn name(&self) -> &str {
        "audit"
    }
}

/// Middleware for enforcing hard deadlines on message processing.
///
/// Unlike TimeoutMiddleware which sets a timeout hint, DeadlineMiddleware
/// enforces a hard deadline (absolute time) by which a message must be processed.
///
/// # Examples
///
/// ```
/// use celers_kombu::{DeadlineMiddleware, MessageMiddleware};
/// use std::time::Duration;
///
/// // Enforce 5-minute deadline from now
/// let middleware = DeadlineMiddleware::new(Duration::from_secs(300));
/// assert_eq!(middleware.name(), "deadline");
/// ```
#[derive(Debug, Clone)]
pub struct DeadlineMiddleware {
    deadline_duration: Duration,
}

impl DeadlineMiddleware {
    /// Create a new deadline middleware with the specified duration from now
    pub fn new(deadline_duration: Duration) -> Self {
        Self { deadline_duration }
    }

    /// Get the deadline duration
    pub fn deadline_duration(&self) -> Duration {
        self.deadline_duration
    }
}

#[async_trait]
impl MessageMiddleware for DeadlineMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Calculate absolute deadline timestamp
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let deadline = now + self.deadline_duration.as_secs();

        message
            .headers
            .extra
            .insert("x-deadline".to_string(), serde_json::json!(deadline));

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Check if deadline has passed
        if let Some(deadline_value) = message.headers.extra.get("x-deadline") {
            if let Some(deadline) = deadline_value.as_u64() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs();

                if now > deadline {
                    // Mark message as deadline-exceeded
                    message
                        .headers
                        .extra
                        .insert("x-deadline-exceeded".to_string(), serde_json::json!(true));
                }
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "deadline"
    }
}

/// Middleware for content type validation and conversion hints.
///
/// Validates that messages have acceptable content types and can inject
/// conversion hints for consumers.
///
/// # Examples
///
/// ```
/// use celers_kombu::{ContentTypeMiddleware, MessageMiddleware};
///
/// // Only allow JSON messages
/// let middleware = ContentTypeMiddleware::new(vec!["application/json".to_string()]);
/// assert_eq!(middleware.name(), "content_type");
/// ```
#[derive(Debug, Clone)]
pub struct ContentTypeMiddleware {
    allowed_content_types: Vec<String>,
    default_content_type: String,
}

impl ContentTypeMiddleware {
    /// Create a new content type middleware
    pub fn new(allowed_content_types: Vec<String>) -> Self {
        Self {
            allowed_content_types,
            default_content_type: "application/json".to_string(),
        }
    }

    /// Set the default content type for messages without one
    pub fn with_default(mut self, content_type: String) -> Self {
        self.default_content_type = content_type;
        self
    }

    /// Check if a content type is allowed
    pub fn is_allowed(&self, content_type: &str) -> bool {
        self.allowed_content_types.is_empty()
            || self
                .allowed_content_types
                .contains(&content_type.to_string())
    }
}

#[async_trait]
impl MessageMiddleware for ContentTypeMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Set default content type if not present
        if message.content_type.is_empty() {
            message.content_type = self.default_content_type.clone();
        }

        // Validate content type
        if !self.is_allowed(&message.content_type) {
            return Err(BrokerError::Configuration(format!(
                "Content type '{}' is not allowed. Allowed types: {:?}",
                message.content_type, self.allowed_content_types
            )));
        }

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Validate content type on consume
        if !self.is_allowed(&message.content_type) {
            message.headers.extra.insert(
                "x-content-type-warning".to_string(),
                serde_json::json!(format!("Unexpected content type: {}", message.content_type)),
            );
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "content_type"
    }
}

/// Middleware for dynamic routing key assignment.
///
/// Assigns routing keys to messages based on custom logic or message content.
/// Useful for implementing dynamic routing strategies.
///
/// # Examples
///
/// ```
/// use celers_kombu::{RoutingKeyMiddleware, MessageMiddleware};
///
/// // Use task name as routing key
/// let middleware = RoutingKeyMiddleware::new(|msg| {
///     format!("tasks.{}", msg.headers.task)
/// });
/// assert_eq!(middleware.name(), "routing_key");
/// ```
pub struct RoutingKeyMiddleware {
    key_generator: Box<dyn Fn(&Message) -> String + Send + Sync>,
}

impl RoutingKeyMiddleware {
    /// Create a new routing key middleware with a custom key generator
    pub fn new<F>(key_generator: F) -> Self
    where
        F: Fn(&Message) -> String + Send + Sync + 'static,
    {
        Self {
            key_generator: Box::new(key_generator),
        }
    }

    /// Create a routing key from task name
    pub fn from_task_name() -> Self {
        Self::new(|msg| format!("tasks.{}", msg.headers.task))
    }

    /// Create a routing key from task name with priority
    pub fn from_task_and_priority() -> Self {
        Self::new(|msg| {
            let priority = msg
                .headers
                .extra
                .get("priority")
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            format!("tasks.{}.priority_{}", msg.headers.task, priority)
        })
    }
}

#[async_trait]
impl MessageMiddleware for RoutingKeyMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let routing_key = (self.key_generator)(message);
        message
            .headers
            .extra
            .insert("x-routing-key".to_string(), serde_json::json!(routing_key));

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "routing_key"
    }
}

/// Idempotency middleware for ensuring exactly-once message processing
///
/// This middleware tracks processed message IDs to prevent duplicate processing.
/// Unlike DeduplicationMiddleware which only prevents duplicate publishing,
/// IdempotencyMiddleware ensures that a message is processed only once even if
/// it's delivered multiple times (e.g., due to network issues or retries).
///
/// # Examples
///
/// ```
/// use celers_kombu::{IdempotencyMiddleware, MessageMiddleware};
///
/// let middleware = IdempotencyMiddleware::new(10000);
/// assert_eq!(middleware.name(), "idempotency");
/// ```
pub struct IdempotencyMiddleware {
    processed_ids: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<String>>>,
    max_cache_size: usize,
}

impl IdempotencyMiddleware {
    /// Create a new idempotency middleware with a custom cache size
    pub fn new(max_cache_size: usize) -> Self {
        Self {
            processed_ids: std::sync::Arc::new(std::sync::Mutex::new(
                std::collections::HashSet::new(),
            )),
            max_cache_size,
        }
    }

    /// Create a new idempotency middleware with default cache size (10,000)
    pub fn with_default_cache() -> Self {
        Self::new(10000)
    }

    /// Check if a message ID has been processed
    pub fn is_processed(&self, message_id: &str) -> bool {
        self.processed_ids.lock().unwrap().contains(message_id)
    }

    /// Mark a message ID as processed
    pub fn mark_processed(&self, message_id: String) {
        let mut cache = self.processed_ids.lock().unwrap();

        // Simple cache eviction: if we exceed max size, clear oldest 20%
        if cache.len() >= self.max_cache_size {
            let to_remove = self.max_cache_size / 5;
            let ids_to_remove: Vec<String> = cache.iter().take(to_remove).cloned().collect();
            for id in ids_to_remove {
                cache.remove(&id);
            }
        }

        cache.insert(message_id);
    }

    /// Clear all processed message IDs
    pub fn clear(&self) {
        self.processed_ids.lock().unwrap().clear();
    }

    /// Get the number of tracked message IDs
    pub fn cache_size(&self) -> usize {
        self.processed_ids.lock().unwrap().len()
    }
}

#[async_trait]
impl MessageMiddleware for IdempotencyMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Add idempotency key to message headers for tracking
        let idempotency_key = format!("{}:{}", message.headers.id, message.headers.task);
        message.headers.extra.insert(
            "x-idempotency-key".to_string(),
            serde_json::json!(idempotency_key),
        );
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Check if message has already been processed
        let idempotency_key = message
            .headers
            .extra
            .get("x-idempotency-key")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| {
                // Fallback to generating key if not present
                format!("{}:{}", message.headers.id, message.headers.task)
            });

        if self.is_processed(&idempotency_key) {
            // Message already processed, mark it in headers
            message
                .headers
                .extra
                .insert("x-already-processed".to_string(), serde_json::json!(true));
        } else {
            // Mark as being processed
            self.mark_processed(idempotency_key.clone());
            message
                .headers
                .extra
                .insert("x-already-processed".to_string(), serde_json::json!(false));
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "idempotency"
    }
}

/// Backoff middleware for automatic retry backoff calculation
///
/// This middleware automatically calculates and injects retry backoff delays
/// based on the number of retries, using exponential backoff with jitter.
/// This helps prevent thundering herd problems when retrying failed messages.
///
/// # Examples
///
/// ```
/// use celers_kombu::{BackoffMiddleware, MessageMiddleware};
/// use std::time::Duration;
///
/// let middleware = BackoffMiddleware::new(
///     Duration::from_secs(1),
///     Duration::from_secs(300),
///     2.0
/// );
/// assert_eq!(middleware.name(), "backoff");
/// ```
pub struct BackoffMiddleware {
    initial_delay: Duration,
    max_delay: Duration,
    multiplier: f64,
}

impl BackoffMiddleware {
    /// Create a new backoff middleware with custom settings
    ///
    /// # Arguments
    ///
    /// * `initial_delay` - Initial retry delay
    /// * `max_delay` - Maximum retry delay
    /// * `multiplier` - Backoff multiplier (typically 2.0)
    pub fn new(initial_delay: Duration, max_delay: Duration, multiplier: f64) -> Self {
        Self {
            initial_delay,
            max_delay,
            multiplier,
        }
    }

    /// Create a new backoff middleware with default settings
    ///
    /// Defaults: 1s initial, 5min max, 2.0 multiplier
    pub fn with_defaults() -> Self {
        Self::new(Duration::from_secs(1), Duration::from_secs(300), 2.0)
    }

    /// Calculate backoff delay for a given retry attempt
    fn calculate_delay(&self, retry_count: u32) -> Duration {
        let delay_secs =
            self.initial_delay.as_secs_f64() * self.multiplier.powi(retry_count as i32);
        let delay = Duration::from_secs_f64(delay_secs.min(self.max_delay.as_secs_f64()));

        // Add jitter (0-25% of the delay)
        let jitter = (delay.as_secs_f64() * 0.25 * rand::random::<f64>()).round() as u64;
        delay + Duration::from_secs(jitter)
    }
}

#[async_trait]
impl MessageMiddleware for BackoffMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        // No action needed on publish
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Calculate and inject backoff delay based on retry count
        let retry_count = message
            .headers
            .extra
            .get("retries")
            .and_then(|v| v.as_u64())
            .unwrap_or(0) as u32;

        let backoff_delay = self.calculate_delay(retry_count);

        message.headers.extra.insert(
            "x-backoff-delay".to_string(),
            serde_json::json!(backoff_delay.as_millis() as u64),
        );

        message.headers.extra.insert(
            "x-next-retry-at".to_string(),
            serde_json::json!((std::time::SystemTime::now() + backoff_delay)
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs()),
        );

        Ok(())
    }

    fn name(&self) -> &str {
        "backoff"
    }
}

/// Caching middleware for storing message processing results
///
/// This middleware caches the results of message processing to avoid
/// reprocessing identical messages. Useful for expensive operations that
/// are idempotent (e.g., external API calls, database queries).
///
/// # Examples
///
/// ```
/// use celers_kombu::{CachingMiddleware, MessageMiddleware};
/// use std::time::Duration;
///
/// let middleware = CachingMiddleware::new(1000, Duration::from_secs(3600));
/// assert_eq!(middleware.name(), "caching");
/// ```
pub struct CachingMiddleware {
    cache: std::sync::Arc<std::sync::Mutex<CacheMap>>,
    max_entries: usize,
    ttl: Duration,
}

type CacheMap = std::collections::HashMap<String, (Vec<u8>, std::time::Instant)>;

impl CachingMiddleware {
    /// Create a new caching middleware with custom settings
    ///
    /// # Arguments
    ///
    /// * `max_entries` - Maximum number of cache entries
    /// * `ttl` - Time-to-live for cache entries
    pub fn new(max_entries: usize, ttl: Duration) -> Self {
        Self {
            cache: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new())),
            max_entries,
            ttl,
        }
    }

    /// Create a new caching middleware with default settings
    ///
    /// Defaults: 10,000 entries, 1 hour TTL
    pub fn with_defaults() -> Self {
        Self::new(10_000, Duration::from_secs(3600))
    }

    /// Generate cache key from message
    fn cache_key(&self, message: &Message) -> String {
        // Use message ID and task name as cache key
        format!("{}:{}", message.headers.id, message.headers.task)
    }

    /// Check if a cached result exists and is still valid
    pub fn get_cached(&self, message: &Message) -> Option<Vec<u8>> {
        let key = self.cache_key(message);
        let mut cache = self.cache.lock().unwrap();

        if let Some((result, timestamp)) = cache.get(&key) {
            if timestamp.elapsed() < self.ttl {
                return Some(result.clone());
            }
            // Remove expired entry
            cache.remove(&key);
        }
        None
    }

    /// Store a result in the cache
    pub fn store_result(&self, message: &Message, result: Vec<u8>) {
        let key = self.cache_key(message);
        let mut cache = self.cache.lock().unwrap();

        // Evict oldest entries if cache is full
        if cache.len() >= self.max_entries {
            let to_remove = cache.len() / 5; // Remove oldest 20%
            let mut entries: Vec<_> = cache.iter().map(|(k, v)| (k.clone(), v.1)).collect();
            entries.sort_by_key(|(_, timestamp)| *timestamp);

            for (key, _) in entries.iter().take(to_remove) {
                cache.remove(key);
            }
        }

        cache.insert(key, (result, std::time::Instant::now()));
    }

    /// Clear all cached results
    pub fn clear(&self) {
        self.cache.lock().unwrap().clear();
    }

    /// Get the number of cached entries
    pub fn cache_size(&self) -> usize {
        self.cache.lock().unwrap().len()
    }
}

#[async_trait]
impl MessageMiddleware for CachingMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        // No action needed on publish
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Check if result is cached
        if let Some(cached_result) = self.get_cached(message) {
            message
                .headers
                .extra
                .insert("x-cache-hit".to_string(), serde_json::json!(true));
            message.headers.extra.insert(
                "x-cached-result-size".to_string(),
                serde_json::json!(cached_result.len()),
            );
        } else {
            message
                .headers
                .extra
                .insert("x-cache-hit".to_string(), serde_json::json!(false));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "caching"
    }
}

/// Bulkhead middleware for limiting concurrent operations per partition
///
/// This middleware implements the bulkhead pattern to prevent resource exhaustion
/// by limiting the number of concurrent operations per partition/queue.
///
/// # Examples
///
/// ```
/// use celers_kombu::BulkheadMiddleware;
///
/// // Create bulkhead with max 50 concurrent operations per partition
/// let bulkhead = BulkheadMiddleware::new(50);
///
/// // Create with custom partition key extractor
/// let bulkhead = BulkheadMiddleware::with_partition_fn(50, |msg| {
///     msg.headers.extra.get("partition_key")
///         .and_then(|v| v.as_str())
///         .map(|s| s.to_string())
///         .unwrap_or_else(|| "default".to_string())
/// });
/// ```
#[derive(Clone)]
pub struct BulkheadMiddleware {
    max_concurrent: usize,
    permits: std::sync::Arc<std::sync::Mutex<HashMap<String, usize>>>,
    partition_fn: std::sync::Arc<dyn Fn(&Message) -> String + Send + Sync>,
}

impl BulkheadMiddleware {
    /// Create a new bulkhead middleware with max concurrent operations
    ///
    /// # Arguments
    ///
    /// * `max_concurrent` - Maximum number of concurrent operations per partition
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            max_concurrent,
            permits: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
            partition_fn: std::sync::Arc::new(|msg| {
                // Default: partition by task name
                msg.headers.task.clone()
            }),
        }
    }

    /// Create with custom partition key extraction function
    pub fn with_partition_fn<F>(max_concurrent: usize, partition_fn: F) -> Self
    where
        F: Fn(&Message) -> String + Send + Sync + 'static,
    {
        Self {
            max_concurrent,
            permits: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
            partition_fn: std::sync::Arc::new(partition_fn),
        }
    }

    /// Try to acquire a permit for the given partition
    pub fn try_acquire(&self, partition: &str) -> bool {
        let mut permits = self.permits.lock().unwrap();
        let current = permits.entry(partition.to_string()).or_insert(0);
        if *current < self.max_concurrent {
            *current += 1;
            true
        } else {
            false
        }
    }

    /// Release a permit for the given partition
    pub fn release(&self, partition: &str) {
        let mut permits = self.permits.lock().unwrap();
        if let Some(current) = permits.get_mut(partition) {
            if *current > 0 {
                *current -= 1;
            }
        }
    }

    /// Get current concurrent operations for a partition
    pub fn current_operations(&self, partition: &str) -> usize {
        self.permits
            .lock()
            .unwrap()
            .get(partition)
            .copied()
            .unwrap_or(0)
    }

    /// Get total concurrent operations across all partitions
    pub fn total_operations(&self) -> usize {
        self.permits.lock().unwrap().values().sum()
    }
}

#[async_trait]
impl MessageMiddleware for BulkheadMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let partition = (self.partition_fn)(message);
        if !self.try_acquire(&partition) {
            message
                .headers
                .extra
                .insert("x-bulkhead-rejected".to_string(), serde_json::json!(true));
            message.headers.extra.insert(
                "x-bulkhead-partition".to_string(),
                serde_json::json!(partition),
            );
            message.headers.extra.insert(
                "x-bulkhead-current".to_string(),
                serde_json::json!(self.max_concurrent),
            );
        } else {
            message.headers.extra.insert(
                "x-bulkhead-partition".to_string(),
                serde_json::json!(partition),
            );
        }
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        let partition = (self.partition_fn)(message);
        self.release(&partition);
        Ok(())
    }

    fn name(&self) -> &str {
        "bulkhead"
    }
}

/// Priority boost middleware for dynamic priority adjustment
///
/// This middleware dynamically adjusts message priority based on configurable rules
/// such as message age, retry count, or custom criteria.
///
/// # Examples
///
/// ```
/// use celers_kombu::{PriorityBoostMiddleware, Priority};
/// use std::time::Duration;
///
/// // Boost priority for messages older than 5 minutes
/// let boost = PriorityBoostMiddleware::new()
///     .with_age_boost(Duration::from_secs(300), Priority::High);
///
/// // Custom boost function
/// let boost = PriorityBoostMiddleware::with_custom_fn(|msg, current_priority| {
///     if msg.headers.retries.unwrap_or(0) > 3 {
///         Priority::Highest
///     } else {
///         current_priority
///     }
/// });
/// ```
pub type PriorityBoostFn = std::sync::Arc<dyn Fn(&Message, Priority) -> Priority + Send + Sync>;

#[derive(Clone)]
pub struct PriorityBoostMiddleware {
    age_threshold: Option<Duration>,
    age_boost_priority: Priority,
    retry_threshold: Option<u32>,
    retry_boost_priority: Priority,
    custom_fn: Option<PriorityBoostFn>,
}

impl PriorityBoostMiddleware {
    /// Create a new priority boost middleware with defaults
    pub fn new() -> Self {
        Self {
            age_threshold: None,
            age_boost_priority: Priority::High,
            retry_threshold: None,
            retry_boost_priority: Priority::High,
            custom_fn: None,
        }
    }

    /// Boost priority for messages older than the specified duration
    pub fn with_age_boost(mut self, threshold: Duration, priority: Priority) -> Self {
        self.age_threshold = Some(threshold);
        self.age_boost_priority = priority;
        self
    }

    /// Boost priority for messages with retry count exceeding threshold
    pub fn with_retry_boost(mut self, threshold: u32, priority: Priority) -> Self {
        self.retry_threshold = Some(threshold);
        self.retry_boost_priority = priority;
        self
    }

    /// Create with custom priority boost function
    pub fn with_custom_fn<F>(custom_fn: F) -> Self
    where
        F: Fn(&Message, Priority) -> Priority + Send + Sync + 'static,
    {
        Self {
            age_threshold: None,
            age_boost_priority: Priority::High,
            retry_threshold: None,
            retry_boost_priority: Priority::High,
            custom_fn: Some(std::sync::Arc::new(custom_fn)),
        }
    }

    /// Calculate boosted priority for a message
    pub fn calculate_priority(&self, message: &Message, current_priority: Priority) -> Priority {
        let mut priority = current_priority;

        // Apply custom function if provided
        if let Some(ref custom_fn) = self.custom_fn {
            return custom_fn(message, priority);
        }

        // Check retry count
        if let Some(retry_threshold) = self.retry_threshold {
            if message.headers.retries.unwrap_or(0) >= retry_threshold {
                priority = priority.max(self.retry_boost_priority);
            }
        }

        // Check message age (using timestamp if available)
        if let Some(age_threshold) = self.age_threshold {
            if let Some(timestamp_value) = message.headers.extra.get("timestamp") {
                if let Some(timestamp_secs) = timestamp_value.as_f64() {
                    let msg_age = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs_f64()
                        - timestamp_secs;
                    if msg_age > age_threshold.as_secs_f64() {
                        priority = priority.max(self.age_boost_priority);
                    }
                }
            }
        }

        priority
    }
}

impl Default for PriorityBoostMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageMiddleware for PriorityBoostMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Get current priority from message headers
        let current_priority = message
            .headers
            .extra
            .get("priority")
            .and_then(|v| v.as_u64())
            .map(|p| Priority::from_u8(p as u8))
            .unwrap_or(Priority::Normal);

        let boosted_priority = self.calculate_priority(message, current_priority);

        if boosted_priority != current_priority {
            message.headers.extra.insert(
                "priority".to_string(),
                serde_json::json!(boosted_priority.as_u8()),
            );
            message
                .headers
                .extra
                .insert("x-priority-boosted".to_string(), serde_json::json!(true));
            message.headers.extra.insert(
                "x-original-priority".to_string(),
                serde_json::json!(current_priority.as_u8()),
            );
        }
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "priority_boost"
    }
}
