//! Advanced middleware implementations.

use async_trait::async_trait;
use celers_protocol::extensions::MessageExt;
use celers_protocol::Message;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use crate::middleware::{effective_priority, effective_retries};
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

/// A pluggable destination for [`AuditMiddleware`] entries.
///
/// By default `AuditMiddleware` only stamps its audit entry into the
/// message's own headers (`audit-publish` / `audit-consume`), which is
/// visible to - and mutable by - any downstream code that also touches the
/// message, and is lost once the message is acknowledged. That is adequate
/// for local debugging, but is not a tamper-resistant audit trail. Provide
/// a sink via [`AuditMiddleware::with_sink`] to *also* forward every entry
/// to a real, durable destination (a log aggregator, an audit table, an
/// event stream, ...).
pub trait AuditSink: Send + Sync {
    /// Record one already-formatted audit entry.
    fn record(&self, entry: &str);
}

impl<F> AuditSink for F
where
    F: Fn(&str) + Send + Sync,
{
    fn record(&self, entry: &str) {
        (self)(entry)
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
///
/// Header stamping alone is visible to (and mutable by) any downstream
/// code that also touches the message. Attach a real destination with
/// [`AuditMiddleware::with_sink`] for a durable, tamper-resistant trail:
///
/// ```
/// use celers_kombu::AuditMiddleware;
/// use std::sync::{Arc, Mutex};
///
/// let log = Arc::new(Mutex::new(Vec::<String>::new()));
/// let log_for_sink = log.clone();
/// let middleware = AuditMiddleware::new(true)
///     .with_sink(move |entry: &str| log_for_sink.lock().unwrap().push(entry.to_string()));
/// ```
#[derive(Clone)]
pub struct AuditMiddleware {
    log_body: bool,
    sink: Option<std::sync::Arc<dyn AuditSink>>,
}

impl std::fmt::Debug for AuditMiddleware {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AuditMiddleware")
            .field("log_body", &self.log_body)
            .field("has_sink", &self.sink.is_some())
            .finish()
    }
}

impl AuditMiddleware {
    /// Create a new audit middleware
    ///
    /// # Arguments
    ///
    /// * `log_body` - Whether to include message body in audit logs
    pub fn new(log_body: bool) -> Self {
        Self {
            log_body,
            sink: None,
        }
    }

    /// Create audit middleware with body logging enabled
    pub fn with_body_logging() -> Self {
        Self::new(true)
    }

    /// Create audit middleware without body logging
    pub fn without_body_logging() -> Self {
        Self::new(false)
    }

    /// Attach a sink that receives a copy of every audit entry in addition
    /// to (not instead of) the existing header stamping. Use this to
    /// deliver audit records to a real destination rather than relying
    /// solely on the message's own (mutable, in-band, ephemeral) headers.
    pub fn with_sink(mut self, sink: impl AuditSink + 'static) -> Self {
        self.sink = Some(std::sync::Arc::new(sink));
        self
    }

    fn create_audit_entry(&self, message: &Message, operation: &str) -> String {
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("SystemTime should be after UNIX_EPOCH")
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

        // Always stamp the entry into the message's own headers (visible
        // to any downstream code that inspects it, but mutable and not
        // durable beyond the message's own lifetime)...
        message.headers.extra.insert(
            "audit-publish".to_string(),
            serde_json::json!(audit_entry.clone()),
        );

        // ...and, if a sink was configured, also forward it to a real,
        // tamper-resistant destination.
        if let Some(ref sink) = self.sink {
            sink.record(&audit_entry);
        }

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

        message.headers.extra.insert(
            "audit-consume".to_string(),
            serde_json::json!(audit_entry.clone()),
        );

        if let Some(ref sink) = self.sink {
            sink.record(&audit_entry);
        }

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
            .expect("SystemTime should be after UNIX_EPOCH")
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
                    .expect("SystemTime should be after UNIX_EPOCH")
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
            // Prefer the typed `properties.priority` field - the one
            // actually populated by `Message::with_priority` and the
            // message builder - falling back to a legacy
            // `headers.extra["priority"]` marker, matching
            // `effective_priority`'s default-of-0 behaviour here for
            // parity with the previous implementation when neither is set.
            let priority = effective_priority(msg, 0);
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

/// Idempotency middleware for detecting - and, optionally, automatically
/// dropping - repeated deliveries of the same message.
///
/// This middleware tracks processed message IDs to recognise duplicate
/// deliveries. Unlike `DeduplicationMiddleware` which only prevents
/// duplicate *publishing*, `IdempotencyMiddleware` recognises when a
/// message is *delivered* more than once (e.g., due to network issues or
/// broker-level redelivery after a lost ack).
///
/// # Detection-only by default
///
/// By default (`new`/`with_default_cache`), `after_consume` only stamps
/// `x-already-processed` on the message and always returns `Ok`; it does
/// not itself stop the message from being handed to your task handler.
/// **Your consumer/worker is responsible for checking
/// `x-already-processed` and skipping re-execution when it is `true`**,
/// e.g. after calling `middleware.after_consume(&mut message).await`:
///
/// ```
/// use celers_kombu::{IdempotencyMiddleware, MessageMiddleware};
///
/// fn should_skip(message: &celers_protocol::Message) -> bool {
///     message
///         .headers
///         .extra
///         .get("x-already-processed")
///         .and_then(|v| v.as_bool())
///         .unwrap_or(false)
/// }
///
/// let middleware = IdempotencyMiddleware::new(10_000);
/// assert_eq!(middleware.name(), "idempotency");
/// // if !should_skip(&message) { run_task(&message).await; }
/// ```
///
/// This is the default (rather than dropping automatically) so that
/// installing `IdempotencyMiddleware` never silently changes existing
/// delivery behaviour for callers who only want the detection signal.
///
/// # Automatic enforcement (opt-in)
///
/// Call [`Self::with_enforcement`] to make `after_consume` itself reject a
/// repeat delivery with an `Err` that [`MessageMiddleware::is_drop_signal`]
/// classifies as a designed drop (mirroring `DeduplicationMiddleware`), so
/// [`crate::MiddlewareChain::process_after_consume`] /
/// [`crate::MiddlewareConsumer::consume_with_middleware`] settle it as a
/// drop rather than handing it to your task handler at all:
///
/// ```
/// use celers_kombu::IdempotencyMiddleware;
///
/// let middleware = IdempotencyMiddleware::new(10_000).with_enforcement(true);
/// ```
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
    processed_ids: std::sync::Arc<std::sync::Mutex<IdempotencyState>>,
    max_cache_size: usize,
    enforce: bool,
}

/// Internal idempotency cache state: a
/// [`HashSet`](std::collections::HashSet) for O(1) membership checks
/// paired with a [`VecDeque`](std::collections::VecDeque) recording true
/// insertion order, so eviction removes the actual oldest entries instead
/// of whatever a `HashSet`'s unspecified iteration order happens to
/// produce first.
struct IdempotencyState {
    seen: std::collections::HashSet<String>,
    order: VecDeque<String>,
}

impl IdempotencyMiddleware {
    /// Create a new idempotency middleware with a custom cache size
    pub fn new(max_cache_size: usize) -> Self {
        Self {
            processed_ids: std::sync::Arc::new(std::sync::Mutex::new(IdempotencyState {
                seen: std::collections::HashSet::new(),
                order: VecDeque::new(),
            })),
            max_cache_size,
            enforce: false,
        }
    }

    /// Create a new idempotency middleware with default cache size (10,000)
    pub fn with_default_cache() -> Self {
        Self::new(10000)
    }

    /// Enable (or disable) automatic enforcement: when `true`,
    /// `after_consume` rejects a repeat delivery outright (see the type
    /// docs' "Automatic enforcement" section) instead of only stamping
    /// `x-already-processed` and letting it through. Defaults to `false`.
    pub fn with_enforcement(mut self, enforce: bool) -> Self {
        self.enforce = enforce;
        self
    }

    /// Check if a message ID has been processed
    pub fn is_processed(&self, message_id: &str) -> bool {
        self.processed_ids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .seen
            .contains(message_id)
    }

    /// Mark a message ID as processed
    pub fn mark_processed(&self, message_id: String) {
        let mut state = self.processed_ids.lock().unwrap_or_else(|e| e.into_inner());

        // Cache eviction: if we're at capacity, evict the oldest 20% by
        // true insertion order (a bare `HashSet` has no ordering, so
        // evicting via its iteration order - the previous approach -
        // removed arbitrary entries rather than the actual oldest ones).
        if state.order.len() >= self.max_cache_size {
            let to_remove = (self.max_cache_size / 5).max(1);
            for _ in 0..to_remove {
                match state.order.pop_front() {
                    Some(oldest) => {
                        state.seen.remove(&oldest);
                    }
                    None => break,
                }
            }
        }

        if state.seen.insert(message_id.clone()) {
            state.order.push_back(message_id);
        }
    }

    /// Clear all processed message IDs
    pub fn clear(&self) {
        let mut state = self.processed_ids.lock().unwrap_or_else(|e| e.into_inner());
        state.seen.clear();
        state.order.clear();
    }

    /// Get the number of tracked message IDs
    pub fn cache_size(&self) -> usize {
        self.processed_ids
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .seen
            .len()
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

            // Opt-in enforcement (see `Self::with_enforcement`): reject the
            // repeat delivery outright rather than merely flagging it and
            // relying on the caller to check `x-already-processed`.
            if self.enforce {
                return Err(BrokerError::OperationFailed(format!(
                    "Message already processed (idempotency key: {})",
                    idempotency_key
                )));
            }
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

    fn is_drop_signal(&self, _err: &BrokerError) -> bool {
        // Under `with_enforcement(true)`, a repeat delivery is this
        // middleware's designed "skip this message" signal (identical in
        // spirit to `DeduplicationMiddleware`), not a processing failure -
        // it must be settled as a drop rather than requeued, or every
        // repeat delivery would loop forever hitting this same check.
        self.enforce
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
        // Calculate and inject backoff delay based on retry count. Prefer
        // the typed `headers.retries` field - the one actually reserved
        // for this purpose on the wire (see `effective_retries`) - falling
        // back to a legacy `headers.extra["retries"]` marker so callers
        // that only ever set it there keep working.
        let retry_count = effective_retries(message);

        let backoff_delay = self.calculate_delay(retry_count);

        message.headers.extra.insert(
            "x-backoff-delay".to_string(),
            serde_json::json!(backoff_delay.as_millis() as u64),
        );

        message.headers.extra.insert(
            "x-next-retry-at".to_string(),
            serde_json::json!((std::time::SystemTime::now() + backoff_delay)
                .duration_since(std::time::UNIX_EPOCH)
                .expect("SystemTime should be after UNIX_EPOCH")
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
/// # This middleware does not populate the cache by itself
///
/// [`MessageMiddleware::after_consume`] runs *before* your task handler
/// executes (there is no post-execution hook in this trait), so it has no
/// result to cache yet - it can only check for and report an existing
/// entry via [`Self::get_cached`] (surfaced as the `x-cache-hit` /
/// `x-cached-result-size` headers). **Your consumer/worker must call
/// [`Self::store_result`] itself once a task handler produces a result**,
/// e.g.:
///
/// ```
/// use celers_kombu::CachingMiddleware;
/// use std::time::Duration;
/// # fn run_task(_body: &[u8]) -> Vec<u8> { Vec::new() }
///
/// let middleware = CachingMiddleware::new(1000, Duration::from_secs(3600));
/// let message = celers_protocol::Message::new(
///     "my_task".to_string(),
///     uuid::Uuid::new_v4(),
///     b"args".to_vec(),
/// );
///
/// let result = match middleware.get_cached(&message) {
///     Some(cached) => cached,
///     None => {
///         let result = run_task(&message.body);
///         middleware.store_result(&message, result.clone());
///         result
///     }
/// };
/// # let _ = result;
/// ```
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
    ///
    /// Keyed by task name plus a hash of the body, **not** the message ID.
    /// The message ID is unique per message by construction, so keying on
    /// it made a cache hit structurally impossible even when populated: no
    /// two distinct messages could ever share a key. Keying on the task
    /// name and body content means two *different* deliveries of "the same
    /// call" (same task, same serialized arguments) - e.g. a republish
    /// after a lost ack, or an explicit retry - correctly map to the same
    /// cache entry.
    fn cache_key(&self, message: &Message) -> String {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let mut hasher = DefaultHasher::new();
        message.body.hash(&mut hasher);
        let body_hash = hasher.finish();
        format!("{}:{:x}", message.headers.task, body_hash)
    }

    /// Check if a cached result exists and is still valid
    pub fn get_cached(&self, message: &Message) -> Option<Vec<u8>> {
        let key = self.cache_key(message);
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());

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
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());

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
        self.cache.lock().unwrap_or_else(|e| e.into_inner()).clear();
    }

    /// Get the number of cached entries
    pub fn cache_size(&self) -> usize {
        self.cache.lock().unwrap_or_else(|e| e.into_inner()).len()
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
    /// Bounds how long an acquired permit can remain outstanding before it
    /// is treated as abandoned and reclaimed. Permits are acquired in
    /// `before_publish` and released in `after_consume`; if a producer and
    /// a consumer run as different processes (the common Celery/Kombu
    /// deployment shape) with different `BulkheadMiddleware` instances,
    /// `release` for a given permit is never observed by the instance that
    /// acquired it. Without a TTL, every publish would permanently consume
    /// a slot and the bulkhead would lock up forever after
    /// `max_concurrent` publishes. Sharing one instance (via `Clone`, which
    /// shares the underlying `Arc`) between the publish and consume sides
    /// of the *same* process still gets prompt, TTL-independent release.
    permit_ttl: Duration,
    permits: std::sync::Arc<std::sync::Mutex<HashMap<String, VecDeque<Instant>>>>,
    partition_fn: std::sync::Arc<dyn Fn(&Message) -> String + Send + Sync>,
}

/// Default permit TTL: generous enough to cover realistic end-to-end
/// publish-to-consume latency, short enough that an abandoned permit
/// (producer-only deployment) does not lock a partition out indefinitely.
const DEFAULT_BULKHEAD_PERMIT_TTL: Duration = Duration::from_secs(300);

impl BulkheadMiddleware {
    /// Create a new bulkhead middleware with max concurrent operations
    ///
    /// # Arguments
    ///
    /// * `max_concurrent` - Maximum number of concurrent operations per partition
    pub fn new(max_concurrent: usize) -> Self {
        Self {
            max_concurrent,
            permit_ttl: DEFAULT_BULKHEAD_PERMIT_TTL,
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
            permit_ttl: DEFAULT_BULKHEAD_PERMIT_TTL,
            permits: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
            partition_fn: std::sync::Arc::new(partition_fn),
        }
    }

    /// Override how long an acquired permit may remain outstanding before
    /// it is reclaimed (see the field doc on [`Self`] for why this
    /// exists). Defaults to 5 minutes.
    pub fn with_permit_ttl(mut self, ttl: Duration) -> Self {
        self.permit_ttl = ttl;
        self
    }

    /// Try to acquire a permit for the given partition. Returns `false` if
    /// the partition already has `max_concurrent` non-expired permits
    /// outstanding.
    pub fn try_acquire(&self, partition: &str) -> bool {
        let mut permits = self.permits.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let entry = permits.entry(partition.to_string()).or_default();
        // Reclaim any permits that outlived the TTL before deciding
        // whether there is room for a new one.
        entry.retain(|acquired_at| now.duration_since(*acquired_at) < self.permit_ttl);

        if entry.len() < self.max_concurrent {
            entry.push_back(now);
            true
        } else {
            false
        }
    }

    /// Release the oldest outstanding permit for the given partition.
    pub fn release(&self, partition: &str) {
        let mut permits = self.permits.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(entry) = permits.get_mut(partition) {
            entry.pop_front();
            if entry.is_empty() {
                // Bound the outer map: don't keep a permanent (empty)
                // entry for every distinct partition string ever seen.
                permits.remove(partition);
            }
        }
    }

    /// Get current (non-expired) concurrent operations for a partition
    pub fn current_operations(&self, partition: &str) -> usize {
        let mut permits = self.permits.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        match permits.get_mut(partition) {
            Some(entry) => {
                entry.retain(|acquired_at| now.duration_since(*acquired_at) < self.permit_ttl);
                entry.len()
            }
            None => 0,
        }
    }

    /// Get total (non-expired) concurrent operations across all partitions
    pub fn total_operations(&self) -> usize {
        let mut permits = self.permits.lock().unwrap_or_else(|e| e.into_inner());
        let now = Instant::now();
        let mut total = 0;
        permits.retain(|_, entry| {
            entry.retain(|acquired_at| now.duration_since(*acquired_at) < self.permit_ttl);
            total += entry.len();
            !entry.is_empty()
        });
        total
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
            // Actually enforce the limit: previously this branch only
            // stamped a "rejected" marker and still returned `Ok(())`, so
            // the message was published regardless and the concurrency
            // limit was never enforced.
            return Err(BrokerError::OperationFailed(format!(
                "Bulkhead limit reached for partition '{}': {} concurrent operations already in flight (max {})",
                partition, self.max_concurrent, self.max_concurrent
            )));
        }

        message.headers.extra.insert(
            "x-bulkhead-partition".to_string(),
            serde_json::json!(partition),
        );
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

        // Check message age. Prefer an explicit `headers.extra["timestamp"]`
        // override (epoch seconds) when present - callers that stamp their
        // own timestamp for testing or custom clocking get exactly the
        // behaviour they asked for - and otherwise fall back to the
        // message's real `created_at` (via `get_age_seconds`), which is
        // always populated by `Message::new` and requires no special
        // wiring. Previously only the (never-populated-in-practice)
        // `timestamp` header was read, so this branch was effectively dead
        // in real deployments.
        if let Some(age_threshold) = self.age_threshold {
            let msg_age_secs = message
                .headers
                .extra
                .get("timestamp")
                .and_then(|v| v.as_f64())
                .map(|timestamp_secs| {
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .map(|d| d.as_secs_f64())
                        .unwrap_or(0.0);
                    now_secs - timestamp_secs
                })
                .or_else(|| message.get_age_seconds().map(|secs| secs as f64));

            if let Some(msg_age) = msg_age_secs {
                if msg_age > age_threshold.as_secs_f64() {
                    priority = priority.max(self.age_boost_priority);
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
        // Get current priority: prefer the typed `properties.priority`
        // field (populated by `Message::with_priority` and the message
        // builder - the field real priority-queue consumers read) falling
        // back to a legacy `headers.extra["priority"]` marker.
        let current_priority =
            Priority::from_u8(effective_priority(message, Priority::Normal.as_u8()));

        let boosted_priority = self.calculate_priority(message, current_priority);

        if boosted_priority != current_priority {
            // Write the boosted value back to both the typed field (so
            // real priority-queue / broker code observes it) and the
            // legacy header (for backward compatibility with anything
            // still reading it there).
            message.properties.priority = Some(boosted_priority.as_u8());
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

#[cfg(test)]
mod hardening_tests {
    use super::*;
    use uuid::Uuid;

    // -------------------------------------------------------------------
    // idx114 / idx125: priority and age must be read from the real fields.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn routing_key_from_task_and_priority_prefers_typed_priority_field() {
        let middleware = RoutingKeyMiddleware::from_task_and_priority();
        let mut msg = Message::new("my_task".to_string(), Uuid::new_v4(), vec![]);
        // Only the typed field is set (not the legacy `headers.extra`
        // marker); the routing key must reflect it rather than silently
        // falling back to 0.
        msg.properties.priority = Some(9);

        middleware.before_publish(&mut msg).await.unwrap();

        let routing_key = msg
            .headers
            .extra
            .get("x-routing-key")
            .unwrap()
            .as_str()
            .unwrap();
        assert_eq!(routing_key, "tasks.my_task.priority_9");
    }

    #[tokio::test]
    async fn priority_boost_before_publish_reads_and_writes_typed_priority() {
        let middleware = PriorityBoostMiddleware::new().with_retry_boost(1, Priority::Highest);
        let mut msg = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        msg.properties.priority = Some(Priority::Normal.as_u8());
        msg.headers.retries = Some(5);

        middleware.before_publish(&mut msg).await.unwrap();

        assert_eq!(msg.properties.priority, Some(Priority::Highest.as_u8()));
        assert!(msg.headers.extra.contains_key("x-priority-boosted"));
    }

    #[test]
    fn priority_boost_age_boost_does_not_fire_for_a_fresh_message() {
        // No `headers.extra["timestamp"]` override is set: the middleware
        // falls back to the message's real creation time
        // (`get_age_seconds`). A freshly constructed message is ~0 seconds
        // old, so a generous threshold must not trigger the boost (and,
        // importantly, the new fallback path must not panic/error).
        let middleware = PriorityBoostMiddleware::new()
            .with_age_boost(Duration::from_secs(3600), Priority::Highest);
        let msg = Message::new("t".to_string(), Uuid::new_v4(), vec![]);

        let boosted = middleware.calculate_priority(&msg, Priority::Normal);
        assert_eq!(boosted, Priority::Normal);
    }

    #[tokio::test]
    async fn priority_boost_age_boost_fires_once_message_actually_ages() {
        // Real-clock based (no `headers.extra["timestamp"]` override, and
        // `get_age_seconds` is backed by `chrono::Utc::now()`, which no
        // mock clock in this dependency graph can fast-forward), so this
        // waits for a real, short elapsed time rather than faking one.
        // `get_age_seconds` truncates to whole seconds, hence the >1s wait
        // against a threshold just below it.
        let middleware = PriorityBoostMiddleware::new()
            .with_age_boost(Duration::from_millis(500), Priority::Highest);
        let msg = Message::new("t".to_string(), Uuid::new_v4(), vec![]);

        tokio::time::sleep(Duration::from_millis(1100)).await;

        let boosted = middleware.calculate_priority(&msg, Priority::Normal);
        assert_eq!(boosted, Priority::Highest);
    }

    // -------------------------------------------------------------------
    // idx121: BackoffMiddleware must read the typed retries field.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn backoff_middleware_reads_typed_retries_field() {
        let middleware =
            BackoffMiddleware::new(Duration::from_secs(1), Duration::from_secs(60), 2.0);
        let mut msg = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        msg.headers.retries = Some(3);

        middleware.after_consume(&mut msg).await.unwrap();

        let delay_ms = msg
            .headers
            .extra
            .get("x-backoff-delay")
            .unwrap()
            .as_u64()
            .unwrap();
        // base=1s, multiplier=2.0, retries=3 -> 8s plus 0-25% jitter.
        assert!((8000..=10000).contains(&delay_ms));
    }

    // -------------------------------------------------------------------
    // idx120: BulkheadMiddleware must actually enforce its limit and must
    // not leak permits forever when only the publish side ever runs.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn bulkhead_rejects_beyond_max_concurrent() {
        let bulkhead = BulkheadMiddleware::new(2);
        let mut msg1 = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        let mut msg2 = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        let mut msg3 = Message::new("t".to_string(), Uuid::new_v4(), vec![]);

        assert!(bulkhead.before_publish(&mut msg1).await.is_ok());
        assert!(bulkhead.before_publish(&mut msg2).await.is_ok());
        // Third concurrent publish for the same partition must be
        // rejected, not silently allowed through.
        let result = bulkhead.before_publish(&mut msg3).await;
        assert!(result.is_err());
        assert_eq!(
            msg3.headers.extra.get("x-bulkhead-rejected"),
            Some(&serde_json::json!(true))
        );
    }

    #[tokio::test]
    async fn bulkhead_reclaims_expired_permits_instead_of_locking_up_forever() {
        // A producer-only instance (never sees `after_consume`, e.g.
        // because the consumer runs in a different process with its own
        // `BulkheadMiddleware`) must not stay permanently rejecting once
        // permits age out.
        let bulkhead = BulkheadMiddleware::new(1).with_permit_ttl(Duration::from_millis(1));
        let mut msg1 = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        assert!(bulkhead.before_publish(&mut msg1).await.is_ok());

        // Immediately over the limit (permit not yet expired).
        let mut msg2 = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        assert!(bulkhead.before_publish(&mut msg2).await.is_err());

        tokio::time::sleep(Duration::from_millis(20)).await;

        // The first permit has now expired and must be reclaimed.
        let mut msg3 = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        assert!(bulkhead.before_publish(&mut msg3).await.is_ok());
    }

    #[tokio::test]
    async fn bulkhead_release_frees_a_slot_immediately() {
        let bulkhead = BulkheadMiddleware::new(1);
        let mut msg1 = Message::new("task".to_string(), Uuid::new_v4(), vec![]);
        assert!(bulkhead.before_publish(&mut msg1).await.is_ok());

        // Same-partition second publish is rejected while the first is
        // still outstanding.
        let mut msg2 = Message::new("task".to_string(), Uuid::new_v4(), vec![]);
        assert!(bulkhead.before_publish(&mut msg2).await.is_err());

        // Releasing (as `after_consume` does) frees the slot right away,
        // with no need to wait for the TTL.
        bulkhead.after_consume(&mut msg1).await.unwrap();
        let mut msg3 = Message::new("task".to_string(), Uuid::new_v4(), vec![]);
        assert!(bulkhead.before_publish(&mut msg3).await.is_ok());
    }

    // -------------------------------------------------------------------
    // idx141: CachingMiddleware's cache key must be able to hit at all.
    // -------------------------------------------------------------------

    #[test]
    fn caching_middleware_key_matches_across_distinct_message_ids() {
        let middleware = CachingMiddleware::with_defaults();
        let body = b"same args".to_vec();

        let msg_a = Message::new("my_task".to_string(), Uuid::new_v4(), body.clone());
        let msg_b = Message::new("my_task".to_string(), Uuid::new_v4(), body);

        // Two distinct messages (different IDs) representing "the same
        // call" (same task, same body) must be able to share a cache
        // entry - keying on the message ID (unique per message by
        // construction) made this structurally impossible before.
        middleware.store_result(&msg_a, b"cached".to_vec());
        assert_eq!(middleware.get_cached(&msg_b), Some(b"cached".to_vec()));
    }

    #[test]
    fn caching_middleware_key_differs_for_different_bodies() {
        let middleware = CachingMiddleware::with_defaults();
        let msg_a = Message::new("my_task".to_string(), Uuid::new_v4(), b"args1".to_vec());
        let msg_b = Message::new("my_task".to_string(), Uuid::new_v4(), b"args2".to_vec());

        middleware.store_result(&msg_a, b"cached".to_vec());
        assert_eq!(middleware.get_cached(&msg_b), None);
    }

    // -------------------------------------------------------------------
    // idx141: AuditMiddleware sink must receive entries alongside headers.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn audit_middleware_sink_receives_entries() {
        let log = std::sync::Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
        let log_for_sink = log.clone();
        let middleware = AuditMiddleware::new(false).with_sink(move |entry: &str| {
            log_for_sink.lock().unwrap().push(entry.to_string());
        });

        let mut msg = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        middleware.before_publish(&mut msg).await.unwrap();
        middleware.after_consume(&mut msg).await.unwrap();

        let entries = log.lock().unwrap();
        assert_eq!(entries.len(), 2);
        assert!(entries[0].contains("PUBLISH"));
        assert!(entries[1].contains("CONSUME"));

        // Header stamping must still happen unchanged (existing
        // contract), sink is additive.
        assert!(msg.headers.extra.contains_key("audit-publish"));
        assert!(msg.headers.extra.contains_key("audit-consume"));
    }

    // -------------------------------------------------------------------
    // idx141: IdempotencyMiddleware must be able to actually enforce
    // (not just detect and flag) when opted in, while leaving the default
    // (detection-only) behaviour completely unchanged for existing callers.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn idempotency_default_behaviour_lets_duplicates_through_unchanged() {
        let middleware = IdempotencyMiddleware::new(1000);
        let task_id = Uuid::new_v4();

        let mut msg1 = Message::new("t".to_string(), task_id, vec![]);
        assert!(middleware.after_consume(&mut msg1).await.is_ok());

        // Second delivery of the same message: still `Ok` by default - the
        // caller opted into detection only, not enforcement.
        let mut msg2 = Message::new("t".to_string(), task_id, vec![]);
        assert!(middleware.after_consume(&mut msg2).await.is_ok());
        assert_eq!(
            msg2.headers.extra.get("x-already-processed"),
            Some(&serde_json::json!(true))
        );
    }

    #[tokio::test]
    async fn idempotency_with_enforcement_rejects_repeat_delivery() {
        let middleware = IdempotencyMiddleware::new(1000).with_enforcement(true);
        let task_id = Uuid::new_v4();

        let mut msg1 = Message::new("t".to_string(), task_id, vec![]);
        assert!(middleware.after_consume(&mut msg1).await.is_ok());

        // Second delivery of the same message must now be rejected.
        let mut msg2 = Message::new("t".to_string(), task_id, vec![]);
        assert!(middleware.after_consume(&mut msg2).await.is_err());
    }

    #[tokio::test]
    async fn idempotency_with_enforcement_settles_repeat_as_a_drop_not_a_failure() {
        let middleware = IdempotencyMiddleware::new(1000).with_enforcement(true);
        let chain = crate::MiddlewareChain::new().with_middleware(Box::new(middleware));
        let task_id = Uuid::new_v4();

        let mut msg1 = Message::new("t".to_string(), task_id, vec![]);
        assert!(chain
            .process_after_consume(&mut msg1)
            .await
            .unwrap()
            .is_accept());

        // The repeat delivery must be classified as a designed drop (via
        // `is_drop_signal`), so `MiddlewareConsumer::consume_with_middleware`
        // settles it instead of requeuing it forever.
        let mut msg2 = Message::new("t".to_string(), task_id, vec![]);
        let decision = chain.process_after_consume(&mut msg2).await.unwrap();
        assert!(decision.is_drop());
    }

    // -------------------------------------------------------------------
    // idx152: IdempotencyMiddleware must evict the true oldest entries.
    // -------------------------------------------------------------------

    #[test]
    fn idempotency_middleware_evicts_oldest_first() {
        let middleware = IdempotencyMiddleware::new(5);
        for i in 0..5 {
            middleware.mark_processed(format!("id-{i}"));
        }
        assert_eq!(middleware.cache_size(), 5);

        // Cache is now full; marking one more must evict the oldest 20%
        // (max(5/5, 1) = 1 entry), i.e. "id-0", not an arbitrary one.
        middleware.mark_processed("id-5".to_string());
        assert!(!middleware.is_processed("id-0"));
        assert!(middleware.is_processed("id-1"));
        assert!(middleware.is_processed("id-5"));
    }
}
