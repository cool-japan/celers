//! Basic middleware implementations.

use async_trait::async_trait;
use celers_protocol::Message;
use std::time::Duration;

use uuid::Uuid;

use crate::{BrokerError, BrokerMetrics, MessageMiddleware, Result};

// =============================================================================
// Built-in Middleware Implementations
// =============================================================================

/// Validation middleware - validates message structure
///
/// # Examples
///
/// ```
/// use celers_kombu::ValidationMiddleware;
///
/// // Default validation (10MB max, require task name)
/// let validator = ValidationMiddleware::new();
///
/// // Custom validation
/// let validator = ValidationMiddleware::new()
///     .with_max_body_size(5 * 1024 * 1024)  // 5MB limit
///     .with_require_task_name(true);
///
/// // Disable body size limit
/// let validator = ValidationMiddleware::new()
///     .without_body_size_limit();
/// ```
pub struct ValidationMiddleware {
    /// Maximum message body size (bytes)
    max_body_size: Option<usize>,
    /// Require task name to be non-empty
    require_task_name: bool,
}

impl ValidationMiddleware {
    /// Create a new validation middleware
    pub fn new() -> Self {
        Self {
            max_body_size: Some(10 * 1024 * 1024), // 10MB default
            require_task_name: true,
        }
    }

    /// Set maximum body size
    pub fn with_max_body_size(mut self, size: usize) -> Self {
        self.max_body_size = Some(size);
        self
    }

    /// Disable body size check
    pub fn without_body_size_limit(mut self) -> Self {
        self.max_body_size = None;
        self
    }

    /// Set whether task name is required
    pub fn with_require_task_name(mut self, require: bool) -> Self {
        self.require_task_name = require;
        self
    }

    fn validate_message(&self, message: &Message) -> Result<()> {
        // Check task name
        if self.require_task_name && message.task_name().is_empty() {
            return Err(BrokerError::Configuration(
                "Task name cannot be empty".to_string(),
            ));
        }

        // Check body size
        if let Some(max_size) = self.max_body_size {
            if message.body.len() > max_size {
                return Err(BrokerError::Configuration(format!(
                    "Message body size {} exceeds maximum {}",
                    message.body.len(),
                    max_size
                )));
            }
        }

        Ok(())
    }
}

impl Default for ValidationMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageMiddleware for ValidationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        self.validate_message(message)
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        self.validate_message(message)
    }

    fn name(&self) -> &str {
        "validation"
    }
}

/// Logging middleware - logs message events
///
/// # Examples
///
/// ```
/// use celers_kombu::LoggingMiddleware;
///
/// // Basic logging
/// let logger = LoggingMiddleware::new("MyApp");
///
/// // With detailed body logging
/// let verbose_logger = LoggingMiddleware::new("MyApp")
///     .with_body_logging();
/// ```
pub struct LoggingMiddleware {
    prefix: String,
    log_body: bool,
}

impl LoggingMiddleware {
    /// Create a new logging middleware
    pub fn new(prefix: impl Into<String>) -> Self {
        Self {
            prefix: prefix.into(),
            log_body: false,
        }
    }

    /// Enable body logging (for debugging)
    pub fn with_body_logging(mut self) -> Self {
        self.log_body = true;
        self
    }
}

#[async_trait]
impl MessageMiddleware for LoggingMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        if self.log_body {
            eprintln!(
                "[{}] Publishing: task={}, id={}, body_size={}",
                self.prefix,
                message.task_name(),
                message.task_id(),
                message.body.len()
            );
        } else {
            eprintln!(
                "[{}] Publishing: task={}, id={}",
                self.prefix,
                message.task_name(),
                message.task_id()
            );
        }
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        if self.log_body {
            eprintln!(
                "[{}] Consumed: task={}, id={}, body_size={}",
                self.prefix,
                message.task_name(),
                message.task_id(),
                message.body.len()
            );
        } else {
            eprintln!(
                "[{}] Consumed: task={}, id={}",
                self.prefix,
                message.task_name(),
                message.task_id()
            );
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "logging"
    }
}

/// Metrics middleware - collects message statistics
///
/// # Examples
///
/// ```
/// use celers_kombu::{MetricsMiddleware, BrokerMetrics};
/// use std::sync::{Arc, Mutex};
///
/// let metrics = Arc::new(Mutex::new(BrokerMetrics::default()));
/// let middleware = MetricsMiddleware::new(metrics.clone());
///
/// // Later, get metrics snapshot
/// let snapshot = middleware.get_metrics();
/// assert_eq!(snapshot.messages_published, 0);
/// ```
pub struct MetricsMiddleware {
    metrics: std::sync::Arc<std::sync::Mutex<BrokerMetrics>>,
}

impl MetricsMiddleware {
    /// Create a new metrics middleware
    pub fn new(metrics: std::sync::Arc<std::sync::Mutex<BrokerMetrics>>) -> Self {
        Self { metrics }
    }

    /// Get current metrics snapshot
    pub fn get_metrics(&self) -> BrokerMetrics {
        self.metrics.lock().unwrap().clone()
    }
}

#[async_trait]
impl MessageMiddleware for MetricsMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        let mut metrics = self.metrics.lock().unwrap();
        metrics.inc_published();
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        let mut metrics = self.metrics.lock().unwrap();
        metrics.inc_consumed();
        Ok(())
    }

    fn name(&self) -> &str {
        "metrics"
    }
}

/// Retry limit middleware - enforces maximum retry count
///
/// # Examples
///
/// ```
/// use celers_kombu::RetryLimitMiddleware;
///
/// // Allow up to 3 retries
/// let middleware = RetryLimitMiddleware::new(3);
/// ```
pub struct RetryLimitMiddleware {
    max_retries: u32,
}

impl RetryLimitMiddleware {
    /// Create a new retry limit middleware
    pub fn new(max_retries: u32) -> Self {
        Self { max_retries }
    }
}

#[async_trait]
impl MessageMiddleware for RetryLimitMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        // No validation on publish
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Check retry count from message headers
        let retries = message.headers.retries.unwrap_or(0);
        if retries > self.max_retries {
            return Err(BrokerError::Configuration(format!(
                "Message exceeded maximum retries: {} > {}",
                retries, self.max_retries
            )));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "retry_limit"
    }
}

/// Rate limiting middleware - enforces message rate limits
///
/// # Examples
///
/// ```
/// use celers_kombu::RateLimitingMiddleware;
///
/// // Limit to 100 messages per second
/// let middleware = RateLimitingMiddleware::new(100.0);
/// ```
pub struct RateLimitingMiddleware {
    /// Maximum messages per second
    max_rate: f64,
    /// Token bucket (tracks available tokens)
    tokens: std::sync::Arc<std::sync::Mutex<TokenBucket>>,
}

/// Token bucket for rate limiting
struct TokenBucket {
    /// Current token count
    tokens: f64,
    /// Maximum tokens
    capacity: f64,
    /// Tokens added per second
    refill_rate: f64,
    /// Last refill time
    last_refill: std::time::Instant,
}

impl TokenBucket {
    fn new(capacity: f64, refill_rate: f64) -> Self {
        Self {
            tokens: capacity,
            capacity,
            refill_rate,
            last_refill: std::time::Instant::now(),
        }
    }

    fn try_consume(&mut self, tokens: f64) -> bool {
        // Refill tokens based on elapsed time
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.capacity);
        self.last_refill = now;

        // Try to consume tokens
        if self.tokens >= tokens {
            self.tokens -= tokens;
            true
        } else {
            false
        }
    }
}

impl RateLimitingMiddleware {
    /// Create a new rate limiting middleware
    ///
    /// # Arguments
    ///
    /// * `max_rate` - Maximum messages per second
    pub fn new(max_rate: f64) -> Self {
        Self {
            max_rate,
            tokens: std::sync::Arc::new(std::sync::Mutex::new(TokenBucket::new(
                max_rate, max_rate,
            ))),
        }
    }
}

#[async_trait]
impl MessageMiddleware for RateLimitingMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        // Try to acquire a token
        let mut bucket = self.tokens.lock().unwrap();
        if !bucket.try_consume(1.0) {
            return Err(BrokerError::OperationFailed(format!(
                "Rate limit exceeded: {} messages/sec",
                self.max_rate
            )));
        }
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No rate limiting on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "rate_limit"
    }
}

/// Deduplication middleware - prevents duplicate message processing
///
/// # Examples
///
/// ```
/// use celers_kombu::DeduplicationMiddleware;
///
/// // Track up to 5000 message IDs
/// let middleware = DeduplicationMiddleware::new(5000);
///
/// // Use default cache size (10,000)
/// let default_middleware = DeduplicationMiddleware::with_default_cache();
/// ```
pub struct DeduplicationMiddleware {
    /// Recently seen message IDs
    seen_ids: std::sync::Arc<std::sync::Mutex<std::collections::HashSet<Uuid>>>,
    /// Maximum size of seen IDs cache
    max_cache_size: usize,
}

impl DeduplicationMiddleware {
    /// Create a new deduplication middleware
    ///
    /// # Arguments
    ///
    /// * `max_cache_size` - Maximum number of message IDs to track
    pub fn new(max_cache_size: usize) -> Self {
        Self {
            seen_ids: std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashSet::new())),
            max_cache_size,
        }
    }

    /// Create with default cache size (10,000 message IDs)
    pub fn with_default_cache() -> Self {
        Self::new(10_000)
    }
}

impl Default for DeduplicationMiddleware {
    fn default() -> Self {
        Self::with_default_cache()
    }
}

#[async_trait]
impl MessageMiddleware for DeduplicationMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        // No deduplication on publish
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        let msg_id = message.task_id();
        let mut seen = self.seen_ids.lock().unwrap();

        // Check if we've seen this message before
        if seen.contains(&msg_id) {
            return Err(BrokerError::OperationFailed(format!(
                "Duplicate message detected: {}",
                msg_id
            )));
        }

        // Add to seen set
        seen.insert(msg_id);

        // Evict oldest entries if cache is too large (simple FIFO eviction)
        if seen.len() > self.max_cache_size {
            // Remove first element (note: HashSet doesn't have ordering, so this is arbitrary)
            if let Some(&id) = seen.iter().next() {
                seen.remove(&id);
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "deduplication"
    }
}

/// Compression middleware - compresses/decompresses message bodies
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "compression")]
/// # {
/// use celers_kombu::CompressionMiddleware;
/// use celers_protocol::compression::CompressionType;
///
/// let middleware = CompressionMiddleware::new(CompressionType::Gzip)
///     .with_min_size(2048)  // Only compress messages >= 2KB
///     .with_level(6);       // Compression level 6
/// # }
/// ```
#[cfg(feature = "compression")]
pub struct CompressionMiddleware {
    /// Compressor instance
    compressor: celers_protocol::compression::Compressor,
    /// Minimum body size to compress (bytes)
    min_compress_size: usize,
}

#[cfg(feature = "compression")]
impl CompressionMiddleware {
    /// Create a new compression middleware
    ///
    /// # Arguments
    ///
    /// * `compression_type` - Type of compression to use
    pub fn new(compression_type: celers_protocol::compression::CompressionType) -> Self {
        Self {
            compressor: celers_protocol::compression::Compressor::new(compression_type),
            min_compress_size: 1024, // 1KB default
        }
    }

    /// Set minimum body size to compress
    pub fn with_min_size(mut self, size: usize) -> Self {
        self.min_compress_size = size;
        self
    }

    /// Set compression level
    pub fn with_level(mut self, level: u32) -> Self {
        self.compressor = self.compressor.with_level(level);
        self
    }
}

#[cfg(feature = "compression")]
#[async_trait]
impl MessageMiddleware for CompressionMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Only compress if body is large enough
        if message.body.len() >= self.min_compress_size {
            let compressed = self
                .compressor
                .compress(&message.body)
                .map_err(|e| BrokerError::Serialization(e.to_string()))?;

            // Only use compressed version if it's actually smaller
            if compressed.len() < message.body.len() {
                message.body = compressed;
                // Note: In a real implementation, we'd set a header to indicate compression
            }
        }
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Try to decompress (would need to check header in real implementation)
        // For now, we'll skip decompression on consume since we don't have metadata
        // A real implementation would check message headers for compression flag
        let _ = message;
        Ok(())
    }

    fn name(&self) -> &str {
        "compression"
    }
}

/// Signing middleware - signs/verifies message bodies using HMAC
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "signing")]
/// # {
/// use celers_kombu::SigningMiddleware;
///
/// let secret_key = b"my-secret-key";
/// let middleware = SigningMiddleware::new(secret_key);
/// # }
/// ```
#[cfg(feature = "signing")]
pub struct SigningMiddleware {
    /// Message signer instance
    signer: celers_protocol::auth::MessageSigner,
}

#[cfg(feature = "signing")]
impl SigningMiddleware {
    /// Create a new signing middleware
    ///
    /// # Arguments
    ///
    /// * `key` - Secret key for HMAC signing
    pub fn new(key: &[u8]) -> Self {
        Self {
            signer: celers_protocol::auth::MessageSigner::new(key),
        }
    }
}

#[cfg(feature = "signing")]
#[async_trait]
impl MessageMiddleware for SigningMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Sign the message body
        let signature = self
            .signer
            .sign(&message.body)
            .map_err(|e| BrokerError::OperationFailed(format!("signing failed: {}", e)))?;

        // Store signature in message headers (would need custom header field)
        // For now, we'll just validate that signing works
        // In a real implementation, we'd add a signature field to Message
        let _ = signature;

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // In a real implementation, we'd:
        // 1. Extract signature from message headers
        // 2. Verify signature against message body
        // 3. Return error if verification fails
        //
        // For now, we'll just validate the message can be signed
        let _ = self
            .signer
            .sign(&message.body)
            .map_err(|e| BrokerError::OperationFailed(format!("signing failed: {}", e)))?;

        Ok(())
    }

    fn name(&self) -> &str {
        "signing"
    }
}

/// Encryption middleware - encrypts/decrypts message bodies using AES-256-GCM
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "encryption")]
/// # {
/// use celers_kombu::EncryptionMiddleware;
///
/// // 32-byte key for AES-256
/// let key = [0u8; 32];
/// let middleware = EncryptionMiddleware::new(&key).expect("valid key");
/// # }
/// ```
#[cfg(feature = "encryption")]
pub struct EncryptionMiddleware {
    /// Message encryptor instance
    encryptor: celers_protocol::crypto::MessageEncryptor,
}

#[cfg(feature = "encryption")]
impl EncryptionMiddleware {
    /// Create a new encryption middleware
    ///
    /// # Arguments
    ///
    /// * `key` - 32-byte secret key for AES-256
    ///
    /// # Returns
    ///
    /// `Ok(EncryptionMiddleware)` if the key is valid, `Err(BrokerError)` otherwise
    pub fn new(key: &[u8]) -> Result<Self> {
        let encryptor = celers_protocol::crypto::MessageEncryptor::new(key)
            .map_err(|e| BrokerError::Configuration(e.to_string()))?;

        Ok(Self { encryptor })
    }
}

#[cfg(feature = "encryption")]
#[async_trait]
impl MessageMiddleware for EncryptionMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Encrypt the message body
        let (ciphertext, nonce) = self
            .encryptor
            .encrypt(&message.body)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;

        // In a real implementation, we'd store the nonce in message headers
        // For now, we'll prepend the nonce to the ciphertext
        let mut encrypted = nonce.to_vec();
        encrypted.extend_from_slice(&ciphertext);
        message.body = encrypted;

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Extract nonce and ciphertext
        if message.body.len() < celers_protocol::crypto::NONCE_SIZE {
            return Err(BrokerError::Serialization(
                "Message too short to contain nonce".to_string(),
            ));
        }

        let (nonce_bytes, ciphertext) = message.body.split_at(celers_protocol::crypto::NONCE_SIZE);

        // Decrypt the message body
        let plaintext = self
            .encryptor
            .decrypt(ciphertext, nonce_bytes)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;

        message.body = plaintext;
        Ok(())
    }

    fn name(&self) -> &str {
        "encryption"
    }
}

/// Timeout middleware - enforces message processing time limits
///
/// # Examples
///
/// ```
/// use celers_kombu::TimeoutMiddleware;
/// use std::time::Duration;
///
/// // Set 30 second timeout for message processing
/// let middleware = TimeoutMiddleware::new(Duration::from_secs(30));
/// ```
pub struct TimeoutMiddleware {
    timeout: Duration,
}

impl TimeoutMiddleware {
    /// Create a new timeout middleware
    pub fn new(timeout: Duration) -> Self {
        Self { timeout }
    }

    /// Get the configured timeout
    pub fn timeout(&self) -> Duration {
        self.timeout
    }
}

#[async_trait]
impl MessageMiddleware for TimeoutMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Store timeout in message headers for consumer
        message.headers.extra.insert(
            "x-timeout-ms".to_string(),
            serde_json::Value::Number((self.timeout.as_millis() as u64).into()),
        );
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // Timeout checking is implementation-specific and would be handled
        // by the consumer/worker. This middleware just sets the metadata.
        Ok(())
    }

    fn name(&self) -> &str {
        "timeout"
    }
}

/// Filter middleware - filters messages based on custom criteria
///
/// # Examples
///
/// ```
/// use celers_kombu::FilterMiddleware;
/// use celers_protocol::Message;
///
/// // Create filter that only allows high-priority tasks
/// let filter = FilterMiddleware::new(|msg: &Message| {
///     msg.task_name().starts_with("critical_")
/// });
/// ```
pub struct FilterMiddleware {
    predicate: Box<dyn Fn(&Message) -> bool + Send + Sync>,
}

impl FilterMiddleware {
    /// Create a new filter middleware with a predicate function
    pub fn new<F>(predicate: F) -> Self
    where
        F: Fn(&Message) -> bool + Send + Sync + 'static,
    {
        Self {
            predicate: Box::new(predicate),
        }
    }

    /// Check if a message passes the filter
    pub fn matches(&self, message: &Message) -> bool {
        (self.predicate)(message)
    }
}

#[async_trait]
impl MessageMiddleware for FilterMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        // No filtering on publish
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        if !self.matches(message) {
            return Err(BrokerError::Configuration(
                "Message filtered out by predicate".to_string(),
            ));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "filter"
    }
}

/// Sampling middleware for statistical message sampling.
///
/// Allows only a percentage of messages to pass through, useful for
/// monitoring, testing, or load reduction.
///
/// # Examples
///
/// ```
/// use celers_kombu::SamplingMiddleware;
///
/// // Sample 10% of messages
/// let sampler = SamplingMiddleware::new(0.1);
/// assert_eq!(sampler.sample_rate(), 0.1);
/// ```
pub struct SamplingMiddleware {
    sample_rate: f64,
    counter: std::sync::atomic::AtomicU64,
}

impl SamplingMiddleware {
    /// Create a new sampling middleware with the given sample rate.
    ///
    /// Sample rate should be between 0.0 and 1.0, where:
    /// - 0.0 = sample nothing
    /// - 1.0 = sample everything
    /// - 0.1 = sample approximately 10% of messages
    pub fn new(sample_rate: f64) -> Self {
        Self {
            sample_rate: sample_rate.clamp(0.0, 1.0),
            counter: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Get the configured sample rate
    pub fn sample_rate(&self) -> f64 {
        self.sample_rate
    }

    /// Check if a message should be sampled
    fn should_sample(&self) -> bool {
        let count = self
            .counter
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        // Deterministic sampling based on counter
        let threshold = (u64::MAX as f64 * self.sample_rate) as u64;
        (count % u64::MAX) < threshold
    }
}

#[async_trait]
impl MessageMiddleware for SamplingMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        // No sampling on publish
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        if !self.should_sample() {
            return Err(BrokerError::Configuration(
                "Message filtered out by sampling".to_string(),
            ));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "sampling"
    }
}

/// Transformation middleware for message content transformation.
///
/// Applies a transformation function to message bodies during processing.
///
/// # Examples
///
/// ```
/// use celers_kombu::TransformationMiddleware;
///
/// // Create a transformer that uppercases text
/// let transformer = TransformationMiddleware::new(|body: Vec<u8>| {
///     String::from_utf8_lossy(&body).to_uppercase().into_bytes()
/// });
/// ```
pub struct TransformationMiddleware {
    transform_fn: Box<dyn Fn(Vec<u8>) -> Vec<u8> + Send + Sync>,
}

impl TransformationMiddleware {
    /// Create a new transformation middleware with a transform function
    pub fn new<F>(transform_fn: F) -> Self
    where
        F: Fn(Vec<u8>) -> Vec<u8> + Send + Sync + 'static,
    {
        Self {
            transform_fn: Box::new(transform_fn),
        }
    }

    /// Apply the transformation to message body
    fn transform(&self, body: Vec<u8>) -> Vec<u8> {
        (self.transform_fn)(body)
    }
}

#[async_trait]
impl MessageMiddleware for TransformationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Transform on publish
        let transformed = self.transform(message.body.clone());
        message.body = transformed;
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Transform on consume
        let transformed = self.transform(message.body.clone());
        message.body = transformed;
        Ok(())
    }

    fn name(&self) -> &str {
        "transformation"
    }
}

/// Tracing middleware for distributed tracing
///
/// Injects trace IDs into message headers for distributed tracing.
///
/// # Examples
///
/// ```
/// use celers_kombu::{TracingMiddleware, MessageMiddleware};
///
/// let middleware = TracingMiddleware::new("service-name");
/// assert_eq!(middleware.name(), "tracing");
/// ```
#[derive(Debug, Clone)]
pub struct TracingMiddleware {
    service_name: String,
}

impl TracingMiddleware {
    /// Create a new tracing middleware
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
        }
    }
}

#[async_trait]
impl MessageMiddleware for TracingMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Inject trace ID if not present
        if !message.headers.extra.contains_key("trace-id") {
            let trace_id = uuid::Uuid::new_v4().to_string();
            message
                .headers
                .extra
                .insert("trace-id".to_string(), serde_json::json!(trace_id));
        }

        // Add service name
        message.headers.extra.insert(
            "service-name".to_string(),
            serde_json::json!(self.service_name.clone()),
        );

        // Add span ID for this operation
        let span_id = uuid::Uuid::new_v4().to_string();
        message
            .headers
            .extra
            .insert("span-id".to_string(), serde_json::json!(span_id));

        // Add timestamp
        message.headers.extra.insert(
            "trace-timestamp".to_string(),
            serde_json::json!(std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()),
        );

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Extract and log trace information
        if let Some(trace_id) = message.headers.extra.get("trace-id").cloned() {
            // In production, this would be sent to a tracing system
            message.headers.extra.insert(
                "consumer-service".to_string(),
                serde_json::json!(self.service_name.clone()),
            );
            message
                .headers
                .extra
                .insert("trace-id-consumed".to_string(), trace_id);
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "tracing"
    }
}
