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
        self.metrics
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[async_trait]
impl MessageMiddleware for MetricsMiddleware {
    async fn before_publish(&self, _message: &mut Message) -> Result<()> {
        let mut metrics = self.metrics.lock().unwrap_or_else(|e| e.into_inner());
        metrics.inc_published();
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        let mut metrics = self.metrics.lock().unwrap_or_else(|e| e.into_inner());
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
        // Check retry count from message headers. Celery's own check is
        // `if request.retries >= max_retries: raise MaxRetriesExceededError`
        // and `celers_protocol::retry::RetryPolicy::should_retry` likewise
        // uses `current_retries < self.max_retries` - i.e. a message whose
        // retry count already *equals* the configured maximum has used up
        // its last allowed attempt and must not be retried again. The
        // previous strict `>` comparison let exactly one extra attempt
        // through.
        let retries = message.headers.retries.unwrap_or(0);
        if retries >= self.max_retries {
            return Err(BrokerError::Configuration(format!(
                "Message exceeded maximum retries: {} >= {}",
                retries, self.max_retries
            )));
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "retry_limit"
    }

    fn is_drop_signal(&self, _err: &BrokerError) -> bool {
        // Once a message has exhausted its retry budget there is no value
        // in redelivering it again - it will just hit this same check
        // forever. Treat exceeding the limit as a designed drop rather
        // than a failure that should be requeued.
        true
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
        let mut bucket = self.tokens.lock().unwrap_or_else(|e| e.into_inner());
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
/// Internal deduplication cache state: a [`HashSet`](std::collections::HashSet)
/// for O(1) membership checks paired with a
/// [`VecDeque`](std::collections::VecDeque) recording true insertion order,
/// so eviction can remove the actual oldest entry instead of whatever a
/// `HashSet`'s unspecified iteration order happens to produce first.
struct DedupState {
    seen: std::collections::HashSet<Uuid>,
    order: std::collections::VecDeque<Uuid>,
}

pub struct DeduplicationMiddleware {
    /// Recently seen message IDs
    seen_ids: std::sync::Arc<std::sync::Mutex<DedupState>>,
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
            seen_ids: std::sync::Arc::new(std::sync::Mutex::new(DedupState {
                seen: std::collections::HashSet::new(),
                order: std::collections::VecDeque::new(),
            })),
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
        let mut state = self.seen_ids.lock().unwrap_or_else(|e| e.into_inner());

        // Check if we've seen this message before
        if state.seen.contains(&msg_id) {
            return Err(BrokerError::OperationFailed(format!(
                "Duplicate message detected: {}",
                msg_id
            )));
        }

        // Add to seen set, recording insertion order.
        state.seen.insert(msg_id);
        state.order.push_back(msg_id);

        // Evict true FIFO (oldest-inserted first) once the cache is too
        // large. A bare `HashSet` has no ordering, so evicting via
        // `seen.iter().next()` (the previous approach) could remove the ID
        // just inserted with probability 1/N and, more generally, dropped
        // recent IDs while ancient ones survived indefinitely.
        while state.order.len() > self.max_cache_size {
            if let Some(oldest) = state.order.pop_front() {
                state.seen.remove(&oldest);
            } else {
                break;
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "deduplication"
    }

    fn is_drop_signal(&self, _err: &BrokerError) -> bool {
        // A duplicate is duplicate forever - redelivering it will hit this
        // exact check again. This is the middleware's designed "skip this
        // message" signal, not a processing failure.
        true
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
    /// Header key used to record the compression encoding so the consumer
    /// can decompress the body with the matching codec.
    ///
    /// Namespaced under an `x-` prefix (matching
    /// [`ORIGINAL_CONTENT_ENCODING_HEADER`](Self::ORIGINAL_CONTENT_ENCODING_HEADER)
    /// and every other middleware-internal header in this crate) rather
    /// than the bare `content-encoding` name this middleware previously
    /// used: `headers.extra` is a general-purpose custom-headers bag that a
    /// producer may also populate with its own entries, and a plain
    /// `content-encoding` key was plausible enough for a producer to pick
    /// for an unrelated purpose that it collided with this middleware's own
    /// compression marker (`after_consume` would then treat that unrelated
    /// value as a compression codec name and attempt to decompress the body
    /// accordingly). The namespaced key makes collisions with
    /// application-chosen header names unlikely.
    ///
    /// `after_consume` also still *reads*
    /// [`LEGACY_COMPRESSION_HEADER`](Self::LEGACY_COMPRESSION_HEADER) as a
    /// fallback: `headers.extra` is `#[serde(flatten)]`-ed onto the wire,
    /// so a message published by a pre-rename build and still in flight
    /// (queued, or requeued after a failed delivery) during an upgrade
    /// carries the old key, not this one. Without the fallback, such a
    /// message would silently skip decompression instead of failing loudly.
    const COMPRESSION_HEADER: &'static str = "x-compression-encoding";
    /// Pre-rename compression-marker key (see
    /// [`COMPRESSION_HEADER`](Self::COMPRESSION_HEADER)). `before_publish`
    /// never writes this any more; `after_consume` still reads it so
    /// already-published messages keep decompressing correctly across the
    /// upgrade.
    const LEGACY_COMPRESSION_HEADER: &'static str = "content-encoding";
    /// Header key used to stash the message's pre-compression
    /// `content_encoding` field so it can be restored exactly on consume.
    const ORIGINAL_CONTENT_ENCODING_HEADER: &'static str = "x-original-content-encoding";

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
                let encoding = self.compressor.compression_type.as_encoding().to_string();

                // Stash the pre-compression `content_encoding` so
                // `after_consume` can restore it exactly, then mark the
                // envelope's own encoding field with the *real* wire
                // encoding of the (now compressed) body. Leaving
                // `content_encoding` untouched would have the published
                // envelope actively misdescribe its own body (e.g. still
                // claiming "utf-8" for gzip bytes), which any consumer
                // that trusts `content-encoding` - including a Python
                // Celery worker - would try to decode as text/JSON.
                message.headers.extra.insert(
                    Self::ORIGINAL_CONTENT_ENCODING_HEADER.to_string(),
                    serde_json::Value::String(message.content_encoding.clone()),
                );
                message.content_encoding = encoding.clone();

                message.body = compressed;
                // Record the algorithm used so the consumer can pick the
                // matching codec for decompression. We store the canonical
                // encoding name (e.g. "gzip") from the compressor's type.
                message.headers.extra.insert(
                    Self::COMPRESSION_HEADER.to_string(),
                    serde_json::Value::String(encoding),
                );
            }
        }
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Check whether this message was compressed on publish. If neither
        // flag is present the body was never compressed, so we leave it
        // untouched. The legacy key is checked second (a message using it
        // was published before the rename to the namespaced key above, and
        // `before_publish` on this build never writes it).
        let encoding = match message
            .headers
            .extra
            .get(Self::COMPRESSION_HEADER)
            .or_else(|| message.headers.extra.get(Self::LEGACY_COMPRESSION_HEADER))
        {
            Some(serde_json::Value::String(encoding)) => encoding.clone(),
            _ => return Ok(()),
        };

        // Resolve the codec from the recorded encoding name and decompress.
        let compression_type = celers_protocol::compression::CompressionType::from_encoding(
            &encoding,
        )
        .ok_or_else(|| {
            BrokerError::Serialization(format!("unknown compression encoding: {}", encoding))
        })?;

        let decompressed = celers_protocol::compression::Compressor::new(compression_type)
            .decompress(&message.body)
            .map_err(|e| BrokerError::Serialization(e.to_string()))?;

        message.body = decompressed;

        // Restore the pre-compression content_encoding exactly (falling
        // back to the well-known default only if the stash header is
        // somehow missing, e.g. a message compressed by an older version
        // of this middleware).
        let restored_encoding = message
            .headers
            .extra
            .remove(Self::ORIGINAL_CONTENT_ENCODING_HEADER)
            .and_then(|v| match v {
                serde_json::Value::String(s) => Some(s),
                _ => None,
            })
            .unwrap_or_else(|| "utf-8".to_string());
        message.content_encoding = restored_encoding;

        // Remove both possible flags so the consumed message is clean and
        // is not mistaken for a still-compressed payload by downstream
        // consumers, regardless of which key this particular message
        // happened to carry.
        message.headers.extra.remove(Self::COMPRESSION_HEADER);
        message
            .headers
            .extra
            .remove(Self::LEGACY_COMPRESSION_HEADER);

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
    /// Header key used to carry the hex-encoded HMAC signature of the body.
    const SIGNATURE_HEADER: &'static str = "signature";

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
        // Sign the message body and store the signature (hex-encoded) in the
        // headers so the consumer can verify integrity/authenticity.
        let signature_hex = self
            .signer
            .sign_hex(&message.body)
            .map_err(|e| BrokerError::OperationFailed(format!("signing failed: {}", e)))?;

        message.headers.extra.insert(
            Self::SIGNATURE_HEADER.to_string(),
            serde_json::Value::String(signature_hex),
        );

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Extract the signature recorded at publish time. A missing signature
        // is treated as a verification failure: we must not accept unsigned
        // messages through a signing middleware.
        let signature_hex = match message.headers.extra.get(Self::SIGNATURE_HEADER) {
            Some(serde_json::Value::String(signature)) => signature.clone(),
            _ => {
                return Err(BrokerError::OperationFailed(
                    "message is missing a signature header".to_string(),
                ));
            }
        };

        // Verify the signature against the body; reject on mismatch.
        self.signer
            .verify_hex(&message.body, &signature_hex)
            .map_err(|e| {
                BrokerError::OperationFailed(format!("signature verification failed: {}", e))
            })?;

        // Remove the signature header so the verified message is clean.
        message.headers.extra.remove(Self::SIGNATURE_HEADER);

        Ok(())
    }

    fn name(&self) -> &str {
        "signing"
    }
}

/// Encryption middleware - encrypts/decrypts message bodies at rest and in
/// transit using authenticated AES-256-GCM.
///
/// This middleware mirrors the round-trip contract used by
/// [`CompressionMiddleware`] and [`SigningMiddleware`]: on publish the body is
/// encrypted and the encryption scheme is recorded in the
/// [`Self::ENCRYPTION_HEADER`] header (alongside the per-message nonce); on
/// consume the body is decrypted and the marker headers are cleared so the
/// delivered message is plaintext and indistinguishable from one that never
/// passed through encryption.
///
/// Confidentiality is provided by AES-256-GCM and integrity/authenticity is
/// provided by the GCM authentication tag: any tampering with the ciphertext,
/// the nonce, or use of the wrong key causes [`Self::after_consume`] to fail
/// with a clear error instead of returning corrupted plaintext.
///
/// # Examples
///
/// ```
/// # #[cfg(feature = "encryption")]
/// # {
/// use celers_kombu::EncryptionMiddleware;
///
/// // 32-byte key for AES-256.
/// let key = [0u8; 32];
/// let middleware = EncryptionMiddleware::new(&key).expect("valid key");
/// # }
/// ```
#[cfg(feature = "encryption")]
pub struct EncryptionMiddleware {
    /// Message encryptor instance (AES-256-GCM).
    encryptor: celers_protocol::crypto::MessageEncryptor,
}

#[cfg(feature = "encryption")]
impl EncryptionMiddleware {
    /// Header key recording the encryption scheme applied to the body so the
    /// consumer knows the payload is ciphertext and which codec to use. The
    /// presence of this header is the marker that triggers decryption.
    pub const ENCRYPTION_HEADER: &'static str = "content-encryption";

    /// Header key carrying the hex-encoded per-message nonce required to
    /// decrypt the body. AES-GCM requires the exact nonce used during
    /// encryption; it is not secret but must be transmitted alongside the
    /// ciphertext.
    pub const NONCE_HEADER: &'static str = "content-encryption-nonce";

    /// Canonical name of the encryption scheme this middleware implements.
    pub const SCHEME: &'static str = "aes-256-gcm";

    /// Create a new encryption middleware.
    ///
    /// # Arguments
    ///
    /// * `key` - 32-byte secret key for AES-256
    ///
    /// # Returns
    ///
    /// `Ok(EncryptionMiddleware)` if the key is valid, `Err(BrokerError)`
    /// otherwise (e.g. the key is not exactly 32 bytes).
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
        // Encrypt the body and record the scheme + nonce in the headers so the
        // consumer can recognise the ciphertext and decrypt it. The nonce is
        // generated fresh for every message by the encryptor.
        let (ciphertext, nonce) = self
            .encryptor
            .encrypt(&message.body)
            .map_err(|e| BrokerError::Serialization(format!("encryption failed: {}", e)))?;

        message.body = ciphertext;
        message.headers.extra.insert(
            Self::ENCRYPTION_HEADER.to_string(),
            serde_json::Value::String(Self::SCHEME.to_string()),
        );
        message.headers.extra.insert(
            Self::NONCE_HEADER.to_string(),
            serde_json::Value::String(hex::encode(&nonce)),
        );

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Determine whether this message was encrypted on publish. Absence of
        // the scheme header means the body is plaintext and must be left
        // untouched (mirrors the compression no-op behaviour).
        let scheme = match message.headers.extra.get(Self::ENCRYPTION_HEADER) {
            Some(serde_json::Value::String(scheme)) => scheme.clone(),
            _ => return Ok(()),
        };

        // Only the scheme this middleware understands is supported; anything
        // else is a configuration/interoperability error.
        if scheme != Self::SCHEME {
            return Err(BrokerError::Serialization(format!(
                "unsupported encryption scheme: {}",
                scheme
            )));
        }

        // The nonce header is mandatory for an encrypted body; a missing nonce
        // means the message is malformed or was tampered with.
        let nonce_hex = match message.headers.extra.get(Self::NONCE_HEADER) {
            Some(serde_json::Value::String(nonce)) => nonce.clone(),
            _ => {
                return Err(BrokerError::Serialization(
                    "encrypted message is missing its nonce header".to_string(),
                ));
            }
        };

        let nonce = hex::decode(&nonce_hex)
            .map_err(|e| BrokerError::Serialization(format!("invalid nonce encoding: {}", e)))?;

        // Decrypt and authenticate. A wrong key, a tampered ciphertext, or a
        // tampered nonce all surface here as a GCM tag verification failure and
        // are rejected rather than returning corrupted plaintext.
        let plaintext = self
            .encryptor
            .decrypt(&message.body, &nonce)
            .map_err(|e| BrokerError::Serialization(format!("decryption failed: {}", e)))?;

        message.body = plaintext;

        // Clear the markers so the delivered message is clean plaintext and is
        // not mistaken for a still-encrypted payload downstream.
        message.headers.extra.remove(Self::ENCRYPTION_HEADER);
        message.headers.extra.remove(Self::NONCE_HEADER);

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

    fn is_drop_signal(&self, _err: &BrokerError) -> bool {
        // A message that fails the predicate today will fail it again on
        // redelivery (the predicate is a pure function of the message
        // content, which redelivery does not change) - this is the
        // middleware's designed "skip this message" signal, not a
        // processing failure that should be retried.
        true
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
        // Deterministic sampling based on counter: bucket the running
        // count into `RESOLUTION` slots and admit the message iff its
        // bucket falls within the leading `sample_rate` fraction of them.
        //
        // The previous formula computed `threshold = u64::MAX * rate` and
        // tested `count % u64::MAX < threshold`. Since `count % u64::MAX`
        // is just `count` for any realistic message count, and `threshold`
        // is astronomically larger than any realistic `count` for every
        // `rate` above ~5e-20, that predicate was true for essentially
        // every message regardless of `rate` (only `rate == 0.0` ever
        // filtered anything). Bucketing into a small, fixed resolution
        // keeps the comparison meaningful at real message volumes.
        const RESOLUTION: u64 = 10_000;
        let bucket = count % RESOLUTION;
        let threshold = (RESOLUTION as f64 * self.sample_rate).round() as u64;
        bucket < threshold
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

    fn is_drop_signal(&self, _err: &BrokerError) -> bool {
        // Statistical sampling is this middleware's designed "skip this
        // message" signal, not a processing failure. Without this override
        // a sampled-out message would be requeued (its `counter` bumped
        // again) rather than settled, defeating the point of sampling as a
        // load-reduction control.
        true
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
                .expect("SystemTime should be after UNIX_EPOCH")
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

#[cfg(test)]
mod hardening_tests {
    use super::*;

    // -------------------------------------------------------------------
    // idx112: SamplingMiddleware must actually sample at the configured
    // rate instead of admitting ~100% of messages for any non-zero rate.
    // -------------------------------------------------------------------

    #[test]
    fn sampling_admits_approximately_configured_fraction() {
        let sampler = SamplingMiddleware::new(0.1);
        let admitted = (0..10_000).filter(|_| sampler.should_sample()).count();
        // Deterministic (counter-based, not random): exactly 1000 of the
        // first 10,000 calls fall in the leading 10% bucket range.
        assert_eq!(admitted, 1000);
    }

    #[test]
    fn sampling_rate_one_admits_everything() {
        let sampler = SamplingMiddleware::new(1.0);
        assert!((0..5_000).all(|_| sampler.should_sample()));
    }

    #[test]
    fn sampling_rate_zero_admits_nothing() {
        let sampler = SamplingMiddleware::new(0.0);
        assert!((0..5_000).all(|_| !sampler.should_sample()));
    }

    // -------------------------------------------------------------------
    // idx122 (completeness): FilterMiddleware and SamplingMiddleware use
    // `Err` from `after_consume` as their designed "drop this message"
    // signal (see idx112 / the type docs), exactly like
    // `DeduplicationMiddleware` and `RetryLimitMiddleware` above. Without a
    // matching `is_drop_signal` override, `MiddlewareChain::process_after_consume`
    // treats that `Err` as a genuine failure, which
    // `MiddlewareConsumer::consume_with_middleware` requeues rather than
    // drops - turning a designed filter/sample-out into an infinite
    // redelivery loop instead of a settled drop.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn filter_rejection_is_settled_as_a_drop_not_a_failure() {
        let chain = crate::MiddlewareChain::new().with_middleware(Box::new(FilterMiddleware::new(
            |msg: &Message| msg.task_name() == "allowed",
        )));

        let mut message = Message::new("rejected_task".to_string(), Uuid::new_v4(), vec![]);
        let decision = chain.process_after_consume(&mut message).await.unwrap();
        assert!(decision.is_drop());
    }

    #[tokio::test]
    async fn sampling_rejection_is_settled_as_a_drop_not_a_failure() {
        let chain =
            crate::MiddlewareChain::new().with_middleware(Box::new(SamplingMiddleware::new(0.0)));

        let mut message = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        let decision = chain.process_after_consume(&mut message).await.unwrap();
        assert!(decision.is_drop());
    }

    // -------------------------------------------------------------------
    // idx152: DeduplicationMiddleware must evict the true oldest entry,
    // not an arbitrary HashSet-order one.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn deduplication_evicts_oldest_inserted_id_first() {
        let middleware = DeduplicationMiddleware::new(2);

        let id_a = Uuid::new_v4();
        let id_b = Uuid::new_v4();
        let id_c = Uuid::new_v4();

        let mut msg_a = Message::new("t".to_string(), id_a, vec![]);
        let mut msg_b = Message::new("t".to_string(), id_b, vec![]);
        let mut msg_c = Message::new("t".to_string(), id_c, vec![]);

        // Insert A, then B, then C (cache size is 2, so inserting C must
        // evict A - the oldest - not an arbitrary entry).
        middleware.after_consume(&mut msg_a).await.unwrap();
        middleware.after_consume(&mut msg_b).await.unwrap();
        middleware.after_consume(&mut msg_c).await.unwrap();

        // B and C are still tracked: redelivering either is still a
        // duplicate. Checked before touching A below, since a successful
        // (non-duplicate) `after_consume` call itself inserts into the
        // cache and would otherwise evict again.
        let mut msg_b_redelivered = Message::new("t".to_string(), id_b, vec![]);
        assert!(middleware
            .after_consume(&mut msg_b_redelivered)
            .await
            .is_err());
        let mut msg_c_redelivered = Message::new("t".to_string(), id_c, vec![]);
        assert!(middleware
            .after_consume(&mut msg_c_redelivered)
            .await
            .is_err());

        // A was evicted: redelivering it is no longer detected as a
        // duplicate.
        let mut msg_a_redelivered = Message::new("t".to_string(), id_a, vec![]);
        assert!(middleware
            .after_consume(&mut msg_a_redelivered)
            .await
            .is_ok());
    }

    // -------------------------------------------------------------------
    // idx153: RetryLimitMiddleware must reject once retries reach (not
    // just exceed) max_retries.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn retry_limit_rejects_at_exactly_max_retries() {
        let middleware = RetryLimitMiddleware::new(3);
        let mut message = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        message.headers.retries = Some(3);

        let result = middleware.after_consume(&mut message).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn retry_limit_allows_below_max_retries() {
        let middleware = RetryLimitMiddleware::new(3);
        let mut message = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        message.headers.retries = Some(2);

        let result = middleware.after_consume(&mut message).await;
        assert!(result.is_ok());
    }

    // -------------------------------------------------------------------
    // idx123: CompressionMiddleware must keep `content_encoding` in sync
    // with the actual wire encoding of the body.
    // -------------------------------------------------------------------

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn compression_updates_and_restores_content_encoding() {
        use celers_protocol::compression::CompressionType;

        let middleware = CompressionMiddleware::new(CompressionType::Gzip).with_min_size(16);
        let original_body = b"compress me ".repeat(64);
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), original_body.clone());
        let original_encoding = msg.content_encoding.clone();

        middleware.before_publish(&mut msg).await.unwrap();

        // The body is now compressed bytes, so `content_encoding` must no
        // longer claim the original (e.g. "utf-8") encoding.
        assert_ne!(msg.content_encoding, original_encoding);
        assert_eq!(msg.content_encoding, "gzip");

        middleware.after_consume(&mut msg).await.unwrap();

        // Round trip restores both the body and the original encoding.
        assert_eq!(msg.body, original_body);
        assert_eq!(msg.content_encoding, original_encoding);
    }

    // -------------------------------------------------------------------
    // idx123 residual: renaming `COMPRESSION_HEADER` off the collision-
    // prone bare "content-encoding" key must not silently break
    // decompression for a message that is still in flight (queued, or
    // requeued after a failed delivery) across the upgrade that ships the
    // rename.
    // -------------------------------------------------------------------

    #[cfg(feature = "compression")]
    #[tokio::test]
    async fn compression_after_consume_still_decompresses_legacy_marker_key() {
        use celers_protocol::compression::CompressionType;

        let middleware = CompressionMiddleware::new(CompressionType::Gzip).with_min_size(16);
        let original_body = b"compress me ".repeat(64);
        let mut msg = Message::new("test".to_string(), Uuid::new_v4(), original_body.clone());
        let original_encoding = msg.content_encoding.clone();

        middleware.before_publish(&mut msg).await.unwrap();
        assert_eq!(msg.content_encoding, "gzip");

        // Simulate a message published under the pre-rename marker key:
        // move the value `before_publish` just wrote under the new
        // (namespaced) key over to the old (bare) one, as if a build from
        // before the rename had published it.
        let encoding_value = msg
            .headers
            .extra
            .remove(CompressionMiddleware::COMPRESSION_HEADER)
            .expect("before_publish sets the marker header for a large enough body");
        msg.headers.extra.insert(
            CompressionMiddleware::LEGACY_COMPRESSION_HEADER.to_string(),
            encoding_value,
        );

        middleware.after_consume(&mut msg).await.unwrap();

        // Still decompresses correctly, and cleans up the legacy key too.
        assert_eq!(msg.body, original_body);
        assert_eq!(msg.content_encoding, original_encoding);
        assert!(!msg
            .headers
            .extra
            .contains_key(CompressionMiddleware::COMPRESSION_HEADER));
        assert!(!msg
            .headers
            .extra
            .contains_key(CompressionMiddleware::LEGACY_COMPRESSION_HEADER));
    }
}
