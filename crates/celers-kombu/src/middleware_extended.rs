//! Extended middleware implementations.

use async_trait::async_trait;
use celers_protocol::Message;
use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use uuid::Uuid;

use crate::middleware::effective_retries;
use crate::{BrokerError, MessageMiddleware, Result};

/// A minimal, dependency-free async sleep primitive.
///
/// `celers-kombu` intentionally keeps `tokio` out of its non-dev dependency
/// graph (it is pulled in only for tests/benches), so
/// [`ThrottlingMiddleware`] cannot use `tokio::time::sleep` directly.
/// `Delay` parks a dedicated OS thread for the requested duration and wakes
/// the polling task when it elapses - the same "don't block the executor"
/// behaviour a runtime-provided timer gives, without adding a dependency.
struct Delay {
    deadline: std::time::Instant,
    waiting: Option<std::thread::JoinHandle<()>>,
}

impl Delay {
    fn new(duration: Duration) -> Self {
        Self {
            deadline: std::time::Instant::now() + duration,
            waiting: None,
        }
    }
}

impl std::future::Future for Delay {
    type Output = ();

    fn poll(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<()> {
        let this = self.get_mut();
        let now = std::time::Instant::now();
        if now >= this.deadline {
            return std::task::Poll::Ready(());
        }

        if this.waiting.is_none() {
            let waker = cx.waker().clone();
            let remaining = this.deadline - now;
            this.waiting = Some(std::thread::spawn(move || {
                std::thread::sleep(remaining);
                waker.wake();
            }));
        }

        std::task::Poll::Pending
    }
}

/// Error classification middleware for intelligent error routing
///
/// This middleware classifies errors into categories and can route messages
/// to different queues based on error type (e.g., transient vs permanent errors).
///
/// # Examples
///
/// ```
/// use celers_kombu::ErrorClassificationMiddleware;
///
/// let classifier = ErrorClassificationMiddleware::new()
///     .with_transient_pattern("timeout|connection")
///     .with_permanent_pattern("validation|schema")
///     .with_max_transient_retries(5);
/// ```
pub struct ErrorClassificationMiddleware {
    transient_patterns: Vec<String>,
    permanent_patterns: Vec<String>,
    max_transient_retries: u32,
    max_permanent_retries: u32,
}

impl ErrorClassificationMiddleware {
    /// Create a new error classification middleware
    pub fn new() -> Self {
        Self {
            transient_patterns: vec![
                "timeout".to_string(),
                "connection".to_string(),
                "network".to_string(),
                "unavailable".to_string(),
            ],
            permanent_patterns: vec![
                "validation".to_string(),
                "schema".to_string(),
                "invalid".to_string(),
                "forbidden".to_string(),
            ],
            max_transient_retries: 10,
            max_permanent_retries: 1,
        }
    }

    /// Add a pattern for transient errors (can be retried)
    pub fn with_transient_pattern(mut self, pattern: &str) -> Self {
        self.transient_patterns.push(pattern.to_string());
        self
    }

    /// Add a pattern for permanent errors (should not be retried)
    pub fn with_permanent_pattern(mut self, pattern: &str) -> Self {
        self.permanent_patterns.push(pattern.to_string());
        self
    }

    /// Set maximum retries for transient errors
    pub fn with_max_transient_retries(mut self, max_retries: u32) -> Self {
        self.max_transient_retries = max_retries;
        self
    }

    /// Set maximum retries for permanent errors
    pub fn with_max_permanent_retries(mut self, max_retries: u32) -> Self {
        self.max_permanent_retries = max_retries;
        self
    }

    /// Classify an error message
    pub fn classify_error(&self, error_msg: &str) -> ErrorClass {
        let error_lower = error_msg.to_lowercase();

        // Check for permanent errors first (more specific)
        for pattern in &self.permanent_patterns {
            if error_lower.contains(&pattern.to_lowercase()) {
                return ErrorClass::Permanent;
            }
        }

        // Check for transient errors
        for pattern in &self.transient_patterns {
            if error_lower.contains(&pattern.to_lowercase()) {
                return ErrorClass::Transient;
            }
        }

        // Unknown errors are treated as transient by default
        ErrorClass::Unknown
    }

    /// Determine if a message should be retried based on error classification
    pub fn should_retry(&self, error_msg: &str, current_retries: u32) -> bool {
        match self.classify_error(error_msg) {
            ErrorClass::Transient => current_retries < self.max_transient_retries,
            ErrorClass::Permanent => current_retries < self.max_permanent_retries,
            ErrorClass::Unknown => current_retries < self.max_transient_retries,
        }
    }
}

impl Default for ErrorClassificationMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

/// Error classification categories
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorClass {
    /// Transient errors that should be retried (e.g., network timeouts)
    Transient,
    /// Permanent errors that should not be retried (e.g., validation errors)
    Permanent,
    /// Unknown error type (treated as transient by default)
    Unknown,
}

#[async_trait]
impl MessageMiddleware for ErrorClassificationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Check if message has error information
        if let Some(error_value) = message.headers.extra.get("error") {
            if let Some(error_msg) = error_value.as_str() {
                let error_class = self.classify_error(error_msg);
                let should_retry =
                    self.should_retry(error_msg, message.headers.retries.unwrap_or(0));

                message.headers.extra.insert(
                    "x-error-class".to_string(),
                    serde_json::json!(match error_class {
                        ErrorClass::Transient => "transient",
                        ErrorClass::Permanent => "permanent",
                        ErrorClass::Unknown => "unknown",
                    }),
                );

                message.headers.extra.insert(
                    "x-should-retry".to_string(),
                    serde_json::json!(should_retry),
                );

                if !should_retry {
                    message.headers.extra.insert(
                        "x-max-retries-exceeded".to_string(),
                        serde_json::json!(true),
                    );
                }
            }
        }
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "error_classification"
    }
}

/// Correlation middleware for distributed tracing
///
/// Automatically generates and propagates correlation IDs across service boundaries
/// for distributed tracing and request tracking.
///
/// # Examples
///
/// ```
/// use celers_kombu::CorrelationMiddleware;
///
/// let correlation = CorrelationMiddleware::new();
/// // Automatically generates correlation ID if not present
/// // Propagates existing correlation ID from headers
/// ```
pub struct CorrelationMiddleware {
    header_name: String,
}

impl CorrelationMiddleware {
    /// Create a new correlation middleware
    pub fn new() -> Self {
        Self {
            header_name: "x-correlation-id".to_string(),
        }
    }

    /// Create with custom header name
    pub fn with_header_name(header_name: &str) -> Self {
        Self {
            header_name: header_name.to_string(),
        }
    }

    fn get_or_generate_correlation_id(&self, message: &Message) -> String {
        message
            .headers
            .extra
            .get(&self.header_name)
            .and_then(|v| v.as_str())
            .map(|s| s.to_string())
            .unwrap_or_else(|| Uuid::new_v4().to_string())
    }
}

impl Default for CorrelationMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageMiddleware for CorrelationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let correlation_id = self.get_or_generate_correlation_id(message);
        message
            .headers
            .extra
            .insert(self.header_name.clone(), serde_json::json!(correlation_id));
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Ensure correlation ID is present for downstream processing
        let correlation_id = self.get_or_generate_correlation_id(message);
        message
            .headers
            .extra
            .insert(self.header_name.clone(), serde_json::json!(correlation_id));
        Ok(())
    }

    fn name(&self) -> &str {
        "correlation"
    }
}

/// Throttling middleware with backpressure support
///
/// Implements advanced throttling with configurable backpressure behavior.
/// Unlike rate limiting which rejects messages, throttling delays them.
///
/// # Examples
///
/// ```
/// use celers_kombu::ThrottlingMiddleware;
/// use std::time::Duration;
///
/// let throttle = ThrottlingMiddleware::new(100.0)  // 100 msg/sec
///     .with_burst_size(200)
///     .with_backpressure_threshold(0.8);
/// ```
pub struct ThrottlingMiddleware {
    pub(crate) max_rate: f64,
    pub(crate) burst_size: usize,
    pub(crate) backpressure_threshold: f64,
    last_refill: std::sync::Mutex<std::time::Instant>,
    available_tokens: std::sync::Mutex<f64>,
}

impl ThrottlingMiddleware {
    /// Create a new throttling middleware
    pub fn new(max_rate: f64) -> Self {
        Self {
            max_rate,
            burst_size: (max_rate * 2.0) as usize,
            backpressure_threshold: 0.8,
            last_refill: std::sync::Mutex::new(std::time::Instant::now()),
            available_tokens: std::sync::Mutex::new(max_rate),
        }
    }

    /// Set burst size (maximum tokens that can accumulate)
    pub fn with_burst_size(mut self, size: usize) -> Self {
        self.burst_size = size;
        self
    }

    /// Set backpressure threshold (0.0-1.0)
    pub fn with_backpressure_threshold(mut self, threshold: f64) -> Self {
        self.backpressure_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    fn refill_tokens(&self) {
        let mut last_refill = self.last_refill.lock().unwrap_or_else(|e| e.into_inner());
        let mut tokens = self
            .available_tokens
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        let now = std::time::Instant::now();
        let elapsed = now.duration_since(*last_refill).as_secs_f64();

        let new_tokens = elapsed * self.max_rate;
        *tokens = (*tokens + new_tokens).min(self.burst_size as f64);
        *last_refill = now;
    }

    fn calculate_delay(&self) -> Duration {
        self.refill_tokens();
        let tokens = self
            .available_tokens
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        if *tokens >= 1.0 {
            Duration::from_millis(0)
        } else {
            let wait_time = (1.0 - *tokens) / self.max_rate;
            Duration::from_secs_f64(wait_time)
        }
    }

    fn should_apply_backpressure(&self) -> bool {
        self.refill_tokens();
        let tokens = self
            .available_tokens
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        (*tokens / self.burst_size as f64) < (1.0 - self.backpressure_threshold)
    }
}

#[async_trait]
impl MessageMiddleware for ThrottlingMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let delay = self.calculate_delay();

        if delay > Duration::from_millis(0) {
            message.headers.extra.insert(
                "x-throttle-delay-ms".to_string(),
                serde_json::json!(delay.as_millis()),
            );
        }

        if self.should_apply_backpressure() {
            message
                .headers
                .extra
                .insert("x-backpressure-active".to_string(), serde_json::json!(true));
        }

        // Actually enforce the computed delay - previously this only
        // annotated the message with a delay hint that nothing honoured,
        // so messages were published at full rate regardless of
        // `max_rate`. No `std::sync::Mutex` guard is held across this
        // `.await` (`calculate_delay`/`should_apply_backpressure` above
        // already acquired and released their locks).
        if delay > Duration::from_millis(0) {
            Delay::new(delay).await;
        }

        // Refill again after the wait (more tokens may have accrued while
        // we slept) and consume one.
        self.refill_tokens();
        let mut tokens = self
            .available_tokens
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if *tokens >= 1.0 {
            *tokens -= 1.0;
        } else {
            // Floating point/refill-timing edge case immediately after
            // waiting exactly the computed delay: never go negative, but
            // still count this publish against the bucket so a burst of
            // concurrent publishers converges on the configured rate
            // rather than bypassing it.
            *tokens = 0.0;
        }

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed
        Ok(())
    }

    fn name(&self) -> &str {
        "throttling"
    }
}

/// Circuit breaker middleware for fault tolerance
///
/// Implements the circuit breaker pattern to prevent cascading failures.
/// Tracks failures and opens the circuit after a threshold is reached.
///
/// `before_publish` rejects while the circuit is open (enough failures
/// within `window`) and `after_consume` records a failure when it finds an
/// `"error"` key in `headers.extra` - a convention nothing in this
/// workspace sets automatically, since middleware hooks run before/after
/// the broker call rather than after your task handler executes. For
/// accurate tracking, call [`CircuitBreakerMiddleware::record_failure`]
/// directly from wherever your task's outcome is actually known; the
/// middleware is `Clone` and shares its state via an internal `Arc`, so a
/// clone kept by your worker and one installed in a
/// [`crate::MiddlewareChain`] observe the same circuit.
///
/// # Examples
///
/// ```
/// use celers_kombu::CircuitBreakerMiddleware;
/// use std::time::Duration;
///
/// let breaker = CircuitBreakerMiddleware::new(5, Duration::from_secs(60));
/// // Opens circuit after 5 failures within 60 seconds
/// ```
#[derive(Clone)]
pub struct CircuitBreakerMiddleware {
    pub(crate) failure_threshold: usize,
    window: Duration,
    failures: Arc<std::sync::Mutex<Vec<std::time::Instant>>>,
}

impl CircuitBreakerMiddleware {
    /// Create a new circuit breaker middleware
    pub fn new(failure_threshold: usize, window: Duration) -> Self {
        Self {
            failure_threshold,
            window,
            failures: Arc::new(std::sync::Mutex::new(Vec::new())),
        }
    }

    /// Record a failure explicitly.
    ///
    /// `after_consume` only records a failure when it finds an `"error"`
    /// key in `headers.extra` - a convention nothing in this workspace
    /// populates automatically, since the middleware trait's hooks run
    /// before/after the broker call rather than after task execution.
    /// Call this method directly (the middleware is `Clone` and shares its
    /// internal state via `Arc`, so a clone kept by your worker updates
    /// the same circuit as one installed in a [`crate::MiddlewareChain`])
    /// when your task handler reports a failure, for accurate tracking
    /// that doesn't depend on that header convention.
    pub fn record_failure(&self) {
        let mut failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        let now = std::time::Instant::now();

        // Remove old failures outside the window
        failures.retain(|&f| now.duration_since(f) < self.window);

        // Add new failure
        failures.push(now);
    }

    /// Clear all recorded failures, closing the circuit immediately.
    pub fn reset(&self) {
        self.failures
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    fn is_circuit_open(&self) -> bool {
        let mut failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        let now = std::time::Instant::now();

        // Clean up old failures
        failures.retain(|&f| now.duration_since(f) < self.window);

        failures.len() >= self.failure_threshold
    }

    fn get_failure_count(&self) -> usize {
        let mut failures = self.failures.lock().unwrap_or_else(|e| e.into_inner());
        let now = std::time::Instant::now();
        failures.retain(|&f| now.duration_since(f) < self.window);
        failures.len()
    }
}

#[async_trait]
impl MessageMiddleware for CircuitBreakerMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        if self.is_circuit_open() {
            message.headers.extra.insert(
                "x-circuit-breaker-open".to_string(),
                serde_json::json!(true),
            );
            message.headers.extra.insert(
                "x-circuit-breaker-failures".to_string(),
                serde_json::json!(self.get_failure_count()),
            );
            return Err(BrokerError::OperationFailed(
                "Circuit breaker is open".to_string(),
            ));
        }
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Check if message has error indicator
        if message.headers.extra.contains_key("error") {
            self.record_failure();
        }

        // Add circuit state to message
        message.headers.extra.insert(
            "x-circuit-breaker-failures".to_string(),
            serde_json::json!(self.get_failure_count()),
        );

        Ok(())
    }

    fn name(&self) -> &str {
        "circuit_breaker"
    }
}

/// Schema validation middleware - validates message structure and content
///
/// This middleware validates messages against configured rules before publishing
/// and after consuming.
///
/// # Examples
///
/// ```
/// use celers_kombu::SchemaValidationMiddleware;
///
/// let validator = SchemaValidationMiddleware::new()
///     .with_required_field("user_id")
///     .with_required_field("action")
///     .with_max_field_count(20);
/// ```
pub struct SchemaValidationMiddleware {
    pub(crate) required_fields: Vec<String>,
    pub(crate) max_field_count: Option<usize>,
    min_body_size: Option<usize>,
    pub(crate) max_body_size: Option<usize>,
}

impl SchemaValidationMiddleware {
    /// Create a new schema validation middleware
    pub fn new() -> Self {
        Self {
            required_fields: Vec::new(),
            max_field_count: None,
            min_body_size: None,
            max_body_size: None,
        }
    }

    /// Add a required field to validation
    pub fn with_required_field(mut self, field: impl Into<String>) -> Self {
        self.required_fields.push(field.into());
        self
    }

    /// Set maximum field count
    pub fn with_max_field_count(mut self, count: usize) -> Self {
        self.max_field_count = Some(count);
        self
    }

    /// Set minimum body size
    pub fn with_min_body_size(mut self, size: usize) -> Self {
        self.min_body_size = Some(size);
        self
    }

    /// Set maximum body size
    pub fn with_max_body_size(mut self, size: usize) -> Self {
        self.max_body_size = Some(size);
        self
    }

    fn validate_message(&self, message: &Message) -> Result<()> {
        // Check required fields
        for field in &self.required_fields {
            if !message.headers.extra.contains_key(field) {
                return Err(BrokerError::Configuration(format!(
                    "Missing required field: {}",
                    field
                )));
            }
        }

        // Check field count
        if let Some(max) = self.max_field_count {
            if message.headers.extra.len() > max {
                return Err(BrokerError::Configuration(format!(
                    "Too many fields: {} > {}",
                    message.headers.extra.len(),
                    max
                )));
            }
        }

        // Check body size
        let body_len = message.body.len();
        if let Some(min) = self.min_body_size {
            if body_len < min {
                return Err(BrokerError::Configuration(format!(
                    "Body too small: {} < {}",
                    body_len, min
                )));
            }
        }
        if let Some(max) = self.max_body_size {
            if body_len > max {
                return Err(BrokerError::Configuration(format!(
                    "Body too large: {} > {}",
                    body_len, max
                )));
            }
        }

        Ok(())
    }
}

impl Default for SchemaValidationMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageMiddleware for SchemaValidationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        self.validate_message(message)?;
        message
            .headers
            .extra
            .insert("x-schema-validated".to_string(), serde_json::json!(true));
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        self.validate_message(message)
    }

    fn name(&self) -> &str {
        "schema_validation"
    }
}

/// Message enrichment middleware - automatically adds metadata
///
/// This middleware enriches messages with contextual metadata such as
/// hostname, environment, version, and timestamps.
///
/// # Examples
///
/// ```
/// use celers_kombu::MessageEnrichmentMiddleware;
///
/// let enricher = MessageEnrichmentMiddleware::new()
///     .with_hostname("worker-01")
///     .with_environment("production")
///     .with_version("1.0.0")
///     .with_add_timestamp(true);
/// ```
pub struct MessageEnrichmentMiddleware {
    pub(crate) hostname: Option<String>,
    pub(crate) environment: Option<String>,
    pub(crate) version: Option<String>,
    pub(crate) add_timestamp: bool,
    custom_metadata: HashMap<String, serde_json::Value>,
}

impl MessageEnrichmentMiddleware {
    /// Create a new message enrichment middleware
    pub fn new() -> Self {
        Self {
            hostname: None,
            environment: None,
            version: None,
            add_timestamp: false,
            custom_metadata: HashMap::new(),
        }
    }

    /// Set hostname metadata
    pub fn with_hostname(mut self, hostname: impl Into<String>) -> Self {
        self.hostname = Some(hostname.into());
        self
    }

    /// Set environment metadata
    pub fn with_environment(mut self, environment: impl Into<String>) -> Self {
        self.environment = Some(environment.into());
        self
    }

    /// Set version metadata
    pub fn with_version(mut self, version: impl Into<String>) -> Self {
        self.version = Some(version.into());
        self
    }

    /// Enable timestamp enrichment
    pub fn with_add_timestamp(mut self, add: bool) -> Self {
        self.add_timestamp = add;
        self
    }

    /// Add custom metadata
    pub fn with_custom_metadata(
        mut self,
        key: impl Into<String>,
        value: serde_json::Value,
    ) -> Self {
        self.custom_metadata.insert(key.into(), value);
        self
    }

    fn enrich_message(&self, message: &mut Message) {
        if let Some(ref hostname) = self.hostname {
            message.headers.extra.insert(
                "x-enrichment-hostname".to_string(),
                serde_json::json!(hostname),
            );
        }

        if let Some(ref environment) = self.environment {
            message.headers.extra.insert(
                "x-enrichment-environment".to_string(),
                serde_json::json!(environment),
            );
        }

        if let Some(ref version) = self.version {
            message.headers.extra.insert(
                "x-enrichment-version".to_string(),
                serde_json::json!(version),
            );
        }

        if self.add_timestamp {
            let timestamp = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("SystemTime should be after UNIX_EPOCH")
                .as_secs();
            message.headers.extra.insert(
                "x-enrichment-timestamp".to_string(),
                serde_json::json!(timestamp),
            );
        }

        for (key, value) in &self.custom_metadata {
            message
                .headers
                .extra
                .insert(format!("x-enrichment-{}", key), value.clone());
        }
    }
}

impl Default for MessageEnrichmentMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageMiddleware for MessageEnrichmentMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        self.enrich_message(message);
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "message_enrichment"
    }
}

/// Retry strategy for intelligent retry handling
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryStrategy {
    /// Exponential backoff (delay doubles each time)
    Exponential,
    /// Linear backoff (delay increases by fixed amount)
    Linear,
    /// Fibonacci backoff (follows fibonacci sequence)
    Fibonacci,
    /// Fixed delay (constant delay between retries)
    Fixed,
}

/// Retry strategy middleware - implements different retry strategies
///
/// This middleware applies various retry strategies to failed messages,
/// calculating appropriate delays based on the retry count.
///
/// # Examples
///
/// ```
/// use celers_kombu::{RetryStrategyMiddleware, RetryStrategy};
/// use std::time::Duration;
///
/// let middleware = RetryStrategyMiddleware::new(RetryStrategy::Exponential)
///     .with_base_delay(Duration::from_secs(1))
///     .with_max_delay(Duration::from_secs(300))
///     .with_max_retries(5);
/// ```
pub struct RetryStrategyMiddleware {
    strategy: RetryStrategy,
    base_delay_ms: u64,
    max_delay_ms: u64,
    max_retries: u32,
}

impl RetryStrategyMiddleware {
    /// Create a new retry strategy middleware
    pub fn new(strategy: RetryStrategy) -> Self {
        Self {
            strategy,
            base_delay_ms: 1000,   // 1 second default
            max_delay_ms: 300_000, // 5 minutes default
            max_retries: 5,
        }
    }

    /// Set the base delay
    pub fn with_base_delay(mut self, delay: Duration) -> Self {
        self.base_delay_ms = delay.as_millis() as u64;
        self
    }

    /// Set the maximum delay
    pub fn with_max_delay(mut self, delay: Duration) -> Self {
        self.max_delay_ms = delay.as_millis() as u64;
        self
    }

    /// Set the maximum number of retries
    pub fn with_max_retries(mut self, retries: u32) -> Self {
        self.max_retries = retries;
        self
    }

    fn calculate_delay(&self, retry_count: u32) -> u64 {
        let delay = match self.strategy {
            RetryStrategy::Exponential => {
                // delay = base * 2^retry_count
                self.base_delay_ms * 2_u64.pow(retry_count)
            }
            RetryStrategy::Linear => {
                // delay = base * retry_count
                self.base_delay_ms * (retry_count as u64 + 1)
            }
            RetryStrategy::Fibonacci => {
                // Calculate fibonacci number for retry_count
                let fib = self.fibonacci(retry_count as usize);
                self.base_delay_ms * fib
            }
            RetryStrategy::Fixed => {
                // Always use base delay
                self.base_delay_ms
            }
        };

        delay.min(self.max_delay_ms)
    }

    fn fibonacci(&self, n: usize) -> u64 {
        match n {
            0 => 1,
            1 => 1,
            _ => {
                let mut a = 1u64;
                let mut b = 1u64;
                for _ in 2..=n {
                    let temp = a + b;
                    a = b;
                    b = temp;
                }
                b
            }
        }
    }
}

impl Default for RetryStrategyMiddleware {
    fn default() -> Self {
        Self::new(RetryStrategy::Exponential)
    }
}

#[async_trait]
impl MessageMiddleware for RetryStrategyMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Get retry count from the typed `headers.retries` field - the
        // field actually reserved for this purpose on the wire. The
        // previous `headers.extra["x-retry-count"]` lookup read a key
        // nothing in the workspace ever populates, so `retry_count` was
        // always 0 and the `max_retries` guard below could never fire.
        let retry_count = effective_retries(message);

        // Check if max retries exceeded
        if retry_count >= self.max_retries {
            return Err(BrokerError::OperationFailed(format!(
                "Max retries ({}) exceeded",
                self.max_retries
            )));
        }

        // Calculate and set delay
        let delay_ms = self.calculate_delay(retry_count);
        message
            .headers
            .extra
            .insert("x-retry-delay-ms".to_string(), serde_json::json!(delay_ms));
        message.headers.extra.insert(
            "x-retry-strategy".to_string(),
            serde_json::json!(format!("{:?}", self.strategy)),
        );

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "retry_strategy"
    }
}

/// Tenant isolation middleware - provides multi-tenancy support
///
/// This middleware enforces tenant isolation by validating and routing
/// messages based on tenant identifiers.
///
/// # Examples
///
/// ```
/// use celers_kombu::TenantIsolationMiddleware;
///
/// let middleware = TenantIsolationMiddleware::new()
///     .with_required_tenant(true)
///     .with_tenant_header("x-tenant-id");
/// ```
pub struct TenantIsolationMiddleware {
    required: bool,
    tenant_header: String,
    allowed_tenants: Option<Vec<String>>,
}

impl TenantIsolationMiddleware {
    /// Create a new tenant isolation middleware
    pub fn new() -> Self {
        Self {
            required: true,
            tenant_header: "x-tenant-id".to_string(),
            allowed_tenants: None,
        }
    }

    /// Set whether tenant ID is required
    pub fn with_required_tenant(mut self, required: bool) -> Self {
        self.required = required;
        self
    }

    /// Set the tenant header name
    pub fn with_tenant_header(mut self, header: impl Into<String>) -> Self {
        self.tenant_header = header.into();
        self
    }

    /// Set allowed tenants (whitelist)
    pub fn with_allowed_tenants(mut self, tenants: Vec<String>) -> Self {
        self.allowed_tenants = Some(tenants);
        self
    }

    fn validate_tenant(&self, tenant_id: Option<&str>) -> Result<()> {
        // Check if tenant is required but missing
        if self.required && tenant_id.is_none() {
            return Err(BrokerError::Configuration(format!(
                "Missing required tenant header: {}",
                self.tenant_header
            )));
        }

        // Check if tenant is in whitelist (if whitelist exists)
        if let (Some(tenant), Some(allowed)) = (tenant_id, &self.allowed_tenants) {
            if !allowed.contains(&tenant.to_string()) {
                return Err(BrokerError::Configuration(format!(
                    "Tenant '{}' not in allowed list",
                    tenant
                )));
            }
        }

        Ok(())
    }
}

impl Default for TenantIsolationMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageMiddleware for TenantIsolationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let tenant_id = message
            .headers
            .extra
            .get(&self.tenant_header)
            .and_then(|v| v.as_str());

        self.validate_tenant(tenant_id)?;

        // Add tenant validation marker
        message
            .headers
            .extra
            .insert("x-tenant-validated".to_string(), serde_json::json!(true));

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        let tenant_id = message
            .headers
            .extra
            .get(&self.tenant_header)
            .and_then(|v| v.as_str());

        self.validate_tenant(tenant_id)
    }

    fn name(&self) -> &str {
        "tenant_isolation"
    }
}

/// Partitioning middleware for distributed load balancing
///
/// Automatically assigns partition keys to messages for distributed processing.
/// Useful for ensuring related messages are processed by the same worker.
///
/// # Examples
///
/// ```
/// use celers_kombu::PartitioningMiddleware;
///
/// let partitioner = PartitioningMiddleware::new(8); // 8 partitions
/// assert_eq!(partitioner.partition_count(), 8);
/// ```
#[derive(Debug, Clone)]
pub struct PartitioningMiddleware {
    partition_count: usize,
    partition_header: String,
    partition_key_fn: Option<String>, // Field name to use for partitioning
}

impl PartitioningMiddleware {
    /// Create a new partitioning middleware with specified partition count
    pub fn new(partition_count: usize) -> Self {
        Self {
            partition_count: partition_count.max(1),
            partition_header: "x-partition-id".to_string(),
            partition_key_fn: None,
        }
    }

    /// Set the partition header name
    pub fn with_partition_header(mut self, header: impl Into<String>) -> Self {
        self.partition_header = header.into();
        self
    }

    /// Set the field name to use for partition key extraction
    pub fn with_partition_key_field(mut self, field: impl Into<String>) -> Self {
        self.partition_key_fn = Some(field.into());
        self
    }

    /// Get the partition count
    pub fn partition_count(&self) -> usize {
        self.partition_count
    }

    fn calculate_partition(&self, message: &Message) -> usize {
        // Try to extract partition key from specified field or use task ID
        let task_id_str = message.headers.id.to_string();
        let key = if let Some(field) = &self.partition_key_fn {
            message
                .headers
                .extra
                .get(field)
                .and_then(|v| v.as_str())
                .unwrap_or(&task_id_str)
        } else {
            // Default to task ID for partitioning
            &task_id_str
        };

        // Hash the key to determine partition. `std::collections::hash_map::
        // DefaultHasher` is explicitly documented as unstable across Rust
        // releases, so hashing the same key could silently route it to a
        // different partition/worker after a toolchain upgrade -- breaking
        // the "related messages go to the same partition" guarantee this
        // middleware exists to provide. `fnv1a_hash` has a fixed, versioned
        // definition that never changes, so the mapping from key to
        // partition is stable forever.
        let hash = crate::utils::fnv1a_hash(key.as_bytes());

        (hash % self.partition_count as u64) as usize
    }
}

impl Default for PartitioningMiddleware {
    fn default() -> Self {
        Self::new(4) // Default to 4 partitions
    }
}

#[async_trait]
impl MessageMiddleware for PartitioningMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let partition_id = self.calculate_partition(message);

        // Inject partition ID into message headers
        message.headers.extra.insert(
            self.partition_header.clone(),
            serde_json::json!(partition_id),
        );

        // Also add total partition count for consumer reference
        message.headers.extra.insert(
            "x-partition-count".to_string(),
            serde_json::json!(self.partition_count),
        );

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "partitioning"
    }
}

/// Adaptive timeout middleware with dynamic timeout adjustment
///
/// Adjusts message timeouts based on historical processing times.
/// Helps optimize resource usage and prevents premature timeouts.
///
/// # Examples
///
/// ```
/// use celers_kombu::AdaptiveTimeoutMiddleware;
/// use std::time::Duration;
///
/// let adaptive = AdaptiveTimeoutMiddleware::new(Duration::from_secs(30));
/// assert!(adaptive.has_samples() == false);
/// ```
#[derive(Debug, Clone)]
pub struct AdaptiveTimeoutMiddleware {
    base_timeout: Duration,
    min_timeout: Duration,
    max_timeout: Duration,
    samples: Arc<Mutex<VecDeque<u64>>>, // Processing times in milliseconds
    max_samples: usize,
    percentile: f64, // Use this percentile for timeout calculation (e.g., 0.95 for p95)
}

impl AdaptiveTimeoutMiddleware {
    /// Create a new adaptive timeout middleware
    pub fn new(base_timeout: Duration) -> Self {
        Self {
            base_timeout,
            min_timeout: Duration::from_secs(1),
            max_timeout: base_timeout.mul_f64(5.0), // Max 5x base timeout
            samples: Arc::new(Mutex::new(VecDeque::new())),
            max_samples: 100,
            percentile: 0.95, // Default to p95
        }
    }

    /// Set minimum timeout
    pub fn with_min_timeout(mut self, timeout: Duration) -> Self {
        self.min_timeout = timeout;
        self
    }

    /// Set maximum timeout
    pub fn with_max_timeout(mut self, timeout: Duration) -> Self {
        self.max_timeout = timeout;
        self
    }

    /// Set the percentile to use for timeout calculation
    pub fn with_percentile(mut self, percentile: f64) -> Self {
        self.percentile = percentile.clamp(0.0, 1.0);
        self
    }

    /// Check if we have collected samples
    pub fn has_samples(&self) -> bool {
        !self
            .samples
            .lock()
            .expect("adaptive timeout samples lock should not be poisoned")
            .is_empty()
    }

    /// Add a processing time sample (milliseconds), evicting the oldest when at capacity
    pub fn add_sample(&self, ms: u64) {
        let mut guard = self
            .samples
            .lock()
            .expect("adaptive timeout samples lock should not be poisoned");
        if guard.len() >= self.max_samples {
            guard.pop_front();
        }
        guard.push_back(ms);
    }

    /// Return the current number of collected samples
    pub fn sample_count(&self) -> usize {
        self.samples
            .lock()
            .expect("adaptive timeout samples lock should not be poisoned")
            .len()
    }

    /// Calculate adaptive timeout based on collected samples
    pub fn calculate_adaptive_timeout(&self) -> Duration {
        let guard = self
            .samples
            .lock()
            .expect("adaptive timeout samples lock should not be poisoned");
        if guard.is_empty() {
            return self.base_timeout;
        }
        let mut sorted_samples: Vec<u64> = guard.iter().copied().collect();
        drop(guard); // release lock before sorting

        sorted_samples.sort_unstable();

        let index = ((sorted_samples.len() as f64 * self.percentile) as usize)
            .min(sorted_samples.len() - 1);
        let timeout_ms = sorted_samples[index];

        // Add 20% buffer to the percentile value
        let buffered_ms = (timeout_ms as f64 * 1.2) as u64;

        let timeout = Duration::from_millis(buffered_ms);

        // Clamp to min/max bounds
        timeout.clamp(self.min_timeout, self.max_timeout)
    }
}

impl Default for AdaptiveTimeoutMiddleware {
    fn default() -> Self {
        Self::new(Duration::from_secs(30))
    }
}

#[async_trait]
impl MessageMiddleware for AdaptiveTimeoutMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let timeout = self.calculate_adaptive_timeout();

        // Inject adaptive timeout into message headers
        message.headers.extra.insert(
            "x-adaptive-timeout".to_string(),
            serde_json::json!(timeout.as_millis() as u64),
        );

        // Also add the percentile used for transparency
        message.headers.extra.insert(
            "x-timeout-percentile".to_string(),
            serde_json::json!(self.percentile),
        );

        // Record dispatch timestamp so after_consume can measure round-trip latency
        let dispatch_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        message.headers.extra.insert(
            "x-dispatch-at".to_string(),
            serde_json::json!(dispatch_at_ms),
        );

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        if let Some(dispatch_at) = message
            .headers
            .extra
            .get("x-dispatch-at")
            .and_then(|v| v.as_u64())
        {
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64;
            if now_ms >= dispatch_at {
                self.add_sample(now_ms - dispatch_at);
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "adaptive_timeout"
    }
}

#[cfg(test)]
mod hardening_tests {
    use super::*;

    // -------------------------------------------------------------------
    // idx119: ThrottlingMiddleware must actually delay publishes once the
    // token bucket is exhausted, not just annotate them.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn throttling_actually_delays_once_bucket_is_exhausted() {
        // 10 tokens/sec, burst of 1: the second publish must wait for a
        // token to regenerate (~100ms) rather than proceeding immediately.
        let middleware = ThrottlingMiddleware::new(10.0).with_burst_size(1);

        let mut msg1 = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        middleware.before_publish(&mut msg1).await.unwrap();

        let mut msg2 = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        let start = std::time::Instant::now();
        middleware.before_publish(&mut msg2).await.unwrap();
        let elapsed = start.elapsed();

        // Generous lower bound (well under the ~100ms expected wait) that
        // still clearly distinguishes "actually waited" from "returned
        // immediately", which is what the bug produced.
        assert!(
            elapsed >= Duration::from_millis(50),
            "expected before_publish to block for close to the refill \
             interval, only waited {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn throttling_does_not_delay_within_burst_capacity() {
        let middleware = ThrottlingMiddleware::new(1000.0).with_burst_size(50);
        let start = std::time::Instant::now();
        for _ in 0..10 {
            let mut msg = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
            middleware.before_publish(&mut msg).await.unwrap();
        }
        // Comfortably within burst capacity: must not have blocked at all.
        assert!(start.elapsed() < Duration::from_millis(50));
    }

    // -------------------------------------------------------------------
    // idx121: RetryStrategyMiddleware must read the typed retries field.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn retry_strategy_middleware_enforces_max_retries_from_typed_field() {
        let middleware = RetryStrategyMiddleware::new(RetryStrategy::Fixed).with_max_retries(3);
        let mut msg = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        msg.headers.retries = Some(3);

        // retries (3) >= max_retries (3): must reject rather than silently
        // computing another delay (the previous
        // `headers.extra["x-retry-count"]` lookup always read 0, so this
        // guard could never fire).
        let result = middleware.before_publish(&mut msg).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn retry_strategy_middleware_allows_below_max_retries() {
        let middleware = RetryStrategyMiddleware::new(RetryStrategy::Fixed).with_max_retries(3);
        let mut msg = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        msg.headers.retries = Some(2);

        let result = middleware.before_publish(&mut msg).await;
        assert!(result.is_ok());
    }

    // -------------------------------------------------------------------
    // idx141: CircuitBreakerMiddleware must expose a real, explicit
    // failure-recording API in addition to the header convention.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn circuit_breaker_record_failure_is_public_and_shared_across_clones() {
        let middleware = CircuitBreakerMiddleware::new(2, Duration::from_secs(60));
        let worker_handle = middleware.clone();

        // Simulate a worker recording task failures directly (not via the
        // `headers.extra["error"]` convention).
        worker_handle.record_failure();
        worker_handle.record_failure();

        // The clone installed in a chain observes the same circuit state
        // (shared via the internal `Arc`), so it now rejects.
        let mut msg = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        let result = middleware.before_publish(&mut msg).await;
        assert!(result.is_err());

        middleware.reset();
        let mut msg2 = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        assert!(middleware.before_publish(&mut msg2).await.is_ok());
    }

    // -------------------------------------------------------------------
    // idx151: PartitioningMiddleware must hash the partition key with a
    // toolchain-stable algorithm, not `DefaultHasher` (explicitly
    // documented as unstable across Rust releases - the same class of bug
    // already fixed for `generate_deduplication_id` in utils/mod.rs). A
    // pinned, independently-computed expected partition is the only way a
    // test can actually catch a regression back to an unstable hasher: an
    // assertion derived by calling this crate's own hashing code again
    // would keep passing even if the algorithm silently changed
    // underneath both call sites.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn partitioning_maps_a_known_key_to_a_pinned_partition() {
        // Independently computed from the standard FNV-1a algorithm (not
        // by calling this crate's own `fnv1a_hash`):
        // `fnv1a_hash(b"customer-42") == 0x1e1b8c70eb18f59e`, and
        // `0x1e1b8c70eb18f59e % 8 == 6`. If this ever changes, either the
        // hashing algorithm changed underneath `calculate_partition` (a
        // real regression) or the partition math changed (needs a
        // matching update here).
        let middleware = PartitioningMiddleware::new(8).with_partition_key_field("route");
        let mut message = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        message
            .headers
            .extra
            .insert("route".to_string(), serde_json::json!("customer-42"));

        middleware.before_publish(&mut message).await.unwrap();

        assert_eq!(
            message.headers.extra.get("x-partition-id"),
            Some(&serde_json::json!(6))
        );
    }

    #[tokio::test]
    async fn partitioning_same_key_maps_to_same_partition_across_instances() {
        // The property that actually matters in production: two
        // independently constructed middleware instances (standing in for
        // two different processes - e.g. a producer and a consumer, or
        // the same fleet before and after a rolling deploy) must agree on
        // the partition for the same key. A hasher whose output depends on
        // anything instance- or process-specific (unlike a fixed-
        // algorithm hash) would break this.
        let key = "order-service-shard-key";
        let mk_message = || {
            let mut message = Message::new("t".to_string(), Uuid::new_v4(), vec![]);
            message
                .headers
                .extra
                .insert("route".to_string(), serde_json::json!(key));
            message
        };

        let middleware_a = PartitioningMiddleware::new(16).with_partition_key_field("route");
        let mut message_a = mk_message();
        middleware_a.before_publish(&mut message_a).await.unwrap();

        let middleware_b = PartitioningMiddleware::new(16).with_partition_key_field("route");
        let mut message_b = mk_message();
        middleware_b.before_publish(&mut message_b).await.unwrap();

        assert_eq!(
            message_a.headers.extra.get("x-partition-id"),
            message_b.headers.extra.get("x-partition-id")
        );
    }
}
