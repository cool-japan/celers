//! Monitoring and operational middleware implementations.

use crate::middleware::effective_priority;
use crate::{BrokerError, BrokerMetrics, MessageMiddleware, Result};
use async_trait::async_trait;
use celers_protocol::extensions::MessageExt;
use celers_protocol::Message;
use std::collections::HashMap;

/// Batch acknowledgment hint middleware
///
/// Provides hints to consumers about optimal batch sizes for acknowledgments.
/// Helps optimize network round-trips and improve throughput.
///
/// # Examples
///
/// ```
/// use celers_kombu::BatchAckHintMiddleware;
///
/// let batch_hint = BatchAckHintMiddleware::new(10);
/// assert_eq!(batch_hint.batch_size(), 10);
/// ```
#[derive(Debug, Clone)]
pub struct BatchAckHintMiddleware {
    batch_size: usize,
    hint_header: String,
}

impl BatchAckHintMiddleware {
    /// Create a new batch acknowledgment hint middleware
    pub fn new(batch_size: usize) -> Self {
        Self {
            batch_size: batch_size.max(1),
            hint_header: "x-batch-ack-hint".to_string(),
        }
    }

    /// Set the hint header name
    pub fn with_hint_header(mut self, header: impl Into<String>) -> Self {
        self.hint_header = header.into();
        self
    }

    /// Get the batch size
    pub fn batch_size(&self) -> usize {
        self.batch_size
    }
}

impl Default for BatchAckHintMiddleware {
    fn default() -> Self {
        Self::new(10)
    }
}

#[async_trait]
impl MessageMiddleware for BatchAckHintMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Inject batch acknowledgment hint
        message
            .headers
            .extra
            .insert(self.hint_header.clone(), serde_json::json!(self.batch_size));

        // Add hint about whether batching is recommended
        message.headers.extra.insert(
            "x-batch-ack-recommended".to_string(),
            serde_json::json!(true),
        );

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // No action needed on consume
        Ok(())
    }

    fn name(&self) -> &str {
        "batch_ack_hint"
    }
}

/// Load shedding middleware for graceful degradation under pressure
///
/// Automatically drops low-priority messages when system load exceeds thresholds.
/// Helps maintain service stability during traffic spikes.
///
/// # Examples
///
/// ```
/// use celers_kombu::LoadSheddingMiddleware;
///
/// let load_shedder = LoadSheddingMiddleware::new(0.8); // 80% threshold
/// assert_eq!(load_shedder.threshold(), 0.8);
/// ```
/// Cloneable handle for updating a [`LoadSheddingMiddleware`]'s current
/// load estimate from outside a middleware chain.
///
/// [`crate::MiddlewareChain`] only ever hands out shared (`&self`)
/// references to its middlewares, so once a `LoadSheddingMiddleware` has
/// been boxed into a chain there is no way to reach a
/// `&mut LoadSheddingMiddleware` to call
/// [`LoadSheddingMiddleware::update_load`] on it directly (that method
/// still works when you hold the middleware itself, e.g. before installing
/// it - this handle is for the common case where you don't). Retain a
/// `LoadHandle` via [`LoadSheddingMiddleware::load_handle`] *before*
/// installing the middleware into a chain, and update it from wherever the
/// real load signal is observed (a periodic queue-depth sampler, CPU load
/// average, etc.).
#[derive(Debug, Clone)]
pub struct LoadHandle {
    current_load: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl LoadHandle {
    /// Update the load estimate (clamped to `0.0..=1.0`).
    pub fn set_load(&self, load: f64) {
        self.current_load.store(
            load.clamp(0.0, 1.0).to_bits(),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Read the current load estimate.
    pub fn load(&self) -> f64 {
        f64::from_bits(self.current_load.load(std::sync::atomic::Ordering::Relaxed))
    }
}

#[derive(Debug, Clone)]
pub struct LoadSheddingMiddleware {
    load_threshold: f64, // Threshold for load shedding (0.0-1.0)
    priority_cutoff: u8, // Drop messages below this priority
    /// Current system load estimate. Behind an `Arc<AtomicU64>` (storing
    /// the `f64` bits) rather than a plain `f64` field so it can be
    /// updated via a cloneable [`LoadHandle`] after the middleware has
    /// been boxed into a chain - see [`Self::load_handle`].
    current_load: std::sync::Arc<std::sync::atomic::AtomicU64>,
}

impl LoadSheddingMiddleware {
    /// Create a new load shedding middleware
    pub fn new(load_threshold: f64) -> Self {
        Self {
            load_threshold: load_threshold.clamp(0.0, 1.0),
            priority_cutoff: 3, // Default: drop priority < 3 (Low and below)
            current_load: std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0.0f64.to_bits())),
        }
    }

    /// Set the priority cutoff
    pub fn with_priority_cutoff(mut self, cutoff: u8) -> Self {
        self.priority_cutoff = cutoff.min(10);
        self
    }

    /// Update current load estimate.
    ///
    /// Requires a direct `&mut LoadSheddingMiddleware`, i.e. this only
    /// works before the middleware has been boxed into a
    /// [`crate::MiddlewareChain`] (which only ever hands out `&self`
    /// afterwards). Get a [`LoadHandle`] via [`Self::load_handle`] first
    /// if you need to keep updating the load once it's installed.
    pub fn update_load(&mut self, load: f64) {
        self.current_load.store(
            load.clamp(0.0, 1.0).to_bits(),
            std::sync::atomic::Ordering::Relaxed,
        );
    }

    /// Get a cloneable handle for updating the load estimate from outside
    /// a middleware chain. See [`LoadHandle`].
    pub fn load_handle(&self) -> LoadHandle {
        LoadHandle {
            current_load: std::sync::Arc::clone(&self.current_load),
        }
    }

    /// Get the load threshold
    pub fn threshold(&self) -> f64 {
        self.load_threshold
    }

    /// Read the current load estimate.
    fn current_load(&self) -> f64 {
        f64::from_bits(self.current_load.load(std::sync::atomic::Ordering::Relaxed))
    }

    /// Check if message should be dropped
    fn should_shed(&self, priority: u8) -> bool {
        self.current_load() > self.load_threshold && priority < self.priority_cutoff
    }
}

impl Default for LoadSheddingMiddleware {
    fn default() -> Self {
        Self::new(0.8)
    }
}

#[async_trait]
impl MessageMiddleware for LoadSheddingMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Prefer the typed `properties.priority` field (populated by
        // `Message::with_priority` / the builder - the field real
        // priority-queue code reads) falling back to a legacy
        // `headers.extra["priority"]` marker.
        let priority = effective_priority(message, 5);

        if self.should_shed(priority) {
            // Inject load shedding marker
            message
                .headers
                .extra
                .insert("x-load-shed".to_string(), serde_json::json!(true));
            message.headers.extra.insert(
                "x-current-load".to_string(),
                serde_json::json!(self.current_load()),
            );

            return Err(BrokerError::OperationFailed(format!(
                "Load shedding: current load {:.2} exceeds threshold {:.2}",
                self.current_load(),
                self.load_threshold
            )));
        }

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        "load_shedding"
    }
}

/// Message priority escalation middleware
///
/// Automatically escalates message priority based on age and retry count.
/// Prevents message starvation in priority queues.
///
/// # Examples
///
/// ```
/// use celers_kombu::MessagePriorityEscalationMiddleware;
///
/// let escalator = MessagePriorityEscalationMiddleware::new(300); // 5 min threshold
/// assert_eq!(escalator.age_threshold_secs(), 300);
/// ```
#[derive(Debug, Clone)]
pub struct MessagePriorityEscalationMiddleware {
    age_threshold_secs: u64, // Age threshold for escalation
    escalation_step: u8,     // Priority increase per threshold
    max_priority: u8,        // Maximum priority (cap)
    escalate_on_retry: bool, // Also escalate based on retry count
}

impl MessagePriorityEscalationMiddleware {
    /// Create a new priority escalation middleware
    pub fn new(age_threshold_secs: u64) -> Self {
        Self {
            age_threshold_secs,
            escalation_step: 1,
            max_priority: 10,
            escalate_on_retry: true,
        }
    }

    /// Set escalation step
    pub fn with_escalation_step(mut self, step: u8) -> Self {
        self.escalation_step = step.max(1);
        self
    }

    /// Set maximum priority
    pub fn with_max_priority(mut self, max: u8) -> Self {
        self.max_priority = max.min(10);
        self
    }

    /// Set whether to escalate on retry
    pub fn with_escalate_on_retry(mut self, enable: bool) -> Self {
        self.escalate_on_retry = enable;
        self
    }

    /// Get age threshold
    pub fn age_threshold_secs(&self) -> u64 {
        self.age_threshold_secs
    }

    /// Calculate escalated priority
    fn calculate_priority(&self, base_priority: u8, age_secs: u64, retries: u32) -> u8 {
        let mut priority = base_priority;

        // Age-based escalation. `age_secs` now reflects a message's real
        // elapsed age (previously always 0, so this branch was dead) and
        // can therefore grow without bound over a message's lifetime;
        // guard the arithmetic accordingly rather than assuming small,
        // hand-picked test inputs:
        //  - `age_threshold_secs == 0` would otherwise divide by zero.
        //  - `age_multiplier * escalation_step` (both derived from
        //    unbounded `age_secs`) could otherwise overflow `u8` for a
        //    sufficiently old message.
        if self.age_threshold_secs > 0 && age_secs >= self.age_threshold_secs {
            let age_multiplier = (age_secs / self.age_threshold_secs).min(u8::MAX as u64) as u8;
            priority = priority.saturating_add(age_multiplier.saturating_mul(self.escalation_step));
        }

        // Retry-based escalation
        if self.escalate_on_retry && retries > 0 {
            let retry_boost = (retries as u8).min(3); // Cap retry boost at 3
            priority = priority.saturating_add(retry_boost);
        }

        priority.min(self.max_priority)
    }
}

impl Default for MessagePriorityEscalationMiddleware {
    fn default() -> Self {
        Self::new(300) // 5 minutes
    }
}

impl MessagePriorityEscalationMiddleware {
    /// Escalate `message`'s priority based on its real age and retry
    /// count, stamping the escalation markers if it changed.
    ///
    /// Run from both `before_publish` and `after_consume` (see the trait
    /// impl below for why): a message can accrue age either while
    /// buffered before its first publish attempt, or while sitting in the
    /// queue between deliveries, and either point is a meaningful moment
    /// to escalate a starved message's priority before it is next
    /// enqueued/requeued.
    fn escalate(&self, message: &mut Message) {
        let base_priority = effective_priority(message, 5);
        // A message's age is available from its real `created_at`
        // (`get_age_seconds`, always populated by `Message::new`) without
        // needing any special wiring. This used to be hard-coded to 0,
        // which disabled the entire age-based escalation branch of
        // `calculate_priority` and silently ignored `age_threshold_secs`.
        let age_secs = message.get_age_seconds().unwrap_or(0).max(0) as u64;
        let retries = message.headers.retries.unwrap_or(0);

        let new_priority = self.calculate_priority(base_priority, age_secs, retries);

        if new_priority != base_priority {
            // Write to both the typed field (so real priority-queue /
            // broker code observes it) and the legacy header (backward
            // compatible with anything still reading it there).
            message.properties.priority = Some(new_priority);
            message
                .headers
                .extra
                .insert("priority".to_string(), serde_json::json!(new_priority));
            message
                .headers
                .extra
                .insert("x-priority-escalated".to_string(), serde_json::json!(true));
            message.headers.extra.insert(
                "x-original-priority".to_string(),
                serde_json::json!(base_priority),
            );
        }
    }
}

#[async_trait]
impl MessageMiddleware for MessagePriorityEscalationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // A message being published for the first time has an age of
        // approximately zero by definition, so in the common case only
        // the retry-count term of `calculate_priority` has any effect
        // here. Age escalation only meaningfully fires when a message is
        // being republished after having been buffered for a while (its
        // `created_at` predates this call) - the more common,
        // architecturally correct case (a message that aged out while
        // sitting in the queue between delivery attempts) is handled by
        // running the same escalation in `after_consume` below.
        self.escalate(message);
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // This is where a message's queue residency is actually
        // observable: `age_secs` here reflects real time spent waiting
        // since `created_at`, and `retries` reflects delivery attempts
        // actually made. If this particular delivery fails and the
        // message is requeued, the escalated priority applies to its next
        // position in the queue, which is exactly what prevents
        // starvation.
        self.escalate(message);
        Ok(())
    }

    fn name(&self) -> &str {
        "priority_escalation"
    }
}

/// Observability middleware for structured logging and metrics
///
/// Provides structured logging (to stderr, in the same style as
/// [`crate::LoggingMiddleware`]) and, when attached via
/// [`Self::with_metrics`], metrics export via a shared [`BrokerMetrics`]
/// (the same counters [`crate::MetricsMiddleware`] uses). `celers-kombu`
/// deliberately has no dependency on a specific tracing/metrics framework
/// (e.g. `tracing`, Prometheus client libraries), so this middleware
/// integrates with what the crate already has rather than pulling one in;
/// see the crate docs for wiring a real exporter downstream of
/// `BrokerMetrics`.
///
/// # Examples
///
/// ```
/// use celers_kombu::ObservabilityMiddleware;
///
/// let observability = ObservabilityMiddleware::new("my-service");
/// assert_eq!(observability.service_name(), "my-service");
/// ```
#[derive(Debug, Clone)]
pub struct ObservabilityMiddleware {
    service_name: String,
    enable_metrics: bool,
    enable_logging: bool,
    log_level: String,
    metrics: Option<std::sync::Arc<std::sync::Mutex<BrokerMetrics>>>,
}

impl ObservabilityMiddleware {
    /// Create a new observability middleware
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            enable_metrics: true,
            enable_logging: true,
            log_level: "info".to_string(),
            metrics: None,
        }
    }

    /// Disable metrics collection
    pub fn without_metrics(mut self) -> Self {
        self.enable_metrics = false;
        self
    }

    /// Disable logging
    pub fn without_logging(mut self) -> Self {
        self.enable_logging = false;
        self
    }

    /// Set log level
    pub fn with_log_level(mut self, level: impl Into<String>) -> Self {
        self.log_level = level.into();
        self
    }

    /// Attach a shared [`BrokerMetrics`] to increment on every publish/
    /// consume (gated on [`Self::without_metrics`] not having been
    /// called). Share the same `Arc<Mutex<BrokerMetrics>>` with a
    /// [`crate::MetricsMiddleware`] to combine both under one counter set.
    pub fn with_metrics(
        mut self,
        metrics: std::sync::Arc<std::sync::Mutex<BrokerMetrics>>,
    ) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Get service name
    pub fn service_name(&self) -> &str {
        &self.service_name
    }

    fn log(&self, event: &str, message: &Message) {
        if !self.enable_logging {
            return;
        }
        eprintln!(
            "[{}] level={} event={} task={} id={} body_size={}",
            self.service_name,
            self.log_level,
            event,
            message.task_name(),
            message.task_id(),
            message.body.len()
        );
    }
}

impl Default for ObservabilityMiddleware {
    fn default() -> Self {
        Self::new("unknown-service")
    }
}

#[async_trait]
impl MessageMiddleware for ObservabilityMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        if self.enable_metrics {
            message.headers.extra.insert(
                "x-observability-enabled".to_string(),
                serde_json::json!(true),
            );
            if let Some(ref metrics) = self.metrics {
                metrics
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .inc_published();
            }
        }

        if self.enable_logging {
            message
                .headers
                .extra
                .insert("x-log-level".to_string(), serde_json::json!(self.log_level));
        }

        message.headers.extra.insert(
            "x-service-name".to_string(),
            serde_json::json!(self.service_name),
        );

        self.log("publish", message);

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // This is the point at which consumption outcome/latency is
        // actually observable, which is exactly what was previously a
        // no-op: neither a log line nor a metric was ever emitted here.
        self.log("consume", message);

        if self.enable_metrics {
            if let Some(ref metrics) = self.metrics {
                metrics
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .inc_consumed();
            }
        }

        Ok(())
    }

    fn name(&self) -> &str {
        "observability"
    }
}

/// Health check middleware - automatic health status tracking
///
/// # Examples
///
/// ```
/// use celers_kombu::HealthCheckMiddleware;
///
/// // Basic health check middleware
/// let health_check = HealthCheckMiddleware::new();
///
/// // With custom health check interval
/// let custom_health = HealthCheckMiddleware::new()
///     .with_check_interval_secs(30);
/// ```
pub struct HealthCheckMiddleware {
    /// Last health check timestamp (seconds since epoch)
    last_check: std::sync::Arc<std::sync::Mutex<u64>>,
    /// Health check interval in seconds
    check_interval_secs: u64,
    /// Health status
    is_healthy: std::sync::Arc<std::sync::Mutex<bool>>,
}

impl HealthCheckMiddleware {
    /// Create a new health check middleware
    pub fn new() -> Self {
        Self {
            last_check: std::sync::Arc::new(std::sync::Mutex::new(0)),
            check_interval_secs: 60, // Default: 1 minute
            is_healthy: std::sync::Arc::new(std::sync::Mutex::new(true)),
        }
    }

    /// Set health check interval
    pub fn with_check_interval_secs(mut self, interval: u64) -> Self {
        self.check_interval_secs = interval;
        self
    }

    /// Get current health status
    pub fn is_healthy(&self) -> bool {
        *self.is_healthy.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Mark as unhealthy
    pub fn mark_unhealthy(&self) {
        *self.is_healthy.lock().unwrap_or_else(|e| e.into_inner()) = false;
    }

    /// Mark as healthy
    pub fn mark_healthy(&self) {
        *self.is_healthy.lock().unwrap_or_else(|e| e.into_inner()) = true;
    }

    fn should_check(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("SystemTime should be after UNIX_EPOCH")
            .as_secs();
        let last = *self.last_check.lock().unwrap_or_else(|e| e.into_inner());
        now - last >= self.check_interval_secs
    }

    fn update_check_time(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("SystemTime should be after UNIX_EPOCH")
            .as_secs();
        *self.last_check.lock().unwrap_or_else(|e| e.into_inner()) = now;
    }

    /// Run the periodic health evaluation and return the resulting status.
    ///
    /// The check is throttled by `check_interval_secs`: it only re-evaluates
    /// once the interval has elapsed, otherwise it returns the last known
    /// status. The current health is derived from the manually controllable
    /// `is_healthy` flag (set via [`mark_healthy`](Self::mark_healthy) /
    /// [`mark_unhealthy`](Self::mark_unhealthy)), which broker integrations
    /// can drive from real connectivity probes.
    fn run_health_check(&self) -> &'static str {
        if self.should_check() {
            self.update_check_time();
        }

        if self.is_healthy() {
            "healthy"
        } else {
            "unhealthy"
        }
    }
}

impl Default for HealthCheckMiddleware {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl MessageMiddleware for HealthCheckMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Evaluate (throttled) health and stamp the message so downstream
        // consumers and monitoring can observe producer health alongside the
        // time the check was performed.
        let health_status = self.run_health_check();
        let checked_at = *self.last_check.lock().unwrap_or_else(|e| e.into_inner());

        message.headers.extra.insert(
            "x-health-status".to_string(),
            serde_json::json!(health_status),
        );
        message.headers.extra.insert(
            "x-health-checked-at".to_string(),
            serde_json::json!(checked_at),
        );
        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // Health check on consume could validate broker connectivity
        Ok(())
    }

    fn name(&self) -> &str {
        "health_check"
    }
}

/// Message tagging middleware - automatic message tagging and categorization
///
/// # Examples
///
/// ```
/// use celers_kombu::MessageTaggingMiddleware;
/// use std::collections::HashMap;
///
/// // Basic tagging with environment
/// let tagging = MessageTaggingMiddleware::new("production");
///
/// // With custom tags
/// let mut tags = HashMap::new();
/// tags.insert("region".to_string(), "us-east-1".to_string());
/// tags.insert("team".to_string(), "platform".to_string());
/// let custom_tagging = MessageTaggingMiddleware::new("production")
///     .with_tags(tags);
/// ```
pub struct MessageTaggingMiddleware {
    /// Environment tag (e.g., "production", "staging")
    environment: String,
    /// Additional custom tags
    tags: HashMap<String, String>,
}

impl MessageTaggingMiddleware {
    /// Create a new message tagging middleware
    pub fn new(environment: impl Into<String>) -> Self {
        Self {
            environment: environment.into(),
            tags: HashMap::new(),
        }
    }

    /// Add custom tags
    pub fn with_tags(mut self, tags: HashMap<String, String>) -> Self {
        self.tags = tags;
        self
    }

    /// Add a single tag
    pub fn with_tag(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.tags.insert(key.into(), value.into());
        self
    }
}

#[async_trait]
impl MessageMiddleware for MessageTaggingMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Inject environment tag
        message.headers.extra.insert(
            "x-environment".to_string(),
            serde_json::json!(self.environment.clone()),
        );

        // Inject custom tags
        for (key, value) in &self.tags {
            message
                .headers
                .extra
                .insert(format!("x-tag-{}", key), serde_json::json!(value.clone()));
        }

        // Auto-categorize based on task name
        let category = if message.task_name().contains("email") {
            "communication"
        } else if message.task_name().contains("report") {
            "analytics"
        } else if message.task_name().contains("process") {
            "computation"
        } else {
            "general"
        };
        message
            .headers
            .extra
            .insert("x-category".to_string(), serde_json::json!(category));

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // Tags are already present after consumption
        Ok(())
    }

    fn name(&self) -> &str {
        "message_tagging"
    }
}

/// Cost attribution middleware - track costs per tenant/project
///
/// # Examples
///
/// ```
/// use celers_kombu::CostAttributionMiddleware;
///
/// // Basic cost attribution
/// let cost_attr = CostAttributionMiddleware::new(0.001); // $0.001 per message
///
/// // With custom cost factors
/// let advanced_cost = CostAttributionMiddleware::new(0.001)
///     .with_compute_cost_per_sec(0.0001)   // $0.0001 per second
///     .with_storage_cost_per_mb(0.00001);  // $0.00001 per MB
/// ```
pub struct CostAttributionMiddleware {
    /// Base cost per message (in dollars)
    message_cost: f64,
    /// Compute cost per second (in dollars)
    compute_cost_per_sec: f64,
    /// Storage cost per MB (in dollars)
    storage_cost_per_mb: f64,
}

impl CostAttributionMiddleware {
    /// Create a new cost attribution middleware
    pub fn new(message_cost: f64) -> Self {
        Self {
            message_cost,
            compute_cost_per_sec: 0.0,
            storage_cost_per_mb: 0.0,
        }
    }

    /// Set compute cost per second
    pub fn with_compute_cost_per_sec(mut self, cost: f64) -> Self {
        self.compute_cost_per_sec = cost;
        self
    }

    /// Set storage cost per MB
    pub fn with_storage_cost_per_mb(mut self, cost: f64) -> Self {
        self.storage_cost_per_mb = cost;
        self
    }

    fn calculate_cost(&self, message: &Message) -> f64 {
        let mut cost = self.message_cost;

        // Add storage cost based on message size
        let size_mb = message.body.len() as f64 / (1024.0 * 1024.0);
        cost += size_mb * self.storage_cost_per_mb;

        cost
    }
}

#[async_trait]
impl MessageMiddleware for CostAttributionMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Calculate and inject cost
        let cost = self.calculate_cost(message);
        message.headers.extra.insert(
            "x-cost-estimate".to_string(),
            serde_json::json!(format!("{:.6}", cost)),
        );

        // Extract tenant/project from message headers if available
        let tenant = message
            .headers
            .extra
            .get("x-tenant")
            .and_then(|v| v.as_str())
            .or_else(|| message.headers.extra.get("tenant").and_then(|v| v.as_str()))
            .unwrap_or("default")
            .to_string();

        message
            .headers
            .extra
            .insert("x-cost-tenant".to_string(), serde_json::json!(tenant));

        // Inject timestamp for cost tracking
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("SystemTime should be after UNIX_EPOCH")
            .as_secs();
        message.headers.extra.insert(
            "x-cost-timestamp".to_string(),
            serde_json::json!(timestamp.to_string()),
        );

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Calculate actual compute cost if processing time is available
        if let Some(cost_timestamp) = message.headers.extra.get("x-cost-timestamp") {
            if let Some(timestamp_str) = cost_timestamp.as_str() {
                if let Ok(start_time) = timestamp_str.parse::<u64>() {
                    let now = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .expect("SystemTime should be after UNIX_EPOCH")
                        .as_secs();
                    let duration_secs = (now - start_time) as f64;
                    let compute_cost = duration_secs * self.compute_cost_per_sec;

                    if let Some(base_cost) = message.headers.extra.get("x-cost-estimate") {
                        if let Some(base_str) = base_cost.as_str() {
                            if let Ok(base) = base_str.parse::<f64>() {
                                let total_cost = base + compute_cost;
                                message.headers.extra.insert(
                                    "x-cost-actual".to_string(),
                                    serde_json::json!(format!("{:.6}", total_cost)),
                                );
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "cost_attribution"
    }
}

/// SLA monitoring middleware - track and enforce SLA requirements
///
/// # Examples
///
/// ```
/// use celers_kombu::SLAMonitoringMiddleware;
///
/// // Basic SLA monitoring with 5-second target
/// let sla_monitor = SLAMonitoringMiddleware::new(5000);
///
/// // With custom percentile and alert threshold
/// let advanced_sla = SLAMonitoringMiddleware::new(3000)
///     .with_percentile(99)
///     .with_alert_threshold(0.95);
/// ```
/// Bounded window of recent SLA processing-time samples, paired with a
/// running within-SLA count so [`SLAMonitoringMiddleware::compliance_rate`]
/// is O(1) instead of rescanning every sample on every call.
struct SlaWindow {
    samples: std::collections::VecDeque<u64>,
    within_sla_count: usize,
}

/// Maximum number of processing-time samples retained. An unbounded `Vec`
/// (the previous design) grows by 8 bytes per consumed message forever, so
/// a long-running worker leaks memory without limit; capping it (like
/// [`AdaptiveTimeoutMiddleware`]'s sample buffer) bounds that at a fixed,
/// small cost while still giving `compliance_rate` a representative recent
/// window.
const MAX_SLA_SAMPLES: usize = 1024;

pub struct SLAMonitoringMiddleware {
    /// Target processing time in milliseconds
    target_ms: u64,
    /// Percentile to track (default 95th percentile)
    percentile: u8,
    /// Alert threshold (0.0-1.0, default 0.9 = 90% compliance)
    alert_threshold: f64,
    /// Bounded processing-time window
    processing_times: std::sync::Arc<std::sync::Mutex<SlaWindow>>,
}

impl SLAMonitoringMiddleware {
    /// Create a new SLA monitoring middleware
    pub fn new(target_ms: u64) -> Self {
        Self {
            target_ms,
            percentile: 95,
            alert_threshold: 0.9,
            processing_times: std::sync::Arc::new(std::sync::Mutex::new(SlaWindow {
                samples: std::collections::VecDeque::new(),
                within_sla_count: 0,
            })),
        }
    }

    /// Set the percentile to track
    pub fn with_percentile(mut self, percentile: u8) -> Self {
        self.percentile = percentile.clamp(1, 99);
        self
    }

    /// Set the alert threshold
    pub fn with_alert_threshold(mut self, threshold: f64) -> Self {
        self.alert_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Record a processing time sample, evicting the oldest sample (and
    /// correspondingly adjusting the running within-SLA count) once the
    /// window is at capacity.
    fn record_sample(&self, processing_time_ms: u64) {
        let mut window = self
            .processing_times
            .lock()
            .unwrap_or_else(|e| e.into_inner());

        if window.samples.len() >= MAX_SLA_SAMPLES {
            if let Some(evicted) = window.samples.pop_front() {
                if evicted <= self.target_ms {
                    window.within_sla_count = window.within_sla_count.saturating_sub(1);
                }
            }
        }

        if processing_time_ms <= self.target_ms {
            window.within_sla_count += 1;
        }
        window.samples.push_back(processing_time_ms);
    }

    /// Get current SLA compliance rate
    pub fn compliance_rate(&self) -> f64 {
        let window = self
            .processing_times
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        if window.samples.is_empty() {
            return 1.0;
        }

        window.within_sla_count as f64 / window.samples.len() as f64
    }

    /// Check if alert should be triggered
    pub fn should_alert(&self) -> bool {
        self.compliance_rate() < self.alert_threshold
    }
}

#[async_trait]
impl MessageMiddleware for SLAMonitoringMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Inject SLA target
        message.headers.extra.insert(
            "x-sla-target-ms".to_string(),
            serde_json::json!(self.target_ms),
        );

        // Inject start timestamp for SLA tracking
        let timestamp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("SystemTime should be after UNIX_EPOCH")
            .as_millis() as u64;
        message
            .headers
            .extra
            .insert("x-sla-start-ms".to_string(), serde_json::json!(timestamp));

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Calculate processing time
        if let Some(start_ms) = message.headers.extra.get("x-sla-start-ms") {
            if let Some(start_str) = start_ms.as_u64() {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("SystemTime should be after UNIX_EPOCH")
                    .as_millis() as u64;
                let processing_time = now - start_str;

                // Record processing time (bounded window, O(1) compliance
                // rate - see `record_sample`).
                self.record_sample(processing_time);

                // Inject SLA status
                let within_sla = processing_time <= self.target_ms;
                message
                    .headers
                    .extra
                    .insert("x-sla-met".to_string(), serde_json::json!(within_sla));
                message.headers.extra.insert(
                    "x-sla-processing-ms".to_string(),
                    serde_json::json!(processing_time),
                );

                // Check if we should alert
                if self.should_alert() {
                    message
                        .headers
                        .extra
                        .insert("x-sla-alert".to_string(), serde_json::json!(true));
                }
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "sla_monitoring"
    }
}

/// Message versioning middleware - handle message schema versions
///
/// # Examples
///
/// ```
/// use celers_kombu::MessageVersioningMiddleware;
///
/// // Basic versioning with current version
/// let versioning = MessageVersioningMiddleware::new("2.0");
///
/// // With backward compatibility
/// let compat_versioning = MessageVersioningMiddleware::new("3.0")
///     .with_min_supported_version("2.5")
///     .with_auto_upgrade(true);
/// ```
pub struct MessageVersioningMiddleware {
    /// Current message version
    current_version: String,
    /// Minimum supported version
    min_supported_version: Option<String>,
    /// Auto-upgrade messages to current version
    auto_upgrade: bool,
}

impl MessageVersioningMiddleware {
    /// Create a new message versioning middleware
    pub fn new(current_version: impl Into<String>) -> Self {
        Self {
            current_version: current_version.into(),
            min_supported_version: None,
            auto_upgrade: false,
        }
    }

    /// Set minimum supported version
    pub fn with_min_supported_version(mut self, version: impl Into<String>) -> Self {
        self.min_supported_version = Some(version.into());
        self
    }

    /// Enable auto-upgrade of messages
    pub fn with_auto_upgrade(mut self, enabled: bool) -> Self {
        self.auto_upgrade = enabled;
        self
    }

    fn is_version_supported(&self, version: &str) -> bool {
        if let Some(ref min_version) = self.min_supported_version {
            // Parse both sides as semantic (major, minor, patch) tuples
            // and compare numerically. A byte-wise string comparison (the
            // previous approach) is wrong across a decade boundary: the
            // string "10.0" compares as *less than* "9.0" because '1' <
            // '9' lexicographically, so a valid newer message would be
            // rejected as unsupported.
            match (Self::parse_semver(version), Self::parse_semver(min_version)) {
                (Some(v), Some(min)) => v >= min,
                // An unparseable version is treated as unsupported rather
                // than silently comparing incomparable values.
                _ => false,
            }
        } else {
            true
        }
    }

    /// Parse a `major[.minor[.patch]]` version string into a comparable
    /// tuple, treating missing components as 0 (so "2" and "2.0.0" compare
    /// equal). Returns `None` if the leading component isn't a valid
    /// number.
    fn parse_semver(version: &str) -> Option<(u64, u64, u64)> {
        let mut parts = version.trim().split('.');
        let major = parts.next()?.parse::<u64>().ok()?;
        let minor = parts
            .next()
            .map(|p| p.parse::<u64>())
            .transpose()
            .ok()?
            .unwrap_or(0);
        let patch = parts
            .next()
            .map(|p| p.parse::<u64>())
            .transpose()
            .ok()?
            .unwrap_or(0);
        Some((major, minor, patch))
    }
}

#[async_trait]
impl MessageMiddleware for MessageVersioningMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Inject current version
        message.headers.extra.insert(
            "x-message-version".to_string(),
            serde_json::json!(self.current_version.clone()),
        );

        // Inject schema version metadata
        message.headers.extra.insert(
            "x-schema-version".to_string(),
            serde_json::json!(self.current_version.clone()),
        );

        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Check message version
        if let Some(msg_version) = message.headers.extra.get("x-message-version") {
            if let Some(version_str) = msg_version.as_str() {
                // Check if version is supported
                if !self.is_version_supported(version_str) {
                    return Err(BrokerError::Configuration(format!(
                        "Unsupported message version: {}. Minimum supported: {:?}",
                        version_str, self.min_supported_version
                    )));
                }

                // Auto-upgrade if needed
                if self.auto_upgrade && version_str != self.current_version {
                    message.headers.extra.insert(
                        "x-upgraded-from".to_string(),
                        serde_json::json!(version_str),
                    );
                    message.headers.extra.insert(
                        "x-message-version".to_string(),
                        serde_json::json!(self.current_version.clone()),
                    );
                }
            }
        } else {
            // No version specified, treat as legacy
            message
                .headers
                .extra
                .insert("x-message-version".to_string(), serde_json::json!("legacy"));
            if self.auto_upgrade {
                message
                    .headers
                    .extra
                    .insert("x-upgraded-from".to_string(), serde_json::json!("legacy"));
                message.headers.extra.insert(
                    "x-message-version".to_string(),
                    serde_json::json!(self.current_version.clone()),
                );
            }
        }
        Ok(())
    }

    fn name(&self) -> &str {
        "message_versioning"
    }

    fn is_drop_signal(&self, _err: &BrokerError) -> bool {
        // A message whose version is below `min_supported_version` will
        // report the exact same version on every redelivery - retrying
        // cannot make an old message become a new one, so this is the
        // middleware's designed "this message can never be processed"
        // signal rather than a transient processing failure that should be
        // requeued forever.
        true
    }
}

/// Type alias for resource usage tracking: (message_count, byte_count, last_reset_timestamp)
type ResourceUsageMap = std::sync::Arc<std::sync::Mutex<HashMap<String, (usize, usize, u64)>>>;

/// Resource quota middleware - enforce per-consumer resource limits
///
/// # Examples
///
/// ```
/// use celers_kombu::ResourceQuotaMiddleware;
///
/// // Basic quota (100 messages per consumer)
/// let quota = ResourceQuotaMiddleware::new(100);
///
/// // With custom limits
/// let advanced_quota = ResourceQuotaMiddleware::new(1000)
///     .with_max_size_bytes(10_000_000)  // 10MB per consumer
///     .with_time_window_secs(60);        // Reset every 60 seconds
/// ```
pub struct ResourceQuotaMiddleware {
    /// Maximum messages per consumer
    max_messages: usize,
    /// Maximum bytes per consumer
    max_size_bytes: usize,
    /// Time window in seconds for quota reset
    time_window_secs: u64,
    /// Usage tracking per consumer
    usage: ResourceUsageMap,
}

impl ResourceQuotaMiddleware {
    /// Create a new resource quota middleware
    pub fn new(max_messages: usize) -> Self {
        Self {
            max_messages,
            max_size_bytes: usize::MAX,
            time_window_secs: 3600, // Default 1 hour
            usage: std::sync::Arc::new(std::sync::Mutex::new(HashMap::new())),
        }
    }

    /// Set maximum bytes per consumer
    pub fn with_max_size_bytes(mut self, max_bytes: usize) -> Self {
        self.max_size_bytes = max_bytes;
        self
    }

    /// Set time window for quota reset
    pub fn with_time_window_secs(mut self, seconds: u64) -> Self {
        self.time_window_secs = seconds;
        self
    }

    /// Get current usage for a consumer
    pub fn get_usage(&self, consumer_id: &str) -> (usize, usize) {
        let usage = self.usage.lock().unwrap_or_else(|e| e.into_inner());
        usage
            .get(consumer_id)
            .map(|(msgs, bytes, _)| (*msgs, *bytes))
            .unwrap_or((0, 0))
    }

    /// Reset quota for a consumer
    pub fn reset_quota(&self, consumer_id: &str) {
        let mut usage = self.usage.lock().unwrap_or_else(|e| e.into_inner());
        usage.remove(consumer_id);
    }

    fn check_and_update_quota(&self, consumer_id: &str, message_size: usize) -> Result<()> {
        let mut usage = self.usage.lock().unwrap_or_else(|e| e.into_inner());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("SystemTime should be after UNIX_EPOCH")
            .as_secs();

        // Opportunistically prune consumers that have been completely
        // idle for more than one full quota window. Without this, `usage`
        // grows by one permanent entry per distinct consumer id ever seen
        // for the lifetime of the process, since nothing else ever
        // shrinks the map.
        usage.retain(|_, (_, _, last_reset)| {
            now.saturating_sub(*last_reset) < self.time_window_secs.saturating_mul(2)
        });

        let (msg_count, byte_count, last_reset) =
            usage.entry(consumer_id.to_string()).or_insert((0, 0, now));

        // Reset if time window elapsed
        if now - *last_reset >= self.time_window_secs {
            *msg_count = 0;
            *byte_count = 0;
            *last_reset = now;
        }

        // Check quota
        if *msg_count >= self.max_messages {
            return Err(BrokerError::Configuration(format!(
                "Message quota exceeded for consumer {}: {}/{}",
                consumer_id, msg_count, self.max_messages
            )));
        }

        if *byte_count + message_size > self.max_size_bytes {
            return Err(BrokerError::Configuration(format!(
                "Size quota exceeded for consumer {}: {}/{}",
                consumer_id, byte_count, self.max_size_bytes
            )));
        }

        // Update usage
        *msg_count += 1;
        *byte_count += message_size;

        Ok(())
    }
}

#[async_trait]
impl MessageMiddleware for ResourceQuotaMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        // Inject quota limits into message headers for transparency
        message.headers.extra.insert(
            "x-quota-max-messages".to_string(),
            serde_json::json!(self.max_messages),
        );
        if self.max_size_bytes != usize::MAX {
            message.headers.extra.insert(
                "x-quota-max-bytes".to_string(),
                serde_json::json!(self.max_size_bytes),
            );
        }
        Ok(())
    }

    async fn after_consume(&self, message: &mut Message) -> Result<()> {
        // Extract consumer ID from message headers
        let consumer_id = message
            .headers
            .extra
            .get("x-consumer-id")
            .and_then(|v| v.as_str())
            .unwrap_or("default")
            .to_string();

        // Check and update quota
        let message_size = message.body.len();
        self.check_and_update_quota(&consumer_id, message_size)?;

        // Inject current usage into message
        let (msg_count, byte_count) = self.get_usage(&consumer_id);
        message.headers.extra.insert(
            "x-quota-used-messages".to_string(),
            serde_json::json!(msg_count),
        );
        message.headers.extra.insert(
            "x-quota-used-bytes".to_string(),
            serde_json::json!(byte_count),
        );

        Ok(())
    }

    fn name(&self) -> &str {
        "resource_quota"
    }

    fn is_drop_signal(&self, _err: &BrokerError) -> bool {
        // Quota exhaustion is this middleware's designed "stop admitting
        // messages for this consumer" signal, not a processing failure -
        // treat it the same as deduplication/filtering/sampling so the
        // message is settled (not endlessly redelivered) rather than
        // propagated as an error.
        true
    }
}

#[cfg(test)]
mod hardening_tests {
    use super::*;
    use uuid::Uuid;

    // -------------------------------------------------------------------
    // idx114: LoadShedding must read the typed priority field.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn load_shedding_sheds_message_built_with_with_priority() {
        let mut load_shedder = LoadSheddingMiddleware::new(0.5);
        load_shedder.update_load(1.0);

        let mut message =
            celers_protocol::Message::new("t".to_string(), Uuid::new_v4(), vec![]).with_priority(1); // Low priority, via the typed field only.

        let result = load_shedder.before_publish(&mut message).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn load_shedding_allows_high_typed_priority_even_under_load() {
        let mut load_shedder = LoadSheddingMiddleware::new(0.5);
        load_shedder.update_load(1.0);

        let mut message =
            celers_protocol::Message::new("t".to_string(), Uuid::new_v4(), vec![]).with_priority(9);

        let result = load_shedder.before_publish(&mut message).await;
        assert!(result.is_ok());
    }

    // -------------------------------------------------------------------
    // idx115: LoadSheddingMiddleware must be armable once boxed into a
    // chain, via a cloneable LoadHandle obtained beforehand.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn load_shedding_handle_updates_load_after_boxed_into_chain() {
        let load_shedder = LoadSheddingMiddleware::new(0.5);
        let handle = load_shedder.load_handle();

        let chain = crate::MiddlewareChain::new().with_middleware(Box::new(load_shedder));

        let mut low_priority_msg =
            celers_protocol::Message::new("t".to_string(), Uuid::new_v4(), vec![]).with_priority(1);
        // Load starts at 0.0: nothing is shed yet.
        assert!(chain
            .process_before_publish(&mut low_priority_msg)
            .await
            .is_ok());

        // Arm it via the handle, exactly as `MiddlewareChain` only ever
        // exposes `&dyn MessageMiddleware` (no `&mut`) once installed.
        handle.set_load(1.0);
        assert_eq!(handle.load(), 1.0);

        let mut low_priority_msg2 =
            celers_protocol::Message::new("t".to_string(), Uuid::new_v4(), vec![]).with_priority(1);
        let result = chain.process_before_publish(&mut low_priority_msg2).await;
        assert!(result.is_err());
    }

    // -------------------------------------------------------------------
    // idx125 / idx203: age-based priority escalation must use the
    // message's real age, and must fire on the consume side (where queue
    // residency is actually observable), not just at publish time.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn priority_escalation_after_consume_escalates_once_message_ages() {
        // Real-clock based: `get_age_seconds` computes elapsed time from
        // `headers.created_at` via `chrono::Utc::now()`. Backdating
        // `created_at` directly (bypassing the wait entirely) would be
        // preferable, but `chrono` is a dependency of `celers-protocol`
        // (which defines the field), not of `celers-kombu` itself - naming
        // `chrono::` here would not compile without adding it to this
        // crate's `Cargo.toml`, which is out of this module's ownership
        // (see the equivalent PriorityBoostMiddleware test for the same
        // constraint). So this waits a bit over one second against a
        // one-second threshold instead.
        let middleware = MessagePriorityEscalationMiddleware::new(1);
        let mut message = celers_protocol::Message::new("t".to_string(), Uuid::new_v4(), vec![]);

        tokio::time::sleep(std::time::Duration::from_millis(1100)).await;

        middleware.after_consume(&mut message).await.unwrap();

        assert!(message.properties.priority.unwrap_or(5) > 5);
        assert!(message.headers.extra.contains_key("x-priority-escalated"));
    }

    #[tokio::test]
    async fn priority_escalation_before_publish_still_escalates_on_retry() {
        // Regression guard: this exact before_publish behaviour is relied
        // on elsewhere in this crate's test suite and must keep working
        // (age is ~0 for a freshly published message, so only the retry
        // term has an effect here).
        let middleware = MessagePriorityEscalationMiddleware::new(300);
        let mut message = celers_protocol::Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        message.headers.retries = Some(2);

        middleware.before_publish(&mut message).await.unwrap();

        let escalated = message
            .headers
            .extra
            .get("priority")
            .and_then(|v| v.as_u64())
            .unwrap();
        assert!(escalated > 5);
        assert_eq!(message.properties.priority, Some(escalated as u8));
    }

    #[test]
    fn priority_escalation_zero_threshold_does_not_panic() {
        // Regression guard for a latent division-by-zero: age escalation
        // now runs on every consume with a real (unbounded, non-zero)
        // age, so a degenerate `age_threshold_secs == 0` configuration
        // must not crash the worker.
        let middleware = MessagePriorityEscalationMiddleware::new(0);
        // Large age_secs on purpose: also guards against the multiplier
        // overflowing `u8`.
        let boosted = middleware.calculate_priority(5, u64::MAX, 0);
        assert!(boosted <= middleware.max_priority);
    }

    // -------------------------------------------------------------------
    // idx126: SLAMonitoringMiddleware must bound its sample window and
    // keep compliance_rate correct while doing so.
    // -------------------------------------------------------------------

    #[test]
    fn sla_monitoring_bounds_sample_window_and_stays_accurate() {
        let middleware = SLAMonitoringMiddleware::new(100).with_alert_threshold(0.5);

        // Push far more samples than MAX_SLA_SAMPLES, alternating
        // within/outside SLA, and confirm compliance_rate matches the
        // last MAX_SLA_SAMPLES samples only (eviction bookkeeping is
        // correct) while the window itself never grows past the cap.
        for i in 0..(MAX_SLA_SAMPLES * 3) {
            let sample_ms = if i % 2 == 0 { 50 } else { 200 }; // half within, half over
            middleware.record_sample(sample_ms);
        }

        let window = middleware
            .processing_times
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        assert_eq!(window.samples.len(), MAX_SLA_SAMPLES);
        drop(window);

        // Exactly half of the retained samples are within the 100ms SLA.
        assert!((middleware.compliance_rate() - 0.5).abs() < 1e-9);
    }

    // -------------------------------------------------------------------
    // idx126: ResourceQuotaMiddleware must not accumulate a permanent
    // entry per distinct consumer id.
    // -------------------------------------------------------------------

    #[test]
    fn resource_quota_prunes_stale_consumers() {
        let middleware = ResourceQuotaMiddleware::new(100).with_time_window_secs(1);

        // Simulate a consumer whose last activity is long past 2x the
        // quota window, by writing directly into the (crate-internal)
        // usage map rather than waiting in real time.
        {
            let mut usage = middleware.usage.lock().unwrap_or_else(|e| e.into_inner());
            usage.insert("stale-consumer".to_string(), (1, 1, 0));
        }
        assert_eq!(middleware.get_usage("stale-consumer"), (1, 1));

        // Any subsequent quota check for a different (or the same)
        // consumer must sweep the stale entry out.
        middleware
            .check_and_update_quota("active-consumer", 10)
            .unwrap();

        assert_eq!(middleware.get_usage("stale-consumer"), (0, 0));
    }

    // -------------------------------------------------------------------
    // idx147: MessageVersioningMiddleware must compare versions
    // numerically, not lexicographically.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn message_versioning_accepts_double_digit_minor_version_bump() {
        let middleware = MessageVersioningMiddleware::new("10.0").with_min_supported_version("9.0");
        let mut message = celers_protocol::Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        message
            .headers
            .extra
            .insert("x-message-version".to_string(), serde_json::json!("10.0"));

        // A byte-wise string comparison rejects "10.0" as less than "9.0";
        // a numeric comparison must accept it.
        let result = middleware.after_consume(&mut message).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn message_versioning_rejects_genuinely_older_version() {
        let middleware = MessageVersioningMiddleware::new("9.0").with_min_supported_version("9.0");
        let mut message = celers_protocol::Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        message
            .headers
            .extra
            .insert("x-message-version".to_string(), serde_json::json!("8.5"));

        let result = middleware.after_consume(&mut message).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn message_versioning_rejection_is_settled_as_a_drop_not_requeued_forever() {
        // A too-old message will report the exact same version on every
        // redelivery, so `MiddlewareChain::process_after_consume` must
        // classify the rejection as a designed drop (via `is_drop_signal`)
        // rather than a genuine failure - otherwise
        // `MiddlewareConsumer::consume_with_middleware` requeues it and it
        // is redelivered forever.
        let middleware = MessageVersioningMiddleware::new("9.0").with_min_supported_version("9.0");
        let chain = crate::MiddlewareChain::new().with_middleware(Box::new(middleware));

        let mut message = celers_protocol::Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        message
            .headers
            .extra
            .insert("x-message-version".to_string(), serde_json::json!("8.5"));

        let decision = chain.process_after_consume(&mut message).await.unwrap();
        assert!(decision.is_drop());
    }

    // -------------------------------------------------------------------
    // idx204: ObservabilityMiddleware must actually emit metrics on the
    // consume path when a shared BrokerMetrics is attached.
    // -------------------------------------------------------------------

    #[tokio::test]
    async fn observability_middleware_increments_shared_metrics() {
        let metrics = std::sync::Arc::new(std::sync::Mutex::new(BrokerMetrics::default()));
        let middleware = ObservabilityMiddleware::new("svc").with_metrics(metrics.clone());

        let mut message = celers_protocol::Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        middleware.before_publish(&mut message).await.unwrap();
        middleware.after_consume(&mut message).await.unwrap();

        let snapshot = metrics.lock().unwrap_or_else(|e| e.into_inner()).clone();
        assert_eq!(snapshot.messages_published, 1);
        assert_eq!(snapshot.messages_consumed, 1);
    }

    #[tokio::test]
    async fn observability_middleware_without_metrics_does_not_touch_shared_metrics() {
        let metrics = std::sync::Arc::new(std::sync::Mutex::new(BrokerMetrics::default()));
        let middleware = ObservabilityMiddleware::new("svc")
            .with_metrics(metrics.clone())
            .without_metrics();

        let mut message = celers_protocol::Message::new("t".to_string(), Uuid::new_v4(), vec![]);
        middleware.before_publish(&mut message).await.unwrap();
        middleware.after_consume(&mut message).await.unwrap();

        let snapshot = metrics.lock().unwrap_or_else(|e| e.into_inner()).clone();
        assert_eq!(snapshot.messages_published, 0);
        assert_eq!(snapshot.messages_consumed, 0);
    }
}
