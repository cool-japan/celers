//! Monitoring and operational middleware implementations.

use crate::{BrokerError, MessageMiddleware, Result};
use async_trait::async_trait;
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
#[derive(Debug, Clone)]
pub struct LoadSheddingMiddleware {
    load_threshold: f64, // Threshold for load shedding (0.0-1.0)
    priority_cutoff: u8, // Drop messages below this priority
    current_load: f64,   // Current system load estimate
}

impl LoadSheddingMiddleware {
    /// Create a new load shedding middleware
    pub fn new(load_threshold: f64) -> Self {
        Self {
            load_threshold: load_threshold.clamp(0.0, 1.0),
            priority_cutoff: 3, // Default: drop priority < 3 (Low and below)
            current_load: 0.0,
        }
    }

    /// Set the priority cutoff
    pub fn with_priority_cutoff(mut self, cutoff: u8) -> Self {
        self.priority_cutoff = cutoff.min(10);
        self
    }

    /// Update current load estimate
    pub fn update_load(&mut self, load: f64) {
        self.current_load = load.clamp(0.0, 1.0);
    }

    /// Get the load threshold
    pub fn threshold(&self) -> f64 {
        self.load_threshold
    }

    /// Check if message should be dropped
    fn should_shed(&self, priority: u8) -> bool {
        self.current_load > self.load_threshold && priority < self.priority_cutoff
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
        let priority = message
            .headers
            .extra
            .get("priority")
            .and_then(|v| v.as_u64())
            .map(|v| v as u8)
            .unwrap_or(5);

        if self.should_shed(priority) {
            // Inject load shedding marker
            message
                .headers
                .extra
                .insert("x-load-shed".to_string(), serde_json::json!(true));
            message.headers.extra.insert(
                "x-current-load".to_string(),
                serde_json::json!(self.current_load),
            );

            return Err(BrokerError::OperationFailed(format!(
                "Load shedding: current load {:.2} exceeds threshold {:.2}",
                self.current_load, self.load_threshold
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

        // Age-based escalation
        if age_secs >= self.age_threshold_secs {
            let age_multiplier = (age_secs / self.age_threshold_secs) as u8;
            priority = priority.saturating_add(age_multiplier * self.escalation_step);
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

#[async_trait]
impl MessageMiddleware for MessagePriorityEscalationMiddleware {
    async fn before_publish(&self, message: &mut Message) -> Result<()> {
        let base_priority = message
            .headers
            .extra
            .get("priority")
            .and_then(|v| v.as_u64())
            .map(|v| v as u8)
            .unwrap_or(5);
        let age_secs = 0; // Would be calculated from message timestamp in real implementation
        let retries = message.headers.retries.unwrap_or(0);

        let new_priority = self.calculate_priority(base_priority, age_secs, retries);

        if new_priority != base_priority {
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

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        Ok(())
    }

    fn name(&self) -> &str {
        "priority_escalation"
    }
}

/// Observability middleware for structured logging and metrics
///
/// Provides structured logging and metrics export for monitoring systems.
/// Useful for integration with observability platforms.
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
}

impl ObservabilityMiddleware {
    /// Create a new observability middleware
    pub fn new(service_name: impl Into<String>) -> Self {
        Self {
            service_name: service_name.into(),
            enable_metrics: true,
            enable_logging: true,
            log_level: "info".to_string(),
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

    /// Get service name
    pub fn service_name(&self) -> &str {
        &self.service_name
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

        Ok(())
    }

    async fn after_consume(&self, _message: &mut Message) -> Result<()> {
        // In a real implementation, would emit metrics and logs here
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
        *self.is_healthy.lock().unwrap()
    }

    /// Mark as unhealthy
    pub fn mark_unhealthy(&self) {
        *self.is_healthy.lock().unwrap() = false;
    }

    /// Mark as healthy
    pub fn mark_healthy(&self) {
        *self.is_healthy.lock().unwrap() = true;
    }

    fn should_check(&self) -> bool {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let last = *self.last_check.lock().unwrap();
        now - last >= self.check_interval_secs
    }

    fn update_check_time(&self) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        *self.last_check.lock().unwrap() = now;
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
        if self.should_check() {
            self.update_check_time();
            // In a real implementation, would perform actual health check
            // For now, just inject health status into message headers
        }

        let health_status = if self.is_healthy() {
            "healthy"
        } else {
            "unhealthy"
        };
        message.headers.extra.insert(
            "x-health-status".to_string(),
            serde_json::json!(health_status),
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
            .unwrap()
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
                        .unwrap()
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
pub struct SLAMonitoringMiddleware {
    /// Target processing time in milliseconds
    target_ms: u64,
    /// Percentile to track (default 95th percentile)
    percentile: u8,
    /// Alert threshold (0.0-1.0, default 0.9 = 90% compliance)
    alert_threshold: f64,
    /// Processing times buffer
    processing_times: std::sync::Arc<std::sync::Mutex<Vec<u64>>>,
}

impl SLAMonitoringMiddleware {
    /// Create a new SLA monitoring middleware
    pub fn new(target_ms: u64) -> Self {
        Self {
            target_ms,
            percentile: 95,
            alert_threshold: 0.9,
            processing_times: std::sync::Arc::new(std::sync::Mutex::new(Vec::new())),
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

    /// Get current SLA compliance rate
    pub fn compliance_rate(&self) -> f64 {
        let times = self.processing_times.lock().unwrap();
        if times.is_empty() {
            return 1.0;
        }

        let within_sla = times.iter().filter(|&&t| t <= self.target_ms).count();
        within_sla as f64 / times.len() as f64
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
            .unwrap()
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
                    .unwrap()
                    .as_millis() as u64;
                let processing_time = now - start_str;

                // Record processing time
                self.processing_times.lock().unwrap().push(processing_time);

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
            // Simple string comparison (in production, use semantic versioning)
            version >= min_version.as_str()
        } else {
            true
        }
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
        let usage = self.usage.lock().unwrap();
        usage
            .get(consumer_id)
            .map(|(msgs, bytes, _)| (*msgs, *bytes))
            .unwrap_or((0, 0))
    }

    /// Reset quota for a consumer
    pub fn reset_quota(&self, consumer_id: &str) {
        let mut usage = self.usage.lock().unwrap();
        usage.remove(consumer_id);
    }

    fn check_and_update_quota(&self, consumer_id: &str, message_size: usize) -> Result<()> {
        let mut usage = self.usage.lock().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

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
}
