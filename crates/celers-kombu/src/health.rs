//! Health check and metrics types.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// =============================================================================
// Health Check
// =============================================================================

/// Health status of a broker
///
/// # Examples
///
/// ```
/// use celers_kombu::HealthStatus;
///
/// let healthy = HealthStatus::Healthy;
/// assert!(healthy.is_healthy());
/// assert!(healthy.is_operational());
/// assert_eq!(healthy.to_string(), "healthy");
///
/// let degraded = HealthStatus::Degraded;
/// assert!(!degraded.is_healthy());
/// assert!(degraded.is_operational());
/// assert_eq!(degraded.to_string(), "degraded");
///
/// let unhealthy = HealthStatus::Unhealthy;
/// assert!(!unhealthy.is_healthy());
/// assert!(!unhealthy.is_operational());
/// assert_eq!(unhealthy.to_string(), "unhealthy");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum HealthStatus {
    /// Broker is healthy and accepting connections
    Healthy,
    /// Broker is degraded but functional
    Degraded,
    /// Broker is unhealthy or unreachable
    Unhealthy,
}

impl HealthStatus {
    /// Check if status is healthy
    pub fn is_healthy(&self) -> bool {
        matches!(self, HealthStatus::Healthy)
    }

    /// Check if status allows operations
    pub fn is_operational(&self) -> bool {
        matches!(self, HealthStatus::Healthy | HealthStatus::Degraded)
    }
}

impl std::fmt::Display for HealthStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HealthStatus::Healthy => write!(f, "healthy"),
            HealthStatus::Degraded => write!(f, "degraded"),
            HealthStatus::Unhealthy => write!(f, "unhealthy"),
        }
    }
}

/// Detailed health check response
///
/// # Examples
///
/// ```
/// use celers_kombu::HealthCheckResponse;
///
/// let response = HealthCheckResponse::healthy("redis", "localhost:6379")
///     .with_latency(15);
///
/// assert!(response.status.is_healthy());
/// assert_eq!(response.broker_type, "redis");
/// assert_eq!(response.latency_ms, Some(15));
///
/// let unhealthy = HealthCheckResponse::unhealthy("amqp", "localhost:5672", "connection refused");
/// assert!(!unhealthy.status.is_healthy());
/// assert_eq!(unhealthy.details.get("reason"), Some(&"connection refused".to_string()));
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthCheckResponse {
    /// Overall health status
    pub status: HealthStatus,
    /// Broker name/type
    pub broker_type: String,
    /// Connection URL (sanitized, no password)
    pub connection: String,
    /// Latency in milliseconds
    pub latency_ms: Option<u64>,
    /// Additional details
    pub details: HashMap<String, String>,
}

impl HealthCheckResponse {
    /// Create a new healthy response
    pub fn healthy(broker_type: &str, connection: &str) -> Self {
        Self {
            status: HealthStatus::Healthy,
            broker_type: broker_type.to_string(),
            connection: connection.to_string(),
            latency_ms: None,
            details: HashMap::new(),
        }
    }

    /// Create a new unhealthy response
    pub fn unhealthy(broker_type: &str, connection: &str, reason: &str) -> Self {
        let mut details = HashMap::new();
        details.insert("reason".to_string(), reason.to_string());
        Self {
            status: HealthStatus::Unhealthy,
            broker_type: broker_type.to_string(),
            connection: connection.to_string(),
            latency_ms: None,
            details,
        }
    }

    /// Set latency
    pub fn with_latency(mut self, latency_ms: u64) -> Self {
        self.latency_ms = Some(latency_ms);
        self
    }

    /// Add a detail
    pub fn with_detail(mut self, key: &str, value: &str) -> Self {
        self.details.insert(key.to_string(), value.to_string());
        self
    }
}

/// Health check trait for brokers
#[async_trait]
pub trait HealthCheck: Send + Sync {
    /// Perform a health check
    async fn health_check(&self) -> HealthCheckResponse;

    /// Perform a simple ping (returns true if broker is reachable)
    async fn ping(&self) -> bool;
}

// =============================================================================
// Metrics
// =============================================================================

/// Broker metrics
///
/// # Examples
///
/// ```
/// use celers_kombu::BrokerMetrics;
///
/// let mut metrics = BrokerMetrics::new();
/// metrics.inc_published();
/// metrics.inc_consumed();
/// metrics.inc_acknowledged();
///
/// assert_eq!(metrics.messages_published, 1);
/// assert_eq!(metrics.messages_consumed, 1);
/// assert_eq!(metrics.messages_acknowledged, 1);
/// ```
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct BrokerMetrics {
    /// Total messages published
    pub messages_published: u64,
    /// Total messages consumed
    pub messages_consumed: u64,
    /// Total messages acknowledged
    pub messages_acknowledged: u64,
    /// Total messages rejected
    pub messages_rejected: u64,
    /// Total publish errors
    pub publish_errors: u64,
    /// Total consume errors
    pub consume_errors: u64,
    /// Current active connections
    pub active_connections: u32,
    /// Total connection attempts
    pub connection_attempts: u64,
    /// Total connection failures
    pub connection_failures: u64,
}

impl BrokerMetrics {
    /// Create new empty metrics
    pub fn new() -> Self {
        Self::default()
    }

    /// Increment messages published
    pub fn inc_published(&mut self) {
        self.messages_published += 1;
    }

    /// Increment messages consumed
    pub fn inc_consumed(&mut self) {
        self.messages_consumed += 1;
    }

    /// Increment messages acknowledged
    pub fn inc_acknowledged(&mut self) {
        self.messages_acknowledged += 1;
    }

    /// Increment messages rejected
    pub fn inc_rejected(&mut self) {
        self.messages_rejected += 1;
    }

    /// Increment publish errors
    pub fn inc_publish_error(&mut self) {
        self.publish_errors += 1;
    }

    /// Increment consume errors
    pub fn inc_consume_error(&mut self) {
        self.consume_errors += 1;
    }

    /// Increment connection attempts
    pub fn inc_connection_attempt(&mut self) {
        self.connection_attempts += 1;
    }

    /// Increment connection failures
    pub fn inc_connection_failure(&mut self) {
        self.connection_failures += 1;
    }
}

/// Metrics provider trait
#[async_trait]
pub trait MetricsProvider: Send + Sync {
    /// Get current metrics snapshot
    async fn get_metrics(&self) -> BrokerMetrics;

    /// Reset all metrics
    async fn reset_metrics(&mut self);
}
