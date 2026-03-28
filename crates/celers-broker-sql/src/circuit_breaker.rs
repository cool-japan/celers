//! Circuit breaker and idempotency types for the MySQL broker
//!
//! Provides resilience patterns for database connection management
//! and duplicate task prevention.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Circuit breaker state for database connection resilience
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CircuitBreakerState {
    Closed,   // Normal operation
    Open,     // Failing, rejecting requests
    HalfOpen, // Testing if service recovered
}

/// Circuit breaker statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CircuitBreakerStats {
    pub state: CircuitBreakerState,
    pub failure_count: u64,
    pub success_count: u64,
    pub last_failure_time: Option<DateTime<Utc>>,
    pub last_state_change: DateTime<Utc>,
}

/// Circuit breaker configuration
#[derive(Debug, Clone)]
pub struct CircuitBreakerConfig {
    /// Number of failures before opening the circuit
    pub failure_threshold: u64,
    /// Duration to wait before transitioning from Open to HalfOpen (in seconds)
    pub timeout_secs: u64,
    /// Number of successful requests required in HalfOpen state to close the circuit
    pub success_threshold: u64,
}

impl Default for CircuitBreakerConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            timeout_secs: 60,
            success_threshold: 2,
        }
    }
}

/// Internal circuit breaker state
#[derive(Debug, Clone)]
pub(crate) struct CircuitBreakerStateInternal {
    pub(crate) state: CircuitBreakerState,
    pub(crate) failure_count: u64,
    pub(crate) success_count: u64,
    pub(crate) last_failure_time: Option<DateTime<Utc>>,
    pub(crate) last_state_change: DateTime<Utc>,
    pub(crate) config: CircuitBreakerConfig,
}

impl CircuitBreakerStateInternal {
    pub(crate) fn new(config: CircuitBreakerConfig) -> Self {
        Self {
            state: CircuitBreakerState::Closed,
            failure_count: 0,
            success_count: 0,
            last_failure_time: None,
            last_state_change: Utc::now(),
            config,
        }
    }
}

/// Idempotency key information for duplicate prevention
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyRecord {
    pub id: Uuid,
    pub idempotency_key: String,
    pub task_name: String,
    pub task_id: Uuid,
    pub created_at: DateTime<Utc>,
    pub expires_at: DateTime<Utc>,
    pub metadata: Option<serde_json::Value>,
}

/// Idempotency statistics per task type
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IdempotencyStats {
    pub task_name: String,
    pub total_keys: i64,
    pub unique_keys: i64,
    pub active_keys: i64,
    pub expired_keys: i64,
    pub oldest_key: Option<DateTime<Utc>>,
    pub newest_key: Option<DateTime<Utc>>,
}

/// Idempotency configuration
#[derive(Debug, Clone)]
pub struct IdempotencyConfig {
    /// Default TTL for idempotency keys (in seconds)
    pub default_ttl_secs: u64,
    /// Whether to automatically cleanup expired keys
    pub auto_cleanup: bool,
}

impl Default for IdempotencyConfig {
    fn default() -> Self {
        Self {
            default_ttl_secs: 86400, // 24 hours
            auto_cleanup: true,
        }
    }
}
