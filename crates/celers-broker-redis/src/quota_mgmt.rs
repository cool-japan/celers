//! Quota management for multi-tenant task queues
//!
//! This module provides quota management capabilities to limit resource
//! usage per user, tenant, or queue. Useful for SaaS applications or
//! multi-tenant systems.
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::quota_mgmt::{QuotaManager, QuotaConfig, QuotaPeriod};
//! use redis::Client;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let client = Client::open("redis://localhost:6379")?;
//!     let mut quota_mgr = QuotaManager::new(client);
//!
//!     // Set a quota: max 1000 tasks per hour
//!     let config = QuotaConfig::new()
//!         .with_max_tasks(1000)
//!         .with_period(QuotaPeriod::Hour);
//!
//!     quota_mgr.set_quota("user:123", config).await?;
//!
//!     // Check if user can enqueue a task
//!     if quota_mgr.check_quota("user:123", 1).await? {
//!         println!("User can enqueue task");
//!     } else {
//!         println!("User quota exceeded");
//!     }
//!
//!     Ok(())
//! }
//! ```

use celers_core::{CelersError, Result};
use redis::{aio::MultiplexedConnection, AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Time period for quota limits
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QuotaPeriod {
    /// Per minute
    Minute,
    /// Per hour
    Hour,
    /// Per day
    Day,
    /// Per week
    Week,
    /// Per month (30 days)
    Month,
}

impl QuotaPeriod {
    /// Get the duration in seconds
    pub fn as_seconds(&self) -> u64 {
        match self {
            QuotaPeriod::Minute => 60,
            QuotaPeriod::Hour => 3600,
            QuotaPeriod::Day => 86400,
            QuotaPeriod::Week => 604800,
            QuotaPeriod::Month => 2592000, // 30 days
        }
    }

    /// Get a human-readable name
    pub fn name(&self) -> &'static str {
        match self {
            QuotaPeriod::Minute => "minute",
            QuotaPeriod::Hour => "hour",
            QuotaPeriod::Day => "day",
            QuotaPeriod::Week => "week",
            QuotaPeriod::Month => "month",
        }
    }
}

/// Quota configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaConfig {
    /// Maximum number of tasks allowed in the period
    pub max_tasks: u64,
    /// Time period for the quota
    pub period: QuotaPeriod,
    /// Whether to allow bursting above the limit
    pub allow_burst: bool,
    /// Burst limit (only used if allow_burst is true)
    pub burst_limit: u64,
}

impl QuotaConfig {
    /// Create a new quota configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of tasks
    pub fn with_max_tasks(mut self, max: u64) -> Self {
        self.max_tasks = max;
        self
    }

    /// Set the time period
    pub fn with_period(mut self, period: QuotaPeriod) -> Self {
        self.period = period;
        self
    }

    /// Enable bursting with a limit
    pub fn with_burst(mut self, burst_limit: u64) -> Self {
        self.allow_burst = true;
        self.burst_limit = burst_limit;
        self
    }
}

impl Default for QuotaConfig {
    fn default() -> Self {
        Self {
            max_tasks: 1000,
            period: QuotaPeriod::Hour,
            allow_burst: false,
            burst_limit: 0,
        }
    }
}

/// Quota usage information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QuotaUsage {
    /// Quota identifier (user, tenant, etc.)
    pub quota_id: String,
    /// Current usage count
    pub current_usage: u64,
    /// Maximum allowed
    pub max_allowed: u64,
    /// Remaining quota
    pub remaining: u64,
    /// Usage percentage (0.0 to 100.0+)
    pub usage_percentage: f64,
    /// Time period
    pub period: QuotaPeriod,
    /// Whether quota is exceeded
    pub is_exceeded: bool,
    /// Seconds until quota resets
    pub reset_in_seconds: u64,
}

impl QuotaUsage {
    /// Check if the quota is nearly exhausted (>= 80% used)
    pub fn is_nearly_exhausted(&self) -> bool {
        self.usage_percentage >= 80.0
    }

    /// Check if the quota is available (< 100% used)
    pub fn is_available(&self) -> bool {
        !self.is_exceeded
    }
}

/// Quota manager for managing per-user/tenant quotas
pub struct QuotaManager {
    client: Client,
    key_prefix: String,
}

impl QuotaManager {
    /// Create a new quota manager
    pub fn new(client: Client) -> Self {
        Self {
            client,
            key_prefix: "quota".to_string(),
        }
    }

    /// Create a quota manager with a custom key prefix
    pub fn with_prefix(client: Client, prefix: impl Into<String>) -> Self {
        Self {
            client,
            key_prefix: prefix.into(),
        }
    }

    /// Get a Redis connection
    async fn get_connection(&self) -> Result<MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))
    }

    /// Get the Redis key for a quota
    fn quota_key(&self, quota_id: &str) -> String {
        format!("{}:{}", self.key_prefix, quota_id)
    }

    /// Get the Redis key for quota config
    fn config_key(&self, quota_id: &str) -> String {
        format!("{}:config:{}", self.key_prefix, quota_id)
    }

    /// Set a quota for a user/tenant
    pub async fn set_quota(&mut self, quota_id: &str, config: QuotaConfig) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let config_key = self.config_key(quota_id);

        let serialized = serde_json::to_string(&config)
            .map_err(|e| CelersError::Serialization(e.to_string()))?;

        conn.set::<_, _, ()>(&config_key, serialized)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to set quota config: {}", e)))?;

        Ok(())
    }

    /// Get the quota configuration for a user/tenant
    pub async fn get_config(&mut self, quota_id: &str) -> Result<Option<QuotaConfig>> {
        let mut conn = self.get_connection().await?;
        let config_key = self.config_key(quota_id);

        let data: Option<String> = conn
            .get(&config_key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get quota config: {}", e)))?;

        match data {
            Some(json) => {
                let config: QuotaConfig = serde_json::from_str(&json)
                    .map_err(|e| CelersError::Deserialization(e.to_string()))?;
                Ok(Some(config))
            }
            None => Ok(None),
        }
    }

    /// Check if a quota allows the specified number of tasks
    pub async fn check_quota(&mut self, quota_id: &str, count: u64) -> Result<bool> {
        let usage = self.get_usage(quota_id).await?;

        match usage {
            Some(usage) => Ok(usage.remaining >= count),
            None => Ok(true), // No quota set, allow all
        }
    }

    /// Consume quota (increment usage counter)
    pub async fn consume(&mut self, quota_id: &str, count: u64) -> Result<u64> {
        let config = match self.get_config(quota_id).await? {
            Some(cfg) => cfg,
            None => return Ok(0), // No quota set
        };

        let mut conn = self.get_connection().await?;
        let quota_key = self.quota_key(quota_id);

        // Increment counter with TTL
        let current: u64 = conn
            .incr(&quota_key, count)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to increment quota: {}", e)))?;

        // Set expiry if this is the first increment
        if current == count {
            conn.expire::<_, ()>(&quota_key, config.period.as_seconds() as i64)
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to set quota expiry: {}", e)))?;
        }

        Ok(current)
    }

    /// Get current quota usage
    pub async fn get_usage(&mut self, quota_id: &str) -> Result<Option<QuotaUsage>> {
        let config = match self.get_config(quota_id).await? {
            Some(cfg) => cfg,
            None => return Ok(None),
        };

        let mut conn = self.get_connection().await?;
        let quota_key = self.quota_key(quota_id);

        let current_usage: u64 = conn.get(&quota_key).await.unwrap_or(0);

        let ttl: i64 = conn
            .ttl(&quota_key)
            .await
            .unwrap_or(config.period.as_seconds() as i64);

        let max_allowed = if config.allow_burst {
            config.max_tasks + config.burst_limit
        } else {
            config.max_tasks
        };

        let remaining = max_allowed.saturating_sub(current_usage);
        let usage_percentage = (current_usage as f64 / max_allowed as f64) * 100.0;
        let is_exceeded = current_usage >= max_allowed;

        Ok(Some(QuotaUsage {
            quota_id: quota_id.to_string(),
            current_usage,
            max_allowed,
            remaining,
            usage_percentage,
            period: config.period,
            is_exceeded,
            reset_in_seconds: ttl.max(0) as u64,
        }))
    }

    /// Reset a quota (clear usage counter)
    pub async fn reset(&mut self, quota_id: &str) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let quota_key = self.quota_key(quota_id);

        conn.del::<_, ()>(&quota_key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to reset quota: {}", e)))?;

        Ok(())
    }

    /// Delete a quota configuration
    pub async fn delete_quota(&mut self, quota_id: &str) -> Result<()> {
        let mut conn = self.get_connection().await?;
        let config_key = self.config_key(quota_id);
        let quota_key = self.quota_key(quota_id);

        redis::pipe()
            .del(&config_key)
            .del(&quota_key)
            .query_async::<redis::Value>(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to delete quota: {}", e)))?;

        Ok(())
    }

    /// Get usage for all quotas
    pub async fn get_all_usage(&mut self) -> Result<HashMap<String, QuotaUsage>> {
        let mut conn = self.get_connection().await?;
        let pattern = format!("{}:config:*", self.key_prefix);

        let keys: Vec<String> = conn
            .keys(&pattern)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get quota keys: {}", e)))?;

        let mut usage_map = HashMap::new();

        for key in keys {
            // Extract quota_id from key
            if let Some(quota_id) = key.strip_prefix(&format!("{}:config:", self.key_prefix)) {
                if let Some(usage) = self.get_usage(quota_id).await? {
                    usage_map.insert(quota_id.to_string(), usage);
                }
            }
        }

        Ok(usage_map)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_quota_period_as_seconds() {
        assert_eq!(QuotaPeriod::Minute.as_seconds(), 60);
        assert_eq!(QuotaPeriod::Hour.as_seconds(), 3600);
        assert_eq!(QuotaPeriod::Day.as_seconds(), 86400);
        assert_eq!(QuotaPeriod::Week.as_seconds(), 604800);
        assert_eq!(QuotaPeriod::Month.as_seconds(), 2592000);
    }

    #[test]
    fn test_quota_period_name() {
        assert_eq!(QuotaPeriod::Minute.name(), "minute");
        assert_eq!(QuotaPeriod::Hour.name(), "hour");
        assert_eq!(QuotaPeriod::Day.name(), "day");
    }

    #[test]
    fn test_quota_config_default() {
        let config = QuotaConfig::default();
        assert_eq!(config.max_tasks, 1000);
        assert_eq!(config.period, QuotaPeriod::Hour);
        assert!(!config.allow_burst);
        assert_eq!(config.burst_limit, 0);
    }

    #[test]
    fn test_quota_config_builder() {
        let config = QuotaConfig::new()
            .with_max_tasks(500)
            .with_period(QuotaPeriod::Day)
            .with_burst(100);

        assert_eq!(config.max_tasks, 500);
        assert_eq!(config.period, QuotaPeriod::Day);
        assert!(config.allow_burst);
        assert_eq!(config.burst_limit, 100);
    }

    #[test]
    fn test_quota_usage_is_nearly_exhausted() {
        let usage = QuotaUsage {
            quota_id: "test".to_string(),
            current_usage: 850,
            max_allowed: 1000,
            remaining: 150,
            usage_percentage: 85.0,
            period: QuotaPeriod::Hour,
            is_exceeded: false,
            reset_in_seconds: 3600,
        };

        assert!(usage.is_nearly_exhausted());
        assert!(usage.is_available());
    }

    #[test]
    fn test_quota_usage_is_exceeded() {
        let usage = QuotaUsage {
            quota_id: "test".to_string(),
            current_usage: 1100,
            max_allowed: 1000,
            remaining: 0,
            usage_percentage: 110.0,
            period: QuotaPeriod::Hour,
            is_exceeded: true,
            reset_in_seconds: 3600,
        };

        assert!(!usage.is_available());
        assert!(usage.is_nearly_exhausted());
    }

    #[test]
    fn test_quota_config_serialization() {
        let config = QuotaConfig::new()
            .with_max_tasks(500)
            .with_period(QuotaPeriod::Day);

        let json = serde_json::to_string(&config).unwrap();
        let deserialized: QuotaConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.max_tasks, 500);
        assert_eq!(deserialized.period, QuotaPeriod::Day);
    }
}
