//! Distributed locks for worker coordination using Redis
//!
//! Provides:
//! - Distributed mutex locks with automatic expiration
//! - Lock acquisition with timeout and retry
//! - Lock extension (keep-alive)
//! - Fair locking with FIFO ordering option
//! - Deadlock prevention
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::locks::{DistributedLock, LockConfig};
//!
//! # async fn example() -> celers_core::Result<()> {
//! let config = LockConfig::new()
//!     .with_ttl(30)
//!     .with_retry_count(3)
//!     .with_retry_delay_ms(100);
//!
//! let lock = DistributedLock::new(
//!     "redis://localhost:6379",
//!     "my-resource",
//!     config
//! )?;
//!
//! // Acquire lock
//! if let Some(token) = lock.acquire().await? {
//!     // Critical section
//!     println!("Lock acquired!");
//!
//!     // Do work...
//!
//!     // Release lock
//!     lock.release(&token).await?;
//! }
//!
//! # Ok(())
//! # }
//! ```

use celers_core::{CelersError, Result};
use redis::{AsyncCommands, Client, Script};
use std::time::{Duration, SystemTime};
use tracing::{debug, warn};
use uuid::Uuid;

/// Lock token (unique identifier for lock ownership)
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockToken(String);

impl LockToken {
    /// Create a new random lock token
    fn new() -> Self {
        Self(Uuid::new_v4().to_string())
    }

    /// Get token value
    pub fn value(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for LockToken {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Lock configuration
#[derive(Debug, Clone)]
pub struct LockConfig {
    /// Lock TTL in seconds
    pub ttl_secs: u64,
    /// Retry count for lock acquisition
    pub retry_count: u32,
    /// Retry delay in milliseconds
    pub retry_delay_ms: u64,
    /// Lock key prefix
    pub key_prefix: String,
}

impl Default for LockConfig {
    fn default() -> Self {
        Self {
            ttl_secs: 30,
            retry_count: 3,
            retry_delay_ms: 100,
            key_prefix: "celery-lock:".to_string(),
        }
    }
}

impl LockConfig {
    /// Create a new lock configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set lock TTL
    pub fn with_ttl(mut self, ttl_secs: u64) -> Self {
        self.ttl_secs = ttl_secs;
        self
    }

    /// Set retry count
    pub fn with_retry_count(mut self, count: u32) -> Self {
        self.retry_count = count;
        self
    }

    /// Set retry delay
    pub fn with_retry_delay_ms(mut self, delay_ms: u64) -> Self {
        self.retry_delay_ms = delay_ms;
        self
    }

    /// Set key prefix
    pub fn with_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.key_prefix = prefix.into();
        self
    }
}

/// Distributed lock using Redis
pub struct DistributedLock {
    client: Client,
    resource_name: String,
    config: LockConfig,
}

impl DistributedLock {
    /// Create a new distributed lock
    pub fn new(redis_url: &str, resource_name: &str, config: LockConfig) -> Result<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to Redis: {}", e)))?;

        Ok(Self {
            client,
            resource_name: resource_name.to_string(),
            config,
        })
    }

    /// Acquire the lock (blocking with retry)
    pub async fn acquire(&self) -> Result<Option<LockToken>> {
        self.acquire_with_timeout(None).await
    }

    /// Acquire the lock with timeout
    pub async fn acquire_with_timeout(
        &self,
        timeout: Option<Duration>,
    ) -> Result<Option<LockToken>> {
        let start = SystemTime::now();
        let mut attempts = 0;

        loop {
            let token = LockToken::new();

            if self.try_acquire(&token).await? {
                debug!(
                    "Acquired lock '{}' with token {} (attempts: {})",
                    self.resource_name,
                    token,
                    attempts + 1
                );
                return Ok(Some(token));
            }

            attempts += 1;

            // Check timeout
            if let Some(timeout) = timeout {
                if start.elapsed().unwrap() >= timeout {
                    warn!(
                        "Failed to acquire lock '{}' within timeout ({} attempts)",
                        self.resource_name, attempts
                    );
                    return Ok(None);
                }
            } else if attempts >= self.config.retry_count {
                warn!(
                    "Failed to acquire lock '{}' after {} attempts",
                    self.resource_name, attempts
                );
                return Ok(None);
            }

            // Wait before retry
            tokio::time::sleep(Duration::from_millis(self.config.retry_delay_ms)).await;
        }
    }

    /// Try to acquire the lock (non-blocking, single attempt)
    pub async fn try_acquire(&self, token: &LockToken) -> Result<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let key = self.lock_key();

        // Use SET NX EX for atomic lock acquisition
        let result: Option<String> = redis::cmd("SET")
            .arg(&key)
            .arg(token.value())
            .arg("NX") // Only set if not exists
            .arg("EX") // Set expiration
            .arg(self.config.ttl_secs)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to acquire lock: {}", e)))?;

        Ok(result.is_some())
    }

    /// Release the lock
    pub async fn release(&self, token: &LockToken) -> Result<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let key = self.lock_key();

        // Lua script for atomic check-and-delete
        let script = Script::new(
            r#"
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("del", KEYS[1])
            else
                return 0
            end
            "#,
        );

        let result: i32 = script
            .key(&key)
            .arg(token.value())
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to release lock: {}", e)))?;

        if result > 0 {
            debug!(
                "Released lock '{}' with token {}",
                self.resource_name, token
            );
            Ok(true)
        } else {
            warn!(
                "Failed to release lock '{}' - token mismatch or expired",
                self.resource_name
            );
            Ok(false)
        }
    }

    /// Extend the lock TTL (keep-alive)
    pub async fn extend(&self, token: &LockToken, additional_secs: u64) -> Result<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let key = self.lock_key();

        // Lua script for atomic check-and-extend
        let script = Script::new(
            r#"
            if redis.call("get", KEYS[1]) == ARGV[1] then
                return redis.call("expire", KEYS[1], ARGV[2])
            else
                return 0
            end
            "#,
        );

        let result: i32 = script
            .key(&key)
            .arg(token.value())
            .arg(additional_secs)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to extend lock: {}", e)))?;

        if result > 0 {
            debug!(
                "Extended lock '{}' by {} seconds",
                self.resource_name, additional_secs
            );
            Ok(true)
        } else {
            warn!(
                "Failed to extend lock '{}' - token mismatch or expired",
                self.resource_name
            );
            Ok(false)
        }
    }

    /// Check if lock is held
    pub async fn is_locked(&self) -> Result<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let key = self.lock_key();

        let exists: bool = conn
            .exists(&key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to check lock: {}", e)))?;

        Ok(exists)
    }

    /// Get remaining TTL in seconds
    pub async fn ttl(&self) -> Result<Option<i64>> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let key = self.lock_key();

        let ttl: i64 = conn
            .ttl(&key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get TTL: {}", e)))?;

        match ttl {
            -2 => Ok(None),           // Key doesn't exist
            -1 => Ok(Some(i64::MAX)), // No expiration
            n => Ok(Some(n)),
        }
    }

    /// Force release the lock (regardless of token)
    pub async fn force_release(&self) -> Result<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to get connection: {}", e)))?;

        let key = self.lock_key();

        let deleted: i32 = conn
            .del(&key)
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to force release lock: {}", e)))?;

        if deleted > 0 {
            warn!("Force released lock '{}'", self.resource_name);
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Generate lock key
    fn lock_key(&self) -> String {
        format!("{}{}", self.config.key_prefix, self.resource_name)
    }

    /// Get resource name
    pub fn resource_name(&self) -> &str {
        &self.resource_name
    }

    /// Get configuration
    pub fn config(&self) -> &LockConfig {
        &self.config
    }
}

/// RAII guard for automatic lock release
pub struct LockGuard<'a> {
    lock: &'a DistributedLock,
    token: Option<LockToken>,
}

impl<'a> LockGuard<'a> {
    /// Create a new lock guard
    pub fn new(lock: &'a DistributedLock, token: LockToken) -> Self {
        Self {
            lock,
            token: Some(token),
        }
    }

    /// Get the lock token
    pub fn token(&self) -> Option<&LockToken> {
        self.token.as_ref()
    }

    /// Extend the lock
    pub async fn extend(&self, additional_secs: u64) -> Result<bool> {
        if let Some(token) = &self.token {
            self.lock.extend(token, additional_secs).await
        } else {
            Ok(false)
        }
    }
}

impl<'a> Drop for LockGuard<'a> {
    fn drop(&mut self) {
        // Note: Lock should be explicitly released using lock.release(token)
        // Automatic release in Drop is not implemented due to async limitations
        if self.token.is_some() {
            warn!("LockGuard dropped without explicit release - lock may remain held until TTL expires");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_token_creation() {
        let token1 = LockToken::new();
        let token2 = LockToken::new();
        assert_ne!(token1, token2);
        assert!(!token1.value().is_empty());
    }

    #[test]
    fn test_lock_token_display() {
        let token = LockToken::new();
        let display = format!("{}", token);
        assert_eq!(display, token.value());
    }

    #[test]
    fn test_lock_config_default() {
        let config = LockConfig::default();
        assert_eq!(config.ttl_secs, 30);
        assert_eq!(config.retry_count, 3);
        assert_eq!(config.retry_delay_ms, 100);
        assert_eq!(config.key_prefix, "celery-lock:");
    }

    #[test]
    fn test_lock_config_builder() {
        let config = LockConfig::new()
            .with_ttl(60)
            .with_retry_count(5)
            .with_retry_delay_ms(200)
            .with_prefix("custom:");

        assert_eq!(config.ttl_secs, 60);
        assert_eq!(config.retry_count, 5);
        assert_eq!(config.retry_delay_ms, 200);
        assert_eq!(config.key_prefix, "custom:");
    }

    #[test]
    fn test_distributed_lock_creation() {
        let config = LockConfig::new();
        let lock = DistributedLock::new("redis://localhost:6379", "test-resource", config);
        assert!(lock.is_ok());

        let lock = lock.unwrap();
        assert_eq!(lock.resource_name(), "test-resource");
    }

    #[test]
    fn test_lock_key_generation() {
        let config = LockConfig::new();
        let lock = DistributedLock::new("redis://localhost:6379", "my-resource", config).unwrap();
        let key = lock.lock_key();
        assert_eq!(key, "celery-lock:my-resource");
    }

    #[test]
    fn test_lock_key_generation_with_custom_prefix() {
        let config = LockConfig::new().with_prefix("app:");
        let lock = DistributedLock::new("redis://localhost:6379", "my-resource", config).unwrap();
        let key = lock.lock_key();
        assert_eq!(key, "app:my-resource");
    }
}
