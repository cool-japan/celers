//! Bulkhead pattern implementation for resource isolation
//!
//! The bulkhead pattern isolates resources to prevent cascading failures.
//! Each bulkhead has a limited number of concurrent operations it can handle.
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::bulkhead::{Bulkhead, BulkheadConfig};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let config = BulkheadConfig::new()
//!         .with_max_concurrent(10)
//!         .with_max_wait_duration_ms(5000);
//!
//!     let bulkhead = Bulkhead::new("api_calls", config);
//!
//!     // Try to acquire a permit
//!     if let Some(_permit) = bulkhead.try_acquire() {
//!         // Do work while holding the permit
//!         perform_api_call().await?;
//!         // Permit is automatically released when dropped
//!     } else {
//!         println!("Bulkhead full, rejecting request");
//!     }
//!
//!     Ok(())
//! }
//!
//! async fn perform_api_call() -> Result<(), Box<dyn std::error::Error>> {
//!     Ok(())
//! }
//! ```

use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::time::timeout;

/// Configuration for a bulkhead
#[derive(Debug, Clone)]
pub struct BulkheadConfig {
    /// Maximum number of concurrent operations
    pub max_concurrent: usize,
    /// Maximum time to wait for a permit (in milliseconds)
    pub max_wait_duration_ms: u64,
}

impl BulkheadConfig {
    /// Create a new bulkhead configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the maximum number of concurrent operations
    pub fn with_max_concurrent(mut self, max: usize) -> Self {
        self.max_concurrent = max;
        self
    }

    /// Set the maximum wait duration in milliseconds
    pub fn with_max_wait_duration_ms(mut self, duration_ms: u64) -> Self {
        self.max_wait_duration_ms = duration_ms;
        self
    }
}

impl Default for BulkheadConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 10,
            max_wait_duration_ms: 5000,
        }
    }
}

/// Bulkhead for limiting concurrent operations
#[derive(Clone)]
pub struct Bulkhead {
    name: String,
    semaphore: Arc<Semaphore>,
    config: BulkheadConfig,
}

/// A permit to use the bulkhead
pub struct BulkheadPermit<'a> {
    #[allow(dead_code)]
    permit: SemaphorePermit<'a>,
    bulkhead_name: String,
}

impl<'a> Drop for BulkheadPermit<'a> {
    fn drop(&mut self) {
        tracing::trace!("Released permit for bulkhead: {}", self.bulkhead_name);
    }
}

impl Bulkhead {
    /// Create a new bulkhead with the given name and configuration
    pub fn new(name: impl Into<String>, config: BulkheadConfig) -> Self {
        let name = name.into();
        tracing::debug!(
            "Creating bulkhead '{}' with max_concurrent={}",
            name,
            config.max_concurrent
        );

        Self {
            name,
            semaphore: Arc::new(Semaphore::new(config.max_concurrent)),
            config,
        }
    }

    /// Try to acquire a permit without waiting
    ///
    /// Returns `Some(permit)` if a permit is available, `None` otherwise.
    pub fn try_acquire(&self) -> Option<BulkheadPermit<'_>> {
        self.semaphore.try_acquire().ok().map(|permit| {
            tracing::trace!("Acquired permit for bulkhead: {}", self.name);
            BulkheadPermit {
                permit,
                bulkhead_name: self.name.clone(),
            }
        })
    }

    /// Acquire a permit, waiting if necessary
    ///
    /// Returns `Some(permit)` if acquired within the timeout, `None` if timed out.
    pub async fn acquire(&self) -> Option<BulkheadPermit<'_>> {
        let wait_duration = Duration::from_millis(self.config.max_wait_duration_ms);

        match timeout(wait_duration, self.semaphore.acquire()).await {
            Ok(Ok(permit)) => {
                tracing::trace!(
                    "Acquired permit for bulkhead: {} (after waiting)",
                    self.name
                );
                Some(BulkheadPermit {
                    permit,
                    bulkhead_name: self.name.clone(),
                })
            }
            Ok(Err(_)) => {
                tracing::warn!("Semaphore closed for bulkhead: {}", self.name);
                None
            }
            Err(_) => {
                tracing::warn!(
                    "Timeout waiting for permit in bulkhead: {} (waited {} ms)",
                    self.name,
                    self.config.max_wait_duration_ms
                );
                None
            }
        }
    }

    /// Get the number of available permits
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Get the maximum concurrent operations allowed
    pub fn max_concurrent(&self) -> usize {
        self.config.max_concurrent
    }

    /// Get the current usage percentage (0.0 to 1.0)
    pub fn usage(&self) -> f64 {
        let available = self.available_permits();
        let max = self.max_concurrent();
        let used = max.saturating_sub(available);
        used as f64 / max as f64
    }

    /// Check if the bulkhead is at capacity
    pub fn is_full(&self) -> bool {
        self.available_permits() == 0
    }

    /// Get the bulkhead name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get bulkhead statistics
    pub fn stats(&self) -> BulkheadStats {
        BulkheadStats {
            name: self.name.clone(),
            max_concurrent: self.max_concurrent(),
            available: self.available_permits(),
            in_use: self
                .max_concurrent()
                .saturating_sub(self.available_permits()),
            usage_percentage: self.usage() * 100.0,
        }
    }
}

/// Statistics for a bulkhead
#[derive(Debug, Clone)]
pub struct BulkheadStats {
    /// Bulkhead name
    pub name: String,
    /// Maximum concurrent operations
    pub max_concurrent: usize,
    /// Available permits
    pub available: usize,
    /// Permits currently in use
    pub in_use: usize,
    /// Usage percentage (0.0 to 100.0)
    pub usage_percentage: f64,
}

impl BulkheadStats {
    /// Check if the bulkhead is healthy (usage < 80%)
    pub fn is_healthy(&self) -> bool {
        self.usage_percentage < 80.0
    }

    /// Check if the bulkhead is at capacity
    pub fn is_full(&self) -> bool {
        self.available == 0
    }
}

/// Manager for multiple bulkheads
pub struct BulkheadManager {
    bulkheads: std::collections::HashMap<String, Bulkhead>,
}

impl BulkheadManager {
    /// Create a new bulkhead manager
    pub fn new() -> Self {
        Self {
            bulkheads: std::collections::HashMap::new(),
        }
    }

    /// Add a bulkhead to the manager
    pub fn add_bulkhead(&mut self, name: impl Into<String>, config: BulkheadConfig) {
        let name = name.into();
        self.bulkheads
            .insert(name.clone(), Bulkhead::new(name, config));
    }

    /// Get a bulkhead by name
    pub fn get(&self, name: &str) -> Option<&Bulkhead> {
        self.bulkheads.get(name)
    }

    /// Get statistics for all bulkheads
    pub fn all_stats(&self) -> Vec<BulkheadStats> {
        self.bulkheads.values().map(|b| b.stats()).collect()
    }

    /// Check if any bulkhead is unhealthy
    pub fn has_unhealthy_bulkheads(&self) -> bool {
        self.all_stats().iter().any(|s| !s.is_healthy())
    }
}

impl Default for BulkheadManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bulkhead_config_default() {
        let config = BulkheadConfig::default();
        assert_eq!(config.max_concurrent, 10);
        assert_eq!(config.max_wait_duration_ms, 5000);
    }

    #[test]
    fn test_bulkhead_config_builder() {
        let config = BulkheadConfig::new()
            .with_max_concurrent(5)
            .with_max_wait_duration_ms(1000);

        assert_eq!(config.max_concurrent, 5);
        assert_eq!(config.max_wait_duration_ms, 1000);
    }

    #[test]
    fn test_bulkhead_creation() {
        let config = BulkheadConfig::new().with_max_concurrent(3);
        let bulkhead = Bulkhead::new("test", config);

        assert_eq!(bulkhead.name(), "test");
        assert_eq!(bulkhead.max_concurrent(), 3);
        assert_eq!(bulkhead.available_permits(), 3);
    }

    #[test]
    fn test_bulkhead_try_acquire() {
        let config = BulkheadConfig::new().with_max_concurrent(2);
        let bulkhead = Bulkhead::new("test", config);

        let permit1 = bulkhead.try_acquire();
        assert!(permit1.is_some());
        assert_eq!(bulkhead.available_permits(), 1);

        let permit2 = bulkhead.try_acquire();
        assert!(permit2.is_some());
        assert_eq!(bulkhead.available_permits(), 0);

        let permit3 = bulkhead.try_acquire();
        assert!(permit3.is_none());
    }

    #[test]
    fn test_bulkhead_usage() {
        let config = BulkheadConfig::new().with_max_concurrent(10);
        let bulkhead = Bulkhead::new("test", config);

        assert_eq!(bulkhead.usage(), 0.0);

        let _permit1 = bulkhead.try_acquire();
        assert_eq!(bulkhead.usage(), 0.1);

        let _permit2 = bulkhead.try_acquire();
        assert_eq!(bulkhead.usage(), 0.2);
    }

    #[test]
    fn test_bulkhead_stats() {
        let config = BulkheadConfig::new().with_max_concurrent(10);
        let bulkhead = Bulkhead::new("test", config);

        let _permit = bulkhead.try_acquire();

        let stats = bulkhead.stats();
        assert_eq!(stats.name, "test");
        assert_eq!(stats.max_concurrent, 10);
        assert_eq!(stats.available, 9);
        assert_eq!(stats.in_use, 1);
        assert_eq!(stats.usage_percentage, 10.0);
        assert!(stats.is_healthy());
        assert!(!stats.is_full());
    }

    #[test]
    fn test_bulkhead_manager() {
        let mut manager = BulkheadManager::new();

        manager.add_bulkhead("api", BulkheadConfig::new().with_max_concurrent(5));
        manager.add_bulkhead("db", BulkheadConfig::new().with_max_concurrent(10));

        assert!(manager.get("api").is_some());
        assert!(manager.get("db").is_some());
        assert!(manager.get("cache").is_none());

        let stats = manager.all_stats();
        assert_eq!(stats.len(), 2);
    }

    #[tokio::test]
    async fn test_bulkhead_acquire_with_timeout() {
        let config = BulkheadConfig::new()
            .with_max_concurrent(1)
            .with_max_wait_duration_ms(100);
        let bulkhead = Bulkhead::new("test", config);

        let _permit1 = bulkhead.try_acquire().unwrap();

        // This should timeout
        let permit2 = bulkhead.acquire().await;
        assert!(permit2.is_none());
    }

    #[tokio::test]
    async fn test_bulkhead_permit_release() {
        let config = BulkheadConfig::new().with_max_concurrent(1);
        let bulkhead = Bulkhead::new("test", config);

        {
            let _permit = bulkhead.try_acquire().unwrap();
            assert_eq!(bulkhead.available_permits(), 0);
        }

        // Permit should be released
        assert_eq!(bulkhead.available_permits(), 1);
    }
}
