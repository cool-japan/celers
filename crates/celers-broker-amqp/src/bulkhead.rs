//! Bulkhead pattern for resource isolation
//!
//! This module implements the bulkhead pattern to isolate resources and prevent
//! cascading failures. Like compartments in a ship, resource pools are isolated
//! so that failure in one doesn't affect others.
//!
//! # Use Cases
//!
//! - **Resource Isolation**: Separate connection pools for different message types
//! - **Concurrency Control**: Limit concurrent operations to prevent overload
//! - **Fair Resource Sharing**: Ensure no single operation consumes all resources
//!
//! # Examples
//!
//! ```
//! use celers_broker_amqp::bulkhead::{Bulkhead, BulkheadConfig};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! // Create a bulkhead with 10 concurrent permits
//! let bulkhead = Bulkhead::new(10);
//!
//! // Execute operation with resource isolation
//! let result = bulkhead.call(async {
//!     // Your operation here
//!     Ok::<_, String>(42)
//! }).await;
//!
//! match result {
//!     Ok(value) => println!("Operation completed: {}", value),
//!     Err(e) => println!("Operation failed: {:?}", e),
//! }
//! # Ok(())
//! # }
//! ```

use serde::{Deserialize, Serialize};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{Semaphore, SemaphorePermit};
use tokio::time::timeout;

/// Bulkhead configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkheadConfig {
    /// Maximum concurrent permits
    pub max_concurrent: usize,
    /// Maximum wait time for permit acquisition
    pub max_wait_time: Option<Duration>,
}

impl Default for BulkheadConfig {
    fn default() -> Self {
        Self {
            max_concurrent: 10,
            max_wait_time: Some(Duration::from_secs(30)),
        }
    }
}

/// Bulkhead error
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum BulkheadError<E> {
    /// No permits available and wait timeout exceeded
    PermitTimeout,
    /// Bulkhead is at capacity
    AtCapacity,
    /// Operation failed
    OperationFailed(E),
}

impl<E: std::fmt::Display> std::fmt::Display for BulkheadError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BulkheadError::PermitTimeout => write!(f, "Bulkhead permit acquisition timeout"),
            BulkheadError::AtCapacity => write!(f, "Bulkhead at capacity"),
            BulkheadError::OperationFailed(e) => write!(f, "Operation failed: {}", e),
        }
    }
}

impl<E: std::error::Error + 'static> std::error::Error for BulkheadError<E> {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BulkheadError::OperationFailed(e) => Some(e),
            _ => None,
        }
    }
}

/// Bulkhead statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BulkheadStats {
    /// Maximum concurrent permits
    pub max_concurrent: usize,
    /// Currently available permits
    pub available_permits: usize,
    /// Currently active operations
    pub active_operations: usize,
    /// Total successful operations
    pub successful_operations: u64,
    /// Total failed operations
    pub failed_operations: u64,
    /// Total rejected operations (timeout/capacity)
    pub rejected_operations: u64,
    /// Resource utilization (0.0-1.0)
    pub utilization: f64,
}

impl BulkheadStats {
    /// Get total operations attempted
    pub fn total_operations(&self) -> u64 {
        self.successful_operations + self.failed_operations + self.rejected_operations
    }

    /// Get success rate
    pub fn success_rate(&self) -> f64 {
        let total = self.total_operations();
        if total == 0 {
            return 0.0;
        }
        self.successful_operations as f64 / total as f64
    }

    /// Get failure rate
    pub fn failure_rate(&self) -> f64 {
        let total = self.total_operations();
        if total == 0 {
            return 0.0;
        }
        self.failed_operations as f64 / total as f64
    }

    /// Get rejection rate
    pub fn rejection_rate(&self) -> f64 {
        let total = self.total_operations();
        if total == 0 {
            return 0.0;
        }
        self.rejected_operations as f64 / total as f64
    }

    /// Check if bulkhead is at capacity
    pub fn is_at_capacity(&self) -> bool {
        self.available_permits == 0
    }

    /// Check if bulkhead is underutilized
    pub fn is_underutilized(&self, threshold: f64) -> bool {
        self.utilization < threshold
    }

    /// Check if bulkhead is overutilized
    pub fn is_overutilized(&self, threshold: f64) -> bool {
        self.utilization > threshold
    }
}

#[derive(Debug)]
struct BulkheadState {
    successful_operations: u64,
    failed_operations: u64,
    rejected_operations: u64,
}

/// Bulkhead for resource isolation and concurrency control
///
/// Uses semaphores to limit concurrent operations, preventing resource exhaustion
/// and ensuring fair resource allocation.
#[derive(Debug, Clone)]
pub struct Bulkhead {
    config: BulkheadConfig,
    semaphore: Arc<Semaphore>,
    state: Arc<tokio::sync::Mutex<BulkheadState>>,
}

impl Bulkhead {
    /// Create a new bulkhead with specified concurrency limit
    ///
    /// # Arguments
    ///
    /// * `max_concurrent` - Maximum number of concurrent operations
    ///
    /// # Examples
    ///
    /// ```
    /// use celers_broker_amqp::bulkhead::Bulkhead;
    ///
    /// let bulkhead = Bulkhead::new(10);
    /// ```
    pub fn new(max_concurrent: usize) -> Self {
        Self::with_config(BulkheadConfig {
            max_concurrent,
            ..Default::default()
        })
    }

    /// Create with custom configuration
    pub fn with_config(config: BulkheadConfig) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(config.max_concurrent)),
            config,
            state: Arc::new(tokio::sync::Mutex::new(BulkheadState {
                successful_operations: 0,
                failed_operations: 0,
                rejected_operations: 0,
            })),
        }
    }

    /// Try to acquire a permit without blocking
    ///
    /// Returns `Some(permit)` if successful, `None` if no permits available
    pub fn try_acquire(&self) -> Option<SemaphorePermit<'_>> {
        self.semaphore.try_acquire().ok()
    }

    /// Execute an operation through the bulkhead
    ///
    /// Acquires a permit before executing the operation. If no permits are available,
    /// waits up to `max_wait_time` (if configured) before failing.
    pub async fn call<F, T, E>(&self, operation: F) -> Result<T, BulkheadError<E>>
    where
        F: Future<Output = Result<T, E>>,
    {
        // Acquire permit with optional timeout
        let permit = if let Some(wait_time) = self.config.max_wait_time {
            match timeout(wait_time, self.semaphore.acquire()).await {
                Ok(Ok(permit)) => permit,
                Ok(Err(_)) => {
                    let mut state = self.state.lock().await;
                    state.rejected_operations += 1;
                    return Err(BulkheadError::PermitTimeout);
                }
                Err(_) => {
                    let mut state = self.state.lock().await;
                    state.rejected_operations += 1;
                    return Err(BulkheadError::PermitTimeout);
                }
            }
        } else {
            match self.semaphore.acquire().await {
                Ok(permit) => permit,
                Err(_) => {
                    let mut state = self.state.lock().await;
                    state.rejected_operations += 1;
                    return Err(BulkheadError::PermitTimeout);
                }
            }
        };

        // Execute operation
        let result = operation.await;

        // Update statistics
        let mut state = self.state.lock().await;
        match &result {
            Ok(_) => state.successful_operations += 1,
            Err(_) => state.failed_operations += 1,
        }
        drop(state);

        // Release permit
        drop(permit);

        result.map_err(BulkheadError::OperationFailed)
    }

    /// Execute an operation with a try_call pattern (non-blocking)
    ///
    /// Returns immediately if no permits are available
    pub async fn try_call<F, T, E>(&self, operation: F) -> Result<T, BulkheadError<E>>
    where
        F: Future<Output = Result<T, E>>,
    {
        // Try to acquire permit
        let permit = match self.semaphore.try_acquire() {
            Ok(permit) => permit,
            Err(_) => {
                let mut state = self.state.lock().await;
                state.rejected_operations += 1;
                return Err(BulkheadError::AtCapacity);
            }
        };

        // Execute operation
        let result = operation.await;

        // Update statistics
        let mut state = self.state.lock().await;
        match &result {
            Ok(_) => state.successful_operations += 1,
            Err(_) => state.failed_operations += 1,
        }
        drop(state);

        // Release permit
        drop(permit);

        result.map_err(BulkheadError::OperationFailed)
    }

    /// Get current statistics
    pub async fn statistics(&self) -> BulkheadStats {
        let state = self.state.lock().await;
        let available = self.semaphore.available_permits();
        let active = self.config.max_concurrent - available;
        let utilization = active as f64 / self.config.max_concurrent as f64;

        BulkheadStats {
            max_concurrent: self.config.max_concurrent,
            available_permits: available,
            active_operations: active,
            successful_operations: state.successful_operations,
            failed_operations: state.failed_operations,
            rejected_operations: state.rejected_operations,
            utilization,
        }
    }

    /// Get number of available permits
    pub fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    /// Get maximum concurrent operations
    pub fn max_concurrent(&self) -> usize {
        self.config.max_concurrent
    }

    /// Check if bulkhead is at capacity
    pub fn is_at_capacity(&self) -> bool {
        self.semaphore.available_permits() == 0
    }

    /// Reset statistics
    pub async fn reset_statistics(&self) {
        let mut state = self.state.lock().await;
        state.successful_operations = 0;
        state.failed_operations = 0;
        state.rejected_operations = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Instant;

    #[tokio::test]
    async fn test_bulkhead_basic() {
        let bulkhead = Bulkhead::new(5);
        assert_eq!(bulkhead.max_concurrent(), 5);
        assert_eq!(bulkhead.available_permits(), 5);
        assert!(!bulkhead.is_at_capacity());
    }

    #[tokio::test]
    async fn test_bulkhead_call_success() {
        let bulkhead = Bulkhead::new(5);

        let result = bulkhead.call(async { Ok::<i32, String>(42) }).await;

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 42);

        let stats = bulkhead.statistics().await;
        assert_eq!(stats.successful_operations, 1);
        assert_eq!(stats.failed_operations, 0);
    }

    #[tokio::test]
    async fn test_bulkhead_call_failure() {
        let bulkhead = Bulkhead::new(5);

        let result = bulkhead
            .call(async { Err::<i32, String>("error".to_string()) })
            .await;

        assert!(result.is_err());

        let stats = bulkhead.statistics().await;
        assert_eq!(stats.successful_operations, 0);
        assert_eq!(stats.failed_operations, 1);
    }

    #[tokio::test]
    async fn test_bulkhead_concurrency_limit() {
        let bulkhead = Arc::new(Bulkhead::new(3));
        let counter = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));

        let mut handles = vec![];

        for _ in 0..10 {
            let bulkhead = Arc::clone(&bulkhead);
            let counter = Arc::clone(&counter);
            let max_concurrent = Arc::clone(&max_concurrent);

            let handle = tokio::spawn(async move {
                bulkhead
                    .call(async {
                        let current = counter.fetch_add(1, Ordering::SeqCst) + 1;
                        max_concurrent.fetch_max(current, Ordering::SeqCst);

                        tokio::time::sleep(Duration::from_millis(50)).await;

                        counter.fetch_sub(1, Ordering::SeqCst);
                        Ok::<(), String>(())
                    })
                    .await
            });

            handles.push(handle);
        }

        for handle in handles {
            handle.await.unwrap().unwrap();
        }

        // Should never exceed max concurrent
        let max = max_concurrent.load(Ordering::SeqCst);
        assert!(max <= 3, "Max concurrent was {}, expected <= 3", max);

        let stats = bulkhead.statistics().await;
        assert_eq!(stats.successful_operations, 10);
    }

    #[tokio::test]
    async fn test_bulkhead_try_call_at_capacity() {
        let bulkhead = Arc::new(Bulkhead::new(2));

        // Acquire all permits
        let permit1 = bulkhead.try_acquire().unwrap();
        let permit2 = bulkhead.try_acquire().unwrap();

        assert!(bulkhead.is_at_capacity());

        // Try to execute should fail immediately
        let result = bulkhead.try_call(async { Ok::<i32, String>(42) }).await;

        assert!(matches!(result, Err(BulkheadError::AtCapacity)));

        let stats = bulkhead.statistics().await;
        assert_eq!(stats.rejected_operations, 1);

        drop(permit1);
        drop(permit2);
    }

    #[tokio::test]
    async fn test_bulkhead_timeout() {
        let bulkhead = Arc::new(Bulkhead::with_config(BulkheadConfig {
            max_concurrent: 1,
            max_wait_time: Some(Duration::from_millis(100)),
        }));

        // Hold the permit
        let _permit = bulkhead.try_acquire().unwrap();

        // This should timeout
        let start = Instant::now();
        let result = bulkhead.call(async { Ok::<i32, String>(42) }).await;
        let elapsed = start.elapsed();

        assert!(matches!(result, Err(BulkheadError::PermitTimeout)));
        assert!(elapsed >= Duration::from_millis(90));
        assert!(elapsed <= Duration::from_millis(200));
    }

    #[tokio::test]
    async fn test_bulkhead_statistics() {
        let bulkhead = Bulkhead::new(5);

        // Execute some operations
        let _ = bulkhead.call(async { Ok::<i32, String>(1) }).await;
        let _ = bulkhead.call(async { Ok::<i32, String>(2) }).await;
        let _ = bulkhead
            .call(async { Err::<i32, String>("error".to_string()) })
            .await;

        let stats = bulkhead.statistics().await;
        assert_eq!(stats.max_concurrent, 5);
        assert_eq!(stats.successful_operations, 2);
        assert_eq!(stats.failed_operations, 1);
        assert_eq!(stats.total_operations(), 3);
        assert!((stats.success_rate() - 0.666).abs() < 0.01);
        assert!((stats.failure_rate() - 0.333).abs() < 0.01);
    }

    #[tokio::test]
    async fn test_bulkhead_stats_helpers() {
        let bulkhead = Bulkhead::new(5);

        let _ = bulkhead.call(async { Ok::<i32, String>(1) }).await;
        let _ = bulkhead
            .call(async { Err::<i32, String>("error".to_string()) })
            .await;

        let stats = bulkhead.statistics().await;
        assert_eq!(stats.total_operations(), 2);
        assert_eq!(stats.success_rate(), 0.5);
        assert_eq!(stats.failure_rate(), 0.5);
        assert_eq!(stats.rejection_rate(), 0.0);
        assert!(!stats.is_at_capacity());
        assert!(stats.is_underutilized(0.5));
        assert!(!stats.is_overutilized(0.5));
    }

    #[tokio::test]
    async fn test_bulkhead_reset_statistics() {
        let bulkhead = Bulkhead::new(5);

        let _ = bulkhead.call(async { Ok::<i32, String>(1) }).await;
        let _ = bulkhead
            .call(async { Err::<i32, String>("error".to_string()) })
            .await;

        let stats = bulkhead.statistics().await;
        assert_eq!(stats.total_operations(), 2);

        bulkhead.reset_statistics().await;

        let stats = bulkhead.statistics().await;
        assert_eq!(stats.total_operations(), 0);
    }

    #[tokio::test]
    async fn test_bulkhead_utilization() {
        let bulkhead = Arc::new(Bulkhead::new(10));

        // Acquire 5 permits
        let permits: Vec<_> = (0..5).map(|_| bulkhead.try_acquire().unwrap()).collect();

        let stats = bulkhead.statistics().await;
        assert_eq!(stats.active_operations, 5);
        assert_eq!(stats.available_permits, 5);
        assert_eq!(stats.utilization, 0.5);

        drop(permits);
    }
}
