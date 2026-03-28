//! Distributed lock backend trait for beat scheduler
//!
//! Provides the `DistributedLockBackend` trait that enables distributed lock
//! management across multiple beat scheduler instances. Implementations can
//! use Redis, databases, or in-memory stores as the backing mechanism.

use async_trait::async_trait;

/// Backend for distributed lock management.
///
/// Used by the beat scheduler to prevent duplicate task execution across
/// multiple scheduler instances. Each implementation provides atomic
/// lock operations with TTL-based expiration.
///
/// # Lock Semantics
///
/// - Locks are identified by a string key (typically the task name)
/// - Each lock has an owner (typically the scheduler instance ID)
/// - Locks expire after a configurable TTL to prevent deadlocks
/// - Only the owner can release or renew a lock
///
/// # Example
///
/// ```ignore
/// use celers_core::lock::DistributedLockBackend;
///
/// async fn schedule_task(backend: &dyn DistributedLockBackend) {
///     let acquired = backend.try_acquire("my_task", "scheduler-1", 300).await.unwrap();
///     if acquired {
///         // Execute the task
///         // ...
///         backend.release("my_task", "scheduler-1").await.unwrap();
///     }
/// }
/// ```
#[async_trait]
pub trait DistributedLockBackend: Send + Sync + std::fmt::Debug {
    /// Try to acquire a lock. Returns `Ok(true)` if acquired successfully.
    ///
    /// If the lock is already held by the same owner, this should return `Ok(true)`.
    /// If the lock is held by a different owner and has not expired, returns `Ok(false)`.
    /// If the lock has expired, it may be acquired by the new owner.
    ///
    /// # Arguments
    /// * `key` - Lock identifier (typically the task name)
    /// * `owner` - Owner identifier (typically the scheduler instance ID)
    /// * `ttl_secs` - Time-to-live for the lock in seconds
    async fn try_acquire(
        &self,
        key: &str,
        owner: &str,
        ttl_secs: u64,
    ) -> crate::error::Result<bool>;

    /// Release a lock. Returns `Ok(true)` if released (was owned by this owner).
    ///
    /// Only the current owner can release a lock. Returns `Ok(false)` if the
    /// lock does not exist or is owned by a different owner.
    ///
    /// # Arguments
    /// * `key` - Lock identifier
    /// * `owner` - Owner identifier (must match the lock's owner)
    async fn release(&self, key: &str, owner: &str) -> crate::error::Result<bool>;

    /// Renew/extend a lock's TTL. Returns `Ok(true)` if renewed.
    ///
    /// Only the current owner can renew a lock. The lock must not have expired.
    /// Returns `Ok(false)` if the lock does not exist, is owned by a different
    /// owner, or has already expired.
    ///
    /// # Arguments
    /// * `key` - Lock identifier
    /// * `owner` - Owner identifier (must match the lock's owner)
    /// * `ttl_secs` - New time-to-live for the lock in seconds
    async fn renew(&self, key: &str, owner: &str, ttl_secs: u64) -> crate::error::Result<bool>;

    /// Check if a lock is currently held by anyone.
    ///
    /// Returns `Ok(true)` if the lock exists and has not expired.
    async fn is_locked(&self, key: &str) -> crate::error::Result<bool>;

    /// Get the current owner of a lock, if any.
    ///
    /// Returns `Ok(None)` if the lock does not exist or has expired.
    async fn owner(&self, key: &str) -> crate::error::Result<Option<String>>;

    /// Release all locks owned by a specific owner.
    ///
    /// Returns the number of locks that were released. This is useful for
    /// cleanup when a scheduler instance shuts down.
    async fn release_all(&self, owner: &str) -> crate::error::Result<u64>;
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper to verify that trait objects can be created
    #[test]
    fn test_trait_is_object_safe() {
        fn _assert_object_safe(_: &dyn DistributedLockBackend) {}
    }
}
