//! Lock management for schedule execution
//!
//! Provides `ScheduleLock` and `LockManager` for preventing duplicate
//! task execution in distributed scenarios.

use crate::config::ScheduleError;
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Schedule lock for preventing duplicate execution in distributed scenarios
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleLock {
    /// Task name this lock is for
    pub task_name: String,
    /// Lock owner identifier (e.g., scheduler instance ID)
    pub owner: String,
    /// When the lock was acquired
    pub acquired_at: DateTime<Utc>,
    /// When the lock expires (for automatic cleanup)
    pub expires_at: DateTime<Utc>,
    /// Lock renewal count (for debugging)
    pub renewal_count: u32,
}

impl ScheduleLock {
    /// Create a new schedule lock
    ///
    /// # Arguments
    /// * `task_name` - Name of the task to lock
    /// * `owner` - Identifier of the lock owner (e.g., scheduler instance ID)
    /// * `ttl_seconds` - Time-to-live for the lock in seconds
    pub fn new(task_name: String, owner: String, ttl_seconds: u64) -> Self {
        let now = Utc::now();
        Self {
            task_name,
            owner,
            acquired_at: now,
            expires_at: now + Duration::seconds(ttl_seconds as i64),
            renewal_count: 0,
        }
    }

    /// Check if the lock has expired
    pub fn is_expired(&self) -> bool {
        Utc::now() > self.expires_at
    }

    /// Check if the lock is owned by the given owner
    pub fn is_owned_by(&self, owner: &str) -> bool {
        self.owner == owner
    }

    /// Renew the lock for another TTL period
    ///
    /// # Arguments
    /// * `ttl_seconds` - Time-to-live for the renewed lock
    ///
    /// # Returns
    /// * `Ok(())` if renewed successfully
    /// * `Err(ScheduleError)` if the lock has already expired
    pub fn renew(&mut self, ttl_seconds: u64) -> Result<(), ScheduleError> {
        if self.is_expired() {
            return Err(ScheduleError::Invalid(
                "Cannot renew expired lock".to_string(),
            ));
        }

        self.expires_at = Utc::now() + Duration::seconds(ttl_seconds as i64);
        self.renewal_count += 1;
        Ok(())
    }

    /// Get remaining time until expiration
    pub fn ttl(&self) -> Duration {
        self.expires_at - Utc::now()
    }

    /// Get age of the lock since acquisition
    pub fn age(&self) -> Duration {
        Utc::now() - self.acquired_at
    }
}

/// Lock manager for schedule locks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LockManager {
    /// Active locks by task name
    pub(crate) locks: HashMap<String, ScheduleLock>,
    /// Default lock TTL in seconds
    pub(crate) default_ttl: u64,
}

impl LockManager {
    /// Create a new lock manager
    ///
    /// # Arguments
    /// * `default_ttl` - Default time-to-live for locks in seconds
    pub fn new(default_ttl: u64) -> Self {
        Self {
            locks: HashMap::new(),
            default_ttl,
        }
    }

    /// Try to acquire a lock for a task
    ///
    /// # Arguments
    /// * `task_name` - Name of the task to lock
    /// * `owner` - Identifier of the lock owner
    /// * `ttl` - Optional custom TTL (uses default if None)
    ///
    /// # Returns
    /// * `Ok(true)` if lock acquired successfully
    /// * `Ok(false)` if lock is already held by another owner
    /// * `Err` on other errors
    pub fn try_acquire(
        &mut self,
        task_name: &str,
        owner: &str,
        ttl: Option<u64>,
    ) -> Result<bool, ScheduleError> {
        // Clean up expired locks first
        self.cleanup_expired();

        // Check if lock exists and is not expired
        if let Some(existing_lock) = self.locks.get(task_name) {
            if !existing_lock.is_expired() {
                // Lock is held by someone else
                if !existing_lock.is_owned_by(owner) {
                    return Ok(false);
                }
                // Lock is already held by us, consider it acquired
                return Ok(true);
            }
        }

        // Acquire the lock
        let ttl_seconds = ttl.unwrap_or(self.default_ttl);
        let lock = ScheduleLock::new(task_name.to_string(), owner.to_string(), ttl_seconds);
        self.locks.insert(task_name.to_string(), lock);
        Ok(true)
    }

    /// Release a lock
    ///
    /// # Arguments
    /// * `task_name` - Name of the task to unlock
    /// * `owner` - Identifier of the lock owner (must match)
    ///
    /// # Returns
    /// * `Ok(true)` if lock was released
    /// * `Ok(false)` if lock doesn't exist or is owned by someone else
    pub fn release(&mut self, task_name: &str, owner: &str) -> Result<bool, ScheduleError> {
        if let Some(lock) = self.locks.get(task_name) {
            if lock.is_owned_by(owner) {
                self.locks.remove(task_name);
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Renew an existing lock
    ///
    /// # Arguments
    /// * `task_name` - Name of the task
    /// * `owner` - Identifier of the lock owner (must match)
    /// * `ttl` - Optional custom TTL (uses default if None)
    ///
    /// # Returns
    /// * `Ok(true)` if lock was renewed
    /// * `Ok(false)` if lock doesn't exist, is owned by someone else, or has expired
    pub fn renew(
        &mut self,
        task_name: &str,
        owner: &str,
        ttl: Option<u64>,
    ) -> Result<bool, ScheduleError> {
        if let Some(lock) = self.locks.get_mut(task_name) {
            if lock.is_owned_by(owner) && !lock.is_expired() {
                let ttl_seconds = ttl.unwrap_or(self.default_ttl);
                lock.renew(ttl_seconds)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Check if a lock is held
    ///
    /// # Arguments
    /// * `task_name` - Name of the task
    ///
    /// # Returns
    /// * `true` if lock exists and is not expired
    /// * `false` otherwise
    pub fn is_locked(&self, task_name: &str) -> bool {
        if let Some(lock) = self.locks.get(task_name) {
            !lock.is_expired()
        } else {
            false
        }
    }

    /// Get information about a lock
    pub fn get_lock(&self, task_name: &str) -> Option<&ScheduleLock> {
        self.locks.get(task_name)
    }

    /// Clean up expired locks
    pub fn cleanup_expired(&mut self) {
        self.locks.retain(|_, lock| !lock.is_expired());
    }

    /// Get all active locks
    pub fn get_active_locks(&self) -> Vec<&ScheduleLock> {
        self.locks
            .values()
            .filter(|lock| !lock.is_expired())
            .collect()
    }

    /// Force release all locks (use with caution)
    pub fn release_all(&mut self) {
        self.locks.clear();
    }
}

impl Default for LockManager {
    fn default() -> Self {
        Self::new(300) // Default 5 minute TTL
    }
}

// ---------------------------------------------------------------------------
// InMemoryLockBackend - implements DistributedLockBackend for single-process use
// ---------------------------------------------------------------------------

use async_trait::async_trait;
use celers_core::lock::DistributedLockBackend;
use std::sync::Arc;
use std::time::{Duration as StdDuration, Instant};
use tokio::sync::RwLock;

/// Entry in the in-memory lock store
#[derive(Debug, Clone)]
struct LockEntry {
    /// Owner of this lock
    owner: String,
    /// When the lock was acquired
    acquired_at: Instant,
    /// Time-to-live for the lock
    ttl: StdDuration,
}

impl LockEntry {
    /// Check if the lock entry has expired
    fn is_expired(&self) -> bool {
        self.acquired_at.elapsed() >= self.ttl
    }
}

/// In-memory distributed lock backend.
///
/// This backend stores locks in a `HashMap` guarded by a `tokio::sync::RwLock`.
/// It is suitable for single-process deployments or testing. For true distributed
/// locking across multiple processes/hosts, use `RedisLockBackend` or `DbLockBackend`.
///
/// Expired locks are lazily cleaned up on each operation.
///
/// # Example
///
/// ```rust
/// use celers_beat::lock::InMemoryLockBackend;
/// use celers_core::lock::DistributedLockBackend;
///
/// # #[tokio::main]
/// # async fn main() {
/// let backend = InMemoryLockBackend::new();
/// let acquired = backend.try_acquire("my_task", "scheduler-1", 300).await.unwrap();
/// assert!(acquired);
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct InMemoryLockBackend {
    locks: Arc<RwLock<HashMap<String, LockEntry>>>,
}

impl InMemoryLockBackend {
    /// Create a new in-memory lock backend
    pub fn new() -> Self {
        Self {
            locks: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the number of currently held (non-expired) locks
    pub async fn active_lock_count(&self) -> usize {
        let locks = self.locks.read().await;
        locks.values().filter(|e| !e.is_expired()).count()
    }
}

impl Default for InMemoryLockBackend {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DistributedLockBackend for InMemoryLockBackend {
    async fn try_acquire(
        &self,
        key: &str,
        owner: &str,
        ttl_secs: u64,
    ) -> celers_core::error::Result<bool> {
        let mut locks = self.locks.write().await;

        // Clean up expired entries lazily
        locks.retain(|_, entry| !entry.is_expired());

        // Check existing lock
        if let Some(existing) = locks.get(key) {
            if !existing.is_expired() {
                // Already owned by same owner => success
                if existing.owner == owner {
                    return Ok(true);
                }
                // Held by another owner
                return Ok(false);
            }
        }

        // Acquire the lock
        locks.insert(
            key.to_string(),
            LockEntry {
                owner: owner.to_string(),
                acquired_at: Instant::now(),
                ttl: StdDuration::from_secs(ttl_secs),
            },
        );
        Ok(true)
    }

    async fn release(&self, key: &str, owner: &str) -> celers_core::error::Result<bool> {
        let mut locks = self.locks.write().await;

        if let Some(entry) = locks.get(key) {
            if entry.owner == owner {
                locks.remove(key);
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn renew(
        &self,
        key: &str,
        owner: &str,
        ttl_secs: u64,
    ) -> celers_core::error::Result<bool> {
        let mut locks = self.locks.write().await;

        if let Some(entry) = locks.get_mut(key) {
            if entry.owner == owner && !entry.is_expired() {
                entry.acquired_at = Instant::now();
                entry.ttl = StdDuration::from_secs(ttl_secs);
                return Ok(true);
            }
        }
        Ok(false)
    }

    async fn is_locked(&self, key: &str) -> celers_core::error::Result<bool> {
        let locks = self.locks.read().await;
        if let Some(entry) = locks.get(key) {
            Ok(!entry.is_expired())
        } else {
            Ok(false)
        }
    }

    async fn owner(&self, key: &str) -> celers_core::error::Result<Option<String>> {
        let locks = self.locks.read().await;
        if let Some(entry) = locks.get(key) {
            if !entry.is_expired() {
                return Ok(Some(entry.owner.clone()));
            }
        }
        Ok(None)
    }

    async fn release_all(&self, owner: &str) -> celers_core::error::Result<u64> {
        let mut locks = self.locks.write().await;
        let before = locks.len();
        locks.retain(|_, entry| entry.owner != owner);
        let after = locks.len();
        Ok((before - after) as u64)
    }
}

#[cfg(test)]
mod in_memory_lock_tests {
    use super::*;

    #[tokio::test]
    async fn test_acquire_and_release() {
        let backend = InMemoryLockBackend::new();

        // Acquire a lock
        let acquired = backend.try_acquire("task1", "owner1", 300).await;
        assert!(acquired.is_ok());
        assert!(acquired.as_ref().is_ok_and(|v| *v));

        // Verify it's locked
        let locked = backend.is_locked("task1").await;
        assert!(locked.is_ok_and(|v| v));

        // Verify owner
        let owner = backend.owner("task1").await;
        assert!(owner.is_ok());
        assert_eq!(
            owner.as_ref().ok().and_then(|o| o.as_deref()),
            Some("owner1")
        );

        // Release
        let released = backend.release("task1", "owner1").await;
        assert!(released.is_ok_and(|v| v));

        // No longer locked
        let locked = backend.is_locked("task1").await;
        assert!(locked.is_ok_and(|v| !v));
    }

    #[tokio::test]
    async fn test_acquire_fails_for_different_owner() {
        let backend = InMemoryLockBackend::new();

        let acquired = backend.try_acquire("task1", "owner1", 300).await;
        assert!(acquired.is_ok_and(|v| v));

        // Different owner cannot acquire
        let acquired2 = backend.try_acquire("task1", "owner2", 300).await;
        assert!(acquired2.is_ok_and(|v| !v));
    }

    #[tokio::test]
    async fn test_same_owner_can_reacquire() {
        let backend = InMemoryLockBackend::new();

        let acquired = backend.try_acquire("task1", "owner1", 300).await;
        assert!(acquired.is_ok_and(|v| v));

        // Same owner can re-acquire (idempotent)
        let acquired2 = backend.try_acquire("task1", "owner1", 300).await;
        assert!(acquired2.is_ok_and(|v| v));
    }

    #[tokio::test]
    async fn test_release_wrong_owner_fails() {
        let backend = InMemoryLockBackend::new();

        let _ = backend.try_acquire("task1", "owner1", 300).await;

        // Wrong owner cannot release
        let released = backend.release("task1", "owner2").await;
        assert!(released.is_ok_and(|v| !v));

        // Still locked
        let locked = backend.is_locked("task1").await;
        assert!(locked.is_ok_and(|v| v));
    }

    #[tokio::test]
    async fn test_renew() {
        let backend = InMemoryLockBackend::new();

        let _ = backend.try_acquire("task1", "owner1", 300).await;

        // Renew by correct owner
        let renewed = backend.renew("task1", "owner1", 600).await;
        assert!(renewed.is_ok_and(|v| v));

        // Renew by wrong owner fails
        let renewed2 = backend.renew("task1", "owner2", 600).await;
        assert!(renewed2.is_ok_and(|v| !v));
    }

    #[tokio::test]
    async fn test_renew_nonexistent_fails() {
        let backend = InMemoryLockBackend::new();

        let renewed = backend.renew("nonexistent", "owner1", 600).await;
        assert!(renewed.is_ok_and(|v| !v));
    }

    #[tokio::test]
    async fn test_expiry() {
        let backend = InMemoryLockBackend::new();

        // Acquire with 0-second TTL (immediately expired)
        // Note: 0 TTL means Duration::from_secs(0), so elapsed() >= 0 is always true
        let _ = backend.try_acquire("task1", "owner1", 0).await;

        // Should be expired immediately
        let locked = backend.is_locked("task1").await;
        assert!(locked.is_ok_and(|v| !v));

        // Another owner can now acquire
        let acquired = backend.try_acquire("task1", "owner2", 300).await;
        assert!(acquired.is_ok_and(|v| v));
    }

    #[tokio::test]
    async fn test_release_all() {
        let backend = InMemoryLockBackend::new();

        let _ = backend.try_acquire("task1", "owner1", 300).await;
        let _ = backend.try_acquire("task2", "owner1", 300).await;
        let _ = backend.try_acquire("task3", "owner2", 300).await;

        // Release all for owner1
        let count = backend.release_all("owner1").await;
        assert!(count.is_ok());
        assert_eq!(count.ok(), Some(2));

        // owner2's lock still held
        let locked = backend.is_locked("task3").await;
        assert!(locked.is_ok_and(|v| v));

        // owner1's locks gone
        let locked1 = backend.is_locked("task1").await;
        assert!(locked1.is_ok_and(|v| !v));
    }

    #[tokio::test]
    async fn test_owner_of_nonexistent() {
        let backend = InMemoryLockBackend::new();

        let owner = backend.owner("nonexistent").await;
        assert!(owner.is_ok());
        assert_eq!(owner.ok().flatten(), None);
    }

    #[tokio::test]
    async fn test_release_nonexistent() {
        let backend = InMemoryLockBackend::new();

        let released = backend.release("nonexistent", "owner1").await;
        assert!(released.is_ok_and(|v| !v));
    }

    #[tokio::test]
    async fn test_active_lock_count() {
        let backend = InMemoryLockBackend::new();

        let _ = backend.try_acquire("task1", "owner1", 300).await;
        let _ = backend.try_acquire("task2", "owner1", 300).await;

        assert_eq!(backend.active_lock_count().await, 2);

        let _ = backend.release("task1", "owner1").await;
        assert_eq!(backend.active_lock_count().await, 1);
    }

    #[tokio::test]
    async fn test_multiple_keys_independent() {
        let backend = InMemoryLockBackend::new();

        let _ = backend.try_acquire("task_a", "owner1", 300).await;
        let _ = backend.try_acquire("task_b", "owner2", 300).await;

        // Both should be locked
        assert!(backend.is_locked("task_a").await.is_ok_and(|v| v));
        assert!(backend.is_locked("task_b").await.is_ok_and(|v| v));

        // Different owners
        let owner_a = backend.owner("task_a").await;
        let owner_b = backend.owner("task_b").await;
        assert_eq!(owner_a.ok().flatten().as_deref(), Some("owner1"));
        assert_eq!(owner_b.ok().flatten().as_deref(), Some("owner2"));
    }
}
