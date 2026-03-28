//! Redis-backed distributed lock backend
//!
//! Uses Redis atomic operations (SET NX EX, Lua scripts) to provide
//! distributed lock management across multiple beat scheduler instances.

use async_trait::async_trait;
use celers_core::error::CelersError;
use celers_core::lock::DistributedLockBackend;
use redis::AsyncCommands;

/// Redis-backed distributed lock backend.
///
/// Uses Redis atomic operations for reliable distributed locking:
/// - `SET key value NX EX ttl` for atomic acquire
/// - Lua scripts for safe release and renewal (owner verification)
/// - `SCAN` + owner check for `release_all`
///
/// # Key Format
///
/// Lock keys are prefixed with a configurable prefix (default: `"celers:beat:lock:"`).
/// For example, a lock for task `"send_report"` uses key `"celers:beat:lock:send_report"`.
///
/// # Example
///
/// ```ignore
/// use celers_backend_redis::lock::RedisLockBackend;
/// use celers_core::lock::DistributedLockBackend;
///
/// let backend = RedisLockBackend::new("redis://localhost").unwrap();
/// let acquired = backend.try_acquire("my_task", "scheduler-1", 300).await.unwrap();
/// ```
#[derive(Debug)]
pub struct RedisLockBackend {
    client: redis::Client,
    prefix: String,
}

impl RedisLockBackend {
    /// Create a new Redis lock backend with default prefix.
    ///
    /// # Arguments
    /// * `redis_url` - Redis connection URL (e.g., `"redis://localhost"`)
    ///
    /// # Errors
    /// Returns an error if the Redis URL is invalid.
    pub fn new(redis_url: &str) -> celers_core::error::Result<Self> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| CelersError::Configuration(format!("Invalid Redis URL: {}", e)))?;
        Ok(Self {
            client,
            prefix: "celers:beat:lock:".to_string(),
        })
    }

    /// Create a new Redis lock backend with a custom prefix.
    ///
    /// # Arguments
    /// * `redis_url` - Redis connection URL
    /// * `prefix` - Custom key prefix for lock keys
    pub fn with_prefix(redis_url: &str, prefix: String) -> celers_core::error::Result<Self> {
        let client = redis::Client::open(redis_url)
            .map_err(|e| CelersError::Configuration(format!("Invalid Redis URL: {}", e)))?;
        Ok(Self { client, prefix })
    }

    /// Build the full Redis key for a lock
    fn lock_key(&self, key: &str) -> String {
        format!("{}{}", self.prefix, key)
    }

    /// Get an async connection from the client
    async fn get_conn(&self) -> celers_core::error::Result<redis::aio::MultiplexedConnection> {
        self.client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Redis connection failed: {}", e)))
    }
}

/// Lua script for safe release: only delete if the current value matches the owner.
///
/// ```lua
/// if redis.call('get', KEYS[1]) == ARGV[1] then
///     return redis.call('del', KEYS[1])
/// else
///     return 0
/// end
/// ```
const RELEASE_SCRIPT: &str = r#"
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
else
    return 0
end
"#;

/// Lua script for safe renewal: only extend TTL if the current value matches the owner.
///
/// ```lua
/// if redis.call('get', KEYS[1]) == ARGV[1] then
///     return redis.call('pexpire', KEYS[1], ARGV[2])
/// else
///     return 0
/// end
/// ```
const RENEW_SCRIPT: &str = r#"
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('pexpire', KEYS[1], ARGV[2])
else
    return 0
end
"#;

#[async_trait]
impl DistributedLockBackend for RedisLockBackend {
    async fn try_acquire(
        &self,
        key: &str,
        owner: &str,
        ttl_secs: u64,
    ) -> celers_core::error::Result<bool> {
        let mut conn = self.get_conn().await?;
        let lock_key = self.lock_key(key);

        // First check if we already own the lock
        let current: Option<String> = conn
            .get(&lock_key)
            .await
            .map_err(|e| CelersError::Broker(format!("Redis GET failed: {}", e)))?;

        if let Some(ref current_owner) = current {
            if current_owner == owner {
                // We already own it, just refresh TTL
                let _: () = conn
                    .pexpire(&lock_key, (ttl_secs * 1000) as i64)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Redis PEXPIRE failed: {}", e)))?;
                return Ok(true);
            }
        }

        // Try atomic SET NX EX
        let result: Option<String> = redis::cmd("SET")
            .arg(&lock_key)
            .arg(owner)
            .arg("NX")
            .arg("EX")
            .arg(ttl_secs)
            .query_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Redis SET NX EX failed: {}", e)))?;

        // Redis returns "OK" on success, nil on failure
        Ok(result.is_some())
    }

    async fn release(&self, key: &str, owner: &str) -> celers_core::error::Result<bool> {
        let mut conn = self.get_conn().await?;
        let lock_key = self.lock_key(key);

        let result: i64 = redis::Script::new(RELEASE_SCRIPT)
            .key(&lock_key)
            .arg(owner)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Redis release script failed: {}", e)))?;

        Ok(result == 1)
    }

    async fn renew(
        &self,
        key: &str,
        owner: &str,
        ttl_secs: u64,
    ) -> celers_core::error::Result<bool> {
        let mut conn = self.get_conn().await?;
        let lock_key = self.lock_key(key);
        let ttl_ms = ttl_secs * 1000;

        let result: i64 = redis::Script::new(RENEW_SCRIPT)
            .key(&lock_key)
            .arg(owner)
            .arg(ttl_ms)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CelersError::Broker(format!("Redis renew script failed: {}", e)))?;

        Ok(result == 1)
    }

    async fn is_locked(&self, key: &str) -> celers_core::error::Result<bool> {
        let mut conn = self.get_conn().await?;
        let lock_key = self.lock_key(key);

        let exists: bool = conn
            .exists(&lock_key)
            .await
            .map_err(|e| CelersError::Broker(format!("Redis EXISTS failed: {}", e)))?;

        Ok(exists)
    }

    async fn owner(&self, key: &str) -> celers_core::error::Result<Option<String>> {
        let mut conn = self.get_conn().await?;
        let lock_key = self.lock_key(key);

        let owner: Option<String> = conn
            .get(&lock_key)
            .await
            .map_err(|e| CelersError::Broker(format!("Redis GET failed: {}", e)))?;

        Ok(owner)
    }

    async fn release_all(&self, owner: &str) -> celers_core::error::Result<u64> {
        let mut conn = self.get_conn().await?;
        let pattern = format!("{}*", self.prefix);
        let mut released: u64 = 0;
        let mut cursor: u64 = 0;

        loop {
            let (next_cursor, keys): (u64, Vec<String>) = redis::cmd("SCAN")
                .arg(cursor)
                .arg("MATCH")
                .arg(&pattern)
                .arg("COUNT")
                .arg(100)
                .query_async(&mut conn)
                .await
                .map_err(|e| CelersError::Broker(format!("Redis SCAN failed: {}", e)))?;

            for key in &keys {
                let current_owner: Option<String> = conn
                    .get(key)
                    .await
                    .map_err(|e| CelersError::Broker(format!("Redis GET failed: {}", e)))?;

                if current_owner.as_deref() == Some(owner) {
                    // Use the release script to ensure atomicity
                    let result: i64 = redis::Script::new(RELEASE_SCRIPT)
                        .key(key)
                        .arg(owner)
                        .invoke_async(&mut conn)
                        .await
                        .map_err(|e| {
                            CelersError::Broker(format!("Redis release script failed: {}", e))
                        })?;
                    if result == 1 {
                        released += 1;
                    }
                }
            }

            cursor = next_cursor;
            if cursor == 0 {
                break;
            }
        }

        Ok(released)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_key_format() {
        let backend = RedisLockBackend::new("redis://localhost").expect("valid URL");
        assert_eq!(backend.lock_key("my_task"), "celers:beat:lock:my_task");
    }

    #[test]
    fn test_custom_prefix() {
        let backend =
            RedisLockBackend::with_prefix("redis://localhost", "custom:prefix:".to_string())
                .expect("valid URL");
        assert_eq!(backend.lock_key("task"), "custom:prefix:task");
    }

    #[test]
    fn test_invalid_redis_url() {
        let result = RedisLockBackend::new("not-a-valid-url");
        assert!(result.is_err());
    }

    // Integration tests require a running Redis instance
    #[tokio::test]
    #[ignore]
    async fn test_redis_acquire_release() {
        let backend = RedisLockBackend::new("redis://localhost").expect("valid URL");

        let acquired = backend
            .try_acquire("test_lock_1", "owner_1", 60)
            .await
            .expect("acquire should succeed");
        assert!(acquired);

        let locked = backend
            .is_locked("test_lock_1")
            .await
            .expect("is_locked should succeed");
        assert!(locked);

        let owner = backend
            .owner("test_lock_1")
            .await
            .expect("owner should succeed");
        assert_eq!(owner.as_deref(), Some("owner_1"));

        let released = backend
            .release("test_lock_1", "owner_1")
            .await
            .expect("release should succeed");
        assert!(released);

        let locked = backend
            .is_locked("test_lock_1")
            .await
            .expect("is_locked should succeed");
        assert!(!locked);
    }

    #[tokio::test]
    #[ignore]
    async fn test_redis_contention() {
        let backend = RedisLockBackend::new("redis://localhost").expect("valid URL");

        // Clean up first
        let _ = backend.release("test_lock_2", "owner_a").await;
        let _ = backend.release("test_lock_2", "owner_b").await;

        let a = backend
            .try_acquire("test_lock_2", "owner_a", 60)
            .await
            .expect("acquire should succeed");
        assert!(a);

        let b = backend
            .try_acquire("test_lock_2", "owner_b", 60)
            .await
            .expect("acquire should succeed");
        assert!(!b);

        // Release by wrong owner fails
        let released = backend
            .release("test_lock_2", "owner_b")
            .await
            .expect("release should succeed");
        assert!(!released);

        // Release by correct owner succeeds
        let released = backend
            .release("test_lock_2", "owner_a")
            .await
            .expect("release should succeed");
        assert!(released);
    }

    #[tokio::test]
    #[ignore]
    async fn test_redis_renew() {
        let backend = RedisLockBackend::new("redis://localhost").expect("valid URL");

        let _ = backend.release("test_lock_3", "owner_1").await;

        let _ = backend
            .try_acquire("test_lock_3", "owner_1", 10)
            .await
            .expect("acquire");

        let renewed = backend
            .renew("test_lock_3", "owner_1", 300)
            .await
            .expect("renew");
        assert!(renewed);

        // Wrong owner cannot renew
        let renewed_bad = backend
            .renew("test_lock_3", "owner_2", 300)
            .await
            .expect("renew");
        assert!(!renewed_bad);

        let _ = backend.release("test_lock_3", "owner_1").await;
    }

    #[tokio::test]
    #[ignore]
    async fn test_redis_release_all() {
        let backend = RedisLockBackend::new("redis://localhost").expect("valid URL");

        let _ = backend
            .try_acquire("test_ra_1", "owner_bulk", 60)
            .await
            .expect("acquire");
        let _ = backend
            .try_acquire("test_ra_2", "owner_bulk", 60)
            .await
            .expect("acquire");
        let _ = backend
            .try_acquire("test_ra_3", "other_owner", 60)
            .await
            .expect("acquire");

        let count = backend
            .release_all("owner_bulk")
            .await
            .expect("release_all");
        assert_eq!(count, 2);

        // other_owner's lock should still exist
        let locked = backend.is_locked("test_ra_3").await.expect("is_locked");
        assert!(locked);

        let _ = backend.release("test_ra_3", "other_owner").await;
    }
}
