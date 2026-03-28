//! Worker coordination for distributed systems
//!
//! This module provides distributed coordination capabilities for workers:
//! - **Leader Election**: Elect a leader worker for singleton tasks
//! - **Distributed Locks**: Acquire exclusive locks for task execution
//! - **Worker Registration**: Register and discover workers in a cluster
//! - **Load Balancing**: Balance load across available workers
//!
//! # Example
//!
//! ```ignore
//! # #[cfg(feature = "redis")]
//! # async fn example() -> celers_core::Result<()> {
//! use celers_worker::worker_coordination::{WorkerCoordinator, CoordinatorConfig};
//!
//! let config = CoordinatorConfig {
//!     redis_url: "redis://127.0.0.1:6379".to_string(),
//!     worker_id: "worker-1".to_string(),
//!     ..Default::default()
//! };
//!
//! let coordinator = WorkerCoordinator::new(config).await?;
//!
//! // Try to become leader
//! if coordinator.try_become_leader("my_singleton_task").await? {
//!     println!("I am the leader!");
//! }
//! # Ok(())
//! # }
//! ```

use async_trait::async_trait;
#[cfg(feature = "redis")]
use celers_core::CelersError;
use celers_core::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
#[allow(unused_imports)]
use tracing::{debug, info};

/// Configuration for worker coordination
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CoordinatorConfig {
    /// Redis connection URL
    pub redis_url: String,

    /// Key prefix for Redis keys
    pub key_prefix: String,

    /// Worker ID (unique identifier for this worker)
    pub worker_id: String,

    /// Worker hostname
    pub worker_hostname: String,

    /// Heartbeat interval in seconds
    pub heartbeat_interval_secs: u64,

    /// Worker TTL in seconds (for registration)
    pub worker_ttl_secs: u64,

    /// Lock TTL in seconds (for distributed locks)
    pub lock_ttl_secs: u64,

    /// Leader TTL in seconds (for leader election)
    pub leader_ttl_secs: u64,
}

impl Default for CoordinatorConfig {
    fn default() -> Self {
        Self {
            redis_url: "redis://127.0.0.1:6379".to_string(),
            key_prefix: "celery:coordination".to_string(),
            worker_id: uuid::Uuid::new_v4().to_string(),
            worker_hostname: hostname::get()
                .unwrap_or_else(|_| std::ffi::OsString::from("unknown"))
                .to_string_lossy()
                .to_string(),
            heartbeat_interval_secs: 30,
            worker_ttl_secs: 60,
            lock_ttl_secs: 30,
            leader_ttl_secs: 60,
        }
    }
}

impl CoordinatorConfig {
    /// Create a new configuration
    pub fn new(redis_url: impl Into<String>, worker_id: impl Into<String>) -> Self {
        Self {
            redis_url: redis_url.into(),
            worker_id: worker_id.into(),
            ..Default::default()
        }
    }

    /// Validate the configuration
    pub fn validate(&self) -> std::result::Result<(), String> {
        if self.redis_url.is_empty() {
            return Err("Redis URL cannot be empty".to_string());
        }
        if self.worker_id.is_empty() {
            return Err("Worker ID cannot be empty".to_string());
        }
        if self.heartbeat_interval_secs == 0 {
            return Err("Heartbeat interval must be greater than 0".to_string());
        }
        if self.worker_ttl_secs == 0 {
            return Err("Worker TTL must be greater than 0".to_string());
        }
        Ok(())
    }
}

/// Worker metadata for registration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerMetadata {
    /// Worker ID
    pub worker_id: String,

    /// Worker hostname
    pub hostname: String,

    /// Registration timestamp
    pub registered_at: u64,

    /// Last heartbeat timestamp
    pub last_heartbeat: u64,

    /// Worker capabilities (tags)
    pub capabilities: Vec<String>,

    /// Current load (number of active tasks)
    pub current_load: usize,

    /// Maximum capacity
    pub max_capacity: usize,
}

impl WorkerMetadata {
    /// Create new worker metadata
    pub fn new(worker_id: impl Into<String>, hostname: impl Into<String>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            worker_id: worker_id.into(),
            hostname: hostname.into(),
            registered_at: now,
            last_heartbeat: now,
            capabilities: Vec::new(),
            current_load: 0,
            max_capacity: 100,
        }
    }

    /// Check if worker is alive based on heartbeat
    pub fn is_alive(&self, ttl_secs: u64) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        now.saturating_sub(self.last_heartbeat) <= ttl_secs
    }

    /// Get load percentage
    pub fn load_percentage(&self) -> f64 {
        if self.max_capacity == 0 {
            return 100.0;
        }
        (self.current_load as f64 / self.max_capacity as f64) * 100.0
    }

    /// Check if worker has capacity
    pub fn has_capacity(&self) -> bool {
        self.current_load < self.max_capacity
    }
}

/// Trait for worker coordinators
#[async_trait]
pub trait Coordinator: Send + Sync {
    /// Register this worker
    async fn register(&self) -> Result<()>;

    /// Unregister this worker
    async fn unregister(&self) -> Result<()>;

    /// Send heartbeat
    async fn heartbeat(&self) -> Result<()>;

    /// Get all registered workers
    async fn get_workers(&self) -> Result<Vec<WorkerMetadata>>;

    /// Try to become leader for a task type
    async fn try_become_leader(&self, task_type: &str) -> Result<bool>;

    /// Check if this worker is the leader for a task type
    async fn is_leader(&self, task_type: &str) -> Result<bool>;

    /// Release leadership for a task type
    async fn release_leadership(&self, task_type: &str) -> Result<()>;

    /// Acquire a distributed lock
    async fn acquire_lock(&self, lock_name: &str, timeout_secs: u64) -> Result<bool>;

    /// Release a distributed lock
    async fn release_lock(&self, lock_name: &str) -> Result<()>;

    /// Get the worker with the least load
    async fn get_least_loaded_worker(&self) -> Result<Option<WorkerMetadata>>;
}

#[cfg(feature = "redis")]
/// Redis-based worker coordinator
pub struct WorkerCoordinator {
    config: CoordinatorConfig,
    client: redis::Client,
    metadata: Arc<RwLock<WorkerMetadata>>,
}

#[cfg(feature = "redis")]
impl WorkerCoordinator {
    /// Create a new worker coordinator
    pub async fn new(config: CoordinatorConfig) -> Result<Self> {
        config.validate().map_err(CelersError::Other)?;

        let client = redis::Client::open(config.redis_url.as_str())
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        // Test connection
        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        redis::cmd("PING")
            .query_async::<String>(&mut conn)
            .await
            .map_err(|e| CelersError::Other(format!("Redis ping failed: {}", e)))?;

        let metadata =
            WorkerMetadata::new(config.worker_id.clone(), config.worker_hostname.clone());

        info!(
            "Worker coordinator initialized for worker {}",
            config.worker_id
        );

        Ok(Self {
            config,
            client,
            metadata: Arc::new(RwLock::new(metadata)),
        })
    }

    /// Get the Redis key for worker registration
    fn workers_key(&self) -> String {
        format!("{}:workers", self.config.key_prefix)
    }

    /// Get the Redis key for a specific worker
    fn worker_key(&self, worker_id: &str) -> String {
        format!("{}:worker:{}", self.config.key_prefix, worker_id)
    }

    /// Get the Redis key for leader election
    fn leader_key(&self, task_type: &str) -> String {
        format!("{}:leader:{}", self.config.key_prefix, task_type)
    }

    /// Get the Redis key for a distributed lock
    fn lock_key(&self, lock_name: &str) -> String {
        format!("{}:lock:{}", self.config.key_prefix, lock_name)
    }

    /// Update worker metadata
    pub async fn update_metadata<F>(&self, f: F)
    where
        F: FnOnce(&mut WorkerMetadata),
    {
        let mut metadata = self.metadata.write().await;
        f(&mut metadata);
    }

    /// Get worker metadata
    pub async fn get_metadata(&self) -> WorkerMetadata {
        self.metadata.read().await.clone()
    }
}

#[cfg(feature = "redis")]
#[async_trait]
impl Coordinator for WorkerCoordinator {
    async fn register(&self) -> Result<()> {
        let metadata = self.metadata.read().await;
        let metadata_json = serde_json::to_string(&*metadata)
            .map_err(|e| CelersError::Other(format!("Serialization error: {}", e)))?;

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        let worker_key = self.worker_key(&self.config.worker_id);
        let workers_key = self.workers_key();

        // Store worker metadata with TTL
        redis::pipe()
            .set(&worker_key, &metadata_json)
            .expire(&worker_key, self.config.worker_ttl_secs as i64)
            .sadd(&workers_key, &self.config.worker_id)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| CelersError::Other(format!("Redis pipeline error: {}", e)))?;

        info!("Worker {} registered", self.config.worker_id);
        Ok(())
    }

    async fn unregister(&self) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        let worker_key = self.worker_key(&self.config.worker_id);
        let workers_key = self.workers_key();

        redis::pipe()
            .del(&worker_key)
            .srem(&workers_key, &self.config.worker_id)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| CelersError::Other(format!("Redis pipeline error: {}", e)))?;

        info!("Worker {} unregistered", self.config.worker_id);
        Ok(())
    }

    async fn heartbeat(&self) -> Result<()> {
        let mut metadata = self.metadata.write().await;
        metadata.last_heartbeat = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let metadata_json = serde_json::to_string(&*metadata)
            .map_err(|e| CelersError::Other(format!("Serialization error: {}", e)))?;

        drop(metadata);

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        let worker_key = self.worker_key(&self.config.worker_id);

        redis::pipe()
            .set(&worker_key, &metadata_json)
            .expire(&worker_key, self.config.worker_ttl_secs as i64)
            .query_async::<()>(&mut conn)
            .await
            .map_err(|e| CelersError::Other(format!("Redis pipeline error: {}", e)))?;

        debug!("Worker {} heartbeat sent", self.config.worker_id);
        Ok(())
    }

    async fn get_workers(&self) -> Result<Vec<WorkerMetadata>> {
        use redis::AsyncCommands;

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        let workers_key = self.workers_key();
        let worker_ids: Vec<String> = conn
            .smembers(&workers_key)
            .await
            .map_err(|e| CelersError::Other(format!("Redis smembers error: {}", e)))?;

        let mut workers = Vec::new();
        for worker_id in worker_ids {
            let worker_key = self.worker_key(&worker_id);
            if let Ok(Some(metadata_json)) = conn.get::<_, Option<String>>(&worker_key).await {
                if let Ok(metadata) = serde_json::from_str::<WorkerMetadata>(&metadata_json) {
                    // Filter out dead workers
                    if metadata.is_alive(self.config.worker_ttl_secs) {
                        workers.push(metadata);
                    } else {
                        // Clean up dead worker
                        let _: std::result::Result<i32, redis::RedisError> =
                            conn.srem(&workers_key, &worker_id).await;
                    }
                }
            }
        }

        Ok(workers)
    }

    async fn try_become_leader(&self, task_type: &str) -> Result<bool> {
        use redis::AsyncCommands;

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        let leader_key = self.leader_key(task_type);
        let ttl_secs = self.config.leader_ttl_secs;

        // Try to set the leader key with NX (only if not exists)
        let result: bool = conn
            .set_options(
                &leader_key,
                &self.config.worker_id,
                redis::SetOptions::default()
                    .with_expiration(redis::SetExpiry::EX(ttl_secs))
                    .conditional_set(redis::ExistenceCheck::NX),
            )
            .await
            .unwrap_or(false);

        if result {
            info!(
                "Worker {} became leader for {}",
                self.config.worker_id, task_type
            );
        } else {
            debug!(
                "Worker {} failed to become leader for {}",
                self.config.worker_id, task_type
            );
        }

        Ok(result)
    }

    async fn is_leader(&self, task_type: &str) -> Result<bool> {
        use redis::AsyncCommands;

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        let leader_key = self.leader_key(task_type);
        let current_leader: Option<String> = conn
            .get(&leader_key)
            .await
            .map_err(|e| CelersError::Other(format!("Redis get error: {}", e)))?;

        Ok(current_leader.as_deref() == Some(&self.config.worker_id))
    }

    async fn release_leadership(&self, task_type: &str) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        let leader_key = self.leader_key(task_type);

        // Only delete if we are the current leader
        let script = r"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
        ";

        let _: i32 = redis::Script::new(script)
            .key(&leader_key)
            .arg(&self.config.worker_id)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CelersError::Other(format!("Redis script error: {}", e)))?;

        info!(
            "Worker {} released leadership for {}",
            self.config.worker_id, task_type
        );
        Ok(())
    }

    async fn acquire_lock(&self, lock_name: &str, timeout_secs: u64) -> Result<bool> {
        use redis::AsyncCommands;

        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        let lock_key = self.lock_key(lock_name);
        let ttl_secs = timeout_secs.min(self.config.lock_ttl_secs);

        // Try to acquire lock with NX
        let result: bool = conn
            .set_options(
                &lock_key,
                &self.config.worker_id,
                redis::SetOptions::default()
                    .with_expiration(redis::SetExpiry::EX(ttl_secs))
                    .conditional_set(redis::ExistenceCheck::NX),
            )
            .await
            .unwrap_or(false);

        if result {
            debug!(
                "Worker {} acquired lock '{}'",
                self.config.worker_id, lock_name
            );
        }

        Ok(result)
    }

    async fn release_lock(&self, lock_name: &str) -> Result<()> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        let lock_key = self.lock_key(lock_name);

        // Only delete if we own the lock
        let script = r"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('DEL', KEYS[1])
            else
                return 0
            end
        ";

        let _: i32 = redis::Script::new(script)
            .key(&lock_key)
            .arg(&self.config.worker_id)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CelersError::Other(format!("Redis script error: {}", e)))?;

        debug!(
            "Worker {} released lock '{}'",
            self.config.worker_id, lock_name
        );
        Ok(())
    }

    async fn get_least_loaded_worker(&self) -> Result<Option<WorkerMetadata>> {
        let workers = self.get_workers().await?;

        if workers.is_empty() {
            return Ok(None);
        }

        // Find worker with lowest load percentage and capacity
        let least_loaded = workers
            .into_iter()
            .filter(|w| w.has_capacity())
            .min_by(|a, b| {
                a.load_percentage()
                    .partial_cmp(&b.load_percentage())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

        Ok(least_loaded)
    }
}

/// In-memory worker coordinator (for testing without Redis)
pub struct InMemoryCoordinator {
    config: CoordinatorConfig,
    workers: Arc<RwLock<HashMap<String, WorkerMetadata>>>,
    leaders: Arc<RwLock<HashMap<String, String>>>,
    locks: Arc<RwLock<HashMap<String, String>>>,
    metadata: Arc<RwLock<WorkerMetadata>>,
}

impl InMemoryCoordinator {
    /// Create a new in-memory coordinator
    pub fn new(config: CoordinatorConfig) -> Self {
        let metadata =
            WorkerMetadata::new(config.worker_id.clone(), config.worker_hostname.clone());

        Self {
            config,
            workers: Arc::new(RwLock::new(HashMap::new())),
            leaders: Arc::new(RwLock::new(HashMap::new())),
            locks: Arc::new(RwLock::new(HashMap::new())),
            metadata: Arc::new(RwLock::new(metadata)),
        }
    }
}

#[async_trait]
impl Coordinator for InMemoryCoordinator {
    async fn register(&self) -> Result<()> {
        let metadata = self.metadata.read().await.clone();
        let mut workers = self.workers.write().await;
        workers.insert(self.config.worker_id.clone(), metadata);
        Ok(())
    }

    async fn unregister(&self) -> Result<()> {
        let mut workers = self.workers.write().await;
        workers.remove(&self.config.worker_id);
        Ok(())
    }

    async fn heartbeat(&self) -> Result<()> {
        let mut metadata = self.metadata.write().await;
        metadata.last_heartbeat = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut workers = self.workers.write().await;
        workers.insert(self.config.worker_id.clone(), metadata.clone());
        Ok(())
    }

    async fn get_workers(&self) -> Result<Vec<WorkerMetadata>> {
        let workers = self.workers.read().await;
        Ok(workers
            .values()
            .filter(|w| w.is_alive(self.config.worker_ttl_secs))
            .cloned()
            .collect())
    }

    async fn try_become_leader(&self, task_type: &str) -> Result<bool> {
        let mut leaders = self.leaders.write().await;
        if !leaders.contains_key(task_type) {
            leaders.insert(task_type.to_string(), self.config.worker_id.clone());
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn is_leader(&self, task_type: &str) -> Result<bool> {
        let leaders = self.leaders.read().await;
        Ok(leaders.get(task_type) == Some(&self.config.worker_id))
    }

    async fn release_leadership(&self, task_type: &str) -> Result<()> {
        let mut leaders = self.leaders.write().await;
        if leaders.get(task_type) == Some(&self.config.worker_id) {
            leaders.remove(task_type);
        }
        Ok(())
    }

    async fn acquire_lock(&self, lock_name: &str, _timeout_secs: u64) -> Result<bool> {
        let mut locks = self.locks.write().await;
        if !locks.contains_key(lock_name) {
            locks.insert(lock_name.to_string(), self.config.worker_id.clone());
            Ok(true)
        } else {
            Ok(false)
        }
    }

    async fn release_lock(&self, lock_name: &str) -> Result<()> {
        let mut locks = self.locks.write().await;
        if locks.get(lock_name) == Some(&self.config.worker_id) {
            locks.remove(lock_name);
        }
        Ok(())
    }

    async fn get_least_loaded_worker(&self) -> Result<Option<WorkerMetadata>> {
        let workers = self.get_workers().await?;

        if workers.is_empty() {
            return Ok(None);
        }

        let least_loaded = workers
            .into_iter()
            .filter(|w| w.has_capacity())
            .min_by(|a, b| {
                a.load_percentage()
                    .partial_cmp(&b.load_percentage())
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

        Ok(least_loaded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_config_validation() {
        let config = CoordinatorConfig::default();
        assert!(config.validate().is_ok());

        let config = CoordinatorConfig {
            worker_id: String::new(),
            ..Default::default()
        };
        assert!(config.validate().is_err());
    }

    #[tokio::test]
    async fn test_worker_metadata() {
        let metadata = WorkerMetadata::new("worker-1", "localhost");
        assert_eq!(metadata.worker_id, "worker-1");
        assert_eq!(metadata.hostname, "localhost");
        assert!(metadata.is_alive(60));
        assert!(metadata.has_capacity());
    }

    #[tokio::test]
    async fn test_in_memory_registration() {
        let config = CoordinatorConfig::default();
        let coordinator = InMemoryCoordinator::new(config);

        coordinator.register().await.unwrap();

        let workers = coordinator.get_workers().await.unwrap();
        assert_eq!(workers.len(), 1);
    }

    #[tokio::test]
    async fn test_in_memory_leader_election() {
        let config = CoordinatorConfig::default();
        let coordinator = InMemoryCoordinator::new(config);

        // Should become leader
        assert!(coordinator.try_become_leader("test_task").await.unwrap());

        // Should be the leader
        assert!(coordinator.is_leader("test_task").await.unwrap());

        // Should not become leader again
        assert!(!coordinator.try_become_leader("test_task").await.unwrap());

        // Release leadership
        coordinator.release_leadership("test_task").await.unwrap();

        // Should not be leader anymore
        assert!(!coordinator.is_leader("test_task").await.unwrap());
    }

    #[tokio::test]
    async fn test_in_memory_locks() {
        let config = CoordinatorConfig::default();
        let coordinator = InMemoryCoordinator::new(config);

        // Should acquire lock
        assert!(coordinator.acquire_lock("test_lock", 30).await.unwrap());

        // Should not acquire same lock again
        assert!(!coordinator.acquire_lock("test_lock", 30).await.unwrap());

        // Release lock
        coordinator.release_lock("test_lock").await.unwrap();

        // Should be able to acquire again
        assert!(coordinator.acquire_lock("test_lock", 30).await.unwrap());
    }
}
