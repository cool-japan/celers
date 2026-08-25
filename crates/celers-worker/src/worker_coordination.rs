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
//! Singleton work is held with a [`LeadershipGuard`], not with a cached
//! `bool`: the guard renews leadership in the background and reports the loss
//! through [`LeadershipGuard::is_current`], so a worker whose leader key
//! expired stops doing the singleton work instead of continuing to believe it
//! is the leader. [`Coordinator::try_become_leader`] on its own is a one-shot
//! election with no renewal — see its own documentation for what that costs.
//!
//! ```no_run
//! # #[cfg(feature = "redis")]
//! # async fn example() -> celers_core::Result<()> {
//! use celers_worker::worker_coordination::{
//!     CoordinatorConfig, LeadershipGuard, WorkerCoordinator,
//! };
//! use std::sync::Arc;
//!
//! let config = CoordinatorConfig {
//!     redis_url: "redis://127.0.0.1:6379".to_string(),
//!     worker_id: "worker-1".to_string(),
//!     ..Default::default()
//! };
//! let leader_ttl_secs = config.leader_ttl_secs;
//!
//! let coordinator: Arc<dyn celers_worker::worker_coordination::Coordinator> =
//!     Arc::new(WorkerCoordinator::new(config).await?);
//!
//! // Try to become leader. `None` means another worker holds it.
//! if let Some(guard) =
//!     LeadershipGuard::try_acquire(coordinator, "my_singleton_task", leader_ttl_secs).await?
//! {
//!     while guard.is_current() {
//!         // ... do one unit of the singleton work, then re-check ...
//!         break;
//!     }
//! }
//! // Dropping `guard` releases leadership; losing it flips `is_current()`.
//! # Ok(())
//! # }
//! ```
//!
//! Regression guard for the fix in idx 190 (the `SystemTime` expectation
//! this module used to carry on its registration/heartbeat/liveness
//! paths): denies `clippy::unwrap_used`/`clippy::expect_used` outside the
//! test module so a future change cannot silently reintroduce a panic
//! here.
#![deny(clippy::unwrap_used, clippy::expect_used)]

use async_trait::async_trait;
#[cfg(feature = "redis")]
use celers_core::CelersError;
use celers_core::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
#[allow(unused_imports)]
use tracing::{debug, error, info};

/// Current Unix time in seconds.
///
/// `SystemTime::now()` is not guaranteed to be at or after `UNIX_EPOCH` (a
/// misconfigured clock can be set earlier), so this never panics/expects
/// on that: an unrepresentable duration is reported as `0` rather than
/// crashing a production coordination path (worker registration,
/// heartbeats, liveness checks).
fn unix_now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

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
        let now = unix_now_secs();

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
        unix_now_secs().saturating_sub(self.last_heartbeat) <= ttl_secs
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

    /// Renew this worker's leadership for `task_type`, extending its TTL
    /// (Redis backend) or simply reconfirming it (in-memory backend,
    /// which has no TTL to lose in the first place).
    ///
    /// A worker that calls [`try_become_leader`](Self::try_become_leader)
    /// once and then caches the returned `true` silently stops being
    /// leader the moment the TTL expires -- nothing else in this trait
    /// keeps leadership alive. Callers doing singleton work must call
    /// this periodically (well within `leader_ttl_secs`) and stop that
    /// work as soon as it returns `Ok(false)`; see [`LeadershipGuard`]
    /// for a helper that does this automatically.
    ///
    /// Returns `Ok(true)` if leadership was renewed/confirmed, `Ok(false)`
    /// if this worker is not (or is no longer) the leader.
    async fn renew_leadership(&self, task_type: &str) -> Result<bool>;

    /// Release leadership for a task type
    async fn release_leadership(&self, task_type: &str) -> Result<()>;

    /// Acquire a distributed lock
    async fn acquire_lock(&self, lock_name: &str, timeout_secs: u64) -> Result<bool>;

    /// Renew a previously-[`acquire_lock`](Self::acquire_lock)ed lock,
    /// extending its TTL. A task holding a lock longer than its TTL
    /// loses mutual exclusion the moment the TTL expires unless it calls
    /// this periodically. Returns `Ok(true)` if renewed, `Ok(false)` if
    /// this worker does not (or no longer) hold the lock.
    async fn renew_lock(&self, lock_name: &str, timeout_secs: u64) -> Result<bool>;

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

        let client = crate::redis_tls::open_client(config.redis_url.as_str())
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
        metadata.last_heartbeat = unix_now_secs();

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
            match conn.get::<_, Option<String>>(&worker_key).await {
                Ok(Some(metadata_json)) => {
                    match serde_json::from_str::<WorkerMetadata>(&metadata_json) {
                        Ok(metadata) if metadata.is_alive(self.config.worker_ttl_secs) => {
                            workers.push(metadata);
                        }
                        _ => {
                            // Stale heartbeat or malformed entry: clean up.
                            let _: std::result::Result<i32, redis::RedisError> =
                                conn.srem(&workers_key, &worker_id).await;
                        }
                    }
                }
                Ok(None) | Err(_) => {
                    // `Ok(None)`: the worker's key already expired (its
                    // TTL ran out) but its id is still in the `:workers`
                    // set -- this was previously never cleaned up here,
                    // so the set accumulated one entry per dead worker
                    // forever, and every future `get_workers()` call paid
                    // an extra round-trip per ghost. `Err(_)`: treat a
                    // failed per-key read the same way rather than
                    // leaving a possibly-stale id in the set indefinitely.
                    let _: std::result::Result<i32, redis::RedisError> =
                        conn.srem(&workers_key, &worker_id).await;
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

        // Try to set the leader key with NX (only if not exists). Any
        // Redis-level failure (connection drop, timeout, ...) propagates
        // via `?` instead of being swallowed into `false`: a Redis
        // outage must be distinguishable from genuinely losing a
        // contested election, since callers may otherwise conclude
        // "not leader" cluster-wide and stop performing singleton work
        // with no error ever surfaced.
        let result: bool = conn
            .set_options(
                &leader_key,
                &self.config.worker_id,
                redis::SetOptions::default()
                    .with_expiration(redis::SetExpiry::EX(ttl_secs))
                    .conditional_set(redis::ExistenceCheck::NX),
            )
            .await
            .map_err(|e| CelersError::Other(format!("Redis set_options error: {}", e)))?;

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

    async fn renew_leadership(&self, task_type: &str) -> Result<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        let leader_key = self.leader_key(task_type);
        let ttl_secs = self.config.leader_ttl_secs;

        // Atomically extend the TTL only if we still hold the key. A
        // plain GET-then-EXPIRE would race: another worker could win the
        // election for the (already-expired) key in between our GET and
        // our EXPIRE, and we would then extend a lease we no longer
        // legitimately own.
        let script = r"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('EXPIRE', KEYS[1], ARGV[2])
            else
                return 0
            end
        ";

        let renewed: i32 = redis::Script::new(script)
            .key(&leader_key)
            .arg(&self.config.worker_id)
            .arg(ttl_secs)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CelersError::Other(format!("Redis script error: {}", e)))?;

        let renewed = renewed == 1;
        if renewed {
            debug!(
                "Worker {} renewed leadership for {}",
                self.config.worker_id, task_type
            );
        } else {
            debug!(
                "Worker {} is no longer leader for {}; renewal skipped",
                self.config.worker_id, task_type
            );
        }
        Ok(renewed)
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

        // Try to acquire lock with NX. As with `try_become_leader`, a
        // Redis-level failure propagates via `?` instead of being
        // swallowed into `false`, so an outage cannot masquerade as
        // ordinary lock contention.
        let result: bool = conn
            .set_options(
                &lock_key,
                &self.config.worker_id,
                redis::SetOptions::default()
                    .with_expiration(redis::SetExpiry::EX(ttl_secs))
                    .conditional_set(redis::ExistenceCheck::NX),
            )
            .await
            .map_err(|e| CelersError::Other(format!("Redis set_options error: {}", e)))?;

        if result {
            debug!(
                "Worker {} acquired lock '{}'",
                self.config.worker_id, lock_name
            );
        }

        Ok(result)
    }

    async fn renew_lock(&self, lock_name: &str, timeout_secs: u64) -> Result<bool> {
        let mut conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Other(format!("Redis connection error: {}", e)))?;

        let lock_key = self.lock_key(lock_name);
        let ttl_secs = timeout_secs.min(self.config.lock_ttl_secs);

        // Same atomic check-and-extend shape as `renew_leadership`: only
        // extend the TTL if we still hold the lock.
        let script = r"
            if redis.call('GET', KEYS[1]) == ARGV[1] then
                return redis.call('EXPIRE', KEYS[1], ARGV[2])
            else
                return 0
            end
        ";

        let renewed: i32 = redis::Script::new(script)
            .key(&lock_key)
            .arg(&self.config.worker_id)
            .arg(ttl_secs)
            .invoke_async(&mut conn)
            .await
            .map_err(|e| CelersError::Other(format!("Redis script error: {}", e)))?;

        Ok(renewed == 1)
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
        metadata.last_heartbeat = unix_now_secs();

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

    async fn renew_leadership(&self, task_type: &str) -> Result<bool> {
        // No TTL to extend in memory: "renewal" is simply reconfirming
        // that we are still recorded as the leader.
        self.is_leader(task_type).await
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

    async fn renew_lock(&self, lock_name: &str, _timeout_secs: u64) -> Result<bool> {
        let locks = self.locks.read().await;
        Ok(locks.get(lock_name) == Some(&self.config.worker_id))
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

/// Holds leadership for a task type for as long as it stays alive.
///
/// [`try_acquire`](Self::try_acquire) becomes leader and spawns a
/// background task that renews leadership at roughly `leader_ttl_secs / 3`
/// intervals (never less than one second) via
/// [`Coordinator::renew_leadership`]; leadership is released when the
/// guard is dropped, or as soon as a renewal attempt reports (or fails to
/// confirm) that leadership was lost.
///
/// This exists because caching the `bool` [`Coordinator::try_become_leader`]
/// returns and never renewing means a worker silently stops being leader
/// the instant the TTL expires -- exactly the pattern this module's own
/// doc example previously showed. Prefer this guard, or at minimum call
/// [`Coordinator::renew_leadership`] periodically yourself and check its
/// result before continuing singleton work.
pub struct LeadershipGuard {
    coordinator: Arc<dyn Coordinator>,
    task_type: String,
    /// Updated by the background renewal task; `is_current()` reads this
    /// rather than making a network round-trip.
    is_leader: Arc<AtomicBool>,
    renewal_handle: Option<tokio::task::JoinHandle<()>>,
}

impl LeadershipGuard {
    /// Try to become leader for `task_type`. Returns `Ok(None)` if
    /// another worker already holds leadership; otherwise returns a
    /// guard that keeps renewing leadership in the background until
    /// dropped.
    pub async fn try_acquire(
        coordinator: Arc<dyn Coordinator>,
        task_type: impl Into<String>,
        leader_ttl_secs: u64,
    ) -> Result<Option<Self>> {
        let task_type = task_type.into();
        if !coordinator.try_become_leader(&task_type).await? {
            return Ok(None);
        }

        let is_leader = Arc::new(AtomicBool::new(true));
        // Renew at 1/3 of the TTL so at least two renewal attempts get a
        // chance to succeed before the TTL could actually expire, even if
        // one tick is delayed.
        let renewal_interval = Duration::from_secs((leader_ttl_secs / 3).max(1));

        let renewal_coordinator = Arc::clone(&coordinator);
        let renewal_task_type = task_type.clone();
        let renewal_flag = Arc::clone(&is_leader);
        let renewal_handle = tokio::spawn(async move {
            loop {
                tokio::time::sleep(renewal_interval).await;
                if !renewal_flag.load(Ordering::Acquire) {
                    // Already lost (or this guard is being torn down);
                    // nothing left to do.
                    return;
                }
                match renewal_coordinator
                    .renew_leadership(&renewal_task_type)
                    .await
                {
                    Ok(true) => continue,
                    Ok(false) => {
                        debug!(
                            "leadership lost for {renewal_task_type}: renewal reported not-leader"
                        );
                        renewal_flag.store(false, Ordering::Release);
                        return;
                    }
                    Err(e) => {
                        // A renewal failure is treated the same as an
                        // explicit loss: without a successful renewal we
                        // cannot distinguish "still leader" from "about
                        // to expire", so the safe assumption is that we
                        // are no longer exclusively holding it.
                        error!(
                            "leadership renewal failed for {renewal_task_type}: {e}; \
                             assuming leadership lost"
                        );
                        renewal_flag.store(false, Ordering::Release);
                        return;
                    }
                }
            }
        });

        Ok(Some(Self {
            coordinator,
            task_type,
            is_leader,
            renewal_handle: Some(renewal_handle),
        }))
    }

    /// Whether this guard still believes it holds leadership, based on
    /// the outcome of the most recent background renewal (no network
    /// round-trip). Becomes `false` as soon as a renewal attempt fails or
    /// reports leadership lost -- callers performing singleton work
    /// should check this (or poll it) and stop promptly once it flips.
    pub fn is_current(&self) -> bool {
        self.is_leader.load(Ordering::Acquire)
    }

    /// The task type this guard holds (or held) leadership for.
    pub fn task_type(&self) -> &str {
        &self.task_type
    }
}

impl Drop for LeadershipGuard {
    fn drop(&mut self) {
        // Stop the renewal loop first so it cannot race the release
        // below (e.g. renew right after we've asked to release).
        if let Some(handle) = self.renewal_handle.take() {
            handle.abort();
        }
        self.is_leader.store(false, Ordering::Release);

        let coordinator = Arc::clone(&self.coordinator);
        let task_type = self.task_type.clone();
        // Best-effort release: spawn rather than block `Drop` on I/O. If
        // no Tokio runtime is available here (e.g. the guard is dropped
        // outside any async context), the leadership key simply expires
        // on its own via its TTL -- it is never left claimed forever.
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                let _ = coordinator.release_leadership(&task_type).await;
            });
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used)]
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

    /// Regression test (idx 190): `WorkerMetadata::new`/`is_alive` used to
    /// `.expect("SystemTime should be after UNIX_EPOCH")`, which would
    /// panic a production registration/liveness-check path if the clock
    /// were ever set before the epoch. `unix_now_secs` must return a
    /// plausible value under normal conditions and never panic.
    #[test]
    fn test_unix_now_secs_is_plausible_and_does_not_panic() {
        let now = unix_now_secs();
        // Sometime after 2020-09-13 -- a loose sanity bound, not an exact
        // check, just confirming this isn't the `0` fallback under normal
        // conditions.
        assert!(
            now > 1_600_000_000,
            "unix_now_secs() looks implausible: {now}"
        );
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

    #[tokio::test]
    async fn test_in_memory_renew_leadership_and_lock() {
        let config = CoordinatorConfig::default();
        let coordinator = InMemoryCoordinator::new(config);

        // Not leader/holder yet: renewal must not falsely report success.
        assert!(!coordinator.renew_leadership("task").await.unwrap());
        assert!(!coordinator.renew_lock("lock", 30).await.unwrap());

        coordinator.try_become_leader("task").await.unwrap();
        coordinator.acquire_lock("lock", 30).await.unwrap();

        assert!(coordinator.renew_leadership("task").await.unwrap());
        assert!(coordinator.renew_lock("lock", 30).await.unwrap());

        coordinator.release_leadership("task").await.unwrap();
        coordinator.release_lock("lock").await.unwrap();

        assert!(!coordinator.renew_leadership("task").await.unwrap());
        assert!(!coordinator.renew_lock("lock", 30).await.unwrap());
    }

    // --- LeadershipGuard regression tests (idx 185) ------------------------

    #[tokio::test]
    async fn test_leadership_guard_acquires_and_reports_current() {
        let coordinator: Arc<dyn Coordinator> =
            Arc::new(InMemoryCoordinator::new(CoordinatorConfig::default()));

        let guard = LeadershipGuard::try_acquire(Arc::clone(&coordinator), "task", 30)
            .await
            .unwrap()
            .expect("should become leader when nobody else holds it");

        assert!(guard.is_current());
        assert_eq!(guard.task_type(), "task");
        assert!(coordinator.is_leader("task").await.unwrap());
    }

    #[tokio::test]
    async fn test_leadership_guard_returns_none_when_already_held() {
        let coordinator: Arc<dyn Coordinator> =
            Arc::new(InMemoryCoordinator::new(CoordinatorConfig::default()));

        let _first = LeadershipGuard::try_acquire(Arc::clone(&coordinator), "task", 30)
            .await
            .unwrap()
            .expect("first acquisition should succeed");

        let second = LeadershipGuard::try_acquire(Arc::clone(&coordinator), "task", 30)
            .await
            .unwrap();
        assert!(
            second.is_none(),
            "a second acquisition must fail while leadership is already held"
        );
    }

    /// Regression test: previously nothing renewed leadership at all, so a
    /// caller caching `try_become_leader`'s `true` had no way to learn
    /// that leadership had actually been lost. The guard's background
    /// renewal loop must detect that and flip `is_current()` to `false`
    /// without the caller polling Redis/the coordinator itself.
    ///
    /// (Uses a real short sleep rather than `tokio::time::pause`/
    /// `advance`: this crate's `tokio` dependency does not enable the
    /// `test-util` feature, so the mockable clock is unavailable here.)
    #[tokio::test]
    async fn test_leadership_guard_detects_externally_lost_leadership() {
        let coordinator: Arc<dyn Coordinator> =
            Arc::new(InMemoryCoordinator::new(CoordinatorConfig::default()));

        // leader_ttl_secs=3 -> renewal every max(3/3, 1) = 1s.
        let guard = LeadershipGuard::try_acquire(Arc::clone(&coordinator), "task", 3)
            .await
            .unwrap()
            .expect("should become leader");
        assert!(guard.is_current());

        // Something external takes leadership away without the guard's
        // involvement (e.g., in the Redis backend, the TTL expiring
        // faster than renewal could keep up).
        coordinator.release_leadership("task").await.unwrap();

        // Wait past one renewal interval so the background task's next
        // tick fires and observes the loss.
        tokio::time::sleep(Duration::from_millis(1300)).await;

        assert!(
            !guard.is_current(),
            "guard must reflect lost leadership once a renewal attempt reports it"
        );
    }

    /// Regression test: dropping a `LeadershipGuard` must release
    /// leadership so another worker can take over promptly, instead of
    /// leaving it claimed until a TTL (which nothing was extending
    /// reliably anyway) eventually expires.
    #[tokio::test]
    async fn test_leadership_guard_releases_on_drop() {
        let coordinator: Arc<dyn Coordinator> =
            Arc::new(InMemoryCoordinator::new(CoordinatorConfig::default()));

        {
            let _guard = LeadershipGuard::try_acquire(Arc::clone(&coordinator), "task", 30)
                .await
                .unwrap()
                .expect("should become leader");
            assert!(coordinator.is_leader("task").await.unwrap());
        }

        // Drop spawns a best-effort release task; give the runtime a
        // chance to run it.
        tokio::task::yield_now().await;
        tokio::task::yield_now().await;

        assert!(
            !coordinator.is_leader("task").await.unwrap(),
            "leadership must be released once the guard is dropped"
        );
    }

    // --- Redis-backed regression tests (idx 185) ---------------------------
    //
    // These run against a real local Redis (redis://127.0.0.1:6379,
    // matching the convention already used elsewhere in this workspace,
    // e.g. celers-broker-redis). Each test uses a random key_prefix so
    // concurrent test runs (and other crates' test suites) never collide.

    #[cfg(feature = "redis")]
    const TEST_REDIS_URL: &str = "redis://127.0.0.1:6379";

    #[cfg(feature = "redis")]
    fn redis_test_config(worker_id: &str) -> CoordinatorConfig {
        CoordinatorConfig {
            redis_url: TEST_REDIS_URL.to_string(),
            key_prefix: format!("celers-worker-coord-test-{}", uuid::Uuid::new_v4()),
            worker_id: worker_id.to_string(),
            worker_hostname: "test-host".to_string(),
            heartbeat_interval_secs: 30,
            worker_ttl_secs: 60,
            lock_ttl_secs: 30,
            leader_ttl_secs: 60,
        }
    }

    /// Regression test: previously nothing extended a leader key's TTL,
    /// so leadership always expired on schedule regardless of whether the
    /// leader was still alive and working. `renew_leadership` must
    /// actually push the expiry out in real Redis.
    #[cfg(feature = "redis")]
    #[tokio::test]
    async fn test_redis_renew_leadership_actually_extends_ttl() {
        let mut config = redis_test_config("worker-A");
        config.leader_ttl_secs = 2;
        let coordinator = WorkerCoordinator::new(config).await.unwrap();

        assert!(coordinator.try_become_leader("task").await.unwrap());

        // Keep renewing well inside the 2s TTL, for longer than the
        // *original* TTL would have allowed if nothing were renewing it.
        for _ in 0..3 {
            tokio::time::sleep(Duration::from_millis(900)).await;
            assert!(
                coordinator.renew_leadership("task").await.unwrap(),
                "renewal should succeed while we are still the leader"
            );
        }

        assert!(
            coordinator.is_leader("task").await.unwrap(),
            "leadership must still hold ~2.7s after a 2s TTL because renewal kept extending it"
        );

        coordinator.release_leadership("task").await.unwrap();
    }

    #[cfg(feature = "redis")]
    #[tokio::test]
    async fn test_redis_renew_leadership_fails_for_non_leader() {
        let prefix = format!("celers-worker-coord-test-{}", uuid::Uuid::new_v4());
        let mut config_a = redis_test_config("worker-A");
        config_a.key_prefix = prefix.clone();
        let mut config_b = redis_test_config("worker-B");
        config_b.key_prefix = prefix;

        let coordinator_a = WorkerCoordinator::new(config_a).await.unwrap();
        let coordinator_b = WorkerCoordinator::new(config_b).await.unwrap();

        assert!(coordinator_a.try_become_leader("task").await.unwrap());

        // B never won the election, so renewing on B's behalf must not
        // pretend it holds (and extend the TTL of) A's leadership.
        assert!(!coordinator_b.renew_leadership("task").await.unwrap());

        coordinator_a.release_leadership("task").await.unwrap();
    }

    #[cfg(feature = "redis")]
    #[tokio::test]
    async fn test_redis_renew_lock_extends_ttl_and_rejects_non_holder() {
        let prefix = format!("celers-worker-coord-test-{}", uuid::Uuid::new_v4());
        let mut config_a = redis_test_config("worker-A");
        config_a.key_prefix = prefix.clone();
        let mut config_b = redis_test_config("worker-B");
        config_b.key_prefix = prefix;

        let coordinator_a = WorkerCoordinator::new(config_a).await.unwrap();
        let coordinator_b = WorkerCoordinator::new(config_b).await.unwrap();

        assert!(coordinator_a.acquire_lock("job", 2).await.unwrap());
        assert!(coordinator_a.renew_lock("job", 2).await.unwrap());
        assert!(
            !coordinator_b.renew_lock("job", 2).await.unwrap(),
            "a worker that does not hold the lock must not be able to renew it"
        );

        coordinator_a.release_lock("job").await.unwrap();
    }

    /// Regression test: `try_become_leader`/`acquire_lock` used to
    /// `.unwrap_or(false)` a Redis-level failure, making an outage
    /// indistinguishable from ordinary contention. A connection that
    /// genuinely cannot be established must surface as `Err`.
    ///
    /// Uses a loopback port nothing is listening on (never the real
    /// shared Redis instance other concurrent tests rely on), so
    /// `get_multiplexed_async_connection` fails immediately and
    /// deterministically (connection refused) without disturbing
    /// anything else.
    #[cfg(feature = "redis")]
    #[tokio::test]
    async fn test_redis_connection_failure_surfaces_as_error_not_lost_election() {
        let config = CoordinatorConfig {
            redis_url: "redis://127.0.0.1:1/".to_string(),
            ..redis_test_config("worker-unreachable")
        };
        // Bypass `WorkerCoordinator::new`'s own connectivity PING (which
        // would itself fail against this address) by constructing the
        // struct directly -- `redis::Client::open` only parses the URL,
        // it does not connect.
        let client = redis::Client::open(config.redis_url.as_str()).unwrap();
        let metadata =
            WorkerMetadata::new(config.worker_id.clone(), config.worker_hostname.clone());
        let coordinator = WorkerCoordinator {
            config,
            client,
            metadata: Arc::new(RwLock::new(metadata)),
        };

        let leader_result = coordinator.try_become_leader("task").await;
        assert!(
            leader_result.is_err(),
            "a connection failure must surface as Err, not Ok(false)"
        );

        let lock_result = coordinator.acquire_lock("lock", 10).await;
        assert!(
            lock_result.is_err(),
            "a connection failure must surface as Err, not Ok(false)"
        );
    }

    /// Regression test: `get_workers()` only removed a stale id from the
    /// `:workers` set when the metadata key still existed but reported
    /// `is_alive() == false`. Once the key's own TTL expired (`GET`
    /// returns `Ok(None)`), the id was never removed, so the set leaked
    /// one entry per dead worker forever.
    #[cfg(feature = "redis")]
    #[tokio::test]
    async fn test_redis_get_workers_cleans_up_expired_worker_from_set() {
        let mut config = redis_test_config("worker-ghost");
        config.worker_ttl_secs = 1; // expires almost immediately
        let coordinator = WorkerCoordinator::new(config.clone()).await.unwrap();

        coordinator.register().await.unwrap();
        assert_eq!(coordinator.get_workers().await.unwrap().len(), 1);

        // Wait out the worker's own key TTL (real Redis expiry -- not
        // something a mocked/paused clock can simulate) while its id
        // necessarily remains in the `:workers` SADD set regardless.
        tokio::time::sleep(Duration::from_millis(1500)).await;

        let workers = coordinator.get_workers().await.unwrap();
        assert!(workers.is_empty());

        // The actual regression check: confirm the ghost id was removed
        // from the set itself, not merely absent from get_workers()'s
        // filtered return value.
        use redis::AsyncCommands;
        let mut conn = coordinator
            .client
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let workers_key = coordinator.workers_key();
        let remaining: Vec<String> = conn.smembers(&workers_key).await.unwrap();
        assert!(
            remaining.is_empty(),
            "expired worker id must be removed from the :workers set, got {remaining:?}"
        );
    }

    /// End-to-end: `LeadershipGuard` against the real Redis-backed
    /// coordinator, with a TTL short enough that the *original* grant
    /// would have expired well before the test finishes if nothing were
    /// renewing it.
    #[cfg(feature = "redis")]
    #[tokio::test]
    async fn test_leadership_guard_keeps_redis_leadership_alive_past_original_ttl() {
        let mut config = redis_test_config("worker-A");
        config.leader_ttl_secs = 2;
        let coordinator: Arc<dyn Coordinator> =
            Arc::new(WorkerCoordinator::new(config).await.unwrap());

        let guard = LeadershipGuard::try_acquire(Arc::clone(&coordinator), "task", 2)
            .await
            .unwrap()
            .expect("should become leader");

        tokio::time::sleep(Duration::from_millis(2500)).await;

        assert!(
            guard.is_current(),
            "background renewal should have kept leadership alive past the original 2s TTL"
        );
        assert!(coordinator.is_leader("task").await.unwrap());
    }
}
