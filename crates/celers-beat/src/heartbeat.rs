//! Beat scheduler heartbeat and failover
//!
//! Provides leader election and health monitoring for multi-instance
//! beat scheduler deployments using distributed locks.
//!
//! # Architecture
//! - Only one beat instance (the Leader) executes scheduled tasks
//! - Other instances run in Standby mode, monitoring leader health
//! - If the leader fails, a standby instance automatically promotes
//! - Heartbeats are sent periodically to indicate liveness
//!
//! # Example
//!
//! ```ignore
//! use celers_beat::heartbeat::{BeatHeartbeat, HeartbeatConfig};
//! use celers_beat::lock::InMemoryLockBackend;
//! use std::sync::Arc;
//!
//! let backend = Arc::new(InMemoryLockBackend::new());
//! let config = HeartbeatConfig::new();
//! let heartbeat = BeatHeartbeat::new("instance-1".to_string(), backend, config);
//!
//! // Try to become the leader
//! let is_leader = heartbeat.try_become_leader().await.unwrap();
//! ```

use celers_core::lock::DistributedLockBackend;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

// ---------------------------------------------------------------------------
// BeatRole
// ---------------------------------------------------------------------------

/// Role of a beat scheduler instance in a multi-instance deployment.
///
/// In a distributed beat scheduler setup, exactly one instance should be
/// the `Leader` that executes scheduled tasks. All other instances remain
/// in `Standby` mode, ready to take over if the leader fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Serialize, Deserialize)]
pub enum BeatRole {
    /// This instance is the active leader executing scheduled tasks
    Leader,
    /// This instance is on standby, monitoring the leader's health
    Standby,
    /// Role has not yet been determined (initial state or after shutdown)
    #[default]
    Unknown,
}

impl std::fmt::Display for BeatRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BeatRole::Leader => write!(f, "leader"),
            BeatRole::Standby => write!(f, "standby"),
            BeatRole::Unknown => write!(f, "unknown"),
        }
    }
}

// ---------------------------------------------------------------------------
// HeartbeatInfo
// ---------------------------------------------------------------------------

/// Snapshot of a beat scheduler instance's heartbeat state.
///
/// Contains metadata about the instance, its role, and the current
/// scheduler state. This information can be used for monitoring dashboards
/// or health check endpoints.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeartbeatInfo {
    /// Unique identifier for this scheduler instance
    pub instance_id: String,
    /// Current role of this instance
    pub role: BeatRole,
    /// Timestamp of the last heartbeat sent
    pub last_heartbeat_at: DateTime<Utc>,
    /// Number of scheduled tasks registered
    pub schedule_count: usize,
    /// Name of the next task due for execution, if any
    pub next_due_task: Option<String>,
    /// Timestamp when this instance started
    pub started_at: DateTime<Utc>,
    /// Version of the celers-beat crate
    pub version: String,
    /// Hostname of the machine running this instance
    pub hostname: String,
}

impl HeartbeatInfo {
    /// Create a new heartbeat info for the given instance.
    ///
    /// The role is initially set to `Unknown`, and timestamps are set
    /// to the current time.
    pub fn new(instance_id: String) -> Self {
        Self {
            instance_id,
            role: BeatRole::Unknown,
            last_heartbeat_at: Utc::now(),
            schedule_count: 0,
            next_due_task: None,
            started_at: Utc::now(),
            version: env!("CARGO_PKG_VERSION").to_string(),
            hostname: hostname(),
        }
    }

    /// Set the role for this heartbeat info (builder pattern).
    pub fn with_role(mut self, role: BeatRole) -> Self {
        self.role = role;
        self
    }

    /// Set the schedule count (builder pattern).
    pub fn with_schedule_count(mut self, count: usize) -> Self {
        self.schedule_count = count;
        self
    }

    /// Set the next due task name (builder pattern).
    pub fn with_next_due_task(mut self, task: Option<String>) -> Self {
        self.next_due_task = task;
        self
    }
}

/// Get the hostname of the current machine.
///
/// Tries `HOSTNAME` (Unix) then `COMPUTERNAME` (Windows) environment
/// variables, falling back to `"unknown"` if neither is set.
fn hostname() -> String {
    std::env::var("HOSTNAME")
        .or_else(|_| std::env::var("COMPUTERNAME"))
        .unwrap_or_else(|_| "unknown".to_string())
}

// ---------------------------------------------------------------------------
// HeartbeatConfig
// ---------------------------------------------------------------------------

/// Configuration for the beat heartbeat and failover system.
///
/// All durations have sensible defaults. The [`validate`](HeartbeatConfig::validate)
/// method checks that the values are internally consistent.
///
/// # Defaults
///
/// | Parameter                | Default  |
/// |--------------------------|----------|
/// | `heartbeat_interval`     | 5 s      |
/// | `heartbeat_ttl`          | 15 s     |
/// | `leader_lease_ttl`       | 30 s     |
/// | `leader_renewal_interval`| 10 s     |
/// | `failover_timeout`       | 45 s     |
/// | `leader_lock_key`        | `"beat_leader"` |
/// | `heartbeat_key_prefix`   | `"beat_heartbeat:"` |
#[derive(Debug, Clone)]
pub struct HeartbeatConfig {
    /// How often heartbeats are sent
    pub heartbeat_interval: Duration,
    /// Time-to-live for heartbeat entries (should be > heartbeat_interval)
    pub heartbeat_ttl: Duration,
    /// Time-to-live for the leader lease (the distributed lock TTL)
    pub leader_lease_ttl: Duration,
    /// How often the leader renews its lease (should be < leader_lease_ttl)
    pub leader_renewal_interval: Duration,
    /// Maximum time to wait before declaring the leader dead
    pub failover_timeout: Duration,
    /// Key used for the leader lock in the distributed lock backend
    pub leader_lock_key: String,
    /// Prefix for per-instance heartbeat keys
    pub heartbeat_key_prefix: String,
}

impl Default for HeartbeatConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_secs(5),
            heartbeat_ttl: Duration::from_secs(15),
            leader_lease_ttl: Duration::from_secs(30),
            leader_renewal_interval: Duration::from_secs(10),
            failover_timeout: Duration::from_secs(45),
            leader_lock_key: "beat_leader".to_string(),
            heartbeat_key_prefix: "beat_heartbeat:".to_string(),
        }
    }
}

impl HeartbeatConfig {
    /// Create a new config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the heartbeat interval (builder pattern).
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Set the heartbeat TTL (builder pattern).
    pub fn with_heartbeat_ttl(mut self, ttl: Duration) -> Self {
        self.heartbeat_ttl = ttl;
        self
    }

    /// Set the leader lease TTL (builder pattern).
    pub fn with_leader_lease_ttl(mut self, ttl: Duration) -> Self {
        self.leader_lease_ttl = ttl;
        self
    }

    /// Set the leader renewal interval (builder pattern).
    pub fn with_leader_renewal_interval(mut self, interval: Duration) -> Self {
        self.leader_renewal_interval = interval;
        self
    }

    /// Set the failover timeout (builder pattern).
    pub fn with_failover_timeout(mut self, timeout: Duration) -> Self {
        self.failover_timeout = timeout;
        self
    }

    /// Set the leader lock key (builder pattern).
    pub fn with_leader_lock_key(mut self, key: impl Into<String>) -> Self {
        self.leader_lock_key = key.into();
        self
    }

    /// Set the heartbeat key prefix (builder pattern).
    pub fn with_heartbeat_key_prefix(mut self, prefix: impl Into<String>) -> Self {
        self.heartbeat_key_prefix = prefix.into();
        self
    }

    /// Validate that the configuration is internally consistent.
    ///
    /// # Errors
    ///
    /// Returns an error string describing the inconsistency if validation
    /// fails. The following invariants are checked:
    ///
    /// - `heartbeat_ttl` must be strictly greater than `heartbeat_interval`
    /// - `leader_lease_ttl` must be strictly greater than `leader_renewal_interval`
    /// - `failover_timeout` must be strictly greater than `leader_lease_ttl`
    pub fn validate(&self) -> Result<(), String> {
        if self.heartbeat_ttl <= self.heartbeat_interval {
            return Err("heartbeat_ttl must be greater than heartbeat_interval".to_string());
        }
        if self.leader_lease_ttl <= self.leader_renewal_interval {
            return Err(
                "leader_lease_ttl must be greater than leader_renewal_interval".to_string(),
            );
        }
        if self.failover_timeout <= self.leader_lease_ttl {
            return Err("failover_timeout must be greater than leader_lease_ttl".to_string());
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// HeartbeatStats
// ---------------------------------------------------------------------------

/// Atomic counters tracking heartbeat and failover activity.
///
/// All operations are lock-free and use `Relaxed` ordering, which is
/// appropriate for statistics that do not need strict cross-thread
/// synchronization of reads.
#[derive(Debug)]
pub struct HeartbeatStats {
    /// Total heartbeats sent by this instance
    heartbeats_sent: AtomicU64,
    /// Number of leader elections this instance won
    leader_elections_won: AtomicU64,
    /// Number of leader elections this instance lost
    leader_elections_lost: AtomicU64,
    /// Number of successful lease renewals
    lease_renewals: AtomicU64,
    /// Number of failed lease renewals (lost leadership)
    lease_renewal_failures: AtomicU64,
    /// Number of failovers detected (leader disappeared)
    failovers_detected: AtomicU64,
}

impl HeartbeatStats {
    /// Create a new stats tracker with all counters at zero.
    pub fn new() -> Self {
        Self {
            heartbeats_sent: AtomicU64::new(0),
            leader_elections_won: AtomicU64::new(0),
            leader_elections_lost: AtomicU64::new(0),
            lease_renewals: AtomicU64::new(0),
            lease_renewal_failures: AtomicU64::new(0),
            failovers_detected: AtomicU64::new(0),
        }
    }

    /// Get total heartbeats sent.
    pub fn heartbeats_sent(&self) -> u64 {
        self.heartbeats_sent.load(Ordering::Relaxed)
    }

    /// Get number of leader elections won.
    pub fn leader_elections_won(&self) -> u64 {
        self.leader_elections_won.load(Ordering::Relaxed)
    }

    /// Get number of leader elections lost.
    pub fn leader_elections_lost(&self) -> u64 {
        self.leader_elections_lost.load(Ordering::Relaxed)
    }

    /// Get number of successful lease renewals.
    pub fn lease_renewals(&self) -> u64 {
        self.lease_renewals.load(Ordering::Relaxed)
    }

    /// Get number of failed lease renewals.
    pub fn lease_renewal_failures(&self) -> u64 {
        self.lease_renewal_failures.load(Ordering::Relaxed)
    }

    /// Get number of failovers detected.
    pub fn failovers_detected(&self) -> u64 {
        self.failovers_detected.load(Ordering::Relaxed)
    }

    /// Record that a heartbeat was sent.
    pub fn record_heartbeat(&self) {
        self.heartbeats_sent.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that this instance won a leader election.
    pub fn record_election_won(&self) {
        self.leader_elections_won.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that this instance lost a leader election.
    pub fn record_election_lost(&self) {
        self.leader_elections_lost.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a successful lease renewal.
    pub fn record_lease_renewal(&self) {
        self.lease_renewals.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a failed lease renewal.
    pub fn record_lease_renewal_failure(&self) {
        self.lease_renewal_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a failover was detected.
    pub fn record_failover(&self) {
        self.failovers_detected.fetch_add(1, Ordering::Relaxed);
    }
}

impl Default for HeartbeatStats {
    fn default() -> Self {
        Self::new()
    }
}

// ---------------------------------------------------------------------------
// BeatHeartbeat
// ---------------------------------------------------------------------------

/// Main heartbeat manager for a beat scheduler instance.
///
/// Manages leader election, lease renewal, and failover detection
/// using a [`DistributedLockBackend`]. Each scheduler instance creates
/// one `BeatHeartbeat` that coordinates with other instances to ensure
/// only one leader is active at any time.
///
/// # Leader Election
///
/// On startup, each instance calls [`try_become_leader`](BeatHeartbeat::try_become_leader).
/// The first instance to acquire the distributed lock becomes the leader.
/// Other instances become standbys.
///
/// # Lease Renewal
///
/// The leader periodically calls [`renew_lease`](BeatHeartbeat::renew_lease)
/// to extend its lock. If renewal fails (e.g., network partition), the
/// instance transitions to `Standby`.
///
/// # Failover
///
/// Standby instances periodically call [`check_leader_health`](BeatHeartbeat::check_leader_health).
/// If the leader's lock has expired, a standby calls `try_become_leader`
/// to promote itself.
pub struct BeatHeartbeat {
    /// Configuration for heartbeat timing and keys
    config: HeartbeatConfig,
    /// Unique identifier for this scheduler instance
    instance_id: String,
    /// Current role of this instance (Leader, Standby, or Unknown)
    role: Arc<RwLock<BeatRole>>,
    /// Distributed lock backend for leader election
    lock_backend: Arc<dyn DistributedLockBackend>,
    /// Current heartbeat information snapshot
    heartbeat_info: Arc<RwLock<HeartbeatInfo>>,
    /// Atomic statistics counters
    stats: Arc<HeartbeatStats>,
    /// Whether the heartbeat loop is running
    running: Arc<AtomicBool>,
}

impl BeatHeartbeat {
    /// Create a new heartbeat manager.
    ///
    /// The instance starts in `Unknown` role. Call [`try_become_leader`](BeatHeartbeat::try_become_leader)
    /// to participate in leader election.
    ///
    /// # Arguments
    /// * `instance_id` - Unique identifier for this scheduler instance
    /// * `lock_backend` - Distributed lock backend for coordination
    /// * `config` - Heartbeat and failover configuration
    pub fn new(
        instance_id: String,
        lock_backend: Arc<dyn DistributedLockBackend>,
        config: HeartbeatConfig,
    ) -> Self {
        let info = HeartbeatInfo::new(instance_id.clone());
        Self {
            config,
            instance_id: instance_id.clone(),
            role: Arc::new(RwLock::new(BeatRole::Unknown)),
            lock_backend,
            heartbeat_info: Arc::new(RwLock::new(info)),
            stats: Arc::new(HeartbeatStats::new()),
            running: Arc::new(AtomicBool::new(true)),
        }
    }

    /// Attempt to become the leader by acquiring the distributed lock.
    ///
    /// If the lock is successfully acquired, the instance transitions to
    /// `Leader` role. If the lock is already held by another instance,
    /// this instance transitions to `Standby` (unless it is already
    /// the leader, in which case the role is unchanged).
    ///
    /// # Returns
    /// `Ok(true)` if leadership was acquired, `Ok(false)` otherwise.
    pub async fn try_become_leader(&self) -> celers_core::error::Result<bool> {
        let acquired = self
            .lock_backend
            .try_acquire(
                &self.config.leader_lock_key,
                &self.instance_id,
                self.config.leader_lease_ttl.as_secs(),
            )
            .await?;

        if acquired {
            let mut role = self.role.write().await;
            *role = BeatRole::Leader;
            let mut info = self.heartbeat_info.write().await;
            info.role = BeatRole::Leader;
            self.stats.record_election_won();
            Ok(true)
        } else {
            let mut role = self.role.write().await;
            if *role != BeatRole::Leader {
                *role = BeatRole::Standby;
            }
            let mut info = self.heartbeat_info.write().await;
            info.role = *role;
            self.stats.record_election_lost();
            Ok(false)
        }
    }

    /// Renew the leader lease by extending the distributed lock TTL.
    ///
    /// Should be called periodically by the leader (at `leader_renewal_interval`).
    /// If renewal fails, the instance transitions to `Standby`.
    ///
    /// # Returns
    /// `Ok(true)` if the lease was renewed, `Ok(false)` if renewal failed
    /// (e.g., the lock expired or was taken by another instance).
    pub async fn renew_lease(&self) -> celers_core::error::Result<bool> {
        let renewed = self
            .lock_backend
            .renew(
                &self.config.leader_lock_key,
                &self.instance_id,
                self.config.leader_lease_ttl.as_secs(),
            )
            .await?;

        if renewed {
            self.stats.record_lease_renewal();
        } else {
            self.stats.record_lease_renewal_failure();
            // Lost leadership
            let mut role = self.role.write().await;
            *role = BeatRole::Standby;
            let mut info = self.heartbeat_info.write().await;
            info.role = BeatRole::Standby;
        }
        Ok(renewed)
    }

    /// Check if the current leader is still alive by inspecting the lock.
    ///
    /// Standby instances use this to detect when the leader has failed
    /// and a new election should be initiated.
    ///
    /// # Returns
    /// `Ok(true)` if a leader lock is currently held, `Ok(false)` if not.
    pub async fn check_leader_health(&self) -> celers_core::error::Result<bool> {
        self.lock_backend
            .is_locked(&self.config.leader_lock_key)
            .await
    }

    /// Update the heartbeat info with the current scheduler state.
    ///
    /// This should be called periodically (at `heartbeat_interval`) to
    /// keep the heartbeat information current.
    ///
    /// # Arguments
    /// * `schedule_count` - Number of tasks currently registered
    /// * `next_due_task` - Name of the next task due for execution
    pub async fn update_info(&self, schedule_count: usize, next_due_task: Option<String>) {
        let current_role = *self.role.read().await;
        let mut info = self.heartbeat_info.write().await;
        info.last_heartbeat_at = Utc::now();
        info.schedule_count = schedule_count;
        info.next_due_task = next_due_task;
        info.role = current_role;
        self.stats.record_heartbeat();
    }

    /// Gracefully shut down the heartbeat manager.
    ///
    /// If this instance is the leader, the leader lock is released so
    /// that another instance can take over immediately instead of
    /// waiting for the lease to expire.
    pub async fn shutdown(&self) -> celers_core::error::Result<()> {
        self.running.store(false, Ordering::SeqCst);
        let role = *self.role.read().await;
        if role == BeatRole::Leader {
            self.lock_backend
                .release(&self.config.leader_lock_key, &self.instance_id)
                .await?;
        }
        let mut r = self.role.write().await;
        *r = BeatRole::Unknown;
        let mut info = self.heartbeat_info.write().await;
        info.role = BeatRole::Unknown;
        Ok(())
    }

    /// Get the current role of this instance.
    pub async fn role(&self) -> BeatRole {
        *self.role.read().await
    }

    /// Check if this instance is currently the leader.
    pub async fn is_leader(&self) -> bool {
        *self.role.read().await == BeatRole::Leader
    }

    /// Check if this instance is currently in standby mode.
    pub async fn is_standby(&self) -> bool {
        *self.role.read().await == BeatRole::Standby
    }

    /// Get a snapshot of the current heartbeat info.
    pub async fn info(&self) -> HeartbeatInfo {
        self.heartbeat_info.read().await.clone()
    }

    /// Get a reference to the heartbeat configuration.
    pub fn config(&self) -> &HeartbeatConfig {
        &self.config
    }

    /// Get the instance ID.
    pub fn instance_id(&self) -> &str {
        &self.instance_id
    }

    /// Get a reference to the statistics tracker.
    pub fn stats(&self) -> &HeartbeatStats {
        &self.stats
    }

    /// Check if the heartbeat loop is still running.
    pub fn is_running(&self) -> bool {
        self.running.load(Ordering::SeqCst)
    }

    /// Execute one tick of the heartbeat loop.
    ///
    /// This is the core state-machine step:
    /// - **Leader**: renew the lease; if renewal fails, try to reacquire.
    /// - **Standby / Unknown**: check if the leader is still alive; if not,
    ///   record a failover and attempt to become leader.
    ///
    /// This method is exposed for testing and for callers that want manual
    /// control over the heartbeat loop timing.
    pub async fn tick(&self) -> celers_core::error::Result<()> {
        let role = self.role().await;
        match role {
            BeatRole::Leader => {
                // Renew lease; if it fails, attempt to reacquire
                if !self.renew_lease().await? {
                    self.try_become_leader().await?;
                }
            }
            BeatRole::Standby | BeatRole::Unknown => {
                // Check if leader is still alive
                let leader_alive = self.check_leader_health().await?;
                if !leader_alive {
                    self.stats.record_failover();
                    self.try_become_leader().await?;
                }
            }
        }
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lock::InMemoryLockBackend;

    fn make_backend() -> Arc<InMemoryLockBackend> {
        Arc::new(InMemoryLockBackend::new())
    }

    fn make_heartbeat(instance_id: &str, backend: Arc<InMemoryLockBackend>) -> BeatHeartbeat {
        BeatHeartbeat::new(instance_id.to_string(), backend, HeartbeatConfig::new())
    }

    // -- Config tests --

    #[test]
    fn test_heartbeat_config_defaults() {
        let config = HeartbeatConfig::new();
        assert_eq!(config.heartbeat_interval, Duration::from_secs(5));
        assert_eq!(config.heartbeat_ttl, Duration::from_secs(15));
        assert_eq!(config.leader_lease_ttl, Duration::from_secs(30));
        assert_eq!(config.leader_renewal_interval, Duration::from_secs(10));
        assert_eq!(config.failover_timeout, Duration::from_secs(45));
        assert_eq!(config.leader_lock_key, "beat_leader");
        assert_eq!(config.heartbeat_key_prefix, "beat_heartbeat:");
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_heartbeat_config_validation() {
        // heartbeat_ttl <= heartbeat_interval => error
        let config = HeartbeatConfig::new().with_heartbeat_ttl(Duration::from_secs(3));
        assert!(config.validate().is_err());

        // leader_lease_ttl <= leader_renewal_interval => error
        let config = HeartbeatConfig::new()
            .with_leader_lease_ttl(Duration::from_secs(5))
            .with_leader_renewal_interval(Duration::from_secs(10));
        assert!(config.validate().is_err());

        // failover_timeout <= leader_lease_ttl => error
        let config = HeartbeatConfig::new().with_failover_timeout(Duration::from_secs(20));
        assert!(config.validate().is_err());
    }

    // -- BeatRole tests --

    #[test]
    fn test_beat_role_display() {
        assert_eq!(BeatRole::Leader.to_string(), "leader");
        assert_eq!(BeatRole::Standby.to_string(), "standby");
        assert_eq!(BeatRole::Unknown.to_string(), "unknown");
    }

    #[test]
    fn test_beat_role_default() {
        assert_eq!(BeatRole::default(), BeatRole::Unknown);
    }

    // -- HeartbeatInfo tests --

    #[test]
    fn test_heartbeat_info_creation() {
        let info = HeartbeatInfo::new("test-instance".to_string());
        assert_eq!(info.instance_id, "test-instance");
        assert_eq!(info.role, BeatRole::Unknown);
        assert_eq!(info.schedule_count, 0);
        assert!(info.next_due_task.is_none());
        assert_eq!(info.version, env!("CARGO_PKG_VERSION"));
    }

    #[test]
    fn test_heartbeat_info_builder() {
        let info = HeartbeatInfo::new("inst".to_string())
            .with_role(BeatRole::Leader)
            .with_schedule_count(5)
            .with_next_due_task(Some("my_task".to_string()));

        assert_eq!(info.role, BeatRole::Leader);
        assert_eq!(info.schedule_count, 5);
        assert_eq!(info.next_due_task.as_deref(), Some("my_task"));
    }

    // -- Leader election tests --

    #[tokio::test]
    async fn test_try_become_leader_success() {
        let backend = make_backend();
        let hb = make_heartbeat("instance-1", backend);

        let result = hb.try_become_leader().await;
        assert!(result.is_ok());
        assert!(result.is_ok_and(|v| v));
        assert!(hb.is_leader().await);
        assert_eq!(hb.stats().leader_elections_won(), 1);
    }

    #[tokio::test]
    async fn test_try_become_leader_already_held() {
        let backend = make_backend();
        let hb1 = make_heartbeat("instance-1", backend.clone());
        let hb2 = make_heartbeat("instance-2", backend);

        // First instance becomes leader
        let r1 = hb1.try_become_leader().await;
        assert!(r1.is_ok_and(|v| v));

        // Second instance fails to become leader
        let r2 = hb2.try_become_leader().await;
        assert!(r2.is_ok_and(|v| !v));
        assert!(hb2.is_standby().await);
        assert_eq!(hb2.stats().leader_elections_lost(), 1);
    }

    #[tokio::test]
    async fn test_renew_lease() {
        let backend = make_backend();
        let hb = make_heartbeat("instance-1", backend);

        // Become leader first
        let _ = hb.try_become_leader().await;
        assert!(hb.is_leader().await);

        // Renew should succeed
        let renewed = hb.renew_lease().await;
        assert!(renewed.is_ok_and(|v| v));
        assert_eq!(hb.stats().lease_renewals(), 1);
        assert!(hb.is_leader().await);
    }

    #[tokio::test]
    async fn test_failover_on_leader_loss() {
        let backend = make_backend();
        let hb1 = make_heartbeat("instance-1", backend.clone());
        let hb2 = make_heartbeat("instance-2", backend.clone());

        // Instance 1 becomes leader
        let _ = hb1.try_become_leader().await;
        assert!(hb1.is_leader().await);

        // Instance 2 is standby
        let _ = hb2.try_become_leader().await;
        assert!(hb2.is_standby().await);

        // Leader releases lock (simulating crash cleanup or graceful shutdown)
        let _ = hb1.shutdown().await;

        // Instance 2 detects no leader and promotes itself
        let leader_alive = hb2.check_leader_health().await;
        assert!(leader_alive.is_ok_and(|v| !v));

        let promoted = hb2.try_become_leader().await;
        assert!(promoted.is_ok_and(|v| v));
        assert!(hb2.is_leader().await);
    }

    #[tokio::test]
    async fn test_graceful_shutdown() {
        let backend = make_backend();
        let hb = make_heartbeat("instance-1", backend.clone());

        // Become leader
        let _ = hb.try_become_leader().await;
        assert!(hb.is_leader().await);

        // Shutdown releases lock
        let result = hb.shutdown().await;
        assert!(result.is_ok());
        assert_eq!(hb.role().await, BeatRole::Unknown);
        assert!(!hb.is_running());

        // Lock should be released
        let locked = backend.is_locked("beat_leader").await;
        assert!(locked.is_ok_and(|v| !v));
    }

    #[tokio::test]
    async fn test_tick_leader_renews() {
        let backend = make_backend();
        let hb = make_heartbeat("instance-1", backend);

        // Become leader
        let _ = hb.try_become_leader().await;
        assert!(hb.is_leader().await);

        // Tick should renew the lease
        let result = hb.tick().await;
        assert!(result.is_ok());
        assert!(hb.is_leader().await);
        assert_eq!(hb.stats().lease_renewals(), 1);
    }

    #[tokio::test]
    async fn test_tick_standby_promotes() {
        let backend = make_backend();
        let hb = make_heartbeat("instance-1", backend);

        // Start as unknown — no leader exists, so tick should promote
        assert_eq!(hb.role().await, BeatRole::Unknown);

        let result = hb.tick().await;
        assert!(result.is_ok());
        assert!(hb.is_leader().await);
        assert_eq!(hb.stats().failovers_detected(), 1);
        assert_eq!(hb.stats().leader_elections_won(), 1);
    }

    #[tokio::test]
    async fn test_stats_tracking() {
        let backend = make_backend();
        let hb = make_heartbeat("instance-1", backend);

        // All start at zero
        assert_eq!(hb.stats().heartbeats_sent(), 0);
        assert_eq!(hb.stats().leader_elections_won(), 0);
        assert_eq!(hb.stats().leader_elections_lost(), 0);
        assert_eq!(hb.stats().lease_renewals(), 0);
        assert_eq!(hb.stats().lease_renewal_failures(), 0);
        assert_eq!(hb.stats().failovers_detected(), 0);

        // Win an election
        let _ = hb.try_become_leader().await;
        assert_eq!(hb.stats().leader_elections_won(), 1);

        // Renew
        let _ = hb.renew_lease().await;
        assert_eq!(hb.stats().lease_renewals(), 1);

        // Send a heartbeat
        hb.update_info(3, Some("task_a".to_string())).await;
        assert_eq!(hb.stats().heartbeats_sent(), 1);
    }

    #[tokio::test]
    async fn test_update_info() {
        let backend = make_backend();
        let hb = make_heartbeat("instance-1", backend);

        // Become leader
        let _ = hb.try_become_leader().await;

        // Update info
        hb.update_info(10, Some("daily_report".to_string())).await;

        let info = hb.info().await;
        assert_eq!(info.schedule_count, 10);
        assert_eq!(info.next_due_task.as_deref(), Some("daily_report"));
        assert_eq!(info.role, BeatRole::Leader);
        assert_eq!(info.instance_id, "instance-1");
    }

    #[tokio::test]
    async fn test_check_leader_health_no_leader() {
        let backend = make_backend();
        let hb = make_heartbeat("instance-1", backend);

        // No leader has been elected
        let alive = hb.check_leader_health().await;
        assert!(alive.is_ok_and(|v| !v));
    }

    #[tokio::test]
    async fn test_check_leader_health_with_leader() {
        let backend = make_backend();
        let hb1 = make_heartbeat("instance-1", backend.clone());
        let hb2 = make_heartbeat("instance-2", backend);

        // Instance 1 becomes leader
        let _ = hb1.try_become_leader().await;

        // Instance 2 sees a live leader
        let alive = hb2.check_leader_health().await;
        assert!(alive.is_ok_and(|v| v));
    }

    #[tokio::test]
    async fn test_config_builder_chain() {
        let config = HeartbeatConfig::new()
            .with_heartbeat_interval(Duration::from_secs(2))
            .with_heartbeat_ttl(Duration::from_secs(10))
            .with_leader_lease_ttl(Duration::from_secs(20))
            .with_leader_renewal_interval(Duration::from_secs(8))
            .with_failover_timeout(Duration::from_secs(30))
            .with_leader_lock_key("custom_leader")
            .with_heartbeat_key_prefix("custom:");

        assert_eq!(config.heartbeat_interval, Duration::from_secs(2));
        assert_eq!(config.heartbeat_ttl, Duration::from_secs(10));
        assert_eq!(config.leader_lease_ttl, Duration::from_secs(20));
        assert_eq!(config.leader_renewal_interval, Duration::from_secs(8));
        assert_eq!(config.failover_timeout, Duration::from_secs(30));
        assert_eq!(config.leader_lock_key, "custom_leader");
        assert_eq!(config.heartbeat_key_prefix, "custom:");
        assert!(config.validate().is_ok());
    }

    #[tokio::test]
    async fn test_renew_lease_not_leader() {
        let backend = make_backend();
        let hb = make_heartbeat("instance-1", backend);

        // Never became leader, so renew should fail gracefully
        let renewed = hb.renew_lease().await;
        assert!(renewed.is_ok_and(|v| !v));
        assert_eq!(hb.stats().lease_renewal_failures(), 1);
    }
}
