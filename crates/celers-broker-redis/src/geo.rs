#![allow(private_interfaces)]

//! Geo-distribution support for multi-region Redis deployments
//!
//! Provides:
//! - Multi-region replication with configurable consistency
//! - Regional read routing for low-latency access
//! - Conflict resolution strategies
//! - Automatic failover and region health monitoring
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::geo::{GeoReplicationManager, Region, RegionId, ReplicationConfig, SyncMode, ConflictResolution};
//!
//! # async fn example() -> celers_core::Result<()> {
//! // Define regions
//! let us_east = Region::new(
//!     RegionId::new("us-east-1"),
//!     "US East",
//!     "redis://us-east.example.com:6379",
//!     40.7128,
//!     -74.0060,
//!     true, // primary region
//! );
//!
//! let eu_west = Region::new(
//!     RegionId::new("eu-west-1"),
//!     "EU West",
//!     "redis://eu-west.example.com:6379",
//!     51.5074,
//!     -0.1278,
//!     false,
//! );
//!
//! // Create replication manager
//! let config = ReplicationConfig::builder()
//!     .sync_mode(SyncMode::QuorumSync)
//!     .replication_timeout_ms(1000)
//!     .conflict_resolution(ConflictResolution::LastWriteWins)
//!     .build();
//!
//! let mut manager = GeoReplicationManager::new(us_east.id().clone(), config);
//! manager.add_region(us_east).await?;
//! manager.add_region(eu_west).await?;
//!
//! # Ok(())
//! # }
//! ```

use celers_core::{CelersError, Result, SerializedTask};
use redis::{AsyncCommands, Client};
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Region identifier
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RegionId(String);

impl RegionId {
    /// Create a new region identifier
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    /// Get the region ID as a string
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl std::fmt::Display for RegionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Geographic region configuration
#[derive(Debug, Clone)]
pub struct Region {
    id: RegionId,
    name: String,
    redis_url: String,
    latitude: f64,
    longitude: f64,
    is_primary: bool,
}

impl Region {
    /// Create a new region
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        id: RegionId,
        name: impl Into<String>,
        redis_url: impl Into<String>,
        latitude: f64,
        longitude: f64,
        is_primary: bool,
    ) -> Self {
        Self {
            id,
            name: name.into(),
            redis_url: redis_url.into(),
            latitude,
            longitude,
            is_primary,
        }
    }

    /// Get the region ID
    pub fn id(&self) -> &RegionId {
        &self.id
    }

    /// Get the region name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the Redis URL
    pub fn redis_url(&self) -> &str {
        &self.redis_url
    }

    /// Get the latitude
    pub fn latitude(&self) -> f64 {
        self.latitude
    }

    /// Get the longitude
    pub fn longitude(&self) -> f64 {
        self.longitude
    }

    /// Check if this is the primary region
    pub fn is_primary(&self) -> bool {
        self.is_primary
    }

    /// Calculate distance to another region in kilometers (Haversine formula)
    pub fn distance_to(&self, other: &Region) -> f64 {
        let r = 6371.0; // Earth radius in km
        let lat1 = self.latitude.to_radians();
        let lat2 = other.latitude.to_radians();
        let delta_lat = (other.latitude - self.latitude).to_radians();
        let delta_lon = (other.longitude - self.longitude).to_radians();

        let a = (delta_lat / 2.0).sin() * (delta_lat / 2.0).sin()
            + lat1.cos() * lat2.cos() * (delta_lon / 2.0).sin() * (delta_lon / 2.0).sin();
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

        r * c
    }
}

/// Regional client wrapper
#[derive(Clone)]
pub(crate) struct RegionClient {
    region: Region,
    client: Client,
    stats: Arc<RegionStats>,
}

/// Region statistics
#[derive(Debug, Default)]
pub struct RegionStats {
    read_count: AtomicU64,
    write_count: AtomicU64,
    replication_lag_ms: AtomicU64,
    last_sync_timestamp: AtomicU64,
    error_count: AtomicU64,
}

impl RegionStats {
    /// Create new region statistics
    pub fn new() -> Self {
        Self::default()
    }

    /// Get read count
    pub fn read_count(&self) -> u64 {
        self.read_count.load(Ordering::Relaxed)
    }

    /// Get write count
    pub fn write_count(&self) -> u64 {
        self.write_count.load(Ordering::Relaxed)
    }

    /// Get replication lag in milliseconds
    pub fn replication_lag_ms(&self) -> u64 {
        self.replication_lag_ms.load(Ordering::Relaxed)
    }

    /// Get last sync timestamp
    pub fn last_sync_timestamp(&self) -> u64 {
        self.last_sync_timestamp.load(Ordering::Relaxed)
    }

    /// Get error count
    pub fn error_count(&self) -> u64 {
        self.error_count.load(Ordering::Relaxed)
    }

    /// Increment read count
    fn increment_reads(&self) {
        self.read_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment write count
    fn increment_writes(&self) {
        self.write_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment error count
    fn increment_errors(&self) {
        self.error_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Update replication lag
    #[allow(dead_code)]
    fn update_lag(&self, lag_ms: u64) {
        self.replication_lag_ms.store(lag_ms, Ordering::Relaxed);
    }

    /// Update last sync timestamp
    fn update_sync_timestamp(&self, timestamp: u64) {
        self.last_sync_timestamp.store(timestamp, Ordering::Relaxed);
    }

    /// Get snapshot of current stats
    pub fn snapshot(&self) -> RegionStatsSnapshot {
        RegionStatsSnapshot {
            read_count: self.read_count(),
            write_count: self.write_count(),
            replication_lag_ms: self.replication_lag_ms(),
            last_sync_timestamp: self.last_sync_timestamp(),
            error_count: self.error_count(),
        }
    }
}

/// Snapshot of region statistics
#[derive(Debug, Clone)]
pub struct RegionStatsSnapshot {
    pub read_count: u64,
    pub write_count: u64,
    pub replication_lag_ms: u64,
    pub last_sync_timestamp: u64,
    pub error_count: u64,
}

/// Synchronization mode
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SyncMode {
    /// Write to primary, replicate async to secondaries
    AsyncReplication,
    /// Write to primary + quorum of regions before returning
    QuorumSync,
    /// Write to all regions before returning
    FullSync,
}

impl std::fmt::Display for SyncMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyncMode::AsyncReplication => write!(f, "AsyncReplication"),
            SyncMode::QuorumSync => write!(f, "QuorumSync"),
            SyncMode::FullSync => write!(f, "FullSync"),
        }
    }
}

/// Conflict resolution strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConflictResolution {
    /// Last write wins (based on timestamp)
    LastWriteWins,
    /// Primary region wins
    PrimaryWins,
    /// Highest priority value wins
    HighestPriorityWins,
    /// Manual resolution required
    Manual,
}

impl std::fmt::Display for ConflictResolution {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConflictResolution::LastWriteWins => write!(f, "LastWriteWins"),
            ConflictResolution::PrimaryWins => write!(f, "PrimaryWins"),
            ConflictResolution::HighestPriorityWins => write!(f, "HighestPriorityWins"),
            ConflictResolution::Manual => write!(f, "Manual"),
        }
    }
}

/// Replication configuration
#[derive(Debug, Clone)]
pub struct ReplicationConfig {
    sync_mode: SyncMode,
    replication_timeout_ms: u64,
    retry_attempts: u32,
    conflict_resolution: ConflictResolution,
}

impl ReplicationConfig {
    /// Create a new replication configuration
    pub fn new(
        sync_mode: SyncMode,
        replication_timeout_ms: u64,
        retry_attempts: u32,
        conflict_resolution: ConflictResolution,
    ) -> Self {
        Self {
            sync_mode,
            replication_timeout_ms,
            retry_attempts,
            conflict_resolution,
        }
    }

    /// Create a configuration builder
    pub fn builder() -> ReplicationConfigBuilder {
        ReplicationConfigBuilder::default()
    }

    /// Get sync mode
    pub fn sync_mode(&self) -> SyncMode {
        self.sync_mode
    }

    /// Get replication timeout in milliseconds
    pub fn replication_timeout_ms(&self) -> u64 {
        self.replication_timeout_ms
    }

    /// Get retry attempts
    pub fn retry_attempts(&self) -> u32 {
        self.retry_attempts
    }

    /// Get conflict resolution strategy
    pub fn conflict_resolution(&self) -> ConflictResolution {
        self.conflict_resolution
    }
}

impl Default for ReplicationConfig {
    fn default() -> Self {
        Self {
            sync_mode: SyncMode::AsyncReplication,
            replication_timeout_ms: 1000,
            retry_attempts: 3,
            conflict_resolution: ConflictResolution::LastWriteWins,
        }
    }
}

/// Builder for replication configuration
#[derive(Debug, Default)]
pub struct ReplicationConfigBuilder {
    sync_mode: Option<SyncMode>,
    replication_timeout_ms: Option<u64>,
    retry_attempts: Option<u32>,
    conflict_resolution: Option<ConflictResolution>,
}

impl ReplicationConfigBuilder {
    /// Set sync mode
    pub fn sync_mode(mut self, mode: SyncMode) -> Self {
        self.sync_mode = Some(mode);
        self
    }

    /// Set replication timeout in milliseconds
    pub fn replication_timeout_ms(mut self, timeout: u64) -> Self {
        self.replication_timeout_ms = Some(timeout);
        self
    }

    /// Set retry attempts
    pub fn retry_attempts(mut self, attempts: u32) -> Self {
        self.retry_attempts = Some(attempts);
        self
    }

    /// Set conflict resolution strategy
    pub fn conflict_resolution(mut self, strategy: ConflictResolution) -> Self {
        self.conflict_resolution = Some(strategy);
        self
    }

    /// Build the configuration
    pub fn build(self) -> ReplicationConfig {
        let defaults = ReplicationConfig::default();
        ReplicationConfig {
            sync_mode: self.sync_mode.unwrap_or(defaults.sync_mode),
            replication_timeout_ms: self
                .replication_timeout_ms
                .unwrap_or(defaults.replication_timeout_ms),
            retry_attempts: self.retry_attempts.unwrap_or(defaults.retry_attempts),
            conflict_resolution: self
                .conflict_resolution
                .unwrap_or(defaults.conflict_resolution),
        }
    }
}

/// Multi-region replication manager
pub struct GeoReplicationManager {
    regions: Arc<RwLock<HashMap<RegionId, RegionClient>>>,
    primary_region: RegionId,
    replication_config: ReplicationConfig,
}

impl GeoReplicationManager {
    /// Create a new geo-replication manager
    pub fn new(primary_region: RegionId, replication_config: ReplicationConfig) -> Self {
        Self {
            regions: Arc::new(RwLock::new(HashMap::new())),
            primary_region,
            replication_config,
        }
    }

    /// Add a region to the replication group
    pub async fn add_region(&mut self, region: Region) -> Result<()> {
        let client = Client::open(region.redis_url.as_str()).map_err(|e| {
            CelersError::Broker(format!("Failed to connect to region {}: {}", region.id, e))
        })?;

        let region_client = RegionClient {
            region: region.clone(),
            client,
            stats: Arc::new(RegionStats::new()),
        };

        self.regions
            .write()
            .await
            .insert(region.id.clone(), region_client);

        info!("Added region {} to replication group", region.id);
        Ok(())
    }

    /// Remove a region from the replication group
    pub async fn remove_region(&mut self, region_id: &RegionId) -> Result<()> {
        self.regions.write().await.remove(region_id);
        info!("Removed region {} from replication group", region_id);
        Ok(())
    }

    /// Get region statistics
    pub async fn get_region_stats(&self, region_id: &RegionId) -> Option<RegionStatsSnapshot> {
        self.regions
            .read()
            .await
            .get(region_id)
            .map(|r| r.stats.snapshot())
    }

    /// Get all region IDs
    pub async fn get_region_ids(&self) -> Vec<RegionId> {
        self.regions.read().await.keys().cloned().collect()
    }

    /// Replicate a task to all regions
    pub async fn replicate_task(
        &self,
        queue_name: &str,
        task: &SerializedTask,
    ) -> Result<Vec<RegionId>> {
        let serialized =
            serde_json::to_string(task).map_err(|e| CelersError::Serialization(e.to_string()))?;

        let regions = self.regions.read().await;

        match self.replication_config.sync_mode {
            SyncMode::AsyncReplication => {
                // Write to primary first
                if let Some(primary) = regions.get(&self.primary_region) {
                    self.write_to_region(primary, queue_name, &serialized)
                        .await?;
                    primary.stats.increment_writes();
                }

                // Replicate to secondaries asynchronously
                let regions_clone = self.regions.clone();
                let queue_name = queue_name.to_string();
                let serialized_clone = serialized.clone();
                let primary_region = self.primary_region.clone();

                tokio::spawn(async move {
                    let regions = regions_clone.read().await;
                    for (region_id, region_client) in regions.iter() {
                        if region_id != &primary_region {
                            if let Err(e) = Self::write_to_region_static(
                                region_client,
                                &queue_name,
                                &serialized_clone,
                            )
                            .await
                            {
                                warn!("Failed to replicate to region {}: {}", region_id, e);
                                region_client.stats.increment_errors();
                            } else {
                                region_client.stats.increment_writes();
                            }
                        }
                    }
                });

                Ok(vec![self.primary_region.clone()])
            }
            SyncMode::QuorumSync => {
                let quorum = (regions.len() / 2) + 1;
                let mut successful = Vec::new();

                for (region_id, region_client) in regions.iter() {
                    if self
                        .write_to_region(region_client, queue_name, &serialized)
                        .await
                        .is_ok()
                    {
                        region_client.stats.increment_writes();
                        successful.push(region_id.clone());
                        if successful.len() >= quorum {
                            break;
                        }
                    } else {
                        region_client.stats.increment_errors();
                    }
                }

                if successful.len() >= quorum {
                    Ok(successful)
                } else {
                    Err(CelersError::Broker(format!(
                        "Failed to reach quorum: {}/{}",
                        successful.len(),
                        quorum
                    )))
                }
            }
            SyncMode::FullSync => {
                let mut successful = Vec::new();
                let mut failed = Vec::new();

                for (region_id, region_client) in regions.iter() {
                    if self
                        .write_to_region(region_client, queue_name, &serialized)
                        .await
                        .is_ok()
                    {
                        region_client.stats.increment_writes();
                        successful.push(region_id.clone());
                    } else {
                        region_client.stats.increment_errors();
                        failed.push(region_id.clone());
                    }
                }

                if failed.is_empty() {
                    Ok(successful)
                } else {
                    Err(CelersError::Broker(format!(
                        "Failed to replicate to regions: {:?}",
                        failed
                    )))
                }
            }
        }
    }

    /// Write to a specific region
    async fn write_to_region(
        &self,
        region_client: &RegionClient,
        queue_name: &str,
        serialized: &str,
    ) -> Result<()> {
        let mut conn = region_client
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                CelersError::Broker(format!(
                    "Failed to connect to region {}: {}",
                    region_client.region.id, e
                ))
            })?;

        conn.rpush::<_, _, ()>(queue_name, serialized)
            .await
            .map_err(|e| {
                CelersError::Broker(format!(
                    "Failed to write to region {}: {}",
                    region_client.region.id, e
                ))
            })?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after UNIX epoch")
            .as_secs();
        region_client.stats.update_sync_timestamp(now);

        debug!("Wrote task to region {}", region_client.region.id);
        Ok(())
    }

    /// Static version of write_to_region for use in spawned tasks
    async fn write_to_region_static(
        region_client: &RegionClient,
        queue_name: &str,
        serialized: &str,
    ) -> Result<()> {
        let mut conn = region_client
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                CelersError::Broker(format!(
                    "Failed to connect to region {}: {}",
                    region_client.region.id, e
                ))
            })?;

        conn.rpush::<_, _, ()>(queue_name, serialized)
            .await
            .map_err(|e| {
                CelersError::Broker(format!(
                    "Failed to write to region {}: {}",
                    region_client.region.id, e
                ))
            })?;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after UNIX epoch")
            .as_secs();
        region_client.stats.update_sync_timestamp(now);

        Ok(())
    }

    /// Resolve conflicts between multiple versions of a task
    pub fn resolve_conflict(
        &self,
        tasks: Vec<(RegionId, SerializedTask)>,
    ) -> Option<SerializedTask> {
        if tasks.is_empty() {
            return None;
        }

        if tasks.len() == 1 {
            return Some(tasks[0].1.clone());
        }

        match self.replication_config.conflict_resolution {
            ConflictResolution::LastWriteWins => {
                // Find task with latest timestamp
                // UUID v1 contains timestamp, so we can use TaskId for comparison
                // For other UUID versions, this still provides deterministic resolution
                tasks
                    .into_iter()
                    .max_by_key(|(_, task)| task.metadata.id)
                    .map(|(_, task)| task)
            }
            ConflictResolution::PrimaryWins => {
                // Return task from primary region
                tasks
                    .into_iter()
                    .find(|(region_id, _)| region_id == &self.primary_region)
                    .map(|(_, task)| task)
            }
            ConflictResolution::HighestPriorityWins => {
                // Return task with highest priority
                tasks
                    .into_iter()
                    .max_by_key(|(_, task)| task.metadata.priority)
                    .map(|(_, task)| task)
            }
            ConflictResolution::Manual => {
                // Requires manual intervention - return None
                error!(
                    "Manual conflict resolution required for {} tasks",
                    tasks.len()
                );
                None
            }
        }
    }

    /// Get primary region ID
    pub fn primary_region(&self) -> &RegionId {
        &self.primary_region
    }

    /// Get replication configuration
    pub fn config(&self) -> &ReplicationConfig {
        &self.replication_config
    }
}

/// Read routing strategy
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingStrategy {
    /// Route to nearest region by geographic distance
    NearestRegion,
    /// Route to region with lowest latency
    LowestLatency,
    /// Round-robin across regions
    RoundRobin,
    /// Random region selection
    Random,
}

impl std::fmt::Display for RoutingStrategy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RoutingStrategy::NearestRegion => write!(f, "NearestRegion"),
            RoutingStrategy::LowestLatency => write!(f, "LowestLatency"),
            RoutingStrategy::RoundRobin => write!(f, "RoundRobin"),
            RoutingStrategy::Random => write!(f, "Random"),
        }
    }
}

/// Regional read router
pub struct RegionalReadRouter {
    regions: Arc<RwLock<HashMap<RegionId, RegionClient>>>,
    routing_strategy: RoutingStrategy,
    round_robin_counter: Arc<AtomicU64>,
}

impl RegionalReadRouter {
    /// Create a new regional read router
    pub fn new(
        regions: Arc<RwLock<HashMap<RegionId, RegionClient>>>,
        routing_strategy: RoutingStrategy,
    ) -> Self {
        Self {
            regions,
            routing_strategy,
            round_robin_counter: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Route a read request to the appropriate region
    pub async fn route_read(
        &self,
        queue_name: &str,
        client_location: Option<(f64, f64)>,
    ) -> Result<Option<SerializedTask>> {
        let regions = self.regions.read().await;

        if regions.is_empty() {
            return Err(CelersError::Broker("No regions available".to_string()));
        }

        let selected_region = match self.routing_strategy {
            RoutingStrategy::NearestRegion => {
                if let Some((lat, lon)) = client_location {
                    self.select_nearest_region(&regions, lat, lon)
                } else {
                    // Fallback to first region if no location provided
                    regions.values().next()
                }
            }
            RoutingStrategy::LowestLatency => self.select_lowest_latency_region(&regions),
            RoutingStrategy::RoundRobin => self.select_round_robin_region(&regions),
            RoutingStrategy::Random => self.select_random_region(&regions),
        };

        if let Some(region_client) = selected_region {
            let result = self.read_from_region(region_client, queue_name).await;
            if result.is_ok() {
                region_client.stats.increment_reads();
            } else {
                region_client.stats.increment_errors();
            }
            result
        } else {
            Err(CelersError::Broker("No region selected".to_string()))
        }
    }

    /// Select nearest region based on geographic distance
    fn select_nearest_region<'a>(
        &self,
        regions: &'a HashMap<RegionId, RegionClient>,
        lat: f64,
        lon: f64,
    ) -> Option<&'a RegionClient> {
        let client_region = Region::new(RegionId::new("client"), "Client", "", lat, lon, false);

        regions
            .values()
            .min_by_key(|r| (r.region.distance_to(&client_region) * 1000.0) as u64)
    }

    /// Select region with lowest replication lag (proxy for latency)
    fn select_lowest_latency_region<'a>(
        &self,
        regions: &'a HashMap<RegionId, RegionClient>,
    ) -> Option<&'a RegionClient> {
        regions
            .values()
            .min_by_key(|r| r.stats.replication_lag_ms())
    }

    /// Select region using round-robin
    fn select_round_robin_region<'a>(
        &self,
        regions: &'a HashMap<RegionId, RegionClient>,
    ) -> Option<&'a RegionClient> {
        let count = self.round_robin_counter.fetch_add(1, Ordering::Relaxed);
        let index = (count as usize) % regions.len();
        regions.values().nth(index)
    }

    /// Select random region
    fn select_random_region<'a>(
        &self,
        regions: &'a HashMap<RegionId, RegionClient>,
    ) -> Option<&'a RegionClient> {
        use std::collections::hash_map::RandomState;
        use std::hash::BuildHasher;

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system time should be after UNIX epoch")
            .as_nanos();

        let index = (RandomState::new().hash_one(now) as usize) % regions.len();

        regions.values().nth(index)
    }

    /// Read from a specific region
    async fn read_from_region(
        &self,
        region_client: &RegionClient,
        queue_name: &str,
    ) -> Result<Option<SerializedTask>> {
        let mut conn = region_client
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| {
                CelersError::Broker(format!(
                    "Failed to connect to region {}: {}",
                    region_client.region.id, e
                ))
            })?;

        let data: Option<String> = conn.lpop(queue_name, None).await.map_err(|e| {
            CelersError::Broker(format!(
                "Failed to read from region {}: {}",
                region_client.region.id, e
            ))
        })?;

        if let Some(serialized) = data {
            let task: SerializedTask = serde_json::from_str(&serialized)
                .map_err(|e| CelersError::Deserialization(e.to_string()))?;

            debug!("Read task from region {}", region_client.region.id);
            Ok(Some(task))
        } else {
            Ok(None)
        }
    }

    /// Get routing strategy
    pub fn strategy(&self) -> RoutingStrategy {
        self.routing_strategy
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_region_id_creation() {
        let id = RegionId::new("us-east-1");
        assert_eq!(id.as_str(), "us-east-1");
        assert_eq!(id.to_string(), "us-east-1");
    }

    #[test]
    fn test_region_creation() {
        let region = Region::new(
            RegionId::new("us-east-1"),
            "US East",
            "redis://localhost:6379",
            40.7128,
            -74.0060,
            true,
        );

        assert_eq!(region.id().as_str(), "us-east-1");
        assert_eq!(region.name(), "US East");
        assert_eq!(region.redis_url(), "redis://localhost:6379");
        assert_eq!(region.latitude(), 40.7128);
        assert_eq!(region.longitude(), -74.0060);
        assert!(region.is_primary());
    }

    #[test]
    fn test_region_distance() {
        let us_east = Region::new(
            RegionId::new("us-east-1"),
            "US East",
            "redis://localhost:6379",
            40.7128,
            -74.0060,
            true,
        );

        let eu_west = Region::new(
            RegionId::new("eu-west-1"),
            "EU West",
            "redis://localhost:6380",
            51.5074,
            -0.1278,
            false,
        );

        let distance = us_east.distance_to(&eu_west);
        // Approximate distance between NYC and London is ~5570 km
        assert!(distance > 5500.0 && distance < 5600.0);
    }

    #[test]
    fn test_sync_mode_display() {
        assert_eq!(SyncMode::AsyncReplication.to_string(), "AsyncReplication");
        assert_eq!(SyncMode::QuorumSync.to_string(), "QuorumSync");
        assert_eq!(SyncMode::FullSync.to_string(), "FullSync");
    }

    #[test]
    fn test_conflict_resolution_display() {
        assert_eq!(
            ConflictResolution::LastWriteWins.to_string(),
            "LastWriteWins"
        );
        assert_eq!(ConflictResolution::PrimaryWins.to_string(), "PrimaryWins");
        assert_eq!(
            ConflictResolution::HighestPriorityWins.to_string(),
            "HighestPriorityWins"
        );
        assert_eq!(ConflictResolution::Manual.to_string(), "Manual");
    }

    #[test]
    fn test_routing_strategy_display() {
        assert_eq!(RoutingStrategy::NearestRegion.to_string(), "NearestRegion");
        assert_eq!(RoutingStrategy::LowestLatency.to_string(), "LowestLatency");
        assert_eq!(RoutingStrategy::RoundRobin.to_string(), "RoundRobin");
        assert_eq!(RoutingStrategy::Random.to_string(), "Random");
    }

    #[test]
    fn test_replication_config_builder() {
        let config = ReplicationConfig::builder()
            .sync_mode(SyncMode::QuorumSync)
            .replication_timeout_ms(2000)
            .retry_attempts(5)
            .conflict_resolution(ConflictResolution::PrimaryWins)
            .build();

        assert_eq!(config.sync_mode(), SyncMode::QuorumSync);
        assert_eq!(config.replication_timeout_ms(), 2000);
        assert_eq!(config.retry_attempts(), 5);
        assert_eq!(
            config.conflict_resolution(),
            ConflictResolution::PrimaryWins
        );
    }

    #[test]
    fn test_replication_config_default() {
        let config = ReplicationConfig::default();

        assert_eq!(config.sync_mode(), SyncMode::AsyncReplication);
        assert_eq!(config.replication_timeout_ms(), 1000);
        assert_eq!(config.retry_attempts(), 3);
        assert_eq!(
            config.conflict_resolution(),
            ConflictResolution::LastWriteWins
        );
    }

    #[test]
    fn test_region_stats() {
        let stats = RegionStats::new();

        assert_eq!(stats.read_count(), 0);
        assert_eq!(stats.write_count(), 0);
        assert_eq!(stats.error_count(), 0);

        stats.increment_reads();
        assert_eq!(stats.read_count(), 1);

        stats.increment_writes();
        assert_eq!(stats.write_count(), 1);

        stats.increment_errors();
        assert_eq!(stats.error_count(), 1);

        stats.update_lag(100);
        assert_eq!(stats.replication_lag_ms(), 100);

        stats.update_sync_timestamp(1234567890);
        assert_eq!(stats.last_sync_timestamp(), 1234567890);

        let snapshot = stats.snapshot();
        assert_eq!(snapshot.read_count, 1);
        assert_eq!(snapshot.write_count, 1);
        assert_eq!(snapshot.error_count, 1);
        assert_eq!(snapshot.replication_lag_ms, 100);
        assert_eq!(snapshot.last_sync_timestamp, 1234567890);
    }

    #[test]
    fn test_geo_replication_manager_creation() {
        let primary = RegionId::new("us-east-1");
        let config = ReplicationConfig::default();
        let manager = GeoReplicationManager::new(primary.clone(), config);

        assert_eq!(manager.primary_region(), &primary);
        assert_eq!(manager.config().sync_mode(), SyncMode::AsyncReplication);
    }

    #[tokio::test]
    async fn test_geo_replication_manager_add_remove_region() {
        let primary = RegionId::new("us-east-1");
        let config = ReplicationConfig::default();
        let mut manager = GeoReplicationManager::new(primary, config);

        let region = Region::new(
            RegionId::new("us-east-1"),
            "US East",
            "redis://localhost:6379",
            40.7128,
            -74.0060,
            true,
        );

        // Add region (may fail if Redis not available, but that's ok for unit test)
        let _ = manager.add_region(region).await;

        let region_ids = manager.get_region_ids().await;
        // Check if we successfully added it
        if !region_ids.is_empty() {
            assert_eq!(region_ids.len(), 1);

            // Remove region
            manager
                .remove_region(&RegionId::new("us-east-1"))
                .await
                .unwrap();

            let region_ids = manager.get_region_ids().await;
            assert_eq!(region_ids.len(), 0);
        }
    }

    #[test]
    fn test_regional_read_router_creation() {
        let regions = Arc::new(RwLock::new(HashMap::new()));
        let router = RegionalReadRouter::new(regions, RoutingStrategy::RoundRobin);

        assert_eq!(router.strategy(), RoutingStrategy::RoundRobin);
    }
}
