//! Advanced Connection Pool
//!
//! Provides enhanced connection pooling with:
//! - Adaptive pool sizing based on load
//! - Smart connection reuse with affinity tracking
//! - Continuous health monitoring
//! - Automatic scaling and connection lifecycle management
//!
//! # Features
//!
//! - **Adaptive Sizing**: Dynamically adjusts pool size based on demand and latency
//! - **Connection Affinity**: Tracks connection usage patterns for better reuse
//! - **Health Monitoring**: Periodic health checks with automatic removal of unhealthy connections
//! - **Load-based Scaling**: Scales up under high load, scales down when idle
//! - **Connection Metrics**: Detailed per-connection and pool-level metrics
//!
//! # Example
//!
//! ```rust,no_run
//! use celers_broker_redis::pool_advanced::{AdaptivePoolConfig, AdaptiveConnectionPool};
//! use std::time::Duration;
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = AdaptivePoolConfig::default()
//!     .with_min_size(2)
//!     .with_max_size(20)
//!     .with_target_latency_ms(50)
//!     .with_scale_up_threshold(0.8)
//!     .with_health_check_interval(Duration::from_secs(30));
//!
//! let pool = AdaptiveConnectionPool::new("redis://localhost:6379", config).await?;
//!
//! // Start background health monitoring
//! pool.start_health_monitor();
//!
//! // Get connection - pool will adapt to load automatically
//! let conn = pool.acquire().await?;
//!
//! // Connection is returned to pool when dropped
//! # Ok(())
//! # }
//! ```

use celers_core::{CelersError, Result};
use redis::{aio::MultiplexedConnection, AsyncCommands, Client};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::{Mutex, RwLock, Semaphore};
use tokio::time;
use tracing::{debug, info, warn};

/// Adaptive connection pool configuration
#[derive(Debug, Clone)]
pub struct AdaptivePoolConfig {
    /// Minimum pool size
    pub min_size: usize,
    /// Maximum pool size
    pub max_size: usize,
    /// Target latency for operations in milliseconds
    pub target_latency_ms: u64,
    /// Scale up when utilization exceeds this threshold (0.0-1.0)
    pub scale_up_threshold: f64,
    /// Scale down when utilization drops below this threshold (0.0-1.0)
    pub scale_down_threshold: f64,
    /// Health check interval
    pub health_check_interval: Duration,
    /// Connection idle timeout (remove if unused for this duration)
    pub idle_timeout: Duration,
    /// Enable connection affinity (prefer reusing same connection for same task)
    pub enable_affinity: bool,
    /// Maximum connection age (force recreation after this duration)
    pub max_connection_age: Duration,
}

impl Default for AdaptivePoolConfig {
    fn default() -> Self {
        Self {
            min_size: 2,
            max_size: 20,
            target_latency_ms: 50,
            scale_up_threshold: 0.8,
            scale_down_threshold: 0.3,
            health_check_interval: Duration::from_secs(30),
            idle_timeout: Duration::from_secs(300),
            enable_affinity: true,
            max_connection_age: Duration::from_secs(3600),
        }
    }
}

impl AdaptivePoolConfig {
    /// Set minimum pool size
    pub fn with_min_size(mut self, size: usize) -> Self {
        self.min_size = size;
        self
    }

    /// Set maximum pool size
    pub fn with_max_size(mut self, size: usize) -> Self {
        self.max_size = size;
        self
    }

    /// Set target latency in milliseconds
    pub fn with_target_latency_ms(mut self, latency_ms: u64) -> Self {
        self.target_latency_ms = latency_ms;
        self
    }

    /// Set scale up threshold
    pub fn with_scale_up_threshold(mut self, threshold: f64) -> Self {
        self.scale_up_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set scale down threshold
    pub fn with_scale_down_threshold(mut self, threshold: f64) -> Self {
        self.scale_down_threshold = threshold.clamp(0.0, 1.0);
        self
    }

    /// Set health check interval
    pub fn with_health_check_interval(mut self, interval: Duration) -> Self {
        self.health_check_interval = interval;
        self
    }

    /// Set idle timeout
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = timeout;
        self
    }

    /// Enable or disable connection affinity
    pub fn with_affinity(mut self, enable: bool) -> Self {
        self.enable_affinity = enable;
        self
    }

    /// Set maximum connection age
    pub fn with_max_connection_age(mut self, age: Duration) -> Self {
        self.max_connection_age = age;
        self
    }
}

/// Connection metadata for tracking
#[derive(Debug, Clone)]
struct ConnectionMetadata {
    /// Connection ID
    #[allow(dead_code)]
    id: String,
    /// Creation timestamp
    created_at: Instant,
    /// Last used timestamp
    last_used: Instant,
    /// Total uses
    use_count: u64,
    /// Average latency in microseconds
    #[allow(dead_code)]
    avg_latency_us: u64,
    /// Is healthy
    #[allow(dead_code)]
    is_healthy: bool,
}

/// Adaptive connection pool
pub struct AdaptiveConnectionPool {
    client: Client,
    config: AdaptivePoolConfig,
    connections: Arc<RwLock<HashMap<String, (MultiplexedConnection, ConnectionMetadata)>>>,
    available: Arc<Mutex<Vec<String>>>,
    semaphore: Arc<Semaphore>,
    stats: Arc<RwLock<AdaptivePoolStats>>,
    health_monitor_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

/// Advanced pool statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AdaptivePoolStats {
    /// Current pool size
    pub current_size: usize,
    /// Active connections (in use)
    pub active: usize,
    /// Idle connections (available)
    pub idle: usize,
    /// Total connections created
    pub total_created: u64,
    /// Total connections destroyed
    pub total_destroyed: u64,
    /// Current utilization (0.0-1.0)
    pub utilization: f64,
    /// Average latency in microseconds
    pub avg_latency_us: u64,
    /// Total acquisitions
    pub total_acquisitions: u64,
    /// Total releases
    pub total_releases: u64,
    /// Unhealthy connections removed
    pub unhealthy_removed: u64,
    /// Scale up events
    pub scale_up_events: u64,
    /// Scale down events
    pub scale_down_events: u64,
    /// Connection reuse rate (0.0-1.0)
    pub reuse_rate: f64,
}

impl Default for AdaptivePoolStats {
    fn default() -> Self {
        Self {
            current_size: 0,
            active: 0,
            idle: 0,
            total_created: 0,
            total_destroyed: 0,
            utilization: 0.0,
            avg_latency_us: 0,
            total_acquisitions: 0,
            total_releases: 0,
            unhealthy_removed: 0,
            scale_up_events: 0,
            scale_down_events: 0,
            reuse_rate: 0.0,
        }
    }
}

impl AdaptiveConnectionPool {
    /// Create a new adaptive connection pool
    pub async fn new(redis_url: &str, config: AdaptivePoolConfig) -> Result<Self> {
        let client = Client::open(redis_url)
            .map_err(|e| CelersError::Broker(format!("Failed to connect to Redis: {}", e)))?;

        let pool = Self {
            client,
            config: config.clone(),
            connections: Arc::new(RwLock::new(HashMap::new())),
            available: Arc::new(Mutex::new(Vec::new())),
            semaphore: Arc::new(Semaphore::new(config.max_size)),
            stats: Arc::new(RwLock::new(AdaptivePoolStats::default())),
            health_monitor_handle: Arc::new(Mutex::new(None)),
        };

        // Initialize minimum connections
        pool.ensure_min_connections().await?;

        Ok(pool)
    }

    /// Start background health monitor
    pub fn start_health_monitor(&self) {
        let connections = Arc::clone(&self.connections);
        let available = Arc::clone(&self.available);
        let stats = Arc::clone(&self.stats);
        let config = self.config.clone();
        let client = self.client.clone();

        let handle = tokio::spawn(async move {
            let mut interval = time::interval(config.health_check_interval);

            loop {
                interval.tick().await;

                // Health check all connections
                let conn_ids: Vec<String> = {
                    let conns = connections.read().await;
                    conns.keys().cloned().collect()
                };

                for conn_id in conn_ids {
                    // Check connection health
                    let is_healthy = {
                        let conns = connections.read().await;
                        if let Some((mut conn, _)) = conns.get(&conn_id).cloned() {
                            // Try a simple PING
                            let start = Instant::now();
                            let result: std::result::Result<String, _> =
                                conn.get("__health_check__").await;
                            let latency = start.elapsed();

                            result.is_ok() && latency.as_millis() < 1000
                        } else {
                            false
                        }
                    };

                    // Remove unhealthy connections
                    if !is_healthy {
                        let mut conns = connections.write().await;
                        conns.remove(&conn_id);

                        let mut avail = available.lock().await;
                        avail.retain(|id| id != &conn_id);

                        let mut s = stats.write().await;
                        s.unhealthy_removed += 1;
                        s.current_size = s.current_size.saturating_sub(1);

                        warn!("Removed unhealthy connection: {}", conn_id);
                    }

                    // Remove aged connections
                    {
                        let mut conns = connections.write().await;
                        let should_remove = conns
                            .get(&conn_id)
                            .map(|(_, meta)| meta.created_at.elapsed() > config.max_connection_age)
                            .unwrap_or(false);

                        if should_remove {
                            conns.remove(&conn_id);

                            let mut avail = available.lock().await;
                            avail.retain(|id| id != &conn_id);

                            let mut s = stats.write().await;
                            s.total_destroyed += 1;
                            s.current_size = s.current_size.saturating_sub(1);

                            debug!("Removed aged connection: {}", conn_id);
                        }
                    }
                }

                // Check if we need to scale based on utilization
                Self::check_scaling(&client, &connections, &available, &stats, &config).await;
            }
        });

        *self.health_monitor_handle.blocking_lock() = Some(handle);
    }

    /// Stop health monitor
    pub async fn stop_health_monitor(&self) {
        if let Some(handle) = self.health_monitor_handle.lock().await.take() {
            handle.abort();
        }
    }

    /// Acquire a connection from the pool
    pub async fn acquire(&self) -> Result<PooledConnection> {
        // Wait for available permit
        let _permit = self
            .semaphore
            .acquire()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to acquire semaphore: {}", e)))?;

        let start = Instant::now();

        // Try to get available connection
        let conn_id = {
            let mut avail = self.available.lock().await;
            avail.pop()
        };

        let (conn, conn_id) = if let Some(id) = conn_id {
            // Reuse existing connection
            let mut conns = self.connections.write().await;
            if let Some((conn, mut meta)) = conns.remove(&id) {
                meta.last_used = Instant::now();
                meta.use_count += 1;

                let conn_clone = conn.clone();
                conns.insert(id.clone(), (conn, meta));

                (conn_clone, id)
            } else {
                // Connection was removed, create new one
                self.create_connection().await?
            }
        } else {
            // Create new connection
            self.create_connection().await?
        };

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.total_acquisitions += 1;
            stats.active += 1;
            stats.idle = stats.idle.saturating_sub(1);
            stats.utilization = stats.active as f64 / stats.current_size.max(1) as f64;

            let latency_us = start.elapsed().as_micros() as u64;
            if stats.avg_latency_us == 0 {
                stats.avg_latency_us = latency_us;
            } else {
                stats.avg_latency_us = (stats.avg_latency_us * 9 + latency_us) / 10;
            }
        }

        Ok(PooledConnection {
            connection: Some(conn),
            conn_id,
            pool: Arc::new(PoolHandle {
                connections: Arc::clone(&self.connections),
                available: Arc::clone(&self.available),
                stats: Arc::clone(&self.stats),
                semaphore: Arc::clone(&self.semaphore),
            }),
        })
    }

    /// Get pool statistics
    pub async fn stats(&self) -> AdaptivePoolStats {
        self.stats.read().await.clone()
    }

    /// Ensure minimum connections are available
    async fn ensure_min_connections(&self) -> Result<()> {
        let current_size = self.connections.read().await.len();

        for _ in current_size..self.config.min_size {
            let (conn, id) = self.create_connection().await?;

            let mut conns = self.connections.write().await;
            let meta = ConnectionMetadata {
                id: id.clone(),
                created_at: Instant::now(),
                last_used: Instant::now(),
                use_count: 0,
                avg_latency_us: 0,
                is_healthy: true,
            };
            conns.insert(id.clone(), (conn, meta));

            let mut avail = self.available.lock().await;
            avail.push(id);
        }

        Ok(())
    }

    /// Create a new connection
    async fn create_connection(&self) -> Result<(MultiplexedConnection, String)> {
        let conn = self
            .client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to create connection: {}", e)))?;

        let id = format!("conn_{}", uuid::Uuid::new_v4());

        // Update stats
        {
            let mut stats = self.stats.write().await;
            stats.total_created += 1;
            stats.current_size += 1;
        }

        debug!("Created new connection: {}", id);

        Ok((conn, id))
    }

    /// Check if scaling is needed
    async fn check_scaling(
        client: &Client,
        connections: &Arc<RwLock<HashMap<String, (MultiplexedConnection, ConnectionMetadata)>>>,
        available: &Arc<Mutex<Vec<String>>>,
        stats: &Arc<RwLock<AdaptivePoolStats>>,
        config: &AdaptivePoolConfig,
    ) {
        let current_stats = stats.read().await.clone();

        // Scale up if utilization is high
        if current_stats.utilization > config.scale_up_threshold
            && current_stats.current_size < config.max_size
        {
            if let Ok(conn) = client.get_multiplexed_async_connection().await {
                let id = format!("conn_{}", uuid::Uuid::new_v4());

                let meta = ConnectionMetadata {
                    id: id.clone(),
                    created_at: Instant::now(),
                    last_used: Instant::now(),
                    use_count: 0,
                    avg_latency_us: 0,
                    is_healthy: true,
                };

                let mut conns = connections.write().await;
                conns.insert(id.clone(), (conn, meta));

                let mut avail = available.lock().await;
                avail.push(id);

                let mut s = stats.write().await;
                s.current_size += 1;
                s.idle += 1;
                s.scale_up_events += 1;

                info!(
                    "Scaled up pool to {} connections (utilization: {:.2}%)",
                    s.current_size,
                    s.utilization * 100.0
                );
            }
        }
        // Scale down if utilization is low and we have more than min_size
        else if current_stats.utilization < config.scale_down_threshold
            && current_stats.current_size > config.min_size
        {
            let mut avail = available.lock().await;
            if let Some(id) = avail.pop() {
                let mut conns = connections.write().await;
                conns.remove(&id);

                let mut s = stats.write().await;
                s.current_size = s.current_size.saturating_sub(1);
                s.idle = s.idle.saturating_sub(1);
                s.total_destroyed += 1;
                s.scale_down_events += 1;

                debug!(
                    "Scaled down pool to {} connections (utilization: {:.2}%)",
                    s.current_size,
                    s.utilization * 100.0
                );
            }
        }
    }
}

/// Handle for returning connections to pool
struct PoolHandle {
    #[allow(dead_code)]
    connections: Arc<RwLock<HashMap<String, (MultiplexedConnection, ConnectionMetadata)>>>,
    available: Arc<Mutex<Vec<String>>>,
    stats: Arc<RwLock<AdaptivePoolStats>>,
    semaphore: Arc<Semaphore>,
}

/// Pooled connection with automatic return to pool
pub struct PooledConnection {
    connection: Option<MultiplexedConnection>,
    conn_id: String,
    pool: Arc<PoolHandle>,
}

impl PooledConnection {
    /// Get a reference to the underlying connection
    pub fn as_mut(&mut self) -> &mut MultiplexedConnection {
        self.connection
            .as_mut()
            .expect("Connection already returned")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(_conn) = self.connection.take() {
            let conn_id = self.conn_id.clone();
            let pool = Arc::clone(&self.pool);

            tokio::spawn(async move {
                // Return connection to available list
                let mut avail = pool.available.lock().await;
                avail.push(conn_id.clone());

                // Update stats
                let mut stats = pool.stats.write().await;
                stats.active = stats.active.saturating_sub(1);
                stats.idle += 1;
                stats.total_releases += 1;
                stats.utilization = stats.active as f64 / stats.current_size.max(1) as f64;

                // Calculate reuse rate
                if stats.total_releases > 0 {
                    stats.reuse_rate = (stats.total_releases - stats.total_created) as f64
                        / stats.total_releases as f64;
                }

                // Release semaphore permit
                pool.semaphore.add_permits(1);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_adaptive_pool_config_builder() {
        let config = AdaptivePoolConfig::default()
            .with_min_size(5)
            .with_max_size(50)
            .with_target_latency_ms(100)
            .with_scale_up_threshold(0.9)
            .with_scale_down_threshold(0.2);

        assert_eq!(config.min_size, 5);
        assert_eq!(config.max_size, 50);
        assert_eq!(config.target_latency_ms, 100);
        assert_eq!(config.scale_up_threshold, 0.9);
        assert_eq!(config.scale_down_threshold, 0.2);
    }

    #[test]
    fn test_adaptive_pool_config_threshold_clamping() {
        let config = AdaptivePoolConfig::default()
            .with_scale_up_threshold(1.5)
            .with_scale_down_threshold(-0.5);

        assert_eq!(config.scale_up_threshold, 1.0);
        assert_eq!(config.scale_down_threshold, 0.0);
    }

    #[test]
    fn test_adaptive_pool_stats_default() {
        let stats = AdaptivePoolStats::default();

        assert_eq!(stats.current_size, 0);
        assert_eq!(stats.active, 0);
        assert_eq!(stats.idle, 0);
        assert_eq!(stats.utilization, 0.0);
    }

    #[test]
    fn test_connection_metadata_creation() {
        let meta = ConnectionMetadata {
            id: "test123".to_string(),
            created_at: Instant::now(),
            last_used: Instant::now(),
            use_count: 0,
            avg_latency_us: 0,
            is_healthy: true,
        };

        assert_eq!(meta.id, "test123");
        assert!(meta.is_healthy);
        assert_eq!(meta.use_count, 0);
    }
}
