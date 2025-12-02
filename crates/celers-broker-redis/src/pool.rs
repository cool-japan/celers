//! Connection pooling for Redis broker
//!
//! Provides efficient connection management with:
//! - Configurable pool size
//! - Connection reuse
//! - Health monitoring
//! - Automatic cleanup

use celers_core::{CelersError, Result};
use redis::{aio::MultiplexedConnection, Client};
use std::sync::Arc;
use tokio::sync::{Mutex, Semaphore};
use tracing::{debug, warn};

/// Connection pool configuration
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum number of connections to maintain
    pub min_idle: usize,
    /// Maximum number of connections allowed
    pub max_size: usize,
    /// Connection timeout in seconds
    pub connection_timeout_secs: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_idle: 2,
            max_size: 10,
            connection_timeout_secs: 5,
        }
    }
}

impl PoolConfig {
    /// Create a new pool configuration
    pub fn new() -> Self {
        Self::default()
    }

    /// Set minimum idle connections
    pub fn with_min_idle(mut self, min_idle: usize) -> Self {
        self.min_idle = min_idle;
        self
    }

    /// Set maximum pool size
    pub fn with_max_size(mut self, max_size: usize) -> Self {
        self.max_size = max_size;
        self
    }

    /// Set connection timeout
    pub fn with_connection_timeout(mut self, timeout_secs: u64) -> Self {
        self.connection_timeout_secs = timeout_secs;
        self
    }
}

/// Redis connection pool
pub struct ConnectionPool {
    client: Client,
    config: PoolConfig,
    semaphore: Arc<Semaphore>,
    connections: Arc<Mutex<Vec<MultiplexedConnection>>>,
    stats: Arc<Mutex<PoolStats>>,
}

/// Connection pool statistics
#[derive(Debug, Clone, Default)]
pub struct PoolStats {
    /// Total connections created
    pub total_created: u64,
    /// Total connections destroyed
    pub total_destroyed: u64,
    /// Current active connections
    pub active: usize,
    /// Current idle connections
    pub idle: usize,
    /// Total connection errors
    pub connection_errors: u64,
    /// Total wait time in milliseconds
    pub total_wait_time_ms: u64,
    /// Total connection acquisitions
    pub total_acquisitions: u64,
    /// Peak active connections
    pub peak_active: usize,
    /// Peak idle connections
    pub peak_idle: usize,
    /// Total connection reuses
    pub total_reuses: u64,
    /// Last error timestamp (Unix timestamp in milliseconds)
    pub last_error_timestamp: Option<u64>,
}

impl PoolStats {
    /// Get average wait time in milliseconds
    pub fn avg_wait_time_ms(&self) -> f64 {
        if self.total_acquisitions == 0 {
            0.0
        } else {
            self.total_wait_time_ms as f64 / self.total_acquisitions as f64
        }
    }

    /// Get connection reuse rate (0.0 to 1.0)
    pub fn reuse_rate(&self) -> f64 {
        if self.total_acquisitions == 0 {
            0.0
        } else {
            self.total_reuses as f64 / self.total_acquisitions as f64
        }
    }

    /// Get total connections (active + idle)
    pub fn total_connections(&self) -> usize {
        self.active + self.idle
    }

    /// Check if pool is healthy
    pub fn is_healthy(&self) -> bool {
        self.connection_errors == 0 || self.total_acquisitions > self.connection_errors * 10
    }
}

impl ConnectionPool {
    /// Create a new connection pool
    pub async fn new(client: Client, config: PoolConfig) -> Result<Self> {
        let pool = Self {
            client: client.clone(),
            config: config.clone(),
            semaphore: Arc::new(Semaphore::new(config.max_size)),
            connections: Arc::new(Mutex::new(Vec::new())),
            stats: Arc::new(Mutex::new(PoolStats::default())),
        };

        // Pre-populate with minimum idle connections
        pool.ensure_min_idle().await?;

        Ok(pool)
    }

    /// Get a connection from the pool
    pub async fn get(&self) -> Result<PooledConnection> {
        let start = std::time::Instant::now();

        // Acquire semaphore permit
        let permit = self.semaphore.clone().acquire_owned().await.map_err(|e| {
            CelersError::Broker(format!("Failed to acquire connection permit: {}", e))
        })?;

        let elapsed = start.elapsed().as_millis() as u64;

        // Try to get an existing connection
        let mut conns = self.connections.lock().await;
        let conn = if let Some(conn) = conns.pop() {
            debug!("Reusing existing connection from pool");
            // Track reuse
            let mut stats = self.stats.lock().await;
            stats.total_reuses += 1;
            drop(stats);
            conn
        } else {
            // Create a new connection
            drop(conns); // Release lock before creating connection
            match self.create_connection().await {
                Ok(conn) => {
                    let mut stats = self.stats.lock().await;
                    stats.total_created += 1;
                    stats.active += 1;
                    debug!("Created new connection (total: {})", stats.total_created);
                    conn
                }
                Err(e) => {
                    let mut stats = self.stats.lock().await;
                    stats.connection_errors += 1;
                    // Record error timestamp
                    if let Ok(now) =
                        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)
                    {
                        stats.last_error_timestamp = Some(now.as_millis() as u64);
                    }
                    drop(permit); // Release semaphore on error
                    return Err(e);
                }
            }
        };

        // Update stats
        let mut stats = self.stats.lock().await;
        stats.total_wait_time_ms += elapsed;
        stats.total_acquisitions += 1;
        if stats.idle > 0 {
            stats.idle -= 1;
        }
        stats.active += 1;

        // Update peak active
        if stats.active > stats.peak_active {
            stats.peak_active = stats.active;
        }

        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.connections.clone(),
            stats: self.stats.clone(),
            _permit: permit,
        })
    }

    /// Create a new Redis connection
    async fn create_connection(&self) -> Result<MultiplexedConnection> {
        let timeout = tokio::time::Duration::from_secs(self.config.connection_timeout_secs);

        tokio::time::timeout(timeout, self.client.get_multiplexed_async_connection())
            .await
            .map_err(|_| CelersError::Broker("Connection timeout".to_string()))?
            .map_err(|e| CelersError::Broker(format!("Failed to create connection: {}", e)))
    }

    /// Ensure minimum idle connections are maintained
    async fn ensure_min_idle(&self) -> Result<()> {
        let mut conns = self.connections.lock().await;
        let current_idle = conns.len();

        if current_idle < self.config.min_idle {
            let needed = self.config.min_idle - current_idle;
            for _ in 0..needed {
                match self.create_connection().await {
                    Ok(conn) => {
                        conns.push(conn);
                        let mut stats = self.stats.lock().await;
                        stats.total_created += 1;
                        stats.idle += 1;
                    }
                    Err(e) => {
                        warn!("Failed to create idle connection: {}", e);
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Get pool statistics
    pub async fn stats(&self) -> PoolStats {
        self.stats.lock().await.clone()
    }

    /// Get pool configuration
    pub fn config(&self) -> &PoolConfig {
        &self.config
    }
}

/// A connection retrieved from the pool
pub struct PooledConnection {
    conn: Option<MultiplexedConnection>,
    pool: Arc<Mutex<Vec<MultiplexedConnection>>>,
    stats: Arc<Mutex<PoolStats>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl PooledConnection {
    /// Get a mutable reference to the connection
    pub fn get_mut(&mut self) -> &mut MultiplexedConnection {
        self.conn.as_mut().expect("Connection already returned")
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let pool = self.pool.clone();
            let stats = self.stats.clone();

            // Return connection to pool in a background task
            tokio::spawn(async move {
                let mut conns = pool.lock().await;
                conns.push(conn);

                let mut s = stats.lock().await;
                if s.active > 0 {
                    s.active -= 1;
                }
                s.idle += 1;

                // Update peak idle
                if s.idle > s.peak_idle {
                    s.peak_idle = s.idle;
                }

                debug!("Returned connection to pool (idle: {})", s.idle);
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_config_default() {
        let config = PoolConfig::default();
        assert_eq!(config.min_idle, 2);
        assert_eq!(config.max_size, 10);
        assert_eq!(config.connection_timeout_secs, 5);
    }

    #[test]
    fn test_pool_config_builder() {
        let config = PoolConfig::new()
            .with_min_idle(5)
            .with_max_size(20)
            .with_connection_timeout(10);

        assert_eq!(config.min_idle, 5);
        assert_eq!(config.max_size, 20);
        assert_eq!(config.connection_timeout_secs, 10);
    }

    #[test]
    fn test_pool_stats_default() {
        let stats = PoolStats::default();
        assert_eq!(stats.total_created, 0);
        assert_eq!(stats.total_destroyed, 0);
        assert_eq!(stats.active, 0);
        assert_eq!(stats.idle, 0);
        assert_eq!(stats.connection_errors, 0);
        assert_eq!(stats.total_wait_time_ms, 0);
        assert_eq!(stats.total_acquisitions, 0);
        assert_eq!(stats.peak_active, 0);
        assert_eq!(stats.peak_idle, 0);
        assert_eq!(stats.total_reuses, 0);
        assert_eq!(stats.last_error_timestamp, None);
    }

    #[test]
    fn test_pool_stats_avg_wait_time() {
        let mut stats = PoolStats::default();
        assert_eq!(stats.avg_wait_time_ms(), 0.0);

        stats.total_acquisitions = 10;
        stats.total_wait_time_ms = 1000;
        assert_eq!(stats.avg_wait_time_ms(), 100.0);
    }

    #[test]
    fn test_pool_stats_reuse_rate() {
        let mut stats = PoolStats::default();
        assert_eq!(stats.reuse_rate(), 0.0);

        stats.total_acquisitions = 10;
        stats.total_reuses = 8;
        assert_eq!(stats.reuse_rate(), 0.8);
    }

    #[test]
    fn test_pool_stats_total_connections() {
        let mut stats = PoolStats::default();
        stats.active = 5;
        stats.idle = 3;
        assert_eq!(stats.total_connections(), 8);
    }

    #[test]
    fn test_pool_stats_is_healthy() {
        let mut stats = PoolStats::default();
        assert!(stats.is_healthy());

        // Few errors relative to acquisitions
        stats.total_acquisitions = 100;
        stats.connection_errors = 5;
        assert!(stats.is_healthy());

        // Too many errors
        stats.connection_errors = 50;
        assert!(!stats.is_healthy());
    }
}
