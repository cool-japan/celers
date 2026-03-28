//! Connection and channel pooling, and message deduplication cache.

use crate::types::{ChannelPoolMetrics, ConnectionPoolMetrics};
use celers_kombu::{BrokerError, Result};
use lapin::{Channel, Connection, ConnectionProperties};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::Mutex;
use tracing::debug;

/// Message deduplication cache entry
#[derive(Debug, Clone)]
struct DeduplicationEntry {
    /// When this entry was created
    created_at: Instant,
}

/// Message deduplication cache
pub(crate) struct DeduplicationCache {
    /// Cache of message IDs
    cache: Arc<Mutex<HashMap<String, DeduplicationEntry>>>,
    /// Maximum cache size
    max_size: usize,
    /// TTL for cache entries
    ttl: Duration,
}

impl DeduplicationCache {
    /// Create a new deduplication cache
    pub(crate) fn new(max_size: usize, ttl: Duration) -> Self {
        Self {
            cache: Arc::new(Mutex::new(HashMap::with_capacity(max_size))),
            max_size,
            ttl,
        }
    }

    /// Check if a message ID is duplicate (returns true if duplicate)
    pub(crate) async fn is_duplicate(&self, message_id: &str) -> bool {
        let mut cache = self.cache.lock().await;

        // Clean expired entries
        let now = Instant::now();
        cache.retain(|_, entry| now.duration_since(entry.created_at) < self.ttl);

        // Check if message ID exists
        if cache.contains_key(message_id) {
            debug!("Duplicate message detected: {}", message_id);
            return true;
        }

        // Add message ID to cache
        if cache.len() >= self.max_size {
            // Remove oldest entry (simple FIFO)
            if let Some(key) = cache.keys().next().cloned() {
                cache.remove(&key);
            }
        }

        cache.insert(
            message_id.to_string(),
            DeduplicationEntry { created_at: now },
        );

        false
    }

    /// Clear the cache
    #[allow(dead_code)]
    pub(crate) async fn clear(&self) {
        let mut cache = self.cache.lock().await;
        cache.clear();
    }

    /// Get cache size
    #[allow(dead_code)]
    pub(crate) async fn size(&self) -> usize {
        let cache = self.cache.lock().await;
        cache.len()
    }
}

/// Connection pool for managing multiple AMQP connections
#[derive(Clone)]
pub(crate) struct ConnectionPool {
    connections: Arc<Mutex<VecDeque<Connection>>>,
    #[allow(dead_code)]
    max_size: usize,
    url: String,
    /// Pool metrics
    metrics: Arc<Mutex<ConnectionPoolMetrics>>,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub(crate) fn new(url: String, max_size: usize) -> Self {
        let metrics = ConnectionPoolMetrics {
            max_pool_size: max_size,
            ..Default::default()
        };

        Self {
            connections: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            max_size,
            url,
            metrics: Arc::new(Mutex::new(metrics)),
        }
    }

    /// Get a connection from the pool or create a new one
    #[allow(dead_code)]
    pub(crate) async fn acquire(&self) -> Result<Connection> {
        let mut pool = self.connections.lock().await;
        let mut metrics = self.metrics.lock().await;

        // Try to get an existing connection
        while let Some(conn) = pool.pop_front() {
            if conn.status().connected() {
                metrics.total_acquired += 1;
                metrics.pool_size = pool.len();
                return Ok(conn);
            }
            // Connection is dead, discard it
            metrics.total_discarded += 1;
            debug!("Discarded dead connection from pool");
        }

        // No available connections, create a new one
        metrics.pool_size = pool.len();
        drop(pool); // Release lock before creating connection
        drop(metrics); // Release metrics lock

        let connection = Connection::connect(&self.url, ConnectionProperties::default())
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to connect: {}", e)))?;

        debug!("Created new connection for pool");

        // Update metrics
        let mut metrics = self.metrics.lock().await;
        metrics.total_created += 1;
        metrics.total_acquired += 1;

        Ok(connection)
    }

    /// Return a connection to the pool
    #[allow(dead_code)]
    pub(crate) async fn release(&self, connection: Connection) {
        if !connection.status().connected() {
            debug!("Not returning dead connection to pool");
            let mut metrics = self.metrics.lock().await;
            metrics.total_discarded += 1;
            return;
        }

        let mut pool = self.connections.lock().await;
        let mut metrics = self.metrics.lock().await;

        if pool.len() < self.max_size {
            pool.push_back(connection);
            metrics.total_released += 1;
            metrics.pool_size = pool.len();
            debug!("Returned connection to pool (size: {})", pool.len());
        } else {
            debug!("Pool full, closing excess connection");
            metrics.pool_full_count += 1;
            drop(pool);
            drop(metrics);
            // Pool is full, close the connection
            let _ = connection.close(200, "Pool full".into()).await;
        }
    }

    /// Close all connections in the pool
    pub(crate) async fn close_all(&self) {
        let mut pool = self.connections.lock().await;
        while let Some(conn) = pool.pop_front() {
            let _ = conn.close(200, "Closing pool".into()).await;
        }
    }

    /// Get current pool metrics
    pub(crate) async fn get_metrics(&self) -> ConnectionPoolMetrics {
        let pool = self.connections.lock().await;
        let mut metrics = self.metrics.lock().await;
        metrics.pool_size = pool.len();
        metrics.clone()
    }
}

/// Channel pool for managing multiple AMQP channels per connection
pub(crate) struct ChannelPool {
    channels: Arc<Mutex<VecDeque<Channel>>>,
    #[allow(dead_code)]
    max_size: usize,
    /// Pool metrics
    metrics: Arc<Mutex<ChannelPoolMetrics>>,
}

impl ChannelPool {
    /// Create a new channel pool
    pub(crate) fn new(max_size: usize) -> Self {
        let metrics = ChannelPoolMetrics {
            max_pool_size: max_size,
            ..Default::default()
        };

        Self {
            channels: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            max_size,
            metrics: Arc::new(Mutex::new(metrics)),
        }
    }

    /// Get a channel from the pool or create a new one
    #[allow(dead_code)]
    pub(crate) async fn acquire(&self, connection: &Connection) -> Result<Channel> {
        let mut pool = self.channels.lock().await;
        let mut metrics = self.metrics.lock().await;

        // Try to get an existing channel
        while let Some(ch) = pool.pop_front() {
            if ch.status().connected() {
                metrics.total_acquired += 1;
                metrics.pool_size = pool.len();
                return Ok(ch);
            }
            // Channel is dead, discard it
            metrics.total_discarded += 1;
            debug!("Discarded dead channel from pool");
        }

        // No available channels, create a new one
        metrics.pool_size = pool.len();
        drop(pool); // Release lock before creating channel
        drop(metrics); // Release metrics lock

        let channel = connection
            .create_channel()
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to create channel: {}", e)))?;

        debug!("Created new channel for pool");

        // Update metrics
        let mut metrics = self.metrics.lock().await;
        metrics.total_created += 1;
        metrics.total_acquired += 1;

        Ok(channel)
    }

    /// Return a channel to the pool
    #[allow(dead_code)]
    pub(crate) async fn release(&self, channel: Channel) {
        if !channel.status().connected() {
            debug!("Not returning dead channel to pool");
            let mut metrics = self.metrics.lock().await;
            metrics.total_discarded += 1;
            return;
        }

        let mut pool = self.channels.lock().await;
        let mut metrics = self.metrics.lock().await;

        if pool.len() < self.max_size {
            pool.push_back(channel);
            metrics.total_released += 1;
            metrics.pool_size = pool.len();
            debug!("Returned channel to pool (size: {})", pool.len());
        } else {
            debug!("Channel pool full, closing excess channel");
            metrics.pool_full_count += 1;
            drop(pool);
            drop(metrics);
            // Pool is full, close the channel
            let _ = channel.close(200, "Pool full".into()).await;
        }
    }

    /// Close all channels in the pool
    pub(crate) async fn close_all(&self) {
        let mut pool = self.channels.lock().await;
        while let Some(ch) = pool.pop_front() {
            let _ = ch.close(200, "Closing pool".into()).await;
        }
    }

    /// Get current pool metrics
    pub(crate) async fn get_metrics(&self) -> ChannelPoolMetrics {
        let pool = self.channels.lock().await;
        let mut metrics = self.metrics.lock().await;
        metrics.pool_size = pool.len();
        metrics.clone()
    }
}
