//! Connection and channel pooling, and message deduplication cache.

use crate::connect::open_connection;
use crate::types::{ChannelPoolMetrics, ConnectionPoolMetrics};
use celers_kombu::{BrokerError, Result};
use lapin::{
    options::BasicQosOptions, options::ConfirmSelectOptions, uri::AMQPUri, Channel, Connection,
};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
// `tokio::time::Instant` (not `std::time::Instant`) so that the TTL logic is
// driven by the tokio clock and can be tested deterministically with
// `tokio::time::pause()` / `advance()`.
use tokio::time::Instant;
use tracing::debug;

/// Internal state of the deduplication cache.
///
/// `seen` answers the membership question in O(1); `order` is the insertion
/// log that makes both TTL expiry and capacity eviction genuinely FIFO and
/// amortised O(1) - the previous implementation swept the whole map on every
/// publish and evicted an arbitrary key in `HashMap` iteration order.
struct DeduplicationState {
    seen: HashMap<String, Instant>,
    order: VecDeque<(String, Instant)>,
}

/// Message deduplication cache
pub(crate) struct DeduplicationCache {
    state: Arc<Mutex<DeduplicationState>>,
    /// Maximum cache size (always >= 1)
    max_size: usize,
    /// TTL for cache entries
    ttl: Duration,
}

impl DeduplicationCache {
    /// Create a new deduplication cache
    pub(crate) fn new(max_size: usize, ttl: Duration) -> Self {
        let max_size = max_size.max(1);
        Self {
            state: Arc::new(Mutex::new(DeduplicationState {
                seen: HashMap::with_capacity(max_size),
                order: VecDeque::with_capacity(max_size),
            })),
            max_size,
            ttl,
        }
    }

    /// Check if a message ID is duplicate (returns true if duplicate)
    pub(crate) async fn is_duplicate(&self, message_id: &str) -> bool {
        let mut state = self.state.lock().await;
        let now = Instant::now();

        // Drop entries that have outlived the TTL. They are at the front of
        // the insertion log, so this stops at the first live entry.
        while let Some((key, inserted_at)) = state.order.front() {
            if now.duration_since(*inserted_at) < self.ttl {
                break;
            }
            let (key, inserted_at) = (key.clone(), *inserted_at);
            state.order.pop_front();
            if state.seen.get(&key) == Some(&inserted_at) {
                state.seen.remove(&key);
            }
        }

        if state.seen.contains_key(message_id) {
            debug!("Duplicate message detected: {}", message_id);
            return true;
        }

        // Capacity eviction: genuinely oldest-first.
        while state.seen.len() >= self.max_size {
            let Some((key, inserted_at)) = state.order.pop_front() else {
                break;
            };
            if state.seen.get(&key) == Some(&inserted_at) {
                state.seen.remove(&key);
            }
        }

        state.seen.insert(message_id.to_string(), now);
        state.order.push_back((message_id.to_string(), now));

        false
    }

    /// Clear the cache
    #[allow(dead_code)]
    pub(crate) async fn clear(&self) {
        let mut state = self.state.lock().await;
        state.seen.clear();
        state.order.clear();
    }

    /// Get cache size
    #[allow(dead_code)]
    pub(crate) async fn size(&self) -> usize {
        let state = self.state.lock().await;
        state.seen.len()
    }
}

/// Connection pool for managing multiple AMQP connections
#[derive(Clone)]
pub(crate) struct ConnectionPool {
    connections: Arc<Mutex<VecDeque<Connection>>>,
    max_size: usize,
    /// Fully resolved connection URI (vhost, heartbeat and connection timeout
    /// applied) so pooled connections cannot drift from the broker's own.
    uri: AMQPUri,
    /// Timeout applied to establishing a new pooled connection
    connect_timeout: Duration,
    /// Pool metrics
    metrics: Arc<Mutex<ConnectionPoolMetrics>>,
}

impl ConnectionPool {
    /// Create a new connection pool
    pub(crate) fn new(uri: AMQPUri, connect_timeout: Duration, max_size: usize) -> Self {
        let metrics = ConnectionPoolMetrics {
            max_pool_size: max_size,
            ..Default::default()
        };

        Self {
            connections: Arc::new(Mutex::new(VecDeque::with_capacity(max_size))),
            max_size,
            uri,
            connect_timeout,
            metrics: Arc::new(Mutex::new(metrics)),
        }
    }

    /// Get a connection from the pool or create a new one
    pub(crate) async fn acquire(&self) -> Result<Connection> {
        {
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

            metrics.pool_size = pool.len();
        }

        let connection = open_connection(&self.uri, self.connect_timeout).await?;

        debug!("Created new connection for pool");

        // Update metrics
        let mut metrics = self.metrics.lock().await;
        metrics.total_created += 1;
        metrics.total_acquired += 1;

        Ok(connection)
    }

    /// Return a connection to the pool
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
        let mut metrics = self.metrics.lock().await;
        metrics.pool_size = 0;
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

    /// Get a channel from the pool, or create and configure a new one.
    ///
    /// A freshly created channel gets the same QoS and publisher-confirm
    /// setup as the broker's primary channel, so a pooled channel is never a
    /// silently weaker publishing path. Channels already in the pool were
    /// configured when they were created.
    pub(crate) async fn acquire(
        &self,
        connection: &Connection,
        prefetch_count: u16,
        prefetch_global: bool,
        publisher_confirms: bool,
    ) -> Result<Channel> {
        {
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

            metrics.pool_size = pool.len();
        }

        let channel = connection
            .create_channel()
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to create channel: {}", e)))?;

        configure_channel(
            &channel,
            prefetch_count,
            prefetch_global,
            publisher_confirms,
        )
        .await?;

        debug!("Created new channel for pool");

        // Update metrics
        let mut metrics = self.metrics.lock().await;
        metrics.total_created += 1;
        metrics.total_acquired += 1;

        Ok(channel)
    }

    /// Return a channel to the pool
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
        let mut metrics = self.metrics.lock().await;
        metrics.pool_size = 0;
    }

    /// Get current pool metrics
    pub(crate) async fn get_metrics(&self) -> ChannelPoolMetrics {
        let pool = self.channels.lock().await;
        let mut metrics = self.metrics.lock().await;
        metrics.pool_size = pool.len();
        metrics.clone()
    }
}

/// Apply the QoS and publisher-confirm settings a freshly created channel needs.
///
/// # Errors
///
/// Returns [`BrokerError::Connection`] if the broker rejects `basic.qos` or
/// `confirm.select`.
pub(crate) async fn configure_channel(
    channel: &Channel,
    prefetch_count: u16,
    prefetch_global: bool,
    publisher_confirms: bool,
) -> Result<()> {
    if prefetch_count > 0 {
        channel
            .basic_qos(
                prefetch_count,
                BasicQosOptions {
                    global: prefetch_global,
                },
            )
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to set QoS: {}", e)))?;
    }

    if publisher_confirms {
        channel
            .confirm_select(ConfirmSelectOptions::default())
            .await
            .map_err(|e| {
                BrokerError::Connection(format!("Failed to enable publisher confirms: {}", e))
            })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn duplicate_is_detected_within_ttl() {
        let cache = DeduplicationCache::new(16, Duration::from_secs(60));
        assert!(!cache.is_duplicate("a").await);
        assert!(cache.is_duplicate("a").await);
        assert!(!cache.is_duplicate("b").await);
        assert_eq!(cache.size().await, 2);
    }

    #[tokio::test(start_paused = true)]
    async fn entries_expire_after_ttl() {
        let cache = DeduplicationCache::new(16, Duration::from_secs(10));
        assert!(!cache.is_duplicate("a").await);

        tokio::time::advance(Duration::from_secs(11)).await;

        // Expired, so it is not a duplicate any more and the stale entry is gone.
        assert!(!cache.is_duplicate("a").await);
        assert_eq!(cache.size().await, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn eviction_is_fifo_and_never_drops_the_newest_entry() {
        let cache = DeduplicationCache::new(3, Duration::from_secs(3600));

        for id in ["m1", "m2", "m3"] {
            assert!(!cache.is_duplicate(id).await);
            tokio::time::advance(Duration::from_millis(1)).await;
        }

        // Inserting a fourth entry must evict the OLDEST ("m1"), never the
        // most recent one and never the entry being inserted.
        assert!(!cache.is_duplicate("m4").await);
        assert_eq!(cache.size().await, 3);

        assert!(cache.is_duplicate("m4").await, "newest entry was evicted");
        assert!(cache.is_duplicate("m3").await);
        assert!(cache.is_duplicate("m2").await);
        assert!(
            !cache.is_duplicate("m1").await,
            "oldest entry should have been evicted"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn zero_capacity_does_not_spin() {
        let cache = DeduplicationCache::new(0, Duration::from_secs(60));
        assert!(!cache.is_duplicate("a").await);
        assert!(cache.is_duplicate("a").await);
        // Capacity is clamped to 1, so the next id evicts the previous one.
        assert!(!cache.is_duplicate("b").await);
        assert!(!cache.is_duplicate("a").await);
    }

    #[tokio::test(start_paused = true)]
    async fn clear_empties_both_index_and_log() {
        let cache = DeduplicationCache::new(4, Duration::from_secs(60));
        assert!(!cache.is_duplicate("a").await);
        cache.clear().await;
        assert_eq!(cache.size().await, 0);
        assert!(!cache.is_duplicate("a").await);
    }
}
