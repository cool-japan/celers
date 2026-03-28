//! Connection state, pool, and observer types.

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::time::Duration;

use crate::Result;

// =============================================================================
// Connection State Callbacks
// =============================================================================

/// Connection state
///
/// # Examples
///
/// ```
/// use celers_kombu::ConnectionState;
///
/// let state = ConnectionState::Connected;
/// assert_eq!(state.to_string(), "connected");
///
/// let disconnected = ConnectionState::Disconnected;
/// assert_eq!(disconnected.to_string(), "disconnected");
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected
    Disconnected,
    /// Connection in progress
    Connecting,
    /// Connected and ready
    Connected,
    /// Reconnecting after failure
    Reconnecting,
}

impl std::fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionState::Disconnected => write!(f, "disconnected"),
            ConnectionState::Connecting => write!(f, "connecting"),
            ConnectionState::Connected => write!(f, "connected"),
            ConnectionState::Reconnecting => write!(f, "reconnecting"),
        }
    }
}

/// Connection event
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    /// Connection established
    Connected,
    /// Connection lost
    Disconnected { reason: String },
    /// Reconnection attempt
    Reconnecting { attempt: u32 },
    /// Reconnection succeeded
    Reconnected,
    /// Reconnection failed
    ReconnectFailed { error: String },
}

/// Connection state observer trait
pub trait ConnectionObserver: Send + Sync {
    /// Called when connection state changes
    fn on_state_change(&self, old_state: ConnectionState, new_state: ConnectionState);

    /// Called on connection events
    fn on_event(&self, event: ConnectionEvent);
}

// =============================================================================
// Connection Pool
// =============================================================================

/// Connection pool configuration
///
/// # Examples
///
/// ```
/// use celers_kombu::PoolConfig;
/// use std::time::Duration;
///
/// let config = PoolConfig::new()
///     .with_min_connections(2)
///     .with_max_connections(20)
///     .with_idle_timeout(Duration::from_secs(300))
///     .with_acquire_timeout(Duration::from_secs(10))
///     .with_max_lifetime(Duration::from_secs(3600));
///
/// assert_eq!(config.min_connections, 2);
/// assert_eq!(config.max_connections, 20);
/// assert_eq!(config.idle_timeout, Some(Duration::from_secs(300)));
/// assert_eq!(config.acquire_timeout, Duration::from_secs(10));
/// ```
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Minimum number of connections to keep in the pool
    pub min_connections: u32,
    /// Maximum number of connections allowed
    pub max_connections: u32,
    /// Connection idle timeout before closing
    pub idle_timeout: Option<Duration>,
    /// Maximum time to wait for a connection
    pub acquire_timeout: Duration,
    /// Maximum connection lifetime
    pub max_lifetime: Option<Duration>,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            min_connections: 1,
            max_connections: 10,
            idle_timeout: Some(Duration::from_secs(300)),
            acquire_timeout: Duration::from_secs(30),
            max_lifetime: Some(Duration::from_secs(1800)),
        }
    }
}

impl PoolConfig {
    /// Create a new pool config
    pub fn new() -> Self {
        Self::default()
    }

    /// Set minimum connections
    pub fn with_min_connections(mut self, min: u32) -> Self {
        self.min_connections = min;
        self
    }

    /// Set maximum connections
    pub fn with_max_connections(mut self, max: u32) -> Self {
        self.max_connections = max;
        self
    }

    /// Set idle timeout
    pub fn with_idle_timeout(mut self, timeout: Duration) -> Self {
        self.idle_timeout = Some(timeout);
        self
    }

    /// Disable idle timeout
    pub fn without_idle_timeout(mut self) -> Self {
        self.idle_timeout = None;
        self
    }

    /// Set acquire timeout
    pub fn with_acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Set max lifetime
    pub fn with_max_lifetime(mut self, lifetime: Duration) -> Self {
        self.max_lifetime = Some(lifetime);
        self
    }
}

/// Pool statistics
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PoolStats {
    /// Total connections created
    pub connections_created: u64,
    /// Total connections closed
    pub connections_closed: u64,
    /// Current active connections
    pub active_connections: u32,
    /// Current idle connections
    pub idle_connections: u32,
    /// Total acquire requests
    pub acquire_requests: u64,
    /// Acquire timeouts
    pub acquire_timeouts: u64,
}

impl PoolStats {
    /// Get total connections in pool
    pub fn total_connections(&self) -> u32 {
        self.active_connections + self.idle_connections
    }
}

/// Connection pool trait
#[async_trait]
pub trait ConnectionPool: Send + Sync {
    /// Get pool configuration
    fn config(&self) -> &PoolConfig;

    /// Get pool statistics
    fn stats(&self) -> PoolStats;

    /// Resize the pool
    async fn resize(&mut self, min: u32, max: u32) -> Result<()>;

    /// Close all idle connections
    async fn shrink(&mut self) -> Result<u32>;

    /// Close all connections
    async fn close(&mut self) -> Result<()>;
}
