//! Redis Sentinel support for high availability
//!
//! Provides automatic master discovery and failover detection through Redis Sentinel.

use crate::{CelersError, Result};
use redis::Client;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use tracing::{debug, error, info, warn};

/// Sentinel configuration
#[derive(Debug, Clone)]
pub struct SentinelConfig {
    /// List of sentinel addresses (host:port)
    pub sentinels: Vec<String>,

    /// Master group name
    pub master_name: String,

    /// Sentinel authentication password (optional)
    pub sentinel_password: Option<String>,

    /// Redis authentication password (optional)
    pub redis_password: Option<String>,

    /// Redis database number
    pub db: i64,

    /// Connection timeout
    pub connect_timeout: Duration,

    /// Failover detection interval
    pub check_interval: Duration,

    /// Number of connection retry attempts
    pub max_retries: u32,
}

impl Default for SentinelConfig {
    fn default() -> Self {
        Self {
            sentinels: vec!["localhost:26379".to_string()],
            master_name: "mymaster".to_string(),
            sentinel_password: None,
            redis_password: None,
            db: 0,
            connect_timeout: Duration::from_secs(5),
            check_interval: Duration::from_secs(10),
            max_retries: 3,
        }
    }
}

impl SentinelConfig {
    /// Create a new builder for SentinelConfig
    pub fn builder() -> SentinelConfigBuilder {
        SentinelConfigBuilder::default()
    }
}

/// Builder for SentinelConfig
#[derive(Default)]
pub struct SentinelConfigBuilder {
    sentinels: Vec<String>,
    master_name: Option<String>,
    sentinel_password: Option<String>,
    redis_password: Option<String>,
    db: Option<i64>,
    connect_timeout: Option<Duration>,
    check_interval: Option<Duration>,
    max_retries: Option<u32>,
}

impl SentinelConfigBuilder {
    /// Add a sentinel address
    pub fn sentinel(mut self, address: impl Into<String>) -> Self {
        self.sentinels.push(address.into());
        self
    }

    /// Set multiple sentinel addresses
    pub fn sentinels(mut self, addresses: Vec<String>) -> Self {
        self.sentinels = addresses;
        self
    }

    /// Set master name
    pub fn master_name(mut self, name: impl Into<String>) -> Self {
        self.master_name = Some(name.into());
        self
    }

    /// Set sentinel password
    pub fn sentinel_password(mut self, password: impl Into<String>) -> Self {
        self.sentinel_password = Some(password.into());
        self
    }

    /// Set Redis password
    pub fn redis_password(mut self, password: impl Into<String>) -> Self {
        self.redis_password = Some(password.into());
        self
    }

    /// Set database number
    pub fn db(mut self, db: i64) -> Self {
        self.db = Some(db);
        self
    }

    /// Set connection timeout
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = Some(timeout);
        self
    }

    /// Set failover check interval
    pub fn check_interval(mut self, interval: Duration) -> Self {
        self.check_interval = Some(interval);
        self
    }

    /// Set maximum retry attempts
    pub fn max_retries(mut self, retries: u32) -> Self {
        self.max_retries = Some(retries);
        self
    }

    /// Build the SentinelConfig
    pub fn build(self) -> Result<SentinelConfig> {
        if self.sentinels.is_empty() {
            return Err(CelersError::Configuration(
                "At least one sentinel address is required".to_string(),
            ));
        }

        let master_name = self
            .master_name
            .ok_or_else(|| CelersError::Configuration("Master name is required".to_string()))?;

        Ok(SentinelConfig {
            sentinels: self.sentinels,
            master_name,
            sentinel_password: self.sentinel_password,
            redis_password: self.redis_password,
            db: self.db.unwrap_or(0),
            connect_timeout: self.connect_timeout.unwrap_or(Duration::from_secs(5)),
            check_interval: self.check_interval.unwrap_or(Duration::from_secs(10)),
            max_retries: self.max_retries.unwrap_or(3),
        })
    }
}

/// Master address information
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MasterAddress {
    pub host: String,
    pub port: u16,
}

impl std::fmt::Display for MasterAddress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.host, self.port)
    }
}

/// Sentinel role information
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SentinelRole {
    Master,
    Slave,
    Sentinel,
    Unknown,
}

/// Sentinel client for master discovery and failover detection
pub struct SentinelClient {
    config: SentinelConfig,
    current_master: Arc<RwLock<Option<MasterAddress>>>,
    redis_client: Arc<RwLock<Option<Client>>>,
}

impl SentinelClient {
    /// Create a new Sentinel client
    pub fn new(config: SentinelConfig) -> Self {
        Self {
            config,
            current_master: Arc::new(RwLock::new(None)),
            redis_client: Arc::new(RwLock::new(None)),
        }
    }

    /// Discover the current master from sentinels
    pub async fn discover_master(&self) -> Result<MasterAddress> {
        for sentinel_addr in &self.config.sentinels {
            debug!("Querying sentinel at {} for master", sentinel_addr);

            match self.query_sentinel(sentinel_addr).await {
                Ok(master) => {
                    info!(
                        "Discovered master at {} from sentinel {}",
                        master, sentinel_addr
                    );

                    // Update current master
                    let mut current = self.current_master.write().await;
                    *current = Some(master.clone());

                    return Ok(master);
                }
                Err(e) => {
                    warn!("Failed to query sentinel {}: {}", sentinel_addr, e);
                    continue;
                }
            }
        }

        Err(CelersError::Broker(
            "Failed to discover master from any sentinel".to_string(),
        ))
    }

    /// Query a specific sentinel for master address
    async fn query_sentinel(&self, sentinel_addr: &str) -> Result<MasterAddress> {
        // Build sentinel connection URL
        let sentinel_url = if let Some(ref password) = self.config.sentinel_password {
            format!("redis://:{}@{}/0", password, sentinel_addr)
        } else {
            format!("redis://{}/0", sentinel_addr)
        };

        let client = Client::open(sentinel_url.as_str())
            .map_err(|e| CelersError::Broker(format!("Failed to create sentinel client: {}", e)))?;

        let mut conn = client
            .get_multiplexed_async_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to connect to sentinel: {}", e)))?;

        // Execute SENTINEL get-master-addr-by-name command
        let result: Vec<String> = redis::cmd("SENTINEL")
            .arg("get-master-addr-by-name")
            .arg(&self.config.master_name)
            .query_async(&mut conn)
            .await
            .map_err(|e| {
                CelersError::Broker(format!("Failed to get master address from sentinel: {}", e))
            })?;

        if result.len() != 2 {
            return Err(CelersError::Broker(
                "Invalid master address response from sentinel".to_string(),
            ));
        }

        let host = result[0].clone();
        let port = result[1].parse::<u16>().map_err(|e| {
            CelersError::Broker(format!("Invalid port number from sentinel: {}", e))
        })?;

        Ok(MasterAddress { host, port })
    }

    /// Get or create a Redis client connected to the current master
    pub async fn get_client(&self) -> Result<Client> {
        // Check if we have a cached client
        {
            let client_lock = self.redis_client.read().await;
            if let Some(ref client) = *client_lock {
                // Verify connection is still valid
                if self.verify_client(client).await {
                    return Ok(client.clone());
                }
            }
        }

        // Need to rediscover master and create new client
        let master = self.discover_master().await?;
        let client = self.create_client(&master)?;

        // Cache the client
        let mut client_lock = self.redis_client.write().await;
        *client_lock = Some(client.clone());

        Ok(client)
    }

    /// Create a Redis client for the master
    fn create_client(&self, master: &MasterAddress) -> Result<Client> {
        let url = if let Some(ref password) = self.config.redis_password {
            format!(
                "redis://:{}@{}:{}/{}",
                password, master.host, master.port, self.config.db
            )
        } else {
            format!("redis://{}:{}/{}", master.host, master.port, self.config.db)
        };

        Client::open(url.as_str())
            .map_err(|e| CelersError::Broker(format!("Failed to create Redis client: {}", e)))
    }

    /// Verify that a client is still connected to the current master
    async fn verify_client(&self, client: &Client) -> bool {
        match client.get_multiplexed_async_connection().await {
            Ok(mut conn) => {
                // Try a simple PING command
                matches!(redis::cmd("PING").query_async::<String>(&mut conn).await, Ok(response) if response == "PONG")
            }
            Err(_) => false,
        }
    }

    /// Get the current master address (if known)
    pub async fn current_master(&self) -> Option<MasterAddress> {
        let current = self.current_master.read().await;
        current.clone()
    }

    /// Check for failover (master change)
    pub async fn check_failover(&self) -> Result<bool> {
        let new_master = self.discover_master().await?;
        let current = self.current_master.read().await;

        match &*current {
            Some(old_master) if old_master != &new_master => {
                info!("Detected failover: {} -> {}", old_master, new_master);
                Ok(true)
            }
            None => {
                // First discovery
                Ok(false)
            }
            _ => Ok(false),
        }
    }

    /// Start monitoring for failovers (runs in background)
    pub fn start_monitoring(&self) -> tokio::task::JoinHandle<()> {
        let client = self.clone();

        tokio::spawn(async move {
            let mut interval = tokio::time::interval(client.config.check_interval);

            loop {
                interval.tick().await;

                match client.check_failover().await {
                    Ok(true) => {
                        info!("Failover detected, invalidating cached client");
                        let mut client_lock = client.redis_client.write().await;
                        *client_lock = None;
                    }
                    Ok(false) => {
                        debug!("No failover detected");
                    }
                    Err(e) => {
                        error!("Failed to check for failover: {}", e);
                    }
                }
            }
        })
    }

    /// Get sentinel configuration
    pub fn config(&self) -> &SentinelConfig {
        &self.config
    }
}

impl Clone for SentinelClient {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            current_master: Arc::clone(&self.current_master),
            redis_client: Arc::clone(&self.redis_client),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sentinel_config_default() {
        let config = SentinelConfig::default();

        assert_eq!(config.sentinels, vec!["localhost:26379"]);
        assert_eq!(config.master_name, "mymaster");
        assert_eq!(config.db, 0);
        assert_eq!(config.connect_timeout, Duration::from_secs(5));
        assert_eq!(config.check_interval, Duration::from_secs(10));
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_sentinel_config_builder() {
        let config = SentinelConfig::builder()
            .sentinel("sentinel1:26379")
            .sentinel("sentinel2:26379")
            .master_name("redis-master")
            .redis_password("secret123")
            .db(1)
            .connect_timeout(Duration::from_secs(10))
            .check_interval(Duration::from_secs(30))
            .max_retries(5)
            .build()
            .unwrap();

        assert_eq!(config.sentinels.len(), 2);
        assert_eq!(config.sentinels[0], "sentinel1:26379");
        assert_eq!(config.sentinels[1], "sentinel2:26379");
        assert_eq!(config.master_name, "redis-master");
        assert_eq!(config.redis_password, Some("secret123".to_string()));
        assert_eq!(config.db, 1);
        assert_eq!(config.connect_timeout, Duration::from_secs(10));
        assert_eq!(config.check_interval, Duration::from_secs(30));
        assert_eq!(config.max_retries, 5);
    }

    #[test]
    fn test_sentinel_config_builder_no_sentinels() {
        let result = SentinelConfig::builder().master_name("mymaster").build();

        assert!(result.is_err());
    }

    #[test]
    fn test_sentinel_config_builder_no_master_name() {
        let result = SentinelConfig::builder()
            .sentinel("localhost:26379")
            .build();

        assert!(result.is_err());
    }

    #[test]
    fn test_master_address_display() {
        let addr = MasterAddress {
            host: "localhost".to_string(),
            port: 6379,
        };

        assert_eq!(addr.to_string(), "localhost:6379");
    }

    #[test]
    fn test_master_address_equality() {
        let addr1 = MasterAddress {
            host: "localhost".to_string(),
            port: 6379,
        };

        let addr2 = MasterAddress {
            host: "localhost".to_string(),
            port: 6379,
        };

        let addr3 = MasterAddress {
            host: "127.0.0.1".to_string(),
            port: 6379,
        };

        assert_eq!(addr1, addr2);
        assert_ne!(addr1, addr3);
    }

    #[test]
    fn test_sentinel_client_creation() {
        let config = SentinelConfig::default();
        let client = SentinelClient::new(config.clone());

        assert_eq!(client.config().master_name, config.master_name);
    }

    #[test]
    fn test_sentinel_client_clone() {
        let config = SentinelConfig::default();
        let client1 = SentinelClient::new(config);
        let client2 = client1.clone();

        assert_eq!(client1.config().master_name, client2.config().master_name);
    }

    #[test]
    fn test_sentinels_builder_method() {
        let sentinels = vec![
            "sentinel1:26379".to_string(),
            "sentinel2:26379".to_string(),
            "sentinel3:26379".to_string(),
        ];

        let config = SentinelConfig::builder()
            .sentinels(sentinels.clone())
            .master_name("mymaster")
            .build()
            .unwrap();

        assert_eq!(config.sentinels, sentinels);
    }
}
