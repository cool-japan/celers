//! Redis connection configuration and management
//!
//! Provides flexible connection configuration including:
//! - Basic Redis URL connections
//! - TLS/SSL support
//! - Connection timeouts
//! - Connection pooling settings
//! - Authentication options

use celers_core::{CelersError, Result};
use redis::Client;
use std::time::Duration;

/// TLS/SSL configuration for Redis connections
#[derive(Debug, Clone, Default)]
pub struct TlsConfig {
    /// Enable TLS/SSL
    pub enabled: bool,
    /// Skip certificate verification (insecure, for testing only)
    pub insecure: bool,
    /// Path to CA certificate file
    pub ca_cert_path: Option<String>,
    /// Path to client certificate file
    pub client_cert_path: Option<String>,
    /// Path to client key file
    pub client_key_path: Option<String>,
}

impl TlsConfig {
    /// Create a new TLS configuration with secure defaults
    pub fn new() -> Self {
        Self::default()
    }

    /// Enable TLS
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Skip certificate verification (insecure, for testing only)
    pub fn insecure(mut self, insecure: bool) -> Self {
        self.insecure = insecure;
        self
    }

    /// Set CA certificate path
    pub fn ca_cert(mut self, path: impl Into<String>) -> Self {
        self.ca_cert_path = Some(path.into());
        self
    }

    /// Set client certificate and key paths
    pub fn client_cert(
        mut self,
        cert_path: impl Into<String>,
        key_path: impl Into<String>,
    ) -> Self {
        self.client_cert_path = Some(cert_path.into());
        self.client_key_path = Some(key_path.into());
        self
    }
}

/// Redis connection configuration
#[derive(Debug, Clone)]
pub struct RedisConfig {
    /// Redis connection URL (e.g., "redis://localhost:6379")
    pub url: String,
    /// TLS/SSL configuration
    pub tls: TlsConfig,
    /// Connection timeout in seconds
    pub connection_timeout: Option<Duration>,
    /// Response timeout in seconds
    pub response_timeout: Option<Duration>,
    /// Database number (0-15)
    pub database: Option<i64>,
    /// Username for authentication (Redis 6+)
    pub username: Option<String>,
    /// Password for authentication
    pub password: Option<String>,
    /// Maximum number of retry attempts
    pub max_retry_attempts: usize,
    /// Retry delay
    pub retry_delay: Duration,
}

impl Default for RedisConfig {
    fn default() -> Self {
        Self {
            url: "redis://localhost:6379".to_string(),
            tls: TlsConfig::default(),
            connection_timeout: Some(Duration::from_secs(5)),
            response_timeout: Some(Duration::from_secs(3)),
            database: Some(0),
            username: None,
            password: None,
            max_retry_attempts: 3,
            retry_delay: Duration::from_millis(100),
        }
    }
}

impl RedisConfig {
    /// Create a new Redis configuration with default values
    pub fn new() -> Self {
        Self::default()
    }

    /// Create from a Redis URL
    pub fn from_url(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            ..Default::default()
        }
    }

    /// Set the Redis URL
    pub fn url(mut self, url: impl Into<String>) -> Self {
        self.url = url.into();
        self
    }

    /// Set TLS configuration
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// Set connection timeout
    pub fn connection_timeout(mut self, timeout: Duration) -> Self {
        self.connection_timeout = Some(timeout);
        self
    }

    /// Set response timeout
    pub fn response_timeout(mut self, timeout: Duration) -> Self {
        self.response_timeout = Some(timeout);
        self
    }

    /// Set database number
    pub fn database(mut self, db: i64) -> Self {
        self.database = Some(db);
        self
    }

    /// Set username for authentication (Redis 6+)
    pub fn username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    /// Set password for authentication
    pub fn password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    /// Set retry configuration
    pub fn retry(mut self, max_attempts: usize, delay: Duration) -> Self {
        self.max_retry_attempts = max_attempts;
        self.retry_delay = delay;
        self
    }

    /// Build a Redis client from this configuration
    pub fn build_client(&self) -> Result<Client> {
        // For now, just use the URL directly as the redis crate handles parsing
        // In a full implementation, we would build ConnectionInfo manually to support TLS
        let client = Client::open(self.url.as_str())
            .map_err(|e| CelersError::Broker(format!("Failed to create Redis client: {}", e)))?;

        Ok(client)
    }

    /// Get a descriptive string for this configuration (without sensitive data)
    pub fn describe(&self) -> String {
        format!(
            "Redis[url={}, tls={}, db={:?}, timeout={:?}]",
            self.sanitized_url(),
            self.tls.enabled,
            self.database,
            self.connection_timeout
        )
    }

    /// Get a sanitized URL (without password)
    fn sanitized_url(&self) -> String {
        let mut url = self.url.clone();
        if let Some(idx) = url.find('@') {
            if let Some(protocol_end) = url.find("://") {
                let protocol = &url[..protocol_end + 3];
                let host_part = &url[idx + 1..];
                url = format!("{}***@{}", protocol, host_part);
            }
        }
        url
    }
}

/// Connection statistics
#[derive(Debug, Clone, Default)]
pub struct ConnectionStats {
    /// Total number of connection attempts
    pub connection_attempts: u64,
    /// Number of successful connections
    pub successful_connections: u64,
    /// Number of failed connections
    pub failed_connections: u64,
    /// Last connection error message
    pub last_error: Option<String>,
}

impl ConnectionStats {
    /// Get connection success rate
    pub fn success_rate(&self) -> f64 {
        if self.connection_attempts == 0 {
            0.0
        } else {
            self.successful_connections as f64 / self.connection_attempts as f64
        }
    }

    /// Check if connections are healthy
    pub fn is_healthy(&self, threshold: f64) -> bool {
        self.success_rate() >= threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tls_config_default() {
        let tls = TlsConfig::default();
        assert!(!tls.enabled);
        assert!(!tls.insecure);
        assert!(tls.ca_cert_path.is_none());
    }

    #[test]
    fn test_tls_config_builder() {
        let tls = TlsConfig::new()
            .enabled(true)
            .ca_cert("/path/to/ca.crt")
            .client_cert("/path/to/client.crt", "/path/to/client.key");

        assert!(tls.enabled);
        assert_eq!(tls.ca_cert_path, Some("/path/to/ca.crt".to_string()));
        assert_eq!(
            tls.client_cert_path,
            Some("/path/to/client.crt".to_string())
        );
        assert_eq!(tls.client_key_path, Some("/path/to/client.key".to_string()));
    }

    #[test]
    fn test_redis_config_default() {
        let config = RedisConfig::default();
        assert_eq!(config.url, "redis://localhost:6379");
        assert_eq!(config.database, Some(0));
        assert!(config.connection_timeout.is_some());
    }

    #[test]
    fn test_redis_config_from_url() {
        let config = RedisConfig::from_url("redis://example.com:6380");
        assert_eq!(config.url, "redis://example.com:6380");
    }

    #[test]
    fn test_redis_config_builder() {
        let config = RedisConfig::new()
            .url("redis://localhost:6379")
            .database(2)
            .username("user")
            .password("pass")
            .connection_timeout(Duration::from_secs(10));

        assert_eq!(config.database, Some(2));
        assert_eq!(config.username, Some("user".to_string()));
        assert_eq!(config.password, Some("pass".to_string()));
        assert_eq!(config.connection_timeout, Some(Duration::from_secs(10)));
    }

    #[test]
    fn test_redis_config_sanitized_url() {
        let config = RedisConfig::new().url("redis://user:password@localhost:6379");
        let sanitized = config.sanitized_url();
        assert!(sanitized.contains("***"));
        assert!(!sanitized.contains("password"));
    }

    #[test]
    fn test_redis_config_describe() {
        let config = RedisConfig::new().url("redis://localhost:6379").database(1);
        let desc = config.describe();
        assert!(desc.contains("Redis"));
        assert!(desc.contains("db=Some(1)"));
    }

    #[test]
    fn test_connection_stats() {
        let mut stats = ConnectionStats::default();
        assert_eq!(stats.success_rate(), 0.0);

        stats.connection_attempts = 10;
        stats.successful_connections = 9;
        stats.failed_connections = 1;

        assert_eq!(stats.success_rate(), 0.9);
        assert!(stats.is_healthy(0.8));
        assert!(!stats.is_healthy(0.95));
    }

    #[test]
    fn test_build_client_basic() {
        let config = RedisConfig::from_url("redis://localhost:6379");
        let result = config.build_client();
        assert!(result.is_ok());
    }

    #[test]
    fn test_build_client_invalid_url() {
        let config = RedisConfig::from_url("invalid://bad-url");
        let result = config.build_client();
        assert!(result.is_err());
    }
}
