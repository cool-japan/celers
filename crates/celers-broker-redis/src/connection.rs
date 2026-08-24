//! Redis connection configuration and management
//!
//! Provides flexible connection configuration including:
//! - Basic Redis URL connections
//! - TLS/SSL support
//! - Connection timeouts
//! - Connection pooling settings
//! - Authentication options

use celers_core::{CelersError, Result};
use redis::{Client, ConnectionAddr, ConnectionInfo, IntoConnectionInfo};
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
    /// Cipher suites to use (e.g., "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256")
    /// If None, uses the default cipher suites
    pub cipher_suites: Option<String>,
    /// Minimum TLS version (e.g., "1.2", "1.3")
    pub min_tls_version: Option<String>,
    /// Maximum TLS version (e.g., "1.2", "1.3")
    pub max_tls_version: Option<String>,
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

    /// Set cipher suites (e.g., "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256")
    pub fn cipher_suites(mut self, suites: impl Into<String>) -> Self {
        self.cipher_suites = Some(suites.into());
        self
    }

    /// Set minimum TLS version (e.g., "1.2", "1.3")
    pub fn min_tls_version(mut self, version: impl Into<String>) -> Self {
        self.min_tls_version = Some(version.into());
        self
    }

    /// Set maximum TLS version (e.g., "1.2", "1.3")
    pub fn max_tls_version(mut self, version: impl Into<String>) -> Self {
        self.max_tls_version = Some(version.into());
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
    ///
    /// `None` means "whatever the URL selects" (database 0 unless the URL
    /// carries a path such as `redis://host/3`). A `Some` value always wins
    /// over the URL.
    pub database: Option<i64>,
    /// Username for authentication (Redis 6+)
    pub username: Option<String>,
    /// Password for authentication
    pub password: Option<String>,
    /// ACL token for authentication (Redis 6+, alternative to username/password)
    pub acl_token: Option<String>,
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
            // Defaulting to `Some(0)` would silently override a database
            // selected in the URL (`redis://host/3`), because the URL parser
            // cannot distinguish "unset" from "explicitly 0".
            database: None,
            username: None,
            password: None,
            acl_token: None,
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

    /// Set ACL token for authentication (Redis 6+)
    /// This is an alternative to username/password authentication
    pub fn acl_token(mut self, token: impl Into<String>) -> Self {
        self.acl_token = Some(token.into());
        self
    }

    /// Set retry configuration
    pub fn retry(mut self, max_attempts: usize, delay: Duration) -> Self {
        self.max_retry_attempts = max_attempts;
        self.retry_delay = delay;
        self
    }

    /// Resolve this configuration into a fully-populated [`ConnectionInfo`].
    ///
    /// Every credential and connection option set on the builder is applied
    /// here. Passing the raw URL to [`Client::open`] instead would drop them
    /// all silently: a config with `password(secret).database(3).tls(...)`
    /// would produce an unauthenticated, unencrypted client on database 0, and
    /// the failure would surface later (as `NOAUTH`) or -- for TLS -- not at
    /// all, as a quiet downgrade to plaintext.
    ///
    /// Precedence: explicit builder values win over anything embedded in the
    /// URL; unset values leave the URL's own settings intact.
    pub fn connection_info(&self) -> Result<ConnectionInfo> {
        let mut info = self
            .url
            .as_str()
            .into_connection_info()
            .map_err(|e| CelersError::Broker(format!("Invalid Redis URL: {}", e)))?;

        let mut redis_settings = info.redis_settings().clone();
        if let Some(username) = &self.username {
            redis_settings = redis_settings.set_username(username);
        }
        // Redis 6 ACL tokens authenticate through the password slot, so a
        // token is simply a password unless an explicit one was given.
        if let Some(password) = self.password.as_ref().or(self.acl_token.as_ref()) {
            redis_settings = redis_settings.set_password(password);
        }
        if let Some(database) = self.database {
            redis_settings = redis_settings.set_db(database);
        }
        info = info.set_redis_settings(redis_settings);

        if self.tls.enabled {
            info = self.apply_tls(info)?;
        }

        Ok(info)
    }

    /// Upgrade the connection address to TLS, refusing to continue if any
    /// requested TLS setting cannot actually be honoured.
    fn apply_tls(&self, info: ConnectionInfo) -> Result<ConnectionInfo> {
        // `redis` builds its TLS parameters from its own feature-gated
        // machinery; there is no public constructor for them. Rather than
        // connect *without* the certificates or protocol bounds the operator
        // asked for -- a silent security downgrade -- refuse the config.
        if self.tls.ca_cert_path.is_some()
            || self.tls.client_cert_path.is_some()
            || self.tls.client_key_path.is_some()
        {
            return Err(CelersError::Broker(
                "TLS custom CA / client certificates are not supported by this build: rebuild \
                 with the `redis` crate's `tls-rustls` feature and pass the certificates through \
                 `redis::Client::build_with_tls`"
                    .to_string(),
            ));
        }
        if self.tls.cipher_suites.is_some()
            || self.tls.min_tls_version.is_some()
            || self.tls.max_tls_version.is_some()
        {
            return Err(CelersError::Broker(
                "TLS cipher suite / protocol version pinning is not configurable through the \
                 `redis` client; configure it on the Redis server instead of setting it here, \
                 where it would be silently ignored"
                    .to_string(),
            ));
        }

        // A `rediss://` URL only parses when the redis crate was compiled with
        // TLS support, which makes it a reliable probe: without it, a TLS
        // address would be built here and only fail much later, at connect
        // time, far from its cause.
        if "rediss://127.0.0.1:6379".into_connection_info().is_err() {
            return Err(CelersError::Broker(
                "TLS is enabled but this build of the `redis` crate has no TLS support; enable \
                 its `tls-rustls` (or `tls-native-tls`) feature"
                    .to_string(),
            ));
        }

        let addr = match info.addr() {
            ConnectionAddr::Tcp(host, port) => ConnectionAddr::TcpTls {
                host: host.clone(),
                port: *port,
                insecure: self.tls.insecure,
                tls_params: None,
            },
            ConnectionAddr::TcpTls {
                host,
                port,
                tls_params,
                ..
            } => ConnectionAddr::TcpTls {
                host: host.clone(),
                port: *port,
                insecure: self.tls.insecure,
                tls_params: tls_params.clone(),
            },
            other => {
                return Err(CelersError::Broker(format!(
                    "TLS is enabled but the URL selects a non-TCP transport ({:?}); TLS applies \
                     to TCP connections only",
                    other
                )))
            }
        };

        let info = info.set_addr(addr);

        // Hard guard: never hand back a client that claims TLS but would
        // connect in plaintext.
        if !matches!(info.addr(), ConnectionAddr::TcpTls { .. }) {
            return Err(CelersError::Broker(
                "Failed to enable TLS for this connection".to_string(),
            ));
        }

        Ok(info)
    }

    /// Build a Redis client from this configuration
    pub fn build_client(&self) -> Result<Client> {
        let info = self.connection_info()?;

        Client::open(info)
            .map_err(|e| CelersError::Broker(format!("Failed to create Redis client: {}", e)))
    }

    /// Connection-manager settings derived from this configuration.
    ///
    /// The timeouts belong to the connection rather than the client, so they
    /// are applied where the connection is actually established.
    pub fn manager_config(&self) -> redis::aio::ConnectionManagerConfig {
        redis::aio::ConnectionManagerConfig::new()
            .set_connection_timeout(self.connection_timeout)
            .set_response_timeout(self.response_timeout)
    }

    /// Get a descriptive string for this configuration (without sensitive data)
    pub fn describe(&self) -> String {
        format!(
            "Redis[url={}, tls={}, db={:?}, auth={}, timeout={:?}]",
            self.sanitized_url(),
            self.tls.enabled,
            self.database,
            self.auth_description(),
            self.connection_timeout
        )
    }

    /// How this configuration authenticates, without revealing the secret.
    fn auth_description(&self) -> &'static str {
        match (
            self.username.is_some(),
            self.password.is_some(),
            self.acl_token.is_some(),
        ) {
            (true, true, _) => "username+password",
            (true, false, true) => "username+acl-token",
            (true, false, false) => "username",
            (false, true, _) => "password",
            (false, false, true) => "acl-token",
            (false, false, false) => "none",
        }
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
        // `None` means "inherit from the URL"; a default of `Some(0)` would
        // silently override a database selected in the URL.
        assert_eq!(config.database, None);
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

    /// The whole point of the builder: credentials and database must reach
    /// the client. Dropping them yields an unauthenticated client on the
    /// wrong database, and the failure surfaces far from its cause.
    #[test]
    fn test_build_client_carries_credentials_and_database() {
        let config = RedisConfig::from_url("redis://example.com:6379")
            .username("svc")
            .password("s3cret")
            .database(3);

        let client = config.build_client().expect("client");
        let info = client.get_connection_info();

        assert_eq!(info.redis_settings().username(), Some("svc"));
        assert_eq!(info.redis_settings().password(), Some("s3cret"));
        assert_eq!(info.redis_settings().db(), 3);
    }

    /// A Redis 6 ACL token authenticates through the password slot.
    #[test]
    fn test_build_client_uses_acl_token_as_password() {
        let config = RedisConfig::from_url("redis://example.com:6379").acl_token("tok-123");
        let client = config.build_client().expect("client");
        assert_eq!(
            client.get_connection_info().redis_settings().password(),
            Some("tok-123")
        );

        // An explicit password wins over the token.
        let config = RedisConfig::from_url("redis://example.com:6379")
            .acl_token("tok-123")
            .password("explicit");
        let client = config.build_client().expect("client");
        assert_eq!(
            client.get_connection_info().redis_settings().password(),
            Some("explicit")
        );
    }

    /// Credentials embedded in the URL must survive when the builder does not
    /// override them, and must lose when it does.
    #[test]
    fn test_url_credentials_precedence() {
        let config = RedisConfig::from_url("redis://url_user:url_pass@example.com:6379/7");
        let info = config.connection_info().expect("info");
        assert_eq!(info.redis_settings().username(), Some("url_user"));
        assert_eq!(info.redis_settings().password(), Some("url_pass"));
        assert_eq!(info.redis_settings().db(), 7, "URL database must survive");

        let config = config.password("override").database(1);
        let info = config.connection_info().expect("info");
        assert_eq!(info.redis_settings().password(), Some("override"));
        assert_eq!(info.redis_settings().db(), 1);
    }

    /// TLS must never be dropped silently: either the address really is a TLS
    /// address, or building the client fails.
    #[test]
    fn test_tls_is_never_silently_downgraded() {
        let config =
            RedisConfig::from_url("redis://example.com:6379").tls(TlsConfig::new().enabled(true));

        match config.connection_info() {
            Ok(info) => assert!(
                matches!(info.addr(), ConnectionAddr::TcpTls { .. }),
                "a TLS-enabled config must not produce a plaintext address"
            ),
            // Acceptable outcome when this build of `redis` has no TLS
            // support -- what must never happen is a plaintext connection.
            Err(e) => assert!(
                e.to_string().contains("TLS"),
                "TLS failures must say so: {e}"
            ),
        }
    }

    /// Settings the client cannot honour must be rejected rather than
    /// quietly ignored -- silently connecting without the requested client
    /// certificate is a security downgrade.
    #[test]
    fn test_unsupported_tls_options_are_rejected() {
        let config = RedisConfig::from_url("redis://example.com:6379").tls(
            TlsConfig::new()
                .enabled(true)
                .client_cert("/tmp/client.crt", "/tmp/client.key"),
        );
        assert!(config.build_client().is_err());

        let config = RedisConfig::from_url("redis://example.com:6379")
            .tls(TlsConfig::new().enabled(true).min_tls_version("1.3"));
        assert!(config.build_client().is_err());

        // With TLS disabled the same options are inert, not an error.
        let config = RedisConfig::from_url("redis://example.com:6379")
            .tls(TlsConfig::new().min_tls_version("1.3"));
        assert!(config.build_client().is_ok());
    }

    #[test]
    fn test_manager_config_carries_timeouts() {
        let config = RedisConfig::from_url("redis://example.com:6379")
            .connection_timeout(Duration::from_secs(7))
            .response_timeout(Duration::from_secs(2));

        let manager_config = config.manager_config();
        assert_eq!(
            manager_config.connection_timeout(),
            Some(Duration::from_secs(7))
        );
        assert_eq!(
            manager_config.response_timeout(),
            Some(Duration::from_secs(2))
        );
    }

    #[test]
    fn test_describe_reports_auth_without_leaking_it() {
        let config = RedisConfig::from_url("redis://example.com:6379")
            .username("svc")
            .password("s3cret");
        let described = config.describe();
        assert!(described.contains("auth=username+password"), "{described}");
        assert!(!described.contains("s3cret"), "{described}");

        assert!(RedisConfig::from_url("redis://example.com:6379")
            .describe()
            .contains("auth=none"));
    }

    #[test]
    fn test_tls_config_cipher_suites() {
        let tls = TlsConfig::new()
            .enabled(true)
            .cipher_suites("TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256");

        assert!(tls.enabled);
        assert_eq!(
            tls.cipher_suites,
            Some("TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256".to_string())
        );
    }

    #[test]
    fn test_tls_config_tls_versions() {
        let tls = TlsConfig::new()
            .min_tls_version("1.2")
            .max_tls_version("1.3");

        assert_eq!(tls.min_tls_version, Some("1.2".to_string()));
        assert_eq!(tls.max_tls_version, Some("1.3".to_string()));
    }

    #[test]
    fn test_redis_config_acl_token() {
        let config = RedisConfig::new().acl_token("my-secret-token");

        assert_eq!(config.acl_token, Some("my-secret-token".to_string()));
    }

    #[test]
    fn test_redis_config_with_full_tls() {
        let tls = TlsConfig::new()
            .enabled(true)
            .ca_cert("/path/to/ca.crt")
            .client_cert("/path/to/client.crt", "/path/to/client.key")
            .cipher_suites("TLS_AES_256_GCM_SHA384")
            .min_tls_version("1.3");

        let config = RedisConfig::new()
            .url("rediss://localhost:6380")
            .tls(tls)
            .database(1);

        assert!(config.tls.enabled);
        assert_eq!(config.tls.ca_cert_path, Some("/path/to/ca.crt".to_string()));
        assert_eq!(
            config.tls.cipher_suites,
            Some("TLS_AES_256_GCM_SHA384".to_string())
        );
        assert_eq!(config.tls.min_tls_version, Some("1.3".to_string()));
    }
}
