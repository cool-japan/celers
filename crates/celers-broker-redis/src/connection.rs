//! Redis connection configuration and management
//!
//! Provides flexible connection configuration including:
//! - Basic Redis URL connections
//! - TLS/SSL support
//! - Connection timeouts
//! - Connection pooling settings
//! - Authentication options

use celers_core::{CelersError, Result};
use redis::{
    aio::MultiplexedConnection, Client, ClientTlsConfig, ConnectionAddr, ConnectionInfo,
    IntoConnectionInfo, TlsCertificates,
};
use std::future::Future;
use std::sync::Once;
use std::time::Duration;
use tracing::{debug, trace};

static TLS_PROVIDER_INIT: Once = Once::new();

/// Install the Pure-Rust (`rustls-rustcrypto`) crypto provider as the process
/// default, once per process.
///
/// # Why this is mandatory, not an optimisation
///
/// The workspace declares `rustls` with `default-features = false`, so **no**
/// rustls crypto-provider feature is enabled anywhere in this build. The
/// `redis` crate's `create_rustls_config` goes through the bare
/// `rustls::ClientConfig::builder()`, which resolves a provider by looking at
/// (a) the process default, then (b) the single enabled provider feature. With
/// no provider feature, a missing process default is a **panic**, not an error.
///
/// Every `redis::Client` this crate constructs therefore goes through
/// [`open_client`] or [`RedisConfig::build_client`], both of which call this
/// first. The same guard exists in `celers-backend-redis` and
/// `celers-broker-amqp` for their own transports.
///
/// Installing a default can only succeed once. A second attempt — or an attempt
/// after another component installed its own — is not an error here; it just
/// means somebody else already made the choice.
///
/// **Ordering matters**: whoever builds a `rustls::ClientConfig` first wins. An
/// application that creates TLS clients through other libraries earlier in
/// startup should call this itself, first thing in `main`.
pub fn install_pure_tls_provider() {
    TLS_PROVIDER_INIT.call_once(|| {
        // `CryptoProvider::install_default` consumes the provider by value.
        if oxitls::pure_provider()
            .as_ref()
            .clone()
            .install_default()
            .is_ok()
        {
            debug!("Installed Pure-Rust rustls crypto provider for Redis TLS");
        } else {
            trace!("A rustls crypto provider was already installed for this process");
        }
    });
}

/// Open a [`redis::Client`] with the Pure-Rust TLS provider installed first.
///
/// A drop-in replacement for [`redis::Client::open`] and the only form this
/// crate uses, so that a `rediss://` URL reaching any code path finds a crypto
/// provider when the handshake starts — see [`install_pure_tls_provider`] for
/// why its absence is a panic rather than an error.
///
/// # Errors
///
/// Returns whatever [`redis::Client::open`] returns for an unusable URL.
pub fn open_client<T: IntoConnectionInfo>(params: T) -> redis::RedisResult<Client> {
    install_pure_tls_provider();
    Client::open(params)
}

/// How long a normal (non-blocking) command may take before the *client*
/// gives up on it.
///
/// The `redis` crate defaults this to **500 ms** — for every connection built
/// with `Client::get_multiplexed_async_connection` or a bare
/// `ConnectionManagerConfig::new()`. Half a second is a plausible budget for
/// an idle laptop and a hopeless one for a loaded machine: under CPU
/// saturation even an `LPUSH` round trip can miss it, and the failure
/// surfaces as a bare `"timed out"` I/O error that looks like a broken Redis
/// rather than an impatient client. Thirty seconds is long enough that
/// hitting it means something is genuinely wrong, and still short enough to
/// stop a caller waiting forever on a wedged server.
pub const DEFAULT_RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long establishing a connection (TCP connect, TLS handshake, `AUTH`,
/// `SELECT`) may take. The `redis` crate default is 1 second, which the same
/// saturated machine misses on the handshake alone.
pub const DEFAULT_CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

/// Headroom added to a blocking command's own timeout when deriving the
/// response timeout of the connection that carries it.
///
/// A blocking command (`BRPOPLPUSH`, `BLPOP`, `XREAD BLOCK`) legitimately
/// sends no reply until either a message arrives or its server-side timeout
/// expires. A response timeout shorter than that server-side wait makes the
/// client kill its own request before the server ever answers — an empty
/// queue then reports `Err("timed out")` instead of "nothing there". The
/// response timeout of such a connection must therefore always be strictly
/// greater than the longest block it will carry.
pub const BLOCKING_RESPONSE_MARGIN: Duration = Duration::from_secs(10);

/// The response timeout a connection carrying blocking commands must use.
///
/// Returns `None` — meaning "never time out the response" — when the
/// requested block is unbounded (`0`, which Redis reads as "wait forever") or
/// so long that adding the margin would overflow.
pub fn blocking_response_timeout(max_block: Duration) -> Option<Duration> {
    if max_block.is_zero() {
        // Redis treats a zero timeout as "block indefinitely", so no
        // client-side deadline can be correct.
        return None;
    }
    max_block.checked_add(BLOCKING_RESPONSE_MARGIN)
}

/// Connection settings for ordinary request/response traffic.
pub fn default_async_config() -> redis::AsyncConnectionConfig {
    redis::AsyncConnectionConfig::new()
        .set_response_timeout(Some(DEFAULT_RESPONSE_TIMEOUT))
        .set_connection_timeout(Some(DEFAULT_CONNECTION_TIMEOUT))
}

/// Connection settings for a connection that will carry blocking commands
/// blocking for at most `max_block`.
pub fn blocking_async_config(max_block: Duration) -> redis::AsyncConnectionConfig {
    redis::AsyncConnectionConfig::new()
        .set_response_timeout(blocking_response_timeout(max_block))
        .set_connection_timeout(Some(DEFAULT_CONNECTION_TIMEOUT))
}

/// Connection-manager settings for ordinary request/response traffic.
///
/// [`redis::aio::ConnectionManagerConfig::new`] carries the same 500 ms
/// response timeout as [`DEFAULT_RESPONSE_TIMEOUT`] documents, so every
/// manager this crate builds goes through here.
pub fn default_manager_config() -> redis::aio::ConnectionManagerConfig {
    redis::aio::ConnectionManagerConfig::new()
        .set_response_timeout(Some(DEFAULT_RESPONSE_TIMEOUT))
        .set_connection_timeout(Some(DEFAULT_CONNECTION_TIMEOUT))
}

/// Connection constructors carrying this crate's timeouts.
///
/// Every connection in this crate is opened through one of these instead of
/// [`redis::Client::get_multiplexed_async_connection`], whose defaults
/// (500 ms response, 1 s connect) are documented on
/// [`DEFAULT_RESPONSE_TIMEOUT`].
pub trait RedisClientExt {
    /// Open a multiplexed connection for ordinary request/response traffic.
    fn celers_multiplexed_connection(
        &self,
    ) -> impl Future<Output = redis::RedisResult<MultiplexedConnection>> + Send;

    /// Open a connection that will carry blocking commands waiting at most
    /// `max_block` — a `Duration::ZERO` meaning "block indefinitely".
    ///
    /// Such a connection must not be multiplexed with ordinary traffic: a
    /// blocking command parks every command queued behind it on the same
    /// socket.
    fn celers_blocking_connection(
        &self,
        max_block: Duration,
    ) -> impl Future<Output = redis::RedisResult<MultiplexedConnection>> + Send;
}

// `async fn` in a trait would return a future with no `Send` bound, which
// every `tokio::spawn`ed and `#[async_trait]` caller in this crate needs; the
// explicit `impl Future + Send` is the point, not an oversight.
#[allow(clippy::manual_async_fn)]
impl RedisClientExt for Client {
    fn celers_multiplexed_connection(
        &self,
    ) -> impl Future<Output = redis::RedisResult<MultiplexedConnection>> + Send {
        async move {
            self.get_multiplexed_async_connection_with_config(&default_async_config())
                .await
        }
    }

    fn celers_blocking_connection(
        &self,
        max_block: Duration,
    ) -> impl Future<Output = redis::RedisResult<MultiplexedConnection>> + Send {
        async move {
            self.get_multiplexed_async_connection_with_config(&blocking_async_config(max_block))
                .await
        }
    }
}

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

    /// Whether any certificate file is configured.
    ///
    /// Used by [`RedisConfig::build_client`] to tell "no TLS wanted" apart from
    /// "TLS wanted but the address is plaintext", which are the same shape in
    /// this struct but very different mistakes.
    pub(crate) fn has_certificate_material(&self) -> bool {
        self.ca_cert_path.is_some()
            || self.client_cert_path.is_some()
            || self.client_key_path.is_some()
    }

    /// Refuse settings that this stack cannot actually enforce.
    ///
    /// Cipher suites and protocol-version bounds have no expression in the
    /// `redis` API at all: [`redis::TlsCertificates`] carries exactly two
    /// fields (`client_tls` and `root_cert`), and `redis`'s
    /// `create_rustls_config` builds its `ClientConfig` with neither
    /// `with_protocol_versions` nor a cipher-suite list. Accepting them
    /// silently would connect on whatever the provider defaults to while the
    /// operator believed they had pinned something — a security downgrade.
    ///
    /// The certificate knobs, by contrast, *are* honoured; see
    /// [`RedisConfig::tls_certificates`].
    ///
    /// # Errors
    ///
    /// [`CelersError::Broker`] if a cipher suite or a TLS version bound is set.
    fn reject_unhonourable_settings(&self) -> Result<()> {
        if self.cipher_suites.is_some()
            || self.min_tls_version.is_some()
            || self.max_tls_version.is_some()
        {
            return Err(CelersError::Broker(
                "TLS cipher suite / protocol version pinning is not expressible through the \
                 `redis` client API; configure it on the Redis server instead of setting it here, \
                 where it would be silently ignored"
                    .to_string(),
            ));
        }
        Ok(())
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
            // See `DEFAULT_RESPONSE_TIMEOUT`: a few seconds is not a safety
            // margin, it is a tripwire that fires on a busy machine.
            connection_timeout: Some(DEFAULT_CONNECTION_TIMEOUT),
            response_timeout: Some(DEFAULT_RESPONSE_TIMEOUT),
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
        self.tls.reject_unhonourable_settings()?;

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

    /// Read the configured PEM material into the shape `redis` wants.
    ///
    /// Returns `Ok(None)` when nothing beyond the default trust store was
    /// asked for, which is the signal to take the plain
    /// [`redis::Client::open`] path.
    ///
    /// # Errors
    ///
    /// * [`CelersError::Broker`] if a client certificate is configured without
    ///   its key (or the other way round) — an mTLS setup that is half
    ///   configured must fail loudly, never fall back to server-only auth.
    /// * [`CelersError::Broker`] if any configured file cannot be read.
    fn tls_certificates(&self) -> Result<Option<TlsCertificates>> {
        let root_cert = match &self.tls.ca_cert_path {
            Some(path) => Some(read_pem(path, "CA certificate")?),
            None => None,
        };

        let client_tls = match (&self.tls.client_cert_path, &self.tls.client_key_path) {
            (Some(cert_path), Some(key_path)) => Some(ClientTlsConfig {
                client_cert: read_pem(cert_path, "client certificate")?,
                client_key: read_pem(key_path, "client key")?,
            }),
            (None, None) => None,
            (Some(_), None) => {
                return Err(CelersError::Broker(
                    "TLS client certificate configured without a client key; mTLS needs both \
                     (use TlsConfig::client_cert(cert_path, key_path))"
                        .to_string(),
                ))
            }
            (None, Some(_)) => {
                return Err(CelersError::Broker(
                    "TLS client key configured without a client certificate; mTLS needs both \
                     (use TlsConfig::client_cert(cert_path, key_path))"
                        .to_string(),
                ))
            }
        };

        if root_cert.is_none() && client_tls.is_none() {
            return Ok(None);
        }

        Ok(Some(TlsCertificates {
            client_tls,
            root_cert,
        }))
    }

    /// Build a Redis client from this configuration
    ///
    /// # TLS
    ///
    /// Installs the Pure-Rust crypto provider first — see
    /// [`install_pure_tls_provider`] for why that is load-bearing rather than
    /// decorative.
    ///
    /// When [`TlsConfig::ca_cert`] or [`TlsConfig::client_cert`] is set, the
    /// client is built through [`redis::Client::build_with_tls`] so the
    /// certificates actually take effect. Note the trust-store semantics that
    /// come with `redis`: a configured CA **replaces** the built-in Mozilla
    /// bundle rather than adding to it, which is what you want for a private
    /// Redis CA and is worth knowing if you expected both to be trusted.
    ///
    /// # Errors
    ///
    /// [`CelersError::Broker`] if the URL is unusable, if a requested TLS
    /// setting cannot be honoured, or if configured certificate files cannot be
    /// read or parsed.
    pub fn build_client(&self) -> Result<Client> {
        install_pure_tls_provider();

        let info = self.connection_info()?;

        // Keyed off the *resolved* address rather than `tls.enabled`, so a
        // `rediss://` URL (which `redis` parses straight into `TcpTls`) picks
        // up the configured certificates too. Keying off the flag would drop
        // them for exactly the URL form that most obviously means TLS.
        if matches!(info.addr(), ConnectionAddr::TcpTls { .. }) {
            // Also runs for a `rediss://` URL with `tls.enabled` left false, a
            // path `apply_tls` never sees. Without it, cipher-suite and
            // version settings would be silently dropped for exactly the URL
            // form that most obviously means TLS.
            self.tls.reject_unhonourable_settings()?;

            if let Some(certificates) = self.tls_certificates()? {
                return Client::build_with_tls(info, certificates).map_err(|e| {
                    CelersError::Broker(format!("Failed to create Redis TLS client: {}", e))
                });
            }
        } else if self.tls.has_certificate_material() {
            // Certificates on a plaintext connection are not "inert extra
            // configuration", they are a config the operator believes is
            // encrypted. Say so instead of connecting in the clear.
            return Err(CelersError::Broker(
                "TLS certificates are configured but this connection is plaintext; use a \
                 `rediss://` URL or `TlsConfig::enabled(true)`"
                    .to_string(),
            ));
        }

        Client::open(info)
            .map_err(|e| CelersError::Broker(format!("Failed to create Redis client: {}", e)))
    }

    /// Connection-manager settings derived from this configuration.
    ///
    /// The timeouts belong to the connection rather than the client, so they
    /// are applied where the connection is actually established.
    ///
    /// Both timeouts are always set explicitly, never left at the `redis`
    /// crate's defaults — see [`DEFAULT_RESPONSE_TIMEOUT`].
    pub fn manager_config(&self) -> redis::aio::ConnectionManagerConfig {
        redis::aio::ConnectionManagerConfig::new()
            .set_connection_timeout(self.connection_timeout)
            .set_response_timeout(self.response_timeout)
    }

    /// Connection settings for one-off multiplexed connections built from
    /// this configuration.
    pub fn async_config(&self) -> redis::AsyncConnectionConfig {
        redis::AsyncConnectionConfig::new()
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

/// Read a PEM file, naming what it was supposed to be when it cannot be read.
///
/// `redis` takes certificate material as raw PEM bytes, so the only thing this
/// adds is an error message that identifies the file and its role — a bare
/// `No such file or directory (os error 2)` at connect time is exactly the kind
/// of failure that costs an operator an afternoon.
fn read_pem(path: &str, role: &str) -> Result<Vec<u8>> {
    std::fs::read(path)
        .map_err(|e| CelersError::Broker(format!("Failed to read TLS {} '{}': {}", role, path, e)))
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
        // Never the `redis` crate's 500 ms / 1 s defaults, and never the
        // 3 s / 5 s this used to carry: both are short enough that a busy
        // machine trips them on healthy traffic.
        assert_eq!(config.connection_timeout, Some(DEFAULT_CONNECTION_TIMEOUT));
        assert_eq!(config.response_timeout, Some(DEFAULT_RESPONSE_TIMEOUT));
    }

    /// A connection carrying a blocking command must outlive the block
    /// itself, or the client kills its own request before the server
    /// answers.
    #[test]
    fn test_blocking_response_timeout_exceeds_the_block() {
        let block = Duration::from_secs(5);
        let timeout = blocking_response_timeout(block).expect("a bounded block has a deadline");
        assert!(
            timeout > block,
            "{:?} must leave room beyond the {:?} block",
            timeout,
            block
        );
        assert_eq!(timeout, block + BLOCKING_RESPONSE_MARGIN);

        // A long block scales the deadline instead of capping it.
        let long = Duration::from_secs(600);
        assert_eq!(
            blocking_response_timeout(long),
            Some(long + BLOCKING_RESPONSE_MARGIN)
        );

        // Redis reads a zero timeout as "block forever", so no client-side
        // deadline can be right.
        assert_eq!(blocking_response_timeout(Duration::ZERO), None);
    }

    /// `AsyncConnectionConfig` exposes no getters, so the manager config —
    /// which does — stands in for "the shared defaults are not the `redis`
    /// crate's".
    #[test]
    fn test_default_manager_config_overrides_crate_defaults() {
        let manager = default_manager_config();
        assert_eq!(manager.response_timeout(), Some(DEFAULT_RESPONSE_TIMEOUT));
        assert_eq!(
            manager.connection_timeout(),
            Some(DEFAULT_CONNECTION_TIMEOUT)
        );

        // The values the `redis` crate would have used unasked.
        assert_ne!(manager.response_timeout(), Some(Duration::from_millis(500)));
        assert_ne!(manager.connection_timeout(), Some(Duration::from_secs(1)));
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
    /// quietly ignored -- silently connecting on whatever the provider
    /// defaults to, while the operator believes they pinned TLS 1.3, is a
    /// security downgrade.
    ///
    /// Cipher suites and version bounds are the two that remain unhonourable:
    /// `redis::TlsCertificates` has exactly two fields and
    /// `create_rustls_config` sets neither a version list nor a suite list.
    #[test]
    fn test_unsupported_tls_options_are_rejected() {
        for tls in [
            TlsConfig::new().enabled(true).min_tls_version("1.3"),
            TlsConfig::new().enabled(true).max_tls_version("1.2"),
            TlsConfig::new()
                .enabled(true)
                .cipher_suites("TLS_AES_256_GCM_SHA384"),
        ] {
            let config = RedisConfig::from_url("redis://example.com:6379").tls(tls);
            let error = config
                .build_client()
                .expect_err("an unhonourable TLS setting must not be ignored")
                .to_string();
            assert!(error.contains("not expressible"), "{error}");
        }

        // With TLS disabled the same options are inert, not an error: nothing
        // about a plaintext connection claims to honour them.
        let config = RedisConfig::from_url("redis://example.com:6379")
            .tls(TlsConfig::new().min_tls_version("1.3"));
        assert!(config.build_client().is_ok());

        // But a `rediss://` URL *is* a TLS connection even with the `enabled`
        // flag left false, so the same setting must be refused there. This is
        // the path `apply_tls` never sees.
        let config = RedisConfig::from_url("rediss://example.com:6379")
            .tls(TlsConfig::new().min_tls_version("1.3"));
        let error = config
            .build_client()
            .expect_err("a rediss:// URL must not silently drop a version pin")
            .to_string();
        assert!(error.contains("not expressible"), "{error}");
    }

    /// The `redis` crate must actually have TLS compiled in — the whole
    /// `rediss://` story rests on the workspace's `tokio-rustls-comp` feature,
    /// and losing it would otherwise show up only as a connect-time failure in
    /// production.
    #[test]
    fn test_the_redis_crate_has_tls_support() {
        let info = "rediss://127.0.0.1:6379"
            .into_connection_info()
            .expect("rediss:// must parse; is the `redis` tls-rustls feature still enabled?");
        assert!(matches!(info.addr(), ConnectionAddr::TcpTls { .. }));
    }

    /// A `rediss://` URL must produce a TLS client without any extra builder
    /// call, and building it must not panic — which is the observable form of
    /// "the Pure-Rust crypto provider was installed first".
    #[test]
    fn test_rediss_url_builds_a_tls_client() {
        let config = RedisConfig::from_url("rediss://example.com:6379");
        let client = config.build_client().expect("client");
        assert!(matches!(
            client.get_connection_info().addr(),
            ConnectionAddr::TcpTls { .. }
        ));
    }

    /// Installing the provider must be safe to repeat: every constructor in
    /// this crate calls it, so it runs many times per process.
    #[test]
    fn test_install_pure_tls_provider_is_idempotent() {
        install_pure_tls_provider();
        install_pure_tls_provider();
    }

    /// `open_client` is the crate-wide replacement for `redis::Client::open`;
    /// it must behave identically for ordinary URLs and accept `rediss://`.
    #[test]
    fn test_open_client_matches_client_open() {
        let client = open_client("redis://example.com:6379/4").expect("client");
        assert_eq!(client.get_connection_info().redis_settings().db(), 4);

        let tls_client = open_client("rediss://example.com:6379").expect("tls client");
        assert!(matches!(
            tls_client.get_connection_info().addr(),
            ConnectionAddr::TcpTls { .. }
        ));

        assert!(open_client("invalid://bad-url").is_err());
    }

    /// A CA certificate must reach `redis::Client::build_with_tls`, i.e. the
    /// resulting client must carry TLS parameters. Before this wiring existed
    /// the same config was rejected outright.
    #[test]
    fn test_custom_ca_certificate_is_wired_through() {
        let dir = std::env::temp_dir().join(format!("celers-redis-tls-ca-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let ca_path = dir.join("ca.pem");
        std::fs::write(&ca_path, SELF_SIGNED_CA_PEM).expect("write ca");

        let config = RedisConfig::from_url("rediss://example.com:6379")
            .tls(TlsConfig::new().ca_cert(ca_path.to_string_lossy().to_string()));

        let client = config.build_client().expect("client with custom CA");
        match client.get_connection_info().addr() {
            ConnectionAddr::TcpTls { tls_params, .. } => assert!(
                tls_params.is_some(),
                "a configured CA must produce TLS parameters, not the default trust store"
            ),
            other => panic!("expected a TLS address, got {other:?}"),
        }

        std::fs::remove_dir_all(&dir).ok();
    }

    /// A CA path that does not exist must fail with a message naming the file
    /// and its role, not with a bare OS error at connect time.
    #[test]
    fn test_missing_ca_certificate_is_reported_clearly() {
        let missing = std::env::temp_dir().join("celers-redis-tls-does-not-exist.pem");
        let config = RedisConfig::from_url("rediss://example.com:6379")
            .tls(TlsConfig::new().ca_cert(missing.to_string_lossy().to_string()));

        let error = config
            .build_client()
            .expect_err("a missing CA file must fail")
            .to_string();
        assert!(error.contains("CA certificate"), "{error}");
        assert!(error.contains("celers-redis-tls-does-not-exist"), "{error}");
    }

    /// Half-configured mTLS must fail loudly. Falling back to server-only
    /// authentication because the key path was forgotten is exactly the silent
    /// downgrade this module exists to prevent.
    #[test]
    fn test_client_certificate_without_key_is_rejected() {
        let mut tls = TlsConfig::new().enabled(true);
        tls.client_cert_path = Some("/tmp/celers-client.pem".to_string());

        let error = RedisConfig::from_url("redis://example.com:6379")
            .tls(tls)
            .build_client()
            .expect_err("a certificate without a key must not connect")
            .to_string();
        assert!(error.contains("client key"), "{error}");

        let mut tls = TlsConfig::new().enabled(true);
        tls.client_key_path = Some("/tmp/celers-client.key".to_string());

        let error = RedisConfig::from_url("redis://example.com:6379")
            .tls(tls)
            .build_client()
            .expect_err("a key without a certificate must not connect")
            .to_string();
        assert!(error.contains("client certificate"), "{error}");
    }

    /// Certificates on a plaintext connection mean the operator thinks the
    /// connection is encrypted. Connecting anyway would be the downgrade.
    #[test]
    fn test_certificates_on_a_plaintext_connection_are_rejected() {
        let config = RedisConfig::from_url("redis://example.com:6379")
            .tls(TlsConfig::new().ca_cert("/tmp/celers-ca.pem"));

        let error = config
            .build_client()
            .expect_err("certificates must not be dropped silently")
            .to_string();
        assert!(error.contains("plaintext"), "{error}");
    }

    /// A syntactically valid but semantically useless PEM must be rejected by
    /// `redis` rather than producing a client that fails much later.
    #[test]
    fn test_unparseable_ca_certificate_is_rejected() {
        let dir = std::env::temp_dir().join(format!("celers-redis-tls-bad-{}", std::process::id()));
        std::fs::create_dir_all(&dir).expect("temp dir");
        let ca_path = dir.join("ca.pem");
        std::fs::write(&ca_path, b"-----BEGIN CERTIFICATE-----\nnot base64\n").expect("write");

        let config = RedisConfig::from_url("rediss://example.com:6379")
            .tls(TlsConfig::new().ca_cert(ca_path.to_string_lossy().to_string()));

        assert!(
            config.build_client().is_err(),
            "a malformed CA PEM must not yield a usable client"
        );

        std::fs::remove_dir_all(&dir).ok();
    }

    // -- live-server TLS probe ----------------------------------------------
    //
    // Needs a real Redis. Skipped, visibly, when `CELERS_TEST_REDIS_URL` is
    // unset, so the default suite stays hermetic.

    /// The live-server URL, or `None` when the suite is not enabled.
    fn live_url() -> Option<String> {
        std::env::var("CELERS_TEST_REDIS_URL")
            .ok()
            .filter(|url| !url.is_empty())
    }

    /// A `rediss://` URL must get as far as the TLS handshake.
    ///
    /// This is the test that would catch the failure mode the whole
    /// [`install_pure_tls_provider`] machinery exists to prevent: with no
    /// rustls provider feature enabled anywhere in this workspace, `redis`'s
    /// bare `rustls::ClientConfig::builder()` **panics** when no process
    /// default has been installed. A unit test cannot see that — the config is
    /// built lazily, at connect time — so it takes a real socket.
    ///
    /// The local Redis speaks no TLS, so the handshake is expected to *fail*.
    /// What matters is *how*: the plaintext control connection must succeed
    /// (proving the server is up and the URL is right) while the `rediss://`
    /// one must return an ordinary transport error (proving we reached the TLS
    /// layer and came back with a `Result`, not a panic or a silent plaintext
    /// downgrade).
    #[tokio::test]
    async fn live_rediss_url_reaches_the_tls_handshake() {
        let Some(url) = live_url() else {
            eprintln!(
                "SKIPPED: live_rediss_url_reaches_the_tls_handshake \
                 (set CELERS_TEST_REDIS_URL to run)"
            );
            return;
        };

        // Control: the same server, in the clear, must be reachable. Without
        // this the TLS failure below would also "pass" against a dead server.
        let plaintext = open_client(url.as_str()).expect("plaintext client");
        plaintext
            .celers_multiplexed_connection()
            .await
            .expect("the control connection must succeed; is CELERS_TEST_REDIS_URL correct?");

        let tls_url = url.replacen("redis://", "rediss://", 1);
        assert!(
            tls_url.starts_with("rediss://"),
            "CELERS_TEST_REDIS_URL must be a redis:// URL to derive a rediss:// one, got {url}"
        );

        let tls_client = open_client(tls_url.as_str()).expect("rediss:// must parse into a client");
        assert!(
            matches!(
                tls_client.get_connection_info().addr(),
                ConnectionAddr::TcpTls { .. }
            ),
            "a rediss:// URL must never resolve to a plaintext address"
        );

        // A short budget on purpose. A plaintext Redis reads the ClientHello
        // as an inline command and simply keeps buffering, so the handshake
        // does not fail fast — it stalls. The crate default of
        // `DEFAULT_CONNECTION_TIMEOUT` (10 s) would make this the slowest test
        // in the suite for no extra signal.
        let probe = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(Duration::from_secs(3)))
            .set_response_timeout(Some(Duration::from_secs(3)));

        // The panic this guards against would abort the test process here.
        let error = tls_client
            .get_multiplexed_async_connection_with_config(&probe)
            .await
            .expect_err("a plaintext Redis must not complete a TLS handshake");

        // Whether the server stalls (timeout) or answers with a RESP error
        // line that rustls rejects as a corrupt record, the failure is a
        // transport failure. What it must never be is `InvalidClientConfig`,
        // which is what `redis` reports when the TLS configuration itself is
        // unusable — i.e. when this crate's TLS wiring is wrong rather than the
        // server's.
        assert_ne!(
            error.kind(),
            redis::ErrorKind::InvalidClientConfig,
            "the failure must come from the handshake, not from an unusable TLS config: {error}"
        );
    }

    /// A throwaway self-signed CA (P-256, `CN=celers-test-ca`, valid to 2126),
    /// generated once and pasted here so the test suite needs no
    /// certificate-minting dependency and no network. Its private key was
    /// discarded: it can authenticate nothing, and exists only to prove that
    /// PEM bytes travel from [`TlsConfig::ca_cert`] into `redis`'s
    /// `TlsConnParams` and parse as a real trust anchor on the way.
    const SELF_SIGNED_CA_PEM: &[u8] = b"-----BEGIN CERTIFICATE-----
MIIBiDCCAS+gAwIBAgIUREhFQykc82cn85aSS5aL1AevE9gwCgYIKoZIzj0EAwIw
GTEXMBUGA1UEAwwOY2VsZXJzLXRlc3QtY2EwIBcNMjYwODI1MTYwNzQ4WhgPMjEy
NjA4MDExNjA3NDhaMBkxFzAVBgNVBAMMDmNlbGVycy10ZXN0LWNhMFkwEwYHKoZI
zj0CAQYIKoZIzj0DAQcDQgAEKv8djUYTeR/2hdteo241Xlzcm0FgC9EDt3x0eJya
iOr9HeL/kwQVEA7jSz0xCJ0BSVlsd37wik1DfzG4+zLHEKNTMFEwHQYDVR0OBBYE
FM64osaR94Hu3TNNK05xMkmdbPb/MB8GA1UdIwQYMBaAFM64osaR94Hu3TNNK05x
MkmdbPb/MA8GA1UdEwEB/wQFMAMBAf8wCgYIKoZIzj0EAwIDRwAwRAIgax8JRtx+
OS3DkbL8yOM6NzXsw1aDVkvFFl+UbCmE95YCIAHTBoGUGFbnVweCT62DcLZCZwWg
Dteb9wDC3Ie+jao0
-----END CERTIFICATE-----
";

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
