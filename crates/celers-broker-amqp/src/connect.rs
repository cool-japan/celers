//! Connection establishment for the AMQP broker.
//!
//! Every `Connection::connect` in this crate goes through [`open_connection`]
//! so that the [`AmqpConfig`](crate::AmqpConfig) heartbeat and connection
//! timeout settings are applied consistently, and so the Pure-Rust TLS crypto
//! provider is installed before the first `amqps://` handshake.

use celers_kombu::{BrokerError, Result};
use lapin::{uri::AMQPUri, Connection, ConnectionProperties};
use std::str::FromStr;
use std::sync::Once;
use std::time::Duration;
use tracing::{debug, trace};

static TLS_PROVIDER_INIT: Once = Once::new();

/// Install the Pure-Rust (`rustls-rustcrypto`) crypto provider as the process
/// default, once per process.
///
/// `rustls-connector` (used by `amq-protocol-tcp` for `amqps://`) builds its
/// client configuration through the bare `rustls::ClientConfig::builder()`,
/// which resolves the *process default* `CryptoProvider` when one is
/// installed. Installing OxiTLS' `rustls-rustcrypto` provider here therefore
/// routes AMQP TLS through the Pure-Rust provider instead of the FFI-backed
/// `aws-lc-rs` one that `lapin`'s default features would otherwise select.
///
/// Installing a default can only ever succeed once; a second attempt (or an
/// attempt after another component installed its own) is not an error here,
/// it simply means somebody else already made the choice.
///
/// **Ordering matters**: whoever builds a `rustls::ClientConfig` first wins.
/// This crate installs the provider when a broker is constructed and again
/// before every connect, but an application that creates TLS clients through
/// other libraries earlier in startup should call this itself, first thing in
/// `main`.
pub fn install_pure_tls_provider() {
    TLS_PROVIDER_INIT.call_once(|| {
        // `CryptoProvider::install_default` consumes the provider by value.
        if oxitls::pure_provider()
            .as_ref()
            .clone()
            .install_default()
            .is_ok()
        {
            debug!("Installed Pure-Rust rustls crypto provider for AMQP TLS");
        } else {
            trace!("A rustls crypto provider was already installed for this process");
        }
    });
}

/// Build the [`AMQPUri`] used to connect, applying the configured heartbeat
/// and connection timeout.
///
/// Values that are already present in the URL query string win: an explicit
/// `?heartbeat=` in the broker URL is a deliberate per-URL override and is
/// never silently replaced by the configuration default.
///
/// # Errors
///
/// Returns [`BrokerError::Connection`] if `url` is not a valid AMQP URI.
pub(crate) fn build_uri(
    url: &str,
    heartbeat: u16,
    connection_timeout: Duration,
) -> Result<AMQPUri> {
    let mut uri = AMQPUri::from_str(url)
        .map_err(|e| BrokerError::Connection(format!("Invalid AMQP URL '{}': {}", url, e)))?;

    if uri.query.heartbeat.is_none() {
        // `0` is meaningful: it disables heartbeats entirely.
        uri.query.heartbeat = Some(heartbeat);
    }

    if uri.query.connection_timeout.is_none() && !connection_timeout.is_zero() {
        let millis = u64::try_from(connection_timeout.as_millis()).unwrap_or(u64::MAX);
        uri.query.connection_timeout = Some(millis);
    }

    Ok(uri)
}

/// Open a connection to the broker, bounded by `connection_timeout`.
///
/// A zero timeout means "wait indefinitely" (the OS TCP timeout applies).
///
/// # Errors
///
/// Returns [`BrokerError::Connection`] if the connection could not be
/// established within the timeout.
pub(crate) async fn open_connection(
    uri: &AMQPUri,
    connection_timeout: Duration,
) -> Result<Connection> {
    install_pure_tls_provider();

    let connect = Connection::connect_uri(uri.clone(), ConnectionProperties::default());

    if connection_timeout.is_zero() {
        return connect
            .await
            .map_err(|e| BrokerError::Connection(format!("Failed to connect: {}", e)));
    }

    match tokio::time::timeout(connection_timeout, connect).await {
        Ok(Ok(connection)) => Ok(connection),
        Ok(Err(e)) => Err(BrokerError::Connection(format!("Failed to connect: {}", e))),
        Err(_) => Err(BrokerError::Connection(format!(
            "Timed out after {:?} while connecting to the AMQP broker",
            connection_timeout
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn build_uri_applies_configured_heartbeat_and_timeout() {
        let uri =
            build_uri("amqp://localhost:5672/%2f", 17, Duration::from_secs(3)).expect("valid uri");
        assert_eq!(uri.query.heartbeat, Some(17));
        assert_eq!(uri.query.connection_timeout, Some(3_000));
    }

    #[test]
    fn build_uri_keeps_explicit_url_query_values() {
        let uri = build_uri(
            "amqp://localhost:5672/%2f?heartbeat=5&connection_timeout=250",
            60,
            Duration::from_secs(30),
        )
        .expect("valid uri");
        assert_eq!(uri.query.heartbeat, Some(5));
        assert_eq!(uri.query.connection_timeout, Some(250));
    }

    #[test]
    fn build_uri_propagates_disabled_heartbeat() {
        let uri = build_uri("amqp://localhost:5672", 0, Duration::ZERO).expect("valid uri");
        assert_eq!(uri.query.heartbeat, Some(0));
        assert_eq!(uri.query.connection_timeout, None);
    }

    #[test]
    fn build_uri_rejects_garbage() {
        assert!(build_uri("not a url", 60, Duration::from_secs(1)).is_err());
    }

    #[test]
    fn install_pure_tls_provider_is_idempotent() {
        install_pure_tls_provider();
        install_pure_tls_provider();
    }
}
