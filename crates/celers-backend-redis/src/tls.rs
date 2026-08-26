// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: Apache-2.0

//! Pure-Rust TLS provider installation for `rediss://` connections.
//!
//! This crate opens its own `redis::Client` values (for results, locks and the
//! event transport) rather than borrowing `celers-broker-redis`'s, so it needs
//! its own copy of the provider guard. The two are independent by design: a
//! result backend is usable without a broker.

use std::sync::Once;

use redis::{Client, IntoConnectionInfo};
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
/// no provider feature, a missing process default is a **panic**, not an error
/// — and it happens lazily, at connect time, far from the code that opened the
/// client.
///
/// Every `redis::Client` this crate constructs therefore goes through
/// [`open_client`], which calls this first.
///
/// Installing a default can only succeed once. A second attempt — or an attempt
/// after another component (`celers-broker-redis`, `celers-broker-amqp`, the
/// application itself) installed its own — is not an error here; it just means
/// somebody else already made the choice.
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
/// Custom CA and client-certificate material is configured through
/// `celers_broker_redis::RedisConfig`, which routes it to
/// `redis::Client::build_with_tls`; a client built here uses the default
/// (webpki) trust store.
///
/// # Errors
///
/// Returns whatever [`redis::Client::open`] returns for an unusable URL.
pub fn open_client<T: IntoConnectionInfo>(params: T) -> redis::RedisResult<Client> {
    install_pure_tls_provider();
    Client::open(params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::ConnectionAddr;

    /// Every constructor in this crate calls it, so it runs many times per
    /// process and must be safe to repeat.
    #[test]
    fn install_pure_tls_provider_is_idempotent() {
        install_pure_tls_provider();
        install_pure_tls_provider();
    }

    /// The `redis` crate must actually have TLS compiled in. Without the
    /// workspace's `tokio-rustls-comp` feature a `rediss://` URL does not even
    /// parse, and the whole TLS story silently disappears.
    #[test]
    fn rediss_urls_resolve_to_a_tls_address() {
        let client = open_client("rediss://example.com:6379").expect("rediss:// must parse");
        assert!(matches!(
            client.get_connection_info().addr(),
            ConnectionAddr::TcpTls { .. }
        ));
    }

    /// Ordinary URLs must behave exactly as `redis::Client::open` did before
    /// the wrapper existed — same parsing, same errors.
    #[test]
    fn plaintext_urls_are_unchanged() {
        let client = open_client("redis://example.com:6379/9").expect("client");
        assert_eq!(client.get_connection_info().redis_settings().db(), 9);
        assert!(matches!(
            client.get_connection_info().addr(),
            ConnectionAddr::Tcp(..)
        ));

        assert!(open_client("invalid://bad-url").is_err());
    }

    /// A `rediss://` URL must get as far as the TLS handshake.
    ///
    /// This is the only test that can observe the failure
    /// [`install_pure_tls_provider`] exists to prevent: the `rustls`
    /// `ClientConfig` is built lazily, at connect time, and without a process
    /// default provider that construction **panics** rather than returning an
    /// error. It therefore needs a real socket.
    ///
    /// The local Redis speaks no TLS, so the handshake is expected to fail.
    /// What matters is that the plaintext control connection succeeds (the
    /// server is up) while the `rediss://` one comes back as an ordinary
    /// transport error — not a panic, and not a silent plaintext connection.
    #[tokio::test]
    async fn live_rediss_url_reaches_the_tls_handshake() {
        let Some(url) = std::env::var("CELERS_TEST_REDIS_URL")
            .ok()
            .filter(|url| !url.is_empty())
        else {
            eprintln!(
                "SKIPPED: live_rediss_url_reaches_the_tls_handshake \
                 (set CELERS_TEST_REDIS_URL to run)"
            );
            return;
        };

        // Control: without this, a dead server would make the TLS assertion
        // below pass for the wrong reason.
        open_client(url.as_str())
            .expect("plaintext client")
            .get_multiplexed_async_connection()
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

        // A short budget on purpose: a plaintext Redis reads the ClientHello as
        // an inline command and keeps buffering, so the handshake stalls rather
        // than failing fast.
        let probe = redis::AsyncConnectionConfig::new()
            .set_connection_timeout(Some(std::time::Duration::from_secs(3)))
            .set_response_timeout(Some(std::time::Duration::from_secs(3)));

        // The panic this guards against would abort the test process here.
        let error = tls_client
            .get_multiplexed_async_connection_with_config(&probe)
            .await
            .expect_err("a plaintext Redis must not complete a TLS handshake");

        assert_ne!(
            error.kind(),
            redis::ErrorKind::InvalidClientConfig,
            "the failure must come from the handshake, not from an unusable TLS config: {error}"
        );
    }
}
