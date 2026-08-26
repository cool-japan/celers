// Copyright (c) 2026 COOLJAPAN OU (Team KitaSan)
// SPDX-License-Identifier: Apache-2.0

//! Pure-Rust TLS provider installation for this crate's `rediss://` clients.
//!
//! # Why every crate that opens a `redis::Client` needs this
//!
//! The workspace declares `rustls` with `default-features = false`, so **no**
//! rustls crypto-provider feature is enabled anywhere in this build. The
//! `redis` crate's `create_rustls_config` goes through the bare
//! `rustls::ClientConfig::builder()`, which resolves a provider by looking at
//! (a) the process default, then (b) the single enabled provider feature. With
//! no provider feature, a missing process default is a **panic** — and it
//! happens lazily, at connect time, far from the code that opened the client.
//!
//! This crate opens its own clients (worker coordination, distributed rate
//! limiting, DLQ storage) rather than borrowing `celers-broker-redis`'s, and it
//! does not depend on that crate, so it carries its own copy of the guard.
//! `celers-backend-redis`, `celers-beat` and `celers-broker-amqp` each carry
//! one for the same reason. They are independent by construction: installing a
//! process default is idempotent, and whoever gets there first wins.

use std::sync::Once;

use redis::{Client, IntoConnectionInfo};
use tracing::{debug, trace};

static TLS_PROVIDER_INIT: Once = Once::new();

/// Install the Pure-Rust (`rustls-rustcrypto`) crypto provider as the process
/// default, once per process.
///
/// See the module documentation for why this is mandatory rather than an
/// optimisation.
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
/// crate uses.
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

    /// A `rediss://` URL must resolve to a TLS address rather than being
    /// rejected — the workspace enables the `redis` crate's TLS features, and
    /// losing them would silently turn every `rediss://` deployment into a
    /// startup error.
    #[test]
    fn rediss_urls_resolve_to_a_tls_address() {
        let client = open_client("rediss://example.com:6379").expect("rediss:// must parse");
        assert!(matches!(
            client.get_connection_info().addr(),
            ConnectionAddr::TcpTls { .. }
        ));
    }

    /// Ordinary URLs must behave exactly as `redis::Client::open` did before
    /// the wrapper existed.
    #[test]
    fn plaintext_urls_are_unchanged() {
        let client = open_client("redis://example.com:6379/5").expect("client");
        assert_eq!(client.get_connection_info().redis_settings().db(), 5);
        assert!(open_client("invalid://bad-url").is_err());
    }
}
