//! [`RedisScheduleStore`]: a [`ScheduleStore`] shared across beat instances.
//!
//! Behind the `redis-store` feature (off by default — see this crate's
//! `Cargo.toml`), so a deployment that never wants a Redis dependency does
//! not get one.
//!
//! The whole scheduler state is one JSON blob (see this module's parent doc),
//! so the store is intentionally simple: one Redis string key, written with
//! `SET` and read with `GET`. There is no compare-and-swap and no
//! server-side merge — see `schedule_store.rs`'s "Leader election" section
//! for exactly what that does and does not guarantee when more than one beat
//! instance writes to the same key.

use async_trait::async_trait;
use redis::{aio::ConnectionManager, AsyncCommands, Client};
use std::sync::Once;
use std::time::Duration;

use super::ScheduleStore;
use crate::config::ScheduleError;

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
/// no provider feature, a missing process default is a **panic**, and it
/// happens lazily at connect time — so a `rediss://` schedule store would
/// abort the beat process instead of returning a
/// [`ScheduleError::Persistence`].
///
/// Duplicated here rather than taken from `celers-broker-redis` for the same
/// reason [`RESPONSE_TIMEOUT`] is: a scheduler crate has no other business
/// depending on a broker crate. `celers-broker-redis`, `celers-backend-redis`,
/// `celers-worker` and `celers-broker-amqp` each carry their own copy;
/// installing a process default is idempotent and whoever gets there first
/// wins.
///
/// **Ordering matters**: an application that creates TLS clients through other
/// libraries earlier in startup should install a provider itself, first thing
/// in `main`.
fn install_pure_tls_provider() {
    TLS_PROVIDER_INIT.call_once(|| {
        // `CryptoProvider::install_default` consumes the provider by value.
        if oxitls::pure_provider()
            .as_ref()
            .clone()
            .install_default()
            .is_ok()
        {
            tracing::debug!("Installed Pure-Rust rustls crypto provider for Redis TLS");
        } else {
            tracing::trace!("A rustls crypto provider was already installed for this process");
        }
    });
}

/// How long a `GET`/`SET`/`DEL` on this store's key may take before the
/// *client* gives up on it.
///
/// The `redis` crate defaults `ConnectionManagerConfig::new()` (what
/// [`Client::get_connection_manager`] uses) to **500 ms** — a budget an idle
/// laptop clears easily and a loaded machine does not: under CPU saturation
/// even one round trip can miss it, and the failure surfaces as a bare
/// `"timed out"` error indistinguishable from a genuinely broken Redis. This
/// is the exact root cause `celers-broker-redis::connection` documents and
/// fixes for its own connections (`DEFAULT_RESPONSE_TIMEOUT`); duplicated
/// here (rather than depending on that broker crate from a scheduler crate,
/// which has no other reason to know about it) so a scheduler under load
/// does not spuriously fail to persist its state and count that as a real
/// [`ScheduleError::Persistence`].
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(30);

/// How long establishing the connection (TCP connect, TLS handshake, `AUTH`,
/// `SELECT`) may take. The `redis` crate default is 1 second, which the same
/// saturated machine can miss on the handshake alone — see
/// [`RESPONSE_TIMEOUT`].
const CONNECTION_TIMEOUT: Duration = Duration::from_secs(10);

/// A [`ScheduleStore`] backed by a single Redis string key.
///
/// Connects through an auto-reconnecting
/// [`redis::aio::ConnectionManager`] — a transient Redis outage surfaces as
/// an `Err` from [`load`](ScheduleStore::load)/[`save`](ScheduleStore::save)
/// rather than requiring the caller to reconnect by hand.
#[derive(Clone)]
pub struct RedisScheduleStore {
    conn: ConnectionManager,
    key: String,
}

impl std::fmt::Debug for RedisScheduleStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisScheduleStore")
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl RedisScheduleStore {
    /// Connect to `redis_url` and store state under `key`.
    ///
    /// # Errors
    ///
    /// Returns [`ScheduleError::Persistence`] if `redis_url` is malformed or
    /// the initial connection cannot be established.
    pub async fn connect(redis_url: &str, key: impl Into<String>) -> Result<Self, ScheduleError> {
        install_pure_tls_provider();

        let client = Client::open(redis_url)
            .map_err(|e| ScheduleError::Persistence(format!("Invalid Redis URL: {e}")))?;
        let config = redis::aio::ConnectionManagerConfig::new()
            .set_response_timeout(Some(RESPONSE_TIMEOUT))
            .set_connection_timeout(Some(CONNECTION_TIMEOUT));
        let conn = client
            .get_connection_manager_with_config(config)
            .await
            .map_err(|e| ScheduleError::Persistence(format!("Failed to connect to Redis: {e}")))?;
        Ok(Self {
            conn,
            key: key.into(),
        })
    }

    /// The Redis key this store reads from and writes to.
    pub fn key(&self) -> &str {
        &self.key
    }
}

#[async_trait]
impl ScheduleStore for RedisScheduleStore {
    async fn load(&self) -> Result<Option<Vec<u8>>, ScheduleError> {
        let mut conn = self.conn.clone();
        let value: Option<Vec<u8>> = conn
            .get(&self.key)
            .await
            .map_err(|e| ScheduleError::Persistence(format!("Redis GET failed: {e}")))?;
        Ok(value)
    }

    async fn save(&self, bytes: &[u8]) -> Result<(), ScheduleError> {
        let mut conn = self.conn.clone();
        conn.set::<_, _, ()>(&self.key, bytes)
            .await
            .map_err(|e| ScheduleError::Persistence(format!("Redis SET failed: {e}")))
    }

    async fn remove(&self) -> Result<(), ScheduleError> {
        let mut conn = self.conn.clone();
        conn.del::<_, ()>(&self.key)
            .await
            .map_err(|e| ScheduleError::Persistence(format!("Redis DEL failed: {e}")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every test in this module needs a real Redis; without one they print
    /// one visible `SKIPPED:` line and return, rather than silently passing
    /// having verified nothing (see this crate's env-gated test convention,
    /// shared with `celers-broker-redis` and friends).
    async fn store_or_skip(test_name: &str, key_suffix: &str) -> Option<RedisScheduleStore> {
        let url = match std::env::var("CELERS_TEST_REDIS_URL") {
            Ok(url) if !url.trim().is_empty() => url,
            _ => {
                eprintln!("SKIPPED: {test_name} (set CELERS_TEST_REDIS_URL to run)");
                return None;
            }
        };
        let key = format!(
            "celers_beat_schedule_store_test_{}_{}",
            key_suffix,
            uuid::Uuid::new_v4().simple()
        );
        Some(
            RedisScheduleStore::connect(&url, key)
                .await
                .expect("connecting to CELERS_TEST_REDIS_URL should succeed"),
        )
    }

    #[tokio::test]
    async fn load_is_none_when_nothing_was_ever_saved() {
        let Some(store) = store_or_skip("load_is_none_when_nothing_was_ever_saved", "empty").await
        else {
            return;
        };
        assert!(store.load().await.expect("load").is_none());
        store.remove().await.expect("cleanup");
    }

    #[tokio::test]
    async fn save_then_load_round_trips_the_exact_bytes() {
        let Some(store) =
            store_or_skip("save_then_load_round_trips_the_exact_bytes", "round_trip").await
        else {
            return;
        };

        store.save(b"{\"tasks\":{}}").await.expect("save");
        let loaded = store.load().await.expect("load").expect("bytes present");
        assert_eq!(loaded, b"{\"tasks\":{}}");

        store.remove().await.expect("cleanup");
    }

    #[tokio::test]
    async fn a_second_save_replaces_the_first() {
        let Some(store) = store_or_skip("a_second_save_replaces_the_first", "overwrite").await
        else {
            return;
        };

        store.save(b"{\"v\":1}").await.expect("first save");
        store.save(b"{\"v\":2}").await.expect("second save");
        let loaded = store.load().await.expect("load").expect("bytes present");
        assert_eq!(loaded, b"{\"v\":2}");

        store.remove().await.expect("cleanup");
    }

    #[tokio::test]
    async fn remove_makes_a_subsequent_load_return_none() {
        let Some(store) =
            store_or_skip("remove_makes_a_subsequent_load_return_none", "remove").await
        else {
            return;
        };

        store.save(b"{\"v\":1}").await.expect("save");
        store.remove().await.expect("remove");
        assert!(store.load().await.expect("load").is_none());
    }

    #[tokio::test]
    async fn two_stores_on_the_same_key_share_state() {
        // The point of a shared store: a second `RedisScheduleStore` pointed
        // at the same key (a stand-in for a second beat instance) sees the
        // first one's write.
        let Some(store_a) = store_or_skip("two_stores_on_the_same_key_share_state", "shared").await
        else {
            return;
        };
        let url = std::env::var("CELERS_TEST_REDIS_URL").expect("checked by store_or_skip");
        let store_b = RedisScheduleStore::connect(&url, store_a.key().to_string())
            .await
            .expect("second connection");

        store_a.save(b"{\"from\":\"a\"}").await.expect("save");
        let loaded = store_b.load().await.expect("load").expect("bytes present");
        assert_eq!(loaded, b"{\"from\":\"a\"}");

        store_a.remove().await.expect("cleanup");
    }
}
