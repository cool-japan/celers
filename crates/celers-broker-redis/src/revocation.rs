//! Redis Pub/Sub bridge for task revocations.
//!
//! [`RedisBroker::cancel`](crate::RedisBroker) /
//! [`RedisBroker::revoke`](celers_core::Broker::revoke) do two things: they
//! record the revocation in the durable `<queue>:revoked` sorted set (scored by
//! its expiry) and they publish a
//! [`RevocationNotice`] on `<queue>:cancel`.
//! This module is the *reading* half of that channel: the subscription a worker
//! obtains through
//! [`Broker::subscribe_revocations`](celers_core::Broker::subscribe_revocations)
//! and feeds into its `RevocationPublisher`, so a task that is already running
//! can be aborted rather than only being refused at its next dispatch.
//!
//! The two halves cover different failures and neither is redundant:
//!
//! | | queued task | running task | worker restarted / disconnected |
//! |---|---|---|---|
//! | `<queue>:revoked` set | refused at dequeue | — | survives |
//! | `<queue>:cancel` Pub/Sub | — | aborted at once | missed (fire-and-forget) |
//!
//! # RESP3 is required
//!
//! Like [`crate::control`], the subscription rides RESP3 server pushes on a
//! multiplexed connection, so it needs Redis 6.0+. A [`crate::RedisBroker`]'s own
//! client speaks RESP2, which would connect happily and then deliver nothing;
//! `revocation_client` therefore re-opens the broker's connection info with
//! the protocol forced to RESP3.

use crate::control::{open_resp3, subscribe_push, RedisPushStream};

use celers_core::revocation_channel::{RevocationNotice, RevocationStream};
use celers_core::Result;

use redis::Client;

/// A live subscription to a Redis revocation channel.
///
/// Obtained from
/// [`Broker::subscribe_revocations`](celers_core::Broker::subscribe_revocations)
/// on a [`RedisBroker`](crate::RedisBroker); dropping it unsubscribes.
pub struct RedisRevocationStream {
    inner: RedisPushStream,
}

impl RedisRevocationStream {
    /// Subscribe to `channel` on an already-RESP3 `client`.
    ///
    /// # Errors
    ///
    /// Returns [`celers_core::CelersError::Broker`] if the subscription cannot
    /// be established.
    pub(crate) async fn connect(client: &Client, channel: &str) -> Result<Self> {
        Ok(Self {
            inner: subscribe_push(client, channel).await?,
        })
    }
}

impl std::fmt::Debug for RedisRevocationStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisRevocationStream")
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl RevocationStream for RedisRevocationStream {
    async fn recv(&mut self) -> Result<Option<RevocationNotice>> {
        match self.inner.recv_body().await? {
            // A malformed body is reported rather than skipped: a revocation
            // dropped in silence is a task that keeps running with nothing in
            // the log to explain why. The subscription stays usable.
            Some(body) => RevocationNotice::from_wire(&body).map(Some),
            None => Ok(None),
        }
    }
}

/// Re-open `client`'s connection info with RESP3 forced, for subscriptions.
///
/// # Errors
///
/// Returns [`celers_core::CelersError::Broker`] if the upgraded connection info
/// is rejected.
pub(crate) fn revocation_client(client: &Client) -> Result<Client> {
    open_resp3(client.get_connection_info().clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use redis::ProtocolVersion;

    #[test]
    fn revocation_client_upgrades_a_resp2_broker_client() {
        // The bug this guards against is silent: a RESP2 client subscribes
        // without error and then never delivers a single message.
        let resp2 = Client::open("redis://127.0.0.1:6379").expect("client");
        assert_eq!(
            resp2.get_connection_info().redis_settings().protocol(),
            ProtocolVersion::RESP2
        );

        let upgraded = revocation_client(&resp2).expect("upgrade");
        assert_eq!(
            upgraded.get_connection_info().redis_settings().protocol(),
            ProtocolVersion::RESP3
        );
    }

    #[test]
    fn revocation_client_preserves_credentials_and_database() {
        let client = Client::open("redis://user:secret@127.0.0.1:6379/3").expect("client");
        let upgraded = revocation_client(&client).expect("upgrade");
        let settings = upgraded.get_connection_info().redis_settings();
        assert_eq!(settings.username(), Some("user"));
        assert_eq!(settings.password(), Some("secret"));
        assert_eq!(settings.protocol(), ProtocolVersion::RESP3);
    }
}
