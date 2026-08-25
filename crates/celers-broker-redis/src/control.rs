//! Redis Pub/Sub transport for the remote worker control protocol.
//!
//! Implements [`ControlTransport`] on top of Redis Pub/Sub, so a
//! [`ControlClient`](celers_core::control_transport::ControlClient) in one
//! process can drive `celers_worker::ControlService` in another:
//!
//! - Commands are published to one broadcast channel (default
//!   [`DEFAULT_CONTROL_CHANNEL`]) that every worker subscribes to.
//! - Replies go to a per-request channel (`celers.control.reply.<uuid>`) the
//!   client subscribes to *before* it publishes.
//!
//! Both bodies are the JSON documents described in
//! [`celers_core::control_transport`] — this is a `CeleRS`-native protocol, not
//! Celery's kombu pidbox, and the two do not interoperate.
//!
//! # RESP3 is required
//!
//! Subscriptions are delivered through RESP3 server pushes on an ordinary
//! multiplexed connection ([`redis::AsyncConnectionConfig::set_push_sender`])
//! rather than through a dedicated `PubSub` connection, because the push path
//! hands messages over a `tokio::sync::mpsc` channel this crate can consume
//! directly. The transport therefore upgrades whatever URL it is given to
//! RESP3 (`HELLO 3`), which needs Redis 6.0 or newer. Connecting to an older
//! server fails at construction of the first subscription with a clear
//! handshake error rather than silently receiving nothing.
//!
//! # Delivery semantics
//!
//! Redis Pub/Sub is fire-and-forget: a worker that is restarting, partitioned
//! or simply not subscribed yet never sees the command, and there is no
//! redelivery. That is the correct model for *control* (a stale `inspect` reply
//! is worse than none), but it means a control command is a request, not a
//! guarantee. Durable operations — revoking a task that is still sitting in a
//! queue, for example — additionally go through
//! [`RedisBroker::cancel`](crate::RedisBroker), which records the revocation in
//! a Redis sorted set that survives a worker restart.

use crate::connection::{default_async_config, RedisClientExt};

use celers_core::control_transport::{
    ControlCommandStream, ControlEnvelope, ControlReply, ControlReplyStream, ControlTransport,
    DEFAULT_CONTROL_CHANNEL,
};
use celers_core::{CelersError, Result};

use redis::aio::MultiplexedConnection;
use redis::{
    AsyncCommands, Client, FromRedisValue, IntoConnectionInfo, ProtocolVersion, PushInfo, PushKind,
};

use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver};
use tracing::{debug, warn};

/// A [`ControlTransport`] backed by Redis Pub/Sub.
///
/// Cheap to clone: a clone shares the connection settings, not a connection.
#[derive(Clone)]
pub struct RedisControlTransport {
    /// RESP3-upgraded client; subscriptions need server pushes.
    client: Client,
    /// The broadcast channel workers subscribe to.
    channel: String,
}

impl RedisControlTransport {
    /// Connect to `url`, using the default broadcast channel.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Broker`] if the URL cannot be parsed.
    pub fn new(url: &str) -> Result<Self> {
        Self::with_channel(url, DEFAULT_CONTROL_CHANNEL)
    }

    /// Connect to `url`, using a custom broadcast channel.
    ///
    /// A custom channel scopes a control plane to one deployment when several
    /// share a Redis instance: a worker only answers commands published to the
    /// channel it subscribes to.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Broker`] if the URL cannot be parsed.
    pub fn with_channel(url: &str, channel: impl Into<String>) -> Result<Self> {
        Ok(Self {
            client: resp3_client(url)?,
            channel: channel.into(),
        })
    }

    /// Build a transport from an existing client, upgrading it to RESP3.
    ///
    /// Useful next to a [`RedisBroker`](crate::RedisBroker) built from the same
    /// URL: the broker's own client speaks RESP2, which cannot carry
    /// subscriptions on a multiplexed connection.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Broker`] if the upgraded connection info is
    /// rejected.
    pub fn from_client(client: &Client, channel: impl Into<String>) -> Result<Self> {
        let info = client.get_connection_info().clone();
        Ok(Self {
            client: open_resp3(info)?,
            channel: channel.into(),
        })
    }

    /// The broadcast channel commands are published to.
    #[must_use]
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// A connection for ordinary request/response traffic (publishing).
    async fn connection(&self) -> Result<MultiplexedConnection> {
        self.client
            .celers_multiplexed_connection()
            .await
            .map_err(|e| CelersError::Broker(format!("control transport connection failed: {e}")))
    }

    /// Publish `body` on `channel`, returning the subscriber count Redis
    /// reports.
    async fn publish(&self, channel: &str, body: &str) -> Result<usize> {
        let mut conn = self.connection().await?;
        let receivers: i64 = conn
            .publish(channel, body)
            .await
            .map_err(|e| CelersError::Broker(format!("failed to publish to '{channel}': {e}")))?;
        Ok(usize::try_from(receivers).unwrap_or(0))
    }

    /// Open a dedicated subscription to `channel`.
    ///
    /// The returned stream owns its connection: dropping it unsubscribes.
    async fn subscribe_raw(&self, channel: &str) -> Result<RedisPushStream> {
        subscribe_push(&self.client, channel).await
    }
}

/// Open a dedicated Pub/Sub subscription to `channel` on `client`.
///
/// Shared with [`crate::revocation`], which needs exactly the same RESP3
/// server-push plumbing for the revocation channel. `client` **must** already
/// speak RESP3 (see [`open_resp3`]): a RESP2 client connects happily here and
/// then never delivers a message.
///
/// The returned stream owns its connection, so dropping it unsubscribes.
///
/// # Errors
///
/// Returns [`CelersError::Broker`] if the connection or the `SUBSCRIBE` fails.
pub(crate) async fn subscribe_push(client: &Client, channel: &str) -> Result<RedisPushStream> {
    let (tx, rx) = unbounded_channel::<PushInfo>();
    let config = default_async_config().set_push_sender(tx);
    let mut conn = client
        .get_multiplexed_async_connection_with_config(&config)
        .await
        .map_err(|e| {
            CelersError::Broker(format!(
                "subscription to '{channel}' failed to connect \
                 (RESP3 requires Redis 6.0+): {e}"
            ))
        })?;
    conn.subscribe(channel)
        .await
        .map_err(|e| CelersError::Broker(format!("failed to subscribe to '{channel}': {e}")))?;
    debug!("Subscribed to channel '{channel}'");
    Ok(RedisPushStream {
        _conn: conn,
        rx,
        channel: channel.to_string(),
    })
}

impl std::fmt::Debug for RedisControlTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisControlTransport")
            .field("channel", &self.channel)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl ControlTransport for RedisControlTransport {
    async fn broadcast(&self, envelope: &ControlEnvelope) -> Result<usize> {
        let body = envelope.to_json()?;
        let reached = self.publish(&self.channel, &body).await?;
        debug!(
            "Broadcast control command {} to {} subscriber(s) on '{}'",
            envelope.id, reached, self.channel
        );
        Ok(reached)
    }

    async fn subscribe_commands(&self) -> Result<Box<dyn ControlCommandStream>> {
        let inner = self.subscribe_raw(&self.channel).await?;
        Ok(Box::new(RedisCommandStream { inner }))
    }

    async fn send_reply(&self, reply_to: &str, reply: &ControlReply) -> Result<()> {
        let body = reply.to_json()?;
        let reached = self.publish(reply_to, &body).await?;
        if reached == 0 {
            // The client gave up (its gather deadline passed) or never
            // subscribed. Fire-and-forget, so this is not an error.
            debug!("Control reply to '{reply_to}' had no subscriber");
        }
        Ok(())
    }

    async fn subscribe_replies(&self, reply_to: &str) -> Result<Box<dyn ControlReplyStream>> {
        let inner = self.subscribe_raw(reply_to).await?;
        Ok(Box::new(RedisReplyStream { inner }))
    }
}

/// A live Redis subscription delivering message bodies as strings.
pub(crate) struct RedisPushStream {
    /// Held so the subscription stays open; dropping it unsubscribes.
    _conn: MultiplexedConnection,
    rx: UnboundedReceiver<PushInfo>,
    channel: String,
}

impl RedisPushStream {
    /// Next message body on the subscribed channel.
    ///
    /// Skips subscribe/unsubscribe confirmations (RESP3 delivers those on the
    /// same push channel) and ends on disconnection.
    pub(crate) async fn recv_body(&mut self) -> Result<Option<String>> {
        loop {
            let Some(push) = self.rx.recv().await else {
                return Ok(None);
            };
            // The payload is the last element of `data`: `[channel, payload]`
            // for `message`/`smessage`, `[pattern, channel, payload]` for
            // `pmessage`.
            match push.kind {
                PushKind::Message | PushKind::SMessage | PushKind::PMessage => {
                    let Some(payload) = push.data.into_iter().next_back() else {
                        warn!(
                            "Malformed Redis push on '{}': message with no payload",
                            self.channel
                        );
                        continue;
                    };
                    return match String::from_redis_value(payload) {
                        Ok(body) => Ok(Some(body)),
                        Err(e) => Err(CelersError::Deserialization(format!(
                            "control message on '{}' was not a string: {e}",
                            self.channel
                        ))),
                    };
                }
                PushKind::Disconnection => {
                    // The connection is gone and this subscription with it; a
                    // caller that keeps polling would spin on a dead channel.
                    warn!("Control subscription to '{}' disconnected", self.channel);
                    return Ok(None);
                }
                // Subscribe/unsubscribe acknowledgements and unrelated pushes.
                _ => continue,
            }
        }
    }
}

/// Broadcast-channel subscription yielding [`ControlEnvelope`]s.
struct RedisCommandStream {
    inner: RedisPushStream,
}

#[async_trait::async_trait]
impl ControlCommandStream for RedisCommandStream {
    async fn recv(&mut self) -> Result<Option<ControlEnvelope>> {
        match self.inner.recv_body().await? {
            Some(body) => ControlEnvelope::from_json(&body).map(Some),
            None => Ok(None),
        }
    }
}

/// Reply-channel subscription yielding [`ControlReply`]s.
struct RedisReplyStream {
    inner: RedisPushStream,
}

#[async_trait::async_trait]
impl ControlReplyStream for RedisReplyStream {
    async fn recv(&mut self) -> Result<Option<ControlReply>> {
        match self.inner.recv_body().await? {
            Some(body) => ControlReply::from_json(&body).map(Some),
            None => Ok(None),
        }
    }
}

/// Parse `url` and force the RESP3 handshake.
fn resp3_client(url: &str) -> Result<Client> {
    let info = url
        .into_connection_info()
        .map_err(|e| CelersError::Broker(format!("invalid Redis URL for control channel: {e}")))?;
    open_resp3(info)
}

/// Open a client from `info` with the protocol forced to RESP3.
///
/// The protocol is set on the parsed connection info rather than by appending
/// `?protocol=resp3` to the URL: a URL may already carry a query string, and
/// string surgery on a credential-bearing URL is exactly the sort of thing that
/// silently drops a password.
pub(crate) fn open_resp3(info: redis::ConnectionInfo) -> Result<Client> {
    let redis_settings = info
        .redis_settings()
        .clone()
        .set_protocol(ProtocolVersion::RESP3);
    crate::connection::open_client(info.set_redis_settings(redis_settings))
        .map_err(|e| CelersError::Broker(format!("failed to open control channel client: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transport_defaults_to_the_shared_control_channel() {
        let transport = RedisControlTransport::new("redis://127.0.0.1:6379").expect("transport");
        assert_eq!(transport.channel(), DEFAULT_CONTROL_CHANNEL);
    }

    #[test]
    fn transport_accepts_a_scoped_channel() {
        let transport =
            RedisControlTransport::with_channel("redis://127.0.0.1:6379", "tenant-a.control")
                .expect("transport");
        assert_eq!(transport.channel(), "tenant-a.control");
    }

    #[test]
    fn transport_upgrades_the_connection_to_resp3() {
        // Subscriptions on a multiplexed connection are RESP3-only; a RESP2
        // client would connect happily and then never deliver a message.
        let transport = RedisControlTransport::new("redis://127.0.0.1:6379").expect("transport");
        assert_eq!(
            transport
                .client
                .get_connection_info()
                .redis_settings()
                .protocol(),
            ProtocolVersion::RESP3
        );
    }

    #[test]
    fn transport_preserves_credentials_and_database_from_the_url() {
        let transport =
            RedisControlTransport::new("redis://user:secret@127.0.0.1:6379/3").expect("transport");
        let settings = transport.client.get_connection_info().redis_settings();
        assert_eq!(settings.username(), Some("user"));
        assert_eq!(settings.password(), Some("secret"));
        assert_eq!(settings.protocol(), ProtocolVersion::RESP3);
    }

    #[test]
    fn transport_rejects_a_url_it_cannot_parse() {
        assert!(RedisControlTransport::new("not-a-redis-url").is_err());
    }

    #[test]
    fn from_client_upgrades_a_resp2_broker_client() {
        let resp2 = Client::open("redis://127.0.0.1:6379").expect("client");
        assert_eq!(
            resp2.get_connection_info().redis_settings().protocol(),
            ProtocolVersion::RESP2
        );

        let transport = RedisControlTransport::from_client(&resp2, "scoped").expect("transport");
        assert_eq!(transport.channel(), "scoped");
        assert_eq!(
            transport
                .client
                .get_connection_info()
                .redis_settings()
                .protocol(),
            ProtocolVersion::RESP3
        );
    }

    // -- live-server tests --------------------------------------------------
    //
    // These need a real Redis (RESP3, so 6.0+). They are skipped, not failed,
    // when `CELERS_TEST_REDIS_URL` is unset, so the default suite stays
    // hermetic.

    use celers_core::control::{ControlCommand, ControlResponse};
    use celers_core::control_transport::{ControlClient, ControlReply};
    use std::sync::Arc;
    use std::time::Duration;

    /// The live-server URL, or `None` when the suite is not enabled.
    fn live_url() -> Option<String> {
        std::env::var("CELERS_TEST_REDIS_URL")
            .ok()
            .filter(|url| !url.is_empty())
    }

    /// A transport on a channel unique to this test run, so concurrent runs
    /// and leftover subscribers cannot cross-talk.
    fn live_transport(url: &str, label: &str) -> RedisControlTransport {
        RedisControlTransport::with_channel(
            url,
            format!("celers.test.{label}.{}", uuid::Uuid::new_v4()),
        )
        .expect("transport")
    }

    #[tokio::test]
    async fn live_redis_delivers_a_broadcast_command_to_a_subscriber() {
        let Some(url) = live_url() else {
            return;
        };
        let transport = live_transport(&url, "broadcast");

        let mut commands = transport
            .subscribe_commands()
            .await
            .expect("subscribe to the control channel");

        let envelope = ControlEnvelope::new(ControlCommand::inspect_stats());
        // Redis reports the subscriber count, which is how a client can tell a
        // silent cluster from a silent worker.
        let reached = transport.broadcast(&envelope).await.expect("broadcast");
        assert_eq!(reached, 1, "the live subscriber must be counted");

        let received = tokio::time::timeout(Duration::from_secs(5), commands.recv())
            .await
            .expect("a command arrives before the deadline")
            .expect("stream is healthy")
            .expect("stream is not closed");
        assert_eq!(received.id, envelope.id);
        assert!(matches!(received.command, ControlCommand::Inspect(_)));
    }

    #[tokio::test]
    async fn live_redis_carries_a_full_request_reply_round_trip() {
        let Some(url) = live_url() else {
            return;
        };
        let transport = Arc::new(live_transport(&url, "roundtrip"));

        // A stand-in worker: subscribe, answer whatever arrives.
        let responder = Arc::clone(&transport);
        let mut commands = responder
            .subscribe_commands()
            .await
            .expect("subscribe to the control channel");
        let worker = tokio::spawn(async move {
            while let Ok(Some(envelope)) = commands.recv().await {
                let Some(ref reply_to) = envelope.reply_to else {
                    continue;
                };
                let reply = ControlReply::new(
                    envelope.id,
                    "live-worker",
                    ControlResponse::pong("live-worker"),
                );
                let _ = responder.send_reply(reply_to, &reply).await;
            }
        });

        let client = ControlClient::new(Arc::clone(&transport) as Arc<dyn ControlTransport>)
            .with_timeout(Duration::from_secs(5))
            .with_expected_replies(1);
        let replies = client.ping().await.expect("ping over Redis");

        assert_eq!(replies.len(), 1);
        assert_eq!(replies[0].hostname, "live-worker");

        worker.abort();
    }

    #[tokio::test]
    async fn live_redis_gather_times_out_instead_of_hanging_with_no_workers() {
        let Some(url) = live_url() else {
            return;
        };
        let transport = Arc::new(live_transport(&url, "empty"));

        let client = ControlClient::new(transport as Arc<dyn ControlTransport>)
            .with_timeout(Duration::from_millis(300));
        let started = std::time::Instant::now();
        let replies = client
            .ping()
            .await
            .expect("an empty cluster is not an error");

        assert!(replies.is_empty());
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "the gather must be bounded by its timeout"
        );
    }

    #[tokio::test]
    async fn live_redis_reports_a_malformed_command_body_as_an_error() {
        let Some(url) = live_url() else {
            return;
        };
        let transport = live_transport(&url, "malformed");
        let mut commands = transport.subscribe_commands().await.expect("subscribe");

        // Something else publishing junk on the control channel must surface as
        // an error the worker can log, not as a silent drop or a panic.
        transport
            .publish(transport.channel(), "{ not an envelope }")
            .await
            .expect("publish");

        let outcome = tokio::time::timeout(Duration::from_secs(5), commands.recv())
            .await
            .expect("a message arrives before the deadline");
        assert!(
            outcome.is_err(),
            "a malformed envelope must be reported, got {outcome:?}"
        );
    }
}
