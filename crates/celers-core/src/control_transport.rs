//! Transport and client for the remote worker control protocol.
//!
//! [`control`](crate::control) defines *what* an operator can ask a worker
//! (`ControlCommand`) and what a worker answers (`ControlResponse`). This
//! module defines *how* those messages travel and who gathers the answers:
//!
//! - [`ControlEnvelope`] / [`ControlReply`] — the framing actually put on the
//!   wire (a bare `ControlCommand` carries no correlation id and no reply
//!   address, so it cannot be answered).
//! - [`ControlTransport`] — a broadcast channel plus a per-request reply
//!   channel, implemented here by [`InMemoryControlTransport`] and, for a real
//!   deployment, by `celers_broker_redis::RedisControlTransport`.
//! - [`ControlClient`] — broadcasts a command and gathers every reply that
//!   arrives before a deadline.
//!
//! The worker side (the thing that subscribes, dispatches and replies) lives in
//! `celers_worker::control`.
//!
//! # Wire format and Celery interoperability
//!
//! **This is a `CeleRS`-native protocol, not Celery's.** Envelopes are JSON
//! documents whose `command` field is a `ControlCommand` serialised with
//! `#[serde(tag = "type", content = "args")]` and PascalCase variant names:
//!
//! ```json
//! {
//!   "id": "1f0c…",
//!   "command": { "type": "Inspect", "args": { "method": "Active" } },
//!   "reply_to": "celers.control.reply.1f0c…",
//!   "timestamp": 1750000000.0
//! }
//! ```
//!
//! Celery's own remote control uses a kombu *broadcast* exchange (a fanout
//! exchange named `celery` / `celeryev`, one auto-delete queue per worker) and
//! a completely different body (`{"method": ..., "arguments": {...},
//! "destination": [...]}`), with replies going to a `reply.celery.pidbox`
//! exchange. None of that is implemented here, so **`celery -A app inspect
//! active` and `celery -A app control shutdown` cannot drive a `CeleRS`
//! worker, and `celers` cannot drive a Python Celery worker.** Interop would
//! need a kombu-pidbox codec on top of this transport; the transport itself is
//! shaped to allow that later (broadcast + per-request reply channel is exactly
//! the pidbox model), but the codec does not exist yet.
//!
//! # Example
//!
//! ```
//! use celers_core::control::ControlCommand;
//! use celers_core::control_transport::{ControlClient, InMemoryControlTransport};
//! use std::sync::Arc;
//! use std::time::Duration;
//!
//! # async fn example() -> celers_core::Result<()> {
//! let transport = Arc::new(InMemoryControlTransport::new());
//! let client = ControlClient::new(transport).with_timeout(Duration::from_millis(50));
//!
//! // No worker is listening, so the gather returns empty rather than hanging.
//! let replies = client.broadcast(ControlCommand::inspect_active()).await?;
//! assert!(replies.is_empty());
//! # Ok(())
//! # }
//! # tokio::runtime::Builder::new_current_thread()
//! #     .enable_all()
//! #     .build()
//! #     .unwrap()
//! #     .block_on(example())
//! #     .unwrap();
//! ```

use crate::control::{ControlCommand, ControlResponse, InspectCommand, InspectResponse};
use crate::{CelersError, Result};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::broadcast;
use uuid::Uuid;

/// The channel every worker subscribes to for broadcast control commands.
pub const DEFAULT_CONTROL_CHANNEL: &str = "celers.control";

/// Prefix for the per-request reply channel a client subscribes to.
pub const DEFAULT_REPLY_CHANNEL_PREFIX: &str = "celers.control.reply";

/// How long [`ControlClient`] waits for replies when no timeout is set.
pub const DEFAULT_GATHER_TIMEOUT: Duration = Duration::from_secs(2);

/// Buffer depth of the in-memory broadcast channels.
///
/// Deep enough that a worker briefly busy dispatching one command does not miss
/// the next; a lagging subscriber skips the overflow rather than blocking the
/// publisher (that is `tokio::sync::broadcast`'s contract).
const CHANNEL_CAPACITY: usize = 256;

/// The reply channel name a request with id `id` should use.
#[must_use]
pub fn reply_channel(id: Uuid) -> String {
    format!("{DEFAULT_REPLY_CHANNEL_PREFIX}.{id}")
}

/// Current wall-clock time as fractional seconds since the Unix epoch.
fn now_secs() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// A control command as it travels on the wire.
///
/// The envelope carries the three things a bare [`ControlCommand`] cannot: a
/// correlation id (so a client can tell its own replies from another client's),
/// a reply address, and an optional destination filter.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlEnvelope {
    /// Correlation id; every [`ControlReply`] to this command repeats it.
    pub id: Uuid,

    /// The command to execute.
    pub command: ControlCommand,

    /// Channel to publish replies to. `None` means fire-and-forget.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reply_to: Option<String>,

    /// Worker hostnames this command applies to. `None` (or an empty list)
    /// means every worker that receives it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub destination: Option<Vec<String>>,

    /// When the command was created (Unix timestamp, fractional seconds).
    pub timestamp: f64,
}

impl ControlEnvelope {
    /// Wrap `command` in a fresh envelope with a random correlation id.
    #[must_use]
    pub fn new(command: ControlCommand) -> Self {
        Self::with_id(Uuid::new_v4(), command)
    }

    /// Wrap `command` in an envelope with a caller-chosen correlation id.
    #[must_use]
    pub fn with_id(id: Uuid, command: ControlCommand) -> Self {
        Self {
            id,
            command,
            reply_to: None,
            destination: None,
            timestamp: now_secs(),
        }
    }

    /// Set the reply channel.
    ///
    /// [`ControlCommand::Ping`] also carries a `reply_to` field of its own,
    /// which predates this envelope. The envelope's value is authoritative;
    /// the one inside `Ping` is kept in sync here so a consumer reading either
    /// field sees the same address.
    #[must_use]
    pub fn with_reply_to(mut self, reply_to: impl Into<String>) -> Self {
        let reply_to = reply_to.into();
        if let ControlCommand::Ping {
            reply_to: ref mut inner,
        } = self.command
        {
            inner.clone_from(&reply_to);
        }
        self.reply_to = Some(reply_to);
        self
    }

    /// Restrict the command to a set of worker hostnames.
    #[must_use]
    pub fn with_destination(mut self, destination: Vec<String>) -> Self {
        self.destination = Some(destination);
        self
    }

    /// Whether a worker called `hostname` should act on this envelope.
    ///
    /// An absent or empty destination list is a broadcast to everyone, matching
    /// Celery's `destination=None` semantics.
    #[must_use]
    pub fn targets(&self, hostname: &str) -> bool {
        match self.destination {
            None => true,
            Some(ref list) if list.is_empty() => true,
            Some(ref list) => list.iter().any(|h| h == hostname),
        }
    }

    /// Serialise to the JSON body put on the wire.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Serialization`] if the command cannot be encoded.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|e| CelersError::Serialization(e.to_string()))
    }

    /// Parse a wire body produced by [`to_json`](Self::to_json).
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Deserialization`] if `body` is not a valid
    /// envelope.
    pub fn from_json(body: &str) -> Result<Self> {
        serde_json::from_str(body).map_err(|e| CelersError::Deserialization(e.to_string()))
    }
}

/// One worker's answer to one [`ControlEnvelope`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ControlReply {
    /// Correlation id copied from the envelope.
    pub command_id: Uuid,

    /// Hostname of the worker that answered.
    pub hostname: String,

    /// The answer itself.
    pub response: ControlResponse,

    /// When the reply was produced (Unix timestamp, fractional seconds).
    pub timestamp: f64,
}

impl ControlReply {
    /// Build a reply to `command_id` from `hostname`.
    #[must_use]
    pub fn new(command_id: Uuid, hostname: impl Into<String>, response: ControlResponse) -> Self {
        Self {
            command_id,
            hostname: hostname.into(),
            response,
            timestamp: now_secs(),
        }
    }

    /// Serialise to the JSON body put on the wire.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Serialization`] if the response cannot be encoded.
    pub fn to_json(&self) -> Result<String> {
        serde_json::to_string(self).map_err(|e| CelersError::Serialization(e.to_string()))
    }

    /// Parse a wire body produced by [`to_json`](Self::to_json).
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Deserialization`] if `body` is not a valid reply.
    pub fn from_json(body: &str) -> Result<Self> {
        serde_json::from_str(body).map_err(|e| CelersError::Deserialization(e.to_string()))
    }

    /// The inspection payload, when this reply is one.
    #[must_use]
    pub fn inspect_response(&self) -> Option<&InspectResponse> {
        match self.response {
            ControlResponse::Inspect(ref inner) => Some(inner),
            _ => None,
        }
    }
}

/// A live subscription to the broadcast control channel.
#[async_trait::async_trait]
pub trait ControlCommandStream: Send {
    /// Wait for the next command.
    ///
    /// Returns `Ok(None)` when the subscription has ended for good (the
    /// transport was dropped or the connection closed).
    ///
    /// # Errors
    ///
    /// Returns a transport error if the underlying channel fails in a way the
    /// caller should log; a malformed message is reported as an error and the
    /// subscription stays usable.
    async fn recv(&mut self) -> Result<Option<ControlEnvelope>>;
}

/// A live subscription to one request's reply channel.
#[async_trait::async_trait]
pub trait ControlReplyStream: Send {
    /// Wait for the next reply.
    ///
    /// Returns `Ok(None)` when no further replies can arrive.
    ///
    /// # Errors
    ///
    /// Returns a transport error if the underlying channel fails.
    async fn recv(&mut self) -> Result<Option<ControlReply>>;
}

/// A broadcast channel for control commands plus per-request reply channels.
///
/// Two implementations ship with `CeleRS`: [`InMemoryControlTransport`] (a
/// process-local hub, used by tests and by embedded single-process setups) and
/// `celers_broker_redis::RedisControlTransport` (Redis Pub/Sub).
#[async_trait::async_trait]
pub trait ControlTransport: Send + Sync {
    /// Publish `envelope` to every subscribed worker.
    ///
    /// Returns how many subscribers the transport believes received it (`0`
    /// when nothing is listening, or when the transport cannot tell).
    ///
    /// # Errors
    ///
    /// Returns a transport error when publishing fails.
    async fn broadcast(&self, envelope: &ControlEnvelope) -> Result<usize>;

    /// Subscribe to the broadcast control channel.
    ///
    /// # Errors
    ///
    /// Returns a transport error when the subscription cannot be established.
    async fn subscribe_commands(&self) -> Result<Box<dyn ControlCommandStream>>;

    /// Publish `reply` to the channel named by an envelope's `reply_to`.
    ///
    /// # Errors
    ///
    /// Returns a transport error when publishing fails. Publishing to a
    /// channel nobody is listening on is **not** an error.
    async fn send_reply(&self, reply_to: &str, reply: &ControlReply) -> Result<()>;

    /// Subscribe to a reply channel. Must be called *before* the matching
    /// [`broadcast`](Self::broadcast), or replies race into the void.
    ///
    /// # Errors
    ///
    /// Returns a transport error when the subscription cannot be established.
    async fn subscribe_replies(&self, reply_to: &str) -> Result<Box<dyn ControlReplyStream>>;
}

// ---------------------------------------------------------------------------
// In-memory transport
// ---------------------------------------------------------------------------

/// Shared state behind every clone of an [`InMemoryControlTransport`].
#[derive(Debug)]
struct ControlHub {
    commands: broadcast::Sender<ControlEnvelope>,
    replies: Mutex<HashMap<String, broadcast::Sender<ControlReply>>>,
}

impl Default for ControlHub {
    fn default() -> Self {
        let (commands, _) = broadcast::channel(CHANNEL_CAPACITY);
        Self {
            commands,
            replies: Mutex::new(HashMap::new()),
        }
    }
}

impl ControlHub {
    /// Lock the reply table, recovering from a poisoned lock.
    ///
    /// The table is a plain map of senders: a panic elsewhere cannot leave it
    /// logically inconsistent, and refusing to deliver replies afterwards would
    /// turn an unrelated panic into a permanently mute control plane.
    fn replies(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<String, broadcast::Sender<ControlReply>>> {
        self.replies.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// A process-local [`ControlTransport`].
///
/// Every clone shares one hub, so a worker built from one clone sees commands
/// broadcast through another. Intended for tests, examples and single-process
/// deployments where the worker and the controller live in the same binary; it
/// carries nothing between processes.
#[derive(Debug, Clone, Default)]
pub struct InMemoryControlTransport {
    hub: Arc<ControlHub>,
}

impl InMemoryControlTransport {
    /// Create a transport with a fresh, empty hub.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Number of workers currently subscribed to the broadcast channel.
    #[must_use]
    pub fn subscriber_count(&self) -> usize {
        self.hub.commands.receiver_count()
    }
}

#[async_trait::async_trait]
impl ControlTransport for InMemoryControlTransport {
    async fn broadcast(&self, envelope: &ControlEnvelope) -> Result<usize> {
        // `send` fails only when there are no receivers, which is "nobody
        // listening", not an error: a control broadcast into an empty cluster
        // is a legitimate no-op.
        Ok(self.hub.commands.send(envelope.clone()).unwrap_or(0))
    }

    async fn subscribe_commands(&self) -> Result<Box<dyn ControlCommandStream>> {
        Ok(Box::new(InMemoryCommandStream {
            rx: self.hub.commands.subscribe(),
        }))
    }

    async fn send_reply(&self, reply_to: &str, reply: &ControlReply) -> Result<()> {
        let sender = self.hub.replies().get(reply_to).cloned();
        if let Some(sender) = sender {
            if sender.send(reply.clone()).is_err() {
                // The client gave up and dropped its receiver; drop the entry
                // so the table does not grow one dead channel per request.
                self.hub.replies().remove(reply_to);
            }
        }
        Ok(())
    }

    async fn subscribe_replies(&self, reply_to: &str) -> Result<Box<dyn ControlReplyStream>> {
        let mut table = self.hub.replies();
        // Prune channels whose client has gone before inserting a new one.
        table.retain(|_, sender| sender.receiver_count() > 0);
        let sender = table.entry(reply_to.to_string()).or_insert_with(|| {
            let (tx, _) = broadcast::channel(CHANNEL_CAPACITY);
            tx
        });
        let rx = sender.subscribe();
        drop(table);
        Ok(Box::new(InMemoryReplyStream { rx }))
    }
}

/// Command subscription handed out by [`InMemoryControlTransport`].
struct InMemoryCommandStream {
    rx: broadcast::Receiver<ControlEnvelope>,
}

#[async_trait::async_trait]
impl ControlCommandStream for InMemoryCommandStream {
    async fn recv(&mut self) -> Result<Option<ControlEnvelope>> {
        match self.rx.recv().await {
            Ok(envelope) => Ok(Some(envelope)),
            Err(broadcast::error::RecvError::Closed) => Ok(None),
            // Dropping commands silently would make a control plane that looks
            // alive and ignores requests; report it. The subscription stays
            // usable, so the caller loops back into `recv`.
            Err(broadcast::error::RecvError::Lagged(skipped)) => Err(CelersError::Other(format!(
                "control subscriber lagged, {skipped} command(s) skipped"
            ))),
        }
    }
}

/// Reply subscription handed out by [`InMemoryControlTransport`].
struct InMemoryReplyStream {
    rx: broadcast::Receiver<ControlReply>,
}

#[async_trait::async_trait]
impl ControlReplyStream for InMemoryReplyStream {
    async fn recv(&mut self) -> Result<Option<ControlReply>> {
        match self.rx.recv().await {
            Ok(reply) => Ok(Some(reply)),
            Err(broadcast::error::RecvError::Closed) => Ok(None),
            Err(broadcast::error::RecvError::Lagged(skipped)) => Err(CelersError::Other(format!(
                "control reply subscriber lagged, {skipped} reply(s) skipped"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// Broadcasts control commands and gathers the replies.
///
/// The gather is deadline-bounded, never worker-count-bounded: a broadcast
/// control plane cannot know how many workers exist, so waiting for "all of
/// them" would hang whenever one is down. Use
/// [`with_expected_replies`](Self::with_expected_replies) when the caller does
/// know, to return as soon as that many have answered.
pub struct ControlClient {
    transport: Arc<dyn ControlTransport>,
    timeout: Duration,
    expected_replies: Option<usize>,
    destination: Option<Vec<String>>,
}

impl ControlClient {
    /// Create a client over `transport` with the default gather timeout.
    #[must_use]
    pub fn new(transport: Arc<dyn ControlTransport>) -> Self {
        Self {
            transport,
            timeout: DEFAULT_GATHER_TIMEOUT,
            expected_replies: None,
            destination: None,
        }
    }

    /// How long to keep gathering replies.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Stop gathering as soon as this many workers have answered.
    #[must_use]
    pub fn with_expected_replies(mut self, expected: usize) -> Self {
        self.expected_replies = Some(expected);
        self
    }

    /// Restrict every command from this client to the given hostnames.
    #[must_use]
    pub fn with_destination(mut self, destination: Vec<String>) -> Self {
        self.destination = Some(destination);
        self
    }

    /// The gather timeout in force.
    #[must_use]
    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    /// The transport this client publishes through.
    #[must_use]
    pub fn transport(&self) -> Arc<dyn ControlTransport> {
        Arc::clone(&self.transport)
    }

    /// Broadcast `command` and gather replies until the deadline.
    ///
    /// Returns an empty vector when nothing answered — that is a normal
    /// outcome (no workers running), not an error.
    ///
    /// # Errors
    ///
    /// Returns a transport error if the subscription or the publish fails.
    pub async fn broadcast(&self, command: ControlCommand) -> Result<Vec<ControlReply>> {
        let id = Uuid::new_v4();
        let reply_to = reply_channel(id);

        // Subscribe *before* publishing: a worker can answer faster than this
        // client can set up its receiver, and an unsubscribed reply is lost.
        let mut replies = self.transport.subscribe_replies(&reply_to).await?;

        let mut envelope = ControlEnvelope::with_id(id, command).with_reply_to(&reply_to);
        if let Some(ref destination) = self.destination {
            envelope = envelope.with_destination(destination.clone());
        }
        self.transport.broadcast(&envelope).await?;

        Ok(self.gather(&mut *replies, id).await)
    }

    /// Broadcast `command` without waiting for any reply.
    ///
    /// Returns the number of subscribers the transport reached.
    ///
    /// # Errors
    ///
    /// Returns a transport error if the publish fails.
    pub async fn broadcast_no_reply(&self, command: ControlCommand) -> Result<usize> {
        let mut envelope = ControlEnvelope::new(command);
        if let Some(ref destination) = self.destination {
            envelope = envelope.with_destination(destination.clone());
        }
        self.transport.broadcast(&envelope).await
    }

    /// Ping every worker, returning one reply per worker that answered.
    ///
    /// # Errors
    ///
    /// Returns a transport error if the broadcast fails.
    pub async fn ping(&self) -> Result<Vec<ControlReply>> {
        // The placeholder is overwritten by `with_reply_to` inside `broadcast`.
        self.broadcast(ControlCommand::ping(String::new())).await
    }

    /// Run an inspect command, returning `(hostname, response)` per worker.
    ///
    /// Workers that answered with an error are reported through
    /// [`inspect_errors`](Self::inspect_errors) instead of being silently
    /// dropped — call [`broadcast`](Self::broadcast) directly when both halves
    /// matter in one pass.
    ///
    /// # Errors
    ///
    /// Returns a transport error if the broadcast fails.
    pub async fn inspect(
        &self,
        command: InspectCommand,
    ) -> Result<Vec<(String, Box<InspectResponse>)>> {
        let replies = self.broadcast(ControlCommand::Inspect(command)).await?;
        Ok(replies
            .into_iter()
            .filter_map(|reply| match reply.response {
                ControlResponse::Inspect(payload) => Some((reply.hostname, payload)),
                _ => None,
            })
            .collect())
    }

    /// The `(hostname, error)` pairs among `replies`.
    #[must_use]
    pub fn inspect_errors(replies: &[ControlReply]) -> Vec<(String, String)> {
        replies
            .iter()
            .filter_map(|reply| match reply.response {
                ControlResponse::Error { ref error } => {
                    Some((reply.hostname.clone(), error.clone()))
                }
                _ => None,
            })
            .collect()
    }

    /// Collect replies carrying `id` until the deadline or the expected count.
    async fn gather(&self, replies: &mut dyn ControlReplyStream, id: Uuid) -> Vec<ControlReply> {
        let deadline = Instant::now() + self.timeout;
        let mut gathered = Vec::new();

        loop {
            if let Some(expected) = self.expected_replies {
                if gathered.len() >= expected {
                    break;
                }
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }
            match tokio::time::timeout(remaining, replies.recv()).await {
                // Deadline hit: return what we have.
                Err(_elapsed) => break,
                Ok(Ok(Some(reply))) => {
                    // Another client's replies can share a channel only if it
                    // reused our id, but filtering keeps the contract explicit.
                    if reply.command_id == id {
                        gathered.push(reply);
                    }
                }
                // Channel closed for good.
                Ok(Ok(None)) => break,
                Ok(Err(e)) => {
                    tracing::warn!("Control reply stream error: {}", e);
                }
            }
        }

        gathered
    }
}

impl std::fmt::Debug for ControlClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ControlClient")
            .field("timeout", &self.timeout)
            .field("expected_replies", &self.expected_replies)
            .field("destination", &self.destination)
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn envelope_keeps_ping_reply_to_in_sync() {
        let envelope = ControlEnvelope::new(ControlCommand::ping("stale")).with_reply_to("fresh");
        assert_eq!(envelope.reply_to.as_deref(), Some("fresh"));
        match envelope.command {
            ControlCommand::Ping { ref reply_to } => assert_eq!(reply_to, "fresh"),
            ref other => panic!("expected ping, got {other:?}"),
        }
    }

    #[test]
    fn envelope_destination_filters_workers() {
        let all = ControlEnvelope::new(ControlCommand::inspect_active());
        assert!(all.targets("worker-1"));

        let empty = all.clone().with_destination(Vec::new());
        assert!(empty.targets("worker-1"));

        let targeted = ControlEnvelope::new(ControlCommand::inspect_active())
            .with_destination(vec!["worker-2".to_string()]);
        assert!(targeted.targets("worker-2"));
        assert!(!targeted.targets("worker-1"));
    }

    #[test]
    fn envelope_round_trips_through_json() {
        let envelope = ControlEnvelope::new(ControlCommand::shutdown(Some(30)))
            .with_reply_to("celers.control.reply.x")
            .with_destination(vec!["a".to_string()]);
        let json = envelope.to_json().expect("serialize");
        let parsed = ControlEnvelope::from_json(&json).expect("deserialize");

        assert_eq!(parsed.id, envelope.id);
        assert_eq!(parsed.reply_to, envelope.reply_to);
        assert_eq!(parsed.destination, envelope.destination);
        match parsed.command {
            ControlCommand::Shutdown { timeout } => assert_eq!(timeout, Some(30)),
            ref other => panic!("expected shutdown, got {other:?}"),
        }
    }

    #[test]
    fn reply_round_trips_through_json() {
        let id = Uuid::new_v4();
        let reply = ControlReply::new(id, "worker-1", ControlResponse::ack(true, None));
        let parsed = ControlReply::from_json(&reply.to_json().expect("serialize")).expect("parse");
        assert_eq!(parsed.command_id, id);
        assert_eq!(parsed.hostname, "worker-1");
        assert!(matches!(
            parsed.response,
            ControlResponse::Ack { ok: true, .. }
        ));
    }

    #[tokio::test]
    async fn in_memory_transport_delivers_broadcast_to_every_subscriber() {
        let transport = InMemoryControlTransport::new();
        let mut first = transport.subscribe_commands().await.expect("subscribe");
        let mut second = transport.subscribe_commands().await.expect("subscribe");
        assert_eq!(transport.subscriber_count(), 2);

        let envelope = ControlEnvelope::new(ControlCommand::inspect_stats());
        let reached = transport.broadcast(&envelope).await.expect("broadcast");
        assert_eq!(reached, 2);

        for stream in [&mut first, &mut second] {
            let received = stream.recv().await.expect("recv").expect("envelope");
            assert_eq!(received.id, envelope.id);
        }
    }

    #[tokio::test]
    async fn broadcast_with_no_subscriber_is_not_an_error() {
        let transport = InMemoryControlTransport::new();
        let envelope = ControlEnvelope::new(ControlCommand::inspect_stats());
        assert_eq!(transport.broadcast(&envelope).await.expect("broadcast"), 0);
    }

    #[tokio::test]
    async fn reply_published_before_subscription_is_dropped_not_queued() {
        // Documents the ordering contract `ControlClient::broadcast` relies on:
        // the client subscribes first, so this case cannot arise there.
        let transport = InMemoryControlTransport::new();
        let reply = ControlReply::new(Uuid::new_v4(), "w", ControlResponse::ack(true, None));
        transport
            .send_reply("celers.control.reply.missing", &reply)
            .await
            .expect("send_reply into the void succeeds");
    }

    #[tokio::test]
    async fn client_gathers_replies_from_two_workers() {
        let transport = Arc::new(InMemoryControlTransport::new());

        for hostname in ["worker-1", "worker-2"] {
            let transport = Arc::clone(&transport);
            let mut commands = transport.subscribe_commands().await.expect("subscribe");
            tokio::spawn(async move {
                while let Ok(Some(envelope)) = commands.recv().await {
                    let Some(ref reply_to) = envelope.reply_to else {
                        continue;
                    };
                    let reply =
                        ControlReply::new(envelope.id, hostname, ControlResponse::pong(hostname));
                    let _ = transport.send_reply(reply_to, &reply).await;
                }
            });
        }

        let client = ControlClient::new(transport)
            .with_timeout(Duration::from_secs(2))
            .with_expected_replies(2);
        let replies = client.ping().await.expect("ping");

        assert_eq!(replies.len(), 2);
        let mut hostnames: Vec<&str> = replies.iter().map(|r| r.hostname.as_str()).collect();
        hostnames.sort_unstable();
        assert_eq!(hostnames, ["worker-1", "worker-2"]);
    }

    #[tokio::test]
    async fn client_returns_empty_when_nothing_answers() {
        let transport = Arc::new(InMemoryControlTransport::new());
        let client = ControlClient::new(transport).with_timeout(Duration::from_millis(50));

        let started = Instant::now();
        let replies = client.broadcast(ControlCommand::inspect_active()).await;
        let replies = replies.expect("broadcast into an empty cluster is not an error");

        assert!(replies.is_empty());
        // Bounded by the timeout, not hanging.
        assert!(started.elapsed() < Duration::from_secs(5));
    }

    #[tokio::test]
    async fn client_ignores_replies_for_other_commands() {
        let transport = Arc::new(InMemoryControlTransport::new());

        {
            let transport = Arc::clone(&transport);
            let mut commands = transport.subscribe_commands().await.expect("subscribe");
            tokio::spawn(async move {
                while let Ok(Some(envelope)) = commands.recv().await {
                    let Some(ref reply_to) = envelope.reply_to else {
                        continue;
                    };
                    // Answer with a mismatched correlation id.
                    let stray = ControlReply::new(
                        Uuid::new_v4(),
                        "worker-1",
                        ControlResponse::ack(true, None),
                    );
                    let _ = transport.send_reply(reply_to, &stray).await;
                }
            });
        }

        let client = ControlClient::new(transport).with_timeout(Duration::from_millis(100));
        let replies = client.ping().await.expect("ping");
        assert!(replies.is_empty(), "stray correlation id must be filtered");
    }
}
