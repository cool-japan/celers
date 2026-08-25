//! Durable task revocation, backed by `celers_revoked_tasks` (migration
//! `008_revocation.sql`) plus a direct `pg_notify` for workers that are
//! already executing the task.
//!
//! Mirrors `celers-broker-redis`'s two-path design (see that crate's
//! `revocation.rs`):
//!
//! | | queued task | running task | worker restarted / disconnected |
//! |---|---|---|---|
//! | `celers_revoked_tasks` row | refused at dispatch (`is_revoked`) | — | survives |
//! | `pg_notify` on `celers_revoked_<queue>` | — | aborted at once | missed (fire-and-forget) |
//!
//! Unlike Redis, `revoke()` here does **not** scan the pending queue for
//! copies to drop: a Postgres task is addressed directly by its row id
//! ([`sql::CANCEL_TASK`]), so there is no bounded-scan analogue of Redis's
//! `REVOKE_SCAN_LIMIT` to reproduce. A task that is still `pending` when
//! `revoke()` runs is cancelled in the same call; the durable
//! `celers_revoked_tasks` row is what refuses a task that was *already*
//! dequeued (and is mid-execution) or one enqueued again under the same id
//! after the revocation.
//!
//! The claim statement (`sql::claim_one_sql`) deliberately does **not**
//! filter against `celers_revoked_tasks`. `revoke()` already removes a
//! pending row outright, so a revoked-but-still-`pending` row should not
//! normally exist; adding a correlated `NOT EXISTS` subquery to the hot
//! dequeue path would cost every deployment a lookup against a table most
//! never populate, make the claim statement's success depend on a migration
//! that revocation callers may not have run, and duplicate the check
//! `is_revoked()` already performs at dispatch (`Worker::with_broker_revocation`
//! consults it before executing a claimed message). The durable table's job
//! is that dispatch-time check and the pub/sub bridge to already-running
//! tasks — not filtering the claim query.
//!
//! # RESP... no, LISTEN/NOTIFY
//!
//! `pg_notify(channel, payload)` is an ordinary function call (unlike the
//! trigger installed by `notifications.rs`, which must splice the channel
//! name into DDL text because a channel name cannot be a bind parameter
//! inside `CREATE FUNCTION`), so both arguments bind as ordinary query
//! parameters here — no channel-name validation dance is needed beyond the
//! queue-name validation `PostgresBroker::with_pool_config` already performs.

use celers_core::revocation_channel::{RevocationNotice, RevocationStream};
use celers_core::{CelersError, Result, TaskId};

use chrono::{Duration as ChronoDuration, Utc};
use oxisql_postgres::{NotificationStream, PgConnection};
use std::time::Duration;

use crate::row_ext::uuid_param;
use crate::sql;
use crate::tls_mode;
use crate::PostgresBroker;

/// Per-call timeout for the underlying `LISTEN` poll.
///
/// `NotificationStream::recv_timeout` cannot distinguish "nothing arrived
/// within the timeout" from "the connection died" (see `notifications.rs`'s
/// `wait_for_notification` for the same, already-documented limitation in
/// this crate) — so [`PgRevocationStream::recv`] treats every timeout as
/// "the subscription ended" and lets the caller resubscribe
/// (`celers_worker`'s revocation bridge already does this, with backoff, for
/// exactly this reason). A short timeout would reconnect constantly on an
/// idle queue; a long one delays noticing a genuinely dead connection. Sixty
/// seconds is a compromise, not a load-bearing constant — the durable
/// `celers_revoked_tasks` row (not this stream) is what guarantees a queued
/// task stays revoked across a gap.
const REVOCATION_LISTEN_POLL_SECS: u64 = 60;

/// Channel name `revoke()`/`subscribe_revocations()` use for one queue.
fn revocation_channel(queue_name: &str) -> String {
    format!("celers_revoked_{queue_name}")
}

/// A live subscription to a Postgres revocation channel.
///
/// Obtained from
/// [`Broker::subscribe_revocations`](celers_core::Broker::subscribe_revocations)
/// on a [`PostgresBroker`]; dropping it tears down the dedicated `LISTEN`
/// connection.
pub struct PgRevocationStream {
    /// Kept alive for the stream's whole lifetime — dropping it would tear
    /// down the underlying connection driver task that feeds `stream`. Same
    /// rationale as `TaskNotificationListener::_conn`.
    _conn: PgConnection,
    stream: NotificationStream,
}

impl std::fmt::Debug for PgRevocationStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PgRevocationStream").finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl RevocationStream for PgRevocationStream {
    async fn recv(&mut self) -> Result<Option<RevocationNotice>> {
        match self
            .stream
            .recv_timeout(Duration::from_secs(REVOCATION_LISTEN_POLL_SECS))
            .await
        {
            // A malformed payload is reported rather than skipped: a
            // revocation dropped in silence is a task that keeps running
            // with nothing in the log to explain why. The subscription
            // stays usable — the caller loops back into `recv`.
            Some(notification) => RevocationNotice::from_wire(&notification.payload).map(Some),
            None => Ok(None),
        }
    }
}

impl PostgresBroker {
    /// Durably record a revocation: upsert `celers_revoked_tasks`, prune
    /// lapsed rows, cancel the task if it is still `pending`, and notify
    /// `celers_revoked_<queue>`.
    ///
    /// See [`Broker::revoke`](celers_core::Broker::revoke) for the public
    /// entry point (`broker_trait.rs`).
    pub(crate) async fn record_revocation(
        &self,
        task_id: &TaskId,
        terminate: bool,
    ) -> Result<bool> {
        let task_id_param = uuid_param(task_id);
        let expires_at =
            (Utc::now() + ChronoDuration::seconds(self.revocation_ttl_secs as i64)).to_rfc3339();

        self.conn
            .execute(
                sql::UPSERT_REVOKED_TASK,
                &[&task_id_param, &self.queue_name, &terminate, &expires_at],
            )
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to record revocation: {}", e)))?;

        // Best effort: a table most deployments keep tiny, but there is no
        // dedicated sweep task (unlike terminal-task retention) — see the
        // migration's doc comment.
        if let Err(e) = self.conn.execute(sql::PRUNE_EXPIRED_REVOCATIONS, &[]).await {
            tracing::warn!(error = %e, "failed to prune expired revocations (non-fatal)");
        }

        // A task still `pending` is removed from the dispatch queue outright;
        // one already `processing` is left alone (revoking it does not
        // change its row — the durable set plus the notify below are what
        // reach it). Both cases still return `true`: the revocation itself
        // was recorded either way.
        let cancelled = self
            .conn
            .execute(sql::CANCEL_TASK, &[&task_id_param, &self.queue_name])
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to cancel pending task: {}", e)))?
            > 0;

        let channel = revocation_channel(&self.queue_name);
        let body = RevocationNotice::new(*task_id, terminate).to_wire();
        self.conn
            .execute("SELECT pg_notify($1::text, $2::text)", &[&channel, &body])
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to publish revocation: {}", e)))?;

        tracing::info!(
            task_id = %task_id,
            queue = %self.queue_name,
            terminate,
            cancelled_pending_copy = cancelled,
            "Revoked task"
        );

        Ok(true)
    }

    /// Whether `task_id` is listed in the durable `celers_revoked_tasks` set
    /// for this broker's queue, with a still-live `expires_at`.
    pub(crate) async fn read_revocation(&self, task_id: &TaskId) -> Result<bool> {
        let task_id_param = uuid_param(task_id);
        let rows = self
            .conn
            .query(sql::IS_REVOKED, &[&task_id_param, &self.queue_name])
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to read revoked set: {}", e)))?;
        Ok(!rows.is_empty())
    }

    /// Subscribe to `celers_revoked_<queue>`, the channel
    /// [`record_revocation`](Self::record_revocation) notifies on.
    ///
    /// Opens a dedicated `LISTEN` connection — see
    /// `create_notification_listener` for why a long-lived listener must not
    /// share the broker's pooled query connections.
    pub(crate) async fn open_revocation_listener(&self) -> Result<PgRevocationStream> {
        let channel = revocation_channel(&self.queue_name);

        let tls_mode = tls_mode::pg_tls_mode_for_url(&self.database_url).map_err(|e| {
            CelersError::Other(format!(
                "Failed to resolve TLS mode for revocation LISTEN connection: {}",
                e
            ))
        })?;
        let conn = PgConnection::connect(&self.database_url, tls_mode)
            .await
            .map_err(|e| {
                CelersError::Other(format!(
                    "Failed to create dedicated revocation LISTEN connection: {}",
                    e
                ))
            })?;

        let stream = conn.listen(&channel).await.map_err(|e| {
            CelersError::Other(format!(
                "Failed to listen on revocation channel {}: {}",
                channel, e
            ))
        })?;

        tracing::info!(channel = %channel, "Subscribed to broker revocations");

        Ok(PgRevocationStream {
            _conn: conn,
            stream,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn revocation_channel_is_scoped_per_queue() {
        assert_eq!(revocation_channel("orders"), "celers_revoked_orders");
        assert_ne!(revocation_channel("orders"), revocation_channel("payments"));
    }
}
