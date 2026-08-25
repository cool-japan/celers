//! Durable task revocation, backed by `celers_revoked_tasks` (migration
//! `011_revocation.sql`).
//!
//! Mirrors `celers-broker-redis`'s `<queue>:revoked` sorted set and
//! `celers-broker-postgres`'s `celers_revoked_tasks` table of the same name.
//! MySQL has no LISTEN/NOTIFY equivalent, so unlike those two,
//! [`MysqlBroker::subscribe_revocations`](celers_core::Broker::subscribe_revocations)
//! is backed by [`MysqlRevocationStream`], which **polls** the table on an
//! interval instead of being pushed to:
//!
//! | | queued task | running task | worker restarted / disconnected |
//! |---|---|---|---|
//! | `celers_revoked_tasks` row | refused at dispatch (`is_revoked`) | — | survives |
//! | poll of `celers_revoked_tasks` | — | aborted within one poll interval | missed only while genuinely disconnected |
//!
//! # Latency, honestly
//!
//! A queued task is refused immediately: `is_revoked()` is a direct,
//! synchronous lookup a worker performs before executing a claimed message,
//! so there is no polling delay on that path at all. The delay this module
//! introduces is specific to the *already-running* task case
//! (`revoke(terminate = true)`, where a worker must be told to abort work in
//! progress): that notice reaches a subscriber only on the poller's next
//! tick, so worst case it is
//! [`DEFAULT_REVOCATION_POLL_INTERVAL_SECS`](crate::DEFAULT_REVOCATION_POLL_INTERVAL_SECS)
//! (configurable via
//! [`MysqlBroker::with_revocation_poll_interval`](crate::MysqlBroker::with_revocation_poll_interval))
//! late. This is a real, deliberate trade-off — not a placeholder — for not
//! needing a second long-lived connection or a MySQL feature this crate
//! otherwise has no use for.
//!
//! Like `celers-broker-postgres`, the claim query is deliberately **not**
//! filtered against `celers_revoked_tasks`: `revoke()` already cancels a
//! still-`pending` row outright, so a correlated subquery on the hot dequeue
//! path would only add cost, not correctness, and would make every dequeue
//! depend on a migration that revocation callers may not have run.

use celers_core::revocation_channel::{RevocationNotice, RevocationStream};
use celers_core::{Broker, CelersError, Result, TaskId};

use chrono::{DateTime, Duration as ChronoDuration, Utc};
use oxisql_core::Connection;
use std::collections::VecDeque;
use std::time::Duration;

use crate::row_ext::RowExt;
use crate::MysqlBroker;

/// `?` order: task_id, queue_name, terminate, revoked_at, expires_at.
///
/// A second `revoke()` for the same task — e.g. escalating `terminate` from
/// `false` to `true` — replaces the row (and, importantly, bumps
/// `revoked_at`, which is what lets [`MysqlRevocationStream`]'s poll cursor
/// see the update as a fresh event rather than missing it because the row's
/// primary key did not change).
const UPSERT_REVOKED_TASK: &str = "\
    INSERT INTO celers_revoked_tasks (task_id, queue_name, terminate, revoked_at, expires_at) \
    VALUES (?, ?, ?, ?, ?) \
    ON DUPLICATE KEY UPDATE \
        terminate = VALUES(terminate), \
        revoked_at = VALUES(revoked_at), \
        expires_at = VALUES(expires_at)";

/// Opportunistic prune, run on every `revoke()` call. No dedicated sweep task
/// exists (unlike terminal-task retention) because this table is expected to
/// stay small: only *currently revoked* ids survive past their `expires_at`.
const PRUNE_EXPIRED_REVOCATIONS: &str =
    "DELETE FROM celers_revoked_tasks WHERE expires_at < NOW(6)";

/// `?` order: task_id, queue_name.
const IS_REVOKED: &str = "\
    SELECT 1 FROM celers_revoked_tasks \
     WHERE task_id = ? AND queue_name = ? AND expires_at > NOW(6)";

/// `?` order: queue_name, revoked_at (the poll cursor watermark).
///
/// Strict `revoked_at > ?`, deliberately — an earlier draft of this poller
/// used `>=` to avoid dropping a revocation written in the exact same
/// instant as the previous poll's newest row, but that has a worse failure
/// mode than the one it avoids: once the watermark reaches a row's own
/// `revoked_at`, a `>=` query matches that row on *every subsequent poll
/// forever* (the watermark can never move past it without moving past a
/// *different* row), redelivering the same notice indefinitely rather than
/// once. A strict `>` cannot do that; the only cost is a theoretical miss of
/// a second, independent revocation landing in the exact same microsecond as
/// the current watermark (this column is `DATETIME(6)`) — astronomically
/// unlikely for two server-side `revoke()` calls, and in any case only
/// affects the best-effort abort-a-running-task path: a *queued* task is
/// still refused by `is_revoked()` regardless of anything this poller does.
const POLL_REVOCATIONS: &str = "\
    SELECT task_id, terminate, revoked_at FROM celers_revoked_tasks \
     WHERE queue_name = ? AND revoked_at > ? \
     ORDER BY revoked_at ASC";

impl MysqlBroker {
    /// Durably record a revocation: upsert `celers_revoked_tasks`, prune
    /// lapsed rows, and cancel the task if it is still `pending`.
    ///
    /// See [`Broker::revoke`](celers_core::Broker::revoke) for the public
    /// entry point (`broker_trait.rs`).
    pub(crate) async fn record_revocation(
        &self,
        task_id: &TaskId,
        terminate: bool,
    ) -> Result<bool> {
        let now = Utc::now();
        let expires_at = now + ChronoDuration::seconds(self.revocation_ttl_secs as i64);
        let task_id_str = task_id.to_string();

        self.connection()
            .execute(
                UPSERT_REVOKED_TASK,
                &[
                    &task_id_str,
                    &self.queue_name,
                    &terminate,
                    &format_datetime(now),
                    &format_datetime(expires_at),
                ],
            )
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to record revocation: {}", e)))?;

        if let Err(e) = self
            .connection()
            .execute(PRUNE_EXPIRED_REVOCATIONS, &[])
            .await
        {
            tracing::warn!(error = %e, "failed to prune expired revocations (non-fatal)");
        }

        // A task still `pending` is cancelled outright; one already
        // `processing` is left alone — the durable row above (checked by
        // `is_revoked` at dispatch) and the poller are what reach that case.
        // Both cases still return `true`: the revocation itself was recorded
        // either way. `cancel()` is deliberately not queue-scoped upstream
        // (see `broker_trait.rs`), which is fine here: task ids are UUIDs.
        let cancelled = self.cancel(task_id).await?;

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
        let task_id_str = task_id.to_string();
        let rows = self
            .connection()
            .query(IS_REVOKED, &[&task_id_str, &self.queue_name])
            .await
            .map_err(|e| CelersError::Broker(format!("Failed to read revoked set: {}", e)))?;
        Ok(!rows.is_empty())
    }

    /// Start polling `celers_revoked_tasks` for this broker's queue.
    ///
    /// Only revocations recorded *after* this call are observed — matching
    /// the fire-and-forget semantics of the Redis/Postgres push channels this
    /// polls in place of (a subscriber does not receive history).
    pub(crate) fn open_revocation_poller(&self) -> MysqlRevocationStream {
        MysqlRevocationStream {
            conn: self.conn.clone(),
            queue_name: self.queue_name.clone(),
            poll_interval: Duration::from_secs(self.revocation_poll_interval_secs.max(1)),
            last_seen: Utc::now(),
            buffered: VecDeque::new(),
        }
    }
}

/// MySQL's canonical `DATETIME`/`TIMESTAMP` text form — see `row_ext.rs`'s
/// "DateTime<Utc> parameter convention (MySQL)" section. Binding
/// `.to_rfc3339()` here would silently fail the server's grammar check.
fn format_datetime(dt: DateTime<Utc>) -> String {
    dt.format("%Y-%m-%d %H:%M:%S%.6f").to_string()
}

/// A poll-based subscription to a MySQL broker's revocations.
///
/// Obtained from
/// [`Broker::subscribe_revocations`](celers_core::Broker::subscribe_revocations)
/// on a [`MysqlBroker`]. Unlike a push-based stream, `recv()` never returns
/// `Ok(None)` on its own — there is no live connection to lose, only a
/// connection *pool* that already handles reconnection transparently (see
/// `MysqlBroker::connection`'s doc) — so it blocks internally until a new
/// revocation appears rather than signalling end-of-stream.
pub struct MysqlRevocationStream {
    conn: oxisql_mysql::MyConnection,
    queue_name: String,
    poll_interval: Duration,
    /// The watermark: only rows with `revoked_at > last_seen` are fetched on
    /// the next poll. Advanced to the newest `revoked_at` seen after every
    /// non-empty poll.
    last_seen: DateTime<Utc>,
    /// Rows fetched by one poll are queued here and drained one at a time —
    /// `recv()` must hand back exactly one notice per call.
    buffered: VecDeque<RevocationNotice>,
}

impl std::fmt::Debug for MysqlRevocationStream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MysqlRevocationStream")
            .field("queue_name", &self.queue_name)
            .field("poll_interval", &self.poll_interval)
            .finish_non_exhaustive()
    }
}

#[async_trait::async_trait]
impl RevocationStream for MysqlRevocationStream {
    async fn recv(&mut self) -> Result<Option<RevocationNotice>> {
        loop {
            if let Some(notice) = self.buffered.pop_front() {
                return Ok(Some(notice));
            }

            tokio::time::sleep(self.poll_interval).await;

            let rows = self
                .conn
                .query(
                    POLL_REVOCATIONS,
                    &[&self.queue_name, &format_datetime(self.last_seen)],
                )
                .await
                .map_err(|e| CelersError::Broker(format!("Failed to poll revocations: {}", e)))?;

            for row in &rows {
                let task_id_str: String = row.col("task_id").map_err(|e| {
                    CelersError::Broker(format!("Malformed revocation row (task_id): {}", e))
                })?;
                let task_id = TaskId::parse_str(&task_id_str).map_err(|e| {
                    CelersError::Broker(format!("Malformed revocation row (task_id): {}", e))
                })?;
                // `terminate` is `TINYINT(1)`, which this crate's driver
                // decodes as an integer on the read side (there is no
                // `Value::Bool` round trip through MySQL's wire protocol —
                // see this module's Cargo-doc-visible note in the source),
                // so it is read as `i64` and compared explicitly rather than
                // as `bool` directly.
                let terminate: i64 = row.col("terminate").map_err(|e| {
                    CelersError::Broker(format!("Malformed revocation row (terminate): {}", e))
                })?;
                let revoked_at: DateTime<Utc> = row.col("revoked_at").map_err(|e| {
                    CelersError::Broker(format!("Malformed revocation row (revoked_at): {}", e))
                })?;

                self.buffered
                    .push_back(RevocationNotice::new(task_id, terminate != 0));
                // `POLL_REVOCATIONS` already guarantees `revoked_at >
                // last_seen` and returns rows in ascending order, so the
                // final iteration always holds the new maximum —
                // unconditional assignment (not `if revoked_at >
                // self.last_seen`) on purpose, so the watermark advances
                // exactly to the last row's timestamp with no dead branch.
                self.last_seen = revoked_at;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn datetime_formatting_matches_the_crate_wide_mysql_convention() {
        let dt = DateTime::parse_from_rfc3339("2026-01-02T03:04:05.123456Z")
            .unwrap()
            .with_timezone(&Utc);
        assert_eq!(format_datetime(dt), "2026-01-02 03:04:05.123456");
    }

    #[test]
    fn sql_text_is_queue_scoped_and_uses_a_watermark_cursor() {
        assert!(UPSERT_REVOKED_TASK.contains("ON DUPLICATE KEY UPDATE"));
        assert!(
            UPSERT_REVOKED_TASK.contains("revoked_at = VALUES(revoked_at)"),
            "re-revoking must bump revoked_at, or the poller's cursor never sees the update"
        );
        assert!(IS_REVOKED.contains("queue_name = ?"));
        assert!(
            IS_REVOKED.contains("expires_at > NOW(6)"),
            "a lapsed revocation must not still block dispatch"
        );
        assert!(
            POLL_REVOCATIONS.contains("revoked_at > ?"),
            "a `>=` cursor would redeliver the newest row forever once the \
             watermark reaches it, whenever no newer revocation ever arrives"
        );
        assert!(POLL_REVOCATIONS.contains("ORDER BY revoked_at ASC"));
    }
}
