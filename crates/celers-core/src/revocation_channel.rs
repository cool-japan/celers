//! The broker-fed revocation channel.
//!
//! [`revocation`](crate::revocation) holds the *worker-local* record of what has
//! been revoked. This module defines how a revocation reaches a worker that did
//! not issue it: the notice put on the wire ([`RevocationNotice`]) and the
//! subscription a worker reads it from ([`RevocationStream`]).
//!
//! Two things a broker can offer make cooperative revocation real, and both are
//! optional methods on [`Broker`](crate::Broker) with inert defaults:
//!
//! - [`Broker::subscribe_revocations`](crate::Broker::subscribe_revocations) —
//!   a live feed of revocations, so a task that is *already running* can be
//!   aborted. `celers_worker::Worker::with_broker_revocation` subscribes to it
//!   and forwards every notice into the worker's
//!   `RevocationPublisher`/`RevocationWatcher` pair.
//! - [`Broker::is_revoked`](crate::Broker::is_revoked) — a lookup against the
//!   broker's *persisted* revoked-id set, so a task that was revoked while it
//!   sat in the queue is refused when it is finally dequeued, even by a worker
//!   that was not running when the revocation was published.
//!
//! Pub/Sub alone is never enough: it is fire-and-forget, so a worker that is
//! restarting or momentarily disconnected never sees the notice. The persisted
//! set is what makes a revocation stick; the live feed is what makes it prompt.
//!
//! # Wire format
//!
//! A notice is a small JSON document:
//!
//! ```json
//! {"task_id": "8f14e45f-…", "terminate": true}
//! ```
//!
//! A bare task id (`8f14e45f-…`) is also accepted and read as
//! `terminate: false`. That is the payload `celers_broker_redis`'s
//! `<queue>:cancel` channel carried before this module existed, and it is the
//! honest reading: [`Broker::cancel`](crate::Broker::cancel) has no `terminate`
//! parameter, and Celery's `revoke(id)` does *not* abort a task that is already
//! running — only `revoke(id, terminate=True)` does.
//!
//! # Example
//!
//! ```
//! use celers_core::revocation_channel::RevocationNotice;
//! use uuid::Uuid;
//!
//! let task_id = Uuid::new_v4();
//! let notice = RevocationNotice::terminate(task_id);
//! let wire = notice.to_wire();
//!
//! assert_eq!(RevocationNotice::from_wire(&wire).unwrap(), notice);
//! // The legacy bare-id form is a non-terminating revocation.
//! assert_eq!(
//!     RevocationNotice::from_wire(&task_id.to_string()).unwrap(),
//!     RevocationNotice::ignore(task_id)
//! );
//! ```

use crate::{CelersError, Result, TaskId};

use serde::{Deserialize, Serialize};

/// One revocation as it travels from a broker to a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RevocationNotice {
    /// The task being revoked.
    pub task_id: TaskId,

    /// Whether a worker that is *already executing* the task should abort it.
    ///
    /// `false` (Celery's `revoke(id)`) only prevents the task from starting;
    /// `true` (`revoke(id, terminate=True)`) also trips the running task's
    /// cancellation token.
    #[serde(default)]
    pub terminate: bool,
}

impl RevocationNotice {
    /// A notice for `task_id` with an explicit `terminate` flag.
    #[must_use]
    pub const fn new(task_id: TaskId, terminate: bool) -> Self {
        Self { task_id, terminate }
    }

    /// A revocation that must not abort a running copy of the task.
    #[must_use]
    pub const fn ignore(task_id: TaskId) -> Self {
        Self::new(task_id, false)
    }

    /// A revocation that must also abort a running copy of the task.
    #[must_use]
    pub const fn terminate(task_id: TaskId) -> Self {
        Self::new(task_id, true)
    }

    /// The body to publish on a revocation channel.
    ///
    /// Serialisation of a `(uuid, bool)` pair cannot fail; should it ever
    /// somehow, the bare task id is emitted instead, which every reader
    /// understands as a non-terminating revocation. Losing the revocation
    /// entirely would be strictly worse than losing the `terminate` flag.
    #[must_use]
    pub fn to_wire(&self) -> String {
        serde_json::to_string(self).unwrap_or_else(|_| self.task_id.to_string())
    }

    /// Parse a body produced by [`to_wire`](Self::to_wire), or a bare task id.
    ///
    /// # Errors
    ///
    /// Returns [`CelersError::Deserialization`] when `body` is neither a
    /// notice document nor a task id.
    pub fn from_wire(body: &str) -> Result<Self> {
        let trimmed = body.trim();
        if trimmed.starts_with('{') {
            return serde_json::from_str(trimmed).map_err(|e| {
                CelersError::Deserialization(format!("malformed revocation notice: {e}"))
            });
        }
        match TaskId::parse_str(trimmed) {
            Ok(task_id) => Ok(Self::ignore(task_id)),
            Err(e) => Err(CelersError::Deserialization(format!(
                "revocation notice is neither JSON nor a task id: {e}"
            ))),
        }
    }
}

/// A live subscription to a broker's revocation channel.
///
/// Obtained from
/// [`Broker::subscribe_revocations`](crate::Broker::subscribe_revocations).
#[async_trait::async_trait]
pub trait RevocationStream: Send {
    /// Wait for the next revocation.
    ///
    /// Returns `Ok(None)` when the subscription has ended (the broker was
    /// dropped, or the connection closed) — a caller that wants to keep
    /// listening must resubscribe.
    ///
    /// # Errors
    ///
    /// Returns a transport error the caller should log; a subscriber that fell
    /// behind is reported this way and the subscription stays usable, so the
    /// caller loops back into `recv`.
    async fn recv(&mut self) -> Result<Option<RevocationNotice>>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn notice_round_trips_through_the_wire_format() {
        for terminate in [false, true] {
            let notice = RevocationNotice::new(Uuid::new_v4(), terminate);
            let parsed = RevocationNotice::from_wire(&notice.to_wire()).expect("parse");
            assert_eq!(parsed, notice);
        }
    }

    #[test]
    fn bare_task_id_is_a_non_terminating_revocation() {
        // The legacy `<queue>:cancel` payload. Reading it as `terminate: true`
        // would make `celers control revoke <id>` kill a running task, which
        // Celery's `revoke` (without `terminate=True`) must never do.
        let task_id = Uuid::new_v4();
        let parsed = RevocationNotice::from_wire(&task_id.to_string()).expect("parse");
        assert_eq!(parsed, RevocationNotice::ignore(task_id));
        assert!(!parsed.terminate);
    }

    #[test]
    fn surrounding_whitespace_is_tolerated() {
        let task_id = Uuid::new_v4();
        let parsed = RevocationNotice::from_wire(&format!("  {task_id}\n")).expect("parse");
        assert_eq!(parsed.task_id, task_id);

        let notice = RevocationNotice::terminate(task_id);
        let padded = format!("\n{}\t", notice.to_wire());
        assert_eq!(RevocationNotice::from_wire(&padded).expect("parse"), notice);
    }

    #[test]
    fn a_missing_terminate_flag_defaults_to_false() {
        let task_id = Uuid::new_v4();
        let body = format!("{{\"task_id\":\"{task_id}\"}}");
        let parsed = RevocationNotice::from_wire(&body).expect("parse");
        assert_eq!(parsed, RevocationNotice::ignore(task_id));
    }

    #[test]
    fn junk_is_reported_not_silently_ignored() {
        // A malformed notice must surface: silently dropping it is how a
        // revoked task keeps running with nothing in the log to say why.
        assert!(RevocationNotice::from_wire("not-a-uuid").is_err());
        assert!(RevocationNotice::from_wire("{ not json }").is_err());
        assert!(RevocationNotice::from_wire("").is_err());
    }
}
