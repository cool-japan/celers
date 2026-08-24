//! Visibility timeout implementation using Lua scripts
//!
//! This module provides Kombu-compatible visibility timeout for Redis,
//! ensuring tasks can be recovered if workers crash.
//!
//! # In-flight bookkeeping
//!
//! A delivered message lives in two structures until it is acknowledged:
//!
//! * `<queue>:processing` — a list holding the raw message. This is what the
//!   monitoring, query and backup modules read to show in-flight work.
//! * `<queue>:unacked` — a sorted set scoring the same message by the Unix
//!   timestamp at which its visibility timeout expires.
//!
//! [`VisibilityManager::pop_to_unacked`] writes both in a single `EVAL`, so a
//! message can never exist in neither the queue nor a recoverable structure.
//! The blocking `BRPOPLPUSH` fast path used by the broker stages onto the
//! processing list first and records the deadline immediately afterwards; if a
//! worker dies in that (sub-millisecond) window, [`VisibilityManager::reap`]
//! adopts the orphan and gives it a deadline, so recovery does not depend on
//! that second write ever happening.
//!
//! # Known limitation
//!
//! Both structures key on the message *body*. Two byte-identical messages
//! therefore collapse into one sorted-set entry, so the second copy is
//! recovered by the reaper rather than acknowledged directly. Task ids are
//! UUIDs, so identical bodies only arise if the same `SerializedTask` value is
//! enqueued twice.

use crate::lua_scripts;
use crate::QueueMode;
use redis::{aio::ConnectionLike, Script};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::warn;

/// Default maximum number of messages one reaper or promotion pass moves.
///
/// Bounds the work a single `EVAL` performs, so a large backlog can never
/// stall the (single-threaded) Redis server in one call.
pub const DEFAULT_SWEEP_BATCH: usize = 500;

/// The Redis keys one queue's atomic operations act on.
///
/// Grouping them keeps the script helpers to a readable arity and guarantees
/// that every call site uses the same naming scheme.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueKeys {
    /// Main queue (a list in FIFO mode, a sorted set in Priority mode)
    pub queue: String,
    /// Processing list holding delivered-but-unacknowledged messages
    pub processing: String,
    /// Sorted set scoring in-flight messages by visibility deadline
    pub unacked: String,
    /// Sorted set of delayed messages scored by execution time
    pub delayed: String,
    /// Dead letter queue
    pub dlq: String,
    /// Sorted set of revoked task ids scored by revocation expiry
    pub revoked: String,
    /// Key whose presence means the queue is paused
    pub pause: String,
}

impl QueueKeys {
    /// Derive the full key set from a queue name.
    ///
    /// The pause key deliberately comes from [`crate::queue_control`] so the
    /// broker and the queue controller can never disagree about it.
    pub fn new(queue_name: &str) -> Self {
        Self {
            queue: queue_name.to_string(),
            processing: format!("{}:processing", queue_name),
            unacked: format!("{}:unacked", queue_name),
            delayed: format!("{}:delayed", queue_name),
            dlq: format!("{}:dlq", queue_name),
            revoked: format!("{}:revoked", queue_name),
            pause: crate::queue_control::pause_key_for(queue_name),
        }
    }
}

/// Outcome of an atomic pop attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PopOutcome {
    /// A message was delivered and staged as in-flight
    Message(String),
    /// The queue held no deliverable message
    Empty,
    /// The queue is paused, so nothing may be delivered
    Paused,
}

impl PopOutcome {
    /// The delivered message, if any.
    pub fn message(self) -> Option<String> {
        match self {
            PopOutcome::Message(msg) => Some(msg),
            _ => None,
        }
    }

    /// Whether the queue is paused.
    pub fn is_paused(&self) -> bool {
        matches!(self, PopOutcome::Paused)
    }
}

/// What to do with a rejected message.
#[derive(Debug, Clone, Copy)]
pub enum NackAction<'a> {
    /// Put the (possibly rewritten) payload back at the *back* of the queue
    Requeue {
        /// Payload to re-enqueue; may differ from the delivered message
        /// (for example with a bumped retry count)
        payload: &'a str,
        /// Sorted-set score to requeue with (Priority mode only)
        score: f64,
    },
    /// Move the message to the dead letter queue
    DeadLetter,
}

/// Helper for executing Lua scripts with visibility timeout
pub struct VisibilityManager {
    pop_script: Script,
    ack_script: Script,
    nack_script: Script,
    recover_script: Script,
    pop_to_unacked_script: Script,
    pop_batch_script: Script,
    ack_unacked_script: Script,
    nack_unacked_script: Script,
    reap_script: Script,
    promote_script: Script,
    revoke_script: Script,
    replay_script: Script,
}

impl VisibilityManager {
    /// Create a manager with every broker script pre-hashed.
    pub fn new() -> Self {
        Self {
            pop_script: Script::new(lua_scripts::POP_WITH_VISIBILITY),
            ack_script: Script::new(lua_scripts::ACK_MESSAGE),
            nack_script: Script::new(lua_scripts::NACK_MESSAGE),
            recover_script: Script::new(lua_scripts::RECOVER_TIMED_OUT),
            pop_to_unacked_script: Script::new(lua_scripts::POP_TO_UNACKED),
            pop_batch_script: Script::new(lua_scripts::POP_BATCH_TO_UNACKED),
            ack_unacked_script: Script::new(lua_scripts::ACK_UNACKED),
            nack_unacked_script: Script::new(lua_scripts::NACK_UNACKED),
            reap_script: Script::new(lua_scripts::REAP_EXPIRED),
            promote_script: Script::new(lua_scripts::PROMOTE_DELAYED),
            revoke_script: Script::new(lua_scripts::REVOKE_TASK),
            replay_script: Script::new(lua_scripts::REPLAY_DLQ),
        }
    }

    /// Pop a message with visibility timeout (non-blocking)
    pub async fn pop_with_visibility<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        queue: &str,
        unacked_set: &str,
        visibility_timeout_secs: u64,
    ) -> redis::RedisResult<Option<String>> {
        let timeout_at = current_timestamp() + visibility_timeout_secs;

        let result: Option<String> = self
            .pop_script
            .key(queue)
            .key(unacked_set)
            .arg(timeout_at)
            .invoke_async(conn)
            .await?;

        Ok(result)
    }

    /// Pop a message with visibility timeout, waiting up to
    /// `block_timeout_secs` for one to arrive.
    ///
    /// Redis forbids blocking inside a script (the server is single-threaded,
    /// so a parked script would stall every other client), so the wait happens
    /// on the client: each individual attempt is the atomic
    /// [`Self::pop_with_visibility`] script, retried with exponential backoff
    /// until the deadline. Callers that need a true blocking pop should use
    /// `BRPOPLPUSH` on a dedicated connection and record the deadline
    /// afterwards, the way [`crate::RedisBroker`] does.
    pub async fn brpop_with_visibility<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        queue: &str,
        unacked_set: &str,
        visibility_timeout_secs: u64,
        block_timeout_secs: u64,
    ) -> redis::RedisResult<Option<String>> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(block_timeout_secs);
        let mut backoff = Duration::from_millis(5);
        let max_backoff = Duration::from_millis(100);

        loop {
            if let Some(message) = self
                .pop_with_visibility(conn, queue, unacked_set, visibility_timeout_secs)
                .await?
            {
                return Ok(Some(message));
            }

            let now = tokio::time::Instant::now();
            if now >= deadline {
                return Ok(None);
            }

            let sleep_for = backoff.min(deadline - now);
            tokio::time::sleep(sleep_for).await;
            backoff = (backoff * 2).min(max_backoff);
        }
    }

    /// Acknowledge a message (remove from unacked set)
    pub async fn ack<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        unacked_set: &str,
        message: &str,
    ) -> redis::RedisResult<i64> {
        let removed: i64 = self
            .ack_script
            .key(unacked_set)
            .arg(message)
            .invoke_async(conn)
            .await?;

        Ok(removed)
    }

    /// Reject a message (send to DLQ or requeue)
    pub async fn nack<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        unacked_set: &str,
        queue: &str,
        dlq: &str,
        message: &str,
        requeue: bool,
    ) -> redis::RedisResult<String> {
        let result: String = self
            .nack_script
            .key(unacked_set)
            .key(queue)
            .key(dlq)
            .arg(message)
            .arg(if requeue { "1" } else { "0" })
            .invoke_async(conn)
            .await?;

        Ok(result)
    }

    /// Recover timed-out messages (move back to queue)
    pub async fn recover_timed_out<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        unacked_set: &str,
        queue: &str,
        max_count: usize,
    ) -> redis::RedisResult<i64> {
        let current_time = current_timestamp();

        let recovered: i64 = self
            .recover_script
            .key(unacked_set)
            .key(queue)
            .arg(current_time)
            .arg(max_count)
            .invoke_async(conn)
            .await?;

        Ok(recovered)
    }

    /// Atomically dequeue one message and register its visibility deadline.
    ///
    /// Revoked messages are dropped rather than delivered; `max_attempts`
    /// bounds how many of them a single call will discard.
    pub async fn pop_to_unacked<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        keys: &QueueKeys,
        mode: QueueMode,
        visibility_timeout_secs: u64,
        max_attempts: usize,
    ) -> redis::RedisResult<PopOutcome> {
        let now = current_timestamp();
        let deadline = now + visibility_timeout_secs;

        let (code, message): (i64, String) = self
            .pop_to_unacked_script
            .key(&keys.queue)
            .key(&keys.processing)
            .key(&keys.unacked)
            .key(&keys.revoked)
            .key(&keys.pause)
            .arg(deadline)
            .arg(mode_arg(mode))
            .arg(max_attempts.max(1))
            .arg(now)
            .invoke_async(conn)
            .await?;

        Ok(match code {
            0 => PopOutcome::Message(message),
            2 => PopOutcome::Paused,
            _ => PopOutcome::Empty,
        })
    }

    /// Atomically dequeue up to `count` messages, registering a visibility
    /// deadline for each.
    pub async fn pop_batch_to_unacked<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        keys: &QueueKeys,
        mode: QueueMode,
        visibility_timeout_secs: u64,
        count: usize,
    ) -> redis::RedisResult<Vec<String>> {
        if count == 0 {
            return Ok(Vec::new());
        }

        let now = current_timestamp();
        let deadline = now + visibility_timeout_secs;

        let messages: Vec<String> = self
            .pop_batch_script
            .key(&keys.queue)
            .key(&keys.processing)
            .key(&keys.unacked)
            .key(&keys.revoked)
            .key(&keys.pause)
            .arg(deadline)
            .arg(mode_arg(mode))
            .arg(count)
            .arg(now)
            .arg(count)
            .invoke_async(conn)
            .await?;

        Ok(messages)
    }

    /// Record a visibility deadline for a message that was staged onto the
    /// processing list by a blocking `BRPOPLPUSH`.
    pub async fn register_unacked<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        keys: &QueueKeys,
        message: &str,
        visibility_timeout_secs: u64,
    ) -> redis::RedisResult<()> {
        let deadline = current_timestamp() + visibility_timeout_secs;
        redis::cmd("ZADD")
            .arg(&keys.unacked)
            .arg(deadline)
            .arg(message)
            .query_async::<()>(conn)
            .await
    }

    /// Acknowledge a message delivered by [`Self::pop_to_unacked`], clearing
    /// both in-flight structures.
    pub async fn ack_unacked<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        keys: &QueueKeys,
        message: &str,
    ) -> redis::RedisResult<i64> {
        self.ack_unacked_script
            .key(&keys.unacked)
            .key(&keys.processing)
            .arg(message)
            .invoke_async(conn)
            .await
    }

    /// Reject a message delivered by [`Self::pop_to_unacked`].
    pub async fn nack_unacked<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        keys: &QueueKeys,
        message: &str,
        action: NackAction<'_>,
        mode: QueueMode,
    ) -> redis::RedisResult<String> {
        let (requeue, payload, score) = match action {
            NackAction::Requeue { payload, score } => ("1", payload, score),
            NackAction::DeadLetter => ("0", message, 0.0),
        };

        self.nack_unacked_script
            .key(&keys.unacked)
            .key(&keys.processing)
            .key(&keys.queue)
            .key(&keys.dlq)
            .arg(message)
            .arg(payload)
            .arg(requeue)
            .arg(mode_arg(mode))
            .arg(score)
            .invoke_async(conn)
            .await
    }

    /// Requeue in-flight messages whose visibility timeout has expired, and
    /// adopt any orphan staged on the processing list without a deadline.
    ///
    /// Returns the number of messages put back on the queue.
    pub async fn reap<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        keys: &QueueKeys,
        mode: QueueMode,
        visibility_timeout_secs: u64,
        max_count: usize,
    ) -> redis::RedisResult<i64> {
        let now = current_timestamp();

        self.reap_script
            .key(&keys.unacked)
            .key(&keys.queue)
            .key(&keys.processing)
            .arg(now)
            .arg(now + visibility_timeout_secs)
            .arg(max_count.max(1))
            .arg(mode_arg(mode))
            .invoke_async(conn)
            .await
    }

    /// Move delayed tasks that are due onto the main queue.
    ///
    /// Returns the number of messages promoted.
    pub async fn promote_delayed<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        keys: &QueueKeys,
        mode: QueueMode,
        max_count: usize,
    ) -> redis::RedisResult<i64> {
        self.promote_script
            .key(&keys.delayed)
            .key(&keys.queue)
            .arg(current_timestamp())
            .arg(max_count.max(1))
            .arg(mode_arg(mode))
            .invoke_async(conn)
            .await
    }

    /// Durably record a revocation and remove pending copies of the task.
    ///
    /// Returns the number of copies removed from the pending structures.
    pub async fn revoke<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        keys: &QueueKeys,
        task_id: &str,
        mode: QueueMode,
        ttl_secs: u64,
        scan_limit: usize,
    ) -> redis::RedisResult<i64> {
        self.revoke_script
            .key(&keys.revoked)
            .key(&keys.queue)
            .key(&keys.delayed)
            .arg(task_id)
            .arg(current_timestamp())
            .arg(ttl_secs)
            .arg(mode_arg(mode))
            .arg(scan_limit.max(1))
            .invoke_async(conn)
            .await
    }

    /// Check whether a task id is currently revoked.
    pub async fn is_revoked<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        keys: &QueueKeys,
        task_id: &str,
    ) -> redis::RedisResult<bool> {
        let expiry: Option<f64> = redis::cmd("ZSCORE")
            .arg(&keys.revoked)
            .arg(task_id)
            .query_async(conn)
            .await?;

        Ok(expiry.is_some_and(|expiry| expiry > current_timestamp() as f64))
    }

    /// Move a batch of dead-letter entries back onto the main queue.
    ///
    /// Each triple is `(original payload, replacement payload, score)`; the
    /// claim and the re-enqueue happen in a single server-side step.
    pub async fn replay_dlq<C: ConnectionLike + Send>(
        &self,
        conn: &mut C,
        keys: &QueueKeys,
        mode: QueueMode,
        entries: &[(String, String, f64)],
    ) -> redis::RedisResult<i64> {
        if entries.is_empty() {
            return Ok(0);
        }

        let mut invocation = self.replay_script.key(&keys.dlq);
        invocation.key(&keys.queue);
        invocation.arg(mode_arg(mode));
        for (original, replacement, score) in entries {
            invocation.arg(original).arg(replacement).arg(*score);
        }

        invocation.invoke_async(conn).await
    }
}

impl Default for VisibilityManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Script argument spelling for a queue mode.
fn mode_arg(mode: QueueMode) -> &'static str {
    if mode.is_priority() {
        "priority"
    } else {
        "fifo"
    }
}

/// Get current Unix timestamp in seconds
///
/// A backwards clock step (NTP correction, VM snapshot restore, container
/// resync) must never abort a worker, so the pre-epoch case degrades to `0`
/// instead of panicking: every in-flight deadline then looks expired and the
/// affected messages are redelivered, which is the safe direction.
fn current_timestamp() -> u64 {
    match SystemTime::now().duration_since(UNIX_EPOCH) {
        Ok(elapsed) => elapsed.as_secs(),
        Err(e) => {
            warn!("System clock is before the Unix epoch ({e}); treating now as 0");
            0
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_visibility_manager_creation() {
        let manager = VisibilityManager::new();
        // Verify scripts are initialized
        assert!(!manager.pop_script.get_hash().is_empty());
        assert!(!manager.pop_to_unacked_script.get_hash().is_empty());
        assert!(!manager.reap_script.get_hash().is_empty());
        assert!(!manager.replay_script.get_hash().is_empty());
    }

    #[test]
    fn test_current_timestamp() {
        let ts = current_timestamp();
        assert!(ts > 1_600_000_000); // After Sep 2020
        assert!(ts < 2_000_000_000); // Before May 2033
    }

    #[test]
    fn test_queue_keys_derivation() {
        let keys = QueueKeys::new("orders");
        assert_eq!(keys.queue, "orders");
        assert_eq!(keys.processing, "orders:processing");
        assert_eq!(keys.unacked, "orders:unacked");
        assert_eq!(keys.delayed, "orders:delayed");
        assert_eq!(keys.dlq, "orders:dlq");
        assert_eq!(keys.revoked, "orders:revoked");
        // Must match the queue controller's own key, or pause is a no-op.
        assert_eq!(keys.pause, crate::queue_control::pause_key_for("orders"));
    }

    #[test]
    fn test_pop_outcome_accessors() {
        assert_eq!(
            PopOutcome::Message("m".to_string()).message(),
            Some("m".to_string())
        );
        assert_eq!(PopOutcome::Empty.message(), None);
        assert_eq!(PopOutcome::Paused.message(), None);
        assert!(PopOutcome::Paused.is_paused());
        assert!(!PopOutcome::Empty.is_paused());
    }

    #[test]
    fn test_mode_arg_spelling() {
        // These strings are compared inside the Lua scripts; a typo here
        // silently downgrades Priority mode to FIFO handling.
        assert_eq!(mode_arg(QueueMode::Priority), "priority");
        assert_eq!(mode_arg(QueueMode::Fifo), "fifo");
        assert!(lua_scripts::POP_TO_UNACKED.contains("== 'priority'"));
        assert!(lua_scripts::REAP_EXPIRED.contains("== 'priority'"));
        assert!(lua_scripts::PROMOTE_DELAYED.contains("== 'priority'"));
    }
}
