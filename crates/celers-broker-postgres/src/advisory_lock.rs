//! Session-scoped PostgreSQL advisory locks, pinned to one pooled connection.
//!
//! # Why a guard type is the only sound API
//!
//! `pg_advisory_lock` / `pg_advisory_unlock` are **session**-scoped: the lock
//! belongs to the backend session that took it, and only that session can
//! release it. [`crate::pool::PgPool`] owns several independent connections
//! and hands out whichever slot is free per statement, so an API shaped like
//!
//! ```text
//! broker.try_advisory_lock(id).await?;   // slot 2 takes the lock
//! // ... critical section ...
//! broker.release_advisory_lock(id).await?; // slot 0 tries to release it
//! ```
//!
//! does not work and cannot be made to work. `pg_advisory_unlock` on a
//! session that does not hold the lock returns `false` (with a
//! `WARNING: you don't own a lock of type ExclusiveLock`) and releases
//! nothing; the lock stays held on slot 2 until that connection is closed —
//! which, for a pooled broker, is process exit. Every subsequent acquisition
//! attempt from any other process then blocks forever on a lock nobody
//! believes they are holding.
//!
//! [`AdvisoryLockGuard`] fixes that by owning the
//! [`crate::pool::PooledConnection`] the lock was taken on for the lock's
//! whole lifetime — the same pattern [`crate::PostgresBroker::migrate`] uses
//! internally for [`crate::MIGRATION_ADVISORY_LOCK_ID`]. The slot cannot be
//! handed to anyone else while the guard lives, so the release is guaranteed
//! to reach the session that took the lock.
//!
//! # Releasing: explicit `release()`, and what `Drop` can do instead
//!
//! `Drop` is not `async` and this crate holds no runtime handle it could
//! block on, so the guard **cannot** issue `pg_advisory_unlock` on the way
//! out. [`AdvisoryLockGuard::release`] is therefore the documented path, and
//! it returns the server's own answer (`true` when a lock was really
//! released).
//!
//! A guard that is dropped without it is not silently ignored either. Simply
//! returning the connection to the pool would be the worst outcome: the next
//! operation to pick up that slot would inherit a session still holding an
//! advisory lock nobody tracks. So an un-released drop instead
//! [`crate::pool::PooledConnection::mark_broken`]s the slot and logs a
//! warning — the next acquisition of that slot closes the old connection and
//! reconnects, and closing the session is what actually releases the lock.
//! The lock is thus bounded by the pool's next use of that slot rather than
//! by process lifetime, and no unrelated statement ever runs on a session
//! carrying a stray lock.

use std::time::{Duration, Instant};

use celers_core::{CelersError, Result};

use crate::pool::PooledConnection;
use crate::row_ext::RowExt;
use crate::PostgresBroker;

/// First delay between `pg_try_advisory_lock` attempts in a bounded
/// acquisition.
const LOCK_RETRY_BASE_DELAY_MS: u64 = 25;

/// Ceiling for the bounded-acquisition backoff. Small enough that a lock
/// released early is picked up promptly, large enough that a long wait is not
/// a busy loop against the server.
const LOCK_RETRY_MAX_DELAY_MS: u64 = 500;

/// Default overall deadline for [`PostgresBroker::migrate`]'s lock
/// acquisition.
///
/// A migration set takes well under a second on any realistic database, so a
/// caller still waiting after a minute is queued behind something that is
/// stuck, not slow — and blocking a worker fleet's start-up forever on it is
/// strictly worse than failing with an actionable error.
pub const DEFAULT_MIGRATION_LOCK_TIMEOUT: Duration = Duration::from_secs(60);

/// Exponential backoff for the `n`-th consecutive failed lock attempt.
fn retry_delay_ms(attempts: u32) -> u64 {
    let shift = attempts.saturating_sub(1).min(10);
    LOCK_RETRY_BASE_DELAY_MS
        .saturating_mul(1u64 << shift)
        .min(LOCK_RETRY_MAX_DELAY_MS)
}

/// Take `lock_id` on `conn`'s session, blocking until it is available.
async fn lock_blocking(conn: &PooledConnection, lock_id: i64) -> Result<()> {
    conn.execute("SELECT pg_advisory_lock($1)", &[&lock_id])
        .await
        .map_err(|e| {
            CelersError::Other(format!("Failed to acquire advisory lock {lock_id}: {e}"))
        })?;
    Ok(())
}

/// One `pg_try_advisory_lock` attempt on `conn`'s session.
async fn try_lock_once(conn: &PooledConnection, lock_id: i64) -> Result<bool> {
    let rows = conn
        .query("SELECT pg_try_advisory_lock($1) AS locked", &[&lock_id])
        .await
        .map_err(|e| {
            CelersError::Other(format!("Failed to acquire advisory lock {lock_id}: {e}"))
        })?;
    let row = rows.into_iter().next().ok_or_else(|| {
        CelersError::Other(format!(
            "Failed to acquire advisory lock {lock_id}: no rows returned"
        ))
    })?;
    row.col("locked")
        .map_err(|e| CelersError::Other(format!("Failed to read locked: {e}")))
}

/// Take `lock_id` on `conn`'s session within `deadline`, or fail.
///
/// Retries `pg_try_advisory_lock` with exponential backoff rather than
/// issuing the blocking `pg_advisory_lock`, because the blocking form has no
/// client-side escape: once the server is waiting on the lock the statement
/// cannot be abandoned without dropping the connection, so a caller queued
/// behind a stuck holder waits forever.
///
/// The first attempt always runs, so a zero deadline degrades to a single
/// `try` rather than to an unconditional failure.
pub(crate) async fn lock_within(
    conn: &PooledConnection,
    lock_id: i64,
    deadline: Duration,
) -> Result<()> {
    let started = Instant::now();
    let mut attempts: u32 = 0;

    loop {
        attempts = attempts.saturating_add(1);
        if try_lock_once(conn, lock_id).await? {
            if attempts > 1 {
                tracing::debug!(
                    lock_id,
                    attempts,
                    waited_ms = started.elapsed().as_millis(),
                    "advisory lock acquired after waiting"
                );
            }
            return Ok(());
        }

        let elapsed = started.elapsed();
        if elapsed >= deadline {
            return Err(CelersError::Other(format!(
                // The `pg_locks` recipe reassembles the 64-bit key from the
                // two `oid` columns it is split across — see
                // `PostgresBroker::is_advisory_lock_held` for why a bare
                // `objid = {lock_id}` is wrong for any key wider than 32 bits.
                "Timed out after {elapsed:?} waiting for advisory lock {lock_id} \
                 ({attempts} attempts); another session is holding it — find it with: \
                 SELECT pid FROM pg_locks WHERE locktype = 'advisory' AND objsubid = 1 \
                 AND ((classid::bigint << 32) | objid::bigint) = {lock_id}"
            )));
        }

        // Never sleep past the deadline: an overshoot would report a wait
        // longer than the caller asked for.
        let remaining = deadline - elapsed;
        let delay = Duration::from_millis(retry_delay_ms(attempts)).min(remaining);
        tokio::time::sleep(delay).await;
    }
}

/// A held PostgreSQL advisory lock, pinned to the connection that took it.
///
/// Obtained from [`PostgresBroker::acquire_advisory_lock`],
/// [`PostgresBroker::try_acquire_advisory_lock`] or
/// [`PostgresBroker::acquire_advisory_lock_within`]. Call
/// [`release`](Self::release) when the critical section is done; see this
/// module's header for what an un-released drop does instead.
///
/// The guard holds one of the broker's pool slots for as long as it lives, so
/// a broker with `max_connections = 1` that is holding a guard has no
/// connection left for anything else. Size the pool accordingly, or use a
/// dedicated broker for locking.
pub struct AdvisoryLockGuard {
    /// `None` only after [`release`](Self::release) has taken it.
    conn: Option<PooledConnection>,
    lock_id: i64,
}

impl std::fmt::Debug for AdvisoryLockGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AdvisoryLockGuard")
            .field("lock_id", &self.lock_id)
            .field("released", &self.conn.is_none())
            .finish()
    }
}

impl AdvisoryLockGuard {
    /// The lock key this guard holds.
    #[must_use]
    pub fn lock_id(&self) -> i64 {
        self.lock_id
    }

    /// Release the lock on the session that took it.
    ///
    /// Returns the server's own answer: `true` when a lock was really
    /// released, `false` when the session turned out not to hold one (which
    /// should be unreachable through this type and is surfaced rather than
    /// hidden).
    ///
    /// # Errors
    ///
    /// Returns an error if the `pg_advisory_unlock` statement itself fails.
    /// The connection is marked broken in that case, so the slot is recycled
    /// and the session — and with it the lock — is closed rather than being
    /// handed to an unrelated statement.
    pub async fn release(mut self) -> Result<bool> {
        let lock_id = self.lock_id;
        let conn = match self.conn.take() {
            Some(conn) => conn,
            // Unreachable: `release` consumes `self`, so it cannot run twice.
            None => return Ok(false),
        };

        let rows = match conn
            .query("SELECT pg_advisory_unlock($1) AS unlocked", &[&lock_id])
            .await
        {
            Ok(rows) => rows,
            Err(e) => {
                conn.mark_broken();
                return Err(CelersError::Other(format!(
                    "Failed to release advisory lock {lock_id}: {e}"
                )));
            }
        };

        let unlocked: bool = match rows.into_iter().next() {
            Some(row) => row
                .col("unlocked")
                .map_err(|e| CelersError::Other(format!("Failed to read unlocked: {e}")))?,
            None => {
                conn.mark_broken();
                return Err(CelersError::Other(format!(
                    "Failed to release advisory lock {lock_id}: no rows returned"
                )));
            }
        };

        if unlocked {
            tracing::debug!(lock_id, "advisory lock released");
        } else {
            tracing::warn!(
                lock_id,
                "pg_advisory_unlock reported the session did not hold the lock"
            );
        }
        Ok(unlocked)
    }
}

impl Drop for AdvisoryLockGuard {
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // `Drop` cannot await, so the unlock statement cannot be issued
            // here. Returning the slot as-is would hand the next caller a
            // session that still holds the lock; marking it broken makes the
            // next acquisition of that slot reconnect, and closing the old
            // session is what releases the lock server-side.
            conn.mark_broken();
            tracing::warn!(
                lock_id = self.lock_id,
                slot = conn.slot_index(),
                "AdvisoryLockGuard dropped without release(); the pool slot is \
                 marked for reconnection, which releases the lock when the old \
                 session is closed. Call release().await for a prompt, \
                 verifiable unlock."
            );
        }
    }
}

impl PostgresBroker {
    /// Take a session-scoped advisory lock, waiting until it is available.
    ///
    /// The returned [`AdvisoryLockGuard`] pins the connection the lock was
    /// taken on, which is what makes the matching release land on the right
    /// session — see this module's header for why the pool makes any other
    /// shape unsound.
    ///
    /// This waits indefinitely. Prefer
    /// [`acquire_advisory_lock_within`](Self::acquire_advisory_lock_within)
    /// anywhere a stuck holder must not wedge the caller forever.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let lock = broker.acquire_advisory_lock(12345).await?;
    /// // ... critical section: only one session in the cluster gets here ...
    /// lock.release().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire_advisory_lock(&self, lock_id: i64) -> Result<AdvisoryLockGuard> {
        let conn = self.connection().await?;
        lock_blocking(&conn, lock_id).await?;
        tracing::debug!(lock_id, "advisory lock acquired (blocking)");
        Ok(AdvisoryLockGuard {
            conn: Some(conn),
            lock_id,
        })
    }

    /// Take a session-scoped advisory lock if it is free right now.
    ///
    /// Returns `Ok(None)` when another session holds it — never blocks.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// match broker.try_acquire_advisory_lock(12345).await? {
    ///     Some(lock) => {
    ///         // ... critical section ...
    ///         lock.release().await?;
    ///     }
    ///     None => println!("lock held by another worker"),
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn try_acquire_advisory_lock(
        &self,
        lock_id: i64,
    ) -> Result<Option<AdvisoryLockGuard>> {
        let conn = self.connection().await?;
        if try_lock_once(&conn, lock_id).await? {
            tracing::debug!(lock_id, "advisory lock acquired");
            Ok(Some(AdvisoryLockGuard {
                conn: Some(conn),
                lock_id,
            }))
        } else {
            tracing::debug!(lock_id, "advisory lock already held elsewhere");
            Ok(None)
        }
    }

    /// Take a session-scoped advisory lock, giving up after `deadline`.
    ///
    /// Retries `pg_try_advisory_lock` with exponential backoff instead of
    /// issuing the blocking `pg_advisory_lock`, because the blocking form
    /// cannot be abandoned client-side once the server is waiting on it.
    ///
    /// # Errors
    ///
    /// Returns an error naming `lock_id` and the elapsed wait if the lock is
    /// still held when `deadline` expires.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use std::time::Duration;
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let lock = broker
    ///     .acquire_advisory_lock_within(12345, Duration::from_secs(5))
    ///     .await?;
    /// lock.release().await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn acquire_advisory_lock_within(
        &self,
        lock_id: i64,
        deadline: Duration,
    ) -> Result<AdvisoryLockGuard> {
        let conn = self.connection().await?;
        lock_within(&conn, lock_id, deadline).await?;
        Ok(AdvisoryLockGuard {
            conn: Some(conn),
            lock_id,
        })
    }
}

// ── Deprecated, but no longer unsound ───────────────────────────────────────
//
// The three methods below predate [`AdvisoryLockGuard`] and used to issue
// their `pg_advisory_lock` / `pg_advisory_unlock` straight through
// [`crate::pool::PgPool`], which hands out a different connection per
// statement — so an acquire and its release usually landed on different
// backend sessions, the unlock returned `false` without releasing anything,
// and the lock stayed held on whichever pooled connection had taken it until
// the process exited.
//
// They keep their signatures (nothing in the workspace calls them, but they
// are public API) and are now backed by
// [`crate::PostgresBroker::advisory_locks`]: the acquire parks the pinned
// connection in that map, the release takes it back out and unlocks on that
// exact connection. That makes the pair correct.
//
// They stay `#[deprecated]` all the same, because a correct implementation
// cannot give them what the guard has: there is no RAII, so a caller that
// returns early or panics between the two calls pins one of the broker's
// pool slots for the broker's whole lifetime, with nothing to notice it.

impl PostgresBroker {
    /// Acquire a PostgreSQL advisory lock for exclusive task processing, if
    /// it is free.
    ///
    /// Returns `true` if the lock was acquired, `false` if another session
    /// holds it. If *this broker* already holds `lock_id`, this returns
    /// `true` without taking a second lock — a second acquisition would run
    /// on a different pooled connection, i.e. a different session, and would
    /// therefore block on the broker's own lock forever.
    ///
    /// # Deprecated
    ///
    /// Use [`PostgresBroker::try_acquire_advisory_lock`], which returns an
    /// [`AdvisoryLockGuard`] that owns the connection the lock lives on.
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// if let Some(lock) = broker.try_acquire_advisory_lock(12345).await? {
    ///     // ... critical section ...
    ///     lock.release().await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    #[deprecated(
        since = "0.3.1",
        note = "a session-scoped advisory lock must keep the connection it was taken on; \
                use try_acquire_advisory_lock, which returns an AdvisoryLockGuard"
    )]
    pub async fn try_advisory_lock(&self, lock_id: i64) -> Result<bool> {
        // Check out the pool slot *before* taking the registry mutex: a
        // broker whose slots are all pinned by held locks would otherwise
        // block here while holding the mutex that `release_advisory_lock`
        // needs to give a slot back.
        let conn = self.connection().await?;
        let mut held = self.advisory_locks.lock().await;
        if held.contains_key(&lock_id) {
            tracing::debug!(lock_id, "advisory lock already held by this broker");
            return Ok(true);
        }
        if try_lock_once(&conn, lock_id).await? {
            held.insert(lock_id, conn);
            tracing::debug!(lock_id, "advisory lock acquired");
            Ok(true)
        } else {
            tracing::debug!(lock_id, "advisory lock already held elsewhere");
            Ok(false)
        }
    }

    /// Acquire a PostgreSQL advisory lock, waiting until it is available.
    ///
    /// Returns immediately if this broker already holds `lock_id` — see
    /// `try_advisory_lock` for why a second acquisition would self-deadlock.
    ///
    /// # Deprecated
    ///
    /// Use [`PostgresBroker::acquire_advisory_lock`] (or
    /// [`PostgresBroker::acquire_advisory_lock_within`] for a bounded wait),
    /// which return an [`AdvisoryLockGuard`] that owns the connection the
    /// lock lives on.
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let lock = broker.acquire_advisory_lock(12345).await?;
    /// // ... critical section ...
    /// lock.release().await?;
    /// # Ok(())
    /// # }
    /// ```
    #[deprecated(
        since = "0.3.1",
        note = "a session-scoped advisory lock must keep the connection it was taken on; \
                use acquire_advisory_lock, which returns an AdvisoryLockGuard"
    )]
    pub async fn advisory_lock(&self, lock_id: i64) -> Result<()> {
        if self.advisory_locks.lock().await.contains_key(&lock_id) {
            tracing::debug!(lock_id, "advisory lock already held by this broker");
            return Ok(());
        }

        // The registry mutex is deliberately NOT held across the blocking
        // acquisition: `pg_advisory_lock` can wait arbitrarily long on
        // another process, and holding the mutex would stall every
        // `release_advisory_lock` on this broker — including releases of
        // unrelated lock ids that might be what the other process is waiting
        // for.
        let conn = self.connection().await?;
        lock_blocking(&conn, lock_id).await?;

        let mut held = self.advisory_locks.lock().await;
        if held.contains_key(&lock_id) {
            // Another call on this broker registered the same id while this
            // one was waiting, so this session now holds a *second*,
            // independent lock on it. Give it straight back so the registry
            // stays a faithful record of what is held.
            if let Err(e) = conn
                .execute("SELECT pg_advisory_unlock($1)", &[&lock_id])
                .await
            {
                conn.mark_broken();
                tracing::warn!(lock_id, error = %e, "failed to release a duplicate advisory lock acquisition");
            }
            return Ok(());
        }
        held.insert(lock_id, conn);
        tracing::debug!(lock_id, "blocking advisory lock acquired");
        Ok(())
    }

    /// Release a PostgreSQL advisory lock this broker holds.
    ///
    /// Returns `true` when a lock was really released. `false` means this
    /// broker was not holding `lock_id` — including the case where some
    /// *other* session holds it, which this broker has no way to release.
    ///
    /// # Deprecated
    ///
    /// Use [`AdvisoryLockGuard::release`].
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// # let broker = PostgresBroker::new("postgres://localhost/db").await?;
    /// let lock = broker.acquire_advisory_lock(12345).await?;
    /// let released = lock.release().await?;
    /// assert!(released);
    /// # Ok(())
    /// # }
    /// ```
    #[deprecated(
        since = "0.3.1",
        note = "a session-scoped advisory lock must be released on the connection it was \
                taken on; use AdvisoryLockGuard::release"
    )]
    pub async fn release_advisory_lock(&self, lock_id: i64) -> Result<bool> {
        let conn = self.advisory_locks.lock().await.remove(&lock_id);
        let conn = match conn {
            Some(conn) => conn,
            None => {
                tracing::warn!(
                    lock_id,
                    "release_advisory_lock: this broker does not hold that lock"
                );
                return Ok(false);
            }
        };

        AdvisoryLockGuard {
            conn: Some(conn),
            lock_id,
        }
        .release()
        .await
    }

    /// Check whether an advisory lock is currently held **by any session** on
    /// the server.
    ///
    /// This reads the cluster-wide `pg_locks` view, so it is unaffected by
    /// which connection asks and is equally valid for locks held through
    /// [`AdvisoryLockGuard`] and through the deprecated trio above.
    ///
    /// # Why the key is reassembled rather than compared to `objid`
    ///
    /// `pg_locks` does not store a 64-bit advisory key in one column. For the
    /// single-argument `pg_advisory_lock(bigint)` form PostgreSQL splits it
    /// across two `oid` columns — `classid` gets the high 32 bits, `objid`
    /// the low 32 — and stamps `objsubid = 1`; the two-argument
    /// `pg_advisory_lock(int, int)` form uses `objsubid = 2` instead.
    ///
    /// A plain `WHERE objid = $1` therefore only answers correctly for keys
    /// that happen to fit in 32 bits. It silently reports `false` for
    /// anything larger — [`crate::MIGRATION_ADVISORY_LOCK_ID`]
    /// (`0x6365_6c65_7273_0001`) included, whose low half is `0x7273_0001` —
    /// and would report a *false positive* for any unrelated key sharing
    /// those low 32 bits. Reassembling `(classid << 32) | objid` is exact:
    /// PostgreSQL's `int8` shift wraps rather than erroring, so the round trip
    /// is lossless even for keys with the top bit set.
    ///
    /// `granted` excludes sessions that are merely queued behind the holder,
    /// and the `database` filter keeps the answer to this database — advisory
    /// locks are scoped per database, so an identical key held in another one
    /// is not this lock.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use celers_broker_postgres::PostgresBroker;
    ///
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let broker = PostgresBroker::new("postgres://localhost/db").await?;
    ///
    /// let is_locked = broker.is_advisory_lock_held(12345).await?;
    /// println!("Lock held: {}", is_locked);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn is_advisory_lock_held(&self, lock_id: i64) -> Result<bool> {
        let rows = self
            .conn
            .query(
                r#"
            SELECT COUNT(*) > 0 as held
            FROM pg_locks
            WHERE locktype = 'advisory'
              AND objsubid = 1
              AND granted
              AND database = (SELECT oid FROM pg_database WHERE datname = current_database())
              AND ((classid::bigint << 32) | objid::bigint) = $1
            "#,
                &[&lock_id],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to check advisory lock: {}", e)))?;

        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to check advisory lock: no rows returned".to_string())
        })?;
        let held: bool = row
            .col("held")
            .map_err(|e| CelersError::Other(format!("Failed to read held: {}", e)))?;
        Ok(held)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn retry_delay_grows_exponentially_and_is_capped() {
        assert_eq!(retry_delay_ms(0), LOCK_RETRY_BASE_DELAY_MS);
        assert_eq!(retry_delay_ms(1), LOCK_RETRY_BASE_DELAY_MS);
        assert_eq!(retry_delay_ms(2), LOCK_RETRY_BASE_DELAY_MS * 2);
        assert_eq!(retry_delay_ms(3), LOCK_RETRY_BASE_DELAY_MS * 4);
        assert_eq!(retry_delay_ms(40), LOCK_RETRY_MAX_DELAY_MS);

        let mut previous = 0;
        for attempts in 0..64 {
            let current = retry_delay_ms(attempts);
            assert!(current >= previous, "backoff regressed at {attempts}");
            assert!(current <= LOCK_RETRY_MAX_DELAY_MS);
            previous = current;
        }
    }

    #[test]
    fn the_default_migration_lock_timeout_is_bounded_and_generous() {
        // The point of the constant is that it is finite; the exact value only
        // has to be far longer than any real migration set.
        assert!(DEFAULT_MIGRATION_LOCK_TIMEOUT >= Duration::from_secs(10));
        assert!(DEFAULT_MIGRATION_LOCK_TIMEOUT <= Duration::from_secs(600));
    }
}
