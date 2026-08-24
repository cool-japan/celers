//! A small internal round-robin connection pool for PostgreSQL, with
//! reconnect-on-failure.
//!
//! `oxisql_postgres::PgConnection` wraps a single `Arc<Mutex<tokio_postgres::
//! Client>>` — cheap to `Clone`, but every clone shares the *same*
//! underlying connection and mutex (confirmed against `oxisql-postgres`'s own
//! doc comments: "The inner `Client` is wrapped in `Arc<Mutex<_>>` to allow
//! the `Connection` trait's `&self` transaction method to obtain exclusive
//! mutable access"). `PostgresResultBackend`/`PostgresAnalytics` cloning that
//! one connection on every `ResultStore` call (see `result_store.rs`) means
//! every concurrent read/write from every worker in the process serializes
//! behind one lock, and — since nothing ever calls `PgConnection::reconnect`
//! — a single database restart or network blip leaves every subsequent
//! operation returning `BackendError::Connection` forever, until the process
//! itself is restarted.
//!
//! [`PgConnPool`] closes both gaps: it holds `size` independent
//! `PgConnection`s (independent TCP connections, independent mutexes), hands
//! one out per operation using a round-robin cursor so concurrent callers no
//! longer serialize behind a single lock, and transparently reconnects a
//! slot whose connection looks dead.
//!
//! # Retry policy
//!
//! - [`PgConnPool::query`] (read-only by construction) transparently retries
//!   once against the freshly reconnected slot on a connection-loss error —
//!   always safe, since a `SELECT` has no side effects to duplicate.
//! - [`PgConnPool::execute`] / [`PgConnPool::execute_batch`] (DML/DDL, not
//!   provably idempotent in general) heal the slot for *next time* but do
//!   **not** retry the failed call itself, to avoid the classic distributed-
//!   systems ambiguity of "did the write already happen server-side before
//!   the response was lost" turning into a silently duplicated side effect
//!   (e.g. a duplicated chord-counter increment). This still eliminates the
//!   "broken forever after one connection drop" failure mode: the *next*
//!   attempt (whether an immediate caller-level retry or later work) gets a
//!   healthy connection instead of the same permanently dead one.
//! - [`PgConnPool::get`] hands out an owned, round-robin-picked
//!   [`PgConnection`] for callers that need [`PgConnection::transaction`]
//!   (which must stay pinned to one connection for its whole lifetime by
//!   definition, so it is deliberately not wrapped by this pool — mirroring
//!   `oxisql_mysql`'s own documented caution that errors inside an active
//!   transaction must not be blindly retried, since the transaction's
//!   server-side state is unknown after a failure).
//!
//! This is intentionally simple — no async connection-acquisition queue, no
//! background health-check task, no external pooling dependency — matching
//! the audit guidance that a lightweight, self-contained fix was preferred
//! here over pulling in a new dependency for this one crate.

use oxisql_core::{Connection, OxiSqlError, Row, ToSqlValue};
use oxisql_postgres::{PgConnection, PgError, TlsMode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

/// Default number of independent connections held by a [`PgConnPool`]
/// created via [`PgConnPool::connect`].
pub const DEFAULT_POOL_SIZE: usize = 4;

struct PgConnPoolInner {
    slots: Vec<RwLock<PgConnection>>,
    next: AtomicUsize,
}

/// A small round-robin pool of independent [`PgConnection`]s with automatic
/// reconnect-on-failure. See the module documentation for the full retry
/// policy.
///
/// Cheap to `Clone`: the slot vector is shared via `Arc`, matching
/// `PgConnection`'s own clone semantics.
#[derive(Clone)]
pub struct PgConnPool {
    inner: Arc<PgConnPoolInner>,
}

impl PgConnPool {
    /// Establish `size` independent connections to `database_url` (clamped
    /// to at least 1) and return a pool that round-robins across them.
    pub async fn connect(
        database_url: &str,
        tls: TlsMode,
        size: usize,
        connect_timeout: Duration,
    ) -> Result<Self, PgError> {
        let size = size.max(1);
        let mut slots = Vec::with_capacity(size);
        for _ in 0..size {
            let conn =
                PgConnection::connect_with_timeout(database_url, tls.clone(), connect_timeout)
                    .await?;
            slots.push(RwLock::new(conn));
        }
        Ok(Self {
            inner: Arc::new(PgConnPoolInner {
                slots,
                next: AtomicUsize::new(0),
            }),
        })
    }

    /// Wrap a single already-established [`PgConnection`] as a one-slot
    /// pool. Callers still get the reconnect-on-failure behavior (as long as
    /// `conn` was created via [`PgConnection::connect`]/`connect_with_timeout`
    /// and so has a `reconnect_uri` to reconnect from — see
    /// [`PgConnection::reconnect`]), but not the concurrency benefit of
    /// multiple independent connections.
    pub fn from_single(conn: PgConnection) -> Self {
        Self {
            inner: Arc::new(PgConnPoolInner {
                slots: vec![RwLock::new(conn)],
                next: AtomicUsize::new(0),
            }),
        }
    }

    /// Number of independent connections held by this pool.
    pub fn size(&self) -> usize {
        self.inner.slots.len()
    }

    /// Pick the next slot index (round robin, wrapping).
    fn next_slot(&self) -> usize {
        self.inner.next.fetch_add(1, Ordering::Relaxed) % self.inner.slots.len()
    }

    /// Hand out an owned, round-robin-picked connection — e.g. to start a
    /// transaction, which must stay pinned to one connection for its whole
    /// lifetime. Cheap: `PgConnection::clone` is `Arc`-based.
    pub async fn get(&self) -> PgConnection {
        let i = self.next_slot();
        self.inner.slots[i].read().await.clone()
    }

    /// Best-effort reconnect of the connection at slot `i`, replacing it in
    /// place. Swallows a failed reconnect attempt (logged via
    /// `tracing::warn!`) rather than propagating it — a slot that cannot
    /// reconnect right now simply stays on its stale connection and will be
    /// retried the next time it looks dead.
    async fn heal_slot(&self, i: usize) {
        let current = self.inner.slots[i].read().await.clone();
        match current.reconnect().await {
            Ok(fresh) => {
                *self.inner.slots[i].write().await = fresh;
                tracing::info!(
                    slot = i,
                    "PgConnPool: reconnected slot after apparent connection loss"
                );
            }
            Err(e) => {
                tracing::warn!(
                    slot = i,
                    error = %e,
                    "PgConnPool: reconnect attempt failed; slot remains on its previous connection"
                );
            }
        }
    }

    /// Heuristic: does `err`'s message look like a lost/broken connection
    /// (worth reconnecting and, for reads, retrying) rather than a
    /// query-level failure (bad SQL, constraint violation, ...) that would
    /// just repeat if retried?
    ///
    /// The `Connection` trait only surfaces the backend-agnostic
    /// `OxiSqlError` to callers (the richer `PgError` — which does
    /// distinguish `PgError::Connection`/`PgError::Postgres` I/O errors from
    /// e.g. `PgError::ConstraintViolation` — is a private implementation
    /// detail of `oxisql-postgres`, not reachable through the trait
    /// boundary), so this stays a conservative substring match on the
    /// error's `Display` text rather than a structured downcast.
    fn looks_like_connection_loss(err: &OxiSqlError) -> bool {
        if matches!(err, OxiSqlError::NotConnected | OxiSqlError::Timeout(_)) {
            return true;
        }
        let msg = err.to_string().to_ascii_lowercase();
        const NEEDLES: [&str; 9] = [
            "connection closed",
            "broken pipe",
            "connection reset",
            "not connected",
            "connection refused",
            "os error",
            "eof",
            "timed out",
            "server closed the connection",
        ];
        NEEDLES.iter().any(|needle| msg.contains(needle))
    }

    /// Execute a DML/DDL statement. See the module-level "Retry policy" —
    /// heals the slot on an apparent connection loss but does not retry this
    /// call.
    pub async fn execute(&self, sql: &str, params: &[&dyn ToSqlValue]) -> Result<u64, OxiSqlError> {
        let i = self.next_slot();
        let conn = self.inner.slots[i].read().await.clone();
        let result = conn.execute(sql, params).await;
        if let Err(ref e) = result {
            if Self::looks_like_connection_loss(e) {
                self.heal_slot(i).await;
            }
        }
        result
    }

    /// Execute a `SELECT` and return the result rows. See the module-level
    /// "Retry policy" — heals the slot and retries once (read-only, always
    /// safe) on an apparent connection loss.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&dyn ToSqlValue],
    ) -> Result<Vec<Row>, OxiSqlError> {
        let i = self.next_slot();
        let conn = self.inner.slots[i].read().await.clone();
        match conn.query(sql, params).await {
            Ok(rows) => Ok(rows),
            Err(e) => {
                if Self::looks_like_connection_loss(&e) {
                    self.heal_slot(i).await;
                    let fresh = self.inner.slots[i].read().await.clone();
                    fresh.query(sql, params).await
                } else {
                    Err(e)
                }
            }
        }
    }

    /// Execute multiple `;`-separated statements via Postgres' simple-query
    /// protocol (required for this crate's migration files, which contain
    /// dollar-quoted `plpgsql` function bodies that a client-side `;` split
    /// would corrupt). See the module-level "Retry policy" — heals the slot
    /// but does not retry (a partially-applied multi-statement batch has its
    /// own re-run risks, and migrations are a rare, explicit, one-shot
    /// operation, not a hot path).
    pub async fn execute_batch(&self, sql: &str) -> Result<u64, OxiSqlError> {
        let i = self.next_slot();
        let conn = self.inner.slots[i].read().await.clone();
        let result = conn.execute_batch(sql).await;
        if let Err(ref e) = result {
            if Self::looks_like_connection_loss(e) {
                self.heal_slot(i).await;
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn looks_like_connection_loss_matches_common_io_errors() {
        assert!(PgConnPool::looks_like_connection_loss(&OxiSqlError::Other(
            "connection closed by peer".to_string()
        )));
        assert!(PgConnPool::looks_like_connection_loss(&OxiSqlError::Other(
            "Broken pipe (os error 32)".to_string()
        )));
        assert!(PgConnPool::looks_like_connection_loss(
            &OxiSqlError::NotConnected
        ));
        assert!(PgConnPool::looks_like_connection_loss(
            &OxiSqlError::Timeout("deadline exceeded".to_string())
        ));
    }

    #[test]
    fn looks_like_connection_loss_does_not_match_query_errors() {
        assert!(!PgConnPool::looks_like_connection_loss(
            &OxiSqlError::ConstraintViolation("duplicate key value".to_string())
        ));
        assert!(!PgConnPool::looks_like_connection_loss(
            &OxiSqlError::Parse("syntax error at or near \"SELCT\"".to_string())
        ));
        assert!(!PgConnPool::looks_like_connection_loss(
            &OxiSqlError::TypeMismatch {
                expected: "I64",
                got: "Text"
            }
        ));
    }

    #[test]
    fn next_slot_round_robins_across_all_slots() {
        // Exercise the cursor logic directly against a pool-shaped inner
        // struct without needing a live database: build a pool with N
        // no-op-equivalent slots is not possible without a connection, so
        // instead assert the arithmetic invariant `next_slot` relies on.
        let next = AtomicUsize::new(0);
        let size = 4usize;
        let picks: Vec<usize> = (0..10)
            .map(|_| next.fetch_add(1, Ordering::Relaxed) % size)
            .collect();
        assert_eq!(picks, vec![0, 1, 2, 3, 0, 1, 2, 3, 0, 1]);
    }
}
