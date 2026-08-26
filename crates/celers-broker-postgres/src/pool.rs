//! Internal fixed-size PostgreSQL connection pool for [`crate::PostgresBroker`].
//!
//! # Why this module exists
//!
//! `oxisql_postgres::PgConnection` is `Clone`, but every clone shares one
//! underlying `Arc<Mutex<tokio_postgres::Client>>`. Holding a single
//! `PgConnection` in the broker therefore serialised *all* database work in
//! the process behind one mutex — and, because
//! `oxisql_core::Connection::transaction()` takes an **owned** guard that is
//! held until `commit`/`rollback`, a single `dequeue()` blocked every
//! concurrent `enqueue`/`ack`/monitoring query for the whole transaction.
//! That defeated the entire point of `FOR UPDATE SKIP LOCKED`
//! *within* a process and silently ignored the `max_connections` argument of
//! [`crate::PostgresBroker::with_pool_config`].
//!
//! [`PgPool`] fixes that by owning `pool_size` **independent** TCP
//! connections (one `PgConnection::connect` call per slot, each with its own
//! `tokio_postgres::Client`), handing one out per operation.
//!
//! # Design
//!
//! * **Slots** — a fixed `Vec` of `Arc<Mutex<Option<PgConnection>>>`. The
//!   `Option` is `None` while a slot has never been established or has been
//!   recycled after a broken connection.
//! * **Round-robin + opportunistic stealing** — an acquisition starts at
//!   `next++ % size` and `try_lock`s each slot in turn; only if every slot is
//!   busy does it `await` the lock on its round-robin slot. This keeps the
//!   fast path allocation- and contention-free while still bounding the
//!   number of live connections.
//! * **Broken-connection detection** — an operation whose error looks like a
//!   dropped connection (see [`is_connection_error`]) flags its slot; the next
//!   acquisition of that slot reconnects instead of handing back a dead
//!   client. A statement is retried once on a fresh connection **only if it is
//!   a read** (see [`is_read_only_statement`]): a write may have committed
//!   server-side before the socket died, so replaying it could double-apply
//!   it. That test is on the SQL text, not on which method was called, because
//!   the delivery path deliberately routes writes through
//!   [`PgPool::query`] — `UPDATE … RETURNING` is how a claim, an ack and a
//!   reject each stay a single atomic statement — and those must not be
//!   replayed: a replayed claim would strand the first claim's row in
//!   `processing` with no worker holding it.
//! * **Backoff** — consecutive reconnect failures on a slot push out that
//!   slot's next attempt exponentially (50 ms → 5 s cap), so a database that
//!   is down does not turn into a reconnect spin loop.
//! * **Honest metrics** — [`PgPool::snapshot`] reports real slot occupancy,
//!   waiter count and an EWMA of acquisition wait time; nothing is fabricated.

use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use oxisql_core::{Connection, OxiSqlError, Row, ToSqlValue, Transaction};
use oxisql_postgres::{PgConnection, TlsMode};
use tokio::sync::{Mutex, OwnedMutexGuard};

/// Number of connection slots used by [`crate::PostgresBroker::new`] /
/// [`crate::PostgresBroker::with_queue`] when the caller does not configure
/// one explicitly.
pub const DEFAULT_POOL_SIZE: u32 = 4;

/// Upper bound accepted for a configured pool size.
///
/// PostgreSQL's own `max_connections` is typically 100; a client-side pool
/// larger than this is a configuration mistake rather than a tuning choice.
pub const MAX_POOL_SIZE: u32 = 256;

/// First reconnect delay after a slot's connection attempt fails.
const RECONNECT_BASE_DELAY_MS: u64 = 50;

/// Ceiling for the exponential reconnect backoff.
const RECONNECT_MAX_DELAY_MS: u64 = 5_000;

/// Default timeout applied to an individual `connect` attempt.
const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);

/// One poolable connection slot.
struct PoolSlot {
    /// The slot's connection, or `None` when it still has to be established.
    conn: Arc<Mutex<Option<PgConnection>>>,
    /// Set when an operation observed a connection-level failure on this
    /// slot; the next acquisition recycles the connection.
    broken: AtomicBool,
    /// Consecutive failed reconnect attempts, used for the backoff curve.
    failures: AtomicU32,
    /// Milliseconds (relative to [`PoolInner::created`]) before which no new
    /// reconnect attempt should be made on this slot.
    next_retry_ms: AtomicU64,
}

/// Shared pool state behind the cheap-to-clone [`PgPool`] handle.
struct PoolInner {
    slots: Vec<PoolSlot>,
    next: AtomicUsize,
    url: String,
    tls: TlsMode,
    connect_timeout: Duration,
    created: Instant,
    in_use: AtomicUsize,
    waiting: AtomicUsize,
    /// Exponentially weighted moving average of acquisition wait, in µs.
    wait_ewma_us: AtomicU64,
}

/// A point-in-time, non-fabricated view of pool occupancy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PoolSnapshot {
    /// Configured number of connection slots.
    pub max_size: u32,
    /// Slots that currently hold an established connection (idle or in use).
    pub established: u32,
    /// Slots currently checked out by an operation.
    pub in_use: u32,
    /// Established slots not currently checked out.
    pub idle: u32,
    /// Tasks currently blocked waiting for a slot.
    pub waiting: u32,
    /// EWMA of how long an acquisition waited, in microseconds.
    pub avg_wait_us: u64,
}

/// Classify an error as "this connection is probably dead".
///
/// `oxisql-postgres` flattens `tokio_postgres` failures into
/// [`OxiSqlError::Execution`] strings, so a substring probe is the only
/// available signal. The list below covers the socket-level and
/// server-initiated shutdown messages that make a `tokio_postgres::Client`
/// permanently unusable; a false positive merely costs one reconnect, while a
/// false negative would wedge the slot forever, so the test is deliberately
/// generous.
pub fn is_connection_error(err: &OxiSqlError) -> bool {
    match err {
        OxiSqlError::NotConnected | OxiSqlError::ConnectionPool(_) => true,
        // A timed-out statement can leave the protocol stream mid-message;
        // recycling the slot is cheaper than risking a desynchronised client.
        OxiSqlError::Timeout(_) => true,
        OxiSqlError::Execution(msg) | OxiSqlError::Other(msg) => {
            let lowered = msg.to_ascii_lowercase();
            const MARKERS: [&str; 12] = [
                "connection closed",
                "connection reset",
                "connection refused",
                "connection unexpectedly closed",
                "broken pipe",
                "not connected",
                "server closed the connection",
                "terminating connection",
                "administrator command",
                "unexpected eof",
                "os error 32",
                "os error 54",
            ];
            MARKERS.iter().any(|m| lowered.contains(m))
        }
        _ => false,
    }
}

/// Whether `sql` is a pure read, and therefore safe to replay on a fresh
/// connection after a mid-flight connection failure.
///
/// Deliberately conservative: only a leading `SELECT`, `EXPLAIN` or `SHOW`
/// counts. A `WITH` is *not* accepted, because a CTE may contain a data
/// modifying statement (`WITH x AS (DELETE … RETURNING …) …`), and an
/// `UPDATE … RETURNING` is not a read no matter that it comes back through
/// [`PgPool::query`].
pub fn is_read_only_statement(sql: &str) -> bool {
    let head = sql.trim_start();
    // Skip any leading line comments so `-- comment\nSELECT …` is still a read.
    let head = head
        .lines()
        .find(|line| {
            let trimmed = line.trim_start();
            !trimmed.is_empty() && !trimmed.starts_with("--")
        })
        .unwrap_or("")
        .trim_start();
    let lowered = head.to_ascii_lowercase();
    lowered.starts_with("select ")
        || lowered.starts_with("select\n")
        || lowered.starts_with("explain ")
        || lowered.starts_with("show ")
}

/// Exponential backoff for the `n`-th consecutive reconnect failure.
fn backoff_ms(failures: u32) -> u64 {
    let shift = failures.saturating_sub(1).min(10);
    RECONNECT_BASE_DELAY_MS
        .saturating_mul(1u64 << shift)
        .min(RECONNECT_MAX_DELAY_MS)
}

/// Milliseconds elapsed since `origin`, saturating instead of panicking.
fn elapsed_ms(origin: Instant) -> u64 {
    u64::try_from(origin.elapsed().as_millis()).unwrap_or(u64::MAX)
}

/// A fixed-size pool of independent PostgreSQL connections.
///
/// Cloning is cheap (an `Arc` bump) and every clone shares the same slots.
#[derive(Clone)]
pub struct PgPool {
    inner: Arc<PoolInner>,
}

impl std::fmt::Debug for PgPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let snap = self.snapshot();
        f.debug_struct("PgPool")
            .field("max_size", &snap.max_size)
            .field("established", &snap.established)
            .field("in_use", &snap.in_use)
            .finish()
    }
}

impl PgPool {
    /// Build a pool of `size` slots against `url`.
    ///
    /// One connection is established eagerly so that an unreachable server or
    /// bad credentials surface from the constructor (matching the previous
    /// single-connection behaviour); the remaining slots connect lazily on
    /// first use, so a large pool does not pay `size` handshakes at startup.
    ///
    /// `size` is clamped to `1..=`[`MAX_POOL_SIZE`].
    pub async fn connect(
        url: &str,
        tls: TlsMode,
        size: u32,
        connect_timeout: Option<Duration>,
    ) -> Result<Self, OxiSqlError> {
        let size = size.clamp(1, MAX_POOL_SIZE) as usize;
        let connect_timeout = connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);

        let mut slots = Vec::with_capacity(size);
        for _ in 0..size {
            slots.push(PoolSlot {
                conn: Arc::new(Mutex::new(None)),
                broken: AtomicBool::new(false),
                failures: AtomicU32::new(0),
                next_retry_ms: AtomicU64::new(0),
            });
        }

        let inner = Arc::new(PoolInner {
            slots,
            next: AtomicUsize::new(0),
            url: url.to_string(),
            tls,
            connect_timeout,
            created: Instant::now(),
            in_use: AtomicUsize::new(0),
            waiting: AtomicUsize::new(0),
            wait_ewma_us: AtomicU64::new(0),
        });

        // Eagerly establish slot 0 so construction fails fast on a bad URL.
        let first = PgConnection::connect_with_timeout(url, inner.tls.clone(), connect_timeout)
            .await
            .map_err(|e| {
                OxiSqlError::ConnectionPool(format!("failed to connect to database: {e}"))
            })?;
        {
            let mut guard = inner.slots[0].conn.lock().await;
            *guard = Some(first);
        }

        Ok(Self { inner })
    }

    /// Number of configured connection slots.
    #[must_use]
    pub fn size(&self) -> u32 {
        u32::try_from(self.inner.slots.len()).unwrap_or(u32::MAX)
    }

    /// The connection string this pool was built from.
    #[must_use]
    pub fn url(&self) -> &str {
        &self.inner.url
    }

    /// Real, measured pool occupancy — never fabricated placeholders.
    #[must_use]
    pub fn snapshot(&self) -> PoolSnapshot {
        let max_size = self.size();
        let established = self
            .inner
            .slots
            .iter()
            .filter(|s| {
                // `try_lock` failing means the slot is checked out, which
                // implies it holds an established connection.
                match s.conn.try_lock() {
                    Ok(g) => g.is_some(),
                    Err(_) => true,
                }
            })
            .count();
        let established = u32::try_from(established).unwrap_or(u32::MAX);
        let in_use = u32::try_from(self.inner.in_use.load(Ordering::Relaxed))
            .unwrap_or(u32::MAX)
            .min(max_size);
        PoolSnapshot {
            max_size,
            established,
            in_use,
            idle: established.saturating_sub(in_use),
            waiting: u32::try_from(self.inner.waiting.load(Ordering::Relaxed)).unwrap_or(u32::MAX),
            avg_wait_us: self.inner.wait_ewma_us.load(Ordering::Relaxed),
        }
    }

    /// Check out one connection.
    ///
    /// The returned guard holds its slot until dropped; hold it only for as
    /// long as the work actually needs (e.g. one transaction).
    pub async fn acquire(&self) -> Result<PooledConnection, OxiSqlError> {
        let inner = &self.inner;
        let slot_count = inner.slots.len();
        let started = Instant::now();

        inner.waiting.fetch_add(1, Ordering::Relaxed);
        let base = inner.next.fetch_add(1, Ordering::Relaxed) % slot_count;

        let mut picked: Option<(usize, OwnedMutexGuard<Option<PgConnection>>)> = None;
        for offset in 0..slot_count {
            let idx = (base + offset) % slot_count;
            if let Ok(guard) = Arc::clone(&inner.slots[idx].conn).try_lock_owned() {
                picked = Some((idx, guard));
                break;
            }
        }
        let (idx, mut guard) = match picked {
            Some(found) => found,
            None => {
                let guard = Arc::clone(&inner.slots[base].conn).lock_owned().await;
                (base, guard)
            }
        };
        inner.waiting.fetch_sub(1, Ordering::Relaxed);

        let slot = &inner.slots[idx];
        if guard.is_none() || slot.broken.load(Ordering::Acquire) {
            // Drop any dead client before reconnecting so the old socket is
            // closed rather than leaked for the lifetime of the pool.
            *guard = None;

            let now_ms = elapsed_ms(inner.created);
            let due_ms = slot.next_retry_ms.load(Ordering::Acquire);
            if due_ms > now_ms {
                let wait = Duration::from_millis(due_ms - now_ms).min(inner.connect_timeout);
                tokio::time::sleep(wait).await;
            }

            match PgConnection::connect_with_timeout(
                &inner.url,
                inner.tls.clone(),
                inner.connect_timeout,
            )
            .await
            {
                Ok(fresh) => {
                    *guard = Some(fresh);
                    slot.broken.store(false, Ordering::Release);
                    slot.failures.store(0, Ordering::Release);
                    slot.next_retry_ms.store(0, Ordering::Release);
                }
                Err(e) => {
                    let failures = slot
                        .failures
                        .fetch_add(1, Ordering::AcqRel)
                        .saturating_add(1);
                    let retry_at = elapsed_ms(inner.created).saturating_add(backoff_ms(failures));
                    slot.next_retry_ms.store(retry_at, Ordering::Release);
                    return Err(OxiSqlError::ConnectionPool(format!(
                        "pool slot {idx}: reconnect attempt {failures} failed: {e}"
                    )));
                }
            }
        }

        inner.in_use.fetch_add(1, Ordering::Relaxed);
        let waited_us = u64::try_from(started.elapsed().as_micros()).unwrap_or(u64::MAX);
        // EWMA with alpha = 1/5, in integer arithmetic.
        let previous = inner.wait_ewma_us.load(Ordering::Relaxed);
        let updated = (previous.saturating_mul(4).saturating_add(waited_us)) / 5;
        inner.wait_ewma_us.store(updated, Ordering::Relaxed);

        Ok(PooledConnection {
            inner: Arc::clone(inner),
            idx,
            guard,
        })
    }

    /// Run a DML/DDL statement on a pooled connection.
    ///
    /// Never auto-retries: a write may have been applied server-side before
    /// the connection dropped, so replaying it could duplicate the effect.
    /// The slot is recycled for the next caller.
    pub async fn execute(&self, sql: &str, params: &[&dyn ToSqlValue]) -> Result<u64, OxiSqlError> {
        let checked_out = self.acquire().await?;
        match checked_out.conn()?.execute(sql, params).await {
            Ok(affected) => Ok(affected),
            Err(e) => {
                if is_connection_error(&e) {
                    checked_out.mark_broken();
                }
                Err(e)
            }
        }
    }

    /// Run a row-returning statement on a pooled connection.
    ///
    /// If the first attempt dies from a connection failure the slot is
    /// recycled, and the statement is retried once on a fresh connection
    /// **only when it is a pure read** ([`is_read_only_statement`]). The
    /// delivery path issues `UPDATE … RETURNING` through this method — that is
    /// what makes a claim/ack/reject one atomic statement — and those must not
    /// be replayed, because the first attempt may have committed before the
    /// socket dropped.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&dyn ToSqlValue],
    ) -> Result<Vec<Row>, OxiSqlError> {
        let first_error = {
            let checked_out = self.acquire().await?;
            match checked_out.conn()?.query(sql, params).await {
                Ok(rows) => return Ok(rows),
                Err(e) => {
                    if !is_connection_error(&e) {
                        return Err(e);
                    }
                    checked_out.mark_broken();
                    if !is_read_only_statement(sql) {
                        return Err(e);
                    }
                    e
                }
            }
        };

        let retry = self.acquire().await?;
        match retry.conn()?.query(sql, params).await {
            Ok(rows) => Ok(rows),
            Err(e) => {
                if is_connection_error(&e) {
                    retry.mark_broken();
                }
                tracing::warn!(
                    first_error = %first_error,
                    retry_error = %e,
                    "pooled query failed twice across two connections"
                );
                Err(e)
            }
        }
    }

    /// Run several `;`-separated statements through the simple-query protocol.
    pub async fn execute_batch(&self, sql: &str) -> Result<u64, OxiSqlError> {
        let checked_out = self.acquire().await?;
        match checked_out.conn()?.execute_batch(sql).await {
            Ok(affected) => Ok(affected),
            Err(e) => {
                if is_connection_error(&e) {
                    checked_out.mark_broken();
                }
                Err(e)
            }
        }
    }

    /// Liveness probe against a pooled connection.
    pub async fn ping(&self) -> Result<(), OxiSqlError> {
        let checked_out = self.acquire().await?;
        match checked_out.conn()?.ping().await {
            Ok(()) => Ok(()),
            Err(e) => {
                if is_connection_error(&e) {
                    checked_out.mark_broken();
                }
                Err(e)
            }
        }
    }
}

/// A connection checked out of a [`PgPool`].
///
/// Transactions are a deliberate two-step (`acquire()` then
/// `transaction()`): the transaction handle borrows this guard, which keeps
/// the slot reserved for the transaction's whole lifetime without any
/// self-referential ownership.
pub struct PooledConnection {
    inner: Arc<PoolInner>,
    idx: usize,
    guard: OwnedMutexGuard<Option<PgConnection>>,
}

impl std::fmt::Debug for PooledConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PooledConnection")
            .field("slot", &self.idx)
            .finish()
    }
}

impl PooledConnection {
    /// Borrow the underlying connection.
    ///
    /// `acquire()` guarantees an established connection, so the `None` arm is
    /// unreachable in practice — it is surfaced as an error rather than an
    /// `unwrap()` to keep this crate panic-free.
    pub fn conn(&self) -> Result<&PgConnection, OxiSqlError> {
        self.guard.as_ref().ok_or(OxiSqlError::NotConnected)
    }

    /// Flag this slot so the next acquisition reconnects it.
    pub fn mark_broken(&self) {
        self.inner.slots[self.idx]
            .broken
            .store(true, Ordering::Release);
    }

    /// The slot index this connection came from (useful in diagnostics).
    #[must_use]
    pub fn slot_index(&self) -> usize {
        self.idx
    }

    /// Run a statement on this specific connection.
    pub async fn execute(&self, sql: &str, params: &[&dyn ToSqlValue]) -> Result<u64, OxiSqlError> {
        match self.conn()?.execute(sql, params).await {
            Ok(affected) => Ok(affected),
            Err(e) => {
                if is_connection_error(&e) {
                    self.mark_broken();
                }
                Err(e)
            }
        }
    }

    /// Run a query on this specific connection.
    pub async fn query(
        &self,
        sql: &str,
        params: &[&dyn ToSqlValue],
    ) -> Result<Vec<Row>, OxiSqlError> {
        match self.conn()?.query(sql, params).await {
            Ok(rows) => Ok(rows),
            Err(e) => {
                if is_connection_error(&e) {
                    self.mark_broken();
                }
                Err(e)
            }
        }
    }

    /// Run a `;`-separated multi-statement batch on this specific connection
    /// (simple-query protocol).
    ///
    /// The pool-wide [`PgPool::execute_batch`] picks whichever slot is free,
    /// which is wrong whenever the batch must share a session with something
    /// else — most importantly `PostgresBroker::migrate`, whose
    /// `pg_advisory_lock` is session-scoped and would otherwise be taken on
    /// one connection and released on another.
    pub async fn execute_batch(&self, sql: &str) -> Result<u64, OxiSqlError> {
        match self.conn()?.execute_batch(sql).await {
            Ok(affected) => Ok(affected),
            Err(e) => {
                if is_connection_error(&e) {
                    self.mark_broken();
                }
                Err(e)
            }
        }
    }

    /// Begin a transaction on this connection.
    ///
    /// The returned handle borrows `self`, so the pool slot stays checked out
    /// until the transaction is committed or rolled back and this guard is
    /// dropped.
    pub async fn transaction(&self) -> Result<Box<dyn Transaction + '_>, OxiSqlError> {
        match self.conn()?.transaction().await {
            Ok(tx) => Ok(tx),
            Err(e) => {
                if is_connection_error(&e) {
                    self.mark_broken();
                }
                Err(e)
            }
        }
    }
}

impl Drop for PooledConnection {
    fn drop(&mut self) {
        // `fetch_update` keeps the counter from wrapping if a decrement ever
        // races ahead of its increment.
        let _ = self
            .inner
            .in_use
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
                Some(current.saturating_sub(1))
            });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_exponentially_and_is_capped() {
        assert_eq!(backoff_ms(0), RECONNECT_BASE_DELAY_MS);
        assert_eq!(backoff_ms(1), RECONNECT_BASE_DELAY_MS);
        assert_eq!(backoff_ms(2), RECONNECT_BASE_DELAY_MS * 2);
        assert_eq!(backoff_ms(3), RECONNECT_BASE_DELAY_MS * 4);
        assert_eq!(backoff_ms(30), RECONNECT_MAX_DELAY_MS);
        // Monotonic non-decreasing across the whole domain.
        let mut previous = 0;
        for failures in 0..64 {
            let current = backoff_ms(failures);
            assert!(current >= previous, "backoff regressed at {failures}");
            assert!(current <= RECONNECT_MAX_DELAY_MS);
            previous = current;
        }
    }

    #[test]
    fn connection_errors_are_recognised() {
        assert!(is_connection_error(&OxiSqlError::NotConnected));
        assert!(is_connection_error(&OxiSqlError::ConnectionPool(
            "exhausted".into()
        )));
        assert!(is_connection_error(&OxiSqlError::Execution(
            "connection closed".into()
        )));
        assert!(is_connection_error(&OxiSqlError::Execution(
            "error communicating with the server: Broken pipe (os error 32)".into()
        )));
        assert!(is_connection_error(&OxiSqlError::Execution(
            "db error: FATAL: terminating connection due to administrator command".into()
        )));
    }

    #[test]
    fn only_pure_reads_are_eligible_for_replay() {
        assert!(is_read_only_statement("SELECT 1"));
        assert!(is_read_only_statement("  select id from celers_tasks"));
        assert!(is_read_only_statement(
            "\n-- a comment\nSELECT COUNT(*) as count"
        ));
        assert!(is_read_only_statement("EXPLAIN (ANALYZE) SELECT 1"));

        // The delivery path routes writes through `query` for `RETURNING`;
        // replaying one could strand a claimed row or double-apply a write.
        assert!(!is_read_only_statement(&crate::sql::claim_one_sql()));
        assert!(!is_read_only_statement(&crate::sql::claim_batch_sql()));
        assert!(!is_read_only_statement(crate::sql::ACK_TASK));
        assert!(!is_read_only_statement(crate::sql::FAIL_TASK));
        assert!(!is_read_only_statement(&crate::sql::reject_requeue_sql(
            crate::types::RetryStrategy::default()
        )));
        assert!(!is_read_only_statement(
            "WITH doomed AS (DELETE FROM celers_tasks RETURNING id) SELECT * FROM doomed"
        ));
        assert!(!is_read_only_statement(
            "INSERT INTO celers_tasks DEFAULT VALUES"
        ));

        // The probe and hook reads must stay replayable.
        assert!(is_read_only_statement(crate::sql::PROBE_TASK_STATE));
        assert!(is_read_only_statement(crate::sql::SELECT_TASK_FOR_HOOKS));
        assert!(is_read_only_statement(crate::sql::QUEUE_SIZE));
    }

    #[test]
    fn query_errors_are_not_mistaken_for_connection_failures() {
        assert!(!is_connection_error(&OxiSqlError::Execution(
            "db error: ERROR: column \"queue_name\" does not exist".into()
        )));
        assert!(!is_connection_error(&OxiSqlError::ConstraintViolation(
            "duplicate key".into()
        )));
        assert!(!is_connection_error(&OxiSqlError::Parse(
            "syntax error".into()
        )));
        assert!(!is_connection_error(&OxiSqlError::TypeMismatch {
            expected: "i64",
            got: "text",
        }));
    }
}
