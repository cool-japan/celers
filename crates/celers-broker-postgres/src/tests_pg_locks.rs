//! Live-PostgreSQL tests for session-scoped advisory locks
//! (`advisory_lock.rs`) and for the bounded lock acquisition in
//! [`crate::PostgresBroker::migrate`].
//!
//! # Why these need a real server
//!
//! An advisory lock only means anything across two *backend sessions*. The
//! bug these tests pin down — an acquire and its release landing on different
//! pooled connections, so `pg_advisory_unlock` released nothing and the lock
//! leaked for the life of the process — is invisible to any in-process test:
//! the Rust code looked symmetric, and only PostgreSQL knew which session
//! held what.
//!
//! Every test therefore opens **two independent brokers**, i.e. two separate
//! sets of TCP connections, and asserts across them: one takes the lock, the
//! other is excluded; the first releases; the second now succeeds. That is
//! the shape the old API could not pass.
//!
//! # Lock-key hygiene
//!
//! PostgreSQL advisory locks live in one flat `bigint` namespace shared by
//! every application on the server, and these tests run concurrently with each
//! other and with anything else pointed at the dev database. Each test
//! therefore mints a random key rather than using a fixed constant, so two
//! tests can never contend for the same lock.
//!
//! Gated on `CELERS_TEST_POSTGRES_URL` exactly like `tests_pg.rs`.

#![cfg(test)]

use std::time::{Duration, Instant};

use celers_core::Broker;
use uuid::Uuid;

use crate::tests_pg::test_pg_url;
use crate::{PostgresBroker, MIGRATION_ADVISORY_LOCK_ID};

/// A lock key unique to one test run, and outside the range any real caller
/// would pick by hand.
fn unique_lock_id() -> i64 {
    // Fold a v4 UUID down to a non-zero `i64`. The top bit is cleared so the
    // key is positive and easy to eyeball in `pg_locks`.
    let raw = Uuid::new_v4().as_u128() as u64;
    ((raw >> 1) as i64) | 1
}

/// Two brokers on the same database, or `None` when no test database is
/// configured. Separate `PostgresBroker`s mean separate connection pools, so
/// nothing they do can accidentally share a session.
async fn two_brokers(test_name: &str) -> Option<(PostgresBroker, PostgresBroker)> {
    let url = match test_pg_url() {
        Some(url) => url,
        None => {
            eprintln!("SKIPPED: {test_name} (set CELERS_TEST_POSTGRES_URL to run)");
            return None;
        }
    };
    let first = PostgresBroker::new(&url)
        .await
        .expect("connect to CELERS_TEST_POSTGRES_URL");
    let second = PostgresBroker::new(&url)
        .await
        .expect("connect to CELERS_TEST_POSTGRES_URL");
    Some((first, second))
}

macro_rules! brokers_or_skip {
    ($name:literal) => {
        match two_brokers($name).await {
            Some(pair) => pair,
            None => return,
        }
    };
}

// ── AdvisoryLockGuard ──────────────────────────────────────────────────────

/// The regression this module exists for: acquire on one broker, prove the
/// other is excluded, release, prove the other now gets it. The old
/// pool-routed API could not pass step three, because its release landed on a
/// session that did not hold the lock.
#[tokio::test]
async fn an_advisory_lock_excludes_another_connection_until_it_is_released() {
    let (holder, contender) = brokers_or_skip!("advisory_lock_excludes_another_connection");
    let lock_id = unique_lock_id();

    let guard = holder
        .try_acquire_advisory_lock(lock_id)
        .await
        .expect("try_acquire_advisory_lock")
        .expect("a fresh lock key must be free");
    assert_eq!(guard.lock_id(), lock_id);

    assert!(
        contender
            .try_acquire_advisory_lock(lock_id)
            .await
            .expect("try_acquire_advisory_lock")
            .is_none(),
        "a second session must not be able to take a held lock"
    );
    assert!(
        holder
            .is_advisory_lock_held(lock_id)
            .await
            .expect("is_advisory_lock_held"),
        "pg_locks must report the lock as held"
    );

    assert!(
        guard.release().await.expect("release"),
        "release must report that a lock was really released — the exact \
         assertion the pool-routed API could never satisfy"
    );

    let reacquired = contender
        .try_acquire_advisory_lock(lock_id)
        .await
        .expect("try_acquire_advisory_lock")
        .expect("the lock must be free once its holder released it");
    assert_eq!(reacquired.lock_id(), lock_id);
    reacquired.release().await.expect("release");

    assert!(
        !holder
            .is_advisory_lock_held(lock_id)
            .await
            .expect("is_advisory_lock_held"),
        "nothing must still hold the key once both guards are released"
    );
}

#[tokio::test]
async fn the_blocking_acquire_waits_for_the_holder_and_then_succeeds() {
    let (holder, waiter) = brokers_or_skip!("blocking_acquire_waits_for_the_holder");
    let lock_id = unique_lock_id();

    let guard = holder
        .acquire_advisory_lock(lock_id)
        .await
        .expect("acquire_advisory_lock");

    // Start the contender, then release after a beat. The contender must
    // observe the release rather than failing or returning early.
    let waiter_task = tokio::spawn(async move {
        let started = Instant::now();
        let acquired = waiter
            .acquire_advisory_lock(lock_id)
            .await
            .expect("acquire_advisory_lock after the holder released");
        let waited = started.elapsed();
        acquired.release().await.expect("release");
        waited
    });

    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(guard.release().await.expect("release"));

    let waited = waiter_task.await.expect("waiter task");
    assert!(
        waited >= Duration::from_millis(200),
        "the contender must actually have waited on the lock, not sailed through \
         ({waited:?})"
    );
}

#[tokio::test]
async fn a_bounded_acquire_fails_fast_instead_of_waiting_forever() {
    let (holder, contender) = brokers_or_skip!("bounded_acquire_fails_fast");
    let lock_id = unique_lock_id();

    let guard = holder
        .acquire_advisory_lock(lock_id)
        .await
        .expect("acquire_advisory_lock");

    let started = Instant::now();
    let err = contender
        .acquire_advisory_lock_within(lock_id, Duration::from_millis(400))
        .await
        .expect_err("a held lock must make a bounded acquisition fail, not hang");
    let elapsed = started.elapsed();

    assert!(
        elapsed >= Duration::from_millis(350),
        "the deadline must actually be waited out, not short-circuited ({elapsed:?})"
    );
    assert!(
        elapsed < Duration::from_secs(10),
        "the deadline must actually bound the wait ({elapsed:?})"
    );
    let message = err.to_string();
    assert!(
        message.contains(&lock_id.to_string()),
        "the error must name the lock key so the operator can find it in pg_locks: {message}"
    );
    assert!(
        message.contains("Timed out"),
        "the error must say it timed out: {message}"
    );

    guard.release().await.expect("release");

    // And the same call succeeds once the lock is free.
    let after = contender
        .acquire_advisory_lock_within(lock_id, Duration::from_millis(400))
        .await
        .expect("a free lock must be acquired well inside the deadline");
    after.release().await.expect("release");
}

/// A zero deadline is a single non-blocking attempt, not an unconditional
/// failure — the "migrate only if nobody else is" shape.
#[tokio::test]
async fn a_zero_deadline_still_makes_one_attempt() {
    let (broker, _other) = brokers_or_skip!("zero_deadline_still_attempts_once");
    let lock_id = unique_lock_id();

    let guard = broker
        .acquire_advisory_lock_within(lock_id, Duration::ZERO)
        .await
        .expect("a free lock must be acquired even with a zero deadline");
    guard.release().await.expect("release");
}

/// Dropping a guard without releasing cannot issue `pg_advisory_unlock`
/// (`Drop` is not async), so it marks the pool slot broken instead: the next
/// acquisition of that slot reconnects, and closing the old session is what
/// releases the lock. This asserts the *pool* stays usable afterwards, which
/// is the property that would break if the slot were handed back as-is.
#[tokio::test]
async fn a_dropped_guard_leaves_the_pool_usable() {
    let (broker, _other) = brokers_or_skip!("dropped_guard_leaves_the_pool_usable");
    let lock_id = unique_lock_id();

    {
        let guard = broker
            .try_acquire_advisory_lock(lock_id)
            .await
            .expect("try_acquire_advisory_lock")
            .expect("a fresh lock key must be free");
        assert_eq!(guard.lock_id(), lock_id);
        // Dropped here without `release()`.
    }

    // Every slot must still serve statements, including the one that was
    // marked broken (which reconnects on its next acquisition).
    for _ in 0..(broker.pool_size() * 2) {
        broker
            .queue_size()
            .await
            .expect("the pool must keep working after a guard was dropped un-released");
    }
}

// ── The deprecated trio, which must still be correct ───────────────────────

/// The three pre-guard methods keep their signatures but now park their
/// connection in `PostgresBroker::advisory_locks`, so an acquire and its
/// release reach the same session. This is the test their stubs in `tests.rs`
/// could not be written as before.
#[tokio::test]
#[allow(deprecated)]
async fn the_deprecated_trio_now_locks_and_unlocks_on_one_session() {
    let (holder, contender) = brokers_or_skip!("deprecated_trio_locks_and_unlocks");
    let lock_id = unique_lock_id();

    assert!(
        holder
            .try_advisory_lock(lock_id)
            .await
            .expect("try_advisory_lock"),
        "a fresh lock key must be free"
    );
    assert!(
        !contender
            .try_advisory_lock(lock_id)
            .await
            .expect("try_advisory_lock"),
        "a second session must not be able to take a held lock"
    );

    // Re-acquiring on the broker that already holds it must be a no-op rather
    // than a self-deadlock on a second pooled connection.
    assert!(
        holder
            .try_advisory_lock(lock_id)
            .await
            .expect("try_advisory_lock"),
        "the holder re-asking for its own lock must succeed immediately"
    );

    assert!(
        holder
            .release_advisory_lock(lock_id)
            .await
            .expect("release_advisory_lock"),
        "the release must reach the session that took the lock"
    );
    assert!(
        !holder
            .is_advisory_lock_held(lock_id)
            .await
            .expect("is_advisory_lock_held"),
        "the lock must really be gone, not merely forgotten"
    );

    assert!(
        contender
            .try_advisory_lock(lock_id)
            .await
            .expect("try_advisory_lock"),
        "the contender must be able to take the released lock"
    );
    assert!(contender
        .release_advisory_lock(lock_id)
        .await
        .expect("release_advisory_lock"));
}

#[tokio::test]
#[allow(deprecated)]
async fn releasing_a_lock_this_broker_does_not_hold_reports_false() {
    let (broker, _other) = brokers_or_skip!("releasing_an_unheld_lock_reports_false");
    let lock_id = unique_lock_id();

    assert!(
        !broker
            .release_advisory_lock(lock_id)
            .await
            .expect("release_advisory_lock must not error on an unheld lock"),
        "releasing a lock this broker never took must report false"
    );
}

/// `is_advisory_lock_held` used to compare the caller's key against
/// `pg_locks.objid` alone, which only holds the low 32 bits. The migration
/// lock key is exactly the case that exposed it.
#[tokio::test]
async fn a_wide_lock_key_is_reported_as_held() {
    let (broker, _other) = brokers_or_skip!("a_wide_lock_key_is_reported_as_held");

    assert!(
        MIGRATION_ADVISORY_LOCK_ID > i64::from(u32::MAX),
        "this test is only meaningful for a key wider than 32 bits"
    );

    let guard = broker
        .acquire_advisory_lock_within(MIGRATION_ADVISORY_LOCK_ID, Duration::from_secs(30))
        .await
        .expect("acquire the migration lock key");
    assert!(
        broker
            .is_advisory_lock_held(MIGRATION_ADVISORY_LOCK_ID)
            .await
            .expect("is_advisory_lock_held"),
        "a 64-bit key must be found by reassembling (classid << 32) | objid, \
         not by matching objid alone"
    );
    guard.release().await.expect("release");
}

// ── migrate()'s bounded lock acquisition ───────────────────────────────────

/// `migrate()` used to issue an unbounded `pg_advisory_lock`. One stalled
/// session holding the migration key wedged every worker in the fleet at
/// start-up, forever, with no error to see. It must now fail fast.
#[tokio::test]
async fn migrate_gives_up_when_another_session_holds_the_migration_lock() {
    let (holder, migrator) = brokers_or_skip!("migrate_gives_up_on_a_held_migration_lock");

    // Serialise against any other test that also wants the *fixed* migration
    // key: unlike the guard tests above, this one cannot mint a random key.
    let blocker = holder
        .acquire_advisory_lock_within(MIGRATION_ADVISORY_LOCK_ID, Duration::from_secs(60))
        .await
        .expect("take the migration lock on a second connection");

    let started = Instant::now();
    let err = migrator
        .migrate_with_lock_timeout(Duration::from_millis(400))
        .await
        .expect_err("migrate must give up rather than block on a held migration lock");
    let elapsed = started.elapsed();

    assert!(
        elapsed < Duration::from_secs(10),
        "the deadline must bound the wait ({elapsed:?})"
    );
    let message = err.to_string();
    assert!(
        message.contains("Failed to lock for migration"),
        "the error must say which step failed: {message}"
    );
    assert!(
        message.contains(&MIGRATION_ADVISORY_LOCK_ID.to_string()),
        "the error must name the lock key: {message}"
    );

    blocker.release().await.expect("release");

    // With the lock free, the same call succeeds — and, because the ledger
    // already lists every file, runs no DDL.
    migrator
        .migrate_with_lock_timeout(Duration::from_secs(30))
        .await
        .expect("migrate must succeed once the migration lock is free");
}

/// `migrate()` releases the lock on both the success and the failure path, so
/// a second call is never blocked by the first.
#[tokio::test]
async fn migrate_releases_the_migration_lock_when_it_finishes() {
    let (broker, other) = brokers_or_skip!("migrate_releases_the_migration_lock");

    broker
        .migrate_with_lock_timeout(Duration::from_secs(30))
        .await
        .expect("migrate");

    // A different broker (a different session) must be able to take the key
    // immediately, which is only true if the first really released it.
    let guard = other
        .acquire_advisory_lock_within(MIGRATION_ADVISORY_LOCK_ID, Duration::from_secs(5))
        .await
        .expect("the migration lock must be free once migrate() returned");
    guard.release().await.expect("release");
}
