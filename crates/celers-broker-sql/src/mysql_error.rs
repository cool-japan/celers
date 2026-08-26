//! MySQL server-error classification, and the bounded retry it drives.
//!
//! Two things live here:
//!
//! * The predicates that recognise the handful of server error numbers this
//!   crate reacts to rather than propagates (`1060`, `1061`, `1213`, `1295`),
//!   all anchored through one matcher — see [`is_mysql_server_error`] for how
//!   the number is recovered and what that costs.
//! * [`with_deadlock_retry`], the bounded, jittered restart loop MySQL's own
//!   documentation prescribes for `ERROR 1213`. Every statement in this crate
//!   that can lose a deadlock race goes through it.

use std::future::Future;
use std::time::Duration;

use celers_core::CelersError;
use oxisql_core::OxiSqlError;

// ========== Server error numbers ==========

/// `ERROR 1060 (42S21): Duplicate column name`.
const DUPLICATE_COLUMN_NAME: u16 = 1060;

/// `ERROR 1061 (42000): Duplicate key name`.
const DUPLICATE_KEY_NAME: u16 = 1061;

/// `ERROR 1213 (40001): Deadlock found when trying to get lock`.
const DEADLOCK_FOUND: u16 = 1213;

/// `ERROR 1295 (HY000): This command is not supported in the prepared
/// statement protocol yet`.
const UNSUPPORTED_IN_PREPARED_PROTOCOL: u16 = 1295;

/// Longest SQLSTATE MySQL emits: it is always exactly five characters
/// (`40001`, `HY000`, …). Used as the upper bound when checking that the
/// parenthesised group in a formatted server error really is a SQLSTATE.
const MAX_SQLSTATE_LEN: usize = 5;

/// Does `e` carry MySQL server error number `code`?
///
/// # Why this is a text match
///
/// `oxisql-mysql` 0.4.1 maps a server error to
/// `OxiSqlError::Execution(<mysql_async::Error as Display>)`. The numeric
/// `mysql_async::ServerError::code` field is discarded at that boundary, and
/// `OxiSqlError` implements `std::error::Error` with the default
/// `source() -> None`, so there is no downcast that can recover it. The
/// number has to come back out of the formatted text, which reads exactly
/// (`mysql_async::error::ServerError`'s own `#[error("ERROR {} ({}): {}")]`,
/// wrapped by `Error::Server`'s `` `Server error: `{}'` ``):
///
/// ```text
/// execution error: Server error: `ERROR 1213 (40001): Deadlock found when trying to get lock; try restarting transaction'
/// ```
///
/// # How the match is anchored
///
/// Each of these predicates used to be a bare `contains("1213")`, which also
/// fires on any message that merely happens to contain those four digits —
/// a task payload, a table called `queue_1213`, a row id, a byte count.
/// [`mysql_server_error_matches`] instead requires the whole
/// `ERROR <code> (<sqlstate>):` prefix: the `ERROR` keyword must start a word,
/// the number must be followed by ` (`, and the parenthesised group must be
/// at most [`MAX_SQLSTATE_LEN`] alphanumeric characters closed by `):`.
///
/// # Limitation, stated plainly
///
/// This is still a text match. A server message that itself quotes the exact
/// prefix `ERROR 1213 (40001):` — an error *about* a stored error string, say
/// — would match, and nothing short of a structured error code can prevent
/// that. `oxisql-core` exposes no such code today; if a future version
/// carries it through, this function is the single place to switch over.
/// The formatting this depends on is pinned two ways: hermetically by this
/// module's own tests, and against a live server by
/// `tests_hardening`'s `a_real_server_error_is_recognised_by_its_code`.
pub(crate) fn is_mysql_server_error(e: &OxiSqlError, code: u16) -> bool {
    mysql_server_error_matches(&e.to_string(), code)
}

/// The text-level half of [`is_mysql_server_error`], split out so the
/// anchoring can be tested without fabricating an [`OxiSqlError`].
pub(crate) fn mysql_server_error_matches(message: &str, code: u16) -> bool {
    let needle = format!("ERROR {code} (");
    let mut from = 0usize;

    while let Some(offset) = message[from..].find(&needle) {
        let start = from + offset;
        let tail = &message[start + needle.len()..];
        if starts_a_word(message, start) && closes_a_sqlstate(tail) {
            return true;
        }
        from = start + needle.len();
    }

    false
}

/// Is the byte at `start` the beginning of a word, rather than the tail of a
/// longer identifier such as `MYERROR 1213 (`?
fn starts_a_word(message: &str, start: usize) -> bool {
    match message[..start].chars().next_back() {
        None => true,
        Some(previous) => !previous.is_alphanumeric() && previous != '_',
    }
}

/// Does `tail` — the text right after `ERROR <code> (` — open with a plausible
/// SQLSTATE closed by `):`?
fn closes_a_sqlstate(tail: &str) -> bool {
    let Some(close) = tail.find(')') else {
        return false;
    };
    let sqlstate = &tail[..close];
    !sqlstate.is_empty()
        && sqlstate.len() <= MAX_SQLSTATE_LEN
        && sqlstate.chars().all(|c| c.is_ascii_alphanumeric())
        && tail[close + 1..].starts_with(':')
}

/// Is this `ERROR 1061 (42000): Duplicate key name` — the index already
/// exists?
///
/// Matched on the error *number*, which is stable across MySQL and MariaDB
/// versions and locales, unlike the message text. Same predicate as
/// `celers-backend-db`'s `mysql_backend::is_duplicate_key_name`, duplicated
/// rather than shared because neither crate depends on the other.
pub(crate) fn is_duplicate_key_name(e: &OxiSqlError) -> bool {
    is_mysql_server_error(e, DUPLICATE_KEY_NAME)
}

/// Is this `ERROR 1295 (HY000)` — a statement the prepared-statement protocol
/// cannot carry?
pub(crate) fn is_unsupported_in_prepared_protocol(e: &OxiSqlError) -> bool {
    is_mysql_server_error(e, UNSUPPORTED_IN_PREPARED_PROTOCOL)
}

/// Is this `ERROR 1060 (42S21): Duplicate column name` — the column already
/// exists?
pub(crate) fn is_duplicate_column_name(e: &OxiSqlError) -> bool {
    is_mysql_server_error(e, DUPLICATE_COLUMN_NAME)
}

/// Is this `ERROR 1213 (40001): Deadlock found when trying to get lock`?
///
/// MySQL's documented remedy is to restart the transaction, which is what
/// [`with_deadlock_retry`] does.
pub(crate) fn is_deadlock(e: &OxiSqlError) -> bool {
    is_mysql_server_error(e, DEADLOCK_FOUND)
}

/// [`is_deadlock`] for an error that has already been wrapped into this
/// crate's own error type.
///
/// Multi-statement operations (a claim, an `ack`, a `reject`) map each driver
/// error into a [`CelersError`] as they go, so by the time the operation
/// returns, the deadlock is a formatted string inside
/// `CelersError::Other("Failed to dequeue task: execution error: Server error:
/// `ERROR 1213 …'")`. The server's own prefix survives that wrapping intact,
/// so the same anchored matcher recognises it — which is why the wrapping is
/// left alone rather than restructured to carry a typed cause.
pub(crate) fn is_deadlock_celers(error: &CelersError) -> bool {
    mysql_server_error_matches(&error.to_string(), DEADLOCK_FOUND)
}

// ========== Bounded deadlock retry ==========

/// Total attempts a deadlock-prone operation gets, the original included.
///
/// Enough to ride out the contention two or three writers produce; a
/// genuinely deadlocked workload fails with the server's own error rather
/// than spinning.
pub(crate) const MAX_DEADLOCK_ATTEMPTS: u32 = 5;

/// Wait ceiling after the first failed attempt. Doubles per attempt.
const BASE_DEADLOCK_BACKOFF: Duration = Duration::from_millis(20);

/// Upper bound on the wait ceiling, reached at the fifth attempt.
const MAX_DEADLOCK_BACKOFF: Duration = Duration::from_millis(320);

/// What [`with_deadlock_retry`] should do after an attempt failed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DeadlockRetry {
    /// Sleep this long, then run the operation again.
    After(Duration),
    /// Propagate the error: it is not a deadlock, or the budget is spent.
    GiveUp,
}

/// The whole retry *policy*, as a pure function — the seam that lets the
/// decision be tested without a server, a clock, or a random number
/// generator.
///
/// `attempts_made` counts attempts that have already run and failed (`1`
/// after the first failure). `jitter` is a sample from `[0.0, 1.0)`.
pub(crate) fn deadlock_retry_decision(
    error: &OxiSqlError,
    attempts_made: u32,
    jitter: f64,
) -> DeadlockRetry {
    retry_decision(is_deadlock(error), attempts_made, jitter)
}

/// [`deadlock_retry_decision`] with the classification already made, shared by
/// both retry loops so there is one policy rather than two.
fn retry_decision(is_deadlock: bool, attempts_made: u32, jitter: f64) -> DeadlockRetry {
    if !is_deadlock || attempts_made >= MAX_DEADLOCK_ATTEMPTS {
        return DeadlockRetry::GiveUp;
    }
    DeadlockRetry::After(deadlock_backoff(attempts_made, jitter))
}

/// Exponentially growing ceiling with *equal jitter*: half the ceiling is
/// fixed, half is random.
///
/// Full jitter (uniform over `[0, ceiling]`) is the more common recipe, but
/// two contenders can both sample near zero and re-collide immediately, which
/// is precisely the failure this loop exists to break. Keeping half the wait
/// deterministic guarantees the second attempt is genuinely later than the
/// first, while the random half stops a fleet of retriers from re-colliding
/// in lockstep.
fn deadlock_backoff(attempts_made: u32, jitter: f64) -> Duration {
    let exponent = attempts_made.saturating_sub(1).min(16);
    let ceiling = BASE_DEADLOCK_BACKOFF
        .saturating_mul(1u32 << exponent)
        .min(MAX_DEADLOCK_BACKOFF);
    let fixed = ceiling / 2;
    // `Duration::mul_f64` panics on a non-finite factor, and `f64::clamp`
    // propagates NaN rather than clamping it, so NaN is mapped to the
    // no-jitter end explicitly instead of being handed to `mul_f64`.
    let fraction = if jitter.is_nan() {
        0.0
    } else {
        jitter.clamp(0.0, 1.0)
    };
    fixed + fixed.mul_f64(fraction)
}

/// Run `attempt`, restarting it while MySQL reports a deadlock and the budget
/// in [`MAX_DEADLOCK_ATTEMPTS`] holds.
///
/// `ERROR 1213` is not a bug and not a failure of the statement: InnoDB picks
/// one transaction in a lock cycle and rolls **it** back, so the losing
/// statement (or transaction — the rollback is complete, which is why a
/// transactional caller may re-open one inside `attempt` and be correct) has
/// simply not happened. Production MySQL code is expected to retry it; not
/// retrying surfaces the deadlock to the caller as a spurious operation
/// failure, which is exactly how a live `revoke` was once seen to fail from
/// the cancel path.
///
/// The closure is called afresh per attempt, so each attempt must build its
/// own statement/transaction:
///
/// ```rust,ignore
/// with_deadlock_retry("cancel", || async {
///     self.connection().execute(CANCEL_SQL, &[&task_id]).await
/// })
/// .await
/// ```
///
/// Note the `async` block: `execute`'s future borrows the parameter slice, so
/// the slice has to be built *and awaited* inside the block rather than
/// returned from the closure body.
pub(crate) async fn with_deadlock_retry<T, F, Fut>(
    operation: &'static str,
    mut attempt: F,
) -> std::result::Result<T, OxiSqlError>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = std::result::Result<T, OxiSqlError>>,
{
    let mut attempts_made = 0u32;

    loop {
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        attempts_made += 1;

        match deadlock_retry_decision(&error, attempts_made, sample_jitter()) {
            DeadlockRetry::GiveUp => return Err(error),
            DeadlockRetry::After(delay) => {
                tracing::warn!(
                    operation,
                    attempts_made,
                    delay_ms = delay.as_millis() as u64,
                    error = %error,
                    "MySQL reported a deadlock; restarting the operation"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// [`with_deadlock_retry`] for a multi-statement operation that has already
/// mapped its errors into [`CelersError`].
///
/// Used by the task-lifecycle paths — the claim, `ack`, `reject` — where the
/// retryable unit is a whole transaction rather than one statement, and where
/// the intermediate steps return this crate's error type.
///
/// # Hooks fire again on a restarted claim
///
/// A claim that loses a deadlock race is rolled back completely: no row was
/// claimed, no state changed. If the loss happens after `BeforeDequeue` has
/// run, the restarted attempt runs it again. That is not a new behaviour —
/// without this loop the deadlock surfaces to the worker, whose own poll loop
/// re-issues the claim and re-fires the hook exactly the same way. What
/// changes is only that the spurious error no longer reaches the caller.
pub(crate) async fn with_deadlock_retry_celers<T, F, Fut>(
    operation: &'static str,
    mut attempt: F,
) -> celers_core::Result<T>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = celers_core::Result<T>>,
{
    let mut attempts_made = 0u32;

    loop {
        let error = match attempt().await {
            Ok(value) => return Ok(value),
            Err(error) => error,
        };
        attempts_made += 1;

        match retry_decision(is_deadlock_celers(&error), attempts_made, sample_jitter()) {
            DeadlockRetry::GiveUp => return Err(error),
            DeadlockRetry::After(delay) => {
                tracing::warn!(
                    operation,
                    attempts_made,
                    delay_ms = delay.as_millis() as u64,
                    error = %error,
                    "MySQL reported a deadlock; restarting the operation"
                );
                tokio::time::sleep(delay).await;
            }
        }
    }
}

/// One uniform sample from `[0.0, 1.0)` for [`deadlock_backoff`].
fn sample_jitter() -> f64 {
    use rand::RngExt;
    rand::rng().random::<f64>()
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Exactly how `oxisql-mysql` surfaces a MySQL server error today — the
    /// format every predicate above depends on.
    fn server_error(code: u16, sqlstate: &str, message: &str) -> OxiSqlError {
        OxiSqlError::Execution(format!(
            "Server error: `ERROR {code} ({sqlstate}): {message}'"
        ))
    }

    fn deadlock_error() -> OxiSqlError {
        server_error(
            1213,
            "40001",
            "Deadlock found when trying to get lock; try restarting transaction",
        )
    }

    // ---------- anchoring ----------

    #[test]
    fn each_predicate_matches_its_own_server_error() {
        assert!(is_duplicate_column_name(&server_error(
            1060,
            "42S21",
            "Duplicate column name 'queue_name'"
        )));
        assert!(is_duplicate_key_name(&server_error(
            1061,
            "42000",
            "Duplicate key name 'idx_tasks_queue_dequeue'"
        )));
        assert!(is_deadlock(&deadlock_error()));
        assert!(is_unsupported_in_prepared_protocol(&server_error(
            1295,
            "HY000",
            "This command is not supported in the prepared statement protocol yet"
        )));
    }

    #[test]
    fn a_predicate_does_not_match_a_different_error_number() {
        let duplicate_key = server_error(1061, "42000", "Duplicate key name 'idx'");
        assert!(!is_deadlock(&duplicate_key));
        assert!(!is_duplicate_column_name(&duplicate_key));
        assert!(!is_unsupported_in_prepared_protocol(&duplicate_key));
    }

    /// The whole point of the anchoring: the digits appearing anywhere in the
    /// message text must not be mistaken for the error number.
    #[test]
    fn digits_in_the_message_body_are_not_the_error_number() {
        let unrelated = server_error(
            1146,
            "42S02",
            "Table 'celers_test.queue_1213' doesn't exist",
        );
        assert!(!is_deadlock(&unrelated));
        assert!(is_mysql_server_error(&unrelated, 1146));

        assert!(!mysql_server_error_matches(
            "execution error: payload rejected: 1213 bytes exceeds the limit",
            1213
        ));
        assert!(!mysql_server_error_matches(
            "connection pool error: retried 1213 times",
            1213
        ));
    }

    #[test]
    fn error_must_start_a_word() {
        assert!(!mysql_server_error_matches("MYERROR 1213 (40001): x", 1213));
        assert!(!mysql_server_error_matches("_ERROR 1213 (40001): x", 1213));
        assert!(mysql_server_error_matches("(ERROR 1213 (40001): x", 1213));
        assert!(mysql_server_error_matches("ERROR 1213 (40001): x", 1213));
    }

    #[test]
    fn the_sqlstate_group_must_close_and_be_followed_by_a_colon() {
        assert!(!mysql_server_error_matches("ERROR 1213 (40001) x", 1213));
        assert!(!mysql_server_error_matches("ERROR 1213 (", 1213));
        assert!(!mysql_server_error_matches("ERROR 1213 (): x", 1213));
        assert!(!mysql_server_error_matches(
            "ERROR 1213 (a long parenthetical): x",
            1213
        ));
        assert!(!mysql_server_error_matches("ERROR 1213 (40-01): x", 1213));
    }

    /// A prefix collision — `12130` must not satisfy a search for `1213`.
    #[test]
    fn a_longer_error_number_is_not_a_prefix_match() {
        assert!(!mysql_server_error_matches("ERROR 12130 (40001): x", 1213));
    }

    /// The first occurrence failing the shape check must not stop the scan.
    #[test]
    fn a_later_occurrence_still_matches() {
        let message = "execution error: text 'ERROR 1213 no state' \
                       then Server error: `ERROR 1213 (40001): Deadlock found'";
        assert!(mysql_server_error_matches(message, 1213));
    }

    #[test]
    fn a_non_server_error_matches_nothing() {
        assert!(!is_deadlock(&OxiSqlError::NotConnected));
        assert!(!is_deadlock(&OxiSqlError::Timeout("1213".into())));
    }

    // ---------- retry policy ----------

    #[test]
    fn a_non_deadlock_error_is_never_retried() {
        let other = server_error(1146, "42S02", "Table doesn't exist");
        assert_eq!(
            deadlock_retry_decision(&other, 1, 0.5),
            DeadlockRetry::GiveUp
        );
    }

    #[test]
    fn a_deadlock_is_retried_until_the_budget_is_spent() {
        for attempts_made in 1..MAX_DEADLOCK_ATTEMPTS {
            assert!(
                matches!(
                    deadlock_retry_decision(&deadlock_error(), attempts_made, 0.5),
                    DeadlockRetry::After(_)
                ),
                "attempt {attempts_made} must still be retried"
            );
        }
        assert_eq!(
            deadlock_retry_decision(&deadlock_error(), MAX_DEADLOCK_ATTEMPTS, 0.5),
            DeadlockRetry::GiveUp,
            "the budget must be bounded"
        );
    }

    #[test]
    fn the_wait_grows_and_is_capped() {
        let waits: Vec<Duration> = (1..MAX_DEADLOCK_ATTEMPTS)
            .map(|attempts_made| deadlock_backoff(attempts_made, 1.0))
            .collect();

        for pair in waits.windows(2) {
            assert!(
                pair[1] >= pair[0],
                "waits must not shrink: {:?} then {:?}",
                pair[0],
                pair[1]
            );
        }
        assert_eq!(waits[0], BASE_DEADLOCK_BACKOFF);
        for wait in &waits {
            assert!(*wait <= MAX_DEADLOCK_BACKOFF);
        }
        assert_eq!(
            deadlock_backoff(64, 1.0),
            MAX_DEADLOCK_BACKOFF,
            "an absurd attempt count must saturate, not overflow"
        );
    }

    /// Equal jitter: never zero (so a retry is always genuinely later), never
    /// above the ceiling, and actually varying with the sample.
    #[test]
    fn jitter_stays_within_half_and_whole_of_the_ceiling() {
        for attempts_made in 1..MAX_DEADLOCK_ATTEMPTS {
            let lowest = deadlock_backoff(attempts_made, 0.0);
            let highest = deadlock_backoff(attempts_made, 1.0);
            assert!(!lowest.is_zero(), "a retry must never be immediate");
            assert!(lowest < highest, "the wait must actually be jittered");
            assert_eq!(lowest * 2, highest);
        }
    }

    #[test]
    fn out_of_range_jitter_is_clamped() {
        let ceiling = deadlock_backoff(1, 1.0);
        assert_eq!(deadlock_backoff(1, 2.0), ceiling);
        assert_eq!(deadlock_backoff(1, -1.0), deadlock_backoff(1, 0.0));
        assert_eq!(deadlock_backoff(1, f64::NAN), deadlock_backoff(1, 0.0));
    }

    // ---------- retry loop ----------

    /// `start_paused` runs the loop's sleeps on tokio's mock clock, so this
    /// exercises the real waits without spending real time.
    #[tokio::test(start_paused = true)]
    async fn the_loop_retries_a_deadlock_and_then_succeeds() {
        let attempts = std::cell::Cell::new(0u32);

        let outcome = with_deadlock_retry("test", || {
            let attempt = attempts.get() + 1;
            attempts.set(attempt);
            async move {
                if attempt < 3 {
                    Err(deadlock_error())
                } else {
                    Ok(attempt)
                }
            }
        })
        .await;

        assert_eq!(outcome.ok(), Some(3));
        assert_eq!(attempts.get(), 3, "the operation must be re-run, not faked");
    }

    #[tokio::test(start_paused = true)]
    async fn the_loop_gives_up_after_the_budget_and_returns_the_last_error() {
        let attempts = std::cell::Cell::new(0u32);

        let outcome: std::result::Result<(), OxiSqlError> = with_deadlock_retry("test", || {
            attempts.set(attempts.get() + 1);
            async move { Err(deadlock_error()) }
        })
        .await;

        let error = outcome.expect_err("a permanent deadlock must surface");
        assert!(is_deadlock(&error), "the server's own error must be kept");
        assert_eq!(attempts.get(), MAX_DEADLOCK_ATTEMPTS);
    }

    #[tokio::test(start_paused = true)]
    async fn the_loop_does_not_retry_anything_else() {
        let attempts = std::cell::Cell::new(0u32);

        let outcome: std::result::Result<(), OxiSqlError> = with_deadlock_retry("test", || {
            attempts.set(attempts.get() + 1);
            async move { Err(OxiSqlError::NotConnected) }
        })
        .await;

        assert!(outcome.is_err());
        assert_eq!(attempts.get(), 1, "a non-deadlock must fail immediately");
    }

    /// The wrapping the lifecycle paths apply must not hide the server's
    /// error number from the classifier.
    #[test]
    fn a_wrapped_deadlock_is_still_recognised() {
        let wrapped = CelersError::Other(format!("Failed to dequeue task: {}", deadlock_error()));
        assert!(is_deadlock_celers(&wrapped));

        let unrelated = CelersError::Other(format!(
            "Failed to dequeue task: {}",
            server_error(1146, "42S02", "Table 'celers_test.nope' doesn't exist")
        ));
        assert!(!is_deadlock_celers(&unrelated));
        assert!(!is_deadlock_celers(&CelersError::Other(
            "queue depth 1213".to_string()
        )));
    }

    #[tokio::test(start_paused = true)]
    async fn the_celers_loop_retries_a_wrapped_deadlock_then_succeeds() {
        let attempts = std::cell::Cell::new(0u32);

        let outcome = with_deadlock_retry_celers("test", || {
            let attempt = attempts.get() + 1;
            attempts.set(attempt);
            async move {
                if attempt < 3 {
                    Err(CelersError::Other(format!(
                        "Failed to dequeue task: {}",
                        deadlock_error()
                    )))
                } else {
                    Ok(attempt)
                }
            }
        })
        .await;

        assert_eq!(outcome.ok(), Some(3));
        assert_eq!(attempts.get(), 3);
    }

    #[tokio::test(start_paused = true)]
    async fn the_celers_loop_leaves_other_errors_alone() {
        let attempts = std::cell::Cell::new(0u32);

        let outcome: celers_core::Result<()> = with_deadlock_retry_celers("test", || {
            attempts.set(attempts.get() + 1);
            async move { Err(CelersError::Other("a hook vetoed the claim".to_string())) }
        })
        .await;

        assert!(outcome.is_err());
        assert_eq!(
            attempts.get(),
            1,
            "a non-deadlock failure must not be retried — a vetoing hook \
             would otherwise be run five times"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn the_celers_loop_gives_up_after_the_budget() {
        let attempts = std::cell::Cell::new(0u32);

        let outcome: celers_core::Result<()> = with_deadlock_retry_celers("test", || {
            attempts.set(attempts.get() + 1);
            async move {
                Err(CelersError::Other(format!(
                    "Failed to dequeue task: {}",
                    deadlock_error()
                )))
            }
        })
        .await;

        assert!(outcome.is_err());
        assert_eq!(attempts.get(), MAX_DEADLOCK_ATTEMPTS);
    }

    #[tokio::test(start_paused = true)]
    async fn a_first_attempt_that_succeeds_is_not_repeated() {
        let attempts = std::cell::Cell::new(0u32);

        let outcome = with_deadlock_retry("test", || {
            attempts.set(attempts.get() + 1);
            async move { Ok(7u32) }
        })
        .await;

        assert_eq!(outcome.ok(), Some(7));
        assert_eq!(attempts.get(), 1);
    }
}
