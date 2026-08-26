//! Canonical SQL text for the MySQL broker's claim/dequeue spine.
//!
//! Every dequeue path in this crate builds its statements through the two
//! builders below instead of embedding its own copy. Three separate
//! hand-written copies previously drifted apart (and all three carried the
//! same two defects: `FOR UPDATE SKIP LOCKED` emitted *before* `LIMIT`, which
//! is a parse error on MySQL, and no `queue_name` predicate, which let brokers
//! on different logical queues steal each other's tasks).
//!
//! # Why the claim is two statements
//!
//! A claim used to be one locking `SELECT` that both *chose* and *locked* its
//! rows:
//!
//! ```sql
//! SELECT ... FROM celers_tasks
//! WHERE queue_name = ? AND state = 'pending' AND scheduled_at <= NOW()
//! ORDER BY priority DESC, created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED
//! ```
//!
//! Under InnoDB's default `REPEATABLE READ` that shape locks far more than
//! the row it returns, in two separate ways:
//!
//! 1. **Across queues.** A locking range scan takes next-key locks, which
//!    cover the index record *after* the scanned range. Measured on this
//!    crate's MySQL 8.0 test server via `performance_schema.data_locks`: a
//!    claim on queue `zzprobe_a` held `X` on the `idx_tasks_queue_dequeue`
//!    record belonging to queue `zzprobe_b` and `X,REC_NOT_GAP` on that row's
//!    primary key. While such a claim was open, queue B's own claim skipped
//!    its only pending row (`SKIP LOCKED` skips a locked *record*, and a
//!    next-key lock locks the record) and `dequeue()` returned `Ok(None)` —
//!    one worker's in-flight claim starving an unrelated queue.
//! 2. **Within a queue.** `ORDER BY priority DESC, created_at ASC` cannot be
//!    served from `idx_tasks_queue_dequeue(queue_name, state, scheduled_at,
//!    priority, created_at)` — `scheduled_at <= NOW()` is a range, so the
//!    index gives no useful order below it — so MySQL filesorts. A filesort
//!    must read every qualifying row *before* `LIMIT` applies, and a locking
//!    read locks every row it reads. One `dequeue()` therefore locked the
//!    queue's entire due backlog for the life of its transaction.
//!
//! Neither is fixable by rewriting the single statement: `READ COMMITTED`
//! (which takes no gap locks) cannot be reached through `oxisql`'s
//! `Connection::transaction()`, which hardcodes `TxOpts::default()`, and
//! `SET TRANSACTION ISOLATION LEVEL` inside an already-open transaction is
//! `ERROR 1568`.
//!
//! So the claim is split:
//!
//! * [`dequeue_candidate_sql`] — a **non-locking** consistent read that picks
//!   candidate ids in priority order. It takes no row locks at all, so its
//!   range scan cannot touch a neighbouring queue.
//! * [`dequeue_claim_sql`] — a locking read **by primary key only**
//!   (`WHERE id IN (...)`, `FORCE INDEX (PRIMARY)`, no `ORDER BY`, no
//!   `LIMIT`). A unique-index equality search that finds its record takes a
//!   record-only lock (`REC_NOT_GAP`), never a gap lock, and with no filesort
//!   there is nothing to over-read: every row it locks is a row the caller
//!   claims.
//!
//! [`crate::broker_dequeue::claim_pending_rows`] drives the two, walking the
//! candidate list in chunks sized to what is still needed so a locked row
//! costs one extra round trip rather than a lost claim.
//!
//! # MySQL `SELECT` clause order
//!
//! MySQL 8.0's `SELECT` grammar is
//! `... [ORDER BY] [LIMIT] [into_option] [FOR UPDATE [NOWAIT | SKIP LOCKED]]`
//! — the locking clause is **last**. PostgreSQL accepts the reversed order for
//! backwards compatibility, which is how the wrong shape reached this
//! MySQL-only crate. `oxisql_mysql` forwards the statement text verbatim over
//! `COM_STMT_EXECUTE`, so a reversed clause order reaches the server as-is and
//! fails with `ERROR 1064`.
//!
//! The builders below emit single-line SQL so the exact text can be asserted
//! by unit tests without whitespace normalisation.

/// The broker's own result table (migration `010_broker_results.sql`).
///
/// Deliberately **not** `celers_task_results`: that name belongs to
/// `celers-backend-db`'s `MysqlResultBackend`, whose schema is incompatible
/// with this one, and both crates auto-migrate. See
/// [`crate::broker_core::MysqlBroker::migrate`]'s legacy-rename step for the
/// upgrade path.
pub(crate) const BROKER_RESULTS_TABLE: &str = "celers_broker_results";

/// What [`BROKER_RESULTS_TABLE`] used to be called, and what
/// `celers-backend-db` still calls *its* (differently shaped) result table.
pub(crate) const LEGACY_BROKER_RESULTS_TABLE: &str = "celers_task_results";

/// A column this crate's result schema has and `celers-backend-db`'s does not.
///
/// Probed on a legacy `celers_task_results` to decide whether it is this
/// crate's table (rename it) or the result backend's (leave it alone).
/// `traceback` is the cleanest discriminator: `celers-backend-db` stores no
/// traceback column at all, under this or any other name.
pub(crate) const BROKER_RESULTS_SIGNATURE_COLUMN: &str = "traceback";

/// The mirror-image discriminator: a column `celers-backend-db`'s result
/// schema has and this crate's does not. Used only as a safety guard — a
/// table carrying *both* signatures is not something either crate created, so
/// it is left untouched rather than renamed.
pub(crate) const BACKEND_RESULTS_SIGNATURE_COLUMN: &str = "result_state";

/// The broker's legacy `celers_results` table (migration `002_results.sql`).
pub(crate) const BROKER_LEGACY_RESULTS_TABLE: &str = "celers_results";

/// What [`BROKER_LEGACY_RESULTS_TABLE`]'s state CHECK constraint used to be
/// called.
///
/// MySQL scopes CHECK constraint names to the **schema**, not the table, and
/// `celers-backend-db`'s `celers_task_results` declares a constraint by this
/// exact name — so on a shared database whichever crate migrated second died
/// with `ERROR 3822 (HY000): Duplicate check constraint name`. Same class of
/// collision as the table name itself.
pub(crate) const LEGACY_RESULT_STATE_CONSTRAINT: &str = "chk_result_state";

/// What that constraint is called now: broker-specific, so it cannot collide
/// with anything `celers-backend-db` creates in the same schema.
pub(crate) const BROKER_RESULT_STATE_CONSTRAINT: &str = "chk_broker_result_state";

/// The predicate [`BROKER_RESULT_STATE_CONSTRAINT`] enforces, character for
/// character as `002_results.sql` declares it — the upgrade path re-adds the
/// constraint under the new name and must not change its meaning while doing
/// so.
pub(crate) const BROKER_RESULT_STATE_PREDICATE: &str = "state IN ('pending', 'success', 'failure')";

/// Columns every dequeue path selects.
///
/// `max_retries`, `priority` and `metadata` are part of the list because the
/// dequeued [`celers_core::SerializedTask`] is rebuilt from the *persisted*
/// task metadata (see [`crate::task_row`]); selecting only
/// `id, task_name, payload, retry_count` is what previously forced the
/// dequeue paths to mint a fresh `TaskMetadata` and lose the task's real id,
/// priority, retry budget and execution timeout.
pub(crate) const DEQUEUE_COLUMNS: &str =
    "id, task_name, payload, retry_count, max_retries, priority, metadata";

/// Step 1 of a claim: pick candidate task ids, in priority order, taking **no
/// locks**.
///
/// A plain `SELECT` under `REPEATABLE READ` is a consistent (MVCC) read, so
/// this scan holds no record locks and no gap locks — which is the entire
/// point of splitting the claim (see this module's header). It selects only
/// `id`, so the candidate list is cheap to carry even when it is generously
/// oversized.
///
/// The `ORDER BY` is the broker's dispatch order and is authoritative: step 2
/// deliberately does not re-sort, it just locks the ids handed to it in the
/// order they arrive here.
///
/// Bind order: `queue_name`, candidate limit.
pub(crate) const fn dequeue_candidate_sql() -> &'static str {
    "SELECT id \
     FROM celers_tasks \
     WHERE queue_name = ? \
     AND state = 'pending' \
     AND scheduled_at <= NOW() \
     ORDER BY priority DESC, created_at ASC \
     LIMIT ?"
}

/// Step 2 of a claim: lock `id_count` already-chosen tasks by primary key.
///
/// # Why this shape, exactly
///
/// * **`WHERE id IN (...)`, and no other predicate on an indexed column.**
///   Re-adding `queue_name = ?` here would hand the optimizer a reason to
///   choose `idx_tasks_queue_dequeue` again, and with it the next-key locks
///   this split exists to avoid. Queue scoping is already enforced — the ids
///   come from [`dequeue_candidate_sql`], which filters on `queue_name`.
/// * **`FORCE INDEX (PRIMARY)`.** The optimizer would almost certainly pick
///   the primary key for an id list anyway, but "almost certainly" is not a
///   guarantee, and the whole lock-footprint argument rests on it. A unique
///   equality search that finds its record takes `X,REC_NOT_GAP` — a record
///   lock with no gap — so no other queue's rows can be caught.
/// * **`AND state = 'pending'`, re-checked.** A locking read bypasses the
///   transaction's MVCC snapshot and sees the latest committed row, so this
///   discards a candidate another worker claimed between step 1 and step 2.
///   Without it the follow-up `UPDATE ... WHERE id IN (...)` (which carries
///   no state predicate of its own) would stomp a row somebody else owns.
/// * **No `ORDER BY` and no `LIMIT`.** Both would force a filesort, and a
///   filesort reads — and therefore locks — every qualifying row before the
///   `LIMIT` applies. That is the same over-locking the single-statement
///   claim suffered. The caller instead asks for exactly as many ids as it
///   still needs, so every row locked here is a row it claims.
///
/// Bind order: the `id_count` task ids, in candidate order.
pub(crate) fn dequeue_claim_sql(id_count: usize) -> String {
    let placeholders = std::iter::repeat_n("?", id_count)
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "SELECT {DEQUEUE_COLUMNS} \
         FROM celers_tasks FORCE INDEX (PRIMARY) \
         WHERE id IN ({placeholders}) \
         AND state = 'pending' \
         FOR UPDATE SKIP LOCKED"
    )
}

/// How many candidate ids [`dequeue_candidate_sql`] fetches for a claim of
/// `limit` tasks.
///
/// Oversized on purpose: every candidate a competing worker already holds
/// costs one entry off this list, and running out means the claim comes back
/// short (or empty) even though claimable rows exist further down the queue.
/// The window is what bounds that.
///
/// The trade-off is deliberate and *not* papered over with a re-probe loop: a
/// claim whose whole window is locked returns fewer rows than asked for, which
/// is exactly what `dequeue`'s `Option`/`Vec` contract already allows and what
/// a worker's poll loop already handles. The window is sized so that only
/// [`CLAIM_CANDIDATE_MULTIPLIER`]-way contention on the very same rows can
/// reach it.
///
/// [`CLAIM_CANDIDATE_MAX`] bounds the *headroom*, never the request: a
/// `dequeue_batch(1000)` on an idle queue must still be able to come back with
/// 1000 tasks, so the ceiling is applied before — not after — the floor of
/// `limit` itself.
pub(crate) fn claim_candidate_limit(limit: usize) -> i64 {
    let with_headroom = limit
        .saturating_mul(CLAIM_CANDIDATE_MULTIPLIER)
        .clamp(CLAIM_CANDIDATE_MIN, CLAIM_CANDIDATE_MAX);
    // `try_from` rather than `as`: a caller passing a `usize` beyond `i64`
    // would otherwise wrap to a negative `LIMIT`.
    i64::try_from(with_headroom.max(limit)).unwrap_or(i64::MAX)
}

/// Candidates fetched per task asked for. Also the number of simultaneously
/// competing claims the window absorbs before a claim comes back short.
const CLAIM_CANDIDATE_MULTIPLIER: usize = 4;

/// Floor for [`claim_candidate_limit`] — a single-task claim still gets real
/// headroom to skip rows other workers hold.
const CLAIM_CANDIDATE_MIN: usize = 8;

/// Ceiling on the *extra* candidates [`claim_candidate_limit`] fetches beyond
/// what the caller asked for, so a large `dequeue_batch` does not multiply
/// into an oversized scan. It never shrinks the window below `limit`.
const CLAIM_CANDIDATE_MAX: usize = 512;

/// Compare-and-swap claim for a due recurring-task configuration.
///
/// The scheduler stores each recurring configuration as a JSON document in
/// `celers_broker_results.result`. Claiming is done by swapping the *whole*
/// stored document for the advanced one, conditional on the document still
/// being byte-identical to what this scheduler read. Exactly one of N
/// competing scheduler instances therefore observes `rows_affected == 1` and
/// enqueues the task; the losers observe `0` and skip.
///
/// The predicate compares the raw stored text rather than
/// `JSON_EXTRACT(result, '$.next_run')` on purpose: re-serialising a
/// `DateTime<Utc>` is not guaranteed to reproduce the stored byte sequence,
/// and whole-document equality needs no JSON functions and no assumption
/// about the column's declared type.
///
/// Bind order: new document, config id, previously observed document.
pub(crate) const RECURRING_CLAIM_SQL: &str = "UPDATE celers_broker_results \
     SET result = ? \
     WHERE task_id = ? \
     AND result = ?";

/// Build the chunked `DELETE` used to prune terminal
/// (`completed`/`cancelled`/`failed`) tasks in one logical queue, older than
/// a caller-supplied cutoff.
///
/// # Two separate MySQL restrictions, one shape
///
/// A naive `DELETE FROM celers_tasks WHERE id IN (SELECT id FROM
/// celers_tasks WHERE ... LIMIT n)` fails on MySQL for two independent
/// reasons, and the nesting below fixes both at once:
///
/// 1. **`ERROR 1093`** (`You can't specify target table 'celers_tasks' for
///    update in FROM clause`) — the inner `SELECT` reads the very table the
///    outer `DELETE` targets.
/// 2. **`ERROR 1235`** (`This version of MySQL doesn't yet support 'LIMIT &
///    IN/ALL/ANY/SOME subquery'`) — a `LIMIT` is not allowed directly inside
///    the subquery operand of an `IN (...)` predicate.
///
/// Wrapping the `LIMIT`-bearing `SELECT` in a second, nested derived table
/// (`... AS terminal_batch`) sidesteps both: MySQL materializes a derived
/// table before the enclosing statement starts its own scan, so the outer
/// `IN` subquery no longer reads `celers_tasks` directly (fixing #1), and
/// that outer subquery itself carries no `LIMIT` — only the innermost
/// `SELECT` does (fixing #2). This is the standard, documented MySQL
/// workaround for both errors, and mirrors the row-count cap
/// `celers-broker-postgres`'s `sql::purge_terminal_sql` uses for the
/// identical purpose (Postgres needs neither restriction worked around, so
/// its version is a single-level subquery).
///
/// `batch_size` is embedded as a literal rather than bound: the caller
/// (`MysqlBroker::purge_terminal_tasks` / `MysqlBroker::spawn_retention_task`)
/// always clamps it to `1..=100_000` before formatting, so this is never
/// attacker-controlled text, and embedding it keeps this builder's shape
/// identical to the already-proven Postgres builder rather than depending on
/// whether a bound `LIMIT` placeholder is honoured inside a doubly-nested
/// derived table on every MySQL/MariaDB version this crate supports.
///
/// Bind order: `queue_name`, then the cutoff timestamp text — MySQL
/// `DATETIME` convention (see `row_ext.rs`'s "DateTime<Utc> parameter
/// convention (MySQL)" section); bind
/// `cutoff.format("%Y-%m-%d %H:%M:%S%.6f")`, never `.to_rfc3339()`.
pub(crate) fn purge_terminal_tasks_sql(batch_size: i64) -> String {
    format!(
        "DELETE FROM celers_tasks \
         WHERE id IN ( \
         SELECT id FROM ( \
         SELECT id \
         FROM celers_tasks \
         WHERE queue_name = ? \
         AND state IN ('completed', 'cancelled', 'failed') \
         AND completed_at IS NOT NULL \
         AND completed_at < ? \
         ORDER BY completed_at ASC, id ASC \
         LIMIT {batch_size} \
         ) AS terminal_batch \
         )"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dequeue_candidate_sql_is_exact() {
        assert_eq!(
            dequeue_candidate_sql(),
            "SELECT id \
             FROM celers_tasks \
             WHERE queue_name = ? \
             AND state = 'pending' \
             AND scheduled_at <= NOW() \
             ORDER BY priority DESC, created_at ASC \
             LIMIT ?"
        );
    }

    #[test]
    fn dequeue_claim_sql_is_exact() {
        assert_eq!(
            dequeue_claim_sql(3),
            "SELECT id, task_name, payload, retry_count, max_retries, priority, metadata \
             FROM celers_tasks FORCE INDEX (PRIMARY) \
             WHERE id IN (?, ?, ?) \
             AND state = 'pending' \
             FOR UPDATE SKIP LOCKED"
        );
    }

    #[test]
    fn dequeue_claim_sql_binds_one_placeholder_per_id() {
        for count in 1..=8 {
            let sql = dequeue_claim_sql(count);
            assert_eq!(
                sql.matches('?').count(),
                count,
                "one placeholder per id, got: {sql}"
            );
        }
    }

    /// The candidate step must take **no** locks: that is what keeps a claim
    /// on one queue off another queue's index records.
    #[test]
    fn candidate_step_is_not_a_locking_read() {
        let sql = dequeue_candidate_sql();
        assert!(
            !sql.contains("FOR UPDATE"),
            "the candidate scan must stay a plain consistent read, got: {sql}"
        );
        assert!(
            !sql.contains("LOCK IN SHARE MODE") && !sql.contains("FOR SHARE"),
            "the candidate scan must take no shared locks either, got: {sql}"
        );
    }

    /// Regression guard for the cross-queue next-key lock: the locking step
    /// may only search the primary key, and may carry no predicate that could
    /// tempt the optimizer onto `idx_tasks_queue_dequeue`.
    #[test]
    fn claim_step_locks_only_by_primary_key() {
        let sql = dequeue_claim_sql(2);
        assert!(sql.contains("FORCE INDEX (PRIMARY)"), "got: {sql}");
        assert!(sql.contains("WHERE id IN ("), "got: {sql}");
        assert!(
            !sql.contains("queue_name"),
            "a queue predicate here re-opens the range scan this split removed, got: {sql}"
        );
        assert!(
            !sql.contains("scheduled_at"),
            "a scheduled_at range here re-opens the range scan this split removed, got: {sql}"
        );
    }

    /// A filesort reads — and a locking read therefore locks — every
    /// qualifying row before `LIMIT` applies, so the locking step must have
    /// neither clause.
    #[test]
    fn claim_step_has_no_order_by_and_no_limit() {
        let sql = dequeue_claim_sql(4);
        assert!(!sql.contains("ORDER BY"), "got: {sql}");
        assert!(!sql.contains("LIMIT"), "got: {sql}");
    }

    /// The claimed row must still be `pending` at lock time; the follow-up
    /// `UPDATE ... WHERE id IN (...)` has no state predicate of its own.
    #[test]
    fn claim_step_rechecks_the_pending_state() {
        assert!(dequeue_claim_sql(1).contains("AND state = 'pending'"));
    }

    /// Regression guard for the `ERROR 1064` parse failure: MySQL requires
    /// `LIMIT` *before* the locking clause, and the locking clause last.
    #[test]
    fn locking_clause_is_last_and_never_precedes_a_limit() {
        let statements = [dequeue_candidate_sql().to_string(), dequeue_claim_sql(2)];
        for sql in statements {
            let Some(lock_pos) = sql.find(" FOR UPDATE") else {
                continue;
            };
            if let Some(limit_pos) = sql.find(" LIMIT ") {
                assert!(
                    limit_pos < lock_pos,
                    "LIMIT must precede FOR UPDATE on MySQL, got: {sql}"
                );
            }
            assert!(
                sql.ends_with("FOR UPDATE SKIP LOCKED"),
                "locking clause must be last, got: {sql}"
            );
        }
    }

    /// Regression guard for cross-queue task theft: queue scoping now lives
    /// entirely on the candidate step, so it has to be there.
    #[test]
    fn candidate_sql_filters_by_queue_name() {
        let sql = dequeue_candidate_sql();
        assert!(sql.contains("WHERE queue_name = ?"), "got: {sql}");
        let queue_pos = sql.find("queue_name = ?").unwrap_or(usize::MAX);
        let state_pos = sql.find("state = 'pending'").unwrap_or(0);
        assert!(
            queue_pos < state_pos,
            "queue predicate must be the leading index column, got: {sql}"
        );
    }

    #[test]
    fn candidate_window_is_oversized_but_bounded() {
        // A single-task claim still gets headroom to skip locked rows.
        assert_eq!(claim_candidate_limit(1), CLAIM_CANDIDATE_MIN as i64);
        assert_eq!(claim_candidate_limit(2), CLAIM_CANDIDATE_MIN as i64);
        // Above the floor it scales with the request.
        assert_eq!(claim_candidate_limit(10), 40);
        // The *headroom* is capped, so a large batch does not multiply into
        // an oversized scan: 1000 tasks fetch 1000 candidates, not 4000.
        assert_eq!(claim_candidate_limit(200), CLAIM_CANDIDATE_MAX as i64);
        assert_eq!(claim_candidate_limit(1_000), 1_000);
    }

    /// The cap bounds the extra candidates, never the request itself — a
    /// `dequeue_batch(n)` on an idle queue must still be able to return `n`.
    /// Clamping the window below `limit` would silently cap every large batch
    /// claim at [`CLAIM_CANDIDATE_MAX`].
    #[test]
    fn candidate_window_never_undershoots_the_request() {
        for limit in [1usize, 3, 8, 25, 100, 128, 512, 1_000, 100_000] {
            let window = claim_candidate_limit(limit);
            assert!(
                window >= limit as i64,
                "window {window} must cover a claim of {limit}"
            );
        }
    }

    /// `limit * multiplier` must not wrap, and the window must never reach the
    /// server as a negative `LIMIT`.
    #[test]
    fn candidate_window_saturates_instead_of_wrapping() {
        assert_eq!(claim_candidate_limit(usize::MAX), i64::MAX);
        for limit in [usize::MAX, usize::MAX / 2, i64::MAX as usize, 1 << 40] {
            assert!(
                claim_candidate_limit(limit) > 0,
                "a wrapped window would reach MySQL as a negative LIMIT"
            );
        }
    }

    #[test]
    fn recurring_claim_is_compare_and_swap() {
        assert_eq!(
            RECURRING_CLAIM_SQL,
            "UPDATE celers_broker_results SET result = ? WHERE task_id = ? AND result = ?"
        );
        // Without the trailing `AND result = ?` the claim is not atomic and
        // every scheduler instance would enqueue the same due task.
        assert!(RECURRING_CLAIM_SQL.ends_with("AND result = ?"));
    }

    #[test]
    fn purge_terminal_sql_is_exact() {
        assert_eq!(
            purge_terminal_tasks_sql(500),
            "DELETE FROM celers_tasks \
             WHERE id IN ( \
             SELECT id FROM ( \
             SELECT id FROM celers_tasks \
             WHERE queue_name = ? \
             AND state IN ('completed', 'cancelled', 'failed') \
             AND completed_at IS NOT NULL \
             AND completed_at < ? \
             ORDER BY completed_at ASC, id ASC \
             LIMIT 500 \
             ) AS terminal_batch \
             )"
        );
    }

    /// Regression guard for `ERROR 1093` (`You can't specify target table
    /// ... for update in FROM clause`): the `SELECT` feeding the outer `IN`
    /// must be wrapped in a derived table, not a bare correlated subquery on
    /// `celers_tasks`.
    #[test]
    fn purge_terminal_sql_wraps_the_select_in_a_derived_table() {
        let sql = purge_terminal_tasks_sql(100);
        assert!(
            sql.contains(") AS terminal_batch"),
            "the inner SELECT must be aliased as a derived table, got: {sql}"
        );
        // Two nested `SELECT id` levels: the derived table and the outer
        // `SELECT id FROM (...)` that feeds `IN`.
        assert_eq!(
            sql.matches("SELECT id").count(),
            2,
            "expected exactly two nested SELECT id levels, got: {sql}"
        );
    }

    /// Regression guard for `ERROR 1235` (`This version of MySQL doesn't yet
    /// support 'LIMIT & IN/ALL/ANY/SOME subquery'`): `LIMIT` may only appear
    /// on the innermost `SELECT`, never on the subquery operand of `IN`
    /// directly.
    #[test]
    fn purge_terminal_sql_limit_is_only_on_the_innermost_select() {
        let sql = purge_terminal_tasks_sql(100);
        assert_eq!(
            sql.matches("LIMIT").count(),
            1,
            "LIMIT must appear exactly once, on the innermost SELECT, got: {sql}"
        );
        let limit_pos = sql.find("LIMIT").expect("a LIMIT clause");
        let derived_alias_pos = sql
            .find(") AS terminal_batch")
            .expect("the derived table alias");
        assert!(
            limit_pos < derived_alias_pos,
            "LIMIT must be inside the derived table, not on the outer IN subquery, got: {sql}"
        );
    }

    #[test]
    fn purge_terminal_sql_filters_by_queue_name_and_terminal_states() {
        let sql = purge_terminal_tasks_sql(100);
        assert!(sql.contains("WHERE queue_name = ?"), "got: {sql}");
        assert!(
            sql.contains("state IN ('completed', 'cancelled', 'failed')"),
            "must never delete a pending or processing task, got: {sql}"
        );
        assert!(
            sql.contains("completed_at < ?"),
            "must be bounded by the caller's cutoff, got: {sql}"
        );
    }

    #[test]
    fn purge_terminal_sql_clamps_batch_size_into_the_text() {
        // The function itself does not clamp — callers do — but confirms the
        // literal really is substituted, not left as a stray placeholder.
        assert!(purge_terminal_tasks_sql(1).contains("LIMIT 1 "));
        assert!(purge_terminal_tasks_sql(100_000).contains("LIMIT 100000 "));
    }
}
