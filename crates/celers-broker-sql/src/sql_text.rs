//! Canonical SQL text for the MySQL broker's claim/dequeue spine.
//!
//! Every dequeue path in this crate builds its `SELECT` through
//! [`dequeue_select_sql`] instead of embedding its own copy of the statement.
//! Three separate hand-written copies previously drifted apart (and all three
//! carried the same two defects: `FOR UPDATE SKIP LOCKED` emitted *before*
//! `LIMIT`, which is a parse error on MySQL, and no `queue_name` predicate,
//! which let brokers on different logical queues steal each other's tasks).
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

/// Build the locking `SELECT` used to claim pending tasks.
///
/// `limit_clause` is the text placed after `LIMIT`: `"1"` for the
/// single-task paths, `"?"` for the batch path (which binds the limit).
///
/// Bind order: `queue_name`, then whatever `limit_clause` requires.
pub(crate) fn dequeue_select_sql(limit_clause: &str) -> String {
    format!(
        "SELECT {DEQUEUE_COLUMNS} \
         FROM celers_tasks \
         WHERE queue_name = ? \
         AND state = 'pending' \
         AND scheduled_at <= NOW() \
         ORDER BY priority DESC, created_at ASC \
         LIMIT {limit_clause} \
         FOR UPDATE SKIP LOCKED"
    )
}

/// Compare-and-swap claim for a due recurring-task configuration.
///
/// The scheduler stores each recurring configuration as a JSON document in
/// `celers_task_results.result`. Claiming is done by swapping the *whole*
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
pub(crate) const RECURRING_CLAIM_SQL: &str = "UPDATE celers_task_results \
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
    fn dequeue_single_sql_is_exact() {
        assert_eq!(
            dequeue_select_sql("1"),
            "SELECT id, task_name, payload, retry_count, max_retries, priority, metadata \
             FROM celers_tasks \
             WHERE queue_name = ? \
             AND state = 'pending' \
             AND scheduled_at <= NOW() \
             ORDER BY priority DESC, created_at ASC \
             LIMIT 1 \
             FOR UPDATE SKIP LOCKED"
        );
    }

    #[test]
    fn dequeue_batch_sql_is_exact() {
        assert_eq!(
            dequeue_select_sql("?"),
            "SELECT id, task_name, payload, retry_count, max_retries, priority, metadata \
             FROM celers_tasks \
             WHERE queue_name = ? \
             AND state = 'pending' \
             AND scheduled_at <= NOW() \
             ORDER BY priority DESC, created_at ASC \
             LIMIT ? \
             FOR UPDATE SKIP LOCKED"
        );
    }

    /// Regression guard for the `ERROR 1064` parse failure: MySQL requires
    /// `LIMIT` *before* the locking clause.
    #[test]
    fn limit_precedes_locking_clause() {
        for limit in ["1", "?", "100"] {
            let sql = dequeue_select_sql(limit);
            let limit_pos = sql
                .find(" LIMIT ")
                .expect("dequeue SQL must contain a LIMIT clause");
            let lock_pos = sql
                .find(" FOR UPDATE SKIP LOCKED")
                .expect("dequeue SQL must contain a locking clause");
            assert!(
                limit_pos < lock_pos,
                "LIMIT must precede FOR UPDATE SKIP LOCKED on MySQL, got: {sql}"
            );
            assert!(
                sql.ends_with("FOR UPDATE SKIP LOCKED"),
                "locking clause must be last, got: {sql}"
            );
        }
    }

    /// Regression guard for cross-queue task theft.
    #[test]
    fn dequeue_sql_filters_by_queue_name() {
        let sql = dequeue_select_sql("1");
        assert!(sql.contains("WHERE queue_name = ?"), "got: {sql}");
        let queue_pos = sql.find("queue_name = ?").unwrap_or(usize::MAX);
        let state_pos = sql.find("state = 'pending'").unwrap_or(0);
        assert!(
            queue_pos < state_pos,
            "queue predicate must be the leading index column, got: {sql}"
        );
    }

    #[test]
    fn recurring_claim_is_compare_and_swap() {
        assert_eq!(
            RECURRING_CLAIM_SQL,
            "UPDATE celers_task_results SET result = ? WHERE task_id = ? AND result = ?"
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
