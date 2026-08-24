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
}
