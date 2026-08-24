//! Centralised SQL text for the task-delivery hot path.
//!
//! Every statement the `Broker` implementation runs lives here as a `const`
//! or a pure builder function. That has two payoffs:
//!
//! * the statements can be asserted on by ordinary unit tests that need no
//!   database at all (see this module's `tests`), which is what lets CI catch
//!   regressions such as "dequeue stopped returning the row id" or "the
//!   enqueue column list lost `queue_name`" without a live PostgreSQL; and
//! * the delivery-path SQL is readable in one place instead of being spread
//!   across raw string literals in half a dozen methods.
//!
//! # Binding conventions
//!
//! * `JSON`/`JSONB` parameters are always bound through a `$n::text::jsonb`
//!   cast. `oxisql-postgres` sends every parameter in PostgreSQL's **binary**
//!   wire format, and a `String` parameter's binary encoding is raw UTF-8 —
//!   which is *not* a valid binary `jsonb` payload (that format starts with a
//!   1-byte version header). The inner `::text` cast makes the server infer
//!   the parameter as `text`, for which raw UTF-8 is exactly right, and the
//!   outer `::jsonb` cast converts it server-side.
//! * `TIMESTAMPTZ` parameters follow the same rule with
//!   `$n::text::timestamptz` and `.to_rfc3339()`, as documented at length in
//!   [`crate::row_ext`].

use crate::types::RetryStrategy;

/// Maximum accepted length of a logical queue name.
pub(crate) const MAX_QUEUE_NAME_LEN: usize = 64;

/// Validate a logical queue label.
///
/// A queue name is always bound as a parameter rather than spliced into SQL
/// text, so this is defence in depth rather than the sole injection barrier —
/// but validating once at construction means no downstream query can ever be
/// handed a name carrying quotes, semicolons or comment markers, no matter
/// how the label was derived (tenant id, HTTP header, config file).
///
/// Accepted: 1..=[`MAX_QUEUE_NAME_LEN`] characters from `[A-Za-z0-9_-]`.
pub(crate) fn validate_queue_name(name: &str) -> Result<(), String> {
    if name.is_empty() {
        return Err("queue name must not be empty".to_string());
    }
    if name.len() > MAX_QUEUE_NAME_LEN {
        return Err(format!(
            "queue name {name:?} is longer than {MAX_QUEUE_NAME_LEN} characters"
        ));
    }
    if let Some(bad) = name
        .chars()
        .find(|c| !(c.is_ascii_alphanumeric() || *c == '_' || *c == '-'))
    {
        return Err(format!(
            "queue name {name:?} contains the disallowed character {bad:?}: \
             only ASCII letters, digits, '_' and '-' are permitted"
        ));
    }
    Ok(())
}

// ── Enqueue ────────────────────────────────────────────────────────────────

/// `$1` id, `$2` task_name, `$3` payload, `$4` priority, `$5` max_retries,
/// `$6` metadata JSON text, `$7` queue_name.
pub(crate) const INSERT_TASK_NOW: &str = r#"
INSERT INTO celers_tasks
    (id, task_name, payload, state, priority, max_retries, metadata, queue_name, created_at, scheduled_at)
VALUES ($1, $2, $3, 'pending', $4, $5, $6::text::jsonb, $7, NOW(), NOW())
"#;

/// As [`INSERT_TASK_NOW`], plus `$8` = an RFC3339 absolute schedule time.
pub(crate) const INSERT_TASK_AT: &str = r#"
INSERT INTO celers_tasks
    (id, task_name, payload, state, priority, max_retries, metadata, queue_name, created_at, scheduled_at)
VALUES ($1, $2, $3, 'pending', $4, $5, $6::text::jsonb, $7, NOW(), $8::text::timestamptz)
"#;

/// As [`INSERT_TASK_NOW`], plus `$8` = a delay in seconds.
pub(crate) const INSERT_TASK_AFTER: &str = r#"
INSERT INTO celers_tasks
    (id, task_name, payload, state, priority, max_retries, metadata, queue_name, created_at, scheduled_at)
VALUES ($1, $2, $3, 'pending', $4, $5, $6::text::jsonb, $7, NOW(), NOW() + ($8::bigint || ' seconds')::INTERVAL)
"#;

// ── Dequeue ────────────────────────────────────────────────────────────────

/// The projection every claim statement returns.
///
/// `metadata::text` is projected explicitly so the value comes back as a text
/// column regardless of how the driver maps `jsonb`, and the row's real `id`
/// is always included — the returned [`celers_core::BrokerMessage`] must
/// carry the database identity, otherwise `ack`/`reject`/`cancel` address a
/// row that does not exist.
pub(crate) const CLAIM_RETURNING: &str = "id, task_name, payload, retry_count, max_retries, \
     priority, attempt_count, created_at, metadata::text AS metadata";

/// Claim a single task: one statement, one round trip, no client-side
/// transaction, so no connection is pinned across four network hops.
///
/// `$1` = queue_name.
///
/// `attempt_count` (not `retry_count`) is what a delivery increments — a task
/// that has never failed must not arrive with a retry already spent.
pub(crate) fn claim_one_sql() -> String {
    format!(
        r#"
UPDATE celers_tasks
   SET state = 'processing',
       started_at = NOW(),
       attempt_count = attempt_count + 1
 WHERE id = (
         SELECT id
           FROM celers_tasks
          WHERE queue_name = $1
            AND state = 'pending'
            AND scheduled_at <= NOW()
          ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
          LIMIT 1
       )
RETURNING {CLAIM_RETURNING}
"#
    )
}

/// Claim up to `$2` tasks in one statement. `$1` = queue_name.
pub(crate) fn claim_batch_sql() -> String {
    format!(
        r#"
UPDATE celers_tasks
   SET state = 'processing',
       started_at = NOW(),
       attempt_count = attempt_count + 1
 WHERE id IN (
         SELECT id
           FROM celers_tasks
          WHERE queue_name = $1
            AND state = 'pending'
            AND scheduled_at <= NOW()
          ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
          LIMIT $2
       )
RETURNING {CLAIM_RETURNING}
"#
    )
}

// ── Ack / reject / cancel ──────────────────────────────────────────────────

/// Complete a task in one atomic, state-guarded statement.
///
/// `$1` = task id, `$2` = queue_name. The `state = 'processing'` guard means a
/// task that was cancelled or requeued while the worker was running cannot be
/// silently resurrected into `completed`, and `RETURNING` lets the caller tell
/// "row updated" from "nothing matched" without a second query.
pub(crate) const ACK_TASK: &str = r#"
UPDATE celers_tasks
   SET state = 'completed',
       completed_at = NOW()
 WHERE id = $1
   AND queue_name = $2
   AND state = 'processing'
RETURNING task_name, payload
"#;

/// `$1` = task id, `$2` = queue_name. Used to explain an ack/reject that
/// matched no row: absent task vs. task already in a terminal state.
pub(crate) const PROBE_TASK_STATE: &str = r#"
SELECT state FROM celers_tasks WHERE id = $1 AND queue_name = $2
"#;

/// Fetch just enough of a task to run lifecycle hooks. `$1` id, `$2` queue.
pub(crate) const SELECT_TASK_FOR_HOOKS: &str = r#"
SELECT task_name, payload FROM celers_tasks WHERE id = $1 AND queue_name = $2
"#;

/// Permanently fail a task (`reject(requeue = false)`).
///
/// `$1` = task id, `$2` = queue_name.
pub(crate) const FAIL_TASK: &str = r#"
UPDATE celers_tasks
   SET state = 'failed',
       completed_at = NOW(),
       started_at = NULL
 WHERE id = $1
   AND queue_name = $2
   AND state = 'processing'
RETURNING task_name, payload
"#;

/// Reject-with-requeue as a single atomic statement.
///
/// The retry-budget decision (`requeue` vs. `fail`) is evaluated *inside* the
/// UPDATE against the row's live `retry_count`/`max_retries`, so a concurrent
/// reject, cancel or retention sweep cannot make the branch act on stale
/// state. The backoff is likewise computed server-side from the row's own
/// `retry_count` via [`RetryStrategy::backoff_sql`], which is what makes a
/// single statement possible at all.
///
/// `$1` = task id, `$2` = queue_name.
///
/// `retry_count` counts *retries*, so the budget is exhausted only once
/// `retry_count + 1 > max_retries` — i.e. a task with `max_retries = 3` is
/// delivered four times in total (initial attempt plus three retries), which
/// is Celery's semantics.
pub(crate) fn reject_requeue_sql(strategy: RetryStrategy) -> String {
    let backoff = strategy.backoff_sql();
    format!(
        r#"
UPDATE celers_tasks
   SET retry_count = retry_count + 1,
       state = CASE WHEN retry_count + 1 > max_retries THEN 'failed' ELSE 'pending' END,
       scheduled_at = CASE WHEN retry_count + 1 > max_retries
                           THEN scheduled_at
                           ELSE NOW() + (({backoff}) || ' seconds')::INTERVAL END,
       completed_at = CASE WHEN retry_count + 1 > max_retries THEN NOW() ELSE NULL END,
       started_at = NULL,
       worker_id = NULL
 WHERE id = $1
   AND queue_name = $2
   AND state = 'processing'
RETURNING task_name, payload, retry_count, max_retries, state
"#
    )
}

/// `$1` = task id, `$2` = queue_name.
pub(crate) const CANCEL_TASK: &str = r#"
UPDATE celers_tasks
   SET state = 'cancelled',
       completed_at = NOW()
 WHERE id = $1
   AND queue_name = $2
   AND state IN ('pending', 'processing')
"#;

/// `$1` = queue_name.
pub(crate) const QUEUE_SIZE: &str = r#"
SELECT COUNT(*) as count
  FROM celers_tasks
 WHERE queue_name = $1
   AND state = 'pending'
"#;

/// Batch ack. `$1..$n` are task ids, `${n+1}` is the queue name.
pub(crate) fn ack_batch_sql(id_count: usize) -> String {
    let placeholders: Vec<String> = (1..=id_count).map(|i| format!("${i}")).collect();
    let queue_idx = id_count + 1;
    format!(
        r#"
UPDATE celers_tasks
   SET state = 'completed',
       completed_at = NOW()
 WHERE id IN ({})
   AND queue_name = ${queue_idx}
   AND state = 'processing'
"#,
        placeholders.join(", ")
    )
}

// ── Archival and retention ─────────────────────────────────────────────────

/// Copy rows matching `where_clause` into `celers_task_history`.
///
/// The projection is explicit: `celers_task_history` has six columns while
/// `celers_tasks` has sixteen, so the historical `SELECT *` form could never
/// execute. `$1` = queue_name; `where_clause` is an additional trusted
/// predicate fragment supplied by the caller.
pub(crate) fn archive_insert_sql(where_clause: &str) -> String {
    format!(
        r#"
INSERT INTO celers_task_history (task_id, state, timestamp, worker_id, message)
SELECT id, state, NOW(), worker_id, error_message
  FROM celers_tasks
 WHERE queue_name = $1
   AND ({where_clause})
"#
    )
}

/// Delete the rows [`archive_insert_sql`] just copied. `$1` = queue_name.
pub(crate) fn archive_delete_sql(where_clause: &str) -> String {
    format!(
        r#"
DELETE FROM celers_tasks
 WHERE queue_name = $1
   AND ({where_clause})
"#
    )
}

/// Predicate fragment selecting at most `batch_size` completed tasks older
/// than `older_than_days`.
///
/// PostgreSQL's `DELETE` has no `LIMIT` clause, so the bound is expressed as
/// an `id IN (SELECT ... LIMIT n)` sub-select that is valid in both the
/// archive INSERT and the DELETE. Both parameters are Rust integers formatted
/// by `format!`, so no caller-controlled text reaches the statement.
///
/// The ordering carries `id` as a tiebreaker, and that is load-bearing rather
/// than cosmetic: this fragment is evaluated **twice** (once by
/// [`archive_insert_sql`], once by [`archive_delete_sql`]), `created_at` is
/// not unique — `enqueue_batch` writes one identical `NOW()` for every task in
/// a batch — and with ties straddling the `LIMIT` boundary the two statements
/// could otherwise select different row sets, deleting a row that was never
/// archived and archiving a row that stays live (and gets archived again next
/// sweep). A total order makes both sub-selects pick the same rows.
pub(crate) fn completed_batch_predicate(older_than_days: i32, batch_size: i64) -> String {
    format!(
        "id IN (SELECT id FROM celers_tasks \
         WHERE queue_name = $1 AND state = 'completed' \
         AND task_name <> '__baseline__' \
         AND created_at < NOW() - INTERVAL '{older_than_days} days' \
         ORDER BY created_at ASC, id ASC LIMIT {batch_size})"
    )
}

/// Chunked purge of terminal tasks. `$1` = queue_name, `$2` = age in seconds.
///
/// Deleting through a bounded sub-select keeps each statement short-lived, so
/// a large backlog never turns into one long row-lock sweep.
///
/// Excludes `task_name = '__baseline__'` rows: those are the marker rows
/// [`crate::PostgresBroker::store_performance_baseline`] writes into
/// `celers_tasks` (see `analytics.rs`), and without this exclusion a
/// baseline silently vanished — and `compare_to_baseline` started reporting
/// "not found" — the moment it aged past `retain_for`. This is a stopgap:
/// baselines belong in their own table rather than as `celers_tasks` rows
/// impersonating completed work, but that needs a migration.
pub(crate) fn purge_terminal_sql(batch_size: i64) -> String {
    format!(
        r#"
DELETE FROM celers_tasks
 WHERE id IN (
         SELECT id
           FROM celers_tasks
          WHERE queue_name = $1
            AND state IN ('completed', 'cancelled', 'failed')
            AND task_name <> '__baseline__'
            AND completed_at IS NOT NULL
            AND completed_at < NOW() - ($2::bigint || ' seconds')::INTERVAL
          ORDER BY completed_at ASC, id ASC
          LIMIT {batch_size}
       )
"#
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn queue_name_validation_accepts_reasonable_labels() {
        assert!(validate_queue_name("default").is_ok());
        assert!(validate_queue_name("tenant-42_high").is_ok());
        assert!(validate_queue_name("A").is_ok());
        assert!(validate_queue_name(&"q".repeat(MAX_QUEUE_NAME_LEN)).is_ok());
    }

    #[test]
    fn queue_name_validation_rejects_injection_and_overlong_labels() {
        assert!(validate_queue_name("").is_err());
        assert!(validate_queue_name("foo'; DROP TABLE celers_tasks; --").is_err());
        assert!(validate_queue_name("tasks'").is_err());
        assert!(validate_queue_name("public.celers_tasks").is_err());
        assert!(validate_queue_name("with space").is_err());
        assert!(validate_queue_name(&"q".repeat(MAX_QUEUE_NAME_LEN + 1)).is_err());
    }

    #[test]
    fn claim_statements_return_the_database_row_id() {
        for sql in [claim_one_sql(), claim_batch_sql()] {
            assert!(sql.contains("RETURNING"), "claim must use RETURNING");
            assert!(
                sql.contains("RETURNING id,"),
                "claim must return the row id so ack/reject can address it"
            );
            assert!(sql.contains("FOR UPDATE SKIP LOCKED"));
            assert!(sql.contains("queue_name = $1"));
        }
    }

    #[test]
    fn claim_statements_do_not_consume_a_retry_on_delivery() {
        for sql in [claim_one_sql(), claim_batch_sql()] {
            assert!(
                !sql.contains("retry_count = retry_count + 1"),
                "delivery must increment attempt_count, never retry_count"
            );
            assert!(sql.contains("attempt_count = attempt_count + 1"));
        }
    }

    #[test]
    fn claim_statements_avoid_a_client_side_transaction_round_trip() {
        // A single UPDATE ... WHERE id = (SELECT ... SKIP LOCKED) statement is
        // what removes the BEGIN/SELECT/UPDATE/COMMIT round trips that used to
        // pin one connection for four network hops.
        let sql = claim_one_sql();
        assert!(sql.trim_start().starts_with("UPDATE celers_tasks"));
        assert_eq!(sql.matches("SELECT").count(), 1);
    }

    #[test]
    fn insert_statements_populate_the_queue_column_and_cast_json() {
        for sql in [INSERT_TASK_NOW, INSERT_TASK_AT, INSERT_TASK_AFTER] {
            assert!(
                sql.contains("queue_name"),
                "every INSERT must set queue_name or dequeue will never see the row"
            );
            assert!(
                sql.contains("$6::text::jsonb"),
                "JSONB parameters must go through a ::text::jsonb cast"
            );
        }
        assert!(INSERT_TASK_AT.contains("$8::text::timestamptz"));
        assert!(INSERT_TASK_AFTER.contains("($8::bigint || ' seconds')::INTERVAL"));
    }

    #[test]
    fn terminal_transitions_are_state_guarded_and_queue_scoped() {
        for sql in [ACK_TASK, FAIL_TASK] {
            assert!(sql.contains("AND state = 'processing'"));
            assert!(sql.contains("queue_name = $2"));
            assert!(sql.contains("RETURNING"));
        }
        assert!(CANCEL_TASK.contains("AND state IN ('pending', 'processing')"));
        assert!(CANCEL_TASK.contains("queue_name = $2"));
        assert!(QUEUE_SIZE.contains("queue_name = $1"));
    }

    #[test]
    fn reject_requeue_is_one_guarded_statement_with_a_server_side_budget_check() {
        let sql = reject_requeue_sql(RetryStrategy::default());
        assert!(sql.contains("AND state = 'processing'"));
        assert!(sql.contains("retry_count = retry_count + 1"));
        assert!(
            sql.contains("retry_count + 1 > max_retries"),
            "budget must be evaluated server-side against the live row"
        );
        assert!(sql.contains("RETURNING task_name, payload, retry_count, max_retries, state"));
        // Exactly one statement: no client-side read-then-write window.
        assert_eq!(sql.matches("UPDATE celers_tasks").count(), 1);
        assert!(!sql.contains("SELECT"));
    }

    #[test]
    fn ack_batch_numbers_placeholders_correctly() {
        let sql = ack_batch_sql(3);
        assert!(sql.contains("id IN ($1, $2, $3)"));
        assert!(sql.contains("queue_name = $4"));
        assert!(sql.contains("AND state = 'processing'"));
    }

    #[test]
    fn archive_projects_explicit_columns_and_never_puts_limit_in_a_delete() {
        let predicate = completed_batch_predicate(30, 10_000);
        let insert = archive_insert_sql(&predicate);
        let delete = archive_delete_sql(&predicate);

        assert!(
            insert.contains(
                "INSERT INTO celers_task_history (task_id, state, timestamp, worker_id, message)"
            ),
            "history table has six columns; SELECT * can never match it"
        );
        assert!(!insert.contains("SELECT *"));
        // The only LIMIT is inside the id sub-select, never at DELETE level.
        assert!(delete.contains("LIMIT 10000"));
        assert!(delete.contains("id IN (SELECT id FROM celers_tasks"));
        let after_delete = delete
            .split("id IN (")
            .next()
            .unwrap_or_default()
            .to_string();
        assert!(
            !after_delete.contains("LIMIT"),
            "PostgreSQL DELETE has no LIMIT clause"
        );
        assert!(insert.contains("queue_name = $1"));
        assert!(delete.contains("queue_name = $1"));
        // The fragment is evaluated twice; a total order is what keeps the
        // INSERT and the DELETE selecting the same rows when `created_at`
        // ties at the LIMIT boundary.
        assert!(predicate.contains("ORDER BY created_at ASC, id ASC"));
        // `store_performance_baseline` marker rows must survive archiving,
        // or `compare_to_baseline` starts reporting "not found" once the
        // baseline ages past the archive window.
        assert!(predicate.contains("task_name <> '__baseline__'"));
        assert!(insert.contains("task_name <> '__baseline__'"));
        assert!(delete.contains("task_name <> '__baseline__'"));
    }

    #[test]
    fn purge_is_chunked_and_queue_scoped() {
        let sql = purge_terminal_sql(5_000);
        assert!(sql.contains("LIMIT 5000"));
        assert!(sql.contains("queue_name = $1"));
        assert!(sql.contains("state IN ('completed', 'cancelled', 'failed')"));
        assert!(sql.trim_start().starts_with("DELETE FROM celers_tasks"));
        // Performance-baseline marker rows are terminal-looking
        // (`state = 'completed'`) but must not be swept by retention.
        assert!(sql.contains("task_name <> '__baseline__'"));
    }

    #[test]
    fn backoff_expressions_are_numeric_and_injection_free() {
        for strategy in [
            RetryStrategy::Immediate,
            RetryStrategy::Fixed { delay_secs: 30 },
            RetryStrategy::Linear {
                base_delay_secs: 10,
                max_delay_secs: 100,
            },
            RetryStrategy::Exponential {
                max_delay_secs: 3600,
            },
            RetryStrategy::ExponentialWithJitter {
                max_delay_secs: 3600,
            },
        ] {
            let expr = strategy.backoff_sql();
            assert!(!expr.contains('\''), "{expr} must not contain quotes");
            assert!(
                !expr.contains(';'),
                "{expr} must not contain a statement break"
            );
            assert!(
                !expr.contains("--"),
                "{expr} must not contain a comment marker"
            );
        }
    }
}
