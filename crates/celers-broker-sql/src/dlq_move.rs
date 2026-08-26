//! The dead-letter move, as ordinary SQL rather than `CALL move_to_dlq(?)`.
//!
//! `001_init.sql` defines a `move_to_dlq` stored procedure, but CeleRS cannot
//! create it: MySQL rejects `CREATE PROCEDURE` over the prepared-statement
//! protocol (`ERROR 1295 (HY000): This command is not supported in the
//! prepared statement protocol yet`), and `oxisql-mysql` 0.4.1 routes every
//! path — `execute`, `query`, `execute_batch`, `call_procedure_multi` —
//! through `mysql_async`'s `exec_iter`, which always prepares. There is no
//! text-protocol escape hatch to reach for.
//!
//! So the procedure does not exist on any database CeleRS migrated, and
//! `CALL move_to_dlq(?)` could only ever fail: a task that exhausted its
//! retries was never actually moved to the dead-letter queue. (Before that
//! was reachable at all, `migrate()` aborted on the `CREATE PROCEDURE` itself,
//! leaving the whole schema half-applied.)
//!
//! The procedure body is exactly [`DLQ_INSERT_SQL`] + [`DLQ_DELETE_SQL`], so
//! issuing them directly — inside a transaction, which is what gave the
//! procedure its atomicity — is a faithful replacement that depends on
//! nothing the driver cannot send. `celers-broker-postgres` likewise does
//! this work in plain SQL.
//!
//! Both call sites are covered live: `tests_hardening`'s
//! `retry_exhaustion_moves_the_task_into_the_dead_letter_queue` drives
//! [`MysqlBroker::move_to_dlq_by_row_id`] through `Broker::reject`, and
//! `reject_batch_moves_an_exhausted_task_into_the_dead_letter_queue` drives
//! the same two statements through `broker_chain.rs`'s batch path.

use celers_core::{CelersError, Result};
use oxisql_core::Connection;

use crate::mysql_error::with_deadlock_retry;
use crate::MysqlBroker;

/// Copy a task row into the dead-letter queue. `?` is the task's row id.
///
/// The DLQ table has no `queue_name` column, so the copied row is identified
/// by `task_id` alone — see `broker_core.rs`'s note on the deliberately
/// queue-blind DLQ helpers.
pub(crate) const DLQ_INSERT_SQL: &str = "\
INSERT INTO celers_dead_letter_queue \
    (id, task_id, task_name, payload, retry_count, error_message, metadata) \
SELECT UUID(), id, task_name, payload, retry_count, error_message, metadata \
FROM celers_tasks WHERE id = ?";

/// Remove the task row that [`DLQ_INSERT_SQL`] just copied. `?` is its row id.
pub(crate) const DLQ_DELETE_SQL: &str = "DELETE FROM celers_tasks WHERE id = ?";

impl MysqlBroker {
    /// Move a task to the Dead Letter Queue, addressing the row by its
    /// already-resolved primary key text.
    ///
    /// Takes the row id as text rather than a [`celers_core::TaskId`] because
    /// `reject` resolves the row id from the receipt handle first (see
    /// [`crate::task_row::resolve_row_id`]).
    ///
    /// Runs [`DLQ_INSERT_SQL`] + [`DLQ_DELETE_SQL`] in one transaction — see
    /// this module's documentation for why the stored procedure cannot be
    /// relied on.
    ///
    /// An insert-then-delete pair against a row another transaction is also
    /// touching is a textbook deadlock candidate, so the whole transaction is
    /// wrapped in [`with_deadlock_retry`]: a lost deadlock race rolls the
    /// transaction back completely, and the closure opens a fresh one on the
    /// next attempt.
    pub(crate) async fn move_to_dlq_by_row_id(&self, row_id: &str) -> Result<()> {
        with_deadlock_retry("move_to_dlq", || async {
            let mut tx = self.conn.transaction().await?;
            tx.execute(DLQ_INSERT_SQL, &[&row_id]).await?;
            tx.execute(DLQ_DELETE_SQL, &[&row_id]).await?;
            tx.commit().await
        })
        .await
        .map_err(|e| CelersError::Other(format!("Failed to move task to DLQ: {}", e)))
    }
}
