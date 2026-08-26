//! Worker-attributed and batch claim paths for `MysqlBroker`.
//!
//! Extracted from `broker_core.rs` (which exceeded the 2000-line limit).
//! `dequeue_with_worker_id`, `dequeue_batch_impl` and
//! [`Broker::dequeue`](celers_core::Broker::dequeue) previously carried three
//! independent copies of the same claim statement and the same row-mapping
//! code, and all three copies carried the same two defects. They now share
//! [`crate::sql_text::dequeue_candidate_sql`] /
//! [`crate::sql_text::dequeue_claim_sql`] and
//! [`crate::task_row::row_to_broker_message`], plus the two helpers below.

use crate::broker_core::MysqlBroker;
use crate::mysql_error::with_deadlock_retry_celers;
use crate::row_ext::RowExt;
use crate::sql_text;
use crate::task_row;
use celers_core::{BrokerMessage, CelersError, Result, SerializedTask, TaskId};
use oxisql_core::{Connection, ToSqlValue, Transaction};
use serde_json::json;
use std::sync::atomic::Ordering;
use uuid::Uuid;

#[cfg(feature = "metrics")]
use celers_metrics::{TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL};

/// Hard ceiling on the locking round trips one claim will spend walking its
/// candidate list.
///
/// Each round asks for exactly the number of tasks still needed, so a round
/// that comes back short means that many candidates were held by competing
/// workers, and `offset` advances by the number asked for whether or not any
/// came back.
///
/// # This is a backstop, not the operative bound
///
/// The candidate window is what actually ends the walk in every case but one.
/// A round advances `offset` by at most `limit`, and
/// [`sql_text::claim_candidate_limit`] hands back roughly `limit * 4`
/// candidates, so the list is exhausted after about four rounds for any
/// `limit >= 2` — well inside this cap. The single exception is `limit == 1`,
/// where the window is its floor of 8 and `offset` advances by 1 per round:
/// there the two bounds coincide exactly, and eight round trips examine
/// exactly the eight candidates on offer.
///
/// It is kept because it makes non-termination structurally impossible rather
/// than merely arguable: the loop's progress otherwise rests on `needed >= 1`
/// holding on every iteration.
const MAX_CLAIM_ROUNDS: usize = 8;

/// Claim up to `limit` pending tasks from `queue_name` inside `tx`.
///
/// `limit` of `None` claims a single task; `Some(n)` claims up to `n`. The
/// returned pairs carry the real database row id alongside the message, so
/// the caller never has to re-derive it.
///
/// # Two statements, not one
///
/// The claim is deliberately split into a non-locking candidate scan and a
/// primary-key-only locking read — see [`crate::sql_text`]'s module header for
/// the `performance_schema.data_locks` measurement that forced it and why
/// neither `READ COMMITTED` nor a rewrite of the single statement was
/// available. The short version: a locking range scan takes next-key locks
/// that reach into a *neighbouring queue's* index records, and its filesort
/// locks the whole due backlog of its own queue.
///
/// # Candidate walk
///
/// Step 1 returns [`sql_text::claim_candidate_limit`] ids in dispatch order.
/// Step 2 then locks them in chunks sized to what is still needed, so no row
/// is ever locked and then discarded. A candidate a competing worker already
/// holds (`SKIP LOCKED`) or has already claimed (the re-checked
/// `state = 'pending'`) simply does not come back, and the next round moves
/// further down the list.
///
/// A claim can still come back short when every candidate in the window is
/// contended — that is the documented trade-off of a bounded window (see
/// [`sql_text::claim_candidate_limit`]), and it is a case `dequeue`'s
/// `Option`/`Vec` contract and every worker poll loop already handle.
pub(crate) async fn claim_pending_rows(
    tx: &mut (dyn Transaction + '_),
    queue_name: &str,
    limit: Option<i64>,
) -> Result<Vec<(Uuid, BrokerMessage)>> {
    let wanted = limit.unwrap_or(1).max(0) as usize;
    if wanted == 0 {
        return Ok(Vec::new());
    }

    let candidates = candidate_ids(tx, queue_name, wanted).await?;

    let mut claimed: Vec<(Uuid, BrokerMessage)> = Vec::with_capacity(wanted);
    let mut offset = 0usize;
    let mut rounds = 0usize;

    while claimed.len() < wanted && offset < candidates.len() && rounds < MAX_CLAIM_ROUNDS {
        let needed = wanted - claimed.len();
        let end = (offset + needed).min(candidates.len());
        let chunk = &candidates[offset..end];
        offset = end;
        rounds += 1;

        let sql = sql_text::dequeue_claim_sql(chunk.len());
        let params: Vec<&dyn ToSqlValue> = chunk.iter().map(|id| id as &dyn ToSqlValue).collect();
        let rows = tx
            .query(&sql, &params)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to dequeue task: {}", e)))?;

        for row in rows.iter() {
            claimed.push(task_row::row_to_broker_message(row)?);
        }
    }

    // A chunk can only ever return rows for the ids it bound, so this is
    // belt and braces against a server handing back more than was asked for.
    claimed.truncate(wanted);
    Ok(claimed)
}

/// Step 1 of the claim: candidate task ids in dispatch order, locking nothing.
async fn candidate_ids(
    tx: &mut (dyn Transaction + '_),
    queue_name: &str,
    wanted: usize,
) -> Result<Vec<String>> {
    let candidate_limit = sql_text::claim_candidate_limit(wanted);
    let rows = tx
        .query(
            sql_text::dequeue_candidate_sql(),
            &[&queue_name, &candidate_limit],
        )
        .await
        .map_err(|e| CelersError::Other(format!("Failed to scan for claimable tasks: {}", e)))?;

    rows.iter()
        .map(|row| {
            row.col::<String>("id")
                .map_err(|e| CelersError::Other(format!("Failed to scan for claimable tasks: {e}")))
        })
        .collect()
}

/// Transition the claimed rows to `processing`, optionally recording the
/// worker that claimed them.
pub(crate) async fn mark_rows_processing(
    tx: &mut (dyn Transaction + '_),
    row_ids: &[String],
    worker_id: Option<&str>,
) -> Result<()> {
    if row_ids.is_empty() {
        return Ok(());
    }

    // MySQL has no array parameter (PostgreSQL's `ANY($1)`), so the id list
    // is expanded into placeholders.
    let placeholders = row_ids.iter().map(|_| "?").collect::<Vec<_>>().join(", ");
    let worker_assignment = if worker_id.is_some() {
        ",\n                    worker_id = ?"
    } else {
        ""
    };
    let update_sql = format!(
        r#"
                UPDATE celers_tasks
                SET state = 'processing',
                    started_at = NOW(),
                    retry_count = retry_count + 1{worker_assignment}
                WHERE id IN ({placeholders})
                "#
    );

    let mut params: Vec<&dyn ToSqlValue> = Vec::with_capacity(row_ids.len() + 1);
    if let Some(worker) = worker_id.as_ref() {
        params.push(worker);
    }
    params.extend(row_ids.iter().map(|id| id as &dyn ToSqlValue));

    tx.execute(&update_sql, &params)
        .await
        .map_err(|e| CelersError::Other(format!("Failed to mark task as processing: {}", e)))?;

    Ok(())
}

impl MysqlBroker {
    /// Dequeue a task and set the worker ID atomically
    ///
    /// This is a convenience method that dequeues a task and sets the worker ID
    /// in a single transaction, which is useful for worker tracking.
    ///
    /// Fires the `BeforeDequeue` hooks while the row is still locked (a hook
    /// error rolls the claim back and leaves the task pending) and the
    /// `AfterDequeue` hooks once the claim is committed.
    pub async fn dequeue_with_worker_id(&self, worker_id: &str) -> Result<Option<BrokerMessage>> {
        // Check if queue is paused
        if self.paused.load(Ordering::SeqCst) {
            return Ok(None);
        }

        with_deadlock_retry_celers("dequeue_with_worker_id", || {
            self.claim_one_with_worker_id(worker_id)
        })
        .await
    }

    /// One claim attempt for [`Self::dequeue_with_worker_id`] — see
    /// `mysql_error::with_deadlock_retry_celers` for why the whole
    /// transaction, and not an individual statement, is the retryable unit.
    async fn claim_one_with_worker_id(&self, worker_id: &str) -> Result<Option<BrokerMessage>> {
        let mut tx = self
            .conn
            .transaction()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let claimed = claim_pending_rows(&mut *tx, &self.queue_name, None).await?;

        let Some((row_id, message)) = claimed.into_iter().next() else {
            tx.rollback().await.map_err(|e| {
                CelersError::Other(format!("Failed to rollback transaction: {}", e))
            })?;
            return Ok(None);
        };

        if let Err(hook_error) = self.fire_before_dequeue(&message.task).await {
            let _ = tx.rollback().await;
            return Err(hook_error);
        }

        mark_rows_processing(&mut *tx, &[row_id.to_string()], Some(worker_id)).await?;

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

        self.fire_after_dequeue(&message.task).await?;

        Ok(Some(message))
    }

    /// Enqueue multiple tasks in a single transaction (batch operation)
    ///
    /// This is significantly faster than individual enqueue calls when
    /// inserting many tasks. Uses a single transaction and prepared statement.
    ///
    /// # Returns
    /// Vector of task IDs in the same order as input tasks
    pub async fn enqueue_batch_impl(&self, tasks: Vec<SerializedTask>) -> Result<Vec<TaskId>> {
        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        let mut tx = self
            .conn
            .transaction()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let mut task_ids = Vec::with_capacity(tasks.len());

        for task in &tasks {
            let task_id = task.metadata.id;
            let db_metadata_str = self.build_task_metadata_document(task, json!({}))?;

            tx.execute(
                r#"
                INSERT INTO celers_tasks
                    (id, queue_name, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                VALUES (?, ?, ?, ?, 'pending', ?, ?, ?, NOW(), NOW())
                "#,
                &[
                    &task_id.to_string(),
                    &self.queue_name,
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &db_metadata_str,
                ],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to enqueue task in batch: {}", e)))?;

            task_ids.push(task_id);
        }

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit batch enqueue: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc_by(tasks.len() as f64);

            // Track per-task-type metrics
            for task in &tasks {
                TASKS_ENQUEUED_BY_TYPE
                    .with_label_values(&[&task.metadata.name])
                    .inc();
            }
        }

        Ok(task_ids)
    }

    /// Dequeue multiple tasks atomically (batch operation)
    ///
    /// Fetches up to `limit` tasks from this broker's logical queue in a
    /// single transaction using `FOR UPDATE ... SKIP LOCKED` for distributed
    /// worker safety.
    ///
    /// # Arguments
    /// * `limit` - Maximum number of tasks to dequeue
    ///
    /// # Returns
    /// Vector of broker messages (may be less than limit if queue has fewer tasks)
    pub async fn dequeue_batch_impl(&self, limit: usize) -> Result<Vec<BrokerMessage>> {
        if limit == 0 || self.paused.load(Ordering::SeqCst) {
            return Ok(Vec::new());
        }

        with_deadlock_retry_celers("dequeue_batch", || self.claim_batch(limit)).await
    }

    /// One batch-claim attempt for [`Self::dequeue_batch_impl`].
    async fn claim_batch(&self, limit: usize) -> Result<Vec<BrokerMessage>> {
        let mut tx = self
            .conn
            .transaction()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let claimed = claim_pending_rows(&mut *tx, &self.queue_name, Some(limit as i64)).await?;

        if claimed.is_empty() {
            tx.rollback().await.map_err(|e| {
                CelersError::Other(format!("Failed to rollback transaction: {}", e))
            })?;
            return Ok(Vec::new());
        }

        let mut row_ids = Vec::with_capacity(claimed.len());
        let mut messages = Vec::with_capacity(claimed.len());
        for (row_id, message) in claimed {
            row_ids.push(row_id.to_string());
            messages.push(message);
        }

        for message in &messages {
            if let Err(hook_error) = self.fire_before_dequeue(&message.task).await {
                let _ = tx.rollback().await;
                return Err(hook_error);
            }
        }

        mark_rows_processing(&mut *tx, &row_ids, None).await?;

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit batch dequeue: {}", e)))?;

        for message in &messages {
            self.fire_after_dequeue(&message.task).await?;
        }

        Ok(messages)
    }
}
