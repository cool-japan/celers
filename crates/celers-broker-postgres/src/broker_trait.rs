//! Broker trait implementation for PostgresBroker
//!
//! # Delivery identity
//!
//! Every message this module returns carries the **database row id** in
//! `task.metadata.id`. That is not a detail: `ack`/`reject`/`cancel` address
//! a task by that id, so a synthesised id (as an earlier version produced via
//! `SerializedTask::new`, which mints a fresh `Uuid::new_v4()`) makes every
//! acknowledgement silently match zero rows and leaves every processed task
//! wedged in `state = 'processing'` forever.
//!
//! # Atomicity
//!
//! Claiming, acking and rejecting are each ONE statement. Besides removing
//! three network round trips from the hot path, that closes the
//! read-then-write window in which a concurrent `cancel`, a competing
//! `reject` or a retention sweep could change the row between the read and
//! the write. Each write is additionally guarded on the state it expects
//! (`AND state = 'processing'`) and reports via `RETURNING` whether it
//! actually matched, so a lost update is logged rather than silently ignored.

use async_trait::async_trait;
use celers_core::{
    Broker, BrokerMessage, CelersError, Result, SerializedTask, TaskId, TaskMetadata, TaskState,
};
use chrono::{DateTime, Utc};
use oxisql_core::Row;
use serde_json::json;
use std::sync::atomic::Ordering;
use uuid::Uuid;

use crate::row_ext::{json_from_row, json_param, uuid_from_row, uuid_param, RowExt};
use crate::sql;
use crate::types::HookContext;
use crate::PostgresBroker;

#[cfg(feature = "metrics")]
use celers_metrics::{TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL};

/// What a claim statement tells us about the row it just claimed.
struct ClaimedTask {
    task: SerializedTask,
    task_id: Uuid,
    retry_count: i32,
    attempt_count: i32,
}

/// Rebuild a task from a claimed row.
///
/// The row's `id` always wins over anything else: it is the handle
/// `ack`/`reject`/`cancel` will be called with. `priority`, `max_retries` and
/// `created_at` come from their columns rather than from
/// [`SerializedTask::new`]'s defaults (which would silently reset a
/// priority-9 task to 0 and a 10-retry task to 3), and the persisted
/// `metadata` document — written at enqueue time — restores the remaining
/// fields (`timeout_secs`, `group_id`, `chord_id`, `on_success_link`,
/// dependencies) that no column carries.
fn claimed_task_from_row(row: &Row) -> Result<ClaimedTask> {
    let task_id: Uuid = uuid_from_row(row, "id")
        .map_err(|e| CelersError::Other(format!("Failed to read task id: {}", e)))?;
    let task_name: String = row
        .col("task_name")
        .map_err(|e| CelersError::Other(format!("Failed to read task_name: {}", e)))?;
    let payload: Vec<u8> = row
        .col("payload")
        .map_err(|e| CelersError::Other(format!("Failed to read payload: {}", e)))?;
    let retry_count: i32 = row
        .col("retry_count")
        .map_err(|e| CelersError::Other(format!("Failed to read retry_count: {}", e)))?;
    let max_retries: i32 = row
        .col("max_retries")
        .map_err(|e| CelersError::Other(format!("Failed to read max_retries: {}", e)))?;
    let priority: i32 = row
        .col("priority")
        .map_err(|e| CelersError::Other(format!("Failed to read priority: {}", e)))?;
    let attempt_count: i32 = row
        .col("attempt_count")
        .map_err(|e| CelersError::Other(format!("Failed to read attempt_count: {}", e)))?;

    let mut task = SerializedTask::new(task_name.clone(), payload);

    // Restore the full metadata document when it round-trips; fall back to
    // the column values otherwise (a task enqueued by an older build, or one
    // whose metadata was rewritten by an external tool, must still dequeue).
    if let Ok(metadata_json) = json_from_row(row, "metadata") {
        if let Ok(persisted) = serde_json::from_value::<TaskMetadata>(metadata_json) {
            task.metadata = persisted;
        }
    }

    task.metadata.id = task_id;
    task.metadata.name = task_name;
    task.metadata.priority = priority;
    task.metadata.max_retries = u32::try_from(max_retries).unwrap_or(0);
    if let Ok(created_at) = row.col::<DateTime<Utc>>("created_at") {
        task.metadata.created_at = created_at;
    }
    task.metadata.updated_at = Utc::now();
    task.metadata.state = if retry_count > 0 {
        TaskState::Retrying(u32::try_from(retry_count).unwrap_or(0))
    } else {
        TaskState::Received
    };

    Ok(ClaimedTask {
        task,
        task_id,
        retry_count,
        attempt_count,
    })
}

/// Read the `task_name`/`payload` pair a lifecycle hook needs.
fn hook_task_from_row(row: &Row) -> Result<SerializedTask> {
    let task_name: String = row
        .col("task_name")
        .map_err(|e| CelersError::Other(format!("Failed to read task_name: {}", e)))?;
    let payload: Vec<u8> = row
        .col("payload")
        .map_err(|e| CelersError::Other(format!("Failed to read payload: {}", e)))?;
    Ok(SerializedTask::new(task_name, payload))
}

/// Resolve which id a terminal transition should address.
///
/// `dequeue` sets the receipt handle to the row id, so a caller that round
/// trips the handle is authoritative; anything else falls back to the id the
/// caller passed. (The handle used to be `retry_count.to_string()`, which
/// carried no identity at all.)
fn resolve_task_id(task_id: &TaskId, receipt_handle: Option<&str>) -> Uuid {
    receipt_handle
        .and_then(|handle| Uuid::parse_str(handle.trim()).ok())
        .unwrap_or(*task_id)
}

impl PostgresBroker {
    /// Explain a terminal transition that matched no row.
    ///
    /// Distinguishes "no such task in this queue" (a real error worth
    /// surfacing — it is how the synthesised-id bug used to hide) from "the
    /// task was already cancelled/requeued/completed", which is a benign race
    /// worth a warning but not a failure.
    async fn report_missed_transition(
        &self,
        operation: &str,
        task_id: &Uuid,
        task_id_param: &oxisql_core::Value,
    ) -> Result<()> {
        let rows = self
            .conn
            .query(sql::PROBE_TASK_STATE, &[task_id_param, &self.queue_name])
            .await
            .map_err(|e| {
                CelersError::Other(format!("Failed to probe task state for {operation}: {}", e))
            })?;

        match rows.into_iter().next() {
            Some(row) => {
                let state: String = row.col("state").unwrap_or_else(|_| "unknown".to_string());
                tracing::warn!(
                    task_id = %task_id,
                    queue = %self.queue_name,
                    state = %state,
                    operation = operation,
                    "Task was not in 'processing' state; transition skipped"
                );
                Ok(())
            }
            None => Err(CelersError::Other(format!(
                "{operation} failed: no task {task_id} in queue '{}'",
                self.queue_name
            ))),
        }
    }
}

#[async_trait]
impl Broker for PostgresBroker {
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        let task_id = task.metadata.id;

        // Run before_enqueue hooks
        let ctx = HookContext {
            queue_name: self.queue_name.clone(),
            task_id: Some(task_id),
            timestamp: Utc::now(),
            metadata: json!({}),
        };
        {
            let hooks = self.hooks.read().await;
            hooks.run_before_enqueue(&ctx, &task).await?;
        }

        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
        });

        // Merge task metadata if present
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = db_metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        // UUID -> uuid_param, JSON metadata -> json_param bound through the
        // statement's `::text::jsonb` cast, everything else is an
        // already-primitive ToSqlValue (String, Vec<u8>, i32).
        let task_id_param = uuid_param(&task_id);
        let metadata_param = json_param(&db_metadata);
        self.conn
            .execute(
                sql::INSERT_TASK_NOW,
                &[
                    &task_id_param,
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &metadata_param,
                    &self.queue_name,
                ],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to enqueue task: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        // Run after_enqueue hooks
        {
            let hooks = self.hooks.read().await;
            hooks.run_after_enqueue(&ctx, &task).await?;
        }

        Ok(task_id)
    }

    async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
        // Check if queue is paused
        if self.paused.load(Ordering::SeqCst) {
            return Ok(None);
        }

        // One statement: `UPDATE ... WHERE id = (SELECT ... FOR UPDATE SKIP
        // LOCKED LIMIT 1) RETURNING ...`. `SKIP LOCKED` still gives lock-free
        // hand-off between claimers, but a claim now costs a single round trip
        // and holds a pooled connection only for that one statement instead of
        // pinning it across BEGIN/SELECT/UPDATE/COMMIT.
        let rows = self
            .conn
            .query(&sql::claim_one_sql(), &[&self.queue_name])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to dequeue task: {}", e)))?;

        let Some(row) = rows.first() else {
            return Ok(None);
        };

        let claimed = claimed_task_from_row(row)?;

        // Run after_dequeue hooks
        let ctx = HookContext {
            queue_name: self.queue_name.clone(),
            task_id: Some(claimed.task_id),
            timestamp: Utc::now(),
            metadata: json!({
                "retry_count": claimed.retry_count,
                "attempt_count": claimed.attempt_count,
            }),
        };
        {
            let hooks = self.hooks.read().await;
            hooks.run_after_dequeue(&ctx, &claimed.task).await?;
        }

        Ok(Some(BrokerMessage {
            task: claimed.task,
            // The receipt handle carries identity, not a counter: any caller
            // that round trips it can ack the exact row that was claimed.
            receipt_handle: Some(claimed.task_id.to_string()),
        }))
    }

    async fn ack(&self, task_id: &TaskId, receipt_handle: Option<&str>) -> Result<()> {
        let task_id = resolve_task_id(task_id, receipt_handle);
        let task_id_param = uuid_param(&task_id);

        // Only pay for a pre-read when a hook actually needs to observe the
        // task before it is completed.
        let hooks_registered = { self.hooks.read().await.has_ack_hooks() };
        let ctx = HookContext {
            queue_name: self.queue_name.clone(),
            task_id: Some(task_id),
            timestamp: Utc::now(),
            metadata: json!({}),
        };

        if hooks_registered {
            let rows = self
                .conn
                .query(
                    sql::SELECT_TASK_FOR_HOOKS,
                    &[&task_id_param, &self.queue_name],
                )
                .await
                .map_err(|e| CelersError::Other(format!("Failed to fetch task for ack: {}", e)))?;
            if let Some(row) = rows.first() {
                let task = hook_task_from_row(row)?;
                let hooks = self.hooks.read().await;
                hooks.run_before_ack(&ctx, &task).await?;
            }
        }

        let updated = self
            .conn
            .query(sql::ACK_TASK, &[&task_id_param, &self.queue_name])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to ack task: {}", e)))?;

        let Some(row) = updated.first() else {
            return self
                .report_missed_transition("ack", &task_id, &task_id_param)
                .await;
        };

        if hooks_registered {
            let task = hook_task_from_row(row)?;
            let hooks = self.hooks.read().await;
            hooks.run_after_ack(&ctx, &task).await?;
        }

        // Terminal rows are kept for auditing; see
        // `PostgresBroker::purge_terminal_tasks` /
        // `PostgresBroker::spawn_retention_task` for bounded pruning of the
        // dispatch table.
        Ok(())
    }

    async fn reject(
        &self,
        task_id: &TaskId,
        receipt_handle: Option<&str>,
        requeue: bool,
    ) -> Result<()> {
        let task_id = resolve_task_id(task_id, receipt_handle);
        let task_id_param = uuid_param(&task_id);

        let hooks_registered = { self.hooks.read().await.has_reject_hooks() };
        let ctx = HookContext {
            queue_name: self.queue_name.clone(),
            task_id: Some(task_id),
            timestamp: Utc::now(),
            metadata: json!({ "requeue": requeue }),
        };

        if hooks_registered {
            let rows = self
                .conn
                .query(
                    sql::SELECT_TASK_FOR_HOOKS,
                    &[&task_id_param, &self.queue_name],
                )
                .await
                .map_err(|e| {
                    CelersError::Other(format!("Failed to fetch task for reject: {}", e))
                })?;
            if let Some(row) = rows.first() {
                let task = hook_task_from_row(row)?;
                let hooks = self.hooks.read().await;
                hooks.run_before_reject(&ctx, &task).await?;
            }
        }

        let statement = if requeue {
            sql::reject_requeue_sql(self.retry_strategy)
        } else {
            sql::FAIL_TASK.to_string()
        };

        let updated = self
            .conn
            .query(&statement, &[&task_id_param, &self.queue_name])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to reject task: {}", e)))?;

        let Some(row) = updated.first() else {
            return self
                .report_missed_transition("reject", &task_id, &task_id_param)
                .await;
        };

        if requeue {
            // The statement itself decided between "retry" and "budget
            // exhausted" against the row's live counters; `state` reports
            // which branch it took.
            let new_state: String = row
                .col("state")
                .map_err(|e| CelersError::Other(format!("Failed to read state: {}", e)))?;
            let retry_count: i32 = row.col("retry_count").unwrap_or(0);
            let max_retries: i32 = row.col("max_retries").unwrap_or(0);

            if new_state == "failed" {
                // Promotion is idempotent (unique index + ON CONFLICT DO
                // NOTHING), and the row is already out of 'processing', so a
                // concurrent reject cannot double-write a DLQ entry.
                self.move_to_dlq(&task_id).await?;
                tracing::info!(
                    task_id = %task_id,
                    retry_count = retry_count,
                    max_retries = max_retries,
                    "Retry budget exhausted; task moved to the dead letter queue"
                );
            } else {
                tracing::info!(
                    task_id = %task_id,
                    retry_count = retry_count,
                    max_retries = max_retries,
                    strategy = ?self.retry_strategy,
                    "Requeued task with backoff"
                );
            }
        }

        if hooks_registered {
            let task = hook_task_from_row(row)?;
            let hooks = self.hooks.read().await;
            hooks.run_after_reject(&ctx, &task).await?;
        }

        Ok(())
    }

    async fn queue_size(&self) -> Result<usize> {
        let rows = self
            .conn
            .query(sql::QUEUE_SIZE, &[&self.queue_name])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get queue size: {}", e)))?;

        // `COUNT(*)` always returns exactly one row, so the check exists
        // purely to avoid silently defaulting if that ever changes.
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to get queue size: no rows returned".to_string())
        })?;
        let count: i64 = row
            .col("count")
            .map_err(|e| CelersError::Other(format!("Failed to read count: {}", e)))?;
        Ok(usize::try_from(count).unwrap_or(0))
    }

    async fn cancel(&self, task_id: &TaskId) -> Result<bool> {
        let task_id_param = uuid_param(task_id);
        let rows_affected = self
            .conn
            .execute(sql::CANCEL_TASK, &[&task_id_param, &self.queue_name])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to cancel task: {}", e)))?;

        Ok(rows_affected > 0)
    }

    /// Schedule a task for execution at a specific Unix timestamp (seconds)
    async fn enqueue_at(&self, task: SerializedTask, execute_at: i64) -> Result<TaskId> {
        let task_id = task.metadata.id;
        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
            "scheduled_for": execute_at,
        });

        // Merge task metadata if present
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = db_metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        // Convert Unix timestamp to PostgreSQL timestamp
        let scheduled_at = chrono::DateTime::from_timestamp(execute_at, 0)
            .ok_or_else(|| CelersError::Other("Invalid timestamp".to_string()))?;

        // `scheduled_at` is bound as an RFC3339 string through the
        // statement's `$8::text::timestamptz` cast — see `row_ext.rs`'s
        // `DateTime<Utc>` parameter convention for why a bare bind of either
        // an `i64` or a `String` against a `TIMESTAMPTZ` parameter is unsafe.
        let task_id_param = uuid_param(&task_id);
        let metadata_param = json_param(&db_metadata);
        let scheduled_at_param = scheduled_at.to_rfc3339();
        self.conn
            .execute(
                sql::INSERT_TASK_AT,
                &[
                    &task_id_param,
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &metadata_param,
                    &self.queue_name,
                    &scheduled_at_param,
                ],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to enqueue delayed task: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        Ok(task_id)
    }

    /// Schedule a task for execution after a delay (seconds)
    async fn enqueue_after(&self, task: SerializedTask, delay_secs: u64) -> Result<TaskId> {
        let task_id = task.metadata.id;
        let mut db_metadata = json!({
            "queue": self.queue_name,
            "enqueued_at": chrono::Utc::now().to_rfc3339(),
            "delay_seconds": delay_secs,
        });

        // Merge task metadata if present
        if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
            if let Some(obj) = db_metadata.as_object_mut() {
                if let Some(meta_obj) = task_meta.as_object() {
                    for (k, v) in meta_obj {
                        obj.insert(k.clone(), v.clone());
                    }
                }
            }
        }

        // `delay_secs` feeds a `|| ' seconds')::INTERVAL` text concatenation,
        // so binding it as `i64` is safe (Postgres infers the parameter from
        // the `||` operator context, not as a TIMESTAMPTZ).
        let task_id_param = uuid_param(&task_id);
        let metadata_param = json_param(&db_metadata);
        let delay_secs_param = i64::try_from(delay_secs).unwrap_or(i64::MAX);
        self.conn
            .execute(
                sql::INSERT_TASK_AFTER,
                &[
                    &task_id_param,
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &metadata_param,
                    &self.queue_name,
                    &delay_secs_param,
                ],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to enqueue delayed task: {}", e)))?;

        #[cfg(feature = "metrics")]
        {
            TASKS_ENQUEUED_TOTAL.inc();
            TASKS_ENQUEUED_BY_TYPE
                .with_label_values(&[&task.metadata.name])
                .inc();
        }

        Ok(task_id)
    }

    // ========== Batch Operations (optimized overrides) ==========

    /// Optimized batch enqueue using a single transaction
    async fn enqueue_batch(&self, tasks: Vec<SerializedTask>) -> Result<Vec<TaskId>> {
        if tasks.is_empty() {
            return Ok(Vec::new());
        }

        // Transactions are a two-step on a pooled broker: check out a
        // connection, then open the transaction on it. The slot stays
        // reserved for exactly this transaction and is released when `conn`
        // is dropped.
        let conn = self.connection().await?;
        let mut tx = conn
            .transaction()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let mut task_ids = Vec::with_capacity(tasks.len());

        for task in &tasks {
            let task_id = task.metadata.id;
            let mut db_metadata = json!({
                "queue": self.queue_name,
                "enqueued_at": chrono::Utc::now().to_rfc3339(),
            });

            if let Ok(task_meta) = serde_json::to_value(&task.metadata) {
                if let Some(obj) = db_metadata.as_object_mut() {
                    if let Some(meta_obj) = task_meta.as_object() {
                        for (k, v) in meta_obj {
                            obj.insert(k.clone(), v.clone());
                        }
                    }
                }
            }

            let task_id_param = uuid_param(&task_id);
            let metadata_param = json_param(&db_metadata);
            tx.execute(
                sql::INSERT_TASK_NOW,
                &[
                    &task_id_param,
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &metadata_param,
                    &self.queue_name,
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
            for task in &tasks {
                TASKS_ENQUEUED_BY_TYPE
                    .with_label_values(&[&task.metadata.name])
                    .inc();
            }
        }

        Ok(task_ids)
    }

    /// Optimized batch dequeue: one `FOR UPDATE SKIP LOCKED` statement
    async fn dequeue_batch(&self, count: usize) -> Result<Vec<BrokerMessage>> {
        if count == 0 || self.paused.load(Ordering::SeqCst) {
            return Ok(Vec::new());
        }

        let count_param = i64::try_from(count).unwrap_or(i64::MAX);
        let rows = self
            .conn
            .query(&sql::claim_batch_sql(), &[&self.queue_name, &count_param])
            .await
            .map_err(|e| CelersError::Other(format!("Failed to dequeue batch: {}", e)))?;

        let mut messages = Vec::with_capacity(rows.len());
        for row in &rows {
            let claimed = claimed_task_from_row(row)?;
            messages.push(BrokerMessage {
                task: claimed.task,
                receipt_handle: Some(claimed.task_id.to_string()),
            });
        }

        Ok(messages)
    }

    /// Optimized batch ack using a single query with an IN (...) list
    async fn ack_batch(&self, tasks: &[(TaskId, Option<String>)]) -> Result<()> {
        if tasks.is_empty() {
            return Ok(());
        }

        // Prefer the receipt handle's identity, exactly as single `ack` does.
        let task_ids: Vec<Uuid> = tasks
            .iter()
            .map(|(id, handle)| resolve_task_id(id, handle.as_deref()))
            .collect();

        // oxisql has no array/slice `ToSqlValue`, so `= ANY($1)` is expressed
        // as a dynamically sized `IN ($1, .., $N)` list. Only the *count* of
        // placeholders is generated — no value is ever spliced into the SQL
        // text, so this stays injection-safe.
        let update_sql = sql::ack_batch_sql(task_ids.len());
        let task_id_params: Vec<oxisql_core::Value> = task_ids.iter().map(uuid_param).collect();
        let mut param_refs: Vec<&dyn oxisql_core::ToSqlValue> = task_id_params
            .iter()
            .map(|p| p as &dyn oxisql_core::ToSqlValue)
            .collect();
        param_refs.push(&self.queue_name);

        let affected = self
            .conn
            .execute(&update_sql, &param_refs)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to batch ack tasks: {}", e)))?;

        if affected < task_ids.len() as u64 {
            tracing::warn!(
                queue = %self.queue_name,
                requested = task_ids.len(),
                acknowledged = affected,
                "Some tasks were not in 'processing' state at batch ack time"
            );
        }

        Ok(())
    }
}
