//! Broker trait implementation for PostgresBroker

use async_trait::async_trait;
use celers_core::{Broker, BrokerMessage, CelersError, Result, SerializedTask, TaskId};
use chrono::Utc;
use oxisql_core::Connection;
use serde_json::json;
use std::sync::atomic::Ordering;
use uuid::Uuid;

use crate::row_ext::{json_param, uuid_from_row, uuid_param, RowExt};
use crate::types::HookContext;
use crate::PostgresBroker;

#[cfg(feature = "metrics")]
use celers_metrics::{TASKS_ENQUEUED_BY_TYPE, TASKS_ENQUEUED_TOTAL};

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

        // Byte-for-byte identical SQL text to the pre-migration sqlx version.
        // UUID -> uuid_param, JSON metadata -> json_param, everything else is
        // an already-primitive ToSqlValue (String, Vec<u8>, i32).
        let task_id_param = uuid_param(&task_id);
        let metadata_param = json_param(&db_metadata);
        self.conn
            .execute(
                r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), NOW())
            "#,
                &[
                    &task_id_param,
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &metadata_param,
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

        // Use FOR UPDATE SKIP LOCKED to atomically claim a task
        // This is the magic that makes distributed workers work without contention
        let mut tx = self
            .conn
            .transaction()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let rows = tx
            .query(
                r#"
            SELECT id, task_name, payload, retry_count
            FROM celers_tasks
            WHERE state = 'pending'
              AND scheduled_at <= NOW()
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT 1
            "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to dequeue task: {}", e)))?;

        // .fetch_optional semantics: take the first row if present, else None.
        if let Some(row) = rows.into_iter().next() {
            let task_id: Uuid = uuid_from_row(&row, "id")
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

            // Mark as processing
            let task_id_param = uuid_param(&task_id);
            tx.execute(
                r#"
                UPDATE celers_tasks
                SET state = 'processing',
                    started_at = NOW(),
                    retry_count = retry_count + 1
                WHERE id = $1
                "#,
                &[&task_id_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to mark task as processing: {}", e)))?;

            tx.commit()
                .await
                .map_err(|e| CelersError::Other(format!("Failed to commit transaction: {}", e)))?;

            let task = SerializedTask::new(task_name, payload);

            // Run after_dequeue hooks
            let ctx = HookContext {
                queue_name: self.queue_name.clone(),
                task_id: Some(task_id),
                timestamp: Utc::now(),
                metadata: json!({"retry_count": retry_count}),
            };
            {
                let hooks = self.hooks.read().await;
                hooks.run_after_dequeue(&ctx, &task).await?;
            }

            Ok(Some(BrokerMessage {
                task,
                receipt_handle: Some(retry_count.to_string()),
            }))
        } else {
            tx.rollback().await.map_err(|e| {
                CelersError::Other(format!("Failed to rollback transaction: {}", e))
            })?;
            Ok(None)
        }
    }

    async fn ack(&self, task_id: &TaskId, _receipt_handle: Option<&str>) -> Result<()> {
        // Fetch task info for hooks
        let task_id_param = uuid_param(task_id);
        let rows = self
            .conn
            .query(
                r#"
            SELECT task_name, payload
            FROM celers_tasks
            WHERE id = $1
            "#,
                &[&task_id_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to fetch task for ack: {}", e)))?;

        if let Some(row) = rows.into_iter().next() {
            let task_name: String = row
                .col("task_name")
                .map_err(|e| CelersError::Other(format!("Failed to read task_name: {}", e)))?;
            let payload: Vec<u8> = row
                .col("payload")
                .map_err(|e| CelersError::Other(format!("Failed to read payload: {}", e)))?;
            let task = SerializedTask::new(task_name, payload);

            // Run before_ack hooks
            let ctx = HookContext {
                queue_name: self.queue_name.clone(),
                task_id: Some(*task_id),
                timestamp: Utc::now(),
                metadata: json!({}),
            };
            {
                let hooks = self.hooks.read().await;
                hooks.run_before_ack(&ctx, &task).await?;
            }

            self.conn
                .execute(
                    r#"
                UPDATE celers_tasks
                SET state = 'completed',
                    completed_at = NOW()
                WHERE id = $1
                "#,
                    &[&task_id_param],
                )
                .await
                .map_err(|e| CelersError::Other(format!("Failed to ack task: {}", e)))?;

            // Run after_ack hooks
            {
                let hooks = self.hooks.read().await;
                hooks.run_after_ack(&ctx, &task).await?;
            }
        }

        // Optionally delete completed tasks after a retention period
        // For now, we keep them for auditing

        Ok(())
    }

    async fn reject(
        &self,
        task_id: &TaskId,
        _receipt_handle: Option<&str>,
        requeue: bool,
    ) -> Result<()> {
        // Fetch task info for hooks
        let task_id_param = uuid_param(task_id);
        let task_rows = self
            .conn
            .query(
                r#"
            SELECT task_name, payload, retry_count, max_retries
            FROM celers_tasks
            WHERE id = $1
            "#,
                &[&task_id_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to fetch task for reject: {}", e)))?;

        if let Some(row) = task_rows.into_iter().next() {
            let task_name: String = row
                .col("task_name")
                .map_err(|e| CelersError::Other(format!("Failed to read task_name: {}", e)))?;
            let payload: Vec<u8> = row
                .col("payload")
                .map_err(|e| CelersError::Other(format!("Failed to read payload: {}", e)))?;
            let task = SerializedTask::new(task_name, payload);

            // Run before_reject hooks
            let ctx = HookContext {
                queue_name: self.queue_name.clone(),
                task_id: Some(*task_id),
                timestamp: Utc::now(),
                metadata: json!({"requeue": requeue}),
            };
            {
                let hooks = self.hooks.read().await;
                hooks.run_before_reject(&ctx, &task).await?;
            }

            if requeue {
                let retry_count: i32 = row.col("retry_count").map_err(|e| {
                    CelersError::Other(format!("Failed to read retry_count: {}", e))
                })?;
                let max_retries: i32 = row.col("max_retries").map_err(|e| {
                    CelersError::Other(format!("Failed to read max_retries: {}", e))
                })?;

                if retry_count >= max_retries {
                    // Move to DLQ
                    self.move_to_dlq(task_id).await?;
                } else {
                    // Requeue with configured retry strategy
                    let backoff_seconds = self.retry_strategy.calculate_backoff(retry_count);

                    // `backoff_seconds || ' seconds')::INTERVAL` — the interval
                    // text-concatenation trick, byte-for-byte preserved from the
                    // pre-migration SQL. `backoff_seconds` is bound as `i64`
                    // (a ToSqlValue primitive) since it feeds a `|| ' seconds'`
                    // *text* concatenation, not a direct DATE/TIMESTAMP cast —
                    // Postgres infers `$1` as `text`-compatible from the `||`
                    // operator context, so this is NOT the same binary-mismatch
                    // hazard as binding a `DateTime<Utc>` (see `row_ext.rs`'s
                    // `DateTime<Utc>` parameter convention doc comment for the
                    // full explanation of that hazard).
                    self.conn
                        .execute(
                            r#"
                    UPDATE celers_tasks
                    SET state = 'pending',
                        scheduled_at = NOW() + ($1 || ' seconds')::INTERVAL,
                        started_at = NULL,
                        worker_id = NULL
                    WHERE id = $2
                    "#,
                            &[&backoff_seconds, &task_id_param],
                        )
                        .await
                        .map_err(|e| {
                            CelersError::Other(format!("Failed to requeue task: {}", e))
                        })?;

                    tracing::info!(
                        task_id = %task_id,
                        retry_count = retry_count,
                        backoff_seconds = backoff_seconds,
                        strategy = ?self.retry_strategy,
                        "Requeued task with backoff"
                    );
                }
            } else {
                // Mark as failed permanently
                self.conn
                    .execute(
                        r#"
                    UPDATE celers_tasks
                    SET state = 'failed',
                        completed_at = NOW()
                    WHERE id = $1
                    "#,
                        &[&task_id_param],
                    )
                    .await
                    .map_err(|e| {
                        CelersError::Other(format!("Failed to mark task as failed: {}", e))
                    })?;
            }

            // Run after_reject hooks
            {
                let hooks = self.hooks.read().await;
                hooks.run_after_reject(&ctx, &task).await?;
            }
        }

        Ok(())
    }

    async fn queue_size(&self) -> Result<usize> {
        let rows = self
            .conn
            .query(
                r#"
            SELECT COUNT(*) as count
            FROM celers_tasks
            WHERE state = 'pending'
            "#,
                &[],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to get queue size: {}", e)))?;

        // .fetch_one semantics: error (not a silent default) if no row came
        // back, preserving sqlx's `fetch_one` behavior exactly. `COUNT(*)`
        // always returns exactly one row, so this should never actually
        // trigger — the check exists purely to avoid silently defaulting.
        let row = rows.into_iter().next().ok_or_else(|| {
            CelersError::Other("Failed to get queue size: no rows returned".to_string())
        })?;
        let count: i64 = row
            .col("count")
            .map_err(|e| CelersError::Other(format!("Failed to read count: {}", e)))?;
        Ok(count as usize)
    }

    async fn cancel(&self, task_id: &TaskId) -> Result<bool> {
        let task_id_param = uuid_param(task_id);
        let rows_affected = self
            .conn
            .execute(
                r#"
            UPDATE celers_tasks
            SET state = 'cancelled',
                completed_at = NOW()
            WHERE id = $1 AND state IN ('pending', 'processing')
            "#,
                &[&task_id_param],
            )
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

        // `scheduled_at` is a `DateTime<Utc>` bound as a query parameter — see
        // `row_ext.rs`'s `DateTime<Utc>` parameter convention doc comment.
        // Bind as an RFC3339 string through a `$7::text::timestamptz` cast,
        // NOT as `.timestamp()` (`i64`) — binding a raw `i64` against a
        // server-inferred `TIMESTAMPTZ` parameter sends malformed binary
        // bytes (oxisql-postgres always uses Postgres binary wire format,
        // and `i64`'s binary encoding is not a valid `TIMESTAMPTZ` binary
        // payload). The `::text` cast makes Postgres infer the parameter as
        // `TEXT`, for which `String`'s binary format IS just raw UTF-8 bytes,
        // so the bind round-trips correctly.
        let task_id_param = uuid_param(&task_id);
        let metadata_param = json_param(&db_metadata);
        let scheduled_at_param = scheduled_at.to_rfc3339();
        self.conn
            .execute(
                r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), $7::text::timestamptz)
            "#,
                &[
                    &task_id_param,
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &metadata_param,
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

        // `delay_secs` feeds the same `|| ' seconds')::INTERVAL` text-concat
        // pattern as `reject()`'s requeue path above — bound as `i64`
        // (ToSqlValue primitive), not a DateTime, so no timestamptz-binary
        // hazard applies here.
        let task_id_param = uuid_param(&task_id);
        let metadata_param = json_param(&db_metadata);
        let delay_secs_param = delay_secs as i64;
        self.conn
            .execute(
                r#"
            INSERT INTO celers_tasks
                (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
            VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), NOW() + ($7 || ' seconds')::INTERVAL)
            "#,
                &[
                    &task_id_param,
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &metadata_param,
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

        let mut tx = self
            .conn
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
                r#"
                INSERT INTO celers_tasks
                    (id, task_name, payload, state, priority, max_retries, metadata, created_at, scheduled_at)
                VALUES ($1, $2, $3, 'pending', $4, $5, $6, NOW(), NOW())
                "#,
                &[
                    &task_id_param,
                    &task.metadata.name,
                    &task.payload,
                    &task.metadata.priority,
                    &(task.metadata.max_retries as i32),
                    &metadata_param,
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

    /// Optimized batch dequeue using a single transaction with FOR UPDATE SKIP LOCKED
    async fn dequeue_batch(&self, count: usize) -> Result<Vec<BrokerMessage>> {
        if count == 0 || self.paused.load(Ordering::SeqCst) {
            return Ok(Vec::new());
        }

        let mut tx = self
            .conn
            .transaction()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to begin transaction: {}", e)))?;

        let count_param = count as i64;
        let rows = tx
            .query(
                r#"
            SELECT id, task_name, payload, retry_count
            FROM celers_tasks
            WHERE state = 'pending'
              AND scheduled_at <= NOW()
            ORDER BY priority DESC, created_at ASC
            FOR UPDATE SKIP LOCKED
            LIMIT $1
            "#,
                &[&count_param],
            )
            .await
            .map_err(|e| CelersError::Other(format!("Failed to dequeue batch: {}", e)))?;

        if rows.is_empty() {
            tx.rollback().await.map_err(|e| {
                CelersError::Other(format!("Failed to rollback transaction: {}", e))
            })?;
            return Ok(Vec::new());
        }

        let mut messages = Vec::with_capacity(rows.len());
        let mut task_ids = Vec::with_capacity(rows.len());

        for row in &rows {
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

            messages.push(BrokerMessage {
                task: SerializedTask::new(task_name, payload),
                receipt_handle: Some(retry_count.to_string()),
            });

            task_ids.push(task_id);
        }

        // `WHERE id = ANY($1)` -> oxisql has no array/slice `ToSqlValue`, so
        // this is rewritten to a dynamically sized `IN ($1, $2, ..., $N)`
        // placeholder list, one `$n` per task id, each bound individually.
        // Only the *count* of placeholders is generated from `task_ids.len()`
        // — no value is ever spliced into the SQL text, so this remains
        // fully injection-safe. Flagged explicitly in the migration report
        // as a genuine (small) semantic rewrite, not a pure mechanical
        // translation.
        let placeholders: Vec<String> = (1..=task_ids.len()).map(|i| format!("${i}")).collect();
        let update_sql = format!(
            r#"
            UPDATE celers_tasks
            SET state = 'processing',
                started_at = NOW(),
                retry_count = retry_count + 1
            WHERE id IN ({})
            "#,
            placeholders.join(", ")
        );
        let task_id_params: Vec<oxisql_core::Value> = task_ids.iter().map(uuid_param).collect();
        let param_refs: Vec<&dyn oxisql_core::ToSqlValue> = task_id_params
            .iter()
            .map(|p| p as &dyn oxisql_core::ToSqlValue)
            .collect();
        tx.execute(&update_sql, &param_refs).await.map_err(|e| {
            CelersError::Other(format!("Failed to mark batch as processing: {}", e))
        })?;

        tx.commit()
            .await
            .map_err(|e| CelersError::Other(format!("Failed to commit batch dequeue: {}", e)))?;

        Ok(messages)
    }

    /// Optimized batch ack using a single query with an IN (...) list
    async fn ack_batch(&self, tasks: &[(TaskId, Option<String>)]) -> Result<()> {
        if tasks.is_empty() {
            return Ok(());
        }

        let task_ids: Vec<Uuid> = tasks.iter().map(|(id, _)| *id).collect();

        // `WHERE id = ANY($1)` -> `IN ($1, .., $N)` rewrite, same pattern and
        // same injection-safety rationale as `dequeue_batch` above.
        let placeholders: Vec<String> = (1..=task_ids.len()).map(|i| format!("${i}")).collect();
        let update_sql = format!(
            r#"
            UPDATE celers_tasks
            SET state = 'completed',
                completed_at = NOW()
            WHERE id IN ({})
            "#,
            placeholders.join(", ")
        );
        let task_id_params: Vec<oxisql_core::Value> = task_ids.iter().map(uuid_param).collect();
        let param_refs: Vec<&dyn oxisql_core::ToSqlValue> = task_id_params
            .iter()
            .map(|p| p as &dyn oxisql_core::ToSqlValue)
            .collect();
        self.conn
            .execute(&update_sql, &param_refs)
            .await
            .map_err(|e| CelersError::Other(format!("Failed to batch ack tasks: {}", e)))?;

        Ok(())
    }
}
