//! Mapping between a claimed `celers_tasks` row and a [`BrokerMessage`].
//!
//! # Why this module exists
//!
//! Every dequeue path used to parse the row's `id` into a variable named
//! `_task_id` and then throw it away, returning
//! `SerializedTask::new(task_name, payload)` — which mints a brand new
//! `Uuid::new_v4()`. The worker then acked with that fresh id, so
//! `UPDATE celers_tasks SET state = 'completed' WHERE id = ?` matched zero
//! rows (silently), the task stayed `processing` until `recover_stuck_tasks`
//! flipped it back to `pending`, and every task eventually reached the DLQ
//! regardless of whether it had succeeded. The task's persisted `priority`,
//! `max_retries` and `timeout_secs` were lost the same way.
//!
//! [`row_to_broker_message`] is the single shared mapping all dequeue paths
//! now go through, so the defect cannot be fixed in one site and left in the
//! others.

use crate::row_ext::RowExt;
use celers_core::{BrokerMessage, CelersError, Result, SerializedTask, TaskId, TaskMetadata};
use oxisql_core::Row;
use uuid::Uuid;

/// The raw column values a dequeue `SELECT` returns for one task.
#[derive(Debug, Clone)]
pub(crate) struct DequeuedRow {
    /// The task's real primary key — the id `ack`/`reject` must target.
    pub(crate) id: Uuid,
    pub(crate) task_name: String,
    pub(crate) payload: Vec<u8>,
    pub(crate) retry_count: i32,
    pub(crate) max_retries: i32,
    pub(crate) priority: i32,
    /// Raw text of the `metadata` JSON column (`NULL` for rows written by
    /// paths that do not persist a metadata document).
    pub(crate) metadata_json: Option<String>,
}

/// Read the columns named by [`crate::sql_text::DEQUEUE_COLUMNS`] out of `row`.
pub(crate) fn read_dequeued_row(row: &Row) -> Result<DequeuedRow> {
    let map_err = |e| CelersError::Other(format!("Failed to read dequeued task row: {e}"));

    let id_str: String = row.col("id").map_err(map_err)?;
    let id = Uuid::parse_str(&id_str)
        .map_err(|e| CelersError::Other(format!("Invalid task UUID {id_str:?}: {e}")))?;

    Ok(DequeuedRow {
        id,
        task_name: row.col("task_name").map_err(map_err)?,
        payload: row.col("payload").map_err(map_err)?,
        retry_count: row.col("retry_count").map_err(map_err)?,
        max_retries: row.col("max_retries").map_err(map_err)?,
        priority: row.col("priority").map_err(map_err)?,
        metadata_json: row.col("metadata").map_err(map_err)?,
    })
}

/// Rebuild the task from its persisted metadata document.
///
/// The `metadata` column holds the task's own serialized [`TaskMetadata`]
/// merged with the broker's own labels (`queue`, `enqueued_at`, and
/// optionally `scheduled_for` / `delay_seconds` / `dedup_key` /
/// `retry_policy` / `trace_context`). `TaskMetadata` does not use
/// `deny_unknown_fields`, so those extra keys are ignored on the way back in.
///
/// Whatever the document says, the *row* is authoritative for `id`, `name`,
/// `priority` and `max_retries`: those are real columns that `reject`,
/// `move_to_dlq` and the dequeue ordering all read directly, so the returned
/// task must agree with them. A missing or unparseable document degrades to a
/// freshly built metadata carrying the same four authoritative values, never
/// to a random id.
pub(crate) fn build_serialized_task(row: &DequeuedRow) -> SerializedTask {
    let mut metadata = row
        .metadata_json
        .as_deref()
        .and_then(decode_task_metadata)
        .unwrap_or_else(|| TaskMetadata::new(row.task_name.clone()));

    metadata.id = row.id;
    metadata.name.clone_from(&row.task_name);
    metadata.priority = row.priority;
    metadata.max_retries = row.max_retries.max(0) as u32;

    SerializedTask {
        metadata,
        payload: row.payload.clone(),
    }
}

/// Decode a stored metadata document into a [`TaskMetadata`].
///
/// Distinguishes the two failure shapes so the logs stay useful:
///
/// * A document with no `id` key is simply not a task-metadata document —
///   older rows and a few auxiliary insert paths store only labels. Expected,
///   logged at debug.
/// * A document that *has* an `id` but still fails to decode is real
///   corruption or a schema/serde mismatch, and is logged at warn.
fn decode_task_metadata(raw: &str) -> Option<TaskMetadata> {
    match serde_json::from_str::<TaskMetadata>(raw) {
        Ok(metadata) => Some(metadata),
        Err(error) => {
            let looks_like_metadata = serde_json::from_str::<serde_json::Value>(raw)
                .ok()
                .and_then(|value| value.get("id").cloned())
                .is_some();
            if looks_like_metadata {
                tracing::warn!(
                    %error,
                    "Task metadata document has an id but could not be decoded; \
                     falling back to column values"
                );
            } else {
                tracing::debug!(
                    %error,
                    "Task row carries a label-only metadata document; \
                     rebuilding task metadata from column values"
                );
            }
            None
        }
    }
}

/// Map a claimed row to `(row id, message)`.
pub(crate) fn row_to_broker_message(row: &Row) -> Result<(Uuid, BrokerMessage)> {
    let dequeued = read_dequeued_row(row)?;
    let receipt_handle = receipt_handle_for(&dequeued.id, dequeued.retry_count);
    let task = build_serialized_task(&dequeued);
    Ok((
        dequeued.id,
        BrokerMessage {
            task,
            receipt_handle: Some(receipt_handle),
        },
    ))
}

/// Format the receipt handle carried back to the worker.
///
/// The handle is `"<row uuid>:<retry count>"`. The row id is the part that
/// matters — it lets `ack`/`reject` target the right row even if a caller
/// hands back a task whose metadata id was rewritten downstream. The retry
/// count is preserved because the previous format carried it alone.
pub(crate) fn receipt_handle_for(id: &Uuid, retry_count: i32) -> String {
    format!("{id}:{retry_count}")
}

/// Decide which row id `ack`/`reject` should bind.
///
/// Prefers the uuid embedded in `receipt_handle` when present and parseable,
/// falling back to `task_id`. Handles produced by an older build (a bare
/// retry count) do not parse as a uuid and fall through to `task_id`, so this
/// stays compatible with in-flight messages across an upgrade.
pub(crate) fn resolve_row_id(task_id: &TaskId, receipt_handle: Option<&str>) -> String {
    receipt_handle
        .and_then(|handle| {
            let candidate = handle.split(':').next().unwrap_or(handle);
            Uuid::parse_str(candidate).ok()
        })
        .unwrap_or(*task_id)
        .to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_row(metadata_json: Option<String>) -> DequeuedRow {
        DequeuedRow {
            id: Uuid::parse_str("11111111-2222-3333-4444-555555555555")
                .expect("static uuid parses"),
            task_name: "send_email".to_string(),
            payload: vec![1, 2, 3],
            retry_count: 2,
            max_retries: 7,
            priority: 42,
            metadata_json,
        }
    }

    /// The bug this crate shipped with: the dequeued task carried a random id.
    #[test]
    fn dequeued_task_keeps_the_database_row_id() {
        let row = sample_row(None);
        let task = build_serialized_task(&row);
        assert_eq!(task.metadata.id, row.id);
        assert_eq!(task.metadata.name, "send_email");
        assert_eq!(task.payload, vec![1, 2, 3]);
    }

    #[test]
    fn dequeued_task_keeps_column_priority_and_retry_budget() {
        let row = sample_row(None);
        let task = build_serialized_task(&row);
        assert_eq!(task.metadata.priority, 42);
        assert_eq!(task.metadata.max_retries, 7);
    }

    /// The persisted metadata document is the enqueue-side merge of the task's
    /// own `TaskMetadata` with the broker's labels; it must decode despite the
    /// extra keys, and must restore fields that have no dedicated column.
    #[test]
    fn persisted_metadata_document_round_trips_with_extra_keys() {
        let mut original = TaskMetadata::new("send_email".to_string());
        original.timeout_secs = Some(45);
        original.priority = 9;
        original.max_retries = 11;

        let mut document =
            serde_json::to_value(&original).expect("TaskMetadata serializes to an object");
        let object = document
            .as_object_mut()
            .expect("TaskMetadata serializes to an object");
        object.insert("queue".to_string(), serde_json::json!("payments"));
        object.insert(
            "enqueued_at".to_string(),
            serde_json::json!("2024-01-01T00:00:00Z"),
        );
        object.insert("dedup_key".to_string(), serde_json::json!("abc"));
        object.insert("scheduled_for".to_string(), serde_json::json!(1700000000));

        let row = sample_row(Some(
            serde_json::to_string(&document).expect("document serializes"),
        ));
        let task = build_serialized_task(&row);

        // Restored from the document (no column exists for it).
        assert_eq!(task.metadata.timeout_secs, Some(45));
        assert_eq!(task.metadata.created_at, original.created_at);
        // Overridden by the authoritative columns.
        assert_eq!(task.metadata.id, row.id);
        assert_eq!(task.metadata.priority, 42);
        assert_eq!(task.metadata.max_retries, 7);
    }

    #[test]
    fn unparseable_metadata_document_degrades_without_losing_the_id() {
        let row = sample_row(Some("{not json".to_string()));
        let task = build_serialized_task(&row);
        assert_eq!(task.metadata.id, row.id);
        assert_eq!(task.metadata.priority, 42);
        assert_eq!(task.metadata.timeout_secs, None);
    }

    /// Some auxiliary insert paths store only labels, not a full metadata
    /// document. That must degrade cleanly, not lose the row id.
    #[test]
    fn label_only_metadata_document_degrades_without_losing_the_id() {
        let row = sample_row(Some(r#"{"queue":"payments","group_id":"g1"}"#.to_string()));
        let task = build_serialized_task(&row);
        assert_eq!(task.metadata.id, row.id);
        assert_eq!(task.metadata.name, "send_email");
        assert_eq!(task.metadata.max_retries, 7);
    }

    #[test]
    fn null_metadata_column_degrades_without_losing_the_id() {
        let row = sample_row(None);
        let task = build_serialized_task(&row);
        assert_eq!(task.metadata.id, row.id);
    }

    #[test]
    fn negative_max_retries_column_does_not_wrap() {
        let mut row = sample_row(None);
        row.max_retries = -5;
        let task = build_serialized_task(&row);
        assert_eq!(task.metadata.max_retries, 0);
    }

    #[test]
    fn receipt_handle_carries_row_id_and_retry_count() {
        let id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").expect("static uuid");
        assert_eq!(
            receipt_handle_for(&id, 3),
            "11111111-2222-3333-4444-555555555555:3"
        );
    }

    #[test]
    fn resolve_row_id_prefers_the_receipt_handle() {
        let row_id = Uuid::parse_str("11111111-2222-3333-4444-555555555555").expect("static uuid");
        let other = Uuid::parse_str("99999999-9999-9999-9999-999999999999").expect("static uuid");
        let handle = receipt_handle_for(&row_id, 1);
        assert_eq!(resolve_row_id(&other, Some(&handle)), row_id.to_string());
        assert_eq!(
            resolve_row_id(&other, Some(&row_id.to_string())),
            row_id.to_string()
        );
    }

    /// Handles minted by an older build were a bare retry count.
    #[test]
    fn resolve_row_id_falls_back_for_legacy_and_missing_handles() {
        let task_id = Uuid::parse_str("99999999-9999-9999-9999-999999999999").expect("static uuid");
        assert_eq!(resolve_row_id(&task_id, Some("4")), task_id.to_string());
        assert_eq!(resolve_row_id(&task_id, Some("")), task_id.to_string());
        assert_eq!(resolve_row_id(&task_id, None), task_id.to_string());
    }
}
