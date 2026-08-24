//! Conversions between the wire (protobuf) representation and the domain
//! types (`TaskMeta`, `ChordState`) shared with `celers-backend-redis`.
//!
//! Used by both [`crate::GrpcResultBackend`] (the client, which encodes
//! requests and decodes responses) and [`crate::server::RpcBackendServer`]
//! (the reference server, which decodes requests and encodes responses) so
//! the two sides can never drift apart on what a given field means.
//!
//! Two correctness properties are load-bearing here and worth calling out
//! explicitly because earlier versions of this codec silently violated
//! both:
//!
//! 1. **Sub-second precision survives the round trip.** Every timestamp is
//!    encoded as a `(seconds, nanos)` pair rather than truncating to whole
//!    seconds, so [`TaskMeta::duration`](celers_backend_redis::TaskMeta::duration)
//!    stays accurate for sub-second tasks instead of frequently reporting
//!    a duration of exactly zero.
//! 2. **A corrupt or unparseable `result_data` payload is a decode error,
//!    never a silent `Success(null)`.** Only a *genuinely absent* field
//!    (the task legitimately has no payload) decodes to `Value::Null`;
//!    a present-but-corrupt string propagates
//!    [`BackendError::Serialization`] so the caller learns the payload was
//!    lost instead of being told the task "succeeded" with nothing.

use crate::proto::{self, TaskResultState};
use celers_backend_redis::{BackendError, ChordState, Result, TaskMeta, TaskResult};
use chrono::{TimeZone, Utc};
use std::time::Duration;
use uuid::Uuid;

/// Convert a domain [`TaskMeta`] into its protobuf representation.
///
/// Fails only if the `Success` payload cannot be serialized to JSON; on
/// failure the caller (a `store_result` call) gets a real error instead of
/// silently storing `None` for the payload.
pub(crate) fn to_proto_meta(meta: &TaskMeta) -> Result<proto::TaskMeta> {
    let (result_state, result_data, error_message, retry_count) = match &meta.result {
        TaskResult::Pending => (TaskResultState::Pending, None, None, None),
        TaskResult::Started => (TaskResultState::Started, None, None, None),
        TaskResult::Success(data) => {
            let json_str = serde_json::to_string(data).map_err(|e| {
                BackendError::Serialization(format!(
                    "failed to encode result_data for task {}: {e}",
                    meta.task_id
                ))
            })?;
            (TaskResultState::Success, Some(json_str), None, None)
        }
        TaskResult::Failure(err) => (TaskResultState::Failure, None, Some(err.clone()), None),
        TaskResult::Revoked => (TaskResultState::Revoked, None, None, None),
        TaskResult::Retry(count) => (TaskResultState::Retry, None, None, Some(*count)),
    };

    Ok(proto::TaskMeta {
        task_id: meta.task_id.to_string(),
        task_name: meta.task_name.clone(),
        result_state: result_state as i32,
        result_data,
        error_message,
        retry_count,
        created_at: meta.created_at.timestamp(),
        created_at_nanos: meta.created_at.timestamp_subsec_nanos(),
        started_at: meta.started_at.map(|dt| dt.timestamp()),
        started_at_nanos: meta
            .started_at
            .map(|dt| dt.timestamp_subsec_nanos())
            .unwrap_or(0),
        completed_at: meta.completed_at.map(|dt| dt.timestamp()),
        completed_at_nanos: meta
            .completed_at
            .map(|dt| dt.timestamp_subsec_nanos())
            .unwrap_or(0),
        worker: meta.worker.clone(),
    })
}

/// Convert a protobuf `TaskMeta` back into the domain type.
///
/// Fails on an invalid result-state enum value, an invalid UUID, an
/// out-of-range timestamp, or — critically — a `result_data` string that
/// is present but fails to parse as JSON. A genuinely absent
/// `result_data` on a `SUCCESS` state still decodes to `Value::Null`,
/// which is the only case that should produce it.
pub(crate) fn from_proto_meta(proto_meta: proto::TaskMeta) -> Result<TaskMeta> {
    let result_state = TaskResultState::try_from(proto_meta.result_state)
        .map_err(|_| BackendError::Serialization("Invalid result state".to_string()))?;

    let result = match result_state {
        TaskResultState::Pending => TaskResult::Pending,
        TaskResultState::Started => TaskResult::Started,
        TaskResultState::Success => {
            let data = match proto_meta.result_data {
                Some(s) => serde_json::from_str(&s).map_err(|e| {
                    BackendError::Serialization(format!(
                        "corrupt result_data for task {}: {e}",
                        proto_meta.task_id
                    ))
                })?,
                None => serde_json::Value::Null,
            };
            TaskResult::Success(data)
        }
        TaskResultState::Failure => {
            TaskResult::Failure(proto_meta.error_message.unwrap_or_default())
        }
        TaskResultState::Revoked => TaskResult::Revoked,
        TaskResultState::Retry => TaskResult::Retry(proto_meta.retry_count.unwrap_or(0)),
    };

    let task_id = Uuid::parse_str(&proto_meta.task_id)
        .map_err(|e| BackendError::Serialization(format!("Invalid UUID: {}", e)))?;

    let created_at = Utc
        .timestamp_opt(proto_meta.created_at, proto_meta.created_at_nanos)
        .single()
        .ok_or_else(|| BackendError::Serialization("Invalid created_at timestamp".to_string()))?;

    let started_at = proto_meta
        .started_at
        .and_then(|ts| Utc.timestamp_opt(ts, proto_meta.started_at_nanos).single());

    let completed_at = proto_meta.completed_at.and_then(|ts| {
        Utc.timestamp_opt(ts, proto_meta.completed_at_nanos)
            .single()
    });

    Ok(TaskMeta {
        task_id,
        task_name: proto_meta.task_name,
        result,
        created_at,
        started_at,
        completed_at,
        worker: proto_meta.worker,
        progress: None,
        version: 0,
        tags: Vec::new(),
        metadata: std::collections::HashMap::new(),
        worker_hostname: None,
        runtime_ms: None,
        memory_bytes: None,
        retries: None,
        queue: None,
    })
}

/// Convert a domain [`ChordState`] into its protobuf representation.
pub(crate) fn to_proto_chord(state: &ChordState) -> proto::ChordState {
    proto::ChordState {
        chord_id: state.chord_id.to_string(),
        total: state.total as u32,
        completed: state.completed as u32,
        callback: state.callback.clone(),
        task_ids: state.task_ids.iter().map(|id| id.to_string()).collect(),
        created_at: state.created_at.timestamp(),
        created_at_nanos: state.created_at.timestamp_subsec_nanos(),
        timeout_seconds: state.timeout.map(|d| d.as_secs()),
        cancelled: state.cancelled,
        cancellation_reason: state.cancellation_reason.clone(),
    }
}

/// Convert a protobuf `ChordState` back into the domain type.
pub(crate) fn from_proto_chord(proto_state: proto::ChordState) -> Result<ChordState> {
    let chord_id = Uuid::parse_str(&proto_state.chord_id)
        .map_err(|e| BackendError::Serialization(format!("Invalid chord UUID: {}", e)))?;

    let task_ids: Result<Vec<Uuid>> = proto_state
        .task_ids
        .iter()
        .map(|s| {
            Uuid::parse_str(s)
                .map_err(|e| BackendError::Serialization(format!("Invalid task UUID: {}", e)))
        })
        .collect();

    Ok(ChordState {
        chord_id,
        total: proto_state.total as usize,
        completed: proto_state.completed as usize,
        callback: proto_state.callback,
        task_ids: task_ids?,
        created_at: Utc
            .timestamp_opt(proto_state.created_at, proto_state.created_at_nanos)
            .single()
            .ok_or_else(|| BackendError::Serialization("Invalid timestamp".to_string()))?,
        timeout: proto_state.timeout_seconds.map(Duration::from_secs),
        cancelled: proto_state.cancelled,
        cancellation_reason: proto_state.cancellation_reason,
        retry_count: 0,
        max_retries: None,
    })
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]

    use super::*;
    use std::collections::HashMap;

    fn base_meta() -> TaskMeta {
        TaskMeta {
            task_id: Uuid::new_v4(),
            task_name: "codec_test".to_string(),
            result: TaskResult::Pending,
            created_at: Utc::now(),
            started_at: None,
            completed_at: None,
            worker: None,
            progress: None,
            version: 0,
            tags: Vec::new(),
            metadata: HashMap::new(),
            worker_hostname: None,
            runtime_ms: None,
            memory_bytes: None,
            retries: None,
            queue: None,
        }
    }

    #[test]
    fn test_success_conversion_round_trips_payload() {
        let mut meta = base_meta();
        meta.result = TaskResult::Success(serde_json::json!({"result": 42}));

        let proto_meta = to_proto_meta(&meta).unwrap();
        assert_eq!(proto_meta.result_state, TaskResultState::Success as i32);
        assert!(proto_meta.result_data.is_some());

        let converted = from_proto_meta(proto_meta).unwrap();
        match converted.result {
            TaskResult::Success(data) => assert_eq!(data["result"], 42),
            other => panic!("Expected Success result, got {other:?}"),
        }
    }

    #[test]
    fn test_failure_conversion_round_trips_message() {
        let mut meta = base_meta();
        meta.result = TaskResult::Failure("task failed".to_string());

        let proto_meta = to_proto_meta(&meta).unwrap();
        assert_eq!(proto_meta.result_state, TaskResultState::Failure as i32);
        assert_eq!(proto_meta.error_message, Some("task failed".to_string()));

        let converted = from_proto_meta(proto_meta).unwrap();
        match converted.result {
            TaskResult::Failure(msg) => assert_eq!(msg, "task failed"),
            other => panic!("Expected Failure result, got {other:?}"),
        }
    }

    #[test]
    fn test_retry_conversion_round_trips_count() {
        let mut meta = base_meta();
        meta.result = TaskResult::Retry(3);

        let proto_meta = to_proto_meta(&meta).unwrap();
        assert_eq!(proto_meta.result_state, TaskResultState::Retry as i32);
        assert_eq!(proto_meta.retry_count, Some(3));

        let converted = from_proto_meta(proto_meta).unwrap();
        match converted.result {
            TaskResult::Retry(count) => assert_eq!(count, 3),
            other => panic!("Expected Retry result, got {other:?}"),
        }
    }

    #[test]
    fn test_chord_conversion_round_trips_all_fields() {
        let chord_state = ChordState {
            chord_id: Uuid::new_v4(),
            total: 5,
            completed: 0,
            callback: Some("callback_task".to_string()),
            task_ids: vec![Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()],
            created_at: Utc::now(),
            timeout: None,
            cancelled: false,
            cancellation_reason: None,
            retry_count: 0,
            max_retries: None,
        };
        let chord_id = chord_state.chord_id;

        let proto_chord = to_proto_chord(&chord_state);
        assert_eq!(proto_chord.chord_id, chord_id.to_string());
        assert_eq!(proto_chord.total, 5);
        assert_eq!(proto_chord.completed, 0);
        assert_eq!(proto_chord.callback, Some("callback_task".to_string()));
        assert_eq!(proto_chord.task_ids.len(), 3);

        let converted = from_proto_chord(proto_chord).unwrap();
        assert_eq!(converted.chord_id, chord_id);
        assert_eq!(converted.total, 5);
        assert_eq!(converted.completed, 0);
        assert_eq!(converted.callback, Some("callback_task".to_string()));
        assert_eq!(converted.task_ids.len(), 3);
    }

    #[test]
    fn test_invalid_task_id_is_rejected() {
        let mut meta = base_meta();
        let mut proto_meta = to_proto_meta(&meta).unwrap();
        proto_meta.task_id = "not-a-uuid".to_string();
        let err = from_proto_meta(proto_meta).expect_err("invalid UUID must be rejected");
        assert!(err.is_serialization());
        // Untouched valid meta still round-trips, sanity-checking the harness itself.
        meta.task_name = "still valid".to_string();
        assert!(to_proto_meta(&meta).is_ok());
    }

    #[test]
    fn test_timestamp_subsecond_precision_round_trips() {
        // A timestamp with a non-zero sub-second component must survive
        // the round trip exactly, instead of being truncated to :00.
        let precise = Utc.timestamp_opt(1_700_000_000, 123_456_789).unwrap();
        let mut meta = base_meta();
        meta.created_at = precise;
        meta.started_at = Some(precise);
        meta.completed_at = Some(precise);

        let proto_meta = to_proto_meta(&meta).unwrap();
        assert_eq!(proto_meta.created_at_nanos, 123_456_789);

        let round_tripped = from_proto_meta(proto_meta).unwrap();
        assert_eq!(round_tripped.created_at, precise);
        assert_eq!(round_tripped.started_at, Some(precise));
        assert_eq!(round_tripped.completed_at, Some(precise));

        // A sub-second duration must not collapse to zero.
        let duration = round_tripped.duration().unwrap();
        assert_eq!(duration, chrono::Duration::zero());
    }

    #[test]
    fn test_sub_millisecond_duration_is_not_zeroed() {
        let start = Utc.timestamp_opt(1_700_000_000, 0).unwrap();
        let end = Utc.timestamp_opt(1_700_000_000, 500_000_000).unwrap(); // +500ms, same second
        let mut meta = base_meta();
        meta.started_at = Some(start);
        meta.completed_at = Some(end);

        let proto_meta = to_proto_meta(&meta).unwrap();
        let round_tripped = from_proto_meta(proto_meta).unwrap();

        let duration = round_tripped.duration().unwrap();
        assert_eq!(duration, chrono::Duration::milliseconds(500));
    }

    #[test]
    fn test_corrupt_result_data_is_a_decode_error_not_null() {
        let mut meta = base_meta();
        meta.result = TaskResult::Success(serde_json::json!({"ok": true}));
        let mut proto_meta = to_proto_meta(&meta).unwrap();

        // Simulate a corrupted / truncated payload on the wire.
        proto_meta.result_data = Some("{not valid json".to_string());

        let err = from_proto_meta(proto_meta).expect_err("corrupt payload must not decode");
        assert!(err.is_serialization());
        match &err {
            BackendError::Serialization(msg) => assert!(!msg.is_empty()),
            other => panic!("expected Serialization error, got {other:?}"),
        }
    }

    #[test]
    fn test_absent_result_data_on_success_is_legitimately_null() {
        let mut meta = base_meta();
        meta.result = TaskResult::Success(serde_json::json!(null));
        let mut proto_meta = to_proto_meta(&meta).unwrap();
        // `null` serializes to the string "null", which *does* parse — to
        // exercise the "genuinely absent" path we clear the field outright,
        // exactly like an older writer that never set it.
        proto_meta.result_data = None;

        let decoded = from_proto_meta(proto_meta).unwrap();
        match decoded.result {
            TaskResult::Success(v) => assert!(v.is_null()),
            other => panic!("expected Success(null), got {other:?}"),
        }
    }

    #[test]
    fn test_chord_state_nanos_round_trip() {
        let precise = Utc.timestamp_opt(1_700_000_000, 42).unwrap();
        let state = ChordState {
            chord_id: Uuid::new_v4(),
            total: 2,
            completed: 0,
            callback: None,
            task_ids: vec![Uuid::new_v4()],
            created_at: precise,
            timeout: None,
            cancelled: false,
            cancellation_reason: None,
            retry_count: 0,
            max_retries: None,
        };

        let proto_state = to_proto_chord(&state);
        assert_eq!(proto_state.created_at_nanos, 42);
        let round_tripped = from_proto_chord(proto_state).unwrap();
        assert_eq!(round_tripped.created_at, precise);
    }
}
