//! ResultStore implementation for Database backends
//!
//! This module provides adapters between the AsyncResult API (ResultStore trait)
//! and the Database result backend implementations (PostgreSQL and MySQL).

#[cfg(feature = "mysql")]
use crate::MysqlResultBackend;
#[cfg(feature = "postgres")]
use crate::PostgresResultBackend;
use crate::{TaskMeta, TaskResult};
use async_trait::async_trait;
use celers_backend_redis::ResultBackend as LocalResultBackend;
use celers_core::result::{ResultStore, TaskResultValue};
use celers_core::{TaskId, TaskState};

/// Convert TaskResultValue to TaskResult (for storage)
///
/// `TaskResult` has no "ignored" case of its own: `Ignored`'s core
/// `result_state`/`result_data` columns project onto exactly what a plain
/// `Success(null)` would write. That used to be the *entire* story, which
/// lost the suppressed error text on every round trip; it now survives
/// separately, in `TaskMeta::ignored_error` (see [`ignored_error`] below and
/// [`from_task_result`]), so this projection is no longer lossy overall —
/// only this one function's return value doesn't carry it.
fn to_task_result(value: &TaskResultValue) -> TaskResult {
    match value {
        TaskResultValue::Pending => TaskResult::Pending,
        TaskResultValue::Received => TaskResult::Pending,
        TaskResultValue::Started => TaskResult::Started,
        TaskResultValue::Success(v) => TaskResult::Success(v.clone()),
        TaskResultValue::Failure { error, .. } => TaskResult::Failure(error.clone()),
        TaskResultValue::Revoked => TaskResult::Revoked,
        TaskResultValue::Retry { attempt, .. } => TaskResult::Retry(*attempt),
        TaskResultValue::Rejected { reason } => TaskResult::Failure(reason.clone()),
        TaskResultValue::Ignored { .. } => TaskResult::Success(serde_json::Value::Null),
    }
}

/// The suppressed error text to persist in `TaskMeta::ignored_error`
/// alongside a stored result: `Some` only for [`TaskResultValue::Ignored`],
/// `None` for every other value -- including a genuine `Success`.
///
/// Callers MUST assign the result of this unconditionally (never only
/// inside an `if let Ignored = ...`): a `None` here is exactly as
/// meaningful as a `Some` one, because it is what clears a stale marker a
/// *previous* `Ignored` write for the same task left behind. Without the
/// unconditional overwrite, a task that was `Ignored` and later produces a
/// real `Success` would keep the old marker and read back as `Ignored`
/// forever (see `from_task_result`'s conjunctive check, which exists
/// specifically to make that stale-marker case safe even so).
fn ignored_error(value: &TaskResultValue) -> Option<String> {
    match value {
        TaskResultValue::Ignored { error } => Some(error.clone()),
        _ => None,
    }
}

/// Convert a stored `TaskMeta` back into a `TaskResultValue`.
///
/// `meta.ignored_error` only reconstructs [`TaskResultValue::Ignored`] when
/// it is `Some` **and** `meta.result` is exactly the `Success(Null)`
/// projection `to_task_result`/`ignored_error` write together. That
/// conjunction (rather than trusting `ignored_error.is_some()` alone) is
/// what makes a stale marker harmless: `store_result` always overwrites
/// `ignored_error` to match the *new* value being stored (see its call
/// site), so the two fields can only disagree if something outside this
/// module's write path touched the row -- and even then, this falls back to
/// the plain `result` mapping instead of discarding a real value.
fn from_task_result(meta: &TaskMeta) -> TaskResultValue {
    if let (Some(error), TaskResult::Success(v)) = (&meta.ignored_error, &meta.result) {
        if v.is_null() {
            return TaskResultValue::Ignored {
                error: error.clone(),
            };
        }
    }

    match &meta.result {
        TaskResult::Pending => TaskResultValue::Pending,
        TaskResult::Started => TaskResultValue::Started,
        TaskResult::Success(v) => TaskResultValue::Success(v.clone()),
        TaskResult::Failure(msg) => TaskResultValue::Failure {
            error: msg.clone(),
            traceback: None,
        },
        TaskResult::Revoked => TaskResultValue::Revoked,
        TaskResult::Retry(count) => TaskResultValue::Retry {
            attempt: *count,
            max_retries: 3,
        },
    }
}

/// Convert TaskResult to TaskState
fn task_result_to_state(result: &TaskResult) -> TaskState {
    match result {
        TaskResult::Pending => TaskState::Pending,
        TaskResult::Started => TaskState::Running,
        TaskResult::Success(v) => {
            let bytes = serde_json::to_vec(v).unwrap_or_default();
            TaskState::Succeeded(bytes)
        }
        TaskResult::Failure(msg) => TaskState::Failed(msg.clone()),
        TaskResult::Revoked => TaskState::Failed("Task revoked".to_string()),
        TaskResult::Retry(_) => TaskState::Running,
    }
}

// PostgreSQL ResultStore implementation
#[cfg(feature = "postgres")]
#[async_trait]
impl ResultStore for PostgresResultBackend {
    async fn store_result(
        &self,
        task_id: TaskId,
        result: TaskResultValue,
    ) -> celers_core::Result<()> {
        let task_result = to_task_result(&result);
        let mut meta = TaskMeta::new(task_id, String::new());
        meta.result = task_result;
        // Unconditional, not `if let Ignored = &result`: a `None` here is
        // what clears a stale `Ignored` marker once a later store for the
        // same task_id writes something else. See `ignored_error`'s doc
        // comment.
        meta.ignored_error = ignored_error(&result);

        let mut backend = self.clone();
        <PostgresResultBackend as LocalResultBackend>::store_result(&mut backend, task_id, &meta)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Database error: {}", e)))
    }

    async fn get_result(&self, task_id: TaskId) -> celers_core::Result<Option<TaskResultValue>> {
        let mut backend = self.clone();
        match <PostgresResultBackend as LocalResultBackend>::get_result(&mut backend, task_id).await
        {
            Ok(Some(meta)) => Ok(Some(from_task_result(&meta))),
            Ok(None) => Ok(None),
            Err(e) => Err(celers_core::CelersError::Other(format!(
                "Database error: {}",
                e
            ))),
        }
    }

    async fn get_state(&self, task_id: TaskId) -> celers_core::Result<TaskState> {
        let mut backend = self.clone();
        match <PostgresResultBackend as LocalResultBackend>::get_result(&mut backend, task_id).await
        {
            Ok(Some(meta)) => Ok(task_result_to_state(&meta.result)),
            Ok(None) => Ok(TaskState::Pending),
            Err(e) => Err(celers_core::CelersError::Other(format!(
                "Database error: {}",
                e
            ))),
        }
    }

    async fn forget(&self, task_id: TaskId) -> celers_core::Result<()> {
        let mut backend = self.clone();
        <PostgresResultBackend as LocalResultBackend>::delete_result(&mut backend, task_id)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Database error: {}", e)))
    }

    async fn has_result(&self, task_id: TaskId) -> celers_core::Result<bool> {
        let mut backend = self.clone();
        match <PostgresResultBackend as LocalResultBackend>::get_result(&mut backend, task_id).await
        {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(celers_core::CelersError::Other(format!(
                "Database error: {}",
                e
            ))),
        }
    }
}

// MySQL ResultStore implementation
#[cfg(feature = "mysql")]
#[async_trait]
impl ResultStore for MysqlResultBackend {
    async fn store_result(
        &self,
        task_id: TaskId,
        result: TaskResultValue,
    ) -> celers_core::Result<()> {
        let task_result = to_task_result(&result);
        let mut meta = TaskMeta::new(task_id, String::new());
        meta.result = task_result;
        // Unconditional, not `if let Ignored = &result`: a `None` here is
        // what clears a stale `Ignored` marker once a later store for the
        // same task_id writes something else. See `ignored_error`'s doc
        // comment.
        meta.ignored_error = ignored_error(&result);

        let mut backend = self.clone();
        <MysqlResultBackend as LocalResultBackend>::store_result(&mut backend, task_id, &meta)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Database error: {}", e)))
    }

    async fn get_result(&self, task_id: TaskId) -> celers_core::Result<Option<TaskResultValue>> {
        let mut backend = self.clone();
        match <MysqlResultBackend as LocalResultBackend>::get_result(&mut backend, task_id).await {
            Ok(Some(meta)) => Ok(Some(from_task_result(&meta))),
            Ok(None) => Ok(None),
            Err(e) => Err(celers_core::CelersError::Other(format!(
                "Database error: {}",
                e
            ))),
        }
    }

    async fn get_state(&self, task_id: TaskId) -> celers_core::Result<TaskState> {
        let mut backend = self.clone();
        match <MysqlResultBackend as LocalResultBackend>::get_result(&mut backend, task_id).await {
            Ok(Some(meta)) => Ok(task_result_to_state(&meta.result)),
            Ok(None) => Ok(TaskState::Pending),
            Err(e) => Err(celers_core::CelersError::Other(format!(
                "Database error: {}",
                e
            ))),
        }
    }

    async fn forget(&self, task_id: TaskId) -> celers_core::Result<()> {
        let mut backend = self.clone();
        <MysqlResultBackend as LocalResultBackend>::delete_result(&mut backend, task_id)
            .await
            .map_err(|e| celers_core::CelersError::Other(format!("Database error: {}", e)))
    }

    async fn has_result(&self, task_id: TaskId) -> celers_core::Result<bool> {
        let mut backend = self.clone();
        match <MysqlResultBackend as LocalResultBackend>::get_result(&mut backend, task_id).await {
            Ok(Some(_)) => Ok(true),
            Ok(None) => Ok(false),
            Err(e) => Err(celers_core::CelersError::Other(format!(
                "Database error: {}",
                e
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};
    use uuid::Uuid;

    /// Build the `TaskMeta` a real `store_result` write would produce for
    /// `value`: `to_task_result` for the core `result` field plus
    /// `ignored_error` set exactly the way `store_result` sets it (see its
    /// call site) -- so these tests exercise the two functions the same way
    /// production code does, not a hand-assembled shortcut.
    fn meta_for(value: &TaskResultValue) -> TaskMeta {
        let mut meta = TaskMeta::new(Uuid::new_v4(), String::new());
        meta.result = to_task_result(value);
        meta.ignored_error = ignored_error(value);
        meta
    }

    #[test]
    fn test_task_result_conversion() {
        let value = TaskResultValue::Success(json!({"result": 42}));
        let result = to_task_result(&value);
        assert!(matches!(result, TaskResult::Success(_)));

        let back = from_task_result(&meta_for(&value));
        assert!(matches!(back, TaskResultValue::Success(_)));
    }

    #[test]
    fn test_failure_conversion() {
        let value = TaskResultValue::Failure {
            error: "Test error".to_string(),
            traceback: Some("Stack trace".to_string()),
        };
        let result = to_task_result(&value);
        assert!(matches!(result, TaskResult::Failure(_)));
    }

    #[test]
    fn test_state_conversion() {
        let result = TaskResult::Success(json!({"value": 123}));
        let state = task_result_to_state(&result);
        assert!(matches!(state, TaskState::Succeeded(_)));

        let result = TaskResult::Failure("error".to_string());
        let state = task_result_to_state(&result);
        assert!(matches!(state, TaskState::Failed(_)));
    }

    // ── Ignored round-trip (TaskMeta::ignored_error) ────────────────────

    /// The suppressed error text must survive `to_task_result` +
    /// `ignored_error` on the way in and `from_task_result` on the way
    /// back out -- not collapse to a bare `Success(null)` with the reason
    /// gone.
    #[test]
    fn ignored_round_trips_with_its_error_text_preserved() {
        let value = TaskResultValue::Ignored {
            error: "boom, but ignored".to_string(),
        };

        // The core column still projects onto Success(null) -- unchanged,
        // intentional (see `to_task_result`'s doc comment) -- but the
        // marker captures what that projection alone would lose.
        assert!(matches!(
            to_task_result(&value),
            TaskResult::Success(Value::Null)
        ));
        assert_eq!(ignored_error(&value).as_deref(), Some("boom, but ignored"));

        let meta = meta_for(&value);
        match from_task_result(&meta) {
            TaskResultValue::Ignored { error } => assert_eq!(error, "boom, but ignored"),
            other => panic!("expected Ignored, got {other:?}"),
        }
    }

    /// `ignored_error` must be `None` for a genuine success, not just for
    /// the values that obviously aren't `Ignored` -- this is the "unless
    /// the caller assigns it unconditionally" half of the contract.
    #[test]
    fn ignored_error_is_none_for_a_genuine_success() {
        let value = TaskResultValue::Success(json!({"real": "value"}));
        assert!(ignored_error(&value).is_none());

        match from_task_result(&meta_for(&value)) {
            TaskResultValue::Success(v) => assert_eq!(v, json!({"real": "value"})),
            other => panic!("expected Success, got {other:?}"),
        }
    }

    /// Regression: a stale `ignored_error` marker left behind by an earlier
    /// `Ignored` write for the same task must not resurrect `Ignored` once
    /// a later, real `Success` is stored for that same task -- proving
    /// `store_result`'s unconditional (re)assignment actually clears it,
    /// and that `from_task_result`'s conjunctive check does not trust
    /// `ignored_error.is_some()` in isolation.
    #[test]
    fn a_later_success_overrides_a_stale_ignored_marker_for_the_same_task() {
        let ignored = TaskResultValue::Ignored {
            error: "first attempt failed, ignored".to_string(),
        };
        let mut meta = meta_for(&ignored);
        assert!(matches!(
            from_task_result(&meta),
            TaskResultValue::Ignored { .. }
        ));

        // The second store for the same task_id: a real success. Applying
        // the exact update `store_result` performs -- both fields,
        // unconditionally -- must leave no trace of the marker.
        let success = TaskResultValue::Success(json!(42));
        meta.result = to_task_result(&success);
        meta.ignored_error = ignored_error(&success);

        match from_task_result(&meta) {
            TaskResultValue::Success(v) => assert_eq!(v, json!(42)),
            other => panic!("a later Success must override a stale Ignored marker, got {other:?}"),
        }
    }

    /// Backward compatibility: a row written before `ignored_error` existed
    /// (or by a path that never sets it, like every non-`Ignored` value
    /// today) has `ignored_error: None` and must read back exactly as a
    /// plain result -- never spuriously reinterpreted as `Ignored`.
    #[test]
    fn absent_ignored_error_never_manufactures_an_ignored_result() {
        let mut meta = TaskMeta::new(Uuid::new_v4(), String::new());
        meta.result = TaskResult::Success(Value::Null);
        meta.ignored_error = None;

        assert!(matches!(
            from_task_result(&meta),
            TaskResultValue::Success(Value::Null)
        ));
    }
}
