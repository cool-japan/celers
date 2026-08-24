//! Regression tests for [`celers_core::ResultStore::store_result_named`].
//!
//! `store_result(&self, TaskId, TaskResultValue)` carries no task name, so a
//! backend that wants to honour a per-task-type TTL had to recover the name by
//! reading the record it was about to write — impossible for the very first
//! write, i.e. exactly the case that matters. `store_result_named` closes that
//! without a breaking trait change: the default implementation delegates to
//! `store_result`, so backends that do not override it behave exactly as before.

use async_trait::async_trait;
use celers_core::result_ttl::ResultTtlConfig;
use celers_core::{ResultStore, TaskId, TaskResultValue, TaskState};
use std::sync::Mutex;
use std::time::Duration;

/// A backend that does *not* override `store_result_named`: it must still see
/// the write, unchanged, through the default delegation.
#[derive(Debug, Default)]
struct LegacyBackend {
    writes: Mutex<Vec<TaskId>>,
}

impl LegacyBackend {
    fn writes(&self) -> Vec<TaskId> {
        self.writes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[async_trait]
impl ResultStore for LegacyBackend {
    async fn store_result(
        &self,
        task_id: TaskId,
        _result: TaskResultValue,
    ) -> celers_core::Result<()> {
        self.writes
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(task_id);
        Ok(())
    }

    async fn get_result(&self, _task_id: TaskId) -> celers_core::Result<Option<TaskResultValue>> {
        Ok(None)
    }

    async fn get_state(&self, _task_id: TaskId) -> celers_core::Result<TaskState> {
        Ok(TaskState::Pending)
    }

    async fn forget(&self, _task_id: TaskId) -> celers_core::Result<()> {
        Ok(())
    }

    async fn has_result(&self, _task_id: TaskId) -> celers_core::Result<bool> {
        Ok(false)
    }
}

/// One recorded write: the task id, the name it arrived with (if any), and the
/// TTL the backend resolved from that name.
type AppliedTtl = (TaskId, Option<String>, Option<Duration>);

/// A name-aware backend: it records the TTL it would have applied, which is
/// only reachable because the name arrives with the write.
#[derive(Debug)]
struct NameAwareBackend {
    ttl_config: ResultTtlConfig,
    applied: Mutex<Vec<AppliedTtl>>,
}

impl NameAwareBackend {
    fn new(ttl_config: ResultTtlConfig) -> Self {
        Self {
            ttl_config,
            applied: Mutex::new(Vec::new()),
        }
    }

    fn applied(&self) -> Vec<AppliedTtl> {
        self.applied
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }
}

#[async_trait]
impl ResultStore for NameAwareBackend {
    async fn store_result(
        &self,
        task_id: TaskId,
        result: TaskResultValue,
    ) -> celers_core::Result<()> {
        // The nameless path falls back to the default TTL.
        self.store_result_named(task_id, None, result).await
    }

    async fn store_result_named(
        &self,
        task_id: TaskId,
        task_name: Option<&str>,
        _result: TaskResultValue,
    ) -> celers_core::Result<()> {
        let ttl = task_name.map_or_else(
            || self.ttl_config.ttl_for(""),
            |name| self.ttl_config.ttl_for(name),
        );
        self.applied
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push((task_id, task_name.map(str::to_string), ttl));
        Ok(())
    }

    async fn get_result(&self, _task_id: TaskId) -> celers_core::Result<Option<TaskResultValue>> {
        Ok(None)
    }

    async fn get_state(&self, _task_id: TaskId) -> celers_core::Result<TaskState> {
        Ok(TaskState::Pending)
    }

    async fn forget(&self, _task_id: TaskId) -> celers_core::Result<()> {
        Ok(())
    }

    async fn has_result(&self, _task_id: TaskId) -> celers_core::Result<bool> {
        Ok(false)
    }
}

#[tokio::test]
async fn the_default_implementation_delegates_to_store_result() {
    let backend = LegacyBackend::default();
    let task_id = TaskId::new_v4();

    backend
        .store_result_named(
            task_id,
            Some("reports.generate"),
            TaskResultValue::Success(serde_json::json!(1)),
        )
        .await
        .expect("the default delegation must not fail");

    assert_eq!(
        backend.writes(),
        vec![task_id],
        "a backend that ignores the name must still see the write"
    );
}

#[tokio::test]
async fn a_name_aware_backend_reaches_the_per_task_ttl_override() {
    let ttl = ResultTtlConfig::with_default(Duration::from_secs(3_600))
        .with_task_ttl("reports.generate", Duration::from_secs(86_400));
    let backend = NameAwareBackend::new(ttl);

    let expensive = TaskId::new_v4();
    let unknown = TaskId::new_v4();

    backend
        .store_result_named(
            expensive,
            Some("reports.generate"),
            TaskResultValue::Success(serde_json::json!("report")),
        )
        .await
        .expect("store must succeed");
    backend
        .store_result_named(
            unknown,
            Some("some.other.task"),
            TaskResultValue::Success(serde_json::json!("x")),
        )
        .await
        .expect("store must succeed");

    let applied = backend.applied();
    assert_eq!(
        applied[0],
        (
            expensive,
            Some("reports.generate".to_string()),
            Some(Duration::from_secs(86_400))
        ),
        "the per-task override is only reachable because the name arrived"
    );
    assert_eq!(
        applied[1],
        (
            unknown,
            Some("some.other.task".to_string()),
            Some(Duration::from_secs(3_600))
        ),
        "an unknown task falls back to the default TTL"
    );
}

#[tokio::test]
async fn a_nameless_write_falls_back_to_the_default_ttl() {
    let ttl = ResultTtlConfig::with_default(Duration::from_secs(3_600))
        .with_task_ttl("reports.generate", Duration::from_secs(86_400));
    let backend = NameAwareBackend::new(ttl);
    let task_id = TaskId::new_v4();

    // The old, name-free entry point still works and must not invent a name.
    backend
        .store_result(task_id, TaskResultValue::Success(serde_json::json!(0)))
        .await
        .expect("store must succeed");

    let applied = backend.applied();
    assert_eq!(applied.len(), 1);
    assert_eq!(applied[0].1, None);
    assert_eq!(applied[0].2, Some(Duration::from_secs(3_600)));
}
