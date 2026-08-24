//! `AsyncResult::forget` must make "forgotten" distinguishable from "never
//! existed" — and must stay byte-for-byte the old behaviour when no tombstone
//! registry is attached.
//!
//! The store used here deliberately does **not** override any of the tombstone
//! hooks, so it exercises the case the registry exists for: a backend that
//! cannot record tombstones at all.

use async_trait::async_trait;
use celers_core::result_tombstone::{ResultExistence, TombstoneRegistry};
use celers_core::{AsyncResult, ResultStore, TaskId, TaskResultValue, TaskState};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// A minimal result store with no tombstone support whatsoever.
#[derive(Clone, Default)]
struct PlainStore {
    results: Arc<Mutex<HashMap<TaskId, TaskResultValue>>>,
    /// Every task id `forget` was called with, in order.
    forgotten: Arc<Mutex<Vec<TaskId>>>,
    /// Every task id `store_tombstone` was called with, in order.
    tombstoned: Arc<Mutex<Vec<TaskId>>>,
}

impl PlainStore {
    fn lock<T>(guard: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
        guard
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
}

#[async_trait]
impl ResultStore for PlainStore {
    async fn store_result(
        &self,
        task_id: TaskId,
        result: TaskResultValue,
    ) -> celers_core::Result<()> {
        Self::lock(&self.results).insert(task_id, result);
        Ok(())
    }

    async fn get_result(&self, task_id: TaskId) -> celers_core::Result<Option<TaskResultValue>> {
        Ok(Self::lock(&self.results).get(&task_id).cloned())
    }

    async fn get_state(&self, task_id: TaskId) -> celers_core::Result<TaskState> {
        Ok(match Self::lock(&self.results).get(&task_id) {
            Some(TaskResultValue::Success(_)) => TaskState::Succeeded(Vec::new()),
            Some(_) => TaskState::Failed("failed".to_string()),
            None => TaskState::Pending,
        })
    }

    async fn forget(&self, task_id: TaskId) -> celers_core::Result<()> {
        Self::lock(&self.results).remove(&task_id);
        Self::lock(&self.forgotten).push(task_id);
        Ok(())
    }

    async fn has_result(&self, task_id: TaskId) -> celers_core::Result<bool> {
        Ok(Self::lock(&self.results).contains_key(&task_id))
    }

    async fn store_tombstone(
        &self,
        tombstone: celers_core::ResultTombstone,
    ) -> celers_core::Result<()> {
        // Records the *call* so the test can prove the backend is still offered
        // the tombstone; it deliberately does not retain it, mimicking a store
        // with no tombstone column.
        Self::lock(&self.tombstoned).push(tombstone.task_id);
        Ok(())
    }
}

fn stored(store: &PlainStore, task_id: TaskId) -> AsyncResult<PlainStore> {
    AsyncResult::new(task_id, store.clone())
}

#[tokio::test]
async fn forgotten_is_distinguishable_from_never_existed() {
    let store = PlainStore::default();
    let registry = Arc::new(TombstoneRegistry::new());

    let forgotten_id = TaskId::new_v4();
    let never_existed_id = TaskId::new_v4();

    store
        .store_result(forgotten_id, TaskResultValue::Success(serde_json::json!(7)))
        .await
        .expect("store");

    let forgotten = stored(&store, forgotten_id).with_tombstone_registry(Arc::clone(&registry));
    let never = stored(&store, never_existed_id).with_tombstone_registry(Arc::clone(&registry));

    // Before the forget both look the same to a backend without tombstones.
    assert!(never.existence().await.expect("existence").is_absent());
    assert!(forgotten.existence().await.expect("existence").is_present());

    forgotten.forget().await.expect("forget");

    // After it, the tombstone is what tells them apart.
    let existence = forgotten.existence().await.expect("existence");
    assert!(existence.is_tombstoned(), "got {existence:?}");
    assert!(existence.was_deleted());
    assert!(never.existence().await.expect("existence").is_absent());

    // The registry is the shared record, so any other handle sees it too.
    assert!(registry.is_tombstoned(forgotten_id));
    assert!(!registry.is_tombstoned(never_existed_id));
    assert_eq!(registry.len(), 1);
}

#[tokio::test]
async fn forget_still_deletes_and_still_offers_the_backend_a_tombstone() {
    let store = PlainStore::default();
    let registry = Arc::new(TombstoneRegistry::new());
    let task_id = TaskId::new_v4();

    store
        .store_result(task_id, TaskResultValue::Success(serde_json::json!("v")))
        .await
        .expect("store");

    stored(&store, task_id)
        .with_tombstone_registry(registry)
        .forget()
        .await
        .expect("forget");

    assert!(!store.has_result(task_id).await.expect("has_result"));
    assert_eq!(*PlainStore::lock(&store.forgotten), vec![task_id]);
    assert_eq!(*PlainStore::lock(&store.tombstoned), vec![task_id]);
}

#[tokio::test]
async fn the_recorded_reason_reaches_a_waiter() {
    let store = PlainStore::default();
    let registry = Arc::new(TombstoneRegistry::new());
    let task_id = TaskId::new_v4();

    let handle = stored(&store, task_id).with_tombstone_registry(Arc::clone(&registry));
    handle
        .forget_with_reason("GDPR erasure request #42")
        .await
        .expect("forget");

    let tombstone = registry.get(task_id).expect("tombstone recorded");
    assert_eq!(
        tombstone.reason.as_deref(),
        Some("GDPR erasure request #42")
    );

    // A waiter must not poll forever for a result that was deleted; the reason
    // is what tells it why.
    let err = handle
        .get(Some(std::time::Duration::from_secs(30)))
        .await
        .expect_err("a forgotten result must end the wait");
    assert!(
        err.to_string().contains("GDPR erasure request #42"),
        "unexpected error: {err}"
    );
}

#[tokio::test]
async fn without_a_registry_forget_is_unchanged() {
    let store = PlainStore::default();
    let task_id = TaskId::new_v4();

    store
        .store_result(task_id, TaskResultValue::Success(serde_json::json!(1)))
        .await
        .expect("store");

    let handle = stored(&store, task_id);
    handle.forget().await.expect("forget");

    assert!(handle.tombstone_registry().is_none());
    assert_eq!(*PlainStore::lock(&store.forgotten), vec![task_id]);
    // No registry means no tombstone offered to the backend either: the call is
    // the plain `ResultStore::forget` it always was.
    assert!(PlainStore::lock(&store.tombstoned).is_empty());
    assert_eq!(
        handle.existence().await.expect("existence"),
        ResultExistence::Absent
    );
}

#[tokio::test]
async fn a_registry_tombstone_wins_over_an_absent_backend_answer() {
    let store = PlainStore::default();
    let registry = Arc::new(TombstoneRegistry::new());
    let task_id = TaskId::new_v4();

    // Something else in the process forgot it (a purge job, another handle).
    registry.mark_forgotten(task_id, Some("nightly purge".to_string()));

    let handle = stored(&store, task_id).with_tombstone_registry(registry);
    let existence = handle.existence().await.expect("existence");
    assert_eq!(
        existence.tombstone().and_then(|t| t.reason.as_deref()),
        Some("nightly purge")
    );
}
