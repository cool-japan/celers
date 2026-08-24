//! Regression tests for the push half of [`celers_core::AsyncResult::get`].
//!
//! `AsyncResult` used to wait on a timer unconditionally, so a backend with a
//! real completion signal (Redis keyspace notifications, PostgreSQL
//! `LISTEN`/`NOTIFY`) had no way to wake waiters. `ResultStore` now exposes
//! `await_result_change`, which the poll loop consults before sleeping; the
//! trait default returns `Ok(false)` so a backend that does not implement it
//! keeps polling exactly as before.

use async_trait::async_trait;
use celers_core::{
    AsyncResult, AsyncResultConfig, ResultStore, TaskId, TaskResultValue, TaskState,
};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

#[derive(Debug, Default)]
struct PushState {
    result: Mutex<Option<TaskResultValue>>,
    awaits: AtomicUsize,
    reads: AtomicUsize,
    /// The `max_wait` the poll loop most recently asked for.
    last_max_wait: Mutex<Option<Duration>>,
}

/// A backend whose result only appears once a waiter has blocked on the
/// notification hook — i.e. the value is *pushed*, never found by polling.
#[derive(Debug, Clone, Default)]
struct PushBackend {
    state: Arc<PushState>,
}

#[async_trait]
impl ResultStore for PushBackend {
    async fn store_result(
        &self,
        _task_id: TaskId,
        result: TaskResultValue,
    ) -> celers_core::Result<()> {
        *self.state.result.lock().unwrap_or_else(|e| e.into_inner()) = Some(result);
        Ok(())
    }

    async fn get_result(&self, _task_id: TaskId) -> celers_core::Result<Option<TaskResultValue>> {
        self.state.reads.fetch_add(1, Ordering::SeqCst);
        Ok(self
            .state
            .result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone())
    }

    async fn get_state(&self, _task_id: TaskId) -> celers_core::Result<TaskState> {
        Ok(TaskState::Pending)
    }

    async fn forget(&self, _task_id: TaskId) -> celers_core::Result<()> {
        Ok(())
    }

    async fn has_result(&self, _task_id: TaskId) -> celers_core::Result<bool> {
        Ok(self
            .state
            .result
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_some())
    }

    async fn await_result_change(
        &self,
        _task_id: TaskId,
        max_wait: Duration,
    ) -> celers_core::Result<bool> {
        *self
            .state
            .last_max_wait
            .lock()
            .unwrap_or_else(|e| e.into_inner()) = Some(max_wait);
        self.state.awaits.fetch_add(1, Ordering::SeqCst);
        // Stand in for "a notification arrived": the value lands while the
        // waiter is blocked here.
        *self.state.result.lock().unwrap_or_else(|e| e.into_inner()) =
            Some(TaskResultValue::Success(serde_json::json!("pushed")));
        Ok(true)
    }
}

/// Uses the default trait implementation: no notification support, pure
/// polling.
#[derive(Debug, Clone)]
struct PollingBackend {
    reads: Arc<AtomicUsize>,
    ready_after: usize,
}

#[async_trait]
impl ResultStore for PollingBackend {
    async fn store_result(
        &self,
        _task_id: TaskId,
        _result: TaskResultValue,
    ) -> celers_core::Result<()> {
        Ok(())
    }

    async fn get_result(&self, _task_id: TaskId) -> celers_core::Result<Option<TaskResultValue>> {
        let reads = self.reads.fetch_add(1, Ordering::SeqCst);
        if reads >= self.ready_after {
            return Ok(Some(TaskResultValue::Success(serde_json::json!("polled"))));
        }
        Ok(Some(TaskResultValue::Started))
    }

    async fn get_state(&self, _task_id: TaskId) -> celers_core::Result<TaskState> {
        Ok(TaskState::Running)
    }

    async fn forget(&self, _task_id: TaskId) -> celers_core::Result<()> {
        Ok(())
    }

    async fn has_result(&self, _task_id: TaskId) -> celers_core::Result<bool> {
        Ok(false)
    }
}

#[tokio::test]
async fn get_waits_on_the_backend_notification_instead_of_the_timer() {
    let backend = PushBackend::default();
    // A poll interval long enough that a timer-driven wait would visibly stall:
    // the test only completes promptly because the push path is taken.
    let result = AsyncResult::new(TaskId::new_v4(), backend.clone())
        .with_config(AsyncResultConfig::fixed(Duration::from_secs(600)));

    let value = tokio::time::timeout(Duration::from_secs(5), result.get(None))
        .await
        .expect("the push path must not fall through to the 600s sleep")
        .expect("get must succeed");

    assert_eq!(value, Some(serde_json::json!("pushed")));
    assert_eq!(
        backend.state.awaits.load(Ordering::SeqCst),
        1,
        "the poll loop must consult await_result_change"
    );
    assert_eq!(
        backend.state.reads.load(Ordering::SeqCst),
        2,
        "one read before the wait, one after the notification"
    );
}

/// The blocking wait must never outlast the caller's own deadline.
#[tokio::test]
async fn the_notification_wait_is_capped_by_the_callers_timeout() {
    let backend = PushBackend::default();
    let result = AsyncResult::new(TaskId::new_v4(), backend.clone()).with_config(
        AsyncResultConfig::fixed(Duration::from_millis(1))
            .with_max_notification_wait(Duration::from_secs(600)),
    );

    let value = result
        .get(Some(Duration::from_secs(2)))
        .await
        .expect("get must succeed");
    assert_eq!(value, Some(serde_json::json!("pushed")));

    let asked = backend
        .state
        .last_max_wait
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .expect("await_result_change must have been called");
    assert!(
        asked <= Duration::from_secs(2),
        "asked to block for {asked:?}, which is past the caller's 2s deadline"
    );
}

/// A backend that always reports "I waited" but never produces a result. Used
/// to prove the loop does not hot-spin as the caller's deadline approaches.
#[derive(Debug, Clone, Default)]
struct NeverReadyBackend {
    awaits: Arc<AtomicUsize>,
}

#[async_trait]
impl ResultStore for NeverReadyBackend {
    async fn store_result(
        &self,
        _task_id: TaskId,
        _result: TaskResultValue,
    ) -> celers_core::Result<()> {
        Ok(())
    }

    async fn get_result(&self, _task_id: TaskId) -> celers_core::Result<Option<TaskResultValue>> {
        Ok(Some(TaskResultValue::Started))
    }

    async fn get_state(&self, _task_id: TaskId) -> celers_core::Result<TaskState> {
        Ok(TaskState::Running)
    }

    async fn forget(&self, _task_id: TaskId) -> celers_core::Result<()> {
        Ok(())
    }

    async fn has_result(&self, _task_id: TaskId) -> celers_core::Result<bool> {
        Ok(false)
    }

    async fn await_result_change(
        &self,
        _task_id: TaskId,
        max_wait: Duration,
    ) -> celers_core::Result<bool> {
        self.awaits.fetch_add(1, Ordering::SeqCst);
        // A contract-abiding backend honours "or `max_wait` elapsed", which for
        // a zero wait means returning at once. If the caller kept asking with a
        // zero budget, this would spin.
        tokio::time::sleep(max_wait).await;
        Ok(true)
    }
}

/// The waiter must end at the caller's deadline having made only a handful of
/// backend calls.
///
/// The risk the poll loop guards against: as the deadline approaches, the
/// remaining budget shrinks to zero, and a backend honouring the documented
/// contract ("`Ok(true)` — a signal arrived *or* `max_wait` elapsed") returns
/// immediately for a zero `max_wait`. Re-asking with a zero budget would spin
/// until the top-of-loop deadline check fires, so the loop skips the hook once
/// the budget is exhausted.
#[tokio::test]
async fn a_zero_remaining_budget_does_not_hot_spin_on_the_notification_hook() {
    let backend = NeverReadyBackend::default();
    let result = AsyncResult::new(TaskId::new_v4(), backend.clone()).with_config(
        AsyncResultConfig::fixed(Duration::from_millis(5))
            .with_max_notification_wait(Duration::from_millis(20)),
    );

    let err = tokio::time::timeout(
        Duration::from_secs(5),
        result.get(Some(Duration::from_millis(60))),
    )
    .await
    .expect("the wait must end at the deadline, not spin")
    .expect_err("the task never completes, so this must time out");
    assert!(matches!(err, celers_core::CelersError::Timeout(_)));

    // Each await consumes at least part of the budget, so the count stays in
    // single digits. A hot spin would produce thousands.
    let awaits = backend.awaits.load(Ordering::SeqCst);
    assert!(
        awaits <= 8,
        "await_result_change was called {awaits} times in 60ms: the loop is spinning"
    );
}

/// A backend that does not override the hook must behave exactly as before.
#[tokio::test]
async fn backends_without_notification_support_still_poll() {
    let backend = PollingBackend {
        reads: Arc::new(AtomicUsize::new(0)),
        ready_after: 2,
    };
    let result = AsyncResult::new(TaskId::new_v4(), backend.clone())
        .with_config(AsyncResultConfig::fixed(Duration::from_millis(1)));

    let value = tokio::time::timeout(Duration::from_secs(5), result.get(None))
        .await
        .expect("polling must still terminate")
        .expect("get must succeed");

    assert_eq!(value, Some(serde_json::json!("polled")));
    assert_eq!(
        backend.reads.load(Ordering::SeqCst),
        3,
        "the default hook returns Ok(false), so the loop keeps re-reading"
    );
}
