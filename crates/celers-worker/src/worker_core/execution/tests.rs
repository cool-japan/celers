//! Unit tests for the task disposition path.
//!
//! [`super`] is 1400+ lines of "what happens to one dequeued message", and it
//! was reachable only through the whole dequeue loop: every one of its
//! functions was referenced by `worker_core.rs` and by itself, and by no test
//! module anywhere. The loop-level suite in
//! [`worker_core::tests`](crate::worker_core) exercises these paths
//! end-to-end, which proves the wiring but does not pin the *decisions*: the
//! retry-schedule precedence rule, the retry-budget boundary, and the
//! deliberate refusals encoded in [`reject_unverified`] (no error route, no
//! result written, no requeue).
//!
//! These tests call the disposition functions directly, so a change to any of
//! those decisions fails here rather than being absorbed by a broker double
//! several layers up.

use super::*;

use crate::dlq::{DlqConfig, DlqHandler};
use crate::execution_context::{SoftTimeout, TaskExecutionContext};
use crate::retry::{RetryConfig, RetryStrategy};
use crate::worker_core::support::{backoff_delay, ActiveTaskGuard, EventSink, InFlightRegistry};

use celers_core::task_signature::SignatureError;
use celers_core::{BrokerMessage, EventEmitter, Task, TaskResultValue, TaskState};

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Mutex;

// --------------------------------------------------------------------------
// Test doubles
// --------------------------------------------------------------------------

/// Records every broker-side disposition so a test can assert on which one
/// actually happened (ack vs requeue vs reject vs re-enqueue).
#[derive(Default)]
struct RecordingBroker {
    enqueued: Mutex<Vec<(SerializedTask, u64)>>,
    acked: Mutex<Vec<TaskId>>,
    requeued: Mutex<Vec<TaskId>>,
    rejected: Mutex<Vec<TaskId>>,
}

impl RecordingBroker {
    fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    fn enqueued(&self) -> Vec<(SerializedTask, u64)> {
        self.enqueued.lock().expect("lock").clone()
    }

    fn acked(&self) -> Vec<TaskId> {
        self.acked.lock().expect("lock").clone()
    }

    fn requeued(&self) -> Vec<TaskId> {
        self.requeued.lock().expect("lock").clone()
    }

    fn rejected(&self) -> Vec<TaskId> {
        self.rejected.lock().expect("lock").clone()
    }
}

#[async_trait::async_trait]
impl Broker for RecordingBroker {
    async fn enqueue(&self, task: SerializedTask) -> Result<TaskId> {
        let task_id = task.metadata.id;
        self.enqueued.lock().expect("lock").push((task, 0));
        Ok(task_id)
    }

    async fn enqueue_after(&self, task: SerializedTask, delay_secs: u64) -> Result<TaskId> {
        let task_id = task.metadata.id;
        self.enqueued.lock().expect("lock").push((task, delay_secs));
        Ok(task_id)
    }

    async fn dequeue(&self) -> Result<Option<BrokerMessage>> {
        Ok(None)
    }

    async fn ack(&self, task_id: &TaskId, _receipt_handle: Option<&str>) -> Result<()> {
        self.acked.lock().expect("lock").push(*task_id);
        Ok(())
    }

    async fn reject(
        &self,
        task_id: &TaskId,
        _receipt_handle: Option<&str>,
        requeue: bool,
    ) -> Result<()> {
        if requeue {
            self.requeued.lock().expect("lock").push(*task_id);
        } else {
            self.rejected.lock().expect("lock").push(*task_id);
        }
        Ok(())
    }

    async fn queue_size(&self) -> Result<usize> {
        Ok(0)
    }

    async fn cancel(&self, _task_id: &TaskId) -> Result<bool> {
        Ok(false)
    }
}

/// A result store that only remembers what was written to it.
#[derive(Default)]
struct RecordingResultStore {
    results: Mutex<HashMap<TaskId, TaskResultValue>>,
}

impl RecordingResultStore {
    fn get(&self, task_id: TaskId) -> Option<TaskResultValue> {
        self.results.lock().expect("lock").get(&task_id).cloned()
    }

    fn len(&self) -> usize {
        self.results.lock().expect("lock").len()
    }
}

#[async_trait::async_trait]
impl ResultStore for RecordingResultStore {
    async fn store_result(&self, task_id: TaskId, result: TaskResultValue) -> Result<()> {
        self.results.lock().expect("lock").insert(task_id, result);
        Ok(())
    }

    async fn get_result(&self, task_id: TaskId) -> Result<Option<TaskResultValue>> {
        Ok(self.get(task_id))
    }

    async fn get_state(&self, task_id: TaskId) -> Result<TaskState> {
        Ok(match self.get(task_id) {
            Some(TaskResultValue::Success(_)) => TaskState::Succeeded(Vec::new()),
            Some(_) => TaskState::Failed("recorded failure".to_string()),
            None => TaskState::Pending,
        })
    }

    async fn forget(&self, task_id: TaskId) -> Result<()> {
        self.results.lock().expect("lock").remove(&task_id);
        Ok(())
    }

    async fn has_result(&self, task_id: TaskId) -> Result<bool> {
        Ok(self.get(task_id).is_some())
    }
}

/// Collects the lifecycle events the sink drained.
#[derive(Default)]
struct CapturingEmitter {
    events: Mutex<Vec<Event>>,
}

impl CapturingEmitter {
    fn events(&self) -> Vec<Event> {
        self.events.lock().expect("lock").clone()
    }
}

#[async_trait::async_trait]
impl EventEmitter for CapturingEmitter {
    async fn emit(&self, event: Event) -> Result<()> {
        self.events.lock().expect("lock").push(event);
        Ok(())
    }

    async fn emit_batch(&self, events: Vec<Event>) -> Result<()> {
        self.events.lock().expect("lock").extend(events);
        Ok(())
    }

    fn is_enabled(&self) -> bool {
        true
    }
}

#[derive(Serialize, Deserialize)]
struct Empty {}

/// Always fails, counting attempts.
struct AlwaysFailingTask {
    runs: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Task for AlwaysFailingTask {
    type Input = Empty;
    type Output = Empty;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        self.runs.fetch_add(1, Ordering::Relaxed);
        Err(CelersError::TaskExecution("deliberate failure".to_string()))
    }

    fn name(&self) -> &'static str {
        "failing_task"
    }
}

/// Sleeps far longer than any deadline a test configures.
struct SleepyTask {
    started: Arc<AtomicUsize>,
}

#[async_trait::async_trait]
impl Task for SleepyTask {
    type Input = Empty;
    type Output = Empty;

    async fn execute(&self, _input: Self::Input) -> Result<Self::Output> {
        self.started.fetch_add(1, Ordering::Relaxed);
        sleep(Duration::from_secs(3_600)).await;
        Ok(Empty {})
    }

    fn name(&self) -> &'static str {
        "sleepy_task"
    }
}

// --------------------------------------------------------------------------
// Helpers
// --------------------------------------------------------------------------

/// A task whose payload is a plain (non-canvas) JSON object.
fn plain_task(name: &str) -> SerializedTask {
    SerializedTask::new(
        name.to_string(),
        serde_json::to_vec(&Empty {}).expect("serialize empty"),
    )
}

/// A task whose payload carries a canvas envelope built from `envelope`.
fn task_with_envelope(name: &str, envelope: serde_json::Value) -> SerializedTask {
    SerializedTask::new(
        name.to_string(),
        serde_json::to_vec(&envelope).expect("serialize envelope"),
    )
}

/// A buffered event sink plus the handle that flushes it.
struct Events {
    sink: EventSink,
    drain: tokio::task::JoinHandle<()>,
    emitter: Arc<CapturingEmitter>,
}

impl Events {
    fn new() -> Self {
        let emitter = Arc::new(CapturingEmitter::default());
        let (sink, drain) = EventSink::buffered(Arc::clone(&emitter), 64, 16);
        Self {
            sink,
            drain,
            emitter,
        }
    }

    /// Drop the sink so the drainer finishes, then return everything it saw.
    async fn drain(self) -> Vec<Event> {
        let Self {
            sink,
            drain,
            emitter,
        } = self;
        drop(sink);
        drain.await.expect("event drainer should not panic");
        emitter.events()
    }
}

/// The reasons carried by every `task-rejected` event in `events`.
fn rejected_reasons(events: &[Event]) -> Vec<String> {
    events
        .iter()
        .filter_map(|event| match event {
            Event::Task(TaskEvent::Rejected { reason, .. }) => Some(reason.clone()),
            _ => None,
        })
        .collect()
}

/// Whether `events` contains a `task-revoked` event.
fn has_revoked(events: &[Event]) -> bool {
    events
        .iter()
        .any(|event| matches!(event, Event::Task(TaskEvent::Revoked { .. })))
}

/// An enabled, memory-backed dead-letter queue.
fn dlq() -> Arc<DlqHandler> {
    Arc::new(DlqHandler::new(DlqConfig::new(true)))
}

/// Everything `run_dispatched_task` needs, with the optional collaborators a
/// test wants to assert on already wired in.
struct DispatchFixture {
    broker: Arc<RecordingBroker>,
    dlq: Arc<DlqHandler>,
    store: Arc<RecordingResultStore>,
    stats: Arc<WorkerStats>,
    in_flight: InFlightRegistry,
    registry: Arc<TaskRegistry>,
}

impl DispatchFixture {
    async fn new(task: impl Task + 'static) -> Self {
        let registry = TaskRegistry::new();
        registry.register(task).await;
        Self {
            broker: RecordingBroker::new(),
            dlq: dlq(),
            store: Arc::new(RecordingResultStore::default()),
            stats: Arc::new(WorkerStats::new()),
            in_flight: InFlightRegistry::new(),
            registry: Arc::new(registry),
        }
    }

    /// Build the dispatch for `task`, registering it as in-flight so the
    /// disposition token is claimable (which is what a real dequeue does).
    fn dispatch(
        &self,
        task: SerializedTask,
        limits: ExecutionLimits,
        max_retries: u32,
        events: &EventSink,
        exec_context: Option<TaskExecutionContext>,
    ) -> TaskDispatch<RecordingBroker> {
        let task_id = task.metadata.id;
        let task = Arc::new(task);
        self.in_flight.register(task_id, None, &task);
        let store: Arc<dyn ResultStore> = Arc::clone(&self.store) as Arc<dyn ResultStore>;
        TaskDispatch {
            broker: Arc::clone(&self.broker),
            registry: Arc::clone(&self.registry),
            task,
            task_id,
            receipt_handle: None,
            events: events.clone(),
            hostname: "test-host".to_string(),
            pid: 4242,
            stats: Arc::clone(&self.stats),
            middleware: None,
            dlq_handler: Some(Arc::clone(&self.dlq)),
            circuit_breaker: None,
            revocation_watcher: None,
            exec_context,
            in_flight: self.in_flight.clone(),
            memory_tracker: None,
            poison_pill: None,
            checkpoints: None,
            health: crate::health::HealthChecker::new(),
            limits,
            max_retries,
            retry_config: RetryConfig::default(),
            max_result_size_bytes: 0,
            signature: None,
            result_store: Some(store),
            #[cfg(feature = "workflows")]
            chord_backend: None,
        }
    }

    /// Run one dispatch to its terminal disposition, with the active-task
    /// accounting the dequeue loop would have set up.
    async fn run(&self, dispatch: TaskDispatch<RecordingBroker>) {
        self.stats.task_started();
        let guard = ActiveTaskGuard::new(Arc::clone(&self.stats), None);
        run_dispatched_task(dispatch, guard).await;
    }
}

// --------------------------------------------------------------------------
// spent_retries
// --------------------------------------------------------------------------

/// `spent_retries` is the input to every retry-budget comparison on this
/// path: only `Retrying(n)` carries a count, and every other state has to
/// read as "no attempts spent" rather than as some default.
#[test]
fn test_spent_retries_counts_only_the_retrying_state() {
    let mut task = plain_task("t");

    for state in [
        TaskState::Pending,
        TaskState::Received,
        TaskState::Reserved,
        TaskState::Running,
        TaskState::Succeeded(Vec::new()),
        TaskState::Failed("boom".to_string()),
        TaskState::Revoked,
    ] {
        task.metadata.state = state.clone();
        assert_eq!(
            spent_retries(&task),
            0,
            "state {state:?} must report no spent retries"
        );
    }

    for spent in [0u32, 1, 7, u32::MAX] {
        task.metadata.state = TaskState::Retrying(spent);
        assert_eq!(spent_retries(&task), spent);
    }
}

// --------------------------------------------------------------------------
// task_backoff_delay: the precedence rule
// --------------------------------------------------------------------------

/// The precedence rule: a task that declared its own backoff schedule gets
/// that schedule, not the worker's. Both halves are tested elsewhere
/// (`TaskRetryPolicy::from_payload` in `error_links`, `calculate_delay` in the
/// retry strategy); this pins which one wins.
#[test]
fn test_task_backoff_delay_prefers_the_policy_the_task_declared() {
    // Worker default: a 60s fixed delay, deliberately unlike the policy below.
    let worker = RetryConfig::new(
        5,
        RetryStrategy::Fixed {
            delay: Duration::from_secs(60),
        },
    );

    let task = task_with_envelope(
        "t",
        serde_json::json!({
            crate::error_links::RETRY_POLICY_KEY: {
                "delay": 2,
                "backoff": 3.0,
                "max_delay": 1_000,
                "jitter": false,
            }
        }),
    );

    // base * backoff^retry_count, straight from the task's own policy.
    assert_eq!(
        task_backoff_delay(&task, &worker, 0),
        Duration::from_secs(2)
    );
    assert_eq!(
        task_backoff_delay(&task, &worker, 1),
        Duration::from_secs(6)
    );
    assert_eq!(
        task_backoff_delay(&task, &worker, 2),
        Duration::from_secs(18)
    );

    // ...and none of those is the worker's configured 60s.
    assert_ne!(
        task_backoff_delay(&task, &worker, 0),
        backoff_delay(&worker, 0)
    );
}

/// Without a declared policy the worker's configured strategy applies
/// unchanged -- including for a payload that is not a canvas envelope at all.
#[test]
fn test_task_backoff_delay_falls_back_to_the_worker_configuration() {
    let worker = RetryConfig::new(
        5,
        RetryStrategy::Exponential {
            base_delay: Duration::from_millis(100),
            max_delay: Duration::from_secs(60),
            multiplier: 2.0,
        },
    );

    // A plain (non-envelope) payload.
    let plain = plain_task("t");
    // An envelope that carries other canvas keys but no retry policy.
    let no_policy = task_with_envelope(
        "t",
        serde_json::json!({ crate::error_links::IGNORE_ERRORS_KEY: false }),
    );
    // A policy naming only `jitter` states no schedule, so it must not
    // displace the worker's.
    let jitter_only = task_with_envelope(
        "t",
        serde_json::json!({ crate::error_links::RETRY_POLICY_KEY: { "jitter": true } }),
    );

    for retry_count in 0..4 {
        let expected = backoff_delay(&worker, retry_count);
        for task in [&plain, &no_policy, &jitter_only] {
            assert_eq!(
                task_backoff_delay(task, &worker, retry_count),
                expected,
                "retry {retry_count} must use the worker's schedule"
            );
        }
    }
}

// --------------------------------------------------------------------------
// reject_unverified: the refusals a forged message must not be able to undo
// --------------------------------------------------------------------------

/// A message that failed signature verification is disposed of *without*
/// running the error route its payload asks for, without writing a result
/// under the id it chose, and without requeue -- otherwise a forged message
/// would be a task-dispatch primitive, a result-forgery primitive, and a
/// replay loop respectively.
#[tokio::test]
async fn test_reject_unverified_ignores_the_payloads_error_route_and_never_requeues() {
    let broker = RecordingBroker::new();
    let dlq = dlq();
    let events = Events::new();

    // A payload that would enqueue an error handler if its route were honoured.
    let task = task_with_envelope(
        "forged_task",
        serde_json::json!({
            crate::error_links::ERROR_ROUTE_KEY: [
                { "task": "attacker_chosen_handler", "args": [], "kwargs": {} }
            ]
        }),
    );
    let task_id = task.metadata.id;

    reject_unverified(
        &broker,
        Some(&dlq),
        &events.sink,
        "test-host",
        4242,
        UnverifiedMessage {
            task: &task,
            receipt_handle: None,
            error: &SignatureError::Mismatch,
        },
    )
    .await;

    // Rejected without requeue: a forged/replayed message must not cycle back
    // into the queue.
    assert_eq!(broker.rejected(), vec![task_id]);
    assert!(broker.requeued().is_empty());
    assert!(broker.acked().is_empty());

    // The attacker-controlled error route was NOT dispatched.
    assert!(
        broker.enqueued().is_empty(),
        "an unverified message must not make the worker enqueue anything"
    );

    // Recorded in the DLQ under its own failure class.
    let entries = dlq.get_entries().await;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].metadata.get("failure_type").map(String::as_str),
        Some("signature_verification")
    );
    assert_eq!(
        entries[0].metadata.get("task_name").map(String::as_str),
        Some("forged_task")
    );
    assert!(entries[0].metadata.contains_key("signature_error"));

    // A `task-rejected` event names the cause and never echoes the payload.
    let observed = events.drain().await;
    let reasons = rejected_reasons(&observed);
    assert_eq!(reasons.len(), 1);
    assert!(
        reasons[0].contains("Signature verification failed"),
        "unexpected reason: {}",
        reasons[0]
    );
    assert!(
        !reasons[0].contains("attacker_chosen_handler"),
        "an unauthenticated payload must not be echoed into an event: {}",
        reasons[0]
    );
}

// --------------------------------------------------------------------------
// dead_letter: the single terminal-failure funnel
// --------------------------------------------------------------------------

/// Every terminal failure has to leave the same three traces: a stored
/// `Failure` (so an `AsyncResult` waiter resolves instead of polling forever),
/// a DLQ entry carrying the failure class, and the broker-side disposition.
#[tokio::test]
async fn test_dead_letter_records_the_failure_for_the_waiter_and_the_operator() {
    let broker = RecordingBroker::new();
    let dlq = dlq();
    let raw_store = Arc::new(RecordingResultStore::default());
    let store: Arc<dyn ResultStore> = Arc::clone(&raw_store) as Arc<dyn ResultStore>;
    let events = Events::new();

    let task = plain_task("terminal_task");
    let task_id = task.metadata.id;

    dead_letter(
        &broker,
        Some(&dlq),
        &events.sink,
        "test-host",
        4242,
        DeadLetterRequest {
            task: &task,
            task_id,
            receipt_handle: None,
            retry_count: 3,
            error_msg: "it broke",
            failure_type: "execution_error",
            extra_metadata: vec![("attempted_by", "unit-test".to_string())],
            dispose: true,
            error_links: false,
            signature: None,
            result_store: Some(&store),
        },
    )
    .await;

    match raw_store.get(task_id) {
        Some(TaskResultValue::Failure { error, .. }) => assert_eq!(error, "it broke"),
        other => panic!("expected a stored Failure, got {other:?}"),
    }

    let entries = dlq.get_entries().await;
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].retry_count, 3);
    assert_eq!(entries[0].error_message, "it broke");
    assert_eq!(
        entries[0].metadata.get("failure_type").map(String::as_str),
        Some("execution_error")
    );
    assert_eq!(
        entries[0].metadata.get("attempted_by").map(String::as_str),
        Some("unit-test")
    );

    assert_eq!(broker.rejected(), vec![task_id]);

    let observed = events.drain().await;
    assert!(
        observed
            .iter()
            .any(|event| matches!(event, Event::Task(TaskEvent::Failed { .. }))),
        "a terminal failure must publish task-failed"
    );
}

/// `dispose: false` means the shutdown drain already requeued this delivery
/// and the broker owns it now. The failure still has to be *recorded* (the
/// caller is still waiting on it), but touching the broker again would ack or
/// reject a message this worker no longer owns.
#[tokio::test]
async fn test_dead_letter_leaves_an_unclaimed_delivery_to_the_broker() {
    let broker = RecordingBroker::new();
    let dlq = dlq();
    let raw_store = Arc::new(RecordingResultStore::default());
    let store: Arc<dyn ResultStore> = Arc::clone(&raw_store) as Arc<dyn ResultStore>;
    let events = Events::new();

    let task = plain_task("unclaimed_task");
    let task_id = task.metadata.id;

    dead_letter(
        &broker,
        Some(&dlq),
        &events.sink,
        "test-host",
        4242,
        DeadLetterRequest {
            task: &task,
            task_id,
            receipt_handle: None,
            retry_count: 0,
            error_msg: "it broke",
            failure_type: "execution_error",
            extra_metadata: Vec::new(),
            dispose: false,
            error_links: false,
            signature: None,
            result_store: Some(&store),
        },
    )
    .await;

    assert!(
        raw_store.get(task_id).is_some(),
        "the waiter still resolves"
    );
    assert_eq!(dlq.get_entries().await.len(), 1);
    assert!(broker.acked().is_empty());
    assert!(broker.rejected().is_empty());
    assert!(broker.requeued().is_empty());

    let _ = events.drain().await;
}

// --------------------------------------------------------------------------
// run_dispatched_task: the retry-budget boundary
// --------------------------------------------------------------------------

/// The boundary the disposition path turns on is `spent_retries < max_retries`:
/// below it the task is re-enqueued with its counter advanced, at it the task
/// is terminal. Both sides are checked against the same budget so an off-by-one
/// in either direction fails.
#[tokio::test]
async fn test_failed_task_retries_below_the_budget_and_dead_letters_at_it() {
    const MAX_RETRIES: u32 = 2;

    for spent in 0..=MAX_RETRIES {
        let runs = Arc::new(AtomicUsize::new(0));
        let fixture = DispatchFixture::new(AlwaysFailingTask {
            runs: Arc::clone(&runs),
        })
        .await;
        let events = Events::new();

        let mut task = plain_task("failing_task");
        task.metadata.state = TaskState::Retrying(spent);
        let task_id = task.metadata.id;

        let dispatch = fixture.dispatch(
            task,
            ExecutionLimits::from_timeout(30),
            MAX_RETRIES,
            &events.sink,
            None,
        );
        fixture.run(dispatch).await;

        assert_eq!(runs.load(Ordering::Relaxed), 1, "the handler ran once");

        if spent < MAX_RETRIES {
            let enqueued = fixture.broker.enqueued();
            assert_eq!(enqueued.len(), 1, "spent {spent}: expected a retry copy");
            assert_eq!(
                enqueued[0].0.metadata.state,
                TaskState::Retrying(spent + 1),
                "spent {spent}: the retry copy must advance the counter"
            );
            // The original delivery is acked once its retry copy is queued.
            assert_eq!(fixture.broker.acked(), vec![task_id]);
            assert!(fixture.dlq.get_entries().await.is_empty());
            assert!(
                fixture.store.get(task_id).is_none(),
                "spent {spent}: a retrying task is not terminal yet"
            );
            assert_eq!(fixture.stats.retried(), 1);
        } else {
            assert!(
                fixture.broker.enqueued().is_empty(),
                "the budget is spent; nothing may be re-enqueued"
            );
            assert_eq!(fixture.broker.rejected(), vec![task_id]);
            let entries = fixture.dlq.get_entries().await;
            assert_eq!(entries.len(), 1);
            assert_eq!(entries[0].retry_count, MAX_RETRIES);
            assert_eq!(
                entries[0].metadata.get("failure_type").map(String::as_str),
                Some("execution_error")
            );
            match fixture.store.get(task_id) {
                Some(TaskResultValue::Failure { error, .. }) => {
                    assert!(error.contains("deliberate failure"), "unexpected: {error}");
                }
                other => panic!("expected a stored Failure, got {other:?}"),
            }
            assert_eq!(fixture.stats.retried(), 0);
        }

        let _ = events.drain().await;
    }
}

// --------------------------------------------------------------------------
// run_dispatched_task: the revoked arm
// --------------------------------------------------------------------------

/// A task revoked while running is abandoned, not retried: a deliberate
/// operator action must not be undone by the retry budget, and the message has
/// to be acked so the broker does not redeliver it to run again.
#[tokio::test]
async fn test_revoked_task_is_acked_without_retry_or_dead_letter() {
    let started = Arc::new(AtomicUsize::new(0));
    let fixture = DispatchFixture::new(SleepyTask {
        started: Arc::clone(&started),
    })
    .await;
    let events = Events::new();

    let task = plain_task("sleepy_task");
    let task_id = task.metadata.id;

    // A token that is already tripped: the revocation arrived while the task
    // was in flight.
    let token = crate::cancellation::CancellationToken::new(task_id);
    token.cancel();
    let exec_context = Some(TaskExecutionContext::with_soft_timeout(
        token,
        SoftTimeout::new(task_id, None),
    ));

    let dispatch = fixture.dispatch(
        task,
        // A generous budget on both axes: neither may change the outcome.
        ExecutionLimits::from_timeout(3_600),
        5,
        &events.sink,
        exec_context,
    );
    fixture.run(dispatch).await;

    assert_eq!(fixture.broker.acked(), vec![task_id]);
    assert!(
        fixture.broker.enqueued().is_empty(),
        "a revoked task is never retried"
    );
    assert!(fixture.broker.rejected().is_empty());
    assert!(
        fixture.dlq.get_entries().await.is_empty(),
        "a revocation is not a failure"
    );
    assert_eq!(fixture.stats.revoked(), 1);
    assert_eq!(fixture.stats.retried(), 0);

    let observed = events.drain().await;
    assert!(has_revoked(&observed), "expected a task-revoked event");
}

// --------------------------------------------------------------------------
// run_dispatched_task: the deadline arms
// --------------------------------------------------------------------------

/// When the hard time limit is the binding deadline the failure has to be
/// reported as one -- a Celery `TimeLimitExceeded`, classed `hard_time_limit`
/// in the DLQ and carrying the limit an operator actually set -- rather than
/// as an ordinary execution timeout.
#[tokio::test]
async fn test_hard_time_limit_dead_letters_as_a_time_limit_breach() {
    let started = Arc::new(AtomicUsize::new(0));
    let fixture = DispatchFixture::new(SleepyTask {
        started: Arc::clone(&started),
    })
    .await;
    let events = Events::new();

    let task = plain_task("sleepy_task");
    let task_id = task.metadata.id;

    let limits = ExecutionLimits::from_timeout(3_600).with_time_limits(
        &celers_core::time_limit::TimeLimitConfig::new().with_hard_limit(Duration::from_millis(30)),
    );
    assert!(limits.hard_limit_is_binding());

    // No retry budget, so the breach is terminal on this attempt.
    let dispatch = fixture.dispatch(task, limits, 0, &events.sink, None);
    fixture.run(dispatch).await;

    assert_eq!(started.load(Ordering::Relaxed), 1);
    let entries = fixture.dlq.get_entries().await;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].metadata.get("failure_type").map(String::as_str),
        Some("hard_time_limit")
    );
    assert_eq!(
        entries[0]
            .metadata
            .get("hard_limit_millis")
            .map(String::as_str),
        Some("30")
    );
    assert!(
        fixture.store.get(task_id).is_some(),
        "the caller must learn the task died on its time limit"
    );
    assert_eq!(fixture.broker.rejected(), vec![task_id]);

    let _ = events.drain().await;
}

/// A plain execution timeout with no hard limit configured keeps the ordinary
/// `timeout` failure class, so the two are distinguishable downstream.
#[tokio::test]
async fn test_plain_execution_timeout_keeps_its_own_failure_class() {
    let started = Arc::new(AtomicUsize::new(0));
    let fixture = DispatchFixture::new(SleepyTask {
        started: Arc::clone(&started),
    })
    .await;
    let events = Events::new();

    let task = plain_task("sleepy_task");
    let task_id = task.metadata.id;

    // `timeout_secs` is whole seconds; one second is the shortest deadline the
    // configuration can express.
    let dispatch = fixture.dispatch(
        task,
        ExecutionLimits::from_timeout(1),
        0,
        &events.sink,
        None,
    );
    fixture.run(dispatch).await;

    let entries = fixture.dlq.get_entries().await;
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].metadata.get("failure_type").map(String::as_str),
        Some("timeout")
    );
    assert_eq!(
        entries[0].metadata.get("timeout_secs").map(String::as_str),
        Some("1")
    );
    assert_eq!(fixture.broker.rejected(), vec![task_id]);

    let _ = events.drain().await;
}

// --------------------------------------------------------------------------
// arm_soft_time_limit
// --------------------------------------------------------------------------

/// The soft limit warns; it never ends the task. Arming it has to trip the
/// cooperative signal, count the expiry and publish the breach -- and the
/// cancellation token must stay untouched, or the worker would treat the task
/// as revoked (acked, never retried) instead of letting it finish.
#[tokio::test]
async fn test_arm_soft_time_limit_trips_the_signal_and_reports_the_breach() {
    let task_id = TaskId::new_v4();
    let stats = Arc::new(WorkerStats::new());
    let events = Events::new();

    let token = crate::cancellation::CancellationToken::new(task_id);
    let context = TaskExecutionContext::with_soft_timeout(
        token.clone(),
        SoftTimeout::new(task_id, Some(Duration::from_millis(10))),
    );
    let limits = ExecutionLimits {
        timeout_secs: 3_600,
        soft_limit: Some(Duration::from_millis(10)),
        hard_limit: None,
    };

    let timer = arm_soft_time_limit(
        Some(&context),
        &limits,
        Instant::now(),
        SoftLimitReporter {
            task_id,
            task_name: "sleepy_task",
            hostname: "test-host",
            stats: &stats,
            events: &events.sink,
        },
    )
    .expect("a configured soft limit must arm a timer");

    timer.await.expect("the soft-limit timer should not panic");

    assert!(context.is_soft_time_limit_exceeded());
    assert_eq!(stats.soft_timeouts(), 1);
    assert!(
        !token.is_cancelled(),
        "a soft limit must not cancel the task"
    );

    let observed = events.drain().await;
    assert!(
        observed
            .iter()
            .any(|event| matches!(event, Event::Task(TaskEvent::SoftTimeLimitExceeded { .. }))),
        "expected a task-soft-time-limit-exceeded event"
    );
}

/// Nothing to arm without a configured soft limit, and nothing to arm without
/// an execution context to trip -- in both cases the timer must simply not
/// exist rather than firing against a task that never asked for one.
#[tokio::test]
async fn test_arm_soft_time_limit_is_inert_without_a_limit_or_a_context() {
    let task_id = TaskId::new_v4();
    let stats = Arc::new(WorkerStats::new());
    let events = Events::new();

    let reporter = || SoftLimitReporter {
        task_id,
        task_name: "sleepy_task",
        hostname: "test-host",
        stats: &stats,
        events: &events.sink,
    };

    let context = TaskExecutionContext::with_soft_timeout(
        crate::cancellation::CancellationToken::new(task_id),
        SoftTimeout::new(task_id, Some(Duration::from_millis(10))),
    );

    // No soft limit in the resolved execution limits.
    assert!(arm_soft_time_limit(
        Some(&context),
        &ExecutionLimits::from_timeout(30),
        Instant::now(),
        reporter(),
    )
    .is_none());

    // A soft limit, but no context for the task to observe it through.
    let limits = ExecutionLimits {
        timeout_secs: 30,
        soft_limit: Some(Duration::from_millis(10)),
        hard_limit: None,
    };
    assert!(arm_soft_time_limit(None, &limits, Instant::now(), reporter()).is_none());

    // A context built without the soft limit has nothing to trip either.
    let unlimited = TaskExecutionContext::with_soft_timeout(
        crate::cancellation::CancellationToken::new(task_id),
        SoftTimeout::unlimited(task_id),
    );
    assert!(arm_soft_time_limit(Some(&unlimited), &limits, Instant::now(), reporter()).is_none());

    assert_eq!(stats.soft_timeouts(), 0);
    let _ = events.drain().await;
}

/// A result store is optional: without one the disposition still has to
/// complete (event, DLQ entry, broker reject) rather than skipping the rest of
/// the terminal path.
#[tokio::test]
async fn test_terminal_failure_without_a_result_store_still_disposes_of_the_message() {
    let runs = Arc::new(AtomicUsize::new(0));
    let fixture = DispatchFixture::new(AlwaysFailingTask {
        runs: Arc::clone(&runs),
    })
    .await;
    let events = Events::new();

    let task = plain_task("failing_task");
    let task_id = task.metadata.id;

    let mut dispatch = fixture.dispatch(
        task,
        ExecutionLimits::from_timeout(30),
        0,
        &events.sink,
        None,
    );
    dispatch.result_store = None;
    fixture.run(dispatch).await;

    assert_eq!(runs.load(Ordering::Relaxed), 1);
    assert_eq!(fixture.dlq.get_entries().await.len(), 1);
    assert_eq!(fixture.broker.rejected(), vec![task_id]);
    assert_eq!(fixture.store.len(), 0);

    let _ = events.drain().await;
}
