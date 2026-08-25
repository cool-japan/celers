//! End-to-end semantics of the facade's workflow helpers.
//!
//! Every test here drives a **running worker** over the in-process
//! [`InMemoryBroker`], so what is asserted is what the runtime does, not what
//! the returned structure looks like. That is deliberate: the helpers in
//! `celers::advanced_patterns` and `celers::error_recovery` used to accept a
//! failure task, a set of compensations or a fallback and then chain everything
//! unconditionally, which type-level or shape-level tests happily accepted. The
//! only assertion that catches "the card is charged even though the balance
//! check failed" is one that runs the workflow.
//!
//! Each helper is covered three ways: the success path, the failure path, and
//! the **negative** case — the failure handler must *not* run when the task
//! succeeds, and the compensations must *not* run when the saga completes.
//!
//! Branch/switch evaluation and chord barriers are covered in
//! `celers-worker`'s own `workflow_semantics` integration test instead: both
//! need worker features (`canvas`, `workflows`) that this facade's manifest
//! does not turn on.

use celers::advanced_patterns::{
    create_conditional_workflow, create_parallel_chains, create_saga_workflow,
};
use celers::error_recovery::{ignore_errors, with_dlq, with_exponential_backoff, with_fallback};
use celers::{Chain, InMemoryBroker, ResultStore, Signature, TaskResultValue};
use celers_core::{Broker, BrokerMessage, Result as CoreResult, SerializedTask, TaskId};

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use celers_core::{CelersError, InMemoryResultBackend, Task, TaskRegistry};
use celers_worker::{Worker, WorkerConfig, WorkerHandle};

/// Deadline for every "wait until" helper: generous for a loaded box, short
/// enough that a genuine hang fails instead of hanging the suite.
const WAIT_DEADLINE: Duration = Duration::from_secs(5);

/// How long to let the runtime keep going before asserting that something did
/// **not** happen. A negative assertion made too early passes for the wrong
/// reason, so it has to outlast several poll intervals.
const SETTLE: Duration = Duration::from_millis(300);

// --------------------------------------------------------------------------
// Test doubles
// --------------------------------------------------------------------------

/// Shared, ordered log of which task ran and what it was handed.
#[derive(Clone, Default)]
struct RunLog {
    entries: Arc<Mutex<Vec<(String, serde_json::Value)>>>,
}

impl RunLog {
    fn record(&self, name: &str, input: serde_json::Value) {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((name.to_string(), input));
    }

    /// Names of the tasks that ran, in order.
    fn names(&self) -> Vec<String> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .map(|(name, _)| name.clone())
            .collect()
    }

    fn ran(&self, name: &str) -> bool {
        self.names().iter().any(|seen| seen == name)
    }

    fn count(&self, name: &str) -> usize {
        self.names().iter().filter(|seen| *seen == name).count()
    }

    /// The payload the first invocation of `name` received.
    fn input_of(&self, name: &str) -> Option<serde_json::Value> {
        self.entries
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .iter()
            .find(|(seen, _)| seen == name)
            .map(|(_, input)| input.clone())
    }
}

/// A task that records its invocation and then succeeds or fails.
struct Recorded {
    name: &'static str,
    log: RunLog,
    /// When set, the task fails with this message instead of succeeding.
    fails_with: Option<&'static str>,
    /// When set, the task *succeeds* with a result of this many bytes — used
    /// to trip the worker's result-size limit.
    result_bytes: Option<usize>,
    /// Counts every invocation, including retries.
    attempts: Arc<AtomicUsize>,
}

impl Recorded {
    fn ok(name: &'static str, log: &RunLog) -> Self {
        Self {
            name,
            log: log.clone(),
            fails_with: None,
            result_bytes: None,
            attempts: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn failing(name: &'static str, log: &RunLog, message: &'static str) -> Self {
        Self {
            name,
            log: log.clone(),
            fails_with: Some(message),
            result_bytes: None,
            attempts: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// A task that succeeds but returns more than the worker will accept.
    fn bulky(name: &'static str, log: &RunLog, result_bytes: usize) -> Self {
        Self {
            name,
            log: log.clone(),
            fails_with: None,
            result_bytes: Some(result_bytes),
            attempts: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn attempts(&self) -> Arc<AtomicUsize> {
        Arc::clone(&self.attempts)
    }
}

#[async_trait::async_trait]
impl Task for Recorded {
    type Input = serde_json::Value;
    type Output = serde_json::Value;

    async fn execute(&self, input: Self::Input) -> CoreResult<Self::Output> {
        self.attempts.fetch_add(1, Ordering::SeqCst);
        self.log.record(self.name, input.clone());
        if let Some(message) = self.fails_with {
            return Err(CelersError::TaskExecution(message.to_string()));
        }
        match self.result_bytes {
            Some(bytes) => Ok(serde_json::Value::String("x".repeat(bytes))),
            None => Ok(serde_json::json!({"ran": self.name})),
        }
    }

    fn name(&self) -> &str {
        self.name
    }
}

/// An [`InMemoryBroker`] that also records the delay of every scheduled
/// enqueue, so a retry *schedule* can be asserted instead of merely timed —
/// the in-memory broker runs `enqueue_after` immediately, which makes wall
/// clock useless for the purpose.
struct DelayRecordingBroker {
    inner: InMemoryBroker,
    delays: Mutex<Vec<(String, u64)>>,
}

impl DelayRecordingBroker {
    fn new() -> Self {
        Self {
            inner: InMemoryBroker::new(),
            delays: Mutex::new(Vec::new()),
        }
    }

    /// Recorded `(task name, delay seconds)` pairs, in order.
    fn delays(&self) -> Vec<(String, u64)> {
        self.delays
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
}

#[async_trait::async_trait]
impl Broker for DelayRecordingBroker {
    async fn enqueue(&self, task: SerializedTask) -> CoreResult<TaskId> {
        self.inner.enqueue(task).await
    }

    /// Records the requested delay and then enqueues **immediately**.
    ///
    /// The real in-memory broker honours the delay, which would make a
    /// `2, 4, 8, 16` schedule take half a minute of wall clock to observe. The
    /// question this broker answers is what schedule the worker *asked* for,
    /// which is the thing a retry policy actually controls.
    async fn enqueue_after(&self, task: SerializedTask, delay_secs: u64) -> CoreResult<TaskId> {
        self.delays
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .push((task.metadata.name.clone(), delay_secs));
        self.inner.enqueue(task).await
    }

    async fn enqueue_at(&self, task: SerializedTask, execute_at: i64) -> CoreResult<TaskId> {
        self.inner.enqueue_at(task, execute_at).await
    }

    async fn dequeue(&self) -> CoreResult<Option<BrokerMessage>> {
        self.inner.dequeue().await
    }

    async fn ack(&self, task_id: &TaskId, receipt_handle: Option<&str>) -> CoreResult<()> {
        self.inner.ack(task_id, receipt_handle).await
    }

    async fn reject(
        &self,
        task_id: &TaskId,
        receipt_handle: Option<&str>,
        requeue: bool,
    ) -> CoreResult<()> {
        self.inner.reject(task_id, receipt_handle, requeue).await
    }

    async fn queue_size(&self) -> CoreResult<usize> {
        self.inner.queue_size().await
    }

    async fn cancel(&self, task_id: &TaskId) -> CoreResult<bool> {
        self.inner.cancel(task_id).await
    }
}

// --------------------------------------------------------------------------
// Harness
// --------------------------------------------------------------------------

/// A worker configuration that reacts fast enough for a test and gives a task
/// exactly `max_retries` retries before the failure becomes terminal.
fn config(hostname: &str, max_retries: u32) -> WorkerConfig {
    WorkerConfig {
        concurrency: 4,
        poll_interval_ms: 5,
        shutdown_timeout_secs: 5,
        max_retries,
        hostname: hostname.to_string(),
        ..Default::default()
    }
}

/// Start a worker over `broker`, recording every outcome in `store`.
async fn start<B: Broker + 'static>(
    broker: &Arc<B>,
    registry: TaskRegistry,
    config: WorkerConfig,
    store: &Arc<InMemoryResultBackend>,
) -> WorkerHandle {
    Worker::new_from_arc(Arc::clone(broker), registry, config)
        .with_result_store(Arc::clone(store) as Arc<dyn ResultStore>)
        .run_with_shutdown()
        .await
        .expect("worker starts")
}

/// Poll `predicate` until it holds or [`WAIT_DEADLINE`] expires.
async fn wait_until<F>(what: &str, mut predicate: F)
where
    F: FnMut() -> bool,
{
    let deadline = Instant::now() + WAIT_DEADLINE;
    while Instant::now() < deadline {
        if predicate() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Give the runtime time to do the thing a test is about to assert it did not
/// do.
async fn settle() {
    tokio::time::sleep(SETTLE).await;
}

/// Dispatch a single signature as a one-step chain.
async fn dispatch<B: Broker>(broker: &B, sig: Signature) {
    Chain::new()
        .then_signature(sig)
        .apply(broker)
        .await
        .expect("dispatch");
}

/// The JSON envelope of the next message on `broker`.
async fn next_payload(broker: &InMemoryBroker) -> (String, serde_json::Value) {
    let message = broker
        .dequeue()
        .await
        .expect("dequeue")
        .expect("a message must have been enqueued");
    let payload =
        serde_json::from_slice(&message.task.payload).expect("the payload is a JSON envelope");
    (message.task.metadata.name.clone(), payload)
}

// --------------------------------------------------------------------------
// create_conditional_workflow
//
// Whether the worker takes exactly one arm is covered in `celers-worker`'s own
// `workflow_semantics` test, which needs the `canvas` worker feature this
// facade cannot turn on for itself. What is pinned here is the other half of
// that contract: the wire shape this helper actually dispatches. Without it,
// the helper could regress to a plain `condition -> success` chain — the
// original defect, where the card was charged whatever the balance check said —
// and the worker-side test would keep passing on its own hand-built branch.
// --------------------------------------------------------------------------

#[tokio::test]
async fn a_conditional_workflow_dispatches_a_branch_not_a_chain() {
    let broker = InMemoryBroker::new();

    create_conditional_workflow(
        "check_balance",
        vec![serde_json::json!({"account_id": 123})],
        "process_payment",
        vec![serde_json::json!({"amount": 100})],
        "send_insufficient_funds_notice",
        vec![serde_json::json!({"account_id": 123})],
    )
    .apply(&broker)
    .await
    .expect("dispatch");

    // Only the condition is enqueued; the arms travel with it, undecided.
    let (name, payload) = next_payload(&broker).await;
    assert_eq!(name, "check_balance");
    assert_eq!(
        broker.queue_size().await.expect("queue size"),
        0,
        "neither arm may be enqueued before the condition has run"
    );

    let tail = payload["chain"]
        .as_array()
        .expect("the condition carries the branch as its tail");
    assert_eq!(tail.len(), 1);

    let step = &tail[0];
    assert_eq!(
        step["step_type"], "branch",
        "a conditional must dispatch a branch step, not an unconditional task"
    );
    assert_eq!(step["then_branch"]["task"], "process_payment");
    assert_eq!(
        step["else_branch"]["task"],
        "send_insufficient_funds_notice"
    );
    assert_eq!(
        step["then_branch"]["args"][0],
        serde_json::json!({"amount": 100}),
        "each arm keeps the arguments it was given"
    );
}

// --------------------------------------------------------------------------
// with_fallback
// --------------------------------------------------------------------------

#[tokio::test]
async fn fallback_runs_only_when_the_primary_fails() {
    let log = RunLog::default();
    let registry = TaskRegistry::new();
    registry
        .register(Recorded::failing(
            "fetch_primary",
            &log,
            "connection refused",
        ))
        .await;
    registry.register(Recorded::ok("fetch_backup", &log)).await;

    let broker = Arc::new(InMemoryBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    let handle = start(&broker, registry, config("fallback-fail", 0), &store).await;

    dispatch(
        broker.as_ref(),
        with_fallback(
            "fetch_primary",
            vec![serde_json::json!({"url": "primary"})],
            "fetch_backup",
            vec![serde_json::json!({"url": "backup"})],
        ),
    )
    .await;

    wait_until("the fallback to run", || log.ran("fetch_backup")).await;
    assert_eq!(log.names(), vec!["fetch_primary", "fetch_backup"]);

    // The handler is told what failed and why.
    let input = log.input_of("fetch_backup").expect("fallback input");
    let descriptor = &input["args"][0];
    assert_eq!(descriptor["task"], "fetch_primary");
    assert!(
        descriptor["error"]
            .as_str()
            .is_some_and(|error| error.contains("connection refused")),
        "the handler must be told what went wrong, got {descriptor}"
    );
    assert_eq!(descriptor["failure_type"], "execution_error");
    // Its own declared arguments survive after the descriptor.
    assert_eq!(input["args"][1], serde_json::json!({"url": "backup"}));

    let _ = handle.shutdown().await;
}

#[tokio::test]
async fn fallback_does_not_run_when_the_primary_succeeds() {
    let log = RunLog::default();
    let registry = TaskRegistry::new();
    registry.register(Recorded::ok("fetch_primary", &log)).await;
    registry.register(Recorded::ok("fetch_backup", &log)).await;

    let broker = Arc::new(InMemoryBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    let handle = start(&broker, registry, config("fallback-ok", 0), &store).await;

    dispatch(
        broker.as_ref(),
        with_fallback("fetch_primary", vec![], "fetch_backup", vec![]),
    )
    .await;

    wait_until("the primary to run", || log.ran("fetch_primary")).await;
    settle().await;

    assert_eq!(
        log.names(),
        vec!["fetch_primary"],
        "the fallback must not run on success — chaining it with `.then()` \
         called the backup API on every successful request"
    );

    let _ = handle.shutdown().await;
}

// --------------------------------------------------------------------------
// with_dlq
// --------------------------------------------------------------------------

#[tokio::test]
async fn dlq_handler_runs_only_when_the_task_fails() {
    let log = RunLog::default();
    let registry = TaskRegistry::new();
    registry
        .register(Recorded::failing("process_payment", &log, "card declined"))
        .await;
    registry
        .register(Recorded::ok("handle_failed_payment", &log))
        .await;

    let broker = Arc::new(InMemoryBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    let handle = start(&broker, registry, config("dlq-fail", 0), &store).await;

    dispatch(
        broker.as_ref(),
        with_dlq(
            "process_payment",
            vec![serde_json::json!({"amount": 100})],
            "handle_failed_payment",
        ),
    )
    .await;

    wait_until("the DLQ handler to run", || {
        log.ran("handle_failed_payment")
    })
    .await;
    let descriptor = log
        .input_of("handle_failed_payment")
        .expect("dlq handler input");
    assert!(
        descriptor["args"][0]["error"]
            .as_str()
            .is_some_and(|error| error.contains("card declined")),
        "the DLQ handler must be told what went wrong, got {descriptor}"
    );

    let _ = handle.shutdown().await;
}

#[tokio::test]
async fn dlq_handler_does_not_run_when_the_task_succeeds() {
    let log = RunLog::default();
    let registry = TaskRegistry::new();
    registry
        .register(Recorded::ok("process_payment", &log))
        .await;
    registry
        .register(Recorded::ok("handle_failed_payment", &log))
        .await;

    let broker = Arc::new(InMemoryBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    let handle = start(&broker, registry, config("dlq-ok", 0), &store).await;

    dispatch(
        broker.as_ref(),
        with_dlq("process_payment", vec![], "handle_failed_payment"),
    )
    .await;

    wait_until("the payment task to run", || log.ran("process_payment")).await;
    settle().await;

    assert_eq!(
        log.count("handle_failed_payment"),
        0,
        "the dead-letter handler fired on a successful payment"
    );

    let _ = handle.shutdown().await;
}

// --------------------------------------------------------------------------
// create_saga_workflow
// --------------------------------------------------------------------------

/// The saga's three forward steps and their compensations, with `failing_step`
/// (if it names one of them) rigged to fail.
async fn build_saga_registry(log: &RunLog, failing_step: &'static str) -> TaskRegistry {
    let registry = TaskRegistry::new();
    for step in ["reserve_inventory", "charge_payment", "ship_order"] {
        if step == failing_step {
            registry
                .register(Recorded::failing(step, log, "step failed"))
                .await;
        } else {
            registry.register(Recorded::ok(step, log)).await;
        }
    }
    for compensation in ["release_inventory", "refund_payment", "cancel_shipment"] {
        registry.register(Recorded::ok(compensation, log)).await;
    }
    registry
}

fn saga_steps() -> Vec<(
    &'static str,
    Vec<serde_json::Value>,
    &'static str,
    Vec<serde_json::Value>,
)> {
    vec![
        (
            "reserve_inventory",
            vec![serde_json::json!(1)],
            "release_inventory",
            vec![serde_json::json!(1)],
        ),
        (
            "charge_payment",
            vec![serde_json::json!(2)],
            "refund_payment",
            vec![serde_json::json!(2)],
        ),
        (
            "ship_order",
            vec![serde_json::json!(3)],
            "cancel_shipment",
            vec![serde_json::json!(3)],
        ),
    ]
}

#[tokio::test]
async fn saga_rolls_completed_steps_back_in_reverse_order() {
    let log = RunLog::default();
    let registry = build_saga_registry(&log, "ship_order").await;

    let broker = Arc::new(InMemoryBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    let handle = start(&broker, registry, config("saga-fail", 0), &store).await;

    create_saga_workflow(saga_steps())
        .apply(broker.as_ref())
        .await
        .expect("dispatch saga");

    wait_until("the rollback to reach the first step", || {
        log.ran("release_inventory")
    })
    .await;
    settle().await;

    assert_eq!(
        log.names(),
        vec![
            "reserve_inventory",
            "charge_payment",
            "ship_order",
            // Reverse order, and only for the steps that completed: the
            // shipment was never made, so it is not cancelled.
            "refund_payment",
            "release_inventory",
        ],
        "a saga must undo its completed steps in reverse"
    );

    let _ = handle.shutdown().await;
}

#[tokio::test]
async fn saga_runs_no_compensation_when_every_step_succeeds() {
    let log = RunLog::default();
    let registry = build_saga_registry(&log, "nothing_fails").await;

    let broker = Arc::new(InMemoryBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    let handle = start(&broker, registry, config("saga-ok", 0), &store).await;

    create_saga_workflow(saga_steps())
        .apply(broker.as_ref())
        .await
        .expect("dispatch saga");

    wait_until("the saga to finish", || log.ran("ship_order")).await;
    settle().await;

    assert_eq!(
        log.names(),
        vec!["reserve_inventory", "charge_payment", "ship_order"],
        "a successful saga compensates nothing"
    );

    let _ = handle.shutdown().await;
}

#[tokio::test]
async fn saga_failing_at_the_first_step_compensates_nothing() {
    let log = RunLog::default();
    let registry = build_saga_registry(&log, "reserve_inventory").await;

    let broker = Arc::new(InMemoryBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    let handle = start(&broker, registry, config("saga-first", 0), &store).await;

    create_saga_workflow(saga_steps())
        .apply(broker.as_ref())
        .await
        .expect("dispatch saga");

    wait_until("the first step to fail", || log.ran("reserve_inventory")).await;
    settle().await;

    assert_eq!(
        log.names(),
        vec!["reserve_inventory"],
        "nothing had completed, so there is nothing to undo — and the rest of \
         the saga must not run either"
    );

    let _ = handle.shutdown().await;
}

// --------------------------------------------------------------------------
// ignore_errors
// --------------------------------------------------------------------------

#[tokio::test]
async fn ignored_failure_lets_the_chain_continue() {
    let log = RunLog::default();
    let registry = TaskRegistry::new();
    registry.register(Recorded::ok("first", &log)).await;
    registry
        .register(Recorded::failing("log_analytics", &log, "sink unreachable"))
        .await;
    registry.register(Recorded::ok("last", &log)).await;

    let broker = Arc::new(InMemoryBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    let handle = start(&broker, registry, config("ignore-on", 0), &store).await;

    let analytics = ignore_errors("log_analytics", vec![]);
    let analytics_id = uuid::Uuid::new_v4();
    let analytics = analytics.with_task_id(analytics_id);

    Chain::new()
        .then("first", vec![])
        .then_signature(analytics)
        .then("last", vec![])
        .apply(broker.as_ref())
        .await
        .expect("dispatch chain");

    wait_until("the step after the ignored failure to run", || {
        log.ran("last")
    })
    .await;
    assert_eq!(log.names(), vec!["first", "log_analytics", "last"]);

    // The non-critical step's failure is recorded as ignored — visible, but not
    // a failure — and the successor was handed a `null` in its place.
    let recorded = wait_for_result(&store, analytics_id).await;
    match recorded {
        TaskResultValue::Ignored { ref error } => {
            assert!(
                error.contains("sink unreachable"),
                "the suppressed error must stay observable, got {error:?}"
            );
        }
        other => panic!("expected an ignored result, got {other:?}"),
    }

    let last_input = log.input_of("last").expect("last input");
    assert_eq!(last_input["args"][0], serde_json::Value::Null);

    let _ = handle.shutdown().await;
}

/// The other side of the suppression boundary: `ignore_errors` covers failures
/// of an *attempt at the task*, never the worker's refusal to run it at all.
///
/// A poison-pill quarantine is such a refusal — it protects every worker from a
/// known-bad message — so a producer flag must not be able to wave it away.
/// Were it suppressible, a task could opt itself back into being retried
/// forever by the very mechanism that exists to stop it, and the caller would
/// see `Ignored` for a message that never ran.
#[tokio::test]
async fn a_quarantined_task_is_not_suppressible() {
    use celers_worker::{PoisonPillConfig, PoisonPillDetector};

    let log = RunLog::default();
    let registry = TaskRegistry::new();
    registry
        .register(Recorded::failing("wedged", &log, "always fails"))
        .await;

    // Quarantine the id before it is ever dispatched, so admission — not
    // execution — is what rejects it.
    let detector = Arc::new(PoisonPillDetector::new(
        PoisonPillConfig::new().with_threshold(1),
    ));
    let wedged_id = uuid::Uuid::new_v4();
    detector.record_failure(wedged_id, "always fails").await;
    assert!(detector.is_poison(&wedged_id).await);

    let broker = Arc::new(InMemoryBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    let handle = Worker::new_from_arc(Arc::clone(&broker), registry, config("quarantine", 0))
        .with_result_store(Arc::clone(&store) as Arc<dyn ResultStore>)
        .with_poison_pill(Arc::clone(&detector))
        .run_with_shutdown()
        .await
        .expect("worker starts");

    // The task asks for suppression — and does not get it.
    dispatch(
        broker.as_ref(),
        ignore_errors("wedged", vec![]).with_task_id(wedged_id),
    )
    .await;

    let recorded = wait_for_result(&store, wedged_id).await;
    assert!(
        recorded.is_failed(),
        "a quarantined message must record a real failure, got {recorded:?}"
    );
    assert!(
        !recorded.is_ignored(),
        "`ignore_errors` must not suppress a refusal to run the task at all"
    );
    assert_eq!(
        log.names(),
        Vec::<String>::new(),
        "the quarantined task must never have executed"
    );

    let _ = handle.shutdown().await;
}

/// Poll `store` until `task_id` has a terminal result, or fail the test.
async fn wait_for_result(store: &Arc<InMemoryResultBackend>, task_id: TaskId) -> TaskResultValue {
    let deadline = Instant::now() + WAIT_DEADLINE;
    while Instant::now() < deadline {
        if let Some(value) = store.get_result(task_id).await.expect("result lookup") {
            if value.is_terminal() {
                return value;
            }
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    panic!("timed out waiting for the result of task {task_id}");
}

#[tokio::test]
async fn an_unsuppressed_failure_still_stops_the_chain() {
    // The control for the test above: without `ignore_errors`, the very same
    // chain stops at the failing step.
    let log = RunLog::default();
    let registry = TaskRegistry::new();
    registry.register(Recorded::ok("first", &log)).await;
    registry
        .register(Recorded::failing("log_analytics", &log, "sink unreachable"))
        .await;
    registry.register(Recorded::ok("last", &log)).await;

    let broker = Arc::new(InMemoryBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    let handle = start(&broker, registry, config("ignore-off", 0), &store).await;

    Chain::new()
        .then("first", vec![])
        .then("log_analytics", vec![])
        .then("last", vec![])
        .apply(broker.as_ref())
        .await
        .expect("dispatch chain");

    wait_until("the failing step to run", || log.ran("log_analytics")).await;
    settle().await;

    assert_eq!(log.names(), vec!["first", "log_analytics"]);

    let _ = handle.shutdown().await;
}

/// Suppression covers every failure of an *attempt at the task*, not only a
/// handler returning `Err`. An oversized result is one of those: the value is
/// discarded either way, so a best-effort step must not strand the chain
/// behind it just because what it produced was too big to carry.
#[tokio::test]
async fn an_oversized_result_is_suppressed_when_errors_are_ignored() {
    let log = RunLog::default();
    let registry = TaskRegistry::new();
    registry.register(Recorded::ok("first", &log)).await;
    registry
        .register(Recorded::bulky("dump_debug_state", &log, 4_096))
        .await;
    registry.register(Recorded::ok("last", &log)).await;

    let broker = Arc::new(InMemoryBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    let mut worker_config = config("oversized-ignored", 0);
    worker_config.max_result_size_bytes = 256;
    let handle = start(&broker, registry, worker_config, &store).await;

    let dump_id = uuid::Uuid::new_v4();
    let dump = ignore_errors("dump_debug_state", vec![]).with_task_id(dump_id);

    Chain::new()
        .then("first", vec![])
        .then_signature(dump)
        .then("last", vec![])
        .apply(broker.as_ref())
        .await
        .expect("dispatch chain");

    wait_until("the step after the oversized result to run", || {
        log.ran("last")
    })
    .await;
    assert_eq!(log.names(), vec!["first", "dump_debug_state", "last"]);

    let recorded = wait_for_result(&store, dump_id).await;
    match recorded {
        TaskResultValue::Ignored { ref error } => {
            assert!(
                error.contains("size") || error.contains("large"),
                "the suppressed size error must stay observable, got {error:?}"
            );
        }
        other => panic!("expected an ignored result, got {other:?}"),
    }

    let last_input = log.input_of("last").expect("last input");
    assert_eq!(last_input["args"][0], serde_json::Value::Null);

    let _ = handle.shutdown().await;
}

/// The control for the test above: without `ignore_errors`, an oversized
/// result is still terminal and still ends the chain.
#[tokio::test]
async fn an_oversized_result_without_suppression_still_stops_the_chain() {
    let log = RunLog::default();
    let registry = TaskRegistry::new();
    registry.register(Recorded::ok("first", &log)).await;
    registry
        .register(Recorded::bulky("dump_debug_state", &log, 4_096))
        .await;
    registry.register(Recorded::ok("last", &log)).await;

    let broker = Arc::new(InMemoryBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    let mut worker_config = config("oversized-terminal", 0);
    worker_config.max_result_size_bytes = 256;
    let handle = start(&broker, registry, worker_config, &store).await;

    let dump_id = uuid::Uuid::new_v4();
    let dump = Signature::new("dump_debug_state".to_string()).with_task_id(dump_id);

    Chain::new()
        .then("first", vec![])
        .then_signature(dump)
        .then("last", vec![])
        .apply(broker.as_ref())
        .await
        .expect("dispatch chain");

    wait_until("the oversized step to run", || log.ran("dump_debug_state")).await;
    settle().await;

    assert_eq!(log.names(), vec!["first", "dump_debug_state"]);

    // And the caller is told, rather than left polling: a terminal failure is
    // recorded even though nothing the task produced could be stored.
    let recorded = wait_for_result(&store, dump_id).await;
    assert!(
        recorded.is_failed(),
        "expected a terminal failure, got {recorded:?}"
    );

    let _ = handle.shutdown().await;
}

// --------------------------------------------------------------------------
// with_exponential_backoff
// --------------------------------------------------------------------------

#[tokio::test]
async fn exponential_backoff_produces_a_growing_retry_schedule() {
    let log = RunLog::default();
    let registry = TaskRegistry::new();
    let flaky = Recorded::failing("call_flaky_api", &log, "503");
    let attempts = flaky.attempts();
    registry.register(flaky).await;

    let broker = Arc::new(DelayRecordingBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    // The worker's own cap must be at least as large as the task's request, or
    // `effective_max_retries` clamps it.
    let handle = start(&broker, registry, config("backoff", 4), &store).await;

    dispatch(
        broker.as_ref(),
        with_exponential_backoff("call_flaky_api", vec![], 4, 2),
    )
    .await;

    // One initial attempt plus four retries.
    wait_until("the retry budget to be spent", || {
        attempts.load(Ordering::SeqCst) == 5
    })
    .await;
    settle().await;

    let delays: Vec<u64> = broker
        .delays()
        .into_iter()
        .filter(|(name, _)| name == "call_flaky_api")
        .map(|(_, secs)| secs)
        .collect();

    assert_eq!(
        delays,
        vec![2, 4, 8, 16],
        "each retry must wait twice as long as the one before it; a single \
         fixed countdown is not a backoff"
    );

    let _ = handle.shutdown().await;
}

#[tokio::test]
async fn a_task_without_a_backoff_policy_keeps_the_worker_schedule() {
    // The negative control: the per-task policy must only apply where one was
    // declared, leaving the worker's configured strategy in charge otherwise.
    let log = RunLog::default();
    let registry = TaskRegistry::new();
    let flaky = Recorded::failing("plain_flaky", &log, "503");
    let attempts = flaky.attempts();
    registry.register(flaky).await;

    let broker = Arc::new(DelayRecordingBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    let mut worker_config = config("no-backoff", 2);
    // A worker strategy that is obviously not `2, 4, 8, …`.
    worker_config.retry_base_delay_ms = 1_000;
    worker_config.retry_max_delay_ms = 1_000;
    let handle = start(&broker, registry, worker_config, &store).await;

    dispatch(
        broker.as_ref(),
        Signature::new("plain_flaky".to_string()).with_retries(2),
    )
    .await;

    wait_until("the retry budget to be spent", || {
        attempts.load(Ordering::SeqCst) == 3
    })
    .await;

    let delays: Vec<u64> = broker
        .delays()
        .into_iter()
        .filter(|(name, _)| name == "plain_flaky")
        .map(|(_, secs)| secs)
        .collect();
    assert!(
        delays.iter().all(|secs| *secs <= 1),
        "the worker's own 1s strategy must still apply, got {delays:?}"
    );

    let _ = handle.shutdown().await;
}

// --------------------------------------------------------------------------
// create_parallel_chains
// --------------------------------------------------------------------------

#[tokio::test]
async fn parallel_chains_run_every_step_of_every_chain() {
    let log = RunLog::default();
    let registry = TaskRegistry::new();
    for name in ["resize", "optimize", "transcode", "thumbnail"] {
        registry.register(Recorded::ok(name, &log)).await;
    }

    let broker = Arc::new(InMemoryBroker::new());
    let store = Arc::new(InMemoryResultBackend::new());
    let handle = start(&broker, registry, config("parallel", 0), &store).await;

    let workflow = create_parallel_chains(
        vec![
            ("images", vec![("resize", vec![]), ("optimize", vec![])]),
            ("videos", vec![("transcode", vec![]), ("thumbnail", vec![])]),
        ],
        None,
    );
    workflow.apply(broker.as_ref()).await.expect("dispatch");

    wait_until("every chain to finish", || {
        log.ran("optimize") && log.ran("thumbnail")
    })
    .await;

    let names = log.names();
    assert_eq!(
        names.len(),
        4,
        "every step of every chain must run, got {names:?}"
    );
    // Order within a chain is guaranteed; order across chains is not.
    let position = |needle: &str| {
        names
            .iter()
            .position(|seen| seen == needle)
            .unwrap_or_else(|| panic!("{needle} never ran"))
    };
    assert!(position("resize") < position("optimize"));
    assert!(position("transcode") < position("thumbnail"));

    let _ = handle.shutdown().await;
}

#[tokio::test]
async fn an_aggregate_without_a_backend_is_refused_rather_than_dropped() {
    // A chord needs a barrier. Silently dispatching the chains and forgetting
    // the aggregate — which is what the old helper did — reports success for a
    // workflow that will never aggregate anything.
    let broker = InMemoryBroker::new();
    let workflow =
        create_parallel_chains(vec![("images", vec![("resize", vec![])])], Some("finalize"));

    let error = workflow
        .apply(&broker)
        .await
        .expect_err("an aggregate cannot be honoured without a result backend");
    assert!(
        error.to_string().contains("apply_with_backend"),
        "the error must point at the way to fix it, got {error}"
    );
}

// --------------------------------------------------------------------------
// create_parallel_chains with an aggregate (chord over chains)
//
// The barrier itself is exercised end to end in `celers-worker`'s own
// `workflow_semantics` test, which is the only place both the result backend
// and the worker's `workflows` feature are available at once. What is pinned
// here is the half this crate owns: the chord the facade *registers*.
// --------------------------------------------------------------------------

#[cfg(feature = "backend-redis")]
mod aggregate_registration {
    use super::*;

    use celers::{ChordState, ResultBackend, TaskMeta};
    use celers_backend_redis::Result as BackendResult;
    use celers_core::Result as BrokerResult;
    use std::collections::HashMap;
    use uuid::Uuid;

    /// Records everything enqueued, so the dispatched shape can be inspected.
    #[derive(Default)]
    struct CapturingBroker {
        enqueued: Mutex<Vec<SerializedTask>>,
    }

    impl CapturingBroker {
        fn tasks(&self) -> Vec<SerializedTask> {
            self.enqueued
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .clone()
        }
    }

    #[async_trait::async_trait]
    impl Broker for CapturingBroker {
        async fn enqueue(&self, task: SerializedTask) -> BrokerResult<TaskId> {
            let id = task.metadata.id;
            self.enqueued
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner)
                .push(task);
            Ok(id)
        }

        async fn dequeue(&self) -> BrokerResult<Option<BrokerMessage>> {
            Ok(None)
        }

        async fn ack(&self, _task_id: &TaskId, _receipt: Option<&str>) -> BrokerResult<()> {
            Ok(())
        }

        async fn reject(
            &self,
            _task_id: &TaskId,
            _receipt: Option<&str>,
            _requeue: bool,
        ) -> BrokerResult<()> {
            Ok(())
        }

        async fn queue_size(&self) -> BrokerResult<usize> {
            Ok(self.tasks().len())
        }

        async fn cancel(&self, _task_id: &TaskId) -> BrokerResult<bool> {
            Ok(false)
        }
    }

    /// Captures the chord state the facade registers.
    #[derive(Default)]
    struct CapturingBackend {
        state: Option<ChordState>,
        results: HashMap<Uuid, TaskMeta>,
    }

    #[async_trait::async_trait]
    impl ResultBackend for CapturingBackend {
        async fn store_result(&mut self, task_id: Uuid, meta: &TaskMeta) -> BackendResult<()> {
            self.results.insert(task_id, meta.clone());
            Ok(())
        }

        async fn get_result(&mut self, task_id: Uuid) -> BackendResult<Option<TaskMeta>> {
            Ok(self.results.get(&task_id).cloned())
        }

        async fn delete_result(&mut self, task_id: Uuid) -> BackendResult<()> {
            self.results.remove(&task_id);
            Ok(())
        }

        async fn set_expiration(&mut self, _task_id: Uuid, _ttl: Duration) -> BackendResult<()> {
            Ok(())
        }

        async fn chord_init(&mut self, state: ChordState) -> BackendResult<()> {
            self.state = Some(state);
            Ok(())
        }

        async fn chord_complete_task(&mut self, _chord_id: Uuid) -> BackendResult<usize> {
            Ok(0)
        }

        async fn chord_get_state(&mut self, _chord_id: Uuid) -> BackendResult<Option<ChordState>> {
            Ok(self.state.clone())
        }
    }

    #[tokio::test]
    async fn the_barrier_counts_chains_not_tasks() {
        let broker = CapturingBroker::default();
        let mut backend = CapturingBackend::default();

        let workflow = create_parallel_chains(
            vec![
                ("images", vec![("resize", vec![]), ("optimize", vec![])]),
                ("videos", vec![("transcode", vec![]), ("thumbnail", vec![])]),
            ],
            Some("finalize"),
        );

        let chord_id = workflow
            .apply_with_backend(&broker, &mut backend)
            .await
            .expect("dispatch with a barrier");

        let state = backend.state.clone().expect("a barrier must be registered");
        assert_eq!(state.chord_id, chord_id);
        assert_eq!(
            state.total, 2,
            "the barrier counts chains, not the four tasks in them"
        );
        assert_eq!(state.callback.as_deref(), Some("finalize"));

        // Only the chain heads are enqueued; each carries its own tail.
        let enqueued = broker.tasks();
        assert_eq!(enqueued.len(), 2);
        let names: Vec<&str> = enqueued
            .iter()
            .map(|task| task.metadata.name.as_str())
            .collect();
        assert_eq!(names, vec!["resize", "transcode"]);

        for task in &enqueued {
            assert!(
                task.metadata.chord_id.is_none(),
                "a chain's head must not count against the barrier — only its \
                 last step has finished the member"
            );
            let payload: serde_json::Value =
                serde_json::from_slice(&task.payload).expect("payload is JSON");
            let tail = payload["chain"]
                .as_array()
                .expect("the head carries a tail");
            assert_eq!(tail.len(), 1);
            // The recorded member ids are exactly the ids the final steps will
            // be dispatched with, and only they carry the chord id.
            let final_step_id = tail[0]["options"]["task_id"]
                .as_str()
                .and_then(|raw| Uuid::parse_str(raw).ok())
                .expect("the final step has a pre-assigned id");
            assert!(
                state.task_ids.contains(&final_step_id),
                "the barrier must be registered against the ids the final steps carry"
            );
            assert_eq!(
                tail[0]["options"]["chord_id"].as_str(),
                Some(chord_id.to_string().as_str())
            );
        }
    }
}
