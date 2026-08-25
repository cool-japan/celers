//! End-to-end execution of `celers-canvas`' advanced patterns.
//!
//! [`Saga`](celers_canvas::Saga), [`Pipeline`](celers_canvas::Pipeline),
//! [`FanIn`](celers_canvas::FanIn), [`FanOut`](celers_canvas::FanOut) and
//! [`ScatterGather`](celers_canvas::ScatterGather) are builders that *lower*
//! onto the executable primitives — a chain, a group, a chord, a rollback route
//! — and `celers-canvas` tests the lowering itself. What only a worker can show
//! is that the lowered graph really runs: that a four-step saga rolls its two
//! completed steps back in reverse when the third fails, that a chord's
//! aggregator fires exactly once with every member's result, that a fan-out's
//! consumers all get their message.
//!
//! Everything here runs against the in-process [`InMemoryBroker`] with this
//! module's own [`handle_workflow_completion`] on the success path and
//! [`crate::error_links::run_error_route`] on the failure path — the same two
//! entry points [`crate::worker_core`] calls after executing a task.

use super::handle_workflow_completion;
use crate::error_links::{run_error_route, TaskFailure};

use async_trait::async_trait;
use celers_backend_redis::{ChordState, Result as BackendResult, ResultBackend, TaskMeta};
use celers_canvas::{
    dispatch, Chord, CompensationWorkflow, FanIn, FanOut, Pipeline, Saga, ScatterGather, Signature,
};
use celers_core::{Broker, InMemoryBroker};
use std::collections::HashMap;
use std::time::Duration;
use uuid::Uuid;

// ============================================================================
// Harness
// ============================================================================

/// What a "worker" observed while running one task.
#[derive(Debug, Clone, PartialEq)]
struct Executed {
    /// Registered task name.
    name: String,
    /// Positional arguments it was handed.
    args: Vec<serde_json::Value>,
}

/// What the simulated worker decided about a task it just ran.
enum Outcome {
    /// The task succeeded, returning these result bytes.
    Success(Vec<u8>),
    /// The task failed for good, with this reason.
    Failure(&'static str),
}

impl Outcome {
    /// A successful JSON result.
    fn json(value: serde_json::Value) -> Self {
        Self::Success(serde_json::to_vec(&value).expect("test results encode"))
    }
}

/// Drain the broker, running every task through `execute` and feeding the
/// outcome back into the workflow machinery exactly as the worker core does:
/// a success goes to [`handle_workflow_completion`] (chain tail, chord
/// barrier), a terminal failure to [`run_error_route`] (the rollback).
///
/// Deterministic: it loops until the queue is empty, with a hard cap so a
/// runaway workflow fails the test instead of hanging it.
async fn run_workflow<F>(
    broker: &InMemoryBroker,
    backend: Option<&mut CountingBackend>,
    mut execute: F,
) -> Vec<Executed>
where
    F: FnMut(&Executed) -> Outcome,
{
    let mut backend = backend;
    let mut observed = Vec::new();

    for _ in 0..64 {
        // `dequeue` blocks until a task arrives; `dequeue_batch` drains what is
        // immediately available and returns straight away, which is what "run
        // until the workflow is finished" needs.
        let Some(message) = broker
            .dequeue_batch(1)
            .await
            .expect("in-memory dequeue never fails")
            .pop()
        else {
            break;
        };

        let task = message.task;
        let envelope: serde_json::Value =
            serde_json::from_slice(&task.payload).expect("canvas payloads are JSON envelopes");

        let step = Executed {
            name: task.metadata.name.clone(),
            args: envelope
                .get("args")
                .and_then(|args| args.as_array())
                .cloned()
                .unwrap_or_default(),
        };

        let outcome = execute(&step);
        observed.push(step);

        broker
            .ack(&task.metadata.id, message.receipt_handle.as_deref())
            .await
            .expect("ack");

        match outcome {
            Outcome::Success(result) => {
                let barrier: Option<&mut (dyn ResultBackend + 'static)> = match backend {
                    Some(ref mut store) => Some(&mut **store),
                    None => None,
                };
                handle_workflow_completion(&task, &result, broker, barrier)
                    .await
                    .expect("workflow continuation must succeed");
            }
            Outcome::Failure(reason) => {
                let failure = TaskFailure {
                    task_id: task.metadata.id,
                    task_name: &task.metadata.name,
                    error: reason,
                    failure_type: "execution_error",
                };
                run_error_route(&task, &failure, broker, None)
                    .await
                    .expect("running the error route must succeed");
            }
        }
    }

    assert_eq!(
        broker.queue_size().await.expect("queue size"),
        0,
        "the workflow must terminate with an empty queue"
    );

    observed
}

/// Result backend whose chord counter really counts, so a barrier can be
/// observed staying shut until the last member completes.
#[derive(Default)]
struct CountingBackend {
    /// Barriers by chord id.
    chords: HashMap<Uuid, ChordState>,
    /// Stored task results.
    results: HashMap<Uuid, TaskMeta>,
    /// Per-chord completion counters.
    completed: HashMap<Uuid, usize>,
}

#[async_trait]
impl ResultBackend for CountingBackend {
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
        self.chords.insert(state.chord_id, state);
        Ok(())
    }

    async fn chord_complete_task(&mut self, chord_id: Uuid) -> BackendResult<usize> {
        let counter = self.completed.entry(chord_id).or_insert(0);
        *counter += 1;
        Ok(*counter)
    }

    async fn chord_get_state(&mut self, chord_id: Uuid) -> BackendResult<Option<ChordState>> {
        Ok(self.chords.get(&chord_id).cloned())
    }
}

/// Register a chord's barrier and dispatch its header.
///
/// This is what [`Chord::apply`] does, spelled out: that method lives behind
/// `celers-canvas`' own `backend-redis` feature, which this crate does not
/// enable (it depends on `celers-backend-redis` directly instead), so the
/// header is built with the very same public dispatch helpers the canvas uses
/// — ids known before the barrier is written, barrier written before anything
/// is enqueued.
async fn register_and_dispatch_chord(
    chord: &Chord,
    broker: &InMemoryBroker,
    backend: &mut CountingBackend,
) -> Uuid {
    let chord_id = Uuid::new_v4();

    let built = dispatch::build_fanout(&chord.header.tasks, Some(chord_id), Some(chord_id))
        .expect("header builds");
    let task_ids: Vec<Uuid> = built.iter().map(|(task, _)| task.metadata.id).collect();

    backend
        .chord_init(
            ChordState::new(chord_id, task_ids.len(), task_ids)
                .with_callback(chord.body.task.clone()),
        )
        .await
        .expect("barrier registers");

    dispatch::dispatch_all(broker, built)
        .await
        .expect("header dispatches");

    chord_id
}

/// Task names, in the order they ran.
fn names(observed: &[Executed]) -> Vec<&str> {
    observed.iter().map(|step| step.name.as_str()).collect()
}

fn sig(name: &str) -> Signature {
    Signature::new(name.to_string())
}

/// A three-step order saga, each step with its undo.
fn order_saga() -> Saga {
    Saga::new(
        CompensationWorkflow::new()
            .step(sig("reserve_inventory"), sig("release_inventory"))
            .step(sig("charge_payment"), sig("refund_payment"))
            .step(sig("ship_order"), sig("cancel_shipment")),
    )
}

// ============================================================================
// Saga
// ============================================================================

/// The happy path: every forward step runs, in order, and no compensation is
/// ever enqueued.
#[tokio::test]
async fn saga_runs_every_forward_step_and_compensates_nothing_on_success() {
    let broker = InMemoryBroker::new();
    order_saga().apply(&broker).await.expect("saga dispatches");

    let observed = run_workflow(&broker, None, |step| {
        Outcome::json(serde_json::json!({"ok": step.name}))
    })
    .await;

    assert_eq!(
        names(&observed),
        vec!["reserve_inventory", "charge_payment", "ship_order"],
        "the forward path is an ordinary chain"
    );
    for step in &observed {
        assert!(
            !step.name.starts_with("release")
                && !step.name.starts_with("refund")
                && !step.name.starts_with("cancel"),
            "no compensation may run when nothing failed, but {} did",
            step.name
        );
    }
}

/// The whole point of a saga: when a late step fails for good, the steps that
/// already succeeded are undone in **reverse** order — and each compensation
/// only after the previous one has finished, which is why this needs more than
/// one hop to prove.
#[tokio::test]
async fn saga_rolls_back_completed_steps_in_reverse_when_a_late_step_fails() {
    let broker = InMemoryBroker::new();
    order_saga().apply(&broker).await.expect("saga dispatches");

    let observed = run_workflow(&broker, None, |step| {
        if step.name == "ship_order" {
            Outcome::Failure("carrier rejected the parcel")
        } else {
            Outcome::json(serde_json::json!({"ok": step.name}))
        }
    })
    .await;

    assert_eq!(
        names(&observed),
        vec![
            "reserve_inventory",
            "charge_payment",
            "ship_order",
            // Newest completed step first: the payment is refunded before the
            // stock it paid for is released.
            "refund_payment",
            "release_inventory",
        ],
        "the rollback must run in reverse declaration order, one hop at a time"
    );

    assert!(
        !names(&observed).contains(&"cancel_shipment"),
        "the step that failed never reported success, so it must not be compensated"
    );

    let refund = &observed[3];
    let descriptor = refund
        .args
        .first()
        .expect("a compensation receives the failure descriptor");
    assert_eq!(
        descriptor.get("task").and_then(|task| task.as_str()),
        Some("ship_order"),
        "the descriptor names the task that failed"
    );
    assert_eq!(
        descriptor.get("error").and_then(|error| error.as_str()),
        Some("carrier rejected the parcel"),
    );
}

/// A failure at the very first step compensates nothing: nothing has happened
/// yet.
#[tokio::test]
async fn saga_first_step_failure_runs_no_compensation() {
    let broker = InMemoryBroker::new();
    order_saga().apply(&broker).await.expect("saga dispatches");

    let observed = run_workflow(&broker, None, |step| {
        if step.name == "reserve_inventory" {
            Outcome::Failure("out of stock")
        } else {
            Outcome::json(serde_json::json!(null))
        }
    })
    .await;

    assert_eq!(
        names(&observed),
        vec!["reserve_inventory"],
        "a saga that fails on its first step runs exactly that step"
    );
}

/// A compensation that fails truncates the rollback — the documented
/// consequence of running compensations as ordinary tasks.
#[tokio::test]
async fn saga_rollback_stops_at_a_compensation_that_fails() {
    let broker = InMemoryBroker::new();
    order_saga().apply(&broker).await.expect("saga dispatches");

    let observed = run_workflow(&broker, None, |step| match step.name.as_str() {
        "ship_order" => Outcome::Failure("carrier rejected the parcel"),
        "refund_payment" => Outcome::Failure("payment gateway unreachable"),
        _ => Outcome::json(serde_json::json!({"ok": step.name})),
    })
    .await;

    assert_eq!(
        names(&observed),
        vec![
            "reserve_inventory",
            "charge_payment",
            "ship_order",
            "refund_payment",
        ],
        "the compensations behind a failed one do not run: the rollback is itself a chain"
    );
}

// ============================================================================
// Pipeline
// ============================================================================

/// Stages run in order, each receiving its predecessor's output.
#[tokio::test]
async fn pipeline_stages_run_in_order_each_fed_by_the_previous_one() {
    let broker = InMemoryBroker::new();

    Pipeline::new()
        .stage(sig("extract").with_args(vec![serde_json::json!("orders.csv")]))
        .stage(sig("transform"))
        .stage(sig("load"))
        .with_buffer_size(8)
        .apply(&broker)
        .await
        .expect("pipeline dispatches");

    let observed = run_workflow(&broker, None, |step| {
        Outcome::json(serde_json::json!(format!("{}-done", step.name)))
    })
    .await;

    assert_eq!(names(&observed), vec!["extract", "transform", "load"]);
    assert_eq!(
        observed[0].args,
        vec![serde_json::json!("orders.csv")],
        "the first stage keeps its own arguments"
    );
    assert_eq!(
        observed[1].args,
        vec![serde_json::json!("extract-done")],
        "each stage receives the previous stage's output"
    );
    assert_eq!(observed[2].args, vec![serde_json::json!("transform-done")]);
}

/// A stage that fails for good ends the pipeline: the stages behind it never
/// run, because a chain step is only enqueued by the *success* path of the one
/// before it.
#[tokio::test]
async fn pipeline_stops_at_a_stage_that_fails() {
    let broker = InMemoryBroker::new();

    Pipeline::new()
        .stage(sig("extract"))
        .stage(sig("transform"))
        .stage(sig("load"))
        .apply(&broker)
        .await
        .expect("pipeline dispatches");

    let observed = run_workflow(&broker, None, |step| {
        if step.name == "transform" {
            Outcome::Failure("malformed row")
        } else {
            Outcome::json(serde_json::json!(format!("{}-done", step.name)))
        }
    })
    .await;

    assert_eq!(
        names(&observed),
        vec!["extract", "transform"],
        "the stages after a failed one must not run"
    );
}

// ============================================================================
// FanOut
// ============================================================================

/// Every consumer runs, none of them chains onward, and the source is the
/// caller's to dispatch.
#[tokio::test]
async fn fan_out_delivers_to_every_consumer_and_stops_there() {
    let broker = InMemoryBroker::new();

    let fan_out = FanOut::new(sig("publish_event"))
        .consumer(sig("update_search_index").with_args(vec![serde_json::json!({"id": 7})]))
        .consumer(sig("send_webhook").with_args(vec![serde_json::json!({"id": 7})]))
        .consumer(sig("invalidate_cache").with_args(vec![serde_json::json!({"id": 7})]));

    fan_out
        .apply_after_source(&broker)
        .await
        .expect("consumers dispatch");

    let observed = run_workflow(&broker, None, |_| Outcome::json(serde_json::json!("ack"))).await;

    let mut ran = names(&observed);
    ran.sort_unstable();
    assert_eq!(
        ran,
        vec!["invalidate_cache", "send_webhook", "update_search_index"],
        "every consumer receives the broadcast"
    );
    for step in &observed {
        assert_eq!(
            step.args,
            vec![serde_json::json!({"id": 7})],
            "each consumer keeps the arguments the source handed it"
        );
    }
}

/// One consumer failing must not take the others with it: a fan-out's members
/// are independent, so the group has no shared fate.
#[tokio::test]
async fn fan_out_consumers_are_independent_when_one_fails() {
    let broker = InMemoryBroker::new();

    FanOut::new(sig("publish_event"))
        .consumer(sig("update_search_index"))
        .consumer(sig("send_webhook"))
        .consumer(sig("invalidate_cache"))
        .apply_after_source(&broker)
        .await
        .expect("consumers dispatch");

    let observed = run_workflow(&broker, None, |step| {
        if step.name == "send_webhook" {
            Outcome::Failure("endpoint returned 500")
        } else {
            Outcome::json(serde_json::json!("ack"))
        }
    })
    .await;

    let mut ran = names(&observed);
    ran.sort_unstable();
    assert_eq!(
        ran,
        vec!["invalidate_cache", "send_webhook", "update_search_index"],
        "the surviving consumers still run: a failed member has no successor to cancel"
    );
}

// ============================================================================
// FanIn
// ============================================================================

/// The aggregator runs exactly once, after every source, with their results in
/// declaration order.
#[tokio::test]
async fn fan_in_aggregator_runs_once_with_every_sources_result() {
    let broker = InMemoryBroker::new();
    let mut backend = CountingBackend::default();

    let fan_in = FanIn::new(sig("merge_reports"))
        .source(sig("fetch_sales"))
        .source(sig("fetch_returns"))
        .source(sig("fetch_refunds"));

    register_and_dispatch_chord(&fan_in.to_chord(), &broker, &mut backend).await;

    let observed = run_workflow(&broker, Some(&mut backend), |step| {
        match step.name.as_str() {
            "fetch_sales" => Outcome::json(serde_json::json!(100)),
            "fetch_returns" => Outcome::json(serde_json::json!(20)),
            "fetch_refunds" => Outcome::json(serde_json::json!(3)),
            _ => Outcome::json(serde_json::json!("aggregated")),
        }
    })
    .await;

    assert_eq!(
        names(&observed),
        vec![
            "fetch_sales",
            "fetch_returns",
            "fetch_refunds",
            "merge_reports",
        ],
        "the aggregator runs once, and only after the last source"
    );

    assert_eq!(
        observed[3].args,
        vec![serde_json::json!([100, 20, 3])],
        "the aggregator receives the sources' results as one positional argument, in order"
    );
}

/// The barrier stays shut while sources are outstanding: with one of three
/// sources finished, nothing else may be enqueued.
#[tokio::test]
async fn fan_in_aggregator_stays_shut_until_the_last_source_finishes() {
    let broker = InMemoryBroker::new();
    let mut backend = CountingBackend::default();

    let fan_in = FanIn::new(sig("merge_reports"))
        .source(sig("fetch_sales"))
        .source(sig("fetch_returns"))
        .source(sig("fetch_refunds"));

    register_and_dispatch_chord(&fan_in.to_chord(), &broker, &mut backend).await;

    // Run exactly one source, by hand, and stop.
    let message = broker
        .dequeue_batch(1)
        .await
        .expect("dequeue")
        .pop()
        .expect("a source is queued");
    let task = message.task;
    broker
        .ack(&task.metadata.id, message.receipt_handle.as_deref())
        .await
        .expect("ack");
    handle_workflow_completion(&task, b"1", &broker, Some(&mut backend))
        .await
        .expect("completion handling");

    assert_eq!(
        broker.queue_size().await.expect("queue size"),
        2,
        "only the two untouched sources remain: no aggregator may be enqueued yet"
    );
}

// ============================================================================
// ScatterGather
// ============================================================================

/// The gather task runs once the workers have all finished, with their results.
#[tokio::test]
async fn scatter_gather_runs_the_gather_task_over_every_worker_result() {
    let broker = InMemoryBroker::new();
    let mut backend = CountingBackend::default();

    let pattern = ScatterGather::new(
        sig("split_batch"),
        vec![
            sig("process_shard").with_args(vec![serde_json::json!(0)]),
            sig("process_shard").with_args(vec![serde_json::json!(1)]),
            sig("process_shard").with_args(vec![serde_json::json!(2)]),
        ],
        sig("merge_shards"),
    )
    .with_timeout(30);

    // The scatter step has already run: it is what produced the shard indices
    // above. See `ScatterGather::apply_after_scatter`.
    register_and_dispatch_chord(&pattern.to_chord(), &broker, &mut backend).await;

    let observed = run_workflow(&broker, Some(&mut backend), |step| {
        if step.name == "process_shard" {
            let shard = step
                .args
                .first()
                .cloned()
                .unwrap_or(serde_json::json!(null));
            Outcome::json(serde_json::json!({"shard": shard}))
        } else {
            Outcome::json(serde_json::json!("merged"))
        }
    })
    .await;

    assert_eq!(
        names(&observed),
        vec![
            "process_shard",
            "process_shard",
            "process_shard",
            "merge_shards",
        ],
        "the gather task runs exactly once, after every worker"
    );
    assert!(
        !names(&observed).contains(&"split_batch"),
        "the scatter step is dispatched by the caller, not by the barrier"
    );
    assert_eq!(
        observed[3].args,
        vec![serde_json::json!([
            {"shard": 0},
            {"shard": 1},
            {"shard": 2},
        ])],
        "the gather task receives every worker's result, in worker order"
    );
}

/// A worker that fails is aggregated as `null` rather than stranding the gather
/// step: the barrier counts completions, and the callback substitutes a null
/// for a result it cannot read.
#[tokio::test]
async fn scatter_gather_gathers_a_null_for_a_worker_that_produced_no_result() {
    let broker = InMemoryBroker::new();
    let mut backend = CountingBackend::default();

    let pattern = ScatterGather::new(
        sig("split_batch"),
        vec![
            sig("process_shard").with_args(vec![serde_json::json!(0)]),
            sig("process_shard").with_args(vec![serde_json::json!(1)]),
        ],
        sig("merge_shards"),
    );

    let chord_id = register_and_dispatch_chord(&pattern.to_chord(), &broker, &mut backend).await;

    // Drain both workers; the first one's result is never stored (as if the
    // worker died between finishing and writing it back), the second's is.
    let mut members = Vec::new();
    while let Some(message) = broker.dequeue_batch(1).await.expect("dequeue").pop() {
        members.push(message.task);
    }
    assert_eq!(members.len(), 2);

    // First member: count it, but store nothing.
    backend
        .chord_complete_task(chord_id)
        .await
        .expect("counter increments");

    // Second member: the ordinary completion path, which trips the barrier.
    let last = members.pop().expect("second member");
    handle_workflow_completion(&last, b"{\"shard\":1}", &broker, Some(&mut backend))
        .await
        .expect("completion handling");

    let observed = run_workflow(&broker, Some(&mut backend), |_| {
        Outcome::json(serde_json::json!("merged"))
    })
    .await;

    assert_eq!(names(&observed), vec!["merge_shards"]);
    assert_eq!(
        observed[0].args,
        vec![serde_json::json!([serde_json::Value::Null, {"shard": 1}])],
        "a missing member result becomes a null instead of aborting the gather"
    );
}
