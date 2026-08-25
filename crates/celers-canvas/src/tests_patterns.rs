//! Tests for the advanced patterns' lowering onto the executable primitives.
//!
//! [`Saga`], [`Pipeline`], [`ScatterGather`], [`FanOut`] and [`FanIn`] are
//! builders: none of them is a runtime primitive. What makes them more than
//! decoration is that each compiles into something the worker already runs — a
//! [`Chain`], a [`Group`] or a [`Chord`] — so these tests assert on the lowered
//! graph *and* on the broker traffic it produces, including the two cases the
//! runtime cannot express and which therefore have to fail loudly.
//!
//! End-to-end execution of the lowered forms (a worker actually running the
//! steps, and a saga's rollback firing in reverse) lives in
//! `celers-worker/src/workflows/patterns_e2e.rs`, which has a worker to drive.

use crate::{
    CanvasError, CompensationWorkflow, FanIn, FanOut, Pipeline, Saga, SagaIsolation, ScatterGather,
    Signature,
};
use celers_core::{Broker, SerializedTask};
use std::sync::{Arc, Mutex};

/// Broker that records every enqueued task, in order.
#[derive(Clone, Default)]
struct RecordingBroker {
    tasks: Arc<Mutex<Vec<SerializedTask>>>,
}

impl RecordingBroker {
    fn tasks(&self) -> Vec<SerializedTask> {
        self.tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone()
    }

    fn names(&self) -> Vec<String> {
        self.tasks()
            .into_iter()
            .map(|task| task.metadata.name)
            .collect()
    }
}

#[async_trait::async_trait]
impl Broker for RecordingBroker {
    async fn enqueue(&self, task: SerializedTask) -> celers_core::Result<celers_core::TaskId> {
        let id = task.metadata.id;
        self.tasks
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .push(task);
        Ok(id)
    }

    async fn dequeue(&self) -> celers_core::Result<Option<celers_core::BrokerMessage>> {
        Ok(None)
    }

    async fn ack(
        &self,
        _task_id: &celers_core::TaskId,
        _receipt_handle: Option<&str>,
    ) -> celers_core::Result<()> {
        Ok(())
    }

    async fn reject(
        &self,
        _task_id: &celers_core::TaskId,
        _receipt_handle: Option<&str>,
        _requeue: bool,
    ) -> celers_core::Result<()> {
        Ok(())
    }

    async fn queue_size(&self) -> celers_core::Result<usize> {
        Ok(self.tasks().len())
    }

    async fn cancel(&self, _task_id: &celers_core::TaskId) -> celers_core::Result<bool> {
        Ok(false)
    }
}

/// The payload of a canvas-dispatched task, as JSON.
fn payload(task: &SerializedTask) -> serde_json::Value {
    serde_json::from_slice(&task.payload).expect("canvas payloads are JSON envelopes")
}

/// The task names of a `chain`/`errback` array in a payload envelope.
fn step_names(envelope: &serde_json::Value, key: &str) -> Vec<String> {
    envelope
        .get(key)
        .and_then(|steps| steps.as_array())
        .map(|steps| {
            steps
                .iter()
                .filter_map(|step| step.get("task").and_then(|task| task.as_str()))
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default()
}

fn sig(name: &str) -> Signature {
    Signature::new(name.to_string())
}

/// A three-step saga: reserve, charge, ship — each with its undo.
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

/// The lowered chain keeps the forward order and gives step *k* the
/// compensations of steps *k-1 … 0*, newest first.
#[test]
fn saga_lowers_to_a_chain_carrying_the_reverse_rollback_per_step() {
    let chain = order_saga().to_chain();

    let forward: Vec<&str> = chain.iter().map(|sig| sig.task.as_str()).collect();
    assert_eq!(
        forward,
        vec!["reserve_inventory", "charge_payment", "ship_order"],
        "the forward steps keep their declaration order"
    );

    let rollback_of = |index: usize| -> Vec<String> {
        chain.tasks[index]
            .options
            .all_link_errors()
            .iter()
            .map(|sig| sig.task.clone())
            .collect()
    };

    assert!(
        rollback_of(0).is_empty(),
        "nothing has happened yet when the first step fails"
    );
    assert_eq!(
        rollback_of(1),
        vec!["release_inventory".to_string()],
        "a failed payment releases the stock that was already reserved"
    );
    assert_eq!(
        rollback_of(2),
        vec![
            "refund_payment".to_string(),
            "release_inventory".to_string()
        ],
        "a failed shipment refunds first, then releases — reverse declaration order"
    );
}

/// A step that failed is not compensated by its own compensation: it never
/// reported success.
#[test]
fn saga_never_schedules_the_failing_steps_own_compensation() {
    let chain = order_saga().to_chain();

    for (index, step) in chain.iter().enumerate() {
        let route: Vec<&str> = step
            .options
            .all_link_errors()
            .iter()
            .map(|sig| sig.task.as_str())
            .collect();
        let own_compensation = ["release_inventory", "refund_payment", "cancel_shipment"][index];
        assert!(
            !route.contains(&own_compensation),
            "step {} must not schedule its own compensation, got {:?}",
            index,
            route
        );
    }
}

/// A failure route the caller declared on a forward step survives the lowering
/// and runs *after* the rollback, whichever field it was declared in.
#[test]
fn saga_keeps_a_steps_own_failure_route_after_the_rollback() {
    let workflow = CompensationWorkflow::new()
        .step(sig("reserve_inventory"), sig("release_inventory"))
        .step(
            sig("charge_payment")
                .with_link_error(sig("page_oncall"))
                .add_link_error(sig("record_incident")),
            sig("refund_payment"),
        );

    let chain = Saga::new(workflow).to_chain();
    let route: Vec<&str> = chain.tasks[1]
        .options
        .all_link_errors()
        .iter()
        .map(|sig| sig.task.as_str())
        .collect();

    assert_eq!(
        route,
        vec!["release_inventory", "page_oncall", "record_incident"],
        "the rollback runs first; the caller's own handlers keep their relative order behind it"
    );
    assert!(
        chain.tasks[1].options.link_error.is_none(),
        "the single-handler field is folded into `link_errors` so the rollback keeps its position"
    );
}

/// The two vectors are public and can be built out of step: a forward step with
/// no compensation contributes nothing to the rollback instead of shifting it.
#[test]
fn saga_lowering_tolerates_vectors_that_are_out_of_step() {
    let rollback_of = |chain: &crate::Chain, index: usize| -> Vec<String> {
        chain.tasks[index]
            .options
            .all_link_errors()
            .iter()
            .map(|sig| sig.task.clone())
            .collect()
    };

    // Both vectors are public and pair up by index. A trailing forward step
    // with nothing to undo must not shift the compensations behind it.
    let missing = CompensationWorkflow {
        forward: vec![
            sig("charge_payment"),
            sig("ship_order"),
            sig("send_receipt"),
        ],
        compensations: vec![sig("refund_payment"), sig("cancel_shipment")],
    }
    .to_chain();

    assert_eq!(missing.len(), 3, "every forward step becomes a chain step");
    assert!(rollback_of(&missing, 0).is_empty());
    assert_eq!(
        rollback_of(&missing, 1),
        vec!["refund_payment".to_string()],
        "the pairing stays index-aligned"
    );
    assert_eq!(
        rollback_of(&missing, 2),
        vec!["cancel_shipment".to_string(), "refund_payment".to_string()],
        "a step with no compensation of its own still rolls back the steps before it"
    );

    // A compensation past the end of `forward` is only reachable from a step
    // that does not exist, so it never runs.
    let extra = CompensationWorkflow {
        forward: vec![sig("charge_payment"), sig("ship_order")],
        compensations: vec![
            sig("refund_payment"),
            sig("cancel_shipment"),
            sig("unreachable"),
        ],
    }
    .to_chain();

    assert_eq!(
        rollback_of(&extra, 1),
        vec!["refund_payment".to_string()],
        "a compensation past the end of `forward` is unreachable and ignored"
    );
}

/// Applying a saga enqueues only its head, with the remaining steps — and their
/// rollback routes — travelling in the head's payload.
#[tokio::test]
async fn saga_apply_enqueues_only_the_head_with_the_rollback_in_the_tail() {
    let broker = RecordingBroker::default();

    order_saga().apply(&broker).await.expect("saga dispatches");

    assert_eq!(
        broker.names(),
        vec!["reserve_inventory".to_string()],
        "only the head is enqueued; the rest ride along in its payload"
    );

    let tasks = broker.tasks();
    let head = payload(&tasks[0]);
    assert_eq!(
        step_names(&head, crate::CHAIN_TAIL_KEY),
        vec!["charge_payment".to_string(), "ship_order".to_string()],
        "the tail carries the remaining forward steps in order"
    );
    assert!(
        head.get(crate::dispatch::ERROR_ROUTE_KEY).is_none(),
        "the first step has no rollback: nothing has happened yet"
    );

    let tail = head
        .get(crate::CHAIN_TAIL_KEY)
        .and_then(|steps| steps.as_array())
        .expect("chain tail is an array");

    let rollback_of = |step: &serde_json::Value| -> Vec<String> {
        step.get("options")
            .and_then(|options| options.get("link_errors"))
            .and_then(|route| route.as_array())
            .map(|route| {
                route
                    .iter()
                    .filter_map(|sig| sig.get("task").and_then(|task| task.as_str()))
                    .map(str::to_string)
                    .collect()
            })
            .unwrap_or_default()
    };

    assert_eq!(
        rollback_of(&tail[0]),
        vec!["release_inventory".to_string()],
        "the tail steps carry their own rollback route to the worker"
    );
    assert_eq!(
        rollback_of(&tail[1]),
        vec![
            "refund_payment".to_string(),
            "release_inventory".to_string()
        ],
    );
}

/// An empty saga has nothing to run and nothing to compensate.
#[tokio::test]
async fn saga_apply_rejects_an_empty_saga_without_enqueuing() {
    let broker = RecordingBroker::default();
    let saga = Saga::new(CompensationWorkflow::new());

    let err = saga
        .apply(&broker)
        .await
        .expect_err("an empty saga is not a workflow");
    assert!(matches!(err, CanvasError::Invalid(_)));
    assert!(broker.names().is_empty());
}

/// The isolation level is advisory: it changes nothing about what is dispatched.
#[test]
fn saga_isolation_does_not_change_the_lowering() {
    let baseline = order_saga().to_chain();
    let serializable = order_saga()
        .with_isolation(SagaIsolation::Serializable)
        .to_chain();

    assert_eq!(
        baseline, serializable,
        "isolation is carried for the caller's bookkeeping, not lowered"
    );
}

// ============================================================================
// Pipeline
// ============================================================================

/// Stages become chain steps, in order.
#[tokio::test]
async fn pipeline_applies_as_a_chain_of_its_stages() {
    let broker = RecordingBroker::default();

    let pipeline = Pipeline::new()
        .stage(sig("extract"))
        .stage(sig("transform"))
        .stage(sig("load"))
        .with_buffer_size(64);

    assert_eq!(pipeline.to_chain().len(), 3);

    pipeline.apply(&broker).await.expect("pipeline dispatches");

    assert_eq!(
        broker.names(),
        vec!["extract".to_string()],
        "a pipeline is a chain: only the first stage is enqueued now"
    );
    let tasks = broker.tasks();
    assert_eq!(
        step_names(&payload(&tasks[0]), crate::CHAIN_TAIL_KEY),
        vec!["transform".to_string(), "load".to_string()],
    );
    assert_eq!(
        tasks[0].metadata.on_success_link.as_deref(),
        Some("transform"),
        "the immediate successor is named on the metadata too"
    );
}

/// An empty pipeline is refused rather than reported as dispatched.
#[tokio::test]
async fn pipeline_apply_rejects_an_empty_pipeline() {
    let broker = RecordingBroker::default();

    let err = Pipeline::new()
        .apply(&broker)
        .await
        .expect_err("a pipeline with no stages is not a workflow");
    assert!(matches!(err, CanvasError::Invalid(_)));
    assert!(broker.names().is_empty());
}

// ============================================================================
// FanOut
// ============================================================================

/// The consumers become one parallel group, all sharing a group id.
#[tokio::test]
async fn fan_out_applies_its_consumers_as_one_group() {
    let broker = RecordingBroker::default();

    let fan_out = FanOut::new(sig("publish_event"))
        .consumer(sig("update_search_index"))
        .consumer(sig("send_webhook"))
        .consumer(sig("invalidate_cache"));

    let group_id = fan_out
        .apply_after_source(&broker)
        .await
        .expect("consumers dispatch");

    assert_eq!(
        broker.names(),
        vec![
            "update_search_index".to_string(),
            "send_webhook".to_string(),
            "invalidate_cache".to_string(),
        ],
        "every consumer is enqueued, in declaration order"
    );
    assert!(
        !broker.names().contains(&"publish_event".to_string()),
        "the source is the caller's to dispatch; it must not be enqueued here"
    );
    for task in broker.tasks() {
        assert_eq!(
            task.metadata.group_id,
            Some(group_id),
            "the consumers form one trackable group"
        );
        assert!(
            payload(&task).get(crate::CHAIN_TAIL_KEY).is_none(),
            "a fan-out member has no successor of its own"
        );
    }
}

/// Dispatching source and consumers together would run them in parallel, on
/// data the source has not produced yet — so it is refused, loudly.
#[tokio::test]
async fn fan_out_apply_refuses_to_dispatch_the_source_with_the_consumers() {
    let broker = RecordingBroker::default();

    let fan_out = FanOut::new(sig("publish_event")).consumer(sig("send_webhook"));

    let err = fan_out
        .apply(&broker)
        .await
        .expect_err("the source cannot be sequenced before the consumers");
    assert!(err.is_invalid());
    assert!(
        err.to_string().contains("apply_after_source"),
        "the error must name the alternative, got: {}",
        err
    );
    assert!(
        broker.names().is_empty(),
        "nothing may be enqueued when the pattern cannot be honoured"
    );
}

/// A fan-out with no consumers broadcasts to nobody.
#[tokio::test]
async fn fan_out_without_consumers_is_rejected() {
    let broker = RecordingBroker::default();

    let err = FanOut::new(sig("publish_event"))
        .apply_after_source(&broker)
        .await
        .expect_err("there is nothing to broadcast to");
    assert!(err.is_invalid());
    assert!(broker.names().is_empty());
}

// ============================================================================
// FanIn / ScatterGather (barrier-free assertions)
// ============================================================================

/// The lowered chord keeps the sources as its header and the aggregator as its
/// body.
#[test]
fn fan_in_lowers_to_a_chord_of_its_sources() {
    let fan_in = FanIn::new(sig("merge_reports"))
        .source(sig("fetch_sales"))
        .source(sig("fetch_returns"));

    let chord = fan_in.to_chord();
    assert_eq!(
        chord.header.task_names(),
        vec!["fetch_sales", "fetch_returns"]
    );
    assert_eq!(chord.body.task, "merge_reports");
}

/// The honest no-barrier dispatch: sources only, aggregator never.
#[tokio::test]
async fn fan_in_apply_sources_only_never_enqueues_the_aggregator() {
    let broker = RecordingBroker::default();

    let fan_in = FanIn::new(sig("merge_reports"))
        .source(sig("fetch_sales"))
        .source(sig("fetch_returns"));

    fan_in
        .apply_sources_only(&broker)
        .await
        .expect("sources dispatch");

    assert_eq!(
        broker.names(),
        vec!["fetch_sales".to_string(), "fetch_returns".to_string()]
    );
    assert!(!broker.names().contains(&"merge_reports".to_string()));
}

/// A fan-in with no sources has nothing to aggregate.
#[tokio::test]
async fn fan_in_without_sources_is_rejected() {
    let broker = RecordingBroker::default();

    let err = FanIn::new(sig("merge_reports"))
        .apply_sources_only(&broker)
        .await
        .expect_err("there is nothing to aggregate");
    assert!(err.is_invalid());
    assert!(broker.names().is_empty());
}

/// The lowered chord runs the workers in parallel and the gather task after
/// them.
#[test]
fn scatter_gather_lowers_to_a_chord_of_its_workers() {
    let pattern = ScatterGather::new(
        sig("split_batch"),
        vec![sig("process_shard"), sig("process_shard")],
        sig("merge_shards"),
    );

    let chord = pattern.to_chord();
    assert_eq!(chord.header.len(), 2);
    assert_eq!(chord.body.task, "merge_shards");
    assert_eq!(
        pattern.to_group().task_names(),
        vec!["process_shard", "process_shard"],
        "the workers on their own are a plain parallel group"
    );
}

/// Dispatching the scatter step together with the workers would run them
/// against data that does not exist yet.
#[tokio::test]
async fn scatter_gather_apply_refuses_to_dispatch_the_scatter_step_with_the_workers() {
    let broker = RecordingBroker::default();

    let pattern = ScatterGather::new(
        sig("split_batch"),
        vec![sig("process_shard")],
        sig("merge_shards"),
    );

    let err = pattern
        .apply(&broker)
        .await
        .expect_err("the scatter step cannot be sequenced before the workers");
    assert!(err.is_invalid());
    assert!(
        err.to_string().contains("apply_after_scatter"),
        "the error must name the alternative, got: {}",
        err
    );
    assert!(broker.names().is_empty());
}

/// Without a result backend there is no barrier to count against, so the
/// gather step cannot be honoured and the dispatch refuses instead of
/// degrading into a plain group.
#[cfg(not(feature = "backend-redis"))]
#[tokio::test]
async fn scatter_gather_after_scatter_refuses_without_a_backend() {
    let broker = RecordingBroker::default();

    let pattern = ScatterGather::new(
        sig("split_batch"),
        vec![sig("process_shard")],
        sig("merge_shards"),
    );

    let err = pattern
        .apply_after_scatter(&broker)
        .await
        .expect_err("a gather barrier needs a result backend");
    assert!(err.is_invalid());
    assert!(
        err.to_string().contains("result backend"),
        "the error must say what is missing, got: {}",
        err
    );
    assert!(broker.names().is_empty());
}

/// Same for a fan-in: its aggregator is a barrier, not a link.
#[cfg(not(feature = "backend-redis"))]
#[tokio::test]
async fn fan_in_apply_refuses_without_a_backend() {
    let broker = RecordingBroker::default();

    let fan_in = FanIn::new(sig("merge_reports")).source(sig("fetch_sales"));

    let err = fan_in
        .apply(&broker)
        .await
        .expect_err("an aggregation barrier needs a result backend");
    assert!(err.is_invalid());
    assert!(broker.names().is_empty());
}

// ============================================================================
// FanIn / ScatterGather barriers (observable only through a result backend)
// ============================================================================

#[cfg(feature = "backend-redis")]
mod barrier {
    use super::*;
    use crate::tests_backend::MockResultBackend;

    /// `FanIn::apply` must register the barrier against the real source task
    /// ids, stamp them with the chord id, and hold the aggregator back.
    #[tokio::test]
    async fn fan_in_apply_registers_the_barrier_and_holds_the_aggregator() {
        let broker = RecordingBroker::default();
        let mut backend = MockResultBackend::new();

        let fan_in = FanIn::new(sig("merge_reports"))
            .source(sig("fetch_sales"))
            .source(sig("fetch_returns"))
            .source(sig("fetch_refunds"));

        let chord_id = fan_in
            .apply(&broker, &mut backend)
            .await
            .expect("fan-in dispatches");

        assert_eq!(
            broker.names(),
            vec![
                "fetch_sales".to_string(),
                "fetch_returns".to_string(),
                "fetch_refunds".to_string(),
            ],
            "only the sources are enqueued; the aggregator waits for the barrier"
        );

        let state = backend.only_state();
        assert_eq!(state.chord_id, chord_id);
        assert_eq!(state.total, 3);
        assert_eq!(state.callback.as_deref(), Some("merge_reports"));
        assert_eq!(
            state.task_ids,
            broker
                .tasks()
                .into_iter()
                .map(|task| task.metadata.id)
                .collect::<Vec<_>>(),
            "the barrier must record the real source task ids, in declaration order"
        );
        for task in broker.tasks() {
            assert_eq!(task.metadata.chord_id, Some(chord_id));
        }
    }

    /// `ScatterGather::apply_after_scatter` registers the gather barrier over
    /// the workers, carries the configured timeout onto it, and never enqueues
    /// the scatter step.
    #[tokio::test]
    async fn scatter_gather_after_scatter_registers_the_barrier_with_its_timeout() {
        let broker = RecordingBroker::default();
        let mut backend = MockResultBackend::new();

        let pattern = ScatterGather::new(
            sig("split_batch"),
            vec![
                sig("process_shard").with_args(vec![serde_json::json!(0)]),
                sig("process_shard").with_args(vec![serde_json::json!(1)]),
            ],
            sig("merge_shards"),
        )
        .with_timeout(90);

        let chord_id = pattern
            .apply_after_scatter(&broker, &mut backend)
            .await
            .expect("workers dispatch");

        assert_eq!(
            broker.names(),
            vec!["process_shard".to_string(), "process_shard".to_string()],
            "the scatter step already ran; only the workers are enqueued"
        );
        assert!(!broker.names().contains(&"merge_shards".to_string()));

        let state = backend.only_state();
        assert_eq!(state.chord_id, chord_id);
        assert_eq!(state.total, 2);
        assert_eq!(state.callback.as_deref(), Some("merge_shards"));
        assert_eq!(
            state.timeout,
            Some(std::time::Duration::from_secs(90)),
            "the gather timeout is recorded on the barrier"
        );
        for task in broker.tasks() {
            assert_eq!(
                task.metadata.chord_id,
                Some(chord_id),
                "every worker must carry the chord id so the worker can count it"
            );
            assert_eq!(task.metadata.group_id, Some(chord_id));
        }
    }

    /// No workers means no barrier: nothing is registered and nothing is
    /// enqueued.
    #[tokio::test]
    async fn scatter_gather_without_workers_registers_no_barrier() {
        let broker = RecordingBroker::default();
        let mut backend = MockResultBackend::new();

        let pattern = ScatterGather::new(sig("split_batch"), vec![], sig("merge_shards"));

        let err = pattern
            .apply_after_scatter(&broker, &mut backend)
            .await
            .expect_err("there is nothing to scatter to");
        assert!(err.is_invalid());
        assert!(backend.states().is_empty());
        assert!(broker.names().is_empty());
    }
}
